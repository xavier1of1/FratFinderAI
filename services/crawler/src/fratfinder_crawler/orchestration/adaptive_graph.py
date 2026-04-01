
from __future__ import annotations

import logging
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Callable

from langgraph.graph import END, StateGraph

from fratfinder_crawler.adaptive import AdaptivePolicy, canonicalize_url, compute_template_signature, discover_frontier_links, evaluate_stop_conditions, host_family, score_frontier_item, score_reward
from fratfinder_crawler.analysis import analyze_page, classify_source, detect_embedded_data, select_extraction_plan
from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.config import Settings, get_settings
from fratfinder_crawler.db.repository import CrawlerRepository
from fratfinder_crawler.http.client import HttpClient
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import AmbiguousRecordError, CrawlMetrics, ExtractedChapter, FrontierItem, PageObservation, PolicyDecision, ReviewItemCandidate
from fratfinder_crawler.normalization import normalize_record
from fratfinder_crawler.orchestration.state import AdaptiveCrawlState

LOGGER = logging.getLogger(__name__)


class AdaptiveCrawlOrchestrator:
    def __init__(self, repository: CrawlerRepository, http_client: HttpClient, registry: AdapterRegistry, *, settings: Settings | None = None, runtime_mode: str = "adaptive_shadow"):
        self._repository = repository
        self._http = http_client
        self._registry = registry
        self._settings = settings or get_settings()
        self._runtime_mode = runtime_mode
        self._policy = AdaptivePolicy(epsilon=self._settings.crawler_adaptive_epsilon, policy_version=self._settings.crawler_policy_version)
        self._graph = self._build_graph()

    def run_for_source(self, source) -> CrawlMetrics:
        run_id = self._repository.start_crawl_run(source.id)
        initial_state: AdaptiveCrawlState = {
            "source": source,
            "run_id": run_id,
            "runtime_mode": self._runtime_mode,
            "policy_mode": self._runtime_mode,
            "seed_urls": [source.list_url],
            "frontier_items": [],
            "visited_urls": [],
            "extracted": [],
            "review_items": [],
            "metrics": CrawlMetrics(),
            "reward_events": [],
            "budget_state": {
                "max_pages": self._settings.crawler_frontier_max_pages_per_source,
                "max_depth": self._settings.crawler_frontier_max_depth,
                "max_empty_streak": self._settings.crawler_frontier_max_empty_streak,
                "saturation_threshold": self._settings.crawler_adaptive_stop_saturation_threshold,
                "min_score": self._settings.crawler_adaptive_min_score,
                "pages_processed": 0,
                "empty_streak": 0,
                "low_yield_streak": 0,
            },
            "navigation_stats": {"frontier_added": 0, "frontier_visited": 0},
            "final_status": "succeeded",
        }
        final_state = self._graph.invoke(initial_state)
        log_event(LOGGER, "adaptive_crawl_run_finished", run_id=run_id, source_slug=source.source_slug, runtime_mode=self._runtime_mode, stop_reason=final_state.get("stop_reason"), records_upserted=final_state["metrics"].records_upserted)
        return final_state["metrics"]

    def _with_error_boundary(self, func: Callable[[AdaptiveCrawlState], dict[str, Any]]) -> Callable[[AdaptiveCrawlState], dict[str, Any]]:
        def wrapper(state: AdaptiveCrawlState) -> dict[str, Any]:
            try:
                return func(state)
            except Exception as exc:  # pragma: no cover
                log_event(LOGGER, "adaptive_graph_node_failed", level=logging.ERROR, error=str(exc), node=func.__name__, run_id=state.get("run_id"), crawl_session_id=state.get("crawl_session_id"))
                return {"error": str(exc), "final_status": "failed"}

        return wrapper

    def _build_graph(self):
        graph = StateGraph(AdaptiveCrawlState)
        nodes = {
            "initialize_session": self._initialize_session,
            "load_session_checkpoint": self._load_session_checkpoint,
            "seed_frontier": self._seed_frontier,
            "select_frontier_item": self._select_frontier_item,
            "fetch_page_http": self._fetch_page_http,
            "analyze_page": self._analyze_page,
            "compute_template_signature": self._compute_template_signature,
            "propose_actions": self._propose_actions,
            "score_actions": self._score_actions,
            "execute_action": self._execute_action,
            "extract_records_or_stubs": self._extract_records_or_stubs,
            "expand_frontier": self._expand_frontier,
            "score_reward": self._score_reward,
            "update_template_memory": self._update_template_memory,
            "update_policy_state": self._update_policy_state,
            "persist_checkpoint": self._persist_checkpoint,
            "evaluate_stop_conditions": self._evaluate_stop_conditions,
            "finalize": self._finalize,
        }
        for name, method in nodes.items():
            graph.add_node(name, self._with_error_boundary(method))
        graph.set_entry_point("initialize_session")
        graph.add_edge("initialize_session", "load_session_checkpoint")
        graph.add_edge("load_session_checkpoint", "seed_frontier")
        graph.add_conditional_edges("seed_frontier", self._after_branch, {"continue": "select_frontier_item", "done": "finalize"})
        graph.add_conditional_edges("select_frontier_item", self._after_branch, {"continue": "fetch_page_http", "done": "finalize"})
        graph.add_conditional_edges("fetch_page_http", self._has_error, {"ok": "analyze_page", "error": "finalize"})
        graph.add_conditional_edges("analyze_page", self._has_error, {"ok": "compute_template_signature", "error": "finalize"})
        graph.add_edge("compute_template_signature", "propose_actions")
        graph.add_edge("propose_actions", "score_actions")
        graph.add_edge("score_actions", "execute_action")
        graph.add_edge("execute_action", "extract_records_or_stubs")
        graph.add_edge("extract_records_or_stubs", "expand_frontier")
        graph.add_edge("expand_frontier", "score_reward")
        graph.add_edge("score_reward", "update_template_memory")
        graph.add_edge("update_template_memory", "update_policy_state")
        graph.add_edge("update_policy_state", "persist_checkpoint")
        graph.add_conditional_edges("persist_checkpoint", self._has_error, {"ok": "evaluate_stop_conditions", "error": "finalize"})
        graph.add_conditional_edges("evaluate_stop_conditions", self._after_branch, {"continue": "select_frontier_item", "done": "finalize"})
        graph.add_edge("finalize", END)
        return graph.compile()

    def _has_error(self, state: AdaptiveCrawlState) -> str:
        return "error" if state.get("error") else "ok"

    def _after_branch(self, state: AdaptiveCrawlState) -> str:
        return "done" if state.get("stop_reason") else "continue"

    def _initialize_session(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        if state.get("crawl_session_id"):
            return {}
        session_id = self._repository.start_crawl_session(
            crawl_run_id=state["run_id"],
            source_id=state["source"].id,
            runtime_mode=state.get("runtime_mode", self._runtime_mode),
            seed_urls=state.get("seed_urls", [state["source"].list_url]),
            budget_config=state.get("budget_state", {}),
        )
        return {"crawl_session_id": session_id}

    def _load_session_checkpoint(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        session = self._repository.load_recent_crawl_session(state["run_id"])
        if session is None:
            return {}
        budget_state = dict(state.get("budget_state") or {})
        budget_state.update(session.get("budget_config") or {})
        return {"budget_state": budget_state}

    def _seed_frontier(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        session_id = state["crawl_session_id"]
        if self._repository.count_frontier_items(session_id, state="queued") > 0 or self._repository.count_frontier_items(session_id, state="visited") > 0:
            return {}
        items = [FrontierItem(id=None, url=url, canonical_url=canonicalize_url(url), parent_url=None, depth=0, anchor_text="seed", discovered_from="seed", score_total=5.0, score_components={"seed": 5.0}) for url in (state.get("seed_urls") or [state["source"].list_url])]
        created = self._repository.enqueue_frontier_items(session_id, items)
        navigation_stats = dict(state.get("navigation_stats") or {})
        navigation_stats["frontier_added"] = navigation_stats.get("frontier_added", 0) + created
        return {"navigation_stats": navigation_stats, "stop_reason": "seed_frontier_empty" if created == 0 else None}

    def _select_frontier_item(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        current = self._repository.pop_next_frontier_item(state["crawl_session_id"])
        if current is None:
            return {"stop_reason": "frontier_empty"}
        if current.score_total < self._settings.crawler_adaptive_min_score:
            return {"current_frontier_item": current, "stop_reason": "min_score_floor"}
        navigation_stats = dict(state.get("navigation_stats") or {})
        navigation_stats["frontier_visited"] = navigation_stats.get("frontier_visited", 0) + 1
        return {"current_frontier_item": current, "current_page_url": current.url, "navigation_stats": navigation_stats, "stop_reason": None}

    def _fetch_page_http(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        started = time.perf_counter()
        html = self._http.get(state["current_frontier_item"].url)
        latency_ms = int((time.perf_counter() - started) * 1000)
        metrics = state["metrics"]
        metrics.pages_processed += 1
        budget_state = dict(state.get("budget_state") or {})
        budget_state["pages_processed"] = metrics.pages_processed
        return {"current_page_html": html, "current_page_status": 200, "current_fetch_latency_ms": latency_ms, "metrics": metrics, "budget_state": budget_state}
    def _analyze_page(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        analysis = analyze_page(state["current_page_html"])
        classification = classify_source(analysis, llm_enabled=False)
        embedded = detect_embedded_data(state["current_page_html"], state["current_page_url"])
        return {"page_analysis": analysis, "classification": classification, "embedded_data": embedded, "page_level_confidence": classification.confidence}

    def _compute_template_signature(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        return {"template_signature": compute_template_signature(state["current_page_url"], state["page_analysis"])}

    def _propose_actions(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        plan = select_extraction_plan(page_analysis=state["page_analysis"], classification=state["classification"], embedded_data=state["embedded_data"], llm_enabled=False, source_metadata=state["source"].metadata)
        actions: list[str] = []
        primary = self._strategy_to_action(plan.primary_strategy)
        if primary:
            actions.append(primary)
        for strategy in plan.fallback_strategies:
            action = self._strategy_to_action(strategy)
            if action and action not in actions:
                actions.append(action)
        if state["classification"].needs_follow_links and "expand_same_section_links" not in actions:
            actions.append("expand_same_section_links")
        if state["page_analysis"].has_map_widget and "expand_map_children" not in actions:
            actions.append("expand_map_children")
        if state["page_analysis"].link_count >= 8 and "expand_internal_links" not in actions:
            actions.append("expand_internal_links")
        if state["page_analysis"].repeated_block_count > 0 and "extract_stubs_only" not in actions:
            actions.append("extract_stubs_only")
        for fallback in ("review_branch", "stop_branch"):
            if fallback not in actions:
                actions.append(fallback)
        return {"candidate_actions": actions}

    def _score_actions(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        current = state["current_frontier_item"]
        profile = self._repository.get_template_profile(state["template_signature"], host_family(state["current_page_url"]))
        context = {
            "page_type": state["classification"].page_type,
            "probable_page_role": state["page_analysis"].probable_page_role,
            "has_map_widget": state["page_analysis"].has_map_widget,
            "has_script_json": state["page_analysis"].has_script_json or state["page_analysis"].has_json_ld,
            "table_count": state["page_analysis"].table_count,
            "repeated_block_count": state["page_analysis"].repeated_block_count,
            "keyword_score": float(current.score_components.get("keyword", 0.0)),
        }
        decisions = self._policy.choose_action(state.get("candidate_actions", []), context=context, template_profile=profile, mode=state.get("runtime_mode", self._runtime_mode))
        if not decisions:
            return {"selected_action": "review_branch", "selected_action_score": 0.0, "selected_action_score_components": {}, "policy_features": context}
        selected = self._select_shadow_action(state, decisions) if state.get("runtime_mode") == "adaptive_shadow" else decisions[0]
        return {"candidate_actions": decisions, "selected_action": selected.action_type, "selected_action_score": selected.score, "selected_action_score_components": selected.score_components, "policy_features": context}

    def _execute_action(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        action = state.get("selected_action") or "review_branch"
        review_items = list(state.get("review_items", []))
        if action == "review_branch":
            review_items.append(ReviewItemCandidate(item_type="adaptive_review_branch", reason="Adaptive runtime could not identify a confident extraction path", source_slug=state["source"].source_slug, chapter_slug=None, payload={"source_url": state["current_page_url"], "templateSignature": state.get("template_signature"), "candidateActions": [d.action_type for d in state.get("candidate_actions", [])]}))
        return {"review_items": review_items, "extraction_notes": action}

    def _extract_records_or_stubs(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        action = state.get("selected_action") or "review_branch"
        strategy = self._action_to_strategy(action, state)
        extracted: list[ExtractedChapter] = []
        seeded_frontier: list[FrontierItem] = []
        metrics = state["metrics"]
        if action.startswith("extract_") and strategy:
            adapter = self._registry.get(strategy)
            if adapter is not None:
                if action == "extract_stubs_only":
                    stubs = adapter.parse_stubs(state["current_page_html"], state["current_page_url"], api_url=state["embedded_data"].api_url, http_client=self._http, source_metadata=state["source"].metadata)
                    for stub in stubs[: self._settings.crawler_frontier_max_pages_per_template]:
                        target_url = stub.detail_url or stub.outbound_chapter_url_candidate
                        if not target_url:
                            continue
                        canonical = canonicalize_url(target_url, state["current_page_url"])
                        score, components = score_frontier_item(canonical, anchor_text=stub.chapter_name, depth=state["current_frontier_item"].depth + 1, source_url=state["source"].list_url, page_analysis=state["page_analysis"], template_bonus=0.7, parent_success_bonus=0.4)
                        seeded_frontier.append(FrontierItem(id=None, url=canonical, canonical_url=canonical, parent_url=state["current_page_url"], depth=state["current_frontier_item"].depth + 1, anchor_text=stub.chapter_name, discovered_from="chapter_stub", score_total=score, score_components=components))
                else:
                    extracted = adapter.parse(state["current_page_html"], state["current_page_url"], api_url=state["embedded_data"].api_url, http_client=self._http, source_metadata=state["source"].metadata)
                    extracted = self._dedupe_extracted_records(extracted)
                    metrics.records_seen += len(extracted)
        return {"extracted": self._merge_extracted_records(state.get("extracted", []), extracted), "extracted_from_current": extracted, "frontier_items": seeded_frontier, "metrics": metrics}

    def _expand_frontier(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        current = state["current_frontier_item"]
        if current.depth >= self._settings.crawler_frontier_max_depth:
            return {"current_links": []}
        new_items = list(state.get("frontier_items", []))
        current_links: list[dict[str, Any]] = []
        for link in discover_frontier_links(state["current_page_html"], state["current_page_url"]):
            url = str(link["url"])
            if host_family(url) != host_family(state["source"].list_url):
                continue
            if state.get("selected_action") == "expand_same_section_links":
                left = "/".join(part for part in current.canonical_url.split("/")[3:5] if part)
                right = "/".join(part for part in url.split("/")[3:5] if part)
                if left and right and left != right:
                    continue
            score, components = score_frontier_item(url, anchor_text=str(link.get("anchor_text") or "") or None, depth=current.depth + 1, source_url=state["source"].list_url, page_analysis=state["page_analysis"], template_bonus=0.2, parent_success_bonus=0.4 if state.get("extracted_from_current") else 0.0)
            if score < self._settings.crawler_adaptive_min_score:
                continue
            new_items.append(FrontierItem(id=None, url=url, canonical_url=url, parent_url=current.url, depth=current.depth + 1, anchor_text=str(link.get("anchor_text") or "") or None, discovered_from="page_link", score_total=score, score_components=components))
            current_links.append({"url": url, "anchorText": link.get("anchor_text"), "score": score})
            if len(current_links) >= self._settings.crawler_frontier_max_pages_per_template:
                break
        if state["embedded_data"].api_url and state.get("selected_action") in {"expand_map_children", "extract_locator_api"}:
            api_url = canonicalize_url(state["embedded_data"].api_url, state["current_page_url"])
            score, components = score_frontier_item(api_url, anchor_text="embedded api", depth=current.depth + 1, source_url=state["source"].list_url, page_analysis=state["page_analysis"], template_bonus=1.5, parent_success_bonus=0.5)
            new_items.append(FrontierItem(id=None, url=api_url, canonical_url=api_url, parent_url=current.url, depth=current.depth + 1, anchor_text="embedded api", discovered_from="embedded_api", score_total=score, score_components=components))
            current_links.append({"url": api_url, "anchorText": "embedded api", "score": score})
        added = self._repository.enqueue_frontier_items(state["crawl_session_id"], new_items)
        navigation_stats = dict(state.get("navigation_stats") or {})
        navigation_stats["frontier_added"] = navigation_stats.get("frontier_added", 0) + added
        return {"current_links": current_links, "navigation_stats": navigation_stats}
    def _score_reward(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        extracted_current = state.get("extracted_from_current", [])
        reward = score_reward(action_type=state.get("selected_action") or "review_branch", extracted=extracted_current, links_added=len(state.get("current_links", [])), review_created=state.get("selected_action") == "review_branch")
        budget_state = dict(state.get("budget_state") or {})
        if extracted_current:
            budget_state["empty_streak"] = 0
            budget_state["low_yield_streak"] = 0
        else:
            budget_state["empty_streak"] = int(budget_state.get("empty_streak", 0)) + 1
            budget_state["low_yield_streak"] = int(budget_state.get("low_yield_streak", 0)) + 1
        self._policy.observe(reward.action_type, reward.reward_value)
        return {"reward_events": [reward], "budget_state": budget_state}

    def _update_template_memory(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        extracted_current = state.get("extracted_from_current", [])
        contact_yield = sum(1 for record in extracted_current if record.website_url or record.contact_email or record.instagram_url)
        self._repository.upsert_template_profile(template_signature=state["template_signature"], host_family=host_family(state["current_page_url"]), page_role_guess=state["page_analysis"].probable_page_role, action_type=state.get("selected_action") or "review_branch", extraction_family=self._action_to_strategy(state.get("selected_action") or "review_branch", state), chapter_yield=len(extracted_current), contact_yield=contact_yield, timeout=False, empty=len(extracted_current) == 0)
        return {}

    def _update_policy_state(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        return {"policy_features": self._policy.snapshot()}

    def _persist_checkpoint(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        candidate_actions = [{"actionType": d.action_type, "score": d.score, "scoreComponents": d.score_components, "predictedReward": d.predicted_reward} for d in state.get("candidate_actions", [])]
        observation_id = self._repository.append_page_observation(PageObservation(id=None, crawl_session_id=state["crawl_session_id"], url=state["current_page_url"], template_signature=state["template_signature"], http_status=state.get("current_page_status"), latency_ms=state.get("current_fetch_latency_ms", 0), page_analysis=_to_serializable(state.get("page_analysis")) or {}, classification=_to_serializable(state.get("classification")) or {}, embedded_data=_to_serializable(state.get("embedded_data")) or {}, candidate_actions=candidate_actions, selected_action=state.get("selected_action"), selected_action_score=state.get("selected_action_score"), selected_action_score_components=state.get("selected_action_score_components") or {}, outcome={"recordsExtracted": len(state.get("extracted_from_current", [])), "linksQueued": len(state.get("current_links", [])), "stopReason": state.get("stop_reason")}))
        for event in state.get("reward_events", []):
            self._repository.append_reward_event(state["crawl_session_id"], observation_id, event)
        self._repository.save_policy_snapshot(policy_version=self._policy.policy_version, runtime_mode=state.get("runtime_mode", self._runtime_mode), feature_schema_version="adaptive-v1", model_payload=self._policy.snapshot(), metrics={"runId": state["run_id"], "sourceSlug": state["source"].source_slug, "templateSignature": state.get("template_signature")})
        return {"persisted_observation_id": observation_id}

    def _evaluate_stop_conditions(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        frontier_remaining = self._repository.count_frontier_items(state["crawl_session_id"], state="queued")
        should_stop, reason = evaluate_stop_conditions(budget_state=state.get("budget_state", {}), frontier_remaining=frontier_remaining, current_score=state.get("current_frontier_item").score_total if state.get("current_frontier_item") else None)
        return {"stop_reason": reason if should_stop else None}

    def _finalize(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        source = state["source"]
        run_id = state["run_id"]
        session_id = state.get("crawl_session_id")
        metrics = state.get("metrics", CrawlMetrics())
        extracted = self._dedupe_extracted_records(state.get("extracted", []))
        review_items = list(state.get("review_items", []))
        persisted: list[dict[str, Any]] = []
        for record in extracted:
            try:
                chapter, provenance = normalize_record(source, record)
                chapter_id = self._repository.upsert_chapter(source, chapter)
                self._repository.insert_provenance(chapter_id, source.id, run_id, provenance)
                metrics.records_upserted += 1
                persisted.append({"chapter": chapter, "chapter_id": chapter_id})
            except AmbiguousRecordError as exc:
                review_items.append(ReviewItemCandidate(item_type="adaptive_ambiguous_record", reason=str(exc), source_slug=source.source_slug, chapter_slug=None, payload={"source_url": record.source_url, "snippet": record.source_snippet}))
        for review_item in review_items:
            self._repository.create_review_item(source.id, run_id, review_item)
            metrics.review_items_created += 1
        for bundle in persisted:
            chapter = bundle["chapter"]
            if not chapter.missing_optional_fields:
                continue
            metrics.field_jobs_created += self._repository.create_field_jobs(chapter_id=bundle["chapter_id"], crawl_run_id=run_id, chapter_slug=chapter.slug, source_slug=source.source_slug, missing_fields=chapter.missing_optional_fields)
        extraction_metadata = {"strategy_used": state.get("selected_action"), "runtime_mode": state.get("runtime_mode", self._runtime_mode), "page_level_confidence": state.get("page_level_confidence"), "template_signature": state.get("template_signature"), "stop_reason": state.get("stop_reason"), "navigation_stats": state.get("navigation_stats", {}), "policy_snapshot": self._policy.snapshot()}
        page_analysis_payload = _to_serializable(state.get("page_analysis"))
        classification_payload = _to_serializable(state.get("classification"))
        if state.get("error"):
            self._repository.create_review_item(source_id=source.id, crawl_run_id=run_id, candidate=ReviewItemCandidate(item_type="crawl_failure", reason=state.get("error", "unknown failure"), source_slug=source.source_slug, chapter_slug=None, payload={"source_url": source.list_url, "runtimeMode": state.get("runtime_mode", self._runtime_mode)}))
            metrics.review_items_created += 1
            self._repository.finish_crawl_run(run_id=run_id, status="failed", metrics=metrics, last_error=state.get("error"), page_analysis=page_analysis_payload, classification=classification_payload, extraction_metadata=extraction_metadata)
            if session_id:
                self._repository.finish_crawl_session(session_id, status="failed", stop_reason=state.get("stop_reason") or "error", summary={"recordsUpserted": metrics.records_upserted, "pagesProcessed": metrics.pages_processed, "reviewItemsCreated": metrics.review_items_created})
            return {"metrics": metrics, "final_status": "failed", "error": state.get("error")}
        status = state.get("final_status", "succeeded")
        if metrics.review_items_created > 0 and metrics.records_upserted == 0:
            status = "partial"
        self._repository.finish_crawl_run(run_id=run_id, status=status, metrics=metrics, page_analysis=page_analysis_payload, classification=classification_payload, extraction_metadata=extraction_metadata)
        if session_id:
            self._repository.finish_crawl_session(session_id, status=status, stop_reason=state.get("stop_reason"), summary={"recordsUpserted": metrics.records_upserted, "pagesProcessed": metrics.pages_processed, "reviewItemsCreated": metrics.review_items_created, "fieldJobsCreated": metrics.field_jobs_created})
        return {"metrics": metrics, "final_status": status, "stop_reason": state.get("stop_reason")}

    def _strategy_to_action(self, strategy: str | None) -> str | None:
        return {"table": "extract_table", "repeated_block": "extract_repeated_block", "script_json": "extract_script_json", "locator_api": "extract_locator_api", "review": "review_branch"}.get(strategy or "")

    def _action_to_strategy(self, action: str, state: AdaptiveCrawlState) -> str | None:
        reverse = {"extract_table": "table", "extract_repeated_block": "repeated_block", "extract_script_json": "script_json", "extract_locator_api": "locator_api"}
        if action == "extract_stubs_only":
            plan = select_extraction_plan(page_analysis=state["page_analysis"], classification=state["classification"], embedded_data=state["embedded_data"], llm_enabled=False, source_metadata=state["source"].metadata)
            return plan.primary_strategy if plan.primary_strategy in {"table", "repeated_block", "script_json", "locator_api"} else "repeated_block"
        return reverse.get(action)

    def _select_shadow_action(self, state: AdaptiveCrawlState, decisions: list[PolicyDecision]) -> PolicyDecision:
        plan = select_extraction_plan(page_analysis=state["page_analysis"], classification=state["classification"], embedded_data=state["embedded_data"], llm_enabled=False, source_metadata=state["source"].metadata)
        action = self._strategy_to_action(plan.primary_strategy) or ("expand_same_section_links" if state["classification"].needs_follow_links else "review_branch")
        for decision in decisions:
            if decision.action_type == action:
                return decision
        return decisions[0]

    def _dedupe_extracted_records(self, records: list[ExtractedChapter]) -> list[ExtractedChapter]:
        deduped: dict[tuple[str, str], ExtractedChapter] = {}
        for record in records:
            key = ((record.name or "").strip().lower(), (record.university_name or "").strip().lower())
            current = deduped.get(key)
            if current is None:
                deduped[key] = record
                continue
            current_score = (current.source_confidence, sum(1 for value in (current.website_url, current.instagram_url, current.contact_email, current.city, current.state) if value))
            new_score = (record.source_confidence, sum(1 for value in (record.website_url, record.instagram_url, record.contact_email, record.city, record.state) if value))
            if new_score > current_score:
                deduped[key] = record
        return list(deduped.values())

    def _merge_extracted_records(self, existing: list[ExtractedChapter], new_records: list[ExtractedChapter]) -> list[ExtractedChapter]:
        return self._dedupe_extracted_records([*existing, *new_records])


def _to_serializable(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if is_dataclass(value):
        return _sanitize_json_value(asdict(value))
    if isinstance(value, dict):
        return _sanitize_json_value(value)
    return None


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value
