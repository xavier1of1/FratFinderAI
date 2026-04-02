
from __future__ import annotations

import logging
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Callable

from langgraph.graph import END, StateGraph

from fratfinder_crawler.adaptive import AdaptivePolicy, build_delayed_credit_events, canonicalize_url, compute_structural_template_signature, compute_template_signature, discover_frontier_links, evaluate_stop_conditions, host_family, score_frontier_item, score_reward, score_terminal_reward
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

_VALID_MISSING_SNIPPET_MARKERS = (
    "suspended",
    "disbanded",
    "inactive chapter",
    "no longer active",
    "charter revoked",
)


class AdaptiveCrawlOrchestrator:
    def __init__(
        self,
        repository: CrawlerRepository,
        http_client: HttpClient,
        registry: AdapterRegistry,
        *,
        settings: Settings | None = None,
        runtime_mode: str = "adaptive_shadow",
        policy_mode: str = "live",
    ):
        self._repository = repository
        self._http = http_client
        self._registry = registry
        self._settings = settings or get_settings()
        self._runtime_mode = runtime_mode
        self._policy_mode = policy_mode
        self._policy = AdaptivePolicy(
            epsilon=self._settings.crawler_adaptive_epsilon,
            policy_version=self._settings.crawler_policy_version,
            live_epsilon=self._settings.crawler_adaptive_live_epsilon,
            train_epsilon=self._settings.crawler_adaptive_train_epsilon,
            risk_timeout_weight=self._settings.crawler_adaptive_risk_timeout_weight,
            risk_requeue_weight=self._settings.crawler_adaptive_risk_requeue_weight,
        )
        if self._settings.crawler_adaptive_policy_restore_enabled:
            self._restore_policy_snapshot()
        self._graph = self._build_graph()

    def run_for_source(self, source, policy_mode: str | None = None) -> CrawlMetrics:
        run_id = self._repository.start_crawl_run(source.id)
        effective_policy_mode = (policy_mode or self._policy_mode or "live").strip().lower()
        initial_state: AdaptiveCrawlState = {
            "source": source,
            "run_id": run_id,
            "runtime_mode": self._runtime_mode,
            "policy_mode": effective_policy_mode,
            "seed_urls": [source.list_url],
            "frontier_items": [],
            "visited_urls": [],
            "extracted": [],
            "review_items": [],
            "metrics": CrawlMetrics(),
            "reward_events": [],
            "observation_index": {},
            "observation_url_index": {},
            "budget_state": {
                "max_pages": self._settings.crawler_frontier_max_pages_per_source,
                "max_depth": self._settings.crawler_frontier_max_depth,
                "max_empty_streak": self._settings.crawler_frontier_max_empty_streak,
                "saturation_threshold": self._settings.crawler_adaptive_stop_saturation_threshold,
                "min_score": self._settings.crawler_adaptive_min_score,
                "high_yield_record_threshold": self._settings.crawler_frontier_high_yield_record_threshold,
                "min_pages_for_high_yield_stop": self._settings.crawler_frontier_min_pages_for_high_yield_stop,
                "pages_processed": 0,
                "records_seen": 0,
                "empty_streak": 0,
                "low_yield_streak": 0,
                "guardrail_hits": 0,
                "valid_missing_total": 0,
                "verified_website_total": 0,
            },
            "navigation_stats": {"frontier_added": 0, "frontier_visited": 0},
            "final_status": "succeeded",
        }
        final_state = self._graph.invoke(initial_state)
        log_event(
            LOGGER,
            "adaptive_crawl_run_finished",
            run_id=run_id,
            source_slug=source.source_slug,
            runtime_mode=self._runtime_mode,
            policy_mode=effective_policy_mode,
            stop_reason=final_state.get("stop_reason"),
            records_upserted=final_state["metrics"].records_upserted,
        )
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
        analysis = state["page_analysis"]
        template_signature = compute_template_signature(state["current_page_url"], analysis)
        structural_signature = compute_structural_template_signature(state["current_page_url"], analysis)
        raw_tokens = [token for token in state["current_page_url"].split("?")[0].lower().split("/") if token]
        raw_fragment = "-".join(raw_tokens[:3]) or "root"
        template_signature_raw = f"{host_family(state['current_page_url'])}|{analysis.probable_page_role}|t{min(analysis.table_count, 3)}|r{min(analysis.repeated_block_count, 4)}|{raw_fragment}"
        return {
            "template_signature": template_signature,
            "structural_template_signature": structural_signature,
            "template_signature_raw": template_signature_raw,
        }

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
        host_profile = self._repository.get_template_profile(state["template_signature"], host_family(state["current_page_url"]))
        structural_profile = self._repository.get_template_profile(state["structural_template_signature"], "__structural__")
        profile = host_profile or structural_profile

        budget_state = dict(state.get("budget_state") or {})
        timeout_risk = min(max((profile.timeout_rate if profile else 0.0), 0.0), 1.0)
        requeue_risk = min(max(float(budget_state.get("low_yield_streak", 0)) / max(float(budget_state.get("saturation_threshold", 4)), 1.0), 0.0), 1.0)

        context = {
            "page_type": state["classification"].page_type,
            "probable_page_role": state["page_analysis"].probable_page_role,
            "has_map_widget": state["page_analysis"].has_map_widget,
            "has_script_json": state["page_analysis"].has_script_json or state["page_analysis"].has_json_ld,
            "table_count": state["page_analysis"].table_count,
            "repeated_block_count": state["page_analysis"].repeated_block_count,
            "keyword_score": float(current.score_components.get("keyword", 0.0)),
            "timeout_risk": timeout_risk,
            "requeue_risk": requeue_risk,
            "policy_mode": state.get("policy_mode", self._policy_mode),
        }
        decisions = self._policy.choose_action(
            state.get("candidate_actions", []),
            context=context,
            template_profile=profile,
            mode=state.get("runtime_mode", self._runtime_mode),
        )
        if not decisions:
            return {
                "candidate_actions": [],
                "selected_action": "review_branch",
                "selected_action_score": 0.0,
                "selected_action_score_components": {},
                "policy_features": context,
                "current_guardrail_flags": ["no_candidate_actions"],
                "current_risk_score": 0.0,
                "context_bucket": self._context_bucket(state),
            }
        selected = self._select_shadow_action(state, decisions) if state.get("runtime_mode") == "adaptive_shadow" else decisions[0]
        guardrail_flags = selected.context.get("guardrailFlags", []) if isinstance(selected.context, dict) else []
        risk_score = min(max(timeout_risk + requeue_risk, 0.0), 2.0)
        return {
            "candidate_actions": decisions,
            "selected_action": selected.action_type,
            "selected_action_score": selected.score,
            "selected_action_score_components": selected.score_components,
            "policy_features": context,
            "current_guardrail_flags": guardrail_flags,
            "current_risk_score": risk_score,
            "context_bucket": self._context_bucket(state),
        }

    def _execute_action(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        action = state.get("selected_action") or "review_branch"
        review_items = list(state.get("review_items", []))
        if action == "review_branch":
            review_items.append(
                ReviewItemCandidate(
                    item_type="adaptive_review_branch",
                    reason="Adaptive runtime could not identify a confident extraction path",
                    source_slug=state["source"].source_slug,
                    chapter_slug=None,
                    payload={
                        "source_url": state["current_page_url"],
                        "templateSignature": state.get("template_signature"),
                        "candidateActions": [d.action_type for d in state.get("candidate_actions", [])],
                        "guardrailFlags": state.get("current_guardrail_flags", []),
                    },
                )
            )
        budget_state = dict(state.get("budget_state") or {})
        guardrails = state.get("current_guardrail_flags") or []
        if guardrails:
            budget_state["guardrail_hits"] = int(budget_state.get("guardrail_hits", 0)) + 1
        return {"review_items": review_items, "extraction_notes": action, "budget_state": budget_state}

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
                    stubs = adapter.parse_stubs(
                        state["current_page_html"],
                        state["current_page_url"],
                        api_url=state["embedded_data"].api_url,
                        http_client=self._http,
                        source_metadata=state["source"].metadata,
                    )
                    for stub in stubs[: self._settings.crawler_frontier_max_pages_per_template]:
                        target_url = stub.detail_url or stub.outbound_chapter_url_candidate
                        if not target_url:
                            continue
                        canonical = canonicalize_url(target_url, state["current_page_url"])
                        score, components = score_frontier_item(
                            canonical,
                            anchor_text=stub.chapter_name,
                            depth=state["current_frontier_item"].depth + 1,
                            source_url=state["source"].list_url,
                            page_analysis=state["page_analysis"],
                            template_bonus=0.7,
                            parent_success_bonus=0.4,
                        )
                        seeded_frontier.append(
                            FrontierItem(
                                id=None,
                                url=canonical,
                                canonical_url=canonical,
                                parent_url=state["current_page_url"],
                                depth=state["current_frontier_item"].depth + 1,
                                anchor_text=stub.chapter_name,
                                discovered_from="chapter_stub",
                                score_total=score,
                                score_components=components,
                            )
                        )
                else:
                    extracted = adapter.parse(
                        state["current_page_html"],
                        state["current_page_url"],
                        api_url=state["embedded_data"].api_url,
                        http_client=self._http,
                        source_metadata=state["source"].metadata,
                    )
                    extracted = self._dedupe_extracted_records(extracted)
                    metrics.records_seen += len(extracted)

        valid_missing_count = self._count_valid_missing_records(extracted)
        verified_website_count = self._count_verified_websites(extracted, state["current_page_url"], state["source"].list_url)
        budget_state = dict(state.get("budget_state") or {})
        budget_state["records_seen"] = int(metrics.records_seen)
        budget_state["valid_missing_total"] = int(budget_state.get("valid_missing_total", 0)) + valid_missing_count
        budget_state["verified_website_total"] = int(budget_state.get("verified_website_total", 0)) + verified_website_count

        return {
            "extracted": self._merge_extracted_records(state.get("extracted", []), extracted),
            "extracted_from_current": extracted,
            "frontier_items": seeded_frontier,
            "metrics": metrics,
            "valid_missing_count_current": valid_missing_count,
            "verified_website_count_current": verified_website_count,
            "budget_state": budget_state,
        }

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
        reward = score_reward(
            action_type=state.get("selected_action") or "review_branch",
            extracted=extracted_current,
            links_added=len(state.get("current_links", [])),
            review_created=state.get("selected_action") == "review_branch",
            valid_missing_count=int(state.get("valid_missing_count_current", 0) or 0),
            verified_website_count=int(state.get("verified_website_count_current", 0) or 0),
            reward_stage="immediate",
        )
        budget_state = dict(state.get("budget_state") or {})
        if extracted_current or state.get("valid_missing_count_current", 0):
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
        selected_action = state.get("selected_action") or "review_branch"
        extraction_family = self._action_to_strategy(selected_action, state)

        self._repository.upsert_template_profile(
            template_signature=state["template_signature"],
            host_family=host_family(state["current_page_url"]),
            page_role_guess=state["page_analysis"].probable_page_role,
            action_type=selected_action,
            extraction_family=extraction_family,
            chapter_yield=len(extracted_current),
            contact_yield=contact_yield,
            timeout=False,
            empty=len(extracted_current) == 0,
        )
        self._repository.upsert_template_profile(
            template_signature=state["structural_template_signature"],
            host_family="__structural__",
            page_role_guess=state["page_analysis"].probable_page_role,
            action_type=selected_action,
            extraction_family=extraction_family,
            chapter_yield=len(extracted_current),
            contact_yield=contact_yield,
            timeout=False,
            empty=len(extracted_current) == 0,
        )
        return {}

    def _update_policy_state(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        return {"policy_features": self._policy.snapshot()}

    def _persist_checkpoint(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        candidate_actions: list[dict[str, Any]] = []
        for decision in state.get("candidate_actions", []):
            if isinstance(decision, PolicyDecision):
                candidate_actions.append(
                    {
                        "actionType": decision.action_type,
                        "score": decision.score,
                        "scoreComponents": decision.score_components,
                        "predictedReward": decision.predicted_reward,
                    }
                )
            elif isinstance(decision, str):
                candidate_actions.append({"actionType": decision, "score": 0.0, "scoreComponents": {}, "predictedReward": 0.0})

        parent_observation_id = None
        parent_url = state.get("current_frontier_item").parent_url if state.get("current_frontier_item") else None
        if parent_url:
            parent_observation_id = (state.get("observation_url_index") or {}).get(canonicalize_url(parent_url))

        observation_id = self._repository.append_page_observation(
            PageObservation(
                id=None,
                crawl_session_id=state["crawl_session_id"],
                url=state["current_page_url"],
                template_signature=state["template_signature"],
                structural_template_signature=state.get("structural_template_signature"),
                http_status=state.get("current_page_status"),
                latency_ms=state.get("current_fetch_latency_ms", 0),
                page_analysis=_to_serializable(state.get("page_analysis")) or {},
                classification=_to_serializable(state.get("classification")) or {},
                embedded_data=_to_serializable(state.get("embedded_data")) or {},
                candidate_actions=candidate_actions,
                selected_action=state.get("selected_action"),
                selected_action_score=state.get("selected_action_score"),
                selected_action_score_components=state.get("selected_action_score_components") or {},
                parent_observation_id=parent_observation_id,
                path_depth=int(state.get("current_frontier_item").depth if state.get("current_frontier_item") else 0),
                risk_score=float(state.get("current_risk_score", 0.0) or 0.0),
                guardrail_flags=[str(flag) for flag in state.get("current_guardrail_flags", [])],
                context_bucket=state.get("context_bucket"),
                outcome={
                    "recordsExtracted": len(state.get("extracted_from_current", [])),
                    "linksQueued": len(state.get("current_links", [])),
                    "stopReason": state.get("stop_reason"),
                    "templateSignatureRaw": state.get("template_signature_raw"),
                    "validMissingCount": int(state.get("valid_missing_count_current", 0) or 0),
                    "verifiedWebsiteCount": int(state.get("verified_website_count_current", 0) or 0),
                },
            )
        )

        immediate_events = list(state.get("reward_events", []))
        for event in immediate_events:
            event.attributed_observation_id = observation_id
            self._repository.append_reward_event(state["crawl_session_id"], observation_id, event)

        observation_index = dict(state.get("observation_index") or {})
        observation_index[str(observation_id)] = {
            "action": state.get("selected_action") or "review_branch",
            "parent": parent_observation_id,
        }
        observation_url_index = dict(state.get("observation_url_index") or {})
        observation_url_index[canonicalize_url(state["current_page_url"])] = observation_id

        delayed_seed = float(state.get("verified_website_count_current", 0) or 0) * 1.2 + float(
            sum(1 for record in state.get("extracted_from_current", []) if record.contact_email or record.instagram_url)
        )
        ancestors = self._ancestor_actions(parent_observation_id, observation_index)
        delayed_events = build_delayed_credit_events(
            ancestor_actions=ancestors,
            base_reward=delayed_seed,
            gamma=self._settings.crawler_adaptive_reward_gamma,
            attributed_observation_id=observation_id,
            max_hops=self._settings.crawler_adaptive_trace_hops,
        )
        for hop, event in enumerate(delayed_events, start=1):
            ancestor_observation_id = ancestors[hop - 1][0] if hop - 1 < len(ancestors) else None
            if ancestor_observation_id is not None:
                self._repository.append_reward_event(state["crawl_session_id"], ancestor_observation_id, event)
                self._policy.observe(event.action_type, event.reward_value)

        self._repository.save_policy_snapshot(
            policy_version=self._policy.policy_version,
            runtime_mode=state.get("runtime_mode", self._runtime_mode),
            feature_schema_version="adaptive-v2.1",
            model_payload=self._policy.snapshot(),
            metrics={
                "runId": state["run_id"],
                "sourceSlug": state["source"].source_slug,
                "templateSignature": state.get("template_signature"),
                "policyMode": state.get("policy_mode", self._policy_mode),
            },
        )
        return {
            "persisted_observation_id": observation_id,
            "observation_index": observation_index,
            "observation_url_index": observation_url_index,
        }

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
        extraction_metadata = {"strategy_used": state.get("selected_action"), "runtime_mode": state.get("runtime_mode", self._runtime_mode), "page_level_confidence": state.get("page_level_confidence"), "template_signature": state.get("template_signature"), "template_signature_raw": state.get("template_signature_raw"), "stop_reason": state.get("stop_reason"), "navigation_stats": state.get("navigation_stats", {}), "policy_snapshot": self._policy.snapshot()}
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
            self._repository.finish_crawl_session(
                session_id,
                status=status,
                stop_reason=state.get("stop_reason"),
                summary={
                    "recordsUpserted": metrics.records_upserted,
                    "pagesProcessed": metrics.pages_processed,
                    "reviewItemsCreated": metrics.review_items_created,
                    "fieldJobsCreated": metrics.field_jobs_created,
                    "guardrailHits": int((state.get("budget_state") or {}).get("guardrail_hits", 0)),
                    "validMissing": int((state.get("budget_state") or {}).get("valid_missing_total", 0)),
                    "verifiedWebsites": int((state.get("budget_state") or {}).get("verified_website_total", 0)),
                },
            )

        queue_efficiency = self._queue_efficiency(metrics=metrics, budget_state=state.get("budget_state") or {})
        terminal_event = score_terminal_reward(
            status=status,
            stop_reason=state.get("stop_reason"),
            queue_efficiency=queue_efficiency,
        )
        if session_id:
            self._repository.append_reward_event(session_id, None, terminal_event)
        self._policy.observe(terminal_event.action_type, terminal_event.reward_value)

        return {"metrics": metrics, "final_status": status, "stop_reason": state.get("stop_reason")}

    def _restore_policy_snapshot(self) -> None:
        snapshot = self._repository.load_latest_policy_snapshot(
            policy_version=self._policy.policy_version,
            runtime_mode=self._runtime_mode,
        )
        if snapshot is None:
            snapshot = self._repository.load_latest_policy_snapshot(policy_version=self._policy.policy_version)
        if snapshot is None:
            return
        model_payload = snapshot.get("model_payload") if isinstance(snapshot, dict) else None
        restored = self._policy.load_snapshot(model_payload if isinstance(model_payload, dict) else None)
        if restored:
            log_event(
                LOGGER,
                "adaptive_policy_snapshot_restored",
                runtime_mode=self._runtime_mode,
                policy_version=self._policy.policy_version,
                snapshot_id=snapshot.get("id"),
                snapshot_created_at=snapshot.get("created_at"),
            )

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

    def _count_valid_missing_records(self, records: list[ExtractedChapter]) -> int:
        count = 0
        for record in records:
            snippet = (record.source_snippet or "").lower()
            has_missing_contact = not record.contact_email and not record.instagram_url
            if has_missing_contact and any(marker in snippet for marker in _VALID_MISSING_SNIPPET_MARKERS):
                count += 1
        return count

    def _count_verified_websites(self, records: list[ExtractedChapter], page_url: str, source_list_url: str) -> int:
        trusted_host = host_family(source_list_url)
        current_host = host_family(page_url)
        if trusted_host != current_host:
            return 0
        return sum(1 for record in records if record.website_url)

    def _context_bucket(self, state: AdaptiveCrawlState) -> str:
        analysis = state.get("page_analysis")
        classification = state.get("classification")
        if not analysis or not classification:
            return "unknown"
        role = str(getattr(analysis, "probable_page_role", "unknown") or "unknown")
        page_type = str(getattr(classification, "page_type", "unknown") or "unknown")
        has_map = "map" if getattr(analysis, "has_map_widget", False) else "nomap"
        has_table = "table" if int(getattr(analysis, "table_count", 0) or 0) > 0 else "notable"
        return f"{page_type}:{role}:{has_map}:{has_table}"

    def _ancestor_actions(
        self,
        parent_observation_id: int | None,
        observation_index: dict[str, dict[str, Any]],
    ) -> list[tuple[int, str]]:
        ancestors: list[tuple[int, str]] = []
        current = parent_observation_id
        hops = 0
        max_hops = max(1, int(self._settings.crawler_adaptive_trace_hops))
        while current is not None and hops < max_hops:
            entry = observation_index.get(str(current))
            if not entry:
                break
            ancestors.append((current, str(entry.get("action") or "review_branch")))
            parent = entry.get("parent")
            current = int(parent) if isinstance(parent, int) else None
            hops += 1
        return ancestors

    def _queue_efficiency(self, *, metrics: CrawlMetrics, budget_state: dict[str, Any]) -> float:
        pages = max(float(metrics.pages_processed), 1.0)
        records = float(metrics.records_seen)
        burn = records / pages
        penalties = float(budget_state.get("low_yield_streak", 0)) * 0.05 + float(budget_state.get("guardrail_hits", 0)) * 0.01
        return max(-1.0, min(1.0, burn - penalties))


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
