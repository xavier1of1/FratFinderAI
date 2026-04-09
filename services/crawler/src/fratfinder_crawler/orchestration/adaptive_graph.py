
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
from fratfinder_crawler.field_jobs import FieldJobEngine, RetryableJobError
from fratfinder_crawler.http.client import HttpClient
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import (
    AmbiguousRecordError,
    ChapterCandidate,
    ChapterValidityDecision,
    ChapterSearchDecision,
    ChapterEvidenceRecord,
    CrawlMetrics,
    ExtractedChapter,
    FieldJob,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FrontierItem,
    PageObservation,
    PolicyDecision,
    ReviewItemCandidate,
    SourceClassification,
)
from fratfinder_crawler.normalization import classify_chapter_validity, normalize_record
from fratfinder_crawler.precision_tools import tool_directory_layout_profiler
from fratfinder_crawler.search import SearchClient
from fratfinder_crawler.orchestration.state import AdaptiveCrawlState
from fratfinder_crawler.orchestration.navigation import (
    build_chapter_candidates,
    classify_chapter_target,
    detect_chapter_index_mode,
    extract_chapter_stubs,
    extract_contacts_from_chapter_site,
    follow_chapter_detail_or_outbound,
)

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
        policy_version: str | None = None,
    ):
        self._repository = repository
        self._http = http_client
        self._registry = registry
        self._settings = settings or get_settings()
        self._runtime_mode = runtime_mode
        self._policy_mode = policy_mode
        self._policy = AdaptivePolicy(
            epsilon=self._settings.crawler_adaptive_epsilon,
            policy_version=(policy_version or self._settings.crawler_policy_version),
            live_epsilon=self._settings.crawler_adaptive_live_epsilon,
            train_epsilon=self._settings.crawler_adaptive_train_epsilon,
            risk_timeout_weight=self._settings.crawler_adaptive_risk_timeout_weight,
            risk_requeue_weight=self._settings.crawler_adaptive_risk_requeue_weight,
        )
        if self._settings.crawler_adaptive_policy_restore_enabled:
            self._restore_policy_snapshot()
        self._inline_enrichment_engine = (
            FieldJobEngine(
                repository=self._repository,
                logger=LOGGER,
                worker_id=f"inline-{self._runtime_mode}",
                source_slug=None,
                search_client=SearchClient(self._settings),
                search_provider=self._settings.crawler_search_provider,
                max_search_pages=self._settings.crawler_search_max_pages_per_job,
                negative_result_cooldown_days=self._settings.crawler_search_negative_cooldown_days,
                dependency_wait_seconds=self._settings.crawler_search_dependency_wait_seconds,
                require_confident_website_for_email=self._settings.crawler_search_require_confident_website_for_email,
                email_escape_on_provider_block=self._settings.crawler_search_email_escape_on_provider_block,
                email_escape_min_website_failures=self._settings.crawler_search_email_escape_min_website_failures,
                transient_short_retries=self._settings.crawler_search_transient_short_retries,
                transient_long_cooldown_seconds=self._settings.crawler_search_transient_long_cooldown_seconds,
                min_no_candidate_backoff_seconds=self._settings.crawler_search_min_no_candidate_backoff_seconds,
                email_max_queries=self._settings.crawler_search_email_max_queries,
                instagram_max_queries=self._settings.crawler_search_instagram_max_queries,
                enable_school_initials=self._settings.crawler_search_enable_school_initials,
                min_school_initial_length=self._settings.crawler_search_min_school_initial_length,
                enable_compact_fraternity=self._settings.crawler_search_enable_compact_fraternity,
                instagram_enable_handle_queries=self._settings.crawler_search_instagram_enable_handle_queries,
                instagram_direct_probe_enabled=self._settings.crawler_search_instagram_direct_probe_enabled,
                greedy_collect_mode=self._settings.crawler_greedy_collect,
            )
            if self._settings.crawler_search_enabled
            else None
        )
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
            "chapter_search_metrics": {
                "sourceClass": None,
                "candidatesExtracted": 0,
                "candidatesRejected": 0,
                "canonicalChaptersCreated": 0,
                "provisionalChaptersCreated": 0,
                "nationalTargetsFollowed": 0,
                "institutionalTargetsFollowed": 0,
                "chapterOwnedTargetsSkipped": 0,
                "broaderWebTargetsFollowed": 0,
                "rejectionReasonCounts": {},
            },
            "chapter_validity_metrics": {
                "invalidCount": 0,
                "repairableCount": 0,
                "provisionalCount": 0,
                "canonicalValidCount": 0,
                "invalidReasonCounts": {},
                "repairReasonCounts": {},
                "contactAdmission": {
                    "blocked_invalid": 0,
                    "blocked_repairable": 0,
                    "admitted_canonical": 0,
                },
                "sourceInvaliditySaturated": False,
            },
            "chapter_search_candidates": [],
            "chapter_search_decisions": [],
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
            "profile_directory_layout": self._profile_directory_layout,
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
        graph.add_conditional_edges("analyze_page", self._has_error, {"ok": "profile_directory_layout", "error": "finalize"})
        graph.add_conditional_edges("profile_directory_layout", self._has_error, {"ok": "compute_template_signature", "error": "finalize"})
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
        chapter_index_mode, chapter_index_mode_confidence, chapter_index_mode_reason = detect_chapter_index_mode(
            state["current_page_html"],
            analysis,
            classification,
            embedded,
            state["source"].metadata,
        )
        return {
            "page_analysis": analysis,
            "classification": classification,
            "embedded_data": embedded,
            "page_level_confidence": classification.confidence,
            "chapter_index_mode": chapter_index_mode,
            "chapter_index_mode_confidence": chapter_index_mode_confidence,
            "chapter_index_mode_reason": chapter_index_mode_reason,
        }

    def _profile_directory_layout(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        profile = tool_directory_layout_profiler(
            html=state["current_page_html"],
            page_url=state["current_page_url"],
        )
        classification = self._apply_directory_layout_profile(
            state["classification"],
            profile.as_dict(),
        )
        return {
            "directory_layout_profile": profile.as_dict(),
            "classification": classification,
            "page_level_confidence": classification.confidence,
        }

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

    def _apply_directory_layout_profile(
        self,
        classification: SourceClassification,
        profile: dict[str, Any] | None,
    ) -> SourceClassification:
        if not profile or str(profile.get("decision")) != "directory_layout_profiled":
            return classification
        metadata = profile.get("metadata") or {}
        layout_family = str(metadata.get("layoutFamily") or "unclassified")
        if layout_family == "unclassified":
            return classification
        recommended_strategy = str(metadata.get("recommendedStrategy") or classification.recommended_strategy or "repeated_block")
        possible_data_locations = list(metadata.get("possibleDataLocations") or classification.possible_data_locations or [])
        profile_confidence = float(profile.get("confidence") or 0.0)
        if classification.page_type == "static_directory" and classification.confidence >= profile_confidence:
            return classification
        return SourceClassification(
            page_type="static_directory",
            confidence=max(classification.confidence, min(profile_confidence, 0.95)),
            recommended_strategy=recommended_strategy,
            needs_follow_links=False,
            possible_data_locations=possible_data_locations,
            classified_by="heuristic",
        )

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
        chapter_stubs = []
        chapter_search_candidates: list[ChapterCandidate] = []
        chapter_search_decisions: list[ChapterSearchDecision] = list(state.get("chapter_search_decisions") or [])
        chapter_follow_pages: dict[str, list[tuple[str, str]]] = {}
        chapter_contact_hints: dict[str, dict[str, str]] = {}
        metrics = state["metrics"]
        review_items = list(state.get("review_items", []))
        chapter_search_metrics = dict(state.get("chapter_search_metrics") or {})
        chapter_validity_metrics = dict(state.get("chapter_validity_metrics") or {})
        chapter_search_started = time.perf_counter()
        if action.startswith("extract_") and strategy and self._page_can_emit_chapters(state):
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
        elif action.startswith("extract_") and strategy:
            review_items.append(
                ReviewItemCandidate(
                    item_type="adaptive_page_role_gate",
                    reason="Page role gated chapter extraction before normalization",
                    source_slug=state["source"].source_slug,
                    chapter_slug=None,
                    payload={
                        "source_url": state["current_page_url"],
                        "pageRole": state["page_analysis"].probable_page_role,
                        "selectedAction": action,
                        "chapterIntentSignals": self._chapter_intent_signal_count(state),
                    },
                )
            )

        if action.startswith("extract_") or action.startswith("expand_"):
            chapter_stubs = extract_chapter_stubs(
                registry=self._registry,
                html=state["current_page_html"],
                source_url=state["current_page_url"],
                mode=state.get("chapter_index_mode", "mixed"),
                embedded_data=state["embedded_data"],
                http_client=self._http,
                source_metadata=state["source"].metadata,
            )
            chapter_stubs, stub_decisions = self._filter_chapter_stubs_for_context(chapter_stubs, state)
            chapter_search_decisions.extend(stub_decisions)
            chapter_search_candidates = build_chapter_candidates(stubs=chapter_stubs, source_url=state["source"].list_url)
            chapter_search_metrics["sourceClass"] = self._chapter_search_source_class(state)
            chapter_search_metrics["candidatesExtracted"] = int(chapter_search_metrics.get("candidatesExtracted", 0) or 0) + len(chapter_search_candidates)
            chapter_validity_metrics = self._merge_candidate_validity_metrics(chapter_validity_metrics, chapter_search_candidates)
            chapter_follow_pages, follow_stats = follow_chapter_detail_or_outbound(
                stubs=chapter_stubs,
                source_url=state["source"].list_url,
                http_client=self._http,
                max_hops_per_stub=self._settings.crawler_navigation_max_hops_per_stub,
                max_pages_per_run=self._settings.crawler_navigation_max_pages_per_run,
                follow_external_chapter_sites=False,
                allow_institutional_follow=True,
            )
            metrics.pages_processed += int(follow_stats.get("fetched_pages", 0) or 0)
            chapter_search_metrics = self._merge_chapter_search_follow_stats(chapter_search_metrics, follow_stats)
            for target_decision in follow_stats.get("target_decisions", []) or []:
                chapter_search_decisions.append(
                    ChapterSearchDecision(
                        chapter_name="",
                        university_name=None,
                        source_class=str(target_decision.get("sourceClass") or "unknown"),
                        decision="follow" if bool(target_decision.get("followAllowed")) else "skip",
                        reason=str(target_decision.get("rejectionReason") or "") or None,
                        target_type=str(target_decision.get("targetType") or "unknown"),
                        provenance="chapter_target",
                        source_url=str(target_decision.get("url") or "") or None,
                    )
                )
            chapter_contact_hints = extract_contacts_from_chapter_site(chapter_stubs, chapter_follow_pages)
            extracted = self._merge_stub_contacts_into_records(
                extracted=extracted,
                stubs=chapter_stubs,
                contact_hints=chapter_contact_hints,
                state=state,
            )
            for stub in chapter_stubs[: self._settings.crawler_frontier_max_pages_per_template]:
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
                    parent_success_bonus=0.5 if chapter_contact_hints.get(self._stub_key(stub)) else 0.3,
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

        filtered_records, gated_reviews = self._filter_records_for_page_context(extracted, state)
        extracted = self._dedupe_extracted_records(filtered_records)
        review_items.extend(gated_reviews)
        metrics.records_seen += len(extracted)
        chapter_search_metrics["chapterSearchWallTimeMs"] = int((time.perf_counter() - chapter_search_started) * 1000)

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
            "chapter_stubs": chapter_stubs,
            "chapter_search_candidates": chapter_search_candidates,
            "chapter_search_decisions": chapter_search_decisions,
            "chapter_search_metrics": chapter_search_metrics,
            "chapter_validity_metrics": chapter_validity_metrics,
            "chapter_follow_pages": chapter_follow_pages,
            "chapter_contact_hints": chapter_contact_hints,
            "metrics": metrics,
            "valid_missing_count_current": valid_missing_count,
            "verified_website_count_current": verified_website_count,
            "budget_state": budget_state,
            "review_items": review_items,
        }

    def _expand_frontier(self, state: AdaptiveCrawlState) -> dict[str, Any]:
        current = state["current_frontier_item"]
        if current.depth >= self._settings.crawler_frontier_max_depth:
            return {"current_links": []}
        new_items = list(state.get("frontier_items", []))
        current_links: list[dict[str, Any]] = []
        generic_link_budget = self._generic_frontier_link_budget(state)
        for link in discover_frontier_links(state["current_page_html"], state["current_page_url"]):
            url = str(link["url"])
            if host_family(url) != host_family(state["source"].list_url):
                continue
            if not self._should_queue_frontier_link(state, link):
                continue
            score, components = score_frontier_item(url, anchor_text=str(link.get("anchor_text") or "") or None, depth=current.depth + 1, source_url=state["source"].list_url, page_analysis=state["page_analysis"], template_bonus=0.2, parent_success_bonus=0.4 if state.get("extracted_from_current") else 0.0)
            if score < self._settings.crawler_adaptive_min_score:
                continue
            new_items.append(FrontierItem(id=None, url=url, canonical_url=url, parent_url=current.url, depth=current.depth + 1, anchor_text=str(link.get("anchor_text") or "") or None, discovered_from="page_link", score_total=score, score_components=components))
            current_links.append({"url": url, "anchorText": link.get("anchor_text"), "score": score})
            if len(current_links) >= generic_link_budget:
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

        delayed_seed = self._delayed_reward_seed(state)
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
        provisional_created = 0
        canonical_created = 0
        repairable_blocked = 0
        invalid_blocked = 0
        inline_enriched = 0
        chapter_search_metrics = dict(state.get("chapter_search_metrics") or {})
        chapter_validity_metrics = {
            "invalidCount": 0,
            "repairableCount": 0,
            "provisionalCount": 0,
            "canonicalValidCount": 0,
            "invalidReasonCounts": {},
            "repairReasonCounts": {},
            "contactAdmission": {
                "blocked_invalid": 0,
                "blocked_repairable": 0,
                "admitted_canonical": 0,
            },
            "sourceInvaliditySaturated": False,
        }
        for record in extracted:
            source_class = self._classify_record_source(source, record.source_url)
            chapter_decision = self._chapter_search_decide_record(record=record, source_class=source_class)
            chapter_search_metrics = self._record_chapter_search_decision(chapter_search_metrics, chapter_decision)
            chapter_validity_metrics = self._record_chapter_validity_decision(chapter_validity_metrics, chapter_decision)
            if chapter_decision.decision == "reject":
                invalid_blocked += 1
                review_items.append(
                    ReviewItemCandidate(
                        item_type="chapter_search_rejected",
                        reason=chapter_decision.reason or "Chapter candidate rejected during chapter search",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "source_url": record.source_url,
                            "recordName": record.name,
                            "universityName": record.university_name,
                            "sourceClass": source_class,
                            "validityClass": chapter_decision.validity_class,
                            "targetType": chapter_decision.target_type,
                            "rejectionReason": chapter_decision.invalid_reason or chapter_decision.reason,
                        },
                    )
                )
                self._persist_record_evidence(
                    source,
                    run_id,
                    None,
                    record,
                    source_class=source_class,
                    evidence_status="review",
                    rejection_reason=chapter_decision.invalid_reason or chapter_decision.reason,
                    validity_class=chapter_decision.validity_class,
                    repair_reason=chapter_decision.repair_reason,
                )
                continue
            if chapter_decision.decision == "repair":
                repairable_blocked += 1
                review_items.append(
                    ReviewItemCandidate(
                        item_type="chapter_repair_candidate",
                        reason=chapter_decision.repair_reason or chapter_decision.reason or "Chapter candidate requires identity repair",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "source_url": record.source_url,
                            "recordName": record.name,
                            "universityName": record.university_name,
                            "sourceClass": source_class,
                            "validityClass": chapter_decision.validity_class,
                            "repairReason": chapter_decision.repair_reason or chapter_decision.reason,
                            "nextAction": chapter_decision.next_action,
                        },
                    )
                )
                self._persist_record_evidence(
                    source,
                    run_id,
                    None,
                    record,
                    source_class=source_class,
                    evidence_status="review",
                    rejection_reason=chapter_decision.repair_reason or chapter_decision.reason,
                    validity_class=chapter_decision.validity_class,
                    repair_reason=chapter_decision.repair_reason,
                )
                continue
            try:
                chapter, provenance = normalize_record(source, record, validity_class=chapter_decision.validity_class)
                if chapter_decision.decision == "provisional":
                    self._repository.upsert_provisional_chapter(
                        fraternity_id=source.fraternity_id,
                        source_id=source.id,
                        slug=chapter.slug,
                        name=chapter.name,
                        university_name=chapter.university_name,
                        city=chapter.city,
                        state=chapter.state,
                        country=chapter.country,
                        website_url=chapter.website_url,
                        instagram_url=chapter.instagram_url,
                        contact_email=chapter.contact_email,
                        evidence_payload={
                            "sourceUrl": record.source_url,
                            "sourceSnippet": record.source_snippet,
                            "sourceConfidence": record.source_confidence,
                            "sourceClass": source_class,
                            "validityClass": chapter_decision.validity_class,
                            "repairReason": chapter_decision.repair_reason,
                        },
                    )
                    provisional_created += 1
                    self._persist_record_evidence(
                        source,
                        run_id,
                        chapter.slug,
                        record,
                        source_class=source_class,
                        evidence_status="observed",
                        validity_class=chapter_decision.validity_class,
                        repair_reason=chapter_decision.repair_reason,
                    )
                    continue

                chapter_id = self._repository.upsert_chapter(source, chapter)
                self._repository.insert_provenance(chapter_id, source.id, run_id, provenance)
                self._persist_record_evidence(
                    source,
                    run_id,
                    chapter.slug,
                    record,
                    chapter_id=chapter_id,
                    source_class=source_class,
                    evidence_status="accepted",
                    validity_class=chapter_decision.validity_class,
                )
                metrics.records_upserted += 1
                canonical_created += 1
                pending_fields = list(chapter.missing_optional_fields)
                if pending_fields:
                    inline_summary = self._run_inline_contact_resolution(
                        chapter_id=chapter_id,
                        chapter=chapter,
                        source=source,
                        crawl_run_id=run_id,
                    )
                    inline_enriched += int(inline_summary["resolved_count"])
                    for candidate in inline_summary["review_items"]:
                        self._repository.create_review_item(source.id, run_id, candidate, chapter_id=chapter_id)
                        metrics.review_items_created += 1
                    pending_fields = inline_summary["pending_fields"]
                persisted.append({"chapter": chapter, "chapter_id": chapter_id, "pending_fields": pending_fields})
            except AmbiguousRecordError as exc:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="adaptive_ambiguous_record",
                        reason=str(exc),
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "source_url": record.source_url,
                            "snippet": record.source_snippet,
                            "pageRole": state["page_analysis"].probable_page_role if state.get("page_analysis") else None,
                            "selectedAction": state.get("selected_action"),
                            "sourceClass": source_class,
                            "validityClass": chapter_decision.validity_class,
                        },
                    )
                )
                self._persist_record_evidence(
                    source,
                    run_id,
                    None,
                    record,
                    source_class=source_class,
                    evidence_status="review",
                    rejection_reason=str(exc),
                    validity_class=chapter_decision.validity_class,
                    repair_reason=chapter_decision.repair_reason,
                )
        for review_item in review_items:
            self._repository.create_review_item(source.id, run_id, review_item)
            metrics.review_items_created += 1
        for bundle in persisted:
            pending_fields = list(bundle.get("pending_fields") or [])
            if not pending_fields:
                continue
            chapter = bundle["chapter"]
            metrics.field_jobs_created += self._repository.create_field_jobs(
                chapter_id=bundle["chapter_id"],
                crawl_run_id=run_id,
                chapter_slug=chapter.slug,
                source_slug=source.source_slug,
                missing_fields=pending_fields,
            )
        extraction_metadata = {
            "strategy_used": state.get("selected_action"),
            "runtime_mode": state.get("runtime_mode", self._runtime_mode),
            "page_level_confidence": state.get("page_level_confidence"),
            "template_signature": state.get("template_signature"),
            "template_signature_raw": state.get("template_signature_raw"),
            "stop_reason": state.get("stop_reason"),
            "navigation_stats": state.get("navigation_stats", {}),
            "policy_snapshot": self._policy.snapshot(),
            "chapter_index_mode": state.get("chapter_index_mode"),
            "chapter_index_reason": state.get("chapter_index_mode_reason"),
            "provisional_created": provisional_created,
            "canonical_created": canonical_created,
            "inline_enriched": inline_enriched,
            "chapter_search": {
                **chapter_search_metrics,
                "canonicalChaptersCreated": canonical_created,
                "provisionalChaptersCreated": provisional_created,
                "coverageState": self._chapter_search_coverage_state(
                    chapter_search_metrics=chapter_search_metrics,
                    canonical_created=canonical_created,
                    provisional_created=provisional_created,
                ),
            },
            "chapter_validity": {
                **chapter_validity_metrics,
                "canonicalValidCount": canonical_created,
                "provisionalCount": provisional_created,
                "sourceInvaliditySaturated": self._chapter_invalidity_saturated(
                    invalid_count=int(chapter_validity_metrics.get("invalidCount", 0) or 0),
                    repairable_count=int(chapter_validity_metrics.get("repairableCount", 0) or 0),
                    canonical_count=canonical_created,
                    provisional_count=provisional_created,
                ),
            },
        }
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
        if extraction_metadata["chapter_validity"].get("sourceInvaliditySaturated"):
            status = "partial" if canonical_created == 0 else status
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

    def _page_can_emit_chapters(self, state: AdaptiveCrawlState) -> bool:
        role = str(state["page_analysis"].probable_page_role or "").strip().lower()
        if role in {"directory", "index"}:
            return True
        if role == "profile" and self._chapter_intent_signal_count(state) >= 2:
            return True
        return self._chapter_intent_signal_count(state) >= 3

    def _chapter_intent_signal_count(self, state: AdaptiveCrawlState) -> int:
        tokens = " ".join(
            part
            for part in (
                state["current_page_url"],
                state["page_analysis"].title or "",
                " ".join(state["page_analysis"].headings or []),
                state["page_analysis"].text_sample or "",
            )
            if part
        ).lower()
        markers = ("chapter", "chapters", "fraternity", "greek", "organization", "student org", "directory", "find a chapter")
        return sum(1 for marker in markers if marker in tokens)

    def _filter_records_for_page_context(
        self,
        extracted: list[ExtractedChapter],
        state: AdaptiveCrawlState,
    ) -> tuple[list[ExtractedChapter], list[ReviewItemCandidate]]:
        if not extracted:
            return [], []
        filtered: list[ExtractedChapter] = []
        reviews: list[ReviewItemCandidate] = []
        role = str(state["page_analysis"].probable_page_role or "").strip().lower()
        allowed = self._page_can_emit_chapters(state)
        for record in extracted:
            name = (record.name or "").strip()
            school = (record.university_name or "").strip()
            if not allowed:
                reviews.append(
                    ReviewItemCandidate(
                        item_type="adaptive_page_role_gate",
                        reason="navigation_noise",
                        source_slug=state["source"].source_slug,
                        chapter_slug=None,
                        payload={
                            "source_url": record.source_url,
                            "pageRole": role,
                            "recordName": name,
                            "universityName": school,
                        },
                    )
                )
                continue
            if role in {"search", "navigation"} and not school:
                reviews.append(
                    ReviewItemCandidate(
                        item_type="adaptive_page_role_gate",
                        reason="missing_institution_signal",
                        source_slug=state["source"].source_slug,
                        chapter_slug=None,
                        payload={"source_url": record.source_url, "pageRole": role, "recordName": name},
                    )
                )
                continue
            filtered.append(record)
        return filtered, reviews

    def _filter_chapter_stubs_for_context(self, stubs: list, state: AdaptiveCrawlState) -> tuple[list, list[ChapterSearchDecision]]:
        if not stubs:
            return [], []
        filtered = []
        decisions: list[ChapterSearchDecision] = []
        for stub in stubs:
            target_url = stub.detail_url or stub.outbound_chapter_url_candidate or ""
            if self._is_irrelevant_frontier_target(target_url, stub.chapter_name):
                decisions.append(
                    ChapterSearchDecision(
                        chapter_name=stub.chapter_name,
                        university_name=stub.university_name,
                        source_class=self._chapter_search_source_class(state),
                        decision="reject",
                        reason="navigation_noise",
                        provenance=stub.provenance,
                        source_url=target_url or None,
                    )
                )
                continue
            if stub.provenance == "anchor_list" and not stub.university_name and self._chapter_intent_score(target_url, stub.chapter_name) < 1:
                decisions.append(
                    ChapterSearchDecision(
                        chapter_name=stub.chapter_name,
                        university_name=stub.university_name,
                        source_class=self._chapter_search_source_class(state),
                        decision="reject",
                        reason="missing_institution_signal",
                        provenance=stub.provenance,
                        source_url=target_url or None,
                    )
                )
                continue
            filtered.append(stub)
        return filtered, decisions

    def _generic_frontier_link_budget(self, state: AdaptiveCrawlState) -> int:
        default_budget = int(self._settings.crawler_frontier_max_pages_per_template)
        if state.get("chapter_stubs") or state.get("extracted_from_current"):
            return max(2, min(default_budget, 3))
        return default_budget

    def _should_queue_frontier_link(self, state: AdaptiveCrawlState, link: dict[str, Any]) -> bool:
        url = str(link.get("url") or "")
        anchor_text = str(link.get("anchor_text") or "")
        current = state["current_frontier_item"]
        if not url:
            return False
        if self._is_irrelevant_frontier_target(url, anchor_text):
            return False
        if state.get("selected_action") == "expand_same_section_links":
            left = "/".join(part for part in current.canonical_url.split("/")[3:5] if part)
            right = "/".join(part for part in url.split("/")[3:5] if part)
            if left and right and left != right:
                return False
        intent_score = self._chapter_intent_score(url, anchor_text)
        if state.get("chapter_stubs") or state.get("extracted_from_current"):
            return intent_score >= 1
        role = str(state["page_analysis"].probable_page_role or "").strip().lower()
        if role in {"directory", "search", "index"}:
            return intent_score >= 1
        return True

    def _is_irrelevant_frontier_target(self, url: str, anchor_text: str | None = None) -> bool:
        lowered = " ".join(part for part in (url, anchor_text or "") if part).lower()
        blocked_markers = (
            "/start-a-chapter",
            "start a chapter",
            "/careers",
            "/career",
            "/about-us",
            "/about/",
            "/history",
            "/notable",
            "/news",
            "/blog",
            "/events",
            "/event",
            "/foundation",
            "/donate",
            "/scholarship",
            "/alumni",
            "/leadership",
            "/staff",
            "/privacy",
            "/terms",
            "/contact-us",
        )
        return any(marker in lowered for marker in blocked_markers)

    def _chapter_intent_score(self, url: str, anchor_text: str | None = None) -> int:
        lowered = " ".join(part for part in (url, anchor_text or "") if part).lower()
        strong_markers = ("chapter", "chapters", "directory", "find-a-chapter", "find a chapter", "chapter-roll", "collegiate")
        weak_markers = ("campus", "university", "college", "undergraduate", "locator", "map")
        score = sum(1 for marker in strong_markers if marker in lowered)
        if score:
            return score
        return 1 if any(marker in lowered for marker in weak_markers) else 0

    def _merge_stub_contacts_into_records(
        self,
        *,
        extracted: list[ExtractedChapter],
        stubs: list,
        contact_hints: dict[str, dict[str, str]],
        state: AdaptiveCrawlState,
    ) -> list[ExtractedChapter]:
        by_key: dict[tuple[str, str], ExtractedChapter] = {}
        for record in extracted:
            key = ((record.name or "").strip().lower(), (record.university_name or "").strip().lower())
            by_key[key] = record

        merged = list(extracted)
        for stub in stubs:
            key = self._stub_key(stub)
            hint = contact_hints.get(key, {})
            record_key = ((stub.chapter_name or "").strip().lower(), (stub.university_name or "").strip().lower())
            target = by_key.get(record_key)
            if target is None:
                outbound = stub.outbound_chapter_url_candidate or stub.detail_url
                if not hint and not outbound:
                    continue
                target = ExtractedChapter(
                    name=stub.chapter_name,
                    university_name=stub.university_name,
                    website_url=hint.get("website_url"),
                    instagram_url=hint.get("instagram_url"),
                    contact_email=hint.get("email"),
                    source_url=outbound or state["current_page_url"],
                    source_snippet=stub.provenance,
                    source_confidence=max(0.7, min(float(stub.confidence or 0.0), 0.92)),
                )
                merged.append(target)
                by_key[record_key] = target
            if hint.get("website_url") and not target.website_url:
                target.website_url = hint["website_url"]
            if hint.get("instagram_url") and not target.instagram_url:
                target.instagram_url = hint["instagram_url"]
            if hint.get("email") and not target.contact_email:
                target.contact_email = hint["email"]
            if not target.source_url:
                target.source_url = stub.detail_url or stub.outbound_chapter_url_candidate or state["current_page_url"]
        return merged

    def _run_inline_contact_resolution(
        self,
        *,
        chapter_id: str,
        chapter,
        source,
        crawl_run_id: int,
    ) -> dict[str, Any]:
        pending_fields = list(chapter.missing_optional_fields)
        review_items: list[ReviewItemCandidate] = []
        resolved_count = 0
        if self._inline_enrichment_engine is None or not pending_fields:
            return {"pending_fields": pending_fields, "review_items": review_items, "resolved_count": resolved_count}

        current_values = {
            "website_url": chapter.website_url,
            "instagram_url": chapter.instagram_url,
            "contact_email": chapter.contact_email,
        }
        current_field_states = dict(chapter.field_states or {})
        for field_name in (FIELD_JOB_FIND_WEBSITE, FIELD_JOB_FIND_EMAIL, FIELD_JOB_FIND_INSTAGRAM):
            if field_name not in pending_fields:
                continue
            job = FieldJob(
                id=f"inline-{crawl_run_id}-{chapter.slug}-{field_name}",
                chapter_id=chapter_id,
                chapter_slug=chapter.slug,
                chapter_name=chapter.name,
                field_name=field_name,
                payload={"mode": "inline_v3"},
                attempts=0,
                max_attempts=1,
                claim_token="inline",
                source_base_url=source.base_url,
                website_url=current_values["website_url"],
                instagram_url=current_values["instagram_url"],
                contact_email=current_values["contact_email"],
                fraternity_slug=source.fraternity_slug,
                source_id=source.id,
                source_slug=source.source_slug,
                university_name=chapter.university_name,
                crawl_run_id=crawl_run_id,
                field_states=current_field_states,
            )
            try:
                result = self._inline_enrichment_engine.process_claimed_job(job)
            except RetryableJobError:
                continue
            except Exception as exc:  # pragma: no cover - guardrail path
                review_items.append(
                    ReviewItemCandidate(
                        item_type="inline_enrichment_failure",
                        reason=f"Inline enrichment failed for {field_name}",
                        source_slug=source.source_slug,
                        chapter_slug=chapter.slug,
                        payload={"error": str(exc), "fieldName": field_name},
                    )
                )
                continue

            if result.review_item is not None:
                review_items.append(result.review_item)

            if result.chapter_updates or result.field_state_updates or result.provenance_records:
                self._repository.apply_inline_enrichment_result(
                    chapter_id=chapter_id,
                    chapter_slug=chapter.slug,
                    fraternity_slug=source.fraternity_slug,
                    source_slug=source.source_slug,
                    source_id=source.id,
                    crawl_run_id=crawl_run_id,
                    chapter_updates=result.chapter_updates,
                    completed_payload=result.completed_payload,
                    field_state_updates=result.field_state_updates,
                    provenance_records=result.provenance_records,
                )
                current_values["website_url"] = result.chapter_updates.get("website_url", current_values["website_url"])
                current_values["instagram_url"] = result.chapter_updates.get("instagram_url", current_values["instagram_url"])
                current_values["contact_email"] = result.chapter_updates.get("contact_email", current_values["contact_email"])
                current_field_states.update(result.field_state_updates or {})

            target_key = {
                FIELD_JOB_FIND_WEBSITE: "website_url",
                FIELD_JOB_FIND_EMAIL: "contact_email",
                FIELD_JOB_FIND_INSTAGRAM: "instagram_url",
            }[field_name]
            if target_key in result.chapter_updates:
                pending_fields.remove(field_name)
                resolved_count += 1
            elif result.review_item is not None and str(result.completed_payload.get("status") or "") == "review_required":
                pending_fields.remove(field_name)
        return {"pending_fields": pending_fields, "review_items": review_items, "resolved_count": resolved_count}

    def _persist_record_evidence(
        self,
        source,
        crawl_run_id: int,
        chapter_slug: str | None,
        record: ExtractedChapter,
        *,
        chapter_id: str | None = None,
        source_class: str,
        evidence_status: str,
        rejection_reason: str | None = None,
        validity_class: str | None = None,
        repair_reason: str | None = None,
    ) -> None:
        trust_tier = "strong_official" if source_class in {"national", "institutional"} and record.source_confidence >= 0.9 else "high" if record.source_confidence >= 0.85 else "medium"
        for field_name, field_value in (
            ("name", record.name),
            ("university_name", record.university_name),
            ("website_url", record.website_url),
            ("instagram_url", record.instagram_url),
            ("contact_email", record.contact_email),
        ):
            if not field_value:
                continue
            self._repository.insert_chapter_evidence(
                ChapterEvidenceRecord(
                    chapter_id=chapter_id,
                    chapter_slug=chapter_slug or "",
                    fraternity_slug=source.fraternity_slug,
                    source_slug=source.source_slug,
                    crawl_run_id=crawl_run_id,
                    field_name=field_name,
                    candidate_value=field_value,
                    confidence=record.source_confidence,
                    trust_tier=trust_tier,
                    evidence_status=evidence_status,
                    source_url=record.source_url,
                    source_snippet=record.source_snippet,
                    metadata={
                        "sourceClass": source_class,
                        "rejectionReason": rejection_reason,
                        "validityClass": validity_class,
                        "repairReason": repair_reason,
                        "runtimeMode": self._runtime_mode,
                        "chapterSearch": True,
                    },
                )
            )

    def _classify_record_source(self, source, record_url: str | None) -> str:
        host = host_family(record_url or "")
        source_host = host_family(source.list_url)
        if host and host == source_host:
            return "national"
        if host.endswith(".edu") or ".edu." in host:
            return "institutional"
        return "wider_web"

    def _stub_key(self, stub) -> str:
        return f"{(stub.chapter_name or '').strip().lower()}::{(stub.university_name or '').strip().lower()}"

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

    def _delayed_reward_seed(self, state: AdaptiveCrawlState) -> float:
        extracted_current = list(state.get("extracted_from_current", []) or [])
        verified_website_count = float(state.get("verified_website_count_current", 0) or 0)
        contact_count = float(sum(1 for record in extracted_current if record.contact_email or record.instagram_url))
        chapter_yield = min(float(len(extracted_current)), 8.0) * 0.35
        valid_missing = min(float(state.get("valid_missing_count_current", 0) or 0), 4.0) * 0.2
        return round((verified_website_count * 1.2) + contact_count + chapter_yield + valid_missing, 4)

    def _chapter_search_source_class(self, state: AdaptiveCrawlState) -> str:
        page_url = str(state.get("current_page_url") or state["source"].list_url)
        host = host_family(page_url)
        source_host = host_family(state["source"].list_url)
        if host == source_host:
            if state.get("chapter_index_mode") == "map_or_api_locator" or state["classification"].page_type == "locator_map":
                return "locator_map"
            return "html_directory"
        if host.endswith(".edu") or ".edu." in host:
            return "institutional_directory"
        return "mixed_directory"

    def _chapter_search_decide_record(self, *, record: ExtractedChapter, source_class: str) -> ChapterSearchDecision:
        target = classify_chapter_target(source_url=record.source_url or "", candidate_url=record.website_url or record.source_url)
        validity = classify_chapter_validity(
            record,
            source_class=source_class,
            target_type=target.target_type,
        )
        if len((record.name or "").strip()) > 120 or len((record.university_name or "").strip()) > 160:
            return ChapterSearchDecision(
                chapter_name=record.name,
                university_name=record.university_name,
                source_class=source_class,
                decision="reject",
                reason="overlong_record_blocked",
                validity_class="invalid_non_chapter",
                target_type=target.target_type,
                source_url=record.source_url,
                invalid_reason="overlong_record_blocked",
                next_action="quarantine_invalid_entities",
            )
        return self._chapter_search_decision_from_validity(validity)

    def _record_chapter_search_decision(
        self,
        metrics: dict[str, Any],
        decision: ChapterSearchDecision,
    ) -> dict[str, Any]:
        next_metrics = dict(metrics or {})
        reason_counts = dict(next_metrics.get("rejectionReasonCounts") or {})
        if decision.decision == "reject":
            next_metrics["candidatesRejected"] = int(next_metrics.get("candidatesRejected", 0) or 0) + 1
            if decision.invalid_reason or decision.reason:
                key = decision.invalid_reason or decision.reason
                reason_counts[key] = int(reason_counts.get(key, 0) or 0) + 1
        next_metrics["rejectionReasonCounts"] = reason_counts
        return next_metrics

    def _chapter_search_decision_from_validity(self, validity: ChapterValidityDecision) -> ChapterSearchDecision:
        decision_map = {
            "canonical_valid": "canonical",
            "repairable_candidate": "repair",
            "provisional_candidate": "provisional",
            "invalid_non_chapter": "reject",
        }
        return ChapterSearchDecision(
            chapter_name=validity.chapter_name,
            university_name=validity.university_name,
            source_class=validity.source_class,
            decision=decision_map.get(validity.validity_class, "reject"),
            validity_class=validity.validity_class,
            reason=validity.invalid_reason or validity.repair_reason,
            target_type=validity.target_type,
            provenance=validity.provenance,
            source_url=validity.source_url,
            invalid_reason=validity.invalid_reason,
            repair_reason=validity.repair_reason,
            next_action=validity.next_action,
        )

    def _merge_candidate_validity_metrics(
        self,
        metrics: dict[str, Any],
        candidates: list[ChapterCandidate],
    ) -> dict[str, Any]:
        next_metrics = dict(metrics or {})
        invalid_reason_counts = dict(next_metrics.get("invalidReasonCounts") or {})
        repair_reason_counts = dict(next_metrics.get("repairReasonCounts") or {})
        for candidate in candidates:
            validity_class = candidate.validity_class or "repairable_candidate"
            if validity_class == "canonical_valid":
                next_metrics["canonicalValidCount"] = int(next_metrics.get("canonicalValidCount", 0) or 0) + 1
            elif validity_class == "provisional_candidate":
                next_metrics["provisionalCount"] = int(next_metrics.get("provisionalCount", 0) or 0) + 1
            elif validity_class == "repairable_candidate":
                next_metrics["repairableCount"] = int(next_metrics.get("repairableCount", 0) or 0) + 1
                if candidate.repair_reason:
                    repair_reason_counts[candidate.repair_reason] = int(repair_reason_counts.get(candidate.repair_reason, 0) or 0) + 1
            else:
                next_metrics["invalidCount"] = int(next_metrics.get("invalidCount", 0) or 0) + 1
                if candidate.invalid_reason:
                    invalid_reason_counts[candidate.invalid_reason] = int(invalid_reason_counts.get(candidate.invalid_reason, 0) or 0) + 1
        next_metrics["invalidReasonCounts"] = invalid_reason_counts
        next_metrics["repairReasonCounts"] = repair_reason_counts
        return next_metrics

    def _record_chapter_validity_decision(
        self,
        metrics: dict[str, Any],
        decision: ChapterSearchDecision,
    ) -> dict[str, Any]:
        next_metrics = dict(metrics or {})
        invalid_reason_counts = dict(next_metrics.get("invalidReasonCounts") or {})
        repair_reason_counts = dict(next_metrics.get("repairReasonCounts") or {})
        validity_class = decision.validity_class or "repairable_candidate"
        if validity_class == "canonical_valid":
            next_metrics["canonicalValidCount"] = int(next_metrics.get("canonicalValidCount", 0) or 0) + 1
            contact_admission = dict(next_metrics.get("contactAdmission") or {})
            contact_admission["admitted_canonical"] = int(contact_admission.get("admitted_canonical", 0) or 0) + 1
            next_metrics["contactAdmission"] = contact_admission
        elif validity_class == "provisional_candidate":
            next_metrics["provisionalCount"] = int(next_metrics.get("provisionalCount", 0) or 0) + 1
        elif validity_class == "repairable_candidate":
            next_metrics["repairableCount"] = int(next_metrics.get("repairableCount", 0) or 0) + 1
            if decision.repair_reason:
                repair_reason_counts[decision.repair_reason] = int(repair_reason_counts.get(decision.repair_reason, 0) or 0) + 1
            contact_admission = dict(next_metrics.get("contactAdmission") or {})
            contact_admission["blocked_repairable"] = int(contact_admission.get("blocked_repairable", 0) or 0) + 1
            next_metrics["contactAdmission"] = contact_admission
        else:
            next_metrics["invalidCount"] = int(next_metrics.get("invalidCount", 0) or 0) + 1
            if decision.invalid_reason or decision.reason:
                key = decision.invalid_reason or decision.reason
                invalid_reason_counts[key] = int(invalid_reason_counts.get(key, 0) or 0) + 1
            contact_admission = dict(next_metrics.get("contactAdmission") or {})
            contact_admission["blocked_invalid"] = int(contact_admission.get("blocked_invalid", 0) or 0) + 1
            next_metrics["contactAdmission"] = contact_admission
        next_metrics["invalidReasonCounts"] = invalid_reason_counts
        next_metrics["repairReasonCounts"] = repair_reason_counts
        return next_metrics

    def _chapter_invalidity_saturated(
        self,
        *,
        invalid_count: int,
        repairable_count: int,
        canonical_count: int,
        provisional_count: int,
    ) -> bool:
        total = invalid_count + repairable_count + canonical_count + provisional_count
        if total < 6:
            return False
        noisy = invalid_count + repairable_count
        return canonical_count == 0 and noisy / max(total, 1) >= 0.6

    def _merge_chapter_search_follow_stats(
        self,
        metrics: dict[str, Any],
        follow_stats: dict[str, Any],
    ) -> dict[str, Any]:
        next_metrics = dict(metrics or {})
        followed = dict(follow_stats.get("followed_by_target_type") or {})
        skipped = dict(follow_stats.get("skipped_by_target_type") or {})
        next_metrics["nationalTargetsFollowed"] = int(next_metrics.get("nationalTargetsFollowed", 0) or 0) + int(followed.get("national_detail", 0) or 0) + int(followed.get("national_listing", 0) or 0)
        next_metrics["institutionalTargetsFollowed"] = int(next_metrics.get("institutionalTargetsFollowed", 0) or 0) + int(followed.get("institutional_page", 0) or 0)
        next_metrics["broaderWebTargetsFollowed"] = int(next_metrics.get("broaderWebTargetsFollowed", 0) or 0) + int(followed.get("broader_web_candidate", 0) or 0)
        next_metrics["chapterOwnedTargetsSkipped"] = int(next_metrics.get("chapterOwnedTargetsSkipped", 0) or 0) + int(skipped.get("chapter_owned_site", 0) or 0)
        return next_metrics

    def _chapter_search_coverage_state(
        self,
        *,
        chapter_search_metrics: dict[str, Any],
        canonical_created: int,
        provisional_created: int,
    ) -> str:
        if canonical_created > 0:
            return "canonical_ready"
        if provisional_created > 0:
            return "provisional_only"
        if int((chapter_search_metrics or {}).get("candidatesExtracted", 0) or 0) > 0:
            return "identity_incomplete"
        return "empty"


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
