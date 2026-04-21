from __future__ import annotations

import json
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, TypedDict
from urllib.parse import urlparse

from langgraph.graph import END, StateGraph

from fratfinder_crawler.db import CrawlerRepository, RequestGraphRepository
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import FraternityCrawlRequestRecord, NormalizedChapter


class RequestGraphState(TypedDict, total=False):
    request_id: str
    graph_run_id: int
    worker_id: str
    runtime_mode: str
    request: FraternityCrawlRequestRecord
    progress: dict[str, Any]
    source_quality: dict[str, Any]
    recovery_reason: str | None
    crawl_stage_started_at: str | None
    crawl_run_baseline_id: int | None
    crawl_runtime_mode_used: str | None
    crawl_fallback_used: bool
    crawl_retry_count: int
    retry_crawl: bool
    crawl_run: dict[str, Any] | None
    field_snapshot: list[dict[str, Any]]
    effective_config: dict[str, Any]
    cycle_state: dict[str, int]
    cycle: int
    preflight_snapshot: dict[str, Any] | None
    latest_field_job_result: dict[str, Any] | None
    skip_enrichment: bool
    continue_enrichment: bool
    graph_status: str
    terminal_reason: str | None
    request_paused: bool
    error: str | None


class RequestSupervisorGraphRuntime:
    def __init__(
        self,
        *,
        request_repository: RequestGraphRepository,
        crawler_repository: CrawlerRepository,
        worker_id: str,
        runtime_mode: str,
        crawl_runtime_mode: str,
        field_job_runtime_mode: str,
        field_job_graph_durability: str,
        free_recovery_attempts: int,
        discover_source: Callable[[str], dict[str, Any]],
        run_crawl: Callable[..., dict[str, Any]],
        process_field_jobs: Callable[..., dict[str, Any]],
        search_preflight: Callable[[], dict[str, Any]],
        logger,
    ):
        self._request_repository = request_repository
        self._crawler_repository = crawler_repository
        self._worker_id = worker_id
        self._runtime_mode = runtime_mode
        self._crawl_runtime_mode = crawl_runtime_mode
        self._field_job_runtime_mode = field_job_runtime_mode
        self._field_job_graph_durability = field_job_graph_durability
        self._free_recovery_attempts = max(1, free_recovery_attempts)
        self._discover_source = discover_source
        self._run_crawl = run_crawl
        self._process_field_jobs = process_field_jobs
        self._search_preflight = search_preflight
        self._logger = logger
        self._graph = self._build_graph()

    def run(self, request_id: str) -> dict[str, Any]:
        run_id = self._request_repository.start_request_graph_run(
            request_id=request_id,
            worker_id=self._worker_id,
            runtime_mode=self._runtime_mode,
            metadata={
                "crawlRuntimeMode": self._crawl_runtime_mode,
                "fieldJobRuntimeMode": self._field_job_runtime_mode,
                "fieldJobGraphDurability": self._field_job_graph_durability,
            },
        )
        initial_state: RequestGraphState = {
            "request_id": request_id,
            "graph_run_id": run_id,
            "worker_id": self._worker_id,
            "runtime_mode": self._runtime_mode,
            "cycle": 0,
            "graph_status": "running",
            "request_paused": False,
            "skip_enrichment": False,
            "continue_enrichment": False,
            "crawl_fallback_used": False,
            "crawl_retry_count": 0,
            "retry_crawl": False,
        }
        final_state = self._graph.invoke(initial_state)
        progress = final_state.get("progress") or {}
        records_seen = (progress.get("crawlRun") or {}).get("recordsSeen", 0)
        queue_remaining = _remaining_actionable_queue(progress)
        cycles_completed = (final_state.get("cycle_state") or {}).get("cyclesCompleted", 0)
        summary = {
            "requestId": request_id,
            "runtimeMode": self._runtime_mode,
            "terminalReason": final_state.get("terminal_reason"),
            "status": final_state.get("graph_status", "failed"),
            "crawlRuntimeModeUsed": final_state.get("crawl_runtime_mode_used"),
            "businessStatus": "progressed" if int(records_seen or 0) > 0 or int(cycles_completed or 0) > 0 or int(queue_remaining or 0) == 0 else "no_business_progress",
            "crawlRunId": progress.get("crawlRun", {}).get("id"),
            "recordsSeen": records_seen,
            "queueRemaining": queue_remaining,
            "cyclesCompleted": cycles_completed,
        }
        self._request_repository.finish_request_graph_run(
            run_id,
            status=final_state.get("graph_status", "failed"),
            summary=summary,
            error_message=final_state.get("error"),
            active_node=final_state.get("terminal_reason"),
        )
        return summary

    def _build_graph(self):
        graph = StateGraph(RequestGraphState)
        graph.add_node("load_request_context", self._instrument("load_request_context", self._load_request_context, phase="supervisor"))
        graph.add_node("recover_source", self._instrument("recover_source", self._recover_source, phase="recovery"))
        graph.add_node("start_crawl", self._instrument("start_crawl", self._start_crawl, phase="crawl"))
        graph.add_node("sync_crawl_progress", self._instrument("sync_crawl_progress", self._sync_crawl_progress, phase="crawl"))
        graph.add_node("purge_inactive_schools", self._instrument("purge_inactive_schools", self._purge_inactive_schools, phase="validation"))
        graph.add_node("enter_enrichment", self._instrument("enter_enrichment", self._enter_enrichment, phase="enrichment"))
        graph.add_node("run_enrichment_cycle", self._instrument("run_enrichment_cycle", self._run_enrichment_cycle, phase="enrichment"))
        graph.add_node("sync_enrichment_progress", self._instrument("sync_enrichment_progress", self._sync_enrichment_progress, phase="enrichment"))
        graph.add_node(
            "evaluate_provisional_promotions",
            self._instrument("evaluate_provisional_promotions", self._evaluate_provisional_promotions, phase="promotion"),
        )
        graph.add_node("finalize", self._instrument("finalize", self._finalize, phase="finalize"))

        graph.set_entry_point("load_request_context")
        graph.add_edge("load_request_context", "recover_source")
        graph.add_conditional_edges("recover_source", self._after_recovery, {"continue": "start_crawl", "finalize": "finalize"})
        graph.add_edge("start_crawl", "sync_crawl_progress")
        graph.add_conditional_edges(
            "sync_crawl_progress",
            self._after_crawl_sync,
            {"recover": "recover_source", "retry": "start_crawl", "continue": "purge_inactive_schools", "finalize": "finalize"},
        )
        graph.add_edge("purge_inactive_schools", "enter_enrichment")
        graph.add_conditional_edges(
            "enter_enrichment",
            self._after_enter_enrichment,
            {"run": "run_enrichment_cycle", "promote": "evaluate_provisional_promotions", "finalize": "finalize"},
        )
        graph.add_edge("run_enrichment_cycle", "sync_enrichment_progress")
        graph.add_conditional_edges(
            "sync_enrichment_progress",
            self._after_enrichment_sync,
            {"continue": "run_enrichment_cycle", "promote": "evaluate_provisional_promotions", "finalize": "finalize"},
        )
        graph.add_edge("evaluate_provisional_promotions", "finalize")
        graph.add_edge("finalize", END)
        return graph.compile()

    def _instrument(self, node_name: str, fn: Callable[[RequestGraphState], dict[str, Any]], *, phase: str):
        def wrapped(state: RequestGraphState) -> dict[str, Any]:
            started = time.perf_counter()
            updates: dict[str, Any]
            status = "ok"
            try:
                updates = fn(state)
            except Exception as exc:  # pragma: no cover
                status = "error"
                updates = {
                    "error": str(exc),
                    "graph_status": "failed",
                    "terminal_reason": "node_failure",
                }
                log_event(
                    self._logger,
                    "request_graph_node_failed",
                    level=40,
                    request_id=state.get("request_id"),
                    node=node_name,
                    error=str(exc),
                )

            latency_ms = int((time.perf_counter() - started) * 1000)
            run_id = state.get("graph_run_id")
            request_id = state.get("request_id")
            if run_id is not None and request_id is not None:
                request_record = updates.get("request") or state.get("request")
                diagnostics = {
                    "terminalReason": updates.get("terminal_reason"),
                    "error": updates.get("error"),
                    "activeStage": request_record.stage if isinstance(request_record, FraternityCrawlRequestRecord) else None,
                }
                metrics_delta = {
                    "cyclesCompleted": int((updates.get("cycle_state") or state.get("cycle_state") or {}).get("cyclesCompleted", 0)),
                    "recordsSeen": int((updates.get("progress") or state.get("progress") or {}).get("crawlRun", {}).get("recordsSeen", 0)),
                }
                self._request_repository.append_request_graph_event(
                    run_id=run_id,
                    request_id=request_id,
                    node_name=node_name,
                    phase=phase,
                    status=status,
                    latency_ms=latency_ms,
                    metrics_delta=metrics_delta,
                    diagnostics=_json_safe(diagnostics),
                )
                self._request_repository.upsert_request_graph_checkpoint(
                    run_id=run_id,
                    request_id=request_id,
                    node_name=node_name,
                    state=_json_safe({**state, **updates}),
                )
                self._request_repository.touch_request_graph_run(
                    run_id,
                    active_node=node_name,
                    summary={
                        "terminalReason": updates.get("terminal_reason"),
                        "recordsSeen": int((updates.get("progress") or state.get("progress") or {}).get("crawlRun", {}).get("recordsSeen", 0)),
                        "cyclesCompleted": int((updates.get("cycle_state") or state.get("cycle_state") or {}).get("cyclesCompleted", 0)),
                    },
                )
            return updates

        return wrapped

    def _after_recovery(self, state: RequestGraphState) -> str:
        if state.get("error") or state.get("request_paused") or state.get("terminal_reason"):
            return "finalize"
        return "continue"

    def _after_crawl_sync(self, state: RequestGraphState) -> str:
        if state.get("error"):
            return "finalize"
        if state.get("retry_crawl"):
            return "retry"
        if state.get("recovery_reason"):
            return "recover"
        if state.get("terminal_reason"):
            return "finalize"
        return "continue"

    def _after_enter_enrichment(self, state: RequestGraphState) -> str:
        if state.get("error") or state.get("terminal_reason"):
            return "finalize"
        if state.get("skip_enrichment"):
            return "promote"
        return "run"

    def _after_enrichment_sync(self, state: RequestGraphState) -> str:
        if state.get("error"):
            return "finalize"
        if state.get("continue_enrichment"):
            return "continue"
        if state.get("terminal_reason") == "completed":
            return "promote"
        if state.get("terminal_reason"):
            return "finalize"
        return "promote"
    def _load_request_context(self, state: RequestGraphState) -> dict[str, Any]:
        request = self._request_repository.get_request(state["request_id"])
        if request is None:
            return {
                "error": f"Request {state['request_id']} not found",
                "graph_status": "failed",
                "terminal_reason": "request_missing",
            }
        source_quality = _current_source_quality(request)
        progress = _clone_progress(request.progress)
        progress = _update_progress_graph(
            progress,
            active_node="load_request_context",
            graph_run_id=state.get("graph_run_id"),
            worker_id=self._worker_id,
            runtime_mode=self._runtime_mode,
        )
        return {
            "request": request,
            "progress": progress,
            "source_quality": source_quality,
            "recovery_reason": "missing_source" if not request.source_slug else "weak_source" if source_quality["isWeak"] else None,
        }

    def _recover_source(self, state: RequestGraphState) -> dict[str, Any]:
        request = state["request"]
        source_quality = dict(state.get("source_quality") or _current_source_quality(request))
        recovery_reason = state.get("recovery_reason")
        if recovery_reason is None:
            return {"source_quality": source_quality, "request_paused": False, "terminal_reason": None}

        recovery_attempts = int(source_quality.get("recoveryAttempts", 0) or 0)
        if recovery_attempts >= self._free_recovery_attempts:
            source_quality["sourceRejectedCount"] = int(source_quality.get("sourceRejectedCount", 0) or 0) + 1
            progress = _update_progress_analytics(state.get("progress") or request.progress, source_quality=source_quality)
            self._request_repository.update_request(
                request.id,
                status="draft",
                stage="awaiting_confirmation",
                progress=progress,
                last_error="Source requires confirmation before crawl execution",
                clear_finished_at=True,
            )
            self._request_repository.append_request_event(
                request.id,
                "source_rejected",
                "Request moved to awaiting_confirmation because source quality is weak or recovery budget was exhausted",
                {
                    "sourceUrl": request.source_url,
                    "sourceQuality": source_quality,
                    "recoveryReason": recovery_reason,
                },
            )
            return {
                "request": self._request_repository.get_request(request.id),
                "progress": progress,
                "source_quality": source_quality,
                "request_paused": True,
                "graph_status": "paused",
                "terminal_reason": "awaiting_confirmation",
                "recovery_reason": None,
            }

        discovery = self._discover_source(request.fraternity_name)
        alternate_url = discovery.get("selected_url")
        alternate_quality = discovery.get("source_quality") or _evaluate_source_url(alternate_url)
        current_score = float(source_quality.get("score", 0.0) or 0.0)
        delta_required = 0.12 if recovery_reason == "zero_chapter" else 0.08
        normalized_current = _normalize_url(request.source_url)
        normalized_alternate = _normalize_url(alternate_url)
        can_switch = bool(alternate_url) and (
            not request.source_url or normalized_current != normalized_alternate
        ) and not bool(alternate_quality.get("isWeak", True)) and float(alternate_quality.get("score", 0.0) or 0.0) > current_score + delta_required

        if not can_switch:
            source_quality["sourceRejectedCount"] = int(source_quality.get("sourceRejectedCount", 0) or 0) + 1
            if recovery_reason == "zero_chapter":
                source_quality["zeroChapterPrevented"] = int(source_quality.get("zeroChapterPrevented", 0) or 0) + 1
            progress = _update_progress_analytics(state.get("progress") or request.progress, source_quality=source_quality)
            self._request_repository.update_request(
                request.id,
                status="draft",
                stage="awaiting_confirmation",
                progress=progress,
                last_error="Source requires confirmation before crawl execution",
                clear_finished_at=True,
            )
            self._request_repository.append_request_event(
                request.id,
                "source_rejected",
                "Request moved to awaiting_confirmation because no stronger source candidate was found",
                {
                    "recoveryReason": recovery_reason,
                    "sourceUrl": request.source_url,
                    "sourceQuality": source_quality,
                    "discovery": discovery,
                },
            )
            return {
                "request": self._request_repository.get_request(request.id),
                "progress": progress,
                "source_quality": source_quality,
                "request_paused": True,
                "graph_status": "paused",
                "terminal_reason": "awaiting_confirmation",
                "recovery_reason": None,
            }

        fraternity_name = str(discovery.get("fraternity_name") or request.fraternity_name)
        fraternity_slug = str(discovery.get("fraternity_slug") or request.fraternity_slug)
        fraternity_id, fraternity_slug = self._crawler_repository.upsert_fraternity(fraternity_slug, fraternity_name, nic_affiliated=True)
        source_slug = f"{fraternity_slug}-main"
        parsed = urlparse(alternate_url)
        self._crawler_repository.upsert_source(
            fraternity_id=fraternity_id,
            slug=source_slug,
            base_url=f"{parsed.scheme}://{parsed.netloc}",
            list_path=alternate_url,
            source_type="html_directory",
            parser_key="directory_v1",
            active=True,
            metadata={
                "discovery": {
                    "selectedUrl": alternate_url,
                    "selectedConfidence": discovery.get("selected_confidence"),
                    "confidenceTier": discovery.get("confidence_tier"),
                    "sourceProvenance": discovery.get("source_provenance"),
                    "fallbackReason": discovery.get("fallback_reason"),
                    "resolutionTrace": discovery.get("resolution_trace") or [],
                }
            },
        )
        next_quality = {
            **alternate_quality,
            "recoveryAttempts": recovery_attempts + 1,
            "recoveredFromUrl": request.source_url,
            "recoveredToUrl": alternate_url,
            "sourceRecoveredCount": int(source_quality.get("sourceRecoveredCount", 0) or 0) + 1,
            "sourceRejectedCount": int(source_quality.get("sourceRejectedCount", 0) or 0),
            "zeroChapterPrevented": int(source_quality.get("zeroChapterPrevented", 0) or 0) + (1 if recovery_reason == "zero_chapter" else 0),
        }
        progress = _clone_progress(state.get("progress") or request.progress)
        progress["discovery"] = {
            "sourceUrl": alternate_url,
            "sourceConfidence": discovery.get("selected_confidence") or 0.0,
            "confidenceTier": discovery.get("confidence_tier") or _confidence_tier(discovery.get("selected_confidence")),
            "sourceProvenance": discovery.get("source_provenance"),
            "fallbackReason": discovery.get("fallback_reason"),
            "sourceQuality": alternate_quality,
            "selectedCandidateRationale": discovery.get("selected_candidate_rationale"),
            "resolutionTrace": discovery.get("resolution_trace") or [],
            "candidates": discovery.get("candidates") or [],
        }
        progress = _update_progress_analytics(progress, source_quality=next_quality)
        self._request_repository.update_request(
            request.id,
            source_slug=source_slug,
            source_url=alternate_url,
            source_confidence=float(discovery.get("selected_confidence") or 0.0),
            progress=progress,
            last_error=None,
        )
        self._request_repository.append_request_event(
            request.id,
            "source_recovered",
            "Recovered request source and switched to a stronger candidate",
            {
                "previousSourceUrl": request.source_url,
                "nextSourceUrl": alternate_url,
                "recoveryReason": recovery_reason,
                "sourceQuality": next_quality,
            },
        )
        return {
            "request": self._request_repository.get_request(request.id),
            "progress": progress,
            "source_quality": next_quality,
            "recovery_reason": None,
            "request_paused": False,
            "terminal_reason": None,
        }

    def _start_crawl(self, state: RequestGraphState) -> dict[str, Any]:
        request = state["request"]
        if not request.source_slug:
            return {
                "error": "Missing source slug for crawl execution",
                "graph_status": "failed",
                "terminal_reason": "missing_source_slug",
            }
        crawl_runtime_mode = str(state.get("crawl_runtime_mode_used") or self._crawl_runtime_mode or "adaptive_assisted")
        crawl_policy_version = request.config.get("crawlPolicyVersion")
        progress = _update_progress_graph(
            state.get("progress") or request.progress,
            active_node="start_crawl",
            graph_run_id=state.get("graph_run_id"),
            worker_id=self._worker_id,
            runtime_mode=self._runtime_mode,
        )
        self._request_repository.update_request(
            request.id,
            status="running",
            stage="crawl_run",
            started_at_now=request.started_at is None,
            clear_finished_at=True,
            progress=progress,
            last_error="",
        )
        self._request_repository.append_request_event(
            request.id,
            "stage_started",
            "Crawl run stage started",
            {
                "stage": "crawl_run",
                "sourceSlug": request.source_slug,
                "runtimeMode": crawl_runtime_mode,
                "policyVersion": crawl_policy_version,
            },
        )
        crawl_stage_started_at = _now_iso()
        baseline = self._request_repository.get_latest_crawl_run_for_source(request.source_slug)
        self._run_crawl(
            source_slug=request.source_slug,
            runtime_mode=crawl_runtime_mode,
            policy_mode="live",
            policy_version=crawl_policy_version,
        )
        return {
            "crawl_stage_started_at": crawl_stage_started_at,
            "crawl_run_baseline_id": int(baseline["id"]) if baseline is not None else None,
            "crawl_runtime_mode_used": crawl_runtime_mode,
            "retry_crawl": False,
        }
    def _sync_crawl_progress(self, state: RequestGraphState) -> dict[str, Any]:
        request = self._request_repository.get_request(state["request_id"])
        if request is None:
            return {
                "error": f"Request {state['request_id']} disappeared during crawl sync",
                "graph_status": "failed",
                "terminal_reason": "request_missing",
            }
        crawl_run = (
            self._request_repository.get_latest_crawl_run_for_source(
                request.source_slug,
                started_after=state.get("crawl_stage_started_at"),
                exclude_run_id=state.get("crawl_run_baseline_id"),
            )
            if request.source_slug
            else None
        )
        field_snapshot = self._request_repository.get_source_field_job_snapshot(request.source_slug) if request.source_slug else []
        source_quality = dict(state.get("source_quality") or _current_source_quality(request))
        progress = _build_progress_snapshot(
            source_url=request.source_url,
            source_confidence=request.source_confidence,
            confidence_tier=_confidence_tier(request.source_confidence),
            source_provenance=(request.progress.get("discovery") or {}).get("sourceProvenance"),
            fallback_reason=(request.progress.get("discovery") or {}).get("fallbackReason"),
            resolution_trace=(request.progress.get("discovery") or {}).get("resolutionTrace") or [],
            candidates=(request.progress.get("discovery") or {}).get("candidates") or [],
            crawl_run=crawl_run,
            field_snapshot=field_snapshot,
            analytics={"sourceQuality": source_quality, "enrichment": (request.progress.get("analytics") or {}).get("enrichment")},
            graph={
                "requestGraphRunId": state.get("graph_run_id"),
                "runtimeMode": self._runtime_mode,
                "workerId": self._worker_id,
                "activeNode": "sync_crawl_progress",
            },
        )
        self._request_repository.update_request(request.id, progress=progress)
        if not crawl_run:
            self._request_repository.update_request(
                request.id,
                status="failed",
                stage="failed",
                finished_at_now=True,
                progress=progress,
                last_error="Crawl completed without recording a new crawl run",
            )
            self._request_repository.append_request_event(
                request.id,
                "stage_failed",
                "Crawl run stage did not create a new crawl run record",
                {"stage": "crawl_run"},
            )
            return {
                "request": self._request_repository.get_request(request.id),
                "progress": progress,
                "graph_status": "failed",
                "terminal_reason": "crawl_run_missing",
            }
        if int(crawl_run.get("records_seen", 0) or 0) <= 0:
            return {
                "request": request,
                "progress": progress,
                "crawl_run": crawl_run,
                "field_snapshot": field_snapshot,
                "recovery_reason": "zero_chapter",
                "source_quality": source_quality,
            }
        self._request_repository.append_request_event(
            request.id,
            "stage_completed",
            "Crawl run stage completed",
            {"stage": "crawl_run", "crawlRunId": crawl_run.get("id")},
        )
        self._request_repository.update_request(request.id, stage="enrichment", progress=progress)
        return {
            "request": self._request_repository.get_request(request.id),
            "progress": progress,
            "crawl_run": crawl_run,
            "field_snapshot": field_snapshot,
            "recovery_reason": None,
            "terminal_reason": None,
        }

    def _purge_inactive_schools(self, state: RequestGraphState) -> dict[str, Any]:
        request = self._request_repository.get_request(state["request_id"])
        if request is None:
            return {
                "error": f"Request {state['request_id']} disappeared during inactive-school purge",
                "graph_status": "failed",
                "terminal_reason": "request_missing",
            }

        progress = _clone_progress(state.get("progress") or request.progress)
        progress = _update_progress_graph(
            progress,
            active_node="purge_inactive_schools",
            graph_run_id=state.get("graph_run_id"),
            worker_id=self._worker_id,
            runtime_mode=self._runtime_mode,
        )
        crawl_run_payload = state.get("crawl_run")
        crawl_run_id = int(crawl_run_payload.get("id")) if isinstance(crawl_run_payload, dict) and crawl_run_payload.get("id") is not None else int((progress.get("crawlRun") or {}).get("id")) if ((progress.get("crawlRun") or {}).get("id")) is not None else None
        if crawl_run_id is None:
            self._request_repository.update_request(request.id, stage="enrichment", progress=progress)
            return {"request": self._request_repository.get_request(request.id), "progress": progress}

        purged_inactive = 0
        purged_banned_school = 0
        for chapter in self._crawler_repository.list_chapters_for_crawl_run(crawl_run_id):
            school_name = str(chapter.get("university_name") or "").strip()
            fraternity_slug = str(chapter.get("fraternity_slug") or "").strip()
            if not school_name or not fraternity_slug:
                continue

            policy = self._crawler_repository.get_school_policy(school_name)
            activity = self._crawler_repository.get_chapter_activity(fraternity_slug=fraternity_slug, school_name=school_name)
            policy_status = str(getattr(policy, "greek_life_status", "unknown") or "unknown")
            activity_status = str(getattr(activity, "chapter_activity_status", "unknown") or "unknown")
            if policy_status != "banned" and activity_status != "confirmed_inactive":
                continue

            reason_code = str(
                getattr(activity, "reason_code", None)
                or getattr(policy, "reason_code", None)
                or ("school_policy_banned" if policy_status == "banned" else "chapter_inactive")
            )
            evidence_url = getattr(activity, "evidence_url", None) or getattr(policy, "evidence_url", None)
            evidence_source_type = getattr(activity, "evidence_source_type", None) or getattr(policy, "evidence_source_type", None)
            source_snippet = str(
                (getattr(activity, "metadata", {}) or {}).get("sourceSnippet")
                or (getattr(policy, "metadata", {}) or {}).get("sourceSnippet")
                or reason_code
            )[:400]
            self._crawler_repository.apply_chapter_inactive_status(
                chapter_id=str(chapter["chapter_id"]),
                chapter_slug=str(chapter["chapter_slug"]),
                fraternity_slug=fraternity_slug,
                source_slug=request.source_slug,
                crawl_run_id=crawl_run_id,
                reason_code=reason_code,
                evidence_url=evidence_url,
                evidence_source_type=evidence_source_type,
                source_snippet=source_snippet,
                provider="request_graph_purge",
                metadata={"requestId": request.id, "stage": "purge_inactive_schools"},
            )
            self._crawler_repository.complete_pending_field_jobs_for_chapter(
                chapter_id=str(chapter["chapter_id"]),
                reason_code=reason_code,
                status="inactive_by_school_validation",
                field_states={
                    "website_url": "inactive",
                    "contact_email": "inactive",
                    "instagram_url": "inactive",
                },
            )
            purged_inactive += 1
            if policy_status == "banned":
                purged_banned_school += 1

        previous_enrichment = dict((((progress.get("analytics") or {}).get("enrichment") or {})))
        previous_queue_triage = dict(previous_enrichment.get("queueTriage") or {})
        if purged_inactive:
            self._request_repository.append_request_event(
                request.id,
                "inactive_school_purged",
                "Known inactive chapters were removed before enrichment",
                {
                    "crawlRunId": crawl_run_id,
                    "purgedInactiveChapters": purged_inactive,
                    "purgedBannedSchoolChapters": purged_banned_school,
                },
            )
        progress = _update_progress_analytics(
            progress,
            enrichment={
                **previous_enrichment,
                "queueTriage": {
                    **previous_queue_triage,
                    "purgedInactiveChapters": int(previous_queue_triage.get("purgedInactiveChapters", 0) or 0) + purged_inactive,
                    "purgedBannedSchoolChapters": int(previous_queue_triage.get("purgedBannedSchoolChapters", 0) or 0) + purged_banned_school,
                },
            },
        )
        self._request_repository.update_request(request.id, stage="purge_inactive_schools", progress=progress)
        return {
            "request": self._request_repository.get_request(request.id),
            "progress": progress,
        }

    def _enter_enrichment(self, state: RequestGraphState) -> dict[str, Any]:
        request = self._request_repository.get_request(state["request_id"])
        if request is None:
            return {
                "error": f"Request {state['request_id']} disappeared during enrichment entry",
                "graph_status": "failed",
                "terminal_reason": "request_missing",
            }
        progress = _clone_progress(state.get("progress") or request.progress)
        total_queue = _remaining_actionable_queue(progress)
        cycle_state = state.get("cycle_state") or {"cyclesCompleted": 0, "lowProgressCycles": 0, "degradedCycleCount": 0, "processedTotal": 0, "requeuedTotal": 0, "failedTerminalTotal": 0}
        effective_config = _compute_adaptive_enrichment_config(request.config, progress, cycle_state)
        previous_enrichment = dict((((progress.get("analytics") or {}).get("enrichment") or {})))
        previous_queue_triage = dict(previous_enrichment.get("queueTriage") or {})
        previous_chapter_repair = dict(previous_enrichment.get("chapterRepair") or {})
        progress = _update_progress_analytics(
            progress,
            source_quality=state.get("source_quality") or _current_source_quality(request),
            enrichment={
                **previous_enrichment,
                "adaptiveMaxEnrichmentCycles": effective_config["maxEnrichmentCycles"],
                "effectiveFieldJobWorkers": effective_config["fieldJobWorkers"],
                "effectiveFieldJobLimitPerCycle": effective_config["fieldJobLimitPerCycle"],
                "cyclesCompleted": cycle_state["cyclesCompleted"],
                "lowProgressCycles": cycle_state["lowProgressCycles"],
                "degradedCycleCount": cycle_state["degradedCycleCount"],
                "queueAtStart": total_queue,
                "queueRemaining": total_queue,
                "runtimeFallbackCount": int(previous_enrichment.get("runtimeFallbackCount", 0) or 0),
                "queueBurnRate": float(previous_enrichment.get("queueBurnRate", 0) or 0),
                "budgetStrategy": previous_enrichment.get("budgetStrategy") or "v3_initial_budget",
                "queueTriage": {
                    "invalidCancelled": int(previous_queue_triage.get("invalidCancelled", 0) or 0),
                    "deferredLongCooldown": int(previous_queue_triage.get("deferredLongCooldown", 0) or 0),
                    "repairQueued": int(previous_queue_triage.get("repairQueued", 0) or 0),
                    "actionableRetained": int(previous_queue_triage.get("actionableRetained", 0) or 0),
                    "sourceInvaliditySaturated": bool(previous_queue_triage.get("sourceInvaliditySaturated", False)),
                    "purgedInactiveChapters": int(previous_queue_triage.get("purgedInactiveChapters", 0) or 0),
                    "purgedBannedSchoolChapters": int(previous_queue_triage.get("purgedBannedSchoolChapters", 0) or 0),
                },
                "chapterRepair": {
                    "queued": int(previous_chapter_repair.get("queued", 0) or 0),
                    "running": int(previous_chapter_repair.get("running", 0) or 0),
                    "promotedToCanonical": int(previous_chapter_repair.get("promotedToCanonical", 0) or 0),
                    "downgradedToProvisional": int(previous_chapter_repair.get("downgradedToProvisional", 0) or 0),
                    "confirmedInvalid": int(previous_chapter_repair.get("confirmedInvalid", 0) or 0),
                    "repairExhausted": int(previous_chapter_repair.get("repairExhausted", 0) or 0),
                    "reconciledHistorical": int(previous_chapter_repair.get("reconciledHistorical", 0) or 0),
                },
            },
        )
        progress = _update_progress_graph(
            progress,
            active_node="enter_enrichment",
            graph_run_id=state.get("graph_run_id"),
            worker_id=self._worker_id,
            runtime_mode=self._runtime_mode,
        )
        self._request_repository.update_request(request.id, stage="enrichment", progress=progress)
        if total_queue <= 0:
            self._request_repository.update_request(
                request.id,
                status="succeeded",
                stage="completed",
                finished_at_now=True,
                progress=progress,
                last_error="",
            )
            self._request_repository.append_request_event(
                request.id,
                "request_completed",
                "Request completed without deferred enrichment work",
                {"totals": progress.get("totals")},
            )
            return {
                "request": self._request_repository.get_request(request.id),
                "progress": progress,
                "cycle_state": cycle_state,
                "effective_config": effective_config,
                "skip_enrichment": True,
                "graph_status": "succeeded",
                "terminal_reason": "completed",
            }
        return {
            "request": request,
            "progress": progress,
            "cycle_state": cycle_state,
            "effective_config": effective_config,
            "skip_enrichment": False,
            "continue_enrichment": False,
        }

    def _run_enrichment_cycle(self, state: RequestGraphState) -> dict[str, Any]:
        request = state["request"]
        effective_config = dict(state.get("effective_config") or request.config)
        cycle_state = dict(state.get("cycle_state") or {"cyclesCompleted": 0, "lowProgressCycles": 0, "degradedCycleCount": 0, "processedTotal": 0, "requeuedTotal": 0, "failedTerminalTotal": 0})
        cycle = int(state.get("cycle") or 0) + 1
        preflight_snapshot = self._search_preflight()
        for provider, payload in (preflight_snapshot.get("provider_health") or {}).items():
            provider_healthy, snapshot_payload = _provider_snapshot_payload(preflight_snapshot, payload)
            self._request_repository.insert_provider_health_snapshot(
                request_id=request.id,
                source_slug=request.source_slug,
                provider=str(provider),
                healthy=provider_healthy,
                success_rate=float(payload.get("success_rate", 0.0) or 0.0),
                probe_count=int(payload.get("attempts", 0) or 0),
                payload=snapshot_payload,
            )
        result = self._process_field_jobs(
            limit=int(effective_config.get("fieldJobLimitPerCycle", 1)),
            source_slug=request.source_slug,
            workers=int(effective_config.get("fieldJobWorkers", 1)),
            require_healthy_search=False,
            run_preflight=False,
            runtime_mode=self._field_job_runtime_mode,
            graph_durability=self._field_job_graph_durability,
        )
        cycle_state["cyclesCompleted"] = cycle
        cycle_state["processedTotal"] = int(cycle_state.get("processedTotal", 0) or 0) + int(result.get("processed", 0) or 0)
        cycle_state["requeuedTotal"] = int(cycle_state.get("requeuedTotal", 0) or 0) + int(result.get("requeued", 0) or 0)
        cycle_state["failedTerminalTotal"] = int(cycle_state.get("failedTerminalTotal", 0) or 0) + int(result.get("failed_terminal", 0) or 0)
        return {
            "cycle": cycle,
            "cycle_state": cycle_state,
            "preflight_snapshot": preflight_snapshot,
            "latest_field_job_result": result,
        }
    def _sync_enrichment_progress(self, state: RequestGraphState) -> dict[str, Any]:
        request = self._request_repository.get_request(state["request_id"])
        if request is None:
            return {
                "error": f"Request {state['request_id']} disappeared during enrichment sync",
                "graph_status": "failed",
                "terminal_reason": "request_missing",
            }
        cycle_state = dict(state.get("cycle_state") or {"cyclesCompleted": 0, "lowProgressCycles": 0, "degradedCycleCount": 0, "processedTotal": 0, "requeuedTotal": 0, "failedTerminalTotal": 0})
        field_result = dict(state.get("latest_field_job_result") or {})
        crawl_run = self._request_repository.get_latest_crawl_run_for_source(request.source_slug) if request.source_slug else None
        field_snapshot = self._request_repository.get_source_field_job_snapshot(request.source_slug) if request.source_slug else []
        progress = _build_progress_snapshot(
            source_url=request.source_url,
            source_confidence=request.source_confidence,
            confidence_tier=_confidence_tier(request.source_confidence),
            source_provenance=(request.progress.get("discovery") or {}).get("sourceProvenance"),
            fallback_reason=(request.progress.get("discovery") or {}).get("fallbackReason"),
            resolution_trace=(request.progress.get("discovery") or {}).get("resolutionTrace") or [],
            candidates=(request.progress.get("discovery") or {}).get("candidates") or [],
            crawl_run=crawl_run,
            field_snapshot=field_snapshot,
            analytics=request.progress.get("analytics"),
            graph={
                "requestGraphRunId": state.get("graph_run_id"),
                "runtimeMode": self._runtime_mode,
                "workerId": self._worker_id,
                "activeNode": "sync_enrichment_progress",
            },
        )
        remaining_queue = _remaining_actionable_queue(progress)
        low_signal_cycle = int(field_result.get("processed", 0) or 0) <= 0 and int(field_result.get("requeued", 0) or 0) > 0
        cycle_state["lowProgressCycles"] = cycle_state["lowProgressCycles"] + 1 if low_signal_cycle else 0
        if int(field_result.get("requeued", 0) or 0) > max(int(field_result.get("processed", 0) or 0) * 3, 20):
            cycle_state["degradedCycleCount"] = cycle_state.get("degradedCycleCount", 0) + 1
        effective_config = _compute_adaptive_enrichment_config(request.config, progress, cycle_state)
        queue_at_start = int((((request.progress.get("analytics") or {}).get("enrichment") or {}).get("queueAtStart", 0)) or _total_field_jobs(progress.get("totals")))
        runtime_fallback_count = int((((request.progress.get("analytics") or {}).get("enrichment") or {}).get("runtimeFallbackCount", 0)) or 0) + int(field_result.get("runtime_fallback_count", 0) or 0)
        previous_enrichment = dict((((request.progress.get("analytics") or {}).get("enrichment") or {})) )
        previous_queue_triage = dict(previous_enrichment.get("queueTriage") or {})
        previous_chapter_repair = dict(previous_enrichment.get("chapterRepair") or {})
        current_queue_triage = dict(field_result.get("queue_triage") or {})
        current_chapter_repair = dict(field_result.get("chapter_repair") or {})
        queue_burn_rate = round((queue_at_start - remaining_queue) / queue_at_start, 4) if queue_at_start > 0 else 0.0
        preflight_probe_queries = list(dict.fromkeys([
            *list(previous_enrichment.get("preflightProbeQueries") or []),
            *list(field_result.get("preflight_probe_queries") or []),
        ]))
        chapter_search_queries = list(dict.fromkeys([
            *list(previous_enrichment.get("chapterSearchQueries") or []),
            *list(field_result.get("chapter_search_queries") or []),
        ]))
        provider_window_state = dict(field_result.get("provider_window_state") or previous_enrichment.get("providerWindowState") or {})
        progress = _update_progress_analytics(
            progress,
            source_quality=state.get("source_quality") or _current_source_quality(request),
            enrichment={
                "adaptiveMaxEnrichmentCycles": effective_config["maxEnrichmentCycles"],
                "effectiveFieldJobWorkers": effective_config["fieldJobWorkers"],
                "effectiveFieldJobLimitPerCycle": effective_config["fieldJobLimitPerCycle"],
                "cyclesCompleted": cycle_state["cyclesCompleted"],
                "lowProgressCycles": cycle_state["lowProgressCycles"],
                "degradedCycleCount": cycle_state["degradedCycleCount"],
                "queueAtStart": queue_at_start,
                "queueRemaining": remaining_queue,
                "runtimeFallbackCount": runtime_fallback_count,
                "queueBurnRate": queue_burn_rate,
                "budgetStrategy": "v3_degraded" if cycle_state["degradedCycleCount"] > 0 else "v3_steady",
                "processedTotal": int(cycle_state.get("processedTotal", 0) or 0),
                "requeuedTotal": int(cycle_state.get("requeuedTotal", 0) or 0),
                "failedTerminalTotal": int(cycle_state.get("failedTerminalTotal", 0) or 0),
                "newCompleteRows": int(previous_enrichment.get("newCompleteRows", 0) or 0) + int(field_result.get("new_complete_rows", 0) or 0),
                "newInactiveValidatedRows": int(previous_enrichment.get("newInactiveValidatedRows", 0) or 0) + int(field_result.get("new_inactive_validated_rows", 0) or 0),
                "newConfirmedAbsentWebsiteRows": int(previous_enrichment.get("newConfirmedAbsentWebsiteRows", 0) or 0) + int(field_result.get("new_confirmed_absent_website_rows", 0) or 0),
                "providerDegradedDeferred": int(previous_enrichment.get("providerDegradedDeferred", 0) or 0) + int(field_result.get("provider_degraded_deferred", 0) or 0),
                "dependencyWaitDeferred": int(previous_enrichment.get("dependencyWaitDeferred", 0) or 0) + int(field_result.get("dependency_wait_deferred", 0) or 0),
                "supportingPageResolved": int(previous_enrichment.get("supportingPageResolved", 0) or 0) + int(field_result.get("supporting_page_resolved", 0) or 0),
                "supportingPageContactResolved": int(previous_enrichment.get("supportingPageContactResolved", 0) or 0) + int(field_result.get("supporting_page_contact_resolved", 0) or 0),
                "externalSearchContactResolved": int(previous_enrichment.get("externalSearchContactResolved", 0) or 0) + int(field_result.get("external_search_contact_resolved", 0) or 0),
                "enrichmentObservationsLogged": int(previous_enrichment.get("enrichmentObservationsLogged", 0) or 0) + int(field_result.get("enrichment_observations_logged", 0) or 0),
                "midBatchProviderRechecks": int(previous_enrichment.get("midBatchProviderRechecks", 0) or 0) + int(field_result.get("mid_batch_provider_rechecks", 0) or 0),
                "midBatchProviderReorders": int(previous_enrichment.get("midBatchProviderReorders", 0) or 0) + int(field_result.get("mid_batch_provider_reorders", 0) or 0),
                "productiveYield": float(field_result.get("productive_yield", 0.0) or 0.0),
                "preflightProbeQueries": preflight_probe_queries,
                "chapterSearchQueries": chapter_search_queries,
                "preflightProbeCount": len(preflight_probe_queries),
                "chapterSearchQueryCount": len(chapter_search_queries),
                "providerWindowState": provider_window_state,
                "queueTriage": {
                    "invalidCancelled": int(previous_queue_triage.get("invalidCancelled", 0) or 0) + int(current_queue_triage.get("invalidCancelled", 0) or 0),
                    "deferredLongCooldown": int(previous_queue_triage.get("deferredLongCooldown", 0) or 0) + int(current_queue_triage.get("deferredLongCooldown", 0) or 0),
                    "repairQueued": int(previous_queue_triage.get("repairQueued", 0) or 0) + int(current_queue_triage.get("repairQueued", 0) or 0),
                    "actionableRetained": int(previous_queue_triage.get("actionableRetained", 0) or 0) + int(current_queue_triage.get("actionableRetained", 0) or 0),
                    "sourceInvaliditySaturated": bool(previous_queue_triage.get("sourceInvaliditySaturated", False) or current_queue_triage.get("sourceInvaliditySaturated", False)),
                    "purgedInactiveChapters": int(previous_queue_triage.get("purgedInactiveChapters", 0) or 0),
                    "purgedBannedSchoolChapters": int(previous_queue_triage.get("purgedBannedSchoolChapters", 0) or 0),
                },
                "chapterRepair": {
                    "queued": int(previous_chapter_repair.get("queued", 0) or 0) + int(current_chapter_repair.get("queued", 0) or 0),
                    "running": int(previous_chapter_repair.get("running", 0) or 0) + int(current_chapter_repair.get("running", 0) or 0),
                    "promotedToCanonical": int(previous_chapter_repair.get("promotedToCanonical", 0) or 0) + int(current_chapter_repair.get("promotedToCanonical", 0) or 0),
                    "downgradedToProvisional": int(previous_chapter_repair.get("downgradedToProvisional", 0) or 0) + int(current_chapter_repair.get("downgradedToProvisional", 0) or 0),
                    "confirmedInvalid": int(previous_chapter_repair.get("confirmedInvalid", 0) or 0) + int(current_chapter_repair.get("confirmedInvalid", 0) or 0),
                    "repairExhausted": int(previous_chapter_repair.get("repairExhausted", 0) or 0) + int(current_chapter_repair.get("repairExhausted", 0) or 0),
                    "reconciledHistorical": int(previous_chapter_repair.get("reconciledHistorical", 0) or 0) + int(current_chapter_repair.get("reconciledHistorical", 0) or 0),
                },
            },
        )
        progress.setdefault("contactResolution", {})
        progress["contactResolution"]["requeued"] = int(cycle_state.get("requeuedTotal", 0) or 0)
        self._request_repository.update_request(request.id, progress=progress)
        self._request_repository.append_request_event(
            request.id,
            "enrichment_cycle",
            f"Enrichment cycle {cycle_state['cyclesCompleted']} completed",
            {
                "cycle": cycle_state["cyclesCompleted"],
                "processed": field_result.get("processed", 0),
                "requeued": field_result.get("requeued", 0),
                "failedTerminal": field_result.get("failed_terminal", 0),
                "runtimeModeUsed": field_result.get("runtime_mode_used", self._field_job_runtime_mode),
                "totals": progress.get("totals"),
                "newCompleteRows": field_result.get("new_complete_rows", 0),
                "providerDegradedDeferred": field_result.get("provider_degraded_deferred", 0),
                "dependencyWaitDeferred": field_result.get("dependency_wait_deferred", 0),
                "supportingPageContactResolved": field_result.get("supporting_page_contact_resolved", 0),
                "externalSearchContactResolved": field_result.get("external_search_contact_resolved", 0),
                "preflightProbeQueries": field_result.get("preflight_probe_queries", []),
                "chapterSearchQueries": field_result.get("chapter_search_queries", []),
                "providerWindowState": field_result.get("provider_window_state", {}),
            },
        )
        if remaining_queue <= 0:
            self._request_repository.update_request(
                request.id,
                status="succeeded",
                stage="completed",
                finished_at_now=True,
                progress=progress,
                last_error="",
            )
            self._request_repository.append_request_event(
                request.id,
                "request_completed",
                "Fraternity crawl request completed",
                {"totals": progress.get("totals")},
            )
            return {
                "request": self._request_repository.get_request(request.id),
                "progress": progress,
                "cycle_state": cycle_state,
                "effective_config": effective_config,
                "continue_enrichment": False,
                "graph_status": "succeeded",
                "terminal_reason": "completed",
            }
        preflight_snapshot = dict(state.get("preflight_snapshot") or {})
        if (
            _should_complete_with_deferred_residual_queue(
                progress=progress,
                effective_config=effective_config,
                preflight_snapshot=preflight_snapshot,
            )
            or _should_complete_stalled_residual_queue(
                progress=progress,
                effective_config=effective_config,
                cycle_state=cycle_state,
            )
        ) and (
            int(cycle_state.get("lowProgressCycles", 0) or 0) >= 2
            or int(cycle_state.get("degradedCycleCount", 0) or 0) >= 2
        ):
            return self._complete_with_deferred_residual_queue(
                request=request,
                progress=progress,
                cycle_state=cycle_state,
                effective_config=effective_config,
                remaining_queue=remaining_queue,
                preflight_snapshot=preflight_snapshot,
                source_quality=state.get("source_quality") or _current_source_quality(request),
                message="Request completed early with small residual actionable queue deferred because provider health was degraded and progress had stalled",
            )
        if cycle_state["cyclesCompleted"] >= int(effective_config.get("maxEnrichmentCycles", 1)):
            if _should_complete_with_deferred_residual_queue(
                progress=progress,
                effective_config=effective_config,
                preflight_snapshot=preflight_snapshot,
            ) or _should_complete_stalled_residual_queue(
                progress=progress,
                effective_config=effective_config,
                cycle_state=cycle_state,
            ):
                return self._complete_with_deferred_residual_queue(
                    request=request,
                    progress=progress,
                    cycle_state=cycle_state,
                    effective_config=effective_config,
                    remaining_queue=remaining_queue,
                    preflight_snapshot=preflight_snapshot,
                    source_quality=state.get("source_quality") or _current_source_quality(request),
                    message="Request completed with small residual actionable queue deferred because provider health was degraded",
                )
            self._request_repository.update_request(
                request.id,
                status="failed",
                stage="failed",
                finished_at_now=True,
                progress=progress,
                last_error="Enrichment cycle budget exhausted before queue drained",
            )
            self._request_repository.append_request_event(
                request.id,
                "request_failed",
                "Request failed: enrichment cycle budget exhausted",
                {"totals": progress.get("totals")},
            )
            return {
                "request": self._request_repository.get_request(request.id),
                "progress": progress,
                "cycle_state": cycle_state,
                "effective_config": effective_config,
                "continue_enrichment": False,
                "graph_status": "failed",
                "terminal_reason": "budget_exhausted",
            }
        if cycle_state["degradedCycleCount"] > 0:
            pause_ms = int(effective_config.get("pauseMs", 0) or 0)
            if pause_ms > 0:
                time.sleep(pause_ms / 1000.0)
        return {
            "request": request,
            "progress": progress,
            "cycle_state": cycle_state,
            "effective_config": effective_config,
            "continue_enrichment": True,
            "graph_status": "running",
            "terminal_reason": None,
        }

    def _complete_with_deferred_residual_queue(
        self,
        *,
        request: FraternityCrawlRequestRecord,
        progress: dict[str, Any],
        cycle_state: dict[str, int],
        effective_config: dict[str, int],
        remaining_queue: int,
        preflight_snapshot: dict[str, Any],
        source_quality: dict[str, Any],
        message: str,
    ) -> dict[str, Any]:
        residual_threshold = _residual_queue_threshold(progress=progress, effective_config=effective_config)
        progress = _update_progress_analytics(
            progress,
            source_quality=source_quality,
            enrichment={
                **(((progress.get("analytics") or {}).get("enrichment") or {})),
                "completionMode": "deferred_provider_residual",
                "residualActionableAtCompletion": remaining_queue,
                "providerDegradedAtCompletion": True,
                "residualThreshold": residual_threshold,
            },
        )
        self._request_repository.update_request(
            request.id,
            status="succeeded",
            stage="completed",
            finished_at_now=True,
            progress=progress,
            last_error="",
        )
        self._request_repository.append_request_event(
            request.id,
            "request_completed",
            message,
            {
                "totals": progress.get("totals"),
                "residualActionable": remaining_queue,
                "completionMode": "deferred_provider_residual",
                "preflightHealthy": bool(preflight_snapshot.get("healthy", True)),
                "preflightSuccessRate": float(preflight_snapshot.get("success_rate", 0.0) or 0.0),
                "lowProgressCycles": int(cycle_state.get("lowProgressCycles", 0) or 0),
                "degradedCycleCount": int(cycle_state.get("degradedCycleCount", 0) or 0),
                "residualThreshold": residual_threshold,
            },
        )
        return {
            "request": self._request_repository.get_request(request.id),
            "progress": progress,
            "cycle_state": cycle_state,
            "effective_config": effective_config,
            "continue_enrichment": False,
            "graph_status": "succeeded",
            "terminal_reason": "completed_deferred_provider_recovery",
        }

    def _evaluate_provisional_promotions(self, state: RequestGraphState) -> dict[str, Any]:
        progress = _clone_progress(state.get("progress") or {})
        provisional = progress.get("provisional") or {}
        provisional.setdefault("evaluated", True)
        provisional.setdefault("autoPromoted", 0)
        provisional.setdefault("reviewRequired", 0)
        provisional.setdefault("rejected", 0)
        provisional.setdefault("remaining", 0)
        request = state.get("request")
        if request is not None:
            provisional_rows = self._request_repository.list_provisional_chapters_for_request(request.id)
            promoted = 0
            review_required = 0
            rejected = 0
            remaining = 0
            source = next(iter(self._crawler_repository.load_sources(request.source_slug)), None) if request.source_slug else None
            for item in provisional_rows:
                has_institution = bool((item.university_name or "").strip())
                has_contact = any(
                    bool((value or "").strip())
                    for value in (item.website_url, item.instagram_url, item.contact_email)
                )
                if source is not None and has_institution and has_contact:
                    field_states = {
                        key: "found"
                        for key, value in {
                            "find_website": item.website_url,
                            "find_instagram": item.instagram_url,
                            "find_email": item.contact_email,
                        }.items()
                        if value
                    }
                    chapter_id = self._crawler_repository.upsert_chapter_discovery(
                        source,
                        NormalizedChapter(
                            fraternity_slug=request.fraternity_slug,
                            source_slug=request.source_slug,
                            slug=item.slug,
                            name=item.name,
                            university_name=item.university_name,
                            city=item.city,
                            state=item.state,
                            country=item.country or "USA",
                            website_url=item.website_url,
                            instagram_url=item.instagram_url,
                            contact_email=item.contact_email,
                            chapter_status="active",
                            field_states=field_states,
                        ),
                    )
                    self._request_repository.update_provisional_chapter_status(
                        item.id,
                        status="promoted",
                        promotion_reason="auto_promoted_contact_and_institution_signal",
                        promoted_chapter_id=chapter_id,
                    )
                    promoted += 1
                    continue

                if not has_institution and not has_contact:
                    self._request_repository.update_provisional_chapter_status(
                        item.id,
                        status="rejected",
                        promotion_reason="rejected_missing_institution_and_contact_signal",
                    )
                    rejected += 1
                    continue

                self._request_repository.update_provisional_chapter_status(
                    item.id,
                    status="review",
                    promotion_reason="review_required_missing_official_contact_signal",
                )
                review_required += 1

            remaining = len(
                self._request_repository.list_provisional_chapters_for_request(
                    request.id,
                    statuses=("provisional",),
                )
            )
            provisional["autoPromoted"] = promoted
            provisional["reviewRequired"] = review_required
            provisional["rejected"] = rejected
            provisional["remaining"] = remaining
        progress["provisional"] = provisional
        if request is not None:
            self._request_repository.update_request(request.id, progress=progress)
        return {"progress": progress}

    def _finalize(self, state: RequestGraphState) -> dict[str, Any]:
        graph_status = state.get("graph_status") or ("failed" if state.get("error") else "succeeded")
        return {
            "graph_status": graph_status,
            "terminal_reason": state.get("terminal_reason") or ("completed" if graph_status == "succeeded" else "failed"),
        }


def _clone_progress(progress: dict[str, Any] | None) -> dict[str, Any]:
    return json.loads(json.dumps(progress or {}))


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=_json_default))


def _json_default(value: Any) -> Any:
    if isinstance(value, FraternityCrawlRequestRecord) or is_dataclass(value):
        return asdict(value)
    return str(value)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _normalize_url(url: str | None) -> str:
    return (url or "").rstrip("/").strip().lower()


def _confidence_tier(value: float | None) -> str:
    confidence = float(value or 0.0)
    if confidence >= 0.8:
        return "high"
    if confidence >= 0.6:
        return "medium"
    return "low"


def _remaining_queue(totals: dict[str, Any] | None) -> int:
    if not totals:
        return 0
    return int(totals.get("queued", 0) or 0) + int(totals.get("running", 0) or 0)


def _remaining_actionable_queue(progress: dict[str, Any] | None) -> int:
    payload = progress or {}
    contact_resolution = payload.get("contactResolution") or {}
    totals = payload.get("totals") or {}
    if contact_resolution:
        return int(contact_resolution.get("queuedActionable", 0) or 0) + int(totals.get("running", 0) or 0)
    return _remaining_queue(totals)


def _residual_queue_threshold(*, progress: dict[str, Any], effective_config: dict[str, int]) -> int:
    enrichment = ((progress.get("analytics") or {}).get("enrichment") or {})
    queue_at_start = int(enrichment.get("queueAtStart", 0) or 0)
    effective_limit = max(1, int(effective_config.get("fieldJobLimitPerCycle", 1) or 1))
    return max(
        2,
        min(
            30,
            max(int(queue_at_start * 0.05) if queue_at_start > 0 else 0, int(effective_limit * 0.75)),
        ),
    )


def _should_complete_with_deferred_residual_queue(
    *,
    progress: dict[str, Any],
    effective_config: dict[str, int],
    preflight_snapshot: dict[str, Any],
) -> bool:
    if bool(preflight_snapshot.get("healthy", True)):
        return False
    remaining_queue = _remaining_actionable_queue(progress)
    if remaining_queue <= 0:
        return False
    enrichment = ((progress.get("analytics") or {}).get("enrichment") or {})
    residual_threshold = _residual_queue_threshold(progress=progress, effective_config=effective_config)
    if remaining_queue > residual_threshold:
        return False
    crawl_run = progress.get("crawlRun") or {}
    contact_resolution = progress.get("contactResolution") or {}
    if int(crawl_run.get("recordsSeen", 0) or 0) <= 0 and int(contact_resolution.get("processed", 0) or 0) <= 0:
        return False
    return True


def _should_complete_stalled_residual_queue(
    *,
    progress: dict[str, Any],
    effective_config: dict[str, int],
    cycle_state: dict[str, int],
) -> bool:
    remaining_queue = _remaining_actionable_queue(progress)
    if remaining_queue <= 0:
        return False
    enrichment = ((progress.get("analytics") or {}).get("enrichment") or {})
    residual_threshold = _residual_queue_threshold(progress=progress, effective_config=effective_config)
    if remaining_queue > residual_threshold:
        return False
    if int(cycle_state.get("lowProgressCycles", 0) or 0) < 2 and int(cycle_state.get("degradedCycleCount", 0) or 0) < 2:
        return False
    if float(enrichment.get("queueBurnRate", 1.0) or 0.0) > 0.05:
        return False
    crawl_run = progress.get("crawlRun") or {}
    contact_resolution = progress.get("contactResolution") or {}
    if int(crawl_run.get("recordsSeen", 0) or 0) <= 0 and int(contact_resolution.get("processed", 0) or 0) <= 0:
        return False
    return True


def _total_field_jobs(totals: dict[str, Any] | None) -> int:
    if not totals:
        return 0
    return sum(int(totals.get(key, 0) or 0) for key in ("queued", "running", "done", "failed"))


def _update_progress_graph(progress: dict[str, Any], *, active_node: str, graph_run_id: int | None, worker_id: str, runtime_mode: str) -> dict[str, Any]:
    next_progress = _clone_progress(progress)
    next_progress.setdefault("graph", {})
    next_progress["graph"].update(
        {
            "requestGraphRunId": graph_run_id,
            "runtimeMode": runtime_mode,
            "workerId": worker_id,
            "activeNode": active_node,
        }
    )
    return next_progress


def _update_progress_analytics(
    progress: dict[str, Any],
    *,
    source_quality: dict[str, Any] | None = None,
    enrichment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    next_progress = _clone_progress(progress)
    next_progress.setdefault("analytics", {})
    if source_quality is not None:
        next_progress["analytics"]["sourceQuality"] = source_quality
    if enrichment is not None:
        next_progress["analytics"]["enrichment"] = enrichment
    return next_progress

def _current_source_quality(request: FraternityCrawlRequestRecord) -> dict[str, Any]:
    progress_quality = (((request.progress or {}).get("analytics") or {}).get("sourceQuality") or {})
    if progress_quality:
        return {
            "score": float(progress_quality.get("score", 0.0) or 0.0),
            "isWeak": bool(progress_quality.get("isWeak", True)),
            "isBlocked": bool(progress_quality.get("isBlocked", False)),
            "reasons": list(progress_quality.get("reasons") or []),
            "recoveryAttempts": int(progress_quality.get("recoveryAttempts", 0) or 0),
            "recoveredFromUrl": progress_quality.get("recoveredFromUrl"),
            "recoveredToUrl": progress_quality.get("recoveredToUrl"),
            "sourceRejectedCount": int(progress_quality.get("sourceRejectedCount", 0) or 0),
            "sourceRecoveredCount": int(progress_quality.get("sourceRecoveredCount", 0) or 0),
            "zeroChapterPrevented": int(progress_quality.get("zeroChapterPrevented", 0) or 0),
        }
    quality = _evaluate_source_url(request.source_url)
    quality.update(
        {
            "recoveryAttempts": 0,
            "recoveredFromUrl": None,
            "recoveredToUrl": None,
            "sourceRejectedCount": 0,
            "sourceRecoveredCount": 0,
            "zeroChapterPrevented": 0,
        }
    )
    return quality


def _evaluate_source_url(url: str | None) -> dict[str, Any]:
    blocked_hosts = {
        "wikipedia.org",
        "www.wikipedia.org",
        "reddit.com",
        "www.reddit.com",
        "facebook.com",
        "www.facebook.com",
        "instagram.com",
        "www.instagram.com",
        "linkedin.com",
        "www.linkedin.com",
        "x.com",
        "twitter.com",
        "stackoverflow.com",
        "stackexchange.com",
        "github.com",
        "medium.com",
        "quora.com",
        "wiktionary.org",
    }
    positive_markers = ["chapter", "chapters", "chapter-directory", "find-a-chapter", "findachapter", "our-chapters", "locations", "locator", "map", "undergraduate"]
    weak_markers = ["alumni", "alumni-groups", "alumnigroups", "member", "members", "memberhub", "portal", "login", "account", "donate"]
    if not url:
        return {"score": 0.0, "isWeak": True, "isBlocked": False, "reasons": ["missing_url"]}
    try:
        parsed = urlparse(url)
        normalized = f"{parsed.hostname or ''}{parsed.path or ''}".lower()
        score = 0.55
        reasons: list[str] = []
        hostname = (parsed.hostname or "").lower().rstrip(".")
        is_blocked = hostname in blocked_hosts or any(hostname.endswith(f".{blocked}") for blocked in blocked_hosts)
        if is_blocked:
            score -= 0.7
            reasons.append("blocked_host")
        positive_hits = [marker for marker in positive_markers if marker in normalized]
        if positive_hits:
            score += min(0.35, len(positive_hits) * 0.08)
            reasons.extend([f"positive:{marker}" for marker in positive_hits])
        weak_hits = [marker for marker in weak_markers if marker in normalized]
        if weak_hits:
            score -= min(0.75, len(weak_hits) * 0.24)
            reasons.extend([f"weak:{marker}" for marker in weak_hits])
        path = (parsed.path or "").rstrip("/")
        if not path:
            score -= 0.12
            reasons.append("generic_root_path")
        elif len([segment for segment in path.split("/") if segment]) >= 2:
            score += 0.06
            reasons.append("deeper_path")
        bounded_score = max(0.0, min(1.0, score))
        return {
            "score": bounded_score,
            "isWeak": is_blocked or bounded_score < 0.45 or bool(weak_hits),
            "isBlocked": is_blocked,
            "reasons": reasons,
        }
    except Exception:
        return {"score": 0.0, "isWeak": True, "isBlocked": False, "reasons": ["invalid_url"]}


def _build_progress_snapshot(
    *,
    source_url: str | None,
    source_confidence: float | None,
    confidence_tier: str,
    source_provenance: str | None,
    fallback_reason: str | None,
    resolution_trace: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    crawl_run: dict[str, Any] | None,
    field_snapshot: list[dict[str, Any]],
    analytics: dict[str, Any] | None = None,
    graph: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fields = {
        "find_website": {"queued": 0, "running": 0, "done": 0, "failed": 0, "queuedActionable": 0, "queuedDeferred": 0, "doneUpdated": 0, "doneReviewRequired": 0, "doneTerminalNoSignal": 0, "doneProviderDegraded": 0},
        "find_email": {"queued": 0, "running": 0, "done": 0, "failed": 0, "queuedActionable": 0, "queuedDeferred": 0, "doneUpdated": 0, "doneReviewRequired": 0, "doneTerminalNoSignal": 0, "doneProviderDegraded": 0},
        "find_instagram": {"queued": 0, "running": 0, "done": 0, "failed": 0, "queuedActionable": 0, "queuedDeferred": 0, "doneUpdated": 0, "doneReviewRequired": 0, "doneTerminalNoSignal": 0, "doneProviderDegraded": 0},
    }
    for item in field_snapshot:
        fields[item["field"]] = {
            "queued": int(item.get("queued", 0) or 0),
            "running": int(item.get("running", 0) or 0),
            "done": int(item.get("done", 0) or 0),
            "failed": int(item.get("failed", 0) or 0),
            "queuedActionable": int(item.get("queued_actionable", item.get("queued", 0)) or 0),
            "queuedDeferred": int(item.get("queued_deferred", 0) or 0),
            "doneUpdated": int(item.get("done_updated", 0) or 0),
            "doneReviewRequired": int(item.get("done_review_required", 0) or 0),
            "doneTerminalNoSignal": int(item.get("done_terminal_no_signal", 0) or 0),
            "doneProviderDegraded": int(item.get("done_provider_degraded", 0) or 0),
        }
    totals = {
        status: sum(int(field.get(status, 0) or 0) for field in fields.values())
        for status in ("queued", "running", "done", "failed")
    }
    enrichment_analytics = (analytics or {}).get("enrichment") or {}
    queue_triage = dict(enrichment_analytics.get("queueTriage") or {})
    chapter_repair = dict(enrichment_analytics.get("chapterRepair") or {})
    writes_by_field = {
        field_name: int(values.get("doneUpdated", 0) or 0)
        for field_name, values in fields.items()
    }
    return {
        "discovery": {
            "sourceUrl": source_url,
            "sourceConfidence": float(source_confidence or 0.0),
            "confidenceTier": confidence_tier,
            "sourceProvenance": source_provenance,
            "fallbackReason": fallback_reason,
            "resolutionTrace": resolution_trace,
            "candidates": candidates,
        },
        "crawlRun": {
            "id": int(crawl_run["id"]) if crawl_run is not None else None,
            "status": crawl_run.get("status") if crawl_run is not None else None,
            "pagesProcessed": int(crawl_run.get("pages_processed", 0) or 0) if crawl_run is not None else 0,
            "recordsSeen": int(crawl_run.get("records_seen", 0) or 0) if crawl_run is not None else 0,
            "recordsUpserted": int(crawl_run.get("records_upserted", 0) or 0) if crawl_run is not None else 0,
            "reviewItemsCreated": int(crawl_run.get("review_items_created", 0) or 0) if crawl_run is not None else 0,
            "fieldJobsCreated": int(crawl_run.get("field_jobs_created", 0) or 0) if crawl_run is not None else 0,
        },
        "chapterSearch": ((crawl_run or {}).get("extraction_metadata") or {}).get("chapter_search", {}) if crawl_run is not None else {},
        "chapterValidity": ((crawl_run or {}).get("extraction_metadata") or {}).get("chapter_validity", {}) if crawl_run is not None else {},
        "queueTriage": {
            "invalidCancelled": int(queue_triage.get("invalidCancelled", 0) or 0),
            "deferredLongCooldown": int(queue_triage.get("deferredLongCooldown", 0) or 0),
            "repairQueued": int(queue_triage.get("repairQueued", 0) or 0),
            "actionableRetained": int(queue_triage.get("actionableRetained", 0) or 0),
            "sourceInvaliditySaturated": bool(queue_triage.get("sourceInvaliditySaturated", False)),
            "purgedInactiveChapters": int(queue_triage.get("purgedInactiveChapters", 0) or 0),
            "purgedBannedSchoolChapters": int(queue_triage.get("purgedBannedSchoolChapters", 0) or 0),
        },
        "chapterRepair": {
            "queued": int(chapter_repair.get("queued", 0) or 0),
            "running": int(chapter_repair.get("running", 0) or 0),
            "promotedToCanonical": int(chapter_repair.get("promotedToCanonical", 0) or 0),
            "downgradedToProvisional": int(chapter_repair.get("downgradedToProvisional", 0) or 0),
            "confirmedInvalid": int(chapter_repair.get("confirmedInvalid", 0) or 0),
            "repairExhausted": int(chapter_repair.get("repairExhausted", 0) or 0),
            "reconciledHistorical": int(chapter_repair.get("reconciledHistorical", 0) or 0),
        },
        "fields": fields,
        "totals": totals,
        "contactResolution": {
            "queuedActionable": sum(int(field.get("queuedActionable", 0) or 0) for field in fields.values()),
            "queuedDeferred": sum(int(field.get("queuedDeferred", 0) or 0) for field in fields.values()),
            "processed": sum(int(field.get("done", 0) or 0) for field in fields.values()),
            "requeued": int(enrichment_analytics.get("requeuedTotal", 0) or 0),
            "reviewRequired": sum(int(field.get("doneReviewRequired", 0) or 0) for field in fields.values()),
            "terminalNoSignal": sum(int(field.get("doneTerminalNoSignal", 0) or 0) for field in fields.values()),
            "providerDegraded": sum(int(field.get("doneProviderDegraded", 0) or 0) for field in fields.values()),
            "autoWritten": sum(writes_by_field.values()),
            "writesByField": writes_by_field,
            "actionableRemaining": sum(int(field.get("queuedActionable", 0) or 0) for field in fields.values()) + int(totals.get("running", 0) or 0),
            "blockedInvalid": int((((crawl_run or {}).get("extraction_metadata") or {}).get("chapter_validity") or {}).get("contactAdmission", {}).get("blocked_invalid", 0) or 0) + int(queue_triage.get("invalidCancelled", 0) or 0),
            "blockedRepairable": int((((crawl_run or {}).get("extraction_metadata") or {}).get("chapter_validity") or {}).get("contactAdmission", {}).get("blocked_repairable", 0) or 0) + int(queue_triage.get("repairQueued", 0) or 0),
            "reconciledHistorical": int(chapter_repair.get("reconciledHistorical", 0) or 0),
            "rejectionReasonCounts": {},
        },
        "analytics": analytics or {},
        "graph": graph or {},
    }


def _provider_snapshot_payload(
    preflight_snapshot: dict[str, Any] | None,
    payload: dict[str, Any] | None,
) -> tuple[bool, dict[str, Any]]:
    provider_payload = dict(payload or {})
    min_success_rate = float((preflight_snapshot or {}).get("min_success_rate", 0.0) or 0.0)
    attempts = int(provider_payload.get("attempts", 0) or 0)
    successes = int(provider_payload.get("successes", 0) or 0)
    success_rate = float(provider_payload.get("success_rate", 0.0) or 0.0)
    low_signal_count = int(provider_payload.get("low_signal", 0) or 0)
    challenge_count = int(provider_payload.get("challenge_or_anomaly", 0) or 0)
    request_error_count = int(provider_payload.get("request_error", 0) or 0)
    unavailable_count = int(provider_payload.get("unavailable", 0) or 0)

    if attempts <= 0:
        provider_healthy = False
        provider_health_reason = "no_attempts"
    elif successes > 0 and success_rate >= min_success_rate:
        provider_healthy = True
        provider_health_reason = "meets_success_threshold"
    elif low_signal_count >= attempts and successes == 0:
        provider_healthy = False
        provider_health_reason = "low_signal_only"
    elif challenge_count > 0 and successes == 0:
        provider_healthy = False
        provider_health_reason = "challenge_or_anomaly"
    elif request_error_count >= attempts and successes == 0:
        provider_healthy = False
        provider_health_reason = "request_error_only"
    elif unavailable_count >= attempts and successes == 0:
        provider_healthy = False
        provider_health_reason = "provider_unavailable"
    else:
        provider_healthy = False
        provider_health_reason = "below_success_threshold"

    provider_payload["provider_healthy"] = provider_healthy
    provider_payload["provider_health_reason"] = provider_health_reason
    provider_payload["batch_healthy"] = bool((preflight_snapshot or {}).get("healthy", False))
    provider_payload["preflight_min_success_rate"] = min_success_rate
    provider_payload["viable_provider"] = bool(provider_payload.get("healthy", provider_healthy))
    return provider_healthy, provider_payload


def _compute_adaptive_enrichment_config(config: dict[str, Any], progress: dict[str, Any], cycle_state: dict[str, int]) -> dict[str, int]:
    discovered = int(((progress.get("crawlRun") or {}).get("recordsSeen", 0)) or 0)
    queue_size = _remaining_actionable_queue(progress)
    base_workers = max(1, int(config.get("fieldJobWorkers", 1) or 1))
    base_limit = max(1, int(config.get("fieldJobLimitPerCycle", 1) or 1))
    queue_pressure = max(queue_size, discovered)

    effective_workers = base_workers
    effective_limit = base_limit
    adaptive_max_cycles = max(1, int(config.get("maxEnrichmentCycles", 1) or 1))
    pause_ms = max(0, int(config.get("pauseMs", 0) or 0))

    if queue_pressure >= 300:
        effective_workers = max(effective_workers, 10)
        effective_limit = max(effective_limit, 100)
        adaptive_max_cycles = max(adaptive_max_cycles, 72)
    elif queue_pressure >= 150:
        effective_workers = max(effective_workers, 8)
        effective_limit = max(effective_limit, 80)
        adaptive_max_cycles = max(adaptive_max_cycles, 48)
    elif queue_pressure >= 60:
        effective_workers = max(effective_workers, 6)
        effective_limit = max(effective_limit, 60)
        adaptive_max_cycles = max(adaptive_max_cycles, 32)

    if int(cycle_state.get("lowProgressCycles", 0) or 0) >= 2:
        effective_workers = max(1, effective_workers - 1)
        effective_limit = max(20, int(effective_limit * 0.8))
        adaptive_max_cycles = min(96, adaptive_max_cycles + 6)

    if int(cycle_state.get("degradedCycleCount", 0) or 0) >= 2:
        effective_workers = max(1, min(effective_workers, 4))
        effective_limit = max(20, min(effective_limit, 50))
        adaptive_max_cycles = min(96, adaptive_max_cycles + 4)

    return {
        "fieldJobWorkers": effective_workers,
        "fieldJobLimitPerCycle": effective_limit,
        "maxEnrichmentCycles": min(96, adaptive_max_cycles),
        "pauseMs": pause_ms,
    }
