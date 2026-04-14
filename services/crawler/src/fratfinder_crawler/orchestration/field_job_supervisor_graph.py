from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypedDict

from langgraph.graph import END, StateGraph

from fratfinder_crawler.logging_utils import log_event

LOGGER = logging.getLogger(__name__)


class FieldJobSupervisorState(TypedDict, total=False):
    worker_limits: list[int]
    runtime_mode: str
    graph_durability: str
    source_slug: str | None
    field_name: str | None
    degraded_mode: bool

    chunk_specs: list[dict[str, int]]
    chunk_results: list[dict[str, Any]]

    workers_executed: int
    processed: int
    requeued: int
    failed_terminal: int
    runtime_fallback_count: int
    runtime_mode_used: str


class FieldJobSupervisorGraphRuntime:
    def __init__(
        self,
        *,
        worker_limits: list[int],
        runtime_mode: str,
        graph_durability: str,
        source_slug: str | None,
        field_name: str | None,
        degraded_mode: bool,
        chunk_processor: Callable[..., dict[str, Any]],
    ):
        self._worker_limits = [max(0, int(limit)) for limit in worker_limits if int(limit) > 0]
        self._runtime_mode = runtime_mode
        self._graph_durability = graph_durability
        self._source_slug = source_slug
        self._field_name = field_name
        self._degraded_mode = degraded_mode
        self._chunk_processor = chunk_processor
        self._graph = self._build_graph()

    def run(self) -> dict[str, Any]:
        state: FieldJobSupervisorState = {
            "worker_limits": self._worker_limits,
            "runtime_mode": self._runtime_mode,
            "graph_durability": self._graph_durability,
            "source_slug": self._source_slug,
            "field_name": self._field_name,
            "degraded_mode": self._degraded_mode,
        }
        result = self._graph.invoke(state)
        aggregate = {
            "processed": int(result.get("processed", 0) or 0),
            "requeued": int(result.get("requeued", 0) or 0),
            "failed_terminal": int(result.get("failed_terminal", 0) or 0),
            "runtime_fallback_count": int(result.get("runtime_fallback_count", 0) or 0),
            "runtime_mode_used": str(result.get("runtime_mode_used") or self._runtime_mode),
        }
        for key in (
            "provider_degraded_deferred",
            "dependency_wait_deferred",
            "supporting_page_resolved",
            "supporting_page_contact_resolved",
            "external_search_contact_resolved",
            "enrichment_observations_logged",
            "mid_batch_provider_rechecks",
            "mid_batch_provider_reorders",
        ):
            aggregate[key] = int(result.get(key, 0) or 0)
        for key in ("preflight_probe_queries", "chapter_search_queries"):
            aggregate[key] = list(result.get(key) or [])
        return aggregate

    def _build_graph(self):
        graph = StateGraph(FieldJobSupervisorState)
        graph.add_node("prepare_chunks", self._prepare_chunks)
        graph.add_node("run_chunks", self._run_chunks)
        graph.add_node("aggregate_results", self._aggregate_results)
        graph.add_node("finalize", self._finalize)

        graph.set_entry_point("prepare_chunks")
        graph.add_conditional_edges("prepare_chunks", self._route_after_prepare, {"run": "run_chunks", "finalize": "finalize"})
        graph.add_edge("run_chunks", "aggregate_results")
        graph.add_edge("aggregate_results", "finalize")
        graph.add_edge("finalize", END)
        return graph.compile()

    def _prepare_chunks(self, state: FieldJobSupervisorState) -> dict[str, object]:
        limits = list(state.get("worker_limits") or [])
        chunk_specs = [
            {"limit": int(limit), "worker_index": index, "total_workers": len(limits)}
            for index, limit in enumerate(limits, start=1)
            if int(limit) > 0
        ]
        log_event(
            LOGGER,
            "field_job_supervisor_chunks_prepared",
            runtime_mode=state.get("runtime_mode"),
            graph_durability=state.get("graph_durability"),
            workers_requested=len(limits),
            workers_prepared=len(chunk_specs),
            source_slug=state.get("source_slug"),
            field_name=state.get("field_name"),
        )
        return {"chunk_specs": chunk_specs, "workers_executed": len(chunk_specs)}

    def _route_after_prepare(self, state: FieldJobSupervisorState) -> str:
        specs = state.get("chunk_specs") or []
        return "run" if len(specs) > 0 else "finalize"

    def _run_chunks(self, state: FieldJobSupervisorState) -> dict[str, object]:
        specs = list(state.get("chunk_specs") or [])
        if not specs:
            return {"chunk_results": []}

        if len(specs) == 1:
            spec = specs[0]
            result = self._chunk_processor(
                limit=int(spec["limit"]),
                source_slug=state.get("source_slug"),
                field_name=state.get("field_name"),
                worker_index=int(spec["worker_index"]),
                total_workers=int(spec["total_workers"]),
                degraded_mode=bool(state.get("degraded_mode", False)),
                runtime_mode=str(state.get("runtime_mode") or "legacy"),
                graph_durability=str(state.get("graph_durability") or "sync"),
            )
            return {"chunk_results": [result]}

        with ThreadPoolExecutor(max_workers=len(specs), thread_name_prefix="field-job-supervisor") as executor:
            futures = [
                executor.submit(
                    self._chunk_processor,
                    int(spec["limit"]),
                    state.get("source_slug"),
                    state.get("field_name"),
                    int(spec["worker_index"]),
                    int(spec["total_workers"]),
                    bool(state.get("degraded_mode", False)),
                    str(state.get("runtime_mode") or "legacy"),
                    str(state.get("graph_durability") or "sync"),
                )
                for spec in specs
            ]
        return {"chunk_results": [future.result() for future in futures]}

    def _aggregate_results(self, state: FieldJobSupervisorState) -> dict[str, Any]:
        processed = 0
        requeued = 0
        failed_terminal = 0
        runtime_fallback_count = 0
        provider_degraded_deferred = 0
        dependency_wait_deferred = 0
        supporting_page_resolved = 0
        supporting_page_contact_resolved = 0
        external_search_contact_resolved = 0
        enrichment_observations_logged = 0
        mid_batch_provider_rechecks = 0
        mid_batch_provider_reorders = 0
        preflight_probe_queries: list[str] = []
        chapter_search_queries: list[str] = []
        provider_window_state: dict[str, Any] = {}
        runtime_modes: set[str] = set()
        for result in state.get("chunk_results") or []:
            processed += int(result.get("processed", 0) or 0)
            requeued += int(result.get("requeued", 0) or 0)
            failed_terminal += int(result.get("failed_terminal", 0) or 0)
            runtime_fallback_count += int(result.get("runtime_fallback_count", 0) or 0)
            provider_degraded_deferred += int(result.get("provider_degraded_deferred", 0) or 0)
            dependency_wait_deferred += int(result.get("dependency_wait_deferred", 0) or 0)
            supporting_page_resolved += int(result.get("supporting_page_resolved", 0) or 0)
            supporting_page_contact_resolved += int(result.get("supporting_page_contact_resolved", 0) or 0)
            external_search_contact_resolved += int(result.get("external_search_contact_resolved", 0) or 0)
            enrichment_observations_logged += int(result.get("enrichment_observations_logged", 0) or 0)
            mid_batch_provider_rechecks += int(result.get("mid_batch_provider_rechecks", 0) or 0)
            mid_batch_provider_reorders += int(result.get("mid_batch_provider_reorders", 0) or 0)
            for query in result.get("preflight_probe_queries") or []:
                if query not in preflight_probe_queries:
                    preflight_probe_queries.append(str(query))
            for query in result.get("chapter_search_queries") or []:
                if query not in chapter_search_queries:
                    chapter_search_queries.append(str(query))
            if isinstance(result.get("provider_window_state"), dict):
                provider_window_state = dict(result["provider_window_state"])
            runtime_modes.add(str(result.get("runtime_mode_used") or state.get("runtime_mode") or "legacy"))

        log_event(
            LOGGER,
            "field_job_supervisor_chunks_aggregated",
            runtime_mode=state.get("runtime_mode"),
            graph_durability=state.get("graph_durability"),
            workers_executed=state.get("workers_executed", 0),
            processed=processed,
            requeued=requeued,
            failed_terminal=failed_terminal,
            runtime_fallback_count=runtime_fallback_count,
            runtime_mode_used=(next(iter(runtime_modes)) if len(runtime_modes) == 1 else "mixed"),
            provider_degraded_deferred=provider_degraded_deferred,
            dependency_wait_deferred=dependency_wait_deferred,
            supporting_page_resolved=supporting_page_resolved,
            supporting_page_contact_resolved=supporting_page_contact_resolved,
            external_search_contact_resolved=external_search_contact_resolved,
            enrichment_observations_logged=enrichment_observations_logged,
            mid_batch_provider_rechecks=mid_batch_provider_rechecks,
            mid_batch_provider_reorders=mid_batch_provider_reorders,
            provider_window_state=provider_window_state,
        )
        return {
            "processed": processed,
            "requeued": requeued,
            "failed_terminal": failed_terminal,
            "runtime_fallback_count": runtime_fallback_count,
            "runtime_mode_used": next(iter(runtime_modes)) if len(runtime_modes) == 1 else "mixed",
            "provider_degraded_deferred": provider_degraded_deferred,
            "dependency_wait_deferred": dependency_wait_deferred,
            "supporting_page_resolved": supporting_page_resolved,
            "supporting_page_contact_resolved": supporting_page_contact_resolved,
            "external_search_contact_resolved": external_search_contact_resolved,
            "enrichment_observations_logged": enrichment_observations_logged,
            "mid_batch_provider_rechecks": mid_batch_provider_rechecks,
            "mid_batch_provider_reorders": mid_batch_provider_reorders,
            "preflight_probe_queries": preflight_probe_queries,
            "chapter_search_queries": chapter_search_queries,
            "provider_window_state": provider_window_state,
        }

    def _finalize(self, state: FieldJobSupervisorState) -> dict[str, Any]:
        result = {
            "processed": int(state.get("processed", 0) or 0),
            "requeued": int(state.get("requeued", 0) or 0),
            "failed_terminal": int(state.get("failed_terminal", 0) or 0),
            "runtime_fallback_count": int(state.get("runtime_fallback_count", 0) or 0),
            "runtime_mode_used": str(state.get("runtime_mode_used") or state.get("runtime_mode") or "legacy"),
        }
        for key in (
            "provider_degraded_deferred",
            "dependency_wait_deferred",
            "supporting_page_resolved",
            "supporting_page_contact_resolved",
            "external_search_contact_resolved",
            "enrichment_observations_logged",
            "mid_batch_provider_rechecks",
            "mid_batch_provider_reorders",
        ):
            result[key] = int(state.get(key, 0) or 0)
        result["preflight_probe_queries"] = list(state.get("preflight_probe_queries") or [])
        result["chapter_search_queries"] = list(state.get("chapter_search_queries") or [])
        result["provider_window_state"] = dict(state.get("provider_window_state") or {})
        return result
