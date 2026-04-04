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
        return {
            "processed": int(result.get("processed", 0) or 0),
            "requeued": int(result.get("requeued", 0) or 0),
            "failed_terminal": int(result.get("failed_terminal", 0) or 0),
            "runtime_fallback_count": int(result.get("runtime_fallback_count", 0) or 0),
            "runtime_mode_used": str(result.get("runtime_mode_used") or self._runtime_mode),
        }

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
        runtime_modes: set[str] = set()
        for result in state.get("chunk_results") or []:
            processed += int(result.get("processed", 0) or 0)
            requeued += int(result.get("requeued", 0) or 0)
            failed_terminal += int(result.get("failed_terminal", 0) or 0)
            runtime_fallback_count += int(result.get("runtime_fallback_count", 0) or 0)
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
        )
        return {
            "processed": processed,
            "requeued": requeued,
            "failed_terminal": failed_terminal,
            "runtime_fallback_count": runtime_fallback_count,
            "runtime_mode_used": next(iter(runtime_modes)) if len(runtime_modes) == 1 else "mixed",
        }

    def _finalize(self, state: FieldJobSupervisorState) -> dict[str, Any]:
        return {
            "processed": int(state.get("processed", 0) or 0),
            "requeued": int(state.get("requeued", 0) or 0),
            "failed_terminal": int(state.get("failed_terminal", 0) or 0),
            "runtime_fallback_count": int(state.get("runtime_fallback_count", 0) or 0),
            "runtime_mode_used": str(state.get("runtime_mode_used") or state.get("runtime_mode") or "legacy"),
        }
