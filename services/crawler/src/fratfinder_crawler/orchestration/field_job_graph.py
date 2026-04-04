from __future__ import annotations

import logging
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, TypedDict

from langgraph.graph import END, StateGraph

from fratfinder_crawler.field_jobs import FieldJobEngine, FieldJobResult, RetryableJobError
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import FIELD_JOB_FIND_WEBSITE, FieldJob, FieldJobDecision

LOGGER = logging.getLogger(__name__)


class FieldJobGraphState(TypedDict, total=False):
    graph_run_id: int
    worker_id: str
    runtime_mode: str
    graph_durability: str
    source_slug: str | None
    field_name_filter: str | None
    limit: int
    iteration: int

    job: FieldJob
    action: str
    terminal_reason: str
    error: str

    resolution_result: FieldJobResult
    retry_error_message: str
    retry_reason_code: str
    retry_backoff_seconds: int | None
    retry_preserve_attempt: bool
    retry_low_signal: bool
    unexpected_error: str

    decision: FieldJobDecision
    decision_metadata: dict[str, Any]
    payload_patch: dict[str, Any]

    processed_delta: int
    requeued_delta: int
    failed_terminal_delta: int


class FieldJobGraphRuntime:
    def __init__(
        self,
        *,
        repository,
        engine: FieldJobEngine,
        worker_id: str,
        runtime_mode: str,
        graph_durability: str,
        source_slug: str | None,
        field_name: str | None,
    ):
        self._repository = repository
        self._engine = engine
        self._worker_id = worker_id
        self._runtime_mode = runtime_mode
        self._graph_durability = graph_durability
        self._source_slug = source_slug
        self._field_name = field_name
        self._graph = self._build_graph()

    def process(self, *, limit: int) -> dict[str, int]:
        run_id = self._repository.start_field_job_graph_run(
            worker_id=self._worker_id,
            runtime_mode=self._runtime_mode,
            source_slug=self._source_slug,
            field_name=self._field_name,
            limit=limit,
            metadata={"graphDurability": self._graph_durability},
        )

        aggregate = {"processed": 0, "requeued": 0, "failed_terminal": 0}
        status = "succeeded"
        error_message: str | None = None

        try:
            for iteration in range(max(0, limit)):
                initial_state: FieldJobGraphState = {
                    "graph_run_id": run_id,
                    "worker_id": self._worker_id,
                    "runtime_mode": self._runtime_mode,
                    "graph_durability": self._graph_durability,
                    "source_slug": self._source_slug,
                    "field_name_filter": self._field_name,
                    "limit": limit,
                    "iteration": iteration + 1,
                }
                final_state = self._graph.invoke(initial_state)
                aggregate["processed"] += int(final_state.get("processed_delta", 0) or 0)
                aggregate["requeued"] += int(final_state.get("requeued_delta", 0) or 0)
                aggregate["failed_terminal"] += int(final_state.get("failed_terminal_delta", 0) or 0)

                if final_state.get("terminal_reason") == "no_job":
                    break

                if final_state.get("error"):
                    status = "partial"
                    error_message = str(final_state.get("error"))

        except Exception as exc:  # pragma: no cover - guardrail path
            status = "failed"
            error_message = str(exc)
            log_event(
                LOGGER,
                "field_job_graph_run_failed",
                level=logging.ERROR,
                runtime_mode=self._runtime_mode,
                worker_id=self._worker_id,
                error=error_message,
            )
        finally:
            self._repository.finish_field_job_graph_run(
                run_id,
                status=status,
                summary={
                    "processed": aggregate["processed"],
                    "requeued": aggregate["requeued"],
                    "failedTerminal": aggregate["failed_terminal"],
                    "graphDurability": self._graph_durability,
                },
                error_message=error_message,
            )

        return aggregate

    def _build_graph(self):
        graph = StateGraph(FieldJobGraphState)
        graph.add_node("load_job", self._instrument("load_job", self._load_job, phase="supervisor"))
        graph.add_node("evaluate_preconditions", self._instrument("evaluate_preconditions", self._evaluate_preconditions, phase="execution"))
        graph.add_node("resolve_job", self._instrument("resolve_job", self._resolve_job, phase="execution"))
        graph.add_node("decide_outcome", self._instrument("decide_outcome", self._decide_outcome, phase="decision"))
        graph.add_node("persist_outcome", self._instrument("persist_outcome", self._persist_outcome, phase="persistence"))
        graph.add_node("finalize", self._instrument("finalize", self._finalize, phase="finalize"))

        graph.set_entry_point("load_job")
        graph.add_conditional_edges("load_job", self._route_after_load, {"finalize": "finalize", "continue": "evaluate_preconditions"})
        graph.add_edge("evaluate_preconditions", "resolve_job")
        graph.add_edge("resolve_job", "decide_outcome")
        graph.add_edge("decide_outcome", "persist_outcome")
        graph.add_edge("persist_outcome", "finalize")
        graph.add_edge("finalize", END)
        return graph.compile()

    def _instrument(
        self,
        node_name: str,
        fn: Callable[[FieldJobGraphState], dict[str, Any]],
        *,
        phase: str,
    ) -> Callable[[FieldJobGraphState], dict[str, Any]]:
        def wrapped(state: FieldJobGraphState) -> dict[str, Any]:
            started = time.perf_counter()
            status = "ok"
            updates: dict[str, Any]
            try:
                updates = fn(state)
            except Exception as exc:  # pragma: no cover - guardrail path
                status = "error"
                updates = {
                    "error": str(exc),
                    "action": "fail_terminal",
                    "unexpected_error": str(exc),
                    "terminal_reason": "node_failure",
                }

            latency_ms = int((time.perf_counter() - started) * 1000)
            run_id = state.get("graph_run_id")
            job = state.get("job")
            attempt = job.attempts if job is not None else None
            job_id = job.id if job is not None else None
            if run_id is not None:
                diagnostics = {
                    "action": updates.get("action"),
                    "terminalReason": updates.get("terminal_reason"),
                    "error": updates.get("error"),
                    "reasonCode": updates.get("retry_reason_code"),
                }
                metrics_delta = {
                    "processed": int(updates.get("processed_delta", 0) or 0),
                    "requeued": int(updates.get("requeued_delta", 0) or 0),
                    "failedTerminal": int(updates.get("failed_terminal_delta", 0) or 0),
                }
                self._repository.append_field_job_graph_event(
                    run_id=run_id,
                    node_name=node_name,
                    phase=phase,
                    status=status,
                    latency_ms=latency_ms,
                    job_id=job_id,
                    attempt=attempt,
                    metrics_delta=metrics_delta,
                    diagnostics=diagnostics,
                )
                self._persist_checkpoint_if_needed(node_name=node_name, state=state, updates=updates)
            return updates

        return wrapped

    def _persist_checkpoint_if_needed(self, *, node_name: str, state: FieldJobGraphState, updates: dict[str, Any]) -> None:
        if self._graph_durability == "exit":
            return
        if node_name not in {"load_job", "decide_outcome", "persist_outcome"} and self._graph_durability == "async":
            return
        combined = dict(state)
        combined.update(updates)
        job = combined.get("job")
        if not isinstance(job, FieldJob):
            return
        snapshot = {
            "node": node_name,
            "runtimeMode": combined.get("runtime_mode"),
            "workerId": combined.get("worker_id"),
            "iteration": combined.get("iteration"),
            "sourceSlug": combined.get("source_slug"),
            "fieldName": combined.get("field_name_filter"),
            "job": {
                "id": job.id,
                "chapterSlug": job.chapter_slug,
                "fieldName": job.field_name,
                "attempts": job.attempts,
                "maxAttempts": job.max_attempts,
            },
            "action": combined.get("action"),
            "terminalReason": combined.get("terminal_reason"),
            "error": combined.get("error"),
            "decision": self._to_json_safe(combined.get("decision")),
            "decisionMetadata": self._to_json_safe(combined.get("decision_metadata")),
        }
        self._repository.upsert_field_job_graph_checkpoint(
            run_id=int(combined["graph_run_id"]),
            job_id=job.id,
            attempt=max(1, int(job.attempts)),
            node_name=node_name,
            state=snapshot,
        )

    def _route_after_load(self, state: FieldJobGraphState) -> str:
        if state.get("terminal_reason") == "no_job":
            return "finalize"
        return "continue"

    def _load_job(self, state: FieldJobGraphState) -> dict[str, Any]:
        job = self._repository.claim_next_field_job(
            self._worker_id,
            source_slug=self._source_slug,
            field_name=self._field_name,
            require_confident_website_for_email=False,
        )
        if job is None:
            return {"terminal_reason": "no_job", "action": "none"}

        log_event(
            LOGGER,
            "field_job_graph_claimed",
            runtime_mode=self._runtime_mode,
            worker_id=self._worker_id,
            field_job_id=job.id,
            chapter_slug=job.chapter_slug,
            field_name=job.field_name,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
        )
        return {"job": job, "terminal_reason": None, "error": None}

    def _evaluate_preconditions(self, state: FieldJobGraphState) -> dict[str, Any]:
        if state.get("job") is None:
            return {"terminal_reason": "no_job"}
        return {}

    def _resolve_job(self, state: FieldJobGraphState) -> dict[str, Any]:
        job = state["job"]
        try:
            result = self._engine.process_claimed_job(job)
            return {"resolution_result": result}
        except RetryableJobError as exc:
            return {
                "retry_error_message": str(exc),
                "retry_reason_code": str(exc.reason_code or "retryable"),
                "retry_backoff_seconds": exc.backoff_seconds,
                "retry_preserve_attempt": bool(exc.preserve_attempt),
                "retry_low_signal": bool(exc.low_signal),
            }
        except Exception as exc:  # pragma: no cover - guardrail path
            return {"unexpected_error": str(exc), "error": str(exc)}

    def _decide_outcome(self, state: FieldJobGraphState) -> dict[str, Any]:
        job = state.get("job")
        if job is None:
            return {"action": "none", "terminal_reason": "no_job"}

        decision = FieldJobDecision(status="noop", reason_codes=[])
        decision_metadata: dict[str, Any] = {}
        payload_patch: dict[str, Any] = {}

        if state.get("resolution_result") is not None:
            result = state["resolution_result"]
            confidence = self._extract_confidence(result)
            candidate_value = self._extract_candidate_value(result)
            candidate_kind = self._candidate_kind_for_field(job.field_name)
            decision = FieldJobDecision(
                status="complete",
                confidence=confidence,
                candidate_kind=candidate_kind,
                candidate_value=candidate_value,
                reason_codes=["resolved"],
                write_allowed=True,
                requires_review=result.review_item is not None,
            )
            decision_metadata = {
                "fieldStates": result.field_state_updates,
                "chapterUpdates": result.chapter_updates,
                "hasReviewItem": result.review_item is not None,
            }
            action = "complete"
        elif state.get("retry_error_message"):
            reason_code = str(state.get("retry_reason_code") or "retryable")
            preserve_attempt = bool(state.get("retry_preserve_attempt", False))
            low_signal = bool(state.get("retry_low_signal", False))
            backoff_seconds = state.get("retry_backoff_seconds")

            retry_limit = min(job.max_attempts, 2) if low_signal and job.field_name == FIELD_JOB_FIND_WEBSITE else job.max_attempts
            should_fail_terminal = (not preserve_attempt) and (job.attempts >= retry_limit)
            action = "fail_terminal" if should_fail_terminal else "requeue"

            decision = FieldJobDecision(
                status=action,
                confidence=None,
                candidate_kind=None,
                candidate_value=None,
                reason_codes=[reason_code],
                write_allowed=False,
                requires_review=False,
            )
            if action == "requeue":
                resolved_backoff = int(backoff_seconds) if isinstance(backoff_seconds, int) else self._engine._base_backoff_seconds * (2 ** (job.attempts - 1))
                retry_exc = RetryableJobError(
                    str(state["retry_error_message"]),
                    backoff_seconds=resolved_backoff,
                    preserve_attempt=preserve_attempt,
                    low_signal=low_signal,
                    reason_code=reason_code,
                )
                payload_patch = self._engine._build_requeue_payload_patch(job, retry_exc, resolved_backoff)
                decision_metadata = {
                    "reasonCode": reason_code,
                    "preserveAttempt": preserve_attempt,
                    "backoffSeconds": resolved_backoff,
                    "payloadPatch": payload_patch,
                }
            else:
                decision_metadata = {
                    "reasonCode": reason_code,
                    "retryLimit": retry_limit,
                    "attempts": job.attempts,
                }
        else:
            action = "fail_terminal"
            decision = FieldJobDecision(
                status="fail_terminal",
                confidence=None,
                candidate_kind=None,
                candidate_value=None,
                reason_codes=["unexpected_failure"],
                write_allowed=False,
                requires_review=False,
            )
            decision_metadata = {"error": state.get("unexpected_error") or state.get("error")}

        self._repository.insert_field_job_graph_decision(
            run_id=state["graph_run_id"],
            job_id=job.id,
            attempt=max(1, int(job.attempts)),
            field_name=job.field_name,
            decision=decision,
            metadata=decision_metadata,
        )

        return {
            "action": action,
            "decision": decision,
            "decision_metadata": decision_metadata,
            "payload_patch": payload_patch,
        }

    def _persist_outcome(self, state: FieldJobGraphState) -> dict[str, Any]:
        job = state.get("job")
        if job is None:
            return {"terminal_reason": "no_job"}

        action = str(state.get("action") or "none")
        processed_delta = 0
        requeued_delta = 0
        failed_terminal_delta = 0

        if action == "complete":
            result = state["resolution_result"]
            if result.review_item is not None:
                self._repository.create_field_job_review_item(job, result.review_item)
            self._repository.complete_field_job(
                job,
                result.chapter_updates,
                result.completed_payload,
                result.field_state_updates,
                result.provenance_records,
            )
            processed_delta = 1
        elif action == "requeue":
            backoff_seconds = int(state.get("decision_metadata", {}).get("backoffSeconds") or self._engine._base_backoff_seconds)
            self._repository.requeue_field_job(
                job,
                str(state.get("retry_error_message") or "retryable failure"),
                backoff_seconds,
                preserve_attempt=bool(state.get("retry_preserve_attempt", False)),
                payload_patch=state.get("payload_patch") or {},
            )
            requeued_delta = 1
        elif action == "fail_terminal":
            error_message = str(state.get("retry_error_message") or state.get("unexpected_error") or state.get("error") or "terminal failure")
            self._repository.fail_field_job_terminal(job, error_message)
            failed_terminal_delta = 1

        return {
            "processed_delta": processed_delta,
            "requeued_delta": requeued_delta,
            "failed_terminal_delta": failed_terminal_delta,
            "terminal_reason": "job_terminalized",
        }

    def _finalize(self, state: FieldJobGraphState) -> dict[str, Any]:
        return {
            "processed_delta": int(state.get("processed_delta", 0) or 0),
            "requeued_delta": int(state.get("requeued_delta", 0) or 0),
            "failed_terminal_delta": int(state.get("failed_terminal_delta", 0) or 0),
        }

    def _extract_confidence(self, result: FieldJobResult) -> float | None:
        value = result.completed_payload.get("confidence")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _extract_candidate_value(self, result: FieldJobResult) -> str | None:
        for key in ("website_url", "contact_email", "instagram_url", "value"):
            value = result.completed_payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for key in ("website_url", "contact_email", "instagram_url"):
            value = result.chapter_updates.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _candidate_kind_for_field(self, field_name: str) -> str | None:
        if field_name == "find_website":
            return "website"
        if field_name == "find_email":
            return "email"
        if field_name == "find_instagram":
            return "instagram"
        return None

    def _to_json_safe(self, value: Any) -> Any:
        if value is None:
            return None
        if is_dataclass(value):
            return {key: self._to_json_safe(item) for key, item in asdict(value).items()}
        if isinstance(value, dict):
            return {str(key): self._to_json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_json_safe(item) for item in value]
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
