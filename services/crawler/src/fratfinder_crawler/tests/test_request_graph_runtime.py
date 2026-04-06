from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from fratfinder_crawler.models import FraternityCrawlRequestRecord
from fratfinder_crawler.orchestration.request_graph import RequestSupervisorGraphRuntime


def _request(
    *,
    request_id: str = "req-1",
    source_slug: str | None = "alpha-main",
    source_url: str | None = "https://alpha.org/chapters",
    source_confidence: float | None = 0.9,
    progress: dict | None = None,
) -> FraternityCrawlRequestRecord:
    return FraternityCrawlRequestRecord(
        id=request_id,
        fraternity_name="Alpha Beta",
        fraternity_slug="alpha-beta",
        source_slug=source_slug,
        source_url=source_url,
        source_confidence=source_confidence,
        status="queued",
        stage="discovery",
        scheduled_for="2026-04-04T00:00:00+00:00",
        started_at=None,
        finished_at=None,
        priority=0,
        config={
            "fieldJobWorkers": 2,
            "fieldJobLimitPerCycle": 10,
            "maxEnrichmentCycles": 4,
            "pauseMs": 0,
        },
        progress=progress or {
            "discovery": {
                "sourceUrl": source_url,
                "sourceConfidence": source_confidence or 0.0,
                "confidenceTier": "high" if (source_confidence or 0.0) >= 0.8 else "medium",
                "sourceProvenance": "verified_registry",
                "fallbackReason": None,
                "resolutionTrace": [],
                "candidates": [],
            }
        },
        last_error=None,
        created_at="2026-04-04T00:00:00+00:00",
        updated_at="2026-04-04T00:00:00+00:00",
    )


class _FakeRequestRepository:
    def __init__(self, request: FraternityCrawlRequestRecord):
        self.request = request
        self.events: list[tuple[str, str]] = []
        self.graph_events: list[dict] = []
        self.checkpoints: list[dict] = []
        self.provider_health_snapshots: list[dict] = []
        self.run_status: dict[int, dict] = {}
        self.latest_crawl_runs: dict[str, dict] = {}
        self.field_snapshots: dict[str, list[dict]] = {}
        self._run_id = 100

    def start_request_graph_run(self, **_: object) -> int:
        self._run_id += 1
        self.run_status[self._run_id] = {"status": "running"}
        return self._run_id

    def finish_request_graph_run(self, run_id: int, **kwargs: object) -> None:
        self.run_status[run_id] = dict(kwargs)

    def get_request(self, request_id: str) -> FraternityCrawlRequestRecord | None:
        return self.request if self.request.id == request_id else None

    def update_request(
        self,
        request_id: str,
        *,
        source_slug: str | None = None,
        source_url: str | None = None,
        source_confidence: float | None = None,
        status: str | None = None,
        stage: str | None = None,
        scheduled_for: str | None = None,
        priority: int | None = None,
        config: dict | None = None,
        progress: dict | None = None,
        last_error: str | None = None,
        started_at_now: bool = False,
        finished_at_now: bool = False,
        clear_finished_at: bool = False,
    ) -> None:
        assert request_id == self.request.id
        updates: dict[str, object] = {}
        if source_slug is not None:
            updates["source_slug"] = source_slug
        if source_url is not None:
            updates["source_url"] = source_url
        if source_confidence is not None:
            updates["source_confidence"] = source_confidence
        if status is not None:
            updates["status"] = status
        if stage is not None:
            updates["stage"] = stage
        if scheduled_for is not None:
            updates["scheduled_for"] = scheduled_for
        if priority is not None:
            updates["priority"] = priority
        if config is not None:
            updates["config"] = config
        if progress is not None:
            updates["progress"] = progress
        if last_error is not None:
            updates["last_error"] = last_error
        if started_at_now:
            updates["started_at"] = "2026-04-04T01:00:00+00:00"
        if finished_at_now:
            updates["finished_at"] = "2026-04-04T02:00:00+00:00"
        if clear_finished_at:
            updates["finished_at"] = None
        self.request = replace(self.request, **updates)

    def append_request_event(self, request_id: str, event_type: str, message: str, payload: dict | None = None) -> None:
        assert request_id == self.request.id
        self.events.append((event_type, message))

    def append_request_graph_event(self, **kwargs: object) -> None:
        self.graph_events.append(dict(kwargs))

    def upsert_request_graph_checkpoint(self, **kwargs: object) -> None:
        self.checkpoints.append(dict(kwargs))

    def touch_request_graph_run(self, run_id: int, **kwargs: object) -> None:
        self.run_status.setdefault(run_id, {}).update(kwargs)

    def get_latest_crawl_run_for_source(
        self,
        source_slug: str,
        *,
        started_after: str | None = None,
        exclude_run_id: int | None = None,
    ) -> dict | None:
        _ = started_after, exclude_run_id
        return self.latest_crawl_runs.get(source_slug)

    def get_source_field_job_snapshot(self, source_slug: str) -> list[dict]:
        return list(self.field_snapshots.get(source_slug, []))

    def insert_provider_health_snapshot(self, **kwargs: object) -> None:
        self.provider_health_snapshots.append(dict(kwargs))


class _FakeCrawlerRepository:
    def __init__(self):
        self.upserted_fraternities: list[dict] = []
        self.upserted_sources: list[dict] = []

    def upsert_fraternity(self, slug: str, name: str, nic_affiliated: bool = True) -> tuple[str, str]:
        self.upserted_fraternities.append({"slug": slug, "name": name, "nicAffiliated": nic_affiliated})
        return "frat-1", slug

    def upsert_source(self, **kwargs: object) -> tuple[str, str]:
        self.upserted_sources.append(dict(kwargs))
        return "source-1", str(kwargs["slug"])


def _build_runtime(
    request_repository: _FakeRequestRepository,
    crawler_repository: _FakeCrawlerRepository,
    *,
    discover_source=None,
    run_crawl=None,
    process_field_jobs=None,
    search_preflight=None,
    free_recovery_attempts: int = 3,
) -> RequestSupervisorGraphRuntime:
    return RequestSupervisorGraphRuntime(
        request_repository=request_repository,
        crawler_repository=crawler_repository,
        worker_id="test-worker",
        runtime_mode="v3_request_supervisor",
        crawl_runtime_mode="adaptive_assisted",
        field_job_runtime_mode="langgraph_primary",
        field_job_graph_durability="sync",
        free_recovery_attempts=free_recovery_attempts,
        discover_source=discover_source or (lambda fraternity_name: {"fraternity_name": fraternity_name, "selected_url": None}),
        run_crawl=run_crawl or (lambda **_: {"runtime_mode": "adaptive_assisted"}),
        process_field_jobs=process_field_jobs or (lambda **_: {"processed": 0, "requeued": 0, "failed_terminal": 0}),
        search_preflight=search_preflight or (lambda: {"healthy": True, "provider_health": {}}),
        logger=SimpleNamespace(),
    )


def test_request_graph_completes_without_enrichment_queue():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_email", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 501,
            "status": "succeeded",
            "pages_processed": 8,
            "records_seen": 5,
            "records_upserted": 5,
            "review_items_created": 0,
            "field_jobs_created": 0,
        }
        return {"runtime_mode": "adaptive_assisted"}

    runtime = _build_runtime(request_repository, _FakeCrawlerRepository(), run_crawl=run_crawl)
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert summary["terminalReason"] == "completed"
    assert request_repository.request.stage == "completed"
    assert request_repository.request.status == "succeeded"
    assert request_repository.request.progress["crawlRun"]["recordsSeen"] == 5
    assert any(event_type == "request_completed" for event_type, _ in request_repository.events)


def test_request_graph_pauses_when_recovery_budget_is_exhausted():
    request = _request(
        source_slug=None,
        source_url="https://facebook.com/not-valid",
        source_confidence=0.2,
        progress={
            "discovery": {
                "sourceUrl": "https://facebook.com/not-valid",
                "sourceConfidence": 0.2,
                "confidenceTier": "low",
                "sourceProvenance": "search",
                "fallbackReason": "weak_source",
                "resolutionTrace": [],
                "candidates": [],
            },
            "analytics": {
                "sourceQuality": {
                    "score": 0.1,
                    "isWeak": True,
                    "isBlocked": True,
                    "reasons": ["blocked_host"],
                    "recoveryAttempts": 3,
                    "recoveredFromUrl": None,
                    "recoveredToUrl": None,
                    "sourceRejectedCount": 0,
                    "sourceRecoveredCount": 0,
                    "zeroChapterPrevented": 0,
                }
            },
        },
    )
    request_repository = _FakeRequestRepository(request)

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        discover_source=lambda _: {
            "fraternity_name": "Alpha Beta",
            "fraternity_slug": "alpha-beta",
            "selected_url": None,
            "selected_confidence": 0.0,
            "confidence_tier": "low",
            "source_provenance": "search",
            "fallback_reason": "weak_source",
            "source_quality": {"score": 0.0, "isWeak": True, "isBlocked": False, "reasons": ["missing_url"]},
            "candidates": [],
            "resolution_trace": [],
        },
    )
    summary = runtime.run(request.id)

    assert summary["status"] == "paused"
    assert summary["terminalReason"] == "awaiting_confirmation"
    assert request_repository.request.stage == "awaiting_confirmation"
    assert request_repository.request.status == "draft"
    assert any(event_type == "source_rejected" for event_type, _ in request_repository.events)


def test_request_graph_runs_enrichment_cycle_until_queue_drains():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 1, "running": 0, "done": 0, "failed": 0},
        {"field": "find_email", "queued": 1, "running": 0, "done": 0, "failed": 0},
        {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 777,
            "status": "succeeded",
            "pages_processed": 12,
            "records_seen": 6,
            "records_upserted": 6,
            "review_items_created": 0,
            "field_jobs_created": 2,
        }
        return {"runtime_mode": "adaptive_assisted"}

    def process_field_jobs(**_: object) -> dict[str, int]:
        request_repository.field_snapshots["alpha-main"] = [
            {"field": "find_website", "queued": 0, "running": 0, "done": 1, "failed": 0},
            {"field": "find_email", "queued": 0, "running": 0, "done": 1, "failed": 0},
            {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
        ]
        return {"processed": 2, "requeued": 0, "failed_terminal": 0, "runtime_mode_used": "langgraph_primary"}

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        run_crawl=run_crawl,
        process_field_jobs=process_field_jobs,
        search_preflight=lambda: {
            "healthy": True,
            "provider_health": {
                "searxng_json": {
                    "attempts": 2,
                    "success_rate": 1.0,
                }
            },
        },
    )
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert summary["queueRemaining"] == 0
    assert summary["cyclesCompleted"] == 1
    assert request_repository.request.stage == "completed"
    assert request_repository.request.progress["analytics"]["enrichment"]["cyclesCompleted"] == 1
    assert request_repository.provider_health_snapshots[0]["provider"] == "searxng_json"


def test_request_graph_completes_when_only_deferred_queue_remains():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 1, "running": 0, "done": 0, "failed": 0, "queued_actionable": 0, "queued_deferred": 1},
        {"field": "find_email", "queued": 0, "running": 0, "done": 1, "failed": 0, "done_terminal_no_signal": 1},
        {"field": "find_instagram", "queued": 0, "running": 0, "done": 1, "failed": 0, "done_provider_degraded": 1},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 778,
            "status": "succeeded",
            "pages_processed": 4,
            "records_seen": 3,
            "records_upserted": 3,
            "review_items_created": 0,
            "field_jobs_created": 1,
        }
        return {"runtime_mode": "adaptive_assisted"}

    runtime = _build_runtime(request_repository, _FakeCrawlerRepository(), run_crawl=run_crawl)
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert summary["queueRemaining"] == 0
    assert request_repository.request.stage == "completed"
    assert request_repository.request.progress["contactResolution"]["queuedDeferred"] == 1
    assert request_repository.request.progress["contactResolution"]["terminalNoSignal"] == 1


def test_request_graph_retries_zero_record_v3_crawl_with_alternate_policy():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_email", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
    ]
    crawl_runtime_calls: list[str] = []

    def run_crawl(**kwargs: object) -> dict[str, object]:
        runtime_mode = str(kwargs["runtime_mode"])
        crawl_runtime_calls.append(runtime_mode)
        if runtime_mode == "adaptive_primary":
            request_repository.latest_crawl_runs["alpha-main"] = {
                "id": 901,
                "status": "succeeded",
                "pages_processed": 4,
                "records_seen": 0,
                "records_upserted": 0,
                "review_items_created": 0,
                "field_jobs_created": 0,
            }
        else:
            request_repository.latest_crawl_runs["alpha-main"] = {
                "id": 902,
                "status": "succeeded",
                "pages_processed": 2,
                "records_seen": 3,
                "records_upserted": 3,
                "review_items_created": 0,
                "field_jobs_created": 0,
            }
        return {"runtime_mode": runtime_mode}

    runtime = RequestSupervisorGraphRuntime(
        request_repository=request_repository,
        crawler_repository=_FakeCrawlerRepository(),
        worker_id="test-worker",
        runtime_mode="v3_request_supervisor",
        crawl_runtime_mode="adaptive_primary",
        field_job_runtime_mode="langgraph_primary",
        field_job_graph_durability="sync",
        free_recovery_attempts=3,
        discover_source=lambda fraternity_name: {"fraternity_name": fraternity_name, "selected_url": None},
        run_crawl=run_crawl,
        process_field_jobs=lambda **_: {"processed": 0, "requeued": 0, "failed_terminal": 0},
        search_preflight=lambda: {"healthy": True, "provider_health": {}},
        logger=SimpleNamespace(),
    )

    summary = runtime.run(request.id)

    assert crawl_runtime_calls == ["adaptive_primary", "adaptive_assisted"]
    assert summary["status"] == "succeeded"
    assert summary["crawlRuntimeModeUsed"] == "adaptive_assisted"
    assert request_repository.request.progress["crawlRun"]["recordsSeen"] == 3
    assert any(event_type == "runtime_retry" for event_type, _ in request_repository.events)


def test_request_graph_passes_crawl_policy_version_to_v3_crawl():
    request = _request()
    request = replace(
        request,
        config={
            **request.config,
            "crawlPolicyVersion": "adaptive-v1-test-promotion",
        },
    )
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_email", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
    ]
    received_policy_versions: list[str | None] = []

    def run_crawl(**kwargs: object) -> dict[str, object]:
        received_policy_versions.append(kwargs.get("policy_version") if isinstance(kwargs.get("policy_version"), str) else None)
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 903,
            "status": "succeeded",
            "pages_processed": 3,
            "records_seen": 2,
            "records_upserted": 2,
            "review_items_created": 0,
            "field_jobs_created": 0,
        }
        return {"runtime_mode": str(kwargs["runtime_mode"])}

    runtime = _build_runtime(request_repository, _FakeCrawlerRepository(), run_crawl=run_crawl)
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert received_policy_versions == ["adaptive-v1-test-promotion"]
