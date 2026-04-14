from __future__ import annotations

import logging
from dataclasses import replace
from types import SimpleNamespace

from fratfinder_crawler.models import ChapterActivityRecord, FraternityCrawlRequestRecord, ProvisionalChapterRecord, SchoolPolicyRecord
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
        self.provisional_chapters: list[ProvisionalChapterRecord] = []
        self.provisional_updates: list[dict] = []
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

    def list_provisional_chapters_for_request(self, request_id: str, *, statuses=("provisional",), limit: int = 200):
        assert request_id == self.request.id
        return [item for item in self.provisional_chapters if item.status in statuses][:limit]

    def update_provisional_chapter_status(
        self,
        provisional_id: str,
        *,
        status: str,
        promotion_reason: str | None = None,
        promoted_chapter_id: str | None = None,
    ) -> None:
        self.provisional_updates.append(
            {
                "id": provisional_id,
                "status": status,
                "promotion_reason": promotion_reason,
                "promoted_chapter_id": promoted_chapter_id,
            }
        )
        self.provisional_chapters = [
            replace(
                item,
                status=status if item.id == provisional_id else item.status,
                promotion_reason=promotion_reason if item.id == provisional_id else item.promotion_reason,
                promoted_chapter_id=promoted_chapter_id if item.id == provisional_id else item.promoted_chapter_id,
            )
            for item in self.provisional_chapters
        ]


class _FakeCrawlerRepository:
    def __init__(self):
        self.upserted_fraternities: list[dict] = []
        self.upserted_sources: list[dict] = []
        self.upserted_chapters: list[dict] = []
        self.school_policies: dict[str, SchoolPolicyRecord] = {}
        self.chapter_activities: dict[tuple[str, str], ChapterActivityRecord] = {}
        self.chapters_for_crawl_run: dict[int, list[dict]] = {}
        self.inactive_applied: list[dict] = []
        self.completed_pending: list[dict] = []

    def upsert_fraternity(self, slug: str, name: str, nic_affiliated: bool = True) -> tuple[str, str]:
        self.upserted_fraternities.append({"slug": slug, "name": name, "nicAffiliated": nic_affiliated})
        return "frat-1", slug

    def upsert_source(self, **kwargs: object) -> tuple[str, str]:
        self.upserted_sources.append(dict(kwargs))
        return "source-1", str(kwargs["slug"])

    def load_sources(self, source_slug: str | None = None):
        if not source_slug:
            return []
        return [SimpleNamespace(id="source-1", fraternity_id="frat-1", source_slug=source_slug, slug=source_slug)]

    def upsert_chapter_discovery(self, source, chapter):
        self.upserted_chapters.append({"source": source, "chapter": chapter})
        return "chapter-1"

    def get_school_policy(self, school_name: str | None):
        return self.school_policies.get((school_name or "").strip().lower())

    def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None):
        return self.chapter_activities.get(((fraternity_slug or "").strip(), (school_name or "").strip().lower()))

    def list_chapters_for_crawl_run(self, crawl_run_id: int):
        return list(self.chapters_for_crawl_run.get(crawl_run_id, []))

    def apply_chapter_inactive_status(self, **kwargs):
        self.inactive_applied.append(dict(kwargs))

    def complete_pending_field_jobs_for_chapter(self, **kwargs):
        self.completed_pending.append(dict(kwargs))
        return 1


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
        logger=logging.getLogger("test-request-graph"),
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


def test_request_graph_purges_cached_inactive_school_before_enrichment():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 1, "running": 0, "done": 0, "failed": 0},
        {"field": "find_email", "queued": 1, "running": 0, "done": 0, "failed": 0},
        {"field": "find_instagram", "queued": 1, "running": 0, "done": 0, "failed": 0},
    ]
    crawler_repository = _FakeCrawlerRepository()
    crawler_repository.school_policies["norwich university"] = SchoolPolicyRecord(
        school_slug="norwich-university",
        school_name="Norwich University",
        greek_life_status="banned",
        confidence=0.97,
        evidence_url="https://archives.norwich.edu/fraternities-banned",
        evidence_source_type="official_school",
        reason_code="strong_ban_phrase",
        metadata={"sourceSnippet": "There are no fraternities at Norwich."},
        last_verified_at="2026-04-08T00:00:00+00:00",
        created_at="2026-04-08T00:00:00+00:00",
        updated_at="2026-04-08T00:00:00+00:00",
    )

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 601,
            "status": "succeeded",
            "pages_processed": 3,
            "records_seen": 2,
            "records_upserted": 2,
            "review_items_created": 0,
            "field_jobs_created": 3,
        }
        crawler_repository.chapters_for_crawl_run[601] = [
            {
                "chapter_id": "chapter-1",
                "chapter_slug": "alpha-norwich-university",
                "chapter_name": "Alpha Chapter",
                "university_name": "Norwich University",
                "fraternity_slug": "alpha-beta",
            }
        ]
        return {"runtime_mode": "adaptive_assisted"}

    runtime = _build_runtime(
        request_repository,
        crawler_repository,
        run_crawl=run_crawl,
        process_field_jobs=lambda **_: request_repository.field_snapshots.__setitem__(
            "alpha-main",
            [
                {"field": "find_website", "queued": 0, "running": 0, "done": 1, "failed": 0},
                {"field": "find_email", "queued": 0, "running": 0, "done": 1, "failed": 0},
                {"field": "find_instagram", "queued": 0, "running": 0, "done": 1, "failed": 0},
            ],
        ) or {"processed": 3, "requeued": 0, "failed_terminal": 0},
    )

    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert crawler_repository.inactive_applied[0]["chapter_slug"] == "alpha-norwich-university"
    assert request_repository.request.stage == "completed"
    assert request_repository.request.progress["queueTriage"]["purgedInactiveChapters"] == 1
    assert summary["terminalReason"] == "completed"
    assert request_repository.request.stage == "completed"
    assert request_repository.request.status == "succeeded"
    assert request_repository.request.progress["crawlRun"]["recordsSeen"] == 2
    assert any(event_type == "request_completed" for event_type, _ in request_repository.events)


def test_request_graph_promotes_reviews_and_rejects_provisional_chapters():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_email", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
    ]
    request_repository.provisional_chapters = [
        ProvisionalChapterRecord(
            id="prov-1",
            fraternity_id="frat-1",
            slug="alpha-beta-test",
            name="Alpha Beta",
            status="provisional",
            request_id=request.id,
            university_name="Example University",
            website_url="https://example.edu/alphabeta",
        ),
        ProvisionalChapterRecord(
            id="prov-2",
            fraternity_id="frat-1",
            slug="alpha-beta-review",
            name="Alpha Beta Review",
            status="provisional",
            request_id=request.id,
            university_name=None,
            website_url="https://weak.example.com/chapter",
        ),
        ProvisionalChapterRecord(
            id="prov-3",
            fraternity_id="frat-1",
            slug="alpha-beta-rejected",
            name="Alpha Beta Rejected",
            status="provisional",
            request_id=request.id,
            university_name=None,
        ),
    ]

    crawler_repository = _FakeCrawlerRepository()
    runtime = _build_runtime(request_repository, crawler_repository)
    result = runtime._evaluate_provisional_promotions({"request": request, "progress": {}})  # type: ignore[attr-defined]

    assert len(crawler_repository.upserted_chapters) == 1
    assert any(update["status"] == "promoted" for update in request_repository.provisional_updates)
    assert any(update["status"] == "review" for update in request_repository.provisional_updates)
    assert any(update["status"] == "rejected" for update in request_repository.provisional_updates)
    assert result["progress"]["provisional"]["autoPromoted"] == 1
    assert result["progress"]["provisional"]["reviewRequired"] == 1
    assert result["progress"]["provisional"]["rejected"] == 1


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


def test_request_graph_completes_with_small_residual_queue_when_provider_is_degraded():
    request = replace(_request(), config={"fieldJobWorkers": 2, "fieldJobLimitPerCycle": 20, "maxEnrichmentCycles": 1, "pauseMs": 0})
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 4, "running": 0, "done": 6, "failed": 0, "queued_actionable": 4, "queued_deferred": 0, "done_updated": 3},
        {"field": "find_email", "queued": 4, "running": 0, "done": 7, "failed": 0, "queued_actionable": 4, "queued_deferred": 0, "done_updated": 1},
        {"field": "find_instagram", "queued": 4, "running": 0, "done": 8, "failed": 0, "queued_actionable": 4, "queued_deferred": 0, "done_provider_degraded": 2, "done_updated": 2},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 779,
            "status": "succeeded",
            "pages_processed": 8,
            "records_seen": 15,
            "records_upserted": 10,
            "review_items_created": 0,
            "field_jobs_created": 12,
        }
        return {"runtime_mode": "adaptive_assisted"}

    def process_field_jobs(**_: object) -> dict[str, int]:
        return {
            "processed": 0,
            "requeued": 12,
            "failed_terminal": 0,
            "runtime_mode_used": "langgraph_primary",
        }

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        run_crawl=run_crawl,
        process_field_jobs=process_field_jobs,
        search_preflight=lambda: {
            "healthy": False,
            "success_rate": 0.0,
            "provider_health": {
                "searxng_json": {"attempts": 4, "success_rate": 0.0},
            },
        },
    )
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert summary["terminalReason"] == "completed_deferred_provider_recovery"
    assert summary["queueRemaining"] == 12
    assert request_repository.request.stage == "completed"
    assert request_repository.request.status == "succeeded"
    assert request_repository.request.progress["analytics"]["enrichment"]["completionMode"] == "deferred_provider_residual"
    assert request_repository.request.progress["analytics"]["enrichment"]["residualActionableAtCompletion"] == 12


def test_request_graph_still_fails_when_residual_queue_is_too_large_even_if_provider_is_degraded():
    request = replace(_request(), config={"fieldJobWorkers": 2, "fieldJobLimitPerCycle": 20, "maxEnrichmentCycles": 1, "pauseMs": 0})
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 20, "running": 0, "done": 2, "failed": 0, "queued_actionable": 20, "queued_deferred": 0},
        {"field": "find_email", "queued": 20, "running": 0, "done": 1, "failed": 0, "queued_actionable": 20, "queued_deferred": 0},
        {"field": "find_instagram", "queued": 20, "running": 0, "done": 1, "failed": 0, "queued_actionable": 20, "queued_deferred": 0},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 780,
            "status": "succeeded",
            "pages_processed": 4,
            "records_seen": 5,
            "records_upserted": 4,
            "review_items_created": 0,
            "field_jobs_created": 60,
        }
        return {"runtime_mode": "adaptive_assisted"}

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        run_crawl=run_crawl,
        process_field_jobs=lambda **_: {"processed": 0, "requeued": 60, "failed_terminal": 0, "runtime_mode_used": "langgraph_primary"},
        search_preflight=lambda: {
            "healthy": False,
            "success_rate": 0.0,
            "provider_health": {
                "searxng_json": {"attempts": 4, "success_rate": 0.0},
            },
        },
    )
    summary = runtime.run(request.id)

    assert summary["status"] == "failed"
    assert summary["terminalReason"] == "budget_exhausted"
    assert request_repository.request.stage == "failed"


def test_request_graph_completes_early_when_small_residual_queue_stalls_under_provider_degradation():
    request = replace(_request(), config={"fieldJobWorkers": 2, "fieldJobLimitPerCycle": 20, "maxEnrichmentCycles": 16, "pauseMs": 0})
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 4, "running": 0, "done": 6, "failed": 0, "queued_actionable": 4},
        {"field": "find_email", "queued": 4, "running": 0, "done": 5, "failed": 0, "queued_actionable": 4},
        {"field": "find_instagram", "queued": 4, "running": 0, "done": 4, "failed": 0, "queued_actionable": 4, "done_provider_degraded": 1},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 781,
            "status": "succeeded",
            "pages_processed": 6,
            "records_seen": 9,
            "records_upserted": 7,
            "review_items_created": 0,
            "field_jobs_created": 12,
        }
        return {"runtime_mode": "adaptive_assisted"}

    def process_field_jobs(**_: object) -> dict[str, int]:
        return {
            "processed": 0,
            "requeued": 12,
            "failed_terminal": 0,
            "runtime_mode_used": "langgraph_primary",
        }

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        run_crawl=run_crawl,
        process_field_jobs=process_field_jobs,
        search_preflight=lambda: {
            "healthy": False,
            "success_rate": 0.0,
            "provider_health": {
                "searxng_json": {"attempts": 4, "success_rate": 0.0},
            },
        },
    )
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert summary["terminalReason"] == "completed_deferred_provider_recovery"
    assert summary["queueRemaining"] == 12
    assert request_repository.request.stage == "completed"
    assert request_repository.request.progress["analytics"]["enrichment"]["completionMode"] == "deferred_provider_residual"


def test_request_graph_completes_early_when_small_residual_queue_stalls_even_if_preflight_is_nominal():
    request = replace(_request(), config={"fieldJobWorkers": 2, "fieldJobLimitPerCycle": 20, "maxEnrichmentCycles": 16, "pauseMs": 0})
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 5, "running": 0, "done": 6, "failed": 0, "queued_actionable": 5},
        {"field": "find_email", "queued": 5, "running": 0, "done": 5, "failed": 0, "queued_actionable": 5},
        {"field": "find_instagram", "queued": 4, "running": 0, "done": 4, "failed": 0, "queued_actionable": 4},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 782,
            "status": "succeeded",
            "pages_processed": 6,
            "records_seen": 9,
            "records_upserted": 7,
            "review_items_created": 0,
            "field_jobs_created": 12,
        }
        return {"runtime_mode": "adaptive_assisted"}

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        run_crawl=run_crawl,
        process_field_jobs=lambda **_: {
            "processed": 0,
            "requeued": 14,
            "failed_terminal": 0,
            "runtime_mode_used": "langgraph_primary",
        },
        search_preflight=lambda: {
            "healthy": True,
            "success_rate": 1.0,
            "provider_health": {
                "bing_html": {"attempts": 4, "success_rate": 1.0},
            },
        },
    )
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert summary["terminalReason"] == "completed_deferred_provider_recovery"
    assert summary["queueRemaining"] == 14


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
