from __future__ import annotations

import logging
from dataclasses import replace
from types import SimpleNamespace

from fratfinder_crawler.models import ChapterActivityRecord, ChapterEvidenceRecord, FraternityCrawlRequestRecord, ProvisionalChapterRecord, SchoolPolicyRecord
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
        self.crawl_runs_by_id: dict[int, dict] = {}
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

    def get_crawl_run_by_id(self, crawl_run_id: int) -> dict | None:
        return self.crawl_runs_by_id.get(crawl_run_id)

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
        self.instagram_candidate_rows: dict[str, list[ChapterEvidenceRecord]] = {}
        self.instagram_resolutions: list[dict] = []
        self.review_items: list[dict] = []

    def upsert_fraternity(self, slug: str, name: str, nic_affiliated: bool = True) -> tuple[str, str]:
        self.upserted_fraternities.append({"slug": slug, "name": name, "nicAffiliated": nic_affiliated})
        return "frat-1", slug

    def upsert_source(self, **kwargs: object) -> tuple[str, str]:
        self.upserted_sources.append(dict(kwargs))
        return "source-1", str(kwargs["slug"])

    def load_sources(self, source_slug: str | None = None):
        if not source_slug:
            return []
        return [
            SimpleNamespace(
                id="source-1",
                fraternity_id="frat-1",
                source_slug=source_slug,
                slug=source_slug,
                base_url="https://example.org",
                list_url="https://example.org/chapters",
                metadata={},
            )
        ]

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

    def fetch_instagram_candidates_for_chapters(self, chapter_ids: list[str]):
        rows: list[ChapterEvidenceRecord] = []
        for chapter_id in chapter_ids:
            rows.extend(self.instagram_candidate_rows.get(chapter_id, []))
        return rows

    def apply_instagram_resolution(self, **kwargs):
        self.instagram_resolutions.append(dict(kwargs))
        return True

    def create_review_item(self, source_id, crawl_run_id, candidate, *, chapter_id=None):
        self.review_items.append(
            {
                "source_id": source_id,
                "crawl_run_id": crawl_run_id,
                "candidate": candidate,
                "chapter_id": chapter_id,
            }
        )
        return "review-1"


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


def test_request_graph_promotes_official_provisional_with_institution_even_without_contact():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.provisional_chapters = [
        ProvisionalChapterRecord(
            id="prov-official",
            fraternity_id="frat-1",
            slug="virginia-tech-chapter",
            name="Virginia Tech Provisional Chapter",
            status="provisional",
            request_id=request.id,
            university_name="Virginia Tech",
            evidence_payload={
                "sourceClass": "national",
                "validityClass": "provisional_candidate",
            },
        )
    ]

    crawler_repository = _FakeCrawlerRepository()

    def _load_sources(_: str | None = None):
        return [
            SimpleNamespace(
                id="source-1",
                fraternity_id="frat-1",
                source_slug="alpha-main",
                slug="alpha-main",
                base_url="https://www.dlp.org",
                list_url="https://www.dlp.org/chapters",
                metadata={"confirmedByOperator": True},
            )
        ]

    crawler_repository.load_sources = _load_sources  # type: ignore[assignment]
    runtime = _build_runtime(request_repository, crawler_repository)
    result = runtime._evaluate_provisional_promotions({"request": request, "progress": {}})  # type: ignore[attr-defined]

    assert len(crawler_repository.upserted_chapters) == 1
    assert any(update["promotion_reason"] == "auto_promoted_official_institution_signal" for update in request_repository.provisional_updates)
    assert result["progress"]["provisional"]["autoPromoted"] == 1


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


def test_request_graph_preserves_confirmed_directory_source_after_zero_record_crawl():
    request = _request(
        source_slug="alpha-main",
        source_url="https://www.thetaxi.org/chapters-and-colonies/",
        source_confidence=0.9,
        progress={
            "discovery": {
                "sourceUrl": "https://www.thetaxi.org/chapters-and-colonies/",
                "sourceConfidence": 0.9,
                "confidenceTier": "high",
                "sourceProvenance": "verified_registry",
                "fallbackReason": None,
                "resolutionTrace": [],
                "candidates": [],
                "confirmedByOperator": True,
                "confirmedAt": "2026-04-04T00:00:00+00:00",
            },
            "analytics": {
                "sourceQuality": {
                    "score": 0.9,
                    "isWeak": False,
                    "isBlocked": False,
                    "reasons": ["positive:chapters", "deeper_path"],
                    "recoveryAttempts": 0,
                    "recoveredFromUrl": None,
                    "recoveredToUrl": None,
                    "sourceRejectedCount": 0,
                    "sourceRecoveredCount": 0,
                    "zeroChapterPrevented": 0,
                    "sourcePreservedCount": 0,
                    "confirmedByOperator": True,
                    "confirmedAt": "2026-04-04T00:00:00+00:00",
                }
            },
        },
    )
    request_repository = _FakeRequestRepository(request)

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 905,
            "status": "succeeded",
            "pages_processed": 4,
            "records_seen": 0,
            "records_upserted": 0,
            "review_items_created": 0,
            "field_jobs_created": 0,
        }
        return {"runtime_mode": "adaptive_assisted"}

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        run_crawl=run_crawl,
        discover_source=lambda fraternity_name: {"fraternity_name": fraternity_name, "selected_url": None},
    )

    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert summary["terminalReason"] == "completed_confirmed_source_zero_record"
    assert request_repository.request.stage == "completed"
    assert request_repository.request.status == "succeeded"
    assert any(event_type == "source_preserved" for event_type, _ in request_repository.events)


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


def test_request_graph_persists_provider_specific_health_not_just_batch_health():
    request = _request()
    request_repository = _FakeRequestRepository(request)
    request_repository.field_snapshots["alpha-main"] = [
        {"field": "find_website", "queued": 1, "running": 0, "done": 0, "failed": 0},
        {"field": "find_email", "queued": 0, "running": 0, "done": 0, "failed": 0},
        {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
    ]

    def run_crawl(**_: object) -> dict[str, object]:
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 778,
            "status": "succeeded",
            "pages_processed": 5,
            "records_seen": 4,
            "records_upserted": 4,
            "review_items_created": 0,
            "field_jobs_created": 1,
        }
        return {"runtime_mode": "adaptive_assisted"}

    def process_field_jobs(**_: object) -> dict[str, int]:
        request_repository.field_snapshots["alpha-main"] = [
            {"field": "find_website", "queued": 0, "running": 0, "done": 1, "failed": 0},
            {"field": "find_email", "queued": 0, "running": 0, "done": 0, "failed": 0},
            {"field": "find_instagram", "queued": 0, "running": 0, "done": 0, "failed": 0},
        ]
        return {"processed": 1, "requeued": 0, "failed_terminal": 0, "runtime_mode_used": "langgraph_primary"}

    runtime = _build_runtime(
        request_repository,
        _FakeCrawlerRepository(),
        run_crawl=run_crawl,
        process_field_jobs=process_field_jobs,
        search_preflight=lambda: {
            "healthy": True,
            "min_success_rate": 0.25,
            "provider_health": {
                "searxng_json": {
                    "attempts": 4,
                    "successes": 0,
                    "request_error": 4,
                    "unavailable": 0,
                    "low_signal": 0,
                    "challenge_or_anomaly": 0,
                    "success_rate": 0.0,
                },
                "bing_html": {
                    "attempts": 4,
                    "successes": 2,
                    "request_error": 0,
                    "unavailable": 0,
                    "low_signal": 0,
                    "challenge_or_anomaly": 0,
                    "success_rate": 0.5,
                },
            },
        },
    )
    summary = runtime.run(request.id)

    assert summary["status"] == "succeeded"
    assert len(request_repository.provider_health_snapshots) == 2
    searxng_snapshot = request_repository.provider_health_snapshots[0]
    bing_snapshot = request_repository.provider_health_snapshots[1]
    assert searxng_snapshot["provider"] == "searxng_json"
    assert searxng_snapshot["healthy"] is False
    assert searxng_snapshot["payload"]["provider_healthy"] is False
    assert searxng_snapshot["payload"]["provider_health_reason"] == "request_error_only"
    assert searxng_snapshot["payload"]["batch_healthy"] is True
    assert bing_snapshot["provider"] == "bing_html"
    assert bing_snapshot["healthy"] is True
    assert bing_snapshot["payload"]["provider_healthy"] is True
    assert bing_snapshot["payload"]["provider_health_reason"] == "meets_success_threshold"


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
    assert summary["queueRemaining"] == 1
    assert summary["queueRemainingActionable"] == 0
    assert summary["queueRemainingNonActionable"] == 1
    assert summary["terminalReason"] == "completed_deferred_non_actionable_queue"
    assert request_repository.request.stage == "completed"
    assert request_repository.request.progress["contactResolution"]["queuedDeferred"] == 1
    assert request_repository.request.progress["contactResolution"]["terminalNoSignal"] == 1
    assert request_repository.request.progress["analytics"]["enrichment"]["completionMode"] == "deferred_non_actionable_residual"


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


def test_request_graph_zero_record_v3_crawl_routes_to_source_recovery_without_runtime_downgrade():
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
        request_repository.latest_crawl_runs["alpha-main"] = {
            "id": 901,
            "status": "succeeded",
            "pages_processed": 4,
            "records_seen": 0,
            "records_upserted": 0,
            "review_items_created": 0,
            "field_jobs_created": 0,
        }
        return {"runtime_mode": runtime_mode}

    runtime = RequestSupervisorGraphRuntime(
        request_repository=request_repository,
        crawler_repository=_FakeCrawlerRepository(),
        worker_id="test-worker",
        runtime_mode="v3_request_supervisor",
        crawl_runtime_mode="adaptive_assisted",
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

    assert crawl_runtime_calls == ["adaptive_assisted"]
    assert summary["status"] == "paused"
    assert summary["terminalReason"] == "awaiting_confirmation"
    assert request_repository.request.progress["crawlRun"]["recordsSeen"] == 0
    assert not any(event_type == "runtime_retry" for event_type, _ in request_repository.events)


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


def test_request_graph_instagram_sweep_resolves_candidates_before_enrichment():
    request = _request(source_slug="delta-chi-main")
    request_repository = _FakeRequestRepository(request)
    crawler_repository = _FakeCrawlerRepository()
    request_repository.latest_crawl_runs["delta-chi-main"] = {
        "id": 911,
        "status": "succeeded",
        "pages_processed": 4,
        "records_seen": 1,
        "records_upserted": 1,
        "review_items_created": 0,
        "field_jobs_created": 1,
    }
    request_repository.crawl_runs_by_id[911] = dict(request_repository.latest_crawl_runs["delta-chi-main"])
    request_repository.request = replace(
        request_repository.request,
        progress={
            **request_repository.request.progress,
            "crawlRun": {
                "id": 911,
                "status": "succeeded",
                "pagesProcessed": 4,
                "recordsSeen": 1,
                "recordsUpserted": 1,
                "reviewItemsCreated": 0,
                "fieldJobsCreated": 1,
            },
        },
    )
    crawler_repository.chapters_for_crawl_run[911] = [
        {
            "chapter_id": "chapter-1",
            "chapter_slug": "chapter-one",
            "chapter_name": "Mississippi State Chapter",
            "university_name": "Mississippi State University",
            "chapter_status": "active",
            "website_url": None,
            "instagram_url": None,
            "contact_email": None,
            "field_states": {},
            "fraternity_slug": "delta-chi",
        }
    ]
    crawler_repository.instagram_candidate_rows["chapter-1"] = [
        ChapterEvidenceRecord(
            chapter_id="chapter-1",
            chapter_slug="chapter-one",
            fraternity_slug="delta-chi",
            source_slug="delta-chi-main",
            crawl_run_id=911,
            field_name="instagram_url",
                candidate_value="https://www.instagram.com/msstatedeltachi/",
                confidence=0.97,
                source_url="https://www.msstate.edu/greek-life/delta-chi",
                source_snippet="Official school chapter page for Delta Chi at Mississippi State University Instagram",
                metadata={
                    "sourceType": "official_school_chapter_page",
                    "pageScope": "school_affiliation_page",
                    "contactSpecificity": "school_specific",
            },
        )
    ]
    runtime = _build_runtime(
        request_repository,
        crawler_repository,
        process_field_jobs=lambda **_: {"processed": 0, "requeued": 0, "failed_terminal": 0},
    )

    updates = runtime._run_instagram_sweep({"request_id": request.id, "progress": request_repository.request.progress})

    assert crawler_repository.instagram_resolutions[0]["instagram_url"] == "https://www.instagram.com/msstatedeltachi/"
    assert crawler_repository.completed_pending[0]["reason_code"] == "resolved_by_global_instagram_sweep"
    sweep = updates["progress"]["analytics"]["enrichment"]["instagramSweep"]
    assert sweep["resolved"] == 1
    assert sweep["jobsCanceled"] == 1


def test_request_graph_instagram_sweep_uses_bound_progress_crawl_run_not_latest_source_run():
    request = _request(
        source_slug="delta-chi-main",
        progress={
            "discovery": {
                "sourceUrl": "https://deltachi.org/chapters",
                "sourceConfidence": 0.9,
                "confidenceTier": "high",
                "sourceProvenance": "verified_registry",
                "fallbackReason": None,
                "resolutionTrace": [],
                "candidates": [],
            },
            "crawlRun": {
                "id": 911,
                "status": "succeeded",
                "pagesProcessed": 4,
                "recordsSeen": 1,
                "recordsUpserted": 1,
                "reviewItemsCreated": 0,
                "fieldJobsCreated": 1,
            },
        },
    )
    request_repository = _FakeRequestRepository(request)
    crawler_repository = _FakeCrawlerRepository()
    request_repository.latest_crawl_runs["delta-chi-main"] = {
        "id": 999,
        "status": "succeeded",
        "pages_processed": 7,
        "records_seen": 4,
        "records_upserted": 4,
        "review_items_created": 0,
        "field_jobs_created": 2,
    }
    request_repository.crawl_runs_by_id[911] = {
        "id": 911,
        "status": "succeeded",
        "pages_processed": 4,
        "records_seen": 1,
        "records_upserted": 1,
        "review_items_created": 0,
        "field_jobs_created": 1,
    }
    crawler_repository.chapters_for_crawl_run[911] = [
        {
            "chapter_id": "chapter-1",
            "chapter_slug": "chapter-one",
            "chapter_name": "Mississippi State Chapter",
            "university_name": "Mississippi State University",
            "chapter_status": "active",
            "website_url": None,
            "instagram_url": None,
            "contact_email": None,
            "field_states": {},
            "fraternity_slug": "delta-chi",
        }
    ]
    crawler_repository.chapters_for_crawl_run[999] = []
    crawler_repository.instagram_candidate_rows["chapter-1"] = [
        ChapterEvidenceRecord(
            chapter_id="chapter-1",
            chapter_slug="chapter-one",
            fraternity_slug="delta-chi",
            source_slug="delta-chi-main",
            crawl_run_id=911,
            field_name="instagram_url",
            candidate_value="https://www.instagram.com/msstatedeltachi/",
            confidence=0.97,
            source_url="https://www.msstate.edu/greek-life/delta-chi",
            source_snippet="Official school chapter page for Delta Chi at Mississippi State University Instagram",
            metadata={
                "sourceType": "official_school_chapter_page",
                "pageScope": "school_affiliation_page",
                "contactSpecificity": "school_specific",
            },
        )
    ]
    runtime = _build_runtime(
        request_repository,
        crawler_repository,
        process_field_jobs=lambda **_: {"processed": 0, "requeued": 0, "failed_terminal": 0},
    )

    updates = runtime._run_instagram_sweep({"request_id": request.id, "progress": request.progress})

    assert crawler_repository.instagram_resolutions[0]["crawl_run_id"] == 911
    assert updates["instagram_sweep_summary"]["resolved"] == 1


def test_request_graph_enrichment_cycle_enables_existing_instagram_validation():
    request = _request(source_slug="alpha-main")
    request_repository = _FakeRequestRepository(request)
    crawler_repository = _FakeCrawlerRepository()
    captured_kwargs: dict[str, object] = {}

    runtime = _build_runtime(
        request_repository,
        crawler_repository,
        process_field_jobs=lambda **kwargs: captured_kwargs.update(kwargs) or {"processed": 0, "requeued": 0, "failed_terminal": 0},
    )

    runtime._run_enrichment_cycle(
        {
            "request_id": request.id,
            "request": request,
            "effective_config": {"fieldJobLimitPerCycle": 10, "fieldJobWorkers": 2},
            "cycle_state": {"cyclesCompleted": 0, "lowProgressCycles": 0, "degradedCycleCount": 0, "processedTotal": 0, "requeuedTotal": 0, "failedTerminalTotal": 0},
        }
    )

    assert captured_kwargs["validate_existing_instagram"] is True
