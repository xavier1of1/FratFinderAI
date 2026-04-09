from contextlib import nullcontext
from types import SimpleNamespace

import fratfinder_crawler.pipeline as pipeline_module

from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.pipeline import CrawlService, _distribute_limit, _worker_id


def test_distribute_limit_caps_workers_to_limit():
    assert _distribute_limit(3, 8) == [1, 1, 1]


def test_distribute_limit_balances_work_evenly():
    assert _distribute_limit(10, 4) == [3, 3, 2, 2]


def test_distribute_limit_handles_zero_limit():
    assert _distribute_limit(0, 8) == []


def test_worker_id_suffixes_only_for_multi_worker_runs():
    assert _worker_id("local-crawler-worker", 1, 1) == "local-crawler-worker"
    assert _worker_id("local-crawler-worker", 3, 8) == "local-crawler-worker-3"


def test_resolve_field_job_runtime_mode_defaults_to_langgraph_primary_for_unknown():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="langgraph_primary", crawler_field_job_graph_durability="sync"))
    assert service._resolve_field_job_runtime_mode("unsupported") == "langgraph_primary"


def test_resolve_field_job_runtime_mode_uses_settings_default():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="langgraph_shadow", crawler_field_job_graph_durability="sync"))
    assert service._resolve_field_job_runtime_mode(None) == "langgraph_shadow"


def test_resolve_field_job_graph_durability_defaults_to_sync_for_unknown():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="legacy", crawler_field_job_graph_durability="sync"))
    assert service._resolve_field_job_graph_durability("invalid") == "sync"


def test_run_request_worker_processes_claimed_requests_once(monkeypatch):
    claimed = [
        SimpleNamespace(id="req-1", fraternity_slug="alpha-beta", source_slug="alpha-main", stage="discovery"),
        SimpleNamespace(id="req-2", fraternity_slug="gamma-delta", source_slug="gamma-main", stage="discovery"),
    ]

    class FakeRequestGraphRepository:
        def __init__(self, connection):
            self._connection = connection

        def upsert_worker_process(self, **kwargs):
            _ = kwargs

        def heartbeat_worker_process(self, worker_id: str, lease_seconds: int | None = None):
            _ = worker_id, lease_seconds

        def stop_worker_process(self, worker_id: str, status: str = "stopped"):
            _ = worker_id, status

        def reconcile_stale_requests(self, max_age_minutes: int) -> int:
            assert max_age_minutes == 45
            return 1

        def claim_next_due_request(self, worker_id: str, *, lease_token: str | None = None, lease_seconds: int | None = None):
            assert worker_id == "local-request-worker"
            assert lease_token
            assert lease_seconds == 180
            return claimed.pop(0) if claimed else None

        def release_request_lease(self, *, request_id: str, worker_id: str, lease_token: str):
            _ = request_id, worker_id, lease_token

        def heartbeat_request_lease(self, *, request_id: str, worker_id: str, lease_token: str, lease_seconds: int):
            _ = request_id, worker_id, lease_token, lease_seconds

    class WorkerService(CrawlService):
        def __init__(self, settings):
            self._settings = settings
            self.run_request_calls: list[str] = []

        def run_request(self, *, request_id: str, runtime_mode: str, crawl_runtime_mode: str | None = None, field_job_runtime_mode: str | None = None, graph_durability: str | None = None):
            self.run_request_calls.append(request_id)
            return {
                "requestId": request_id,
                "status": "succeeded" if request_id == "req-1" else "paused",
                "terminalReason": "completed" if request_id == "req-1" else "awaiting_confirmation",
            }

    settings = SimpleNamespace(
        crawler_v3_request_batch_limit=5,
        crawler_v3_request_poll_seconds=1,
        crawler_v3_request_stale_minutes=45,
        crawler_v3_request_worker_id="local-request-worker",
        crawler_v3_request_worker_runtime_owner="python_request_worker",
        crawler_v3_request_worker_lease_seconds=180,
        crawler_v3_request_worker_heartbeat_seconds=30,
        crawler_v3_crawl_runtime_mode="adaptive_primary",
        crawler_v3_field_job_runtime_mode="langgraph_primary",
        crawler_v3_field_job_graph_durability="sync",
    )
    service = WorkerService(settings)

    monkeypatch.setattr(pipeline_module, "get_connection", lambda settings: nullcontext(object()))
    monkeypatch.setattr(pipeline_module, "RequestGraphRepository", FakeRequestGraphRepository)

    result = service.run_request_worker(once=True, limit=3)

    assert service.run_request_calls == ["req-1", "req-2"]
    assert result["processed"] == 2
    assert result["succeeded"] == 1
    assert result["paused"] == 1
    assert result["failed"] == 0


class _QueueTriageRepository:
    def __init__(
        self,
        jobs: list[FieldJob],
        snippets_by_chapter: dict[str, list[str]] | None = None,
        pending_field_jobs: set[tuple[str, str]] | None = None,
    ):
        self.jobs = jobs
        self.snippets_by_chapter = snippets_by_chapter or {}
        self.pending_field_jobs = pending_field_jobs or set()
        self.patched: list[dict[str, object]] = []
        self.repairs: list[dict[str, object]] = []
        self.repair_jobs: list[dict[str, object]] = []
        self.completed_repair_jobs: list[dict[str, object]] = []
        self.claimable_repair_jobs: list[object] = []

    def list_queued_field_jobs_for_triage(self, *, limit: int = 200, source_slug: str | None = None, field_name: str | None = None):
        _ = limit, source_slug, field_name
        return list(self.jobs)

    def patch_queued_field_job(self, field_job_id: str, **kwargs: object) -> bool:
        self.patched.append({"field_job_id": field_job_id, **kwargs})
        return True

    def fetch_provenance_snippets(self, chapter_id: str) -> list[str]:
        return list(self.snippets_by_chapter.get(chapter_id, []))

    def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
        return (chapter_id, field_name) in self.pending_field_jobs

    def update_chapter_identity_repair(self, **kwargs: object) -> bool:
        self.repairs.append(dict(kwargs))
        return True

    def enqueue_chapter_repair_job(self, **kwargs: object) -> bool:
        self.repair_jobs.append(dict(kwargs))
        return True

    def claim_next_chapter_repair_job(self, worker_id: str, source_slug: str | None = None):
        _ = worker_id, source_slug
        return self.claimable_repair_jobs.pop(0) if self.claimable_repair_jobs else None

    def list_queued_field_jobs_for_chapter(self, chapter_id: str):
        return [job for job in self.jobs if job.chapter_id == chapter_id]

    def complete_chapter_repair_job(self, job, **kwargs: object) -> None:
        self.completed_repair_jobs.append({"job": job, **kwargs})


def _field_job(
    *,
    chapter_id: str = "chapter-1",
    chapter_slug: str = "chapter-1",
    chapter_name: str = "Alpha Test",
    field_name: str = "find_website",
    university_name: str | None = None,
    payload: dict[str, object] | None = None,
    source_slug: str | None = "alpha-main",
) -> FieldJob:
    raw_payload = dict(payload or {"sourceSlug": source_slug})
    return FieldJob(
        id=f"{chapter_id}-{field_name}",
        chapter_id=chapter_id,
        chapter_slug=chapter_slug,
        chapter_name=chapter_name,
        field_name=field_name,
        payload=raw_payload,
        attempts=0,
        max_attempts=3,
        claim_token="",
        source_base_url="https://example.org/chapters",
        website_url=None,
        instagram_url=None,
        contact_email=None,
        fraternity_slug="alpha-beta",
        source_id="source-1",
        source_slug=source_slug,
        university_name=university_name,
        crawl_run_id=11,
        field_states={},
        priority=0,
        queue_state=str(raw_payload.get("queue_state") or "actionable"),
    )


def test_reconcile_field_job_queue_cancels_invalid_historical_jobs():
    settings = SimpleNamespace(crawler_field_job_runtime_mode="langgraph_primary", crawler_field_job_graph_durability="sync")
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[_field_job(chapter_name="School of Medicine", university_name="1819")]
    )

    triage, repair = service._reconcile_field_job_queue(
        repo,
        source_slug="sigma-alpha-epsilon-main",
        field_name="find_website",
        limit=20,
        policy_pack=service._resolve_field_job_policy_pack("sigma-alpha-epsilon-main"),
    )

    assert triage["invalidCancelled"] == 1
    assert repair["reconciledHistorical"] == 1
    assert repo.patched[0]["status"] == "failed"
    assert repo.patched[0]["payload_patch"]["queueTriage"]["outcome"] == "cancel_invalid"


def test_reconcile_field_job_queue_repairs_candidate_school_into_actionable():
    settings = SimpleNamespace(crawler_field_job_runtime_mode="langgraph_primary", crawler_field_job_graph_durability="sync")
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[_field_job(university_name=None, payload={"candidateSchoolName": "Example University", "sourceSlug": "alpha-main"})]
    )

    triage, repair = service._reconcile_field_job_queue(
        repo,
        source_slug="alpha-delta-gamma-main",
        field_name="find_website",
        limit=20,
        policy_pack=service._resolve_field_job_policy_pack("alpha-delta-gamma-main"),
    )

    assert triage["actionableRetained"] == 0
    assert triage["repairQueued"] == 1
    assert repair["queued"] == 1
    assert repo.repair_jobs[0]["chapter_id"] == "chapter-1"
    assert repo.patched[0]["payload_patch"]["chapterRepair"]["state"] == "queued"


def test_reconcile_field_job_queue_defers_email_until_website_is_ready():
    settings = SimpleNamespace(
        crawler_field_job_runtime_mode="langgraph_primary",
        crawler_field_job_graph_durability="sync",
        crawler_search_require_confident_website_for_email=True,
        crawler_search_dependency_wait_seconds=420,
    )
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[
            _field_job(
                field_name="find_email",
                university_name="Example University",
            )
        ],
        pending_field_jobs={("chapter-1", "find_website")},
    )

    triage, repair = service._reconcile_field_job_queue(
        repo,
        source_slug="alpha-main",
        field_name=None,
        limit=20,
        policy_pack=service._resolve_field_job_policy_pack("alpha-main"),
    )

    assert triage["dependencyDeferred"] == 1
    assert repair["reconciledHistorical"] == 1
    assert repo.patched[0]["status"] == "queued"
    assert repo.patched[0]["scheduled_delay_seconds"] == 420
    assert repo.patched[0]["payload_patch"]["queueTriage"]["outcome"] == "defer_email_until_website"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["reasonCode"] == "dependency_wait"


def test_reconcile_field_job_queue_defers_email_when_website_is_missing_without_pending_job():
    settings = SimpleNamespace(
        crawler_field_job_runtime_mode="langgraph_primary",
        crawler_field_job_graph_durability="sync",
        crawler_search_require_confident_website_for_email=True,
        crawler_search_dependency_wait_seconds=420,
    )
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[
            _field_job(
                field_name="find_email",
                university_name="Example University",
            )
        ],
    )

    triage, repair = service._reconcile_field_job_queue(
        repo,
        source_slug="alpha-main",
        field_name=None,
        limit=20,
        policy_pack=service._resolve_field_job_policy_pack("alpha-main"),
    )

    assert triage["dependencyDeferred"] == 1
    assert repair["reconciledHistorical"] == 1
    assert repo.patched[0]["status"] == "queued"
    assert repo.patched[0]["scheduled_delay_seconds"] == 1800
    assert repo.patched[0]["payload_patch"]["queueTriage"]["outcome"] == "defer_email_without_website"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["reasonCode"] == "website_required"


def test_reconcile_field_job_queue_preserves_deferred_canonical_jobs():
    settings = SimpleNamespace(
        crawler_field_job_runtime_mode="langgraph_primary",
        crawler_field_job_graph_durability="sync",
    )
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[
            _field_job(
                field_name="find_website",
                university_name="Example University",
                payload={
                    "sourceSlug": "alpha-main",
                    "queue_state": "deferred",
                    "contactResolution": {"queueState": "deferred", "reasonCode": "provider_degraded"},
                },
            )
        ],
    )

    triage, repair = service._reconcile_field_job_queue(
        repo,
        source_slug="alpha-main",
        field_name=None,
        limit=20,
        policy_pack=service._resolve_field_job_policy_pack("alpha-main"),
    )

    assert triage["actionableRetained"] == 1
    assert repair["reconciledHistorical"] == 1
    assert repo.patched[0]["payload_patch"]["queueTriage"]["outcome"] == "keep_deferred"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["queueState"] == "deferred"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["reasonCode"] == "provider_degraded"
    assert repo.patched[0]["scheduled_delay_seconds"] is None


def test_process_chapter_repair_queue_promotes_canonical_and_unblocks_jobs():
    settings = SimpleNamespace(crawler_field_job_runtime_mode="langgraph_primary", crawler_field_job_graph_durability="sync", crawler_field_job_worker_id="repair-worker")
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[_field_job(university_name=None, payload={"candidateSchoolName": "Example University", "sourceSlug": "alpha-main"})]
    )
    repo.claimable_repair_jobs.append(
        SimpleNamespace(
            id="repair-1",
            chapter_id="chapter-1",
            chapter_slug="chapter-1",
            chapter_name="Alpha Test",
            source_slug="alpha-main",
            payload={"candidateSchoolName": "Example University", "sourceSlug": "alpha-main"},
            attempts=1,
            max_attempts=3,
            priority=0,
            claim_token="claim-1",
            repair_state="running",
            university_name=None,
            website_url=None,
            instagram_url=None,
            contact_email=None,
        )
    )

    summary = service._process_chapter_repair_queue(
        repo,
        source_slug="alpha-main",
        limit=2,
        policy_pack=service._resolve_field_job_policy_pack("alpha-delta-gamma-main"),
    )

    assert summary["running"] == 1
    assert summary["promotedToCanonical"] == 1
    assert repo.repairs[0]["university_name"] == "Example University"
    assert repo.completed_repair_jobs[0]["repair_state"] == "promoted_to_canonical_valid"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["queueState"] == "actionable"


def test_process_chapter_repair_queue_keeps_email_deferred_without_confident_website():
    settings = SimpleNamespace(
        crawler_field_job_runtime_mode="langgraph_primary",
        crawler_field_job_graph_durability="sync",
        crawler_field_job_worker_id="repair-worker",
        crawler_search_require_confident_website_for_email=True,
    )
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[
            _field_job(
                field_name="find_email",
                university_name=None,
                payload={"candidateSchoolName": "Example University", "sourceSlug": "alpha-main"},
            )
        ]
    )
    repo.claimable_repair_jobs.append(
        SimpleNamespace(
            id="repair-1",
            chapter_id="chapter-1",
            chapter_slug="chapter-1",
            chapter_name="Alpha Test",
            source_slug="alpha-main",
            payload={"candidateSchoolName": "Example University", "sourceSlug": "alpha-main"},
            attempts=1,
            max_attempts=3,
            priority=0,
            claim_token="claim-1",
            repair_state="running",
            university_name=None,
            website_url=None,
            instagram_url=None,
            contact_email=None,
        )
    )

    summary = service._process_chapter_repair_queue(
        repo,
        source_slug="alpha-main",
        limit=2,
        policy_pack=service._resolve_field_job_policy_pack("alpha-delta-gamma-main"),
    )

    assert summary["promotedToCanonical"] == 1
    assert repo.completed_repair_jobs[0]["repair_state"] == "promoted_to_canonical_valid"
    assert repo.patched[0]["payload_patch"]["queueTriage"]["outcome"] == "defer_email_without_website"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["queueState"] == "deferred"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["reasonCode"] == "website_required"
    assert repo.patched[0]["scheduled_delay_seconds"] == 1800
