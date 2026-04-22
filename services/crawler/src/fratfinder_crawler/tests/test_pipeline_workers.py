from contextlib import nullcontext
from dataclasses import replace
from types import SimpleNamespace

import fratfinder_crawler.pipeline as pipeline_module
import pytest

from fratfinder_crawler.config import Settings
from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.pipeline import (
    CrawlService,
    _balanced_kpi_weights,
    _bootstrap_confidence,
    _classify_field_job_identity,
    _coerce_int,
    _compute_balanced_score,
    _build_enrichment_shadow_context,
    _accuracy_recovery_metrics_payload,
    _all_attempted_providers_below_threshold,
    _default_epoch_report_path,
    _distribute_limit,
    _field_job_batch_delta_payload,
    _infer_university_name_for_job,
    _job_supporting_page_ready,
    _linear_slope,
    _merge_field_job_chunk_results,
    _merge_unique_texts,
    _normalize_source_slugs,
    _preflight_snapshot_is_healthy,
    _probe_queries_from_preflight,
    _provider_window_success_rate,
    _provider_window_state_from_preflight,
    _provider_order_from_settings,
    _render_epoch_report,
    _reorder_search_settings_from_window,
    _infer_repair_family,
    _safe_ratio,
    _search_settings_from_preflight,
    _select_registry_url,
    _slugify,
    _worker_id,
)


def test_distribute_limit_caps_workers_to_limit():
    assert _distribute_limit(3, 8) == [1, 1, 1]


def test_distribute_limit_balances_work_evenly():
    assert _distribute_limit(10, 4) == [3, 3, 2, 2]


def test_distribute_limit_handles_zero_limit():
    assert _distribute_limit(0, 8) == []


def test_worker_id_suffixes_only_for_multi_worker_runs():
    assert _worker_id("local-crawler-worker", 1, 1) == "local-crawler-worker"
    assert _worker_id("local-crawler-worker", 3, 8) == "local-crawler-worker-3"


def test_pipeline_numeric_helpers_cover_zero_slope_and_trend_cases():
    assert _safe_ratio(9, 3) == 3
    assert _safe_ratio(9, 0) == 0.0
    assert _linear_slope([1.0]) == 0.0
    assert _linear_slope([1.0, 3.0, 5.0]) > 0


def test_pipeline_normalization_helpers_trim_dedupe_and_slugify():
    assert _normalize_source_slugs([" alpha-main ", "", "beta-main", "alpha-main"]) == ["alpha-main", "beta-main"]
    assert _slugify("Theta Chi @ Penn State!") == "theta-chi-penn-state"
    assert _coerce_int("42") == 42
    assert _coerce_int("forty-two") is None


def test_balanced_kpi_helpers_normalize_weights_and_compute_score():
    weights = _balanced_kpi_weights('{"coverage": 2, "throughput": 1, "queue": 1, "reliability": 0}')
    assert round(sum(weights.values()), 6) == 1.0
    assert weights["coverage"] > weights["throughput"]
    assert _balanced_kpi_weights("{bad json") == {"coverage": 0.45, "throughput": 0.2, "queue": 0.2, "reliability": 0.15}

    score = _compute_balanced_score(
        {
            "anyContactRateDelta": 0.4,
            "jobsPerMinuteDelta": 5.0,
            "pagesPerRecordDelta": -0.5,
            "reviewRateDelta": 0.1,
        },
        weights,
    )
    assert score > 0


def test_registry_url_selection_prefers_chapterish_links_and_falls_back():
    selected, reason = _select_registry_url(
        {
            "chapterish_links": [
                {"url": "https://example.org/news", "text": "News"},
                {"url": "https://example.org/chapters", "text": "Find a Chapter"},
            ],
            "final_url": "https://example.org/home",
        }
    )
    assert selected == "https://example.org/chapters"
    assert reason == "chapterish_link"

    selected, reason = _select_registry_url({"final_url": "https://example.org/home"})
    assert selected == "https://example.org/home"
    assert reason == "final_url"

    selected, reason = _select_registry_url({"base": "https://example.org/base"})
    assert selected == "https://example.org/base"
    assert reason == "base_url"


def test_epoch_report_helpers_render_expected_sections():
    default_path = _default_epoch_report_path()
    assert default_path.startswith("docs/reports/ADAPTIVE_EPOCH_REPORT_")
    assert default_path.endswith(".md")

    report = _render_epoch_report(
        epochs=2,
        runtime_mode="adaptive_assisted",
        train_sources=["alpha-main"],
        eval_sources=["beta-main"],
        epoch_rows=[
            {
                "epoch": 1,
                "kpis": {
                    "recordsPerPageDelta": 0.4,
                    "pagesPerRecordDelta": -0.2,
                    "upsertRatioDelta": 0.1,
                    "jobsPerMinuteDelta": 1.5,
                    "reviewRateDelta": -0.1,
                    "anyContactRateDelta": 0.3,
                    "balancedScore": 0.25,
                },
            }
        ],
        slope={"balancedScoreSlope": 0.25},
    )

    assert "# Adaptive Train/Eval Epoch Report" in report
    assert "Train sources: `alpha-main`" in report
    assert "| Epoch | Records/Page Delta |" in report
    assert '"epoch": 1' in report


def test_bootstrap_confidence_rewards_healthy_chapterish_targets_and_clamps():
    assert round(_bootstrap_confidence(200, "chapterish_link"), 2) == 0.95
    assert round(_bootstrap_confidence(500, "final_url"), 2) == 0.45


def test_search_settings_from_preflight_promotes_recently_healthy_provider():
    settings = Settings(
        database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
        CRAWLER_SEARCH_PROVIDER="auto",
        CRAWLER_SEARCH_PROVIDER_ORDER_FREE="searxng_json,serper_api,tavily_api,duckduckgo_html,bing_html,brave_html",
    )

    reordered = _search_settings_from_preflight(
        settings,
        {
            "provider_health": {
                "searxng_json": {"attempts": 4, "successes": 0, "request_error": 4, "unavailable": 0, "success_rate": 0.0},
                "serper_api": {"attempts": 4, "successes": 0, "request_error": 4, "unavailable": 0, "success_rate": 0.0},
                "bing_html": {"attempts": 4, "successes": 4, "request_error": 0, "unavailable": 0, "success_rate": 1.0},
            }
        },
    )

    assert reordered.crawler_search_provider_order_free.split(",")[0] == "bing_html"


def test_provider_window_success_rate_aggregates_provider_health():
    assert _provider_window_success_rate(None) == 1.0
    assert _provider_window_success_rate({}) == 1.0
    assert _provider_window_success_rate(
        {
            "bing_html": {"attempts": 8, "successes": 4},
            "serper_api": {"attempts": 4, "successes": 0},
        }
    ) == 0.3333


def test_search_preflight_remains_healthy_when_probe_success_is_strong_even_if_fallback_attempts_fail(monkeypatch):
    class FakeSearchClient:
        def __init__(self, settings):
            self.settings = settings
            self._attempts = [
                {"provider": "bing_html", "status": "success", "result_count": 3},
                {"provider": "searxng_json", "status": "request_error"},
                {"provider": "serper_api", "status": "request_error"},
                {"provider": "tavily_api", "status": "request_error"},
                {"provider": "duckduckgo_html", "status": "request_error"},
                {"provider": "brave_html", "status": "request_error"},
            ]

        def search(self, query: str, max_results: int | None = None):
            _ = query, max_results
            return [SimpleNamespace(url="https://example.edu", title="Example", snippet="Example")]

        def consume_last_provider_attempts(self):
            return list(self._attempts)

    monkeypatch.setattr(pipeline_module, "SearchClient", FakeSearchClient)

    service = CrawlService(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto",
            CRAWLER_SEARCH_PREFLIGHT_MIN_SUCCESS_RATE="0.34",
        )
    )

    snapshot = service.search_preflight(probes=2)

    assert snapshot["success_rate"] == 1.0
    assert snapshot["provider_window_success_rate"] == 0.1667
    assert snapshot["healthy"] is True
    assert snapshot["viable_providers"] == ["bing_html"]


def test_search_preflight_tracks_provider_specific_failure_modes(monkeypatch):
    class FakeSearchClient:
        def __init__(self, settings):
            self.settings = settings
            self._attempts = [
                {"provider": "bing_html", "status": "success", "result_count": 3},
                {"provider": "searxng_json", "status": "low_signal", "result_count": 3, "failure_type": "low_signal_fallback"},
                {"provider": "duckduckgo_html", "status": "unavailable", "failure_type": "challenge_or_anomaly"},
            ]

        def search(self, query: str, max_results: int | None = None):
            _ = query, max_results
            return [SimpleNamespace(url="https://example.edu", title="Example", snippet="Example")]

        def consume_last_provider_attempts(self):
            return list(self._attempts)

    monkeypatch.setattr(pipeline_module, "SearchClient", FakeSearchClient)

    service = CrawlService(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto",
            CRAWLER_SEARCH_PREFLIGHT_MIN_SUCCESS_RATE="0.25",
        )
    )

    snapshot = service.search_preflight(probes=1)

    bing = snapshot["provider_health"]["bing_html"]
    searxng = snapshot["provider_health"]["searxng_json"]
    duckduckgo = snapshot["provider_health"]["duckduckgo_html"]

    assert bing["healthy"] is True
    assert bing["health_reason"] == "meets_success_threshold"
    assert searxng["low_signal"] == 1
    assert searxng["failure_types"]["low_signal_fallback"] == 1
    assert searxng["healthy"] is False
    assert searxng["health_reason"] == "low_signal_only"
    assert duckduckgo["challenge_or_anomaly"] == 1
    assert duckduckgo["failure_types"]["challenge_or_anomaly"] == 1
    assert duckduckgo["healthy"] is False
    assert duckduckgo["health_reason"] == "challenge_or_anomaly"


def test_resolve_field_job_runtime_mode_rejects_unknown_values():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="langgraph_primary", crawler_field_job_graph_durability="sync"))
    with pytest.raises(ValueError, match="Unsupported field-job runtime"):
        service._resolve_field_job_runtime_mode("unsupported")


def test_resolve_field_job_runtime_mode_uses_settings_default():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="langgraph_primary", crawler_field_job_graph_durability="sync"))
    assert service._resolve_field_job_runtime_mode(None) == "langgraph_primary"


def test_resolve_field_job_graph_durability_rejects_unknown_values():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="legacy", crawler_field_job_graph_durability="sync"))
    with pytest.raises(ValueError, match="Unsupported field-job graph durability"):
        service._resolve_field_job_graph_durability("invalid")


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
        crawler_v3_crawl_runtime_mode="adaptive_assisted",
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
        self.school_policy = None
        self.chapter_activity = None
        self.official_school_evidence_url = None
        self.patched: list[dict[str, object]] = []
        self.repairs: list[dict[str, object]] = []
        self.repair_jobs: list[dict[str, object]] = []
        self.completed_repair_jobs: list[dict[str, object]] = []
        self.claimable_repair_jobs: list[object] = []
        self.created_field_jobs: list[dict[str, object]] = []

    def backfill_field_job_typed_queue_state(self) -> dict[str, int]:
        return {"blocked_reason_populated": 0, "blocked_repairable_rows": 0}

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

    def get_school_policy(self, school_name: str | None):
        _ = school_name
        return self.school_policy

    def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None):
        _ = fraternity_slug, school_name
        return self.chapter_activity

    def get_reusable_official_school_evidence_url(self, *, fraternity_slug: str | None, school_name: str | None):
        _ = fraternity_slug, school_name
        return self.official_school_evidence_url

    def create_field_jobs(
        self,
        chapter_id: str,
        crawl_run_id: int | None,
        chapter_slug: str,
        source_slug: str | None,
        missing_fields: list[str],
    ) -> int:
        self.created_field_jobs.append(
            {
                "chapter_id": chapter_id,
                "crawl_run_id": crawl_run_id,
                "chapter_slug": chapter_slug,
                "source_slug": source_slug,
                "missing_fields": list(missing_fields),
            }
        )
        return len(missing_fields)

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


def test_classify_field_job_identity_flags_historical_noise_as_invalid():
    decision = _classify_field_job_identity(
        _field_job(chapter_name="Basketball", university_name="1887")
    )

    assert decision.validity_class == "invalid_non_chapter"


def test_infer_university_name_for_job_prefers_valid_payload_candidate():
    job = _field_job(
        chapter_name="Alpha Beta",
        university_name=None,
        payload={"candidateSchoolName": "Example University", "sourceSlug": "alpha-main"},
    )

    inferred = _infer_university_name_for_job(job, snippets=[])

    assert inferred == "Example University"


def test_infer_university_name_for_job_extracts_valid_school_from_snippet():
    job = _field_job(chapter_name="Alpha Beta", university_name=None, payload={"sourceSlug": "alpha-main"})

    inferred = _infer_university_name_for_job(
        job,
        snippets=["Connect with the Alpha Beta chapter at Example State University for rush updates."],
    )

    assert inferred == "Example State University"


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
    assert repo.patched[0]["payload_patch"]["contactResolution"]["queueState"] == "blocked_provider"
    assert repo.patched[0]["payload_patch"]["contactResolution"]["reasonCode"] == "provider_degraded"
    assert repo.patched[0]["scheduled_delay_seconds"] == 900


def test_reconcile_field_job_queue_reactivates_authoritative_provider_blocked_jobs_in_degraded_mode():
    settings = SimpleNamespace(
        crawler_field_job_runtime_mode="langgraph_primary",
        crawler_field_job_graph_durability="sync",
        crawler_search_transient_long_cooldown_seconds=900,
        crawler_search_dependency_wait_seconds=300,
    )
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[
            _field_job(
                field_name="verify_school_match",
                university_name="Example University",
                payload={
                    "sourceSlug": "alpha-main",
                    "queue_state": "blocked_provider",
                    "contactResolution": {"queueState": "blocked_provider", "reasonCode": "provider_degraded"},
                },
            )
        ],
    )
    repo.school_policy = SimpleNamespace(greek_life_status="allowed", evidence_source_type="official_school")

    triage, repair = service._reconcile_field_job_queue(
        repo,
        source_slug="alpha-main",
        field_name=None,
        limit=20,
        policy_pack=service._resolve_field_job_policy_pack("alpha-main"),
        preflight_snapshot={"healthy": False},
    )

    assert triage["providerRetryCandidatesConsidered"] == 1
    assert triage["providerRetryCandidatesAdmitted"] == 1
    assert repair["reconciledHistorical"] == 1
    assert repo.patched[0]["payload_patch"]["contactResolution"]["queueState"] == "actionable"
    assert repo.patched[0]["scheduled_delay_seconds"] == 0


def test_reconcile_field_job_queue_reactivates_dependency_blocked_job_when_support_exists():
    settings = SimpleNamespace(
        crawler_field_job_runtime_mode="langgraph_primary",
        crawler_field_job_graph_durability="sync",
        crawler_search_dependency_wait_seconds=300,
    )
    service = CrawlService(settings)
    repo = _QueueTriageRepository(
        jobs=[
            _field_job(
                field_name="find_email",
                university_name="Example University",
                payload={
                    "sourceSlug": "alpha-main",
                    "queue_state": "blocked_dependency",
                    "contactResolution": {
                        "queueState": "blocked_dependency",
                        "reasonCode": "dependency_wait",
                        "supportingPageUrl": "https://example.edu/greek/alpha",
                        "supportingPageScope": "school_affiliation_page",
                    },
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
        preflight_snapshot={"healthy": False},
    )

    assert triage["dependencyReactivatedFromExistingSupport"] == 1
    assert repair["reconciledHistorical"] == 1
    assert repo.patched[0]["payload_patch"]["contactResolution"]["queueState"] == "actionable"


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


def test_throughput_helper_metrics_extract_and_delta_correctly():
    preflight_snapshot = {
        "probe_outcomes": [
            {"query": 'site:example.edu alpha'},
            {"query": 'site:example.edu alpha'},
            {"query": 'site:example.edu beta'},
            "ignored",
        ]
    }
    assert _probe_queries_from_preflight(preflight_snapshot) == ['site:example.edu alpha', 'site:example.edu beta']
    assert _merge_unique_texts(['alpha', 'beta'], ['beta', 'gamma'], None) == ['alpha', 'beta', 'gamma']

    before = SimpleNamespace(
        complete_rows=10,
        chapter_specific_contact_rows=8,
        nationals_only_contact_rows=1,
        inactive_validated_rows=2,
        confirmed_absent_website_rows=3,
        active_rows_with_chapter_specific_email=4,
        active_rows_with_chapter_specific_instagram=5,
        active_rows_with_any_contact=6,
        total_chapters=40,
    )
    after = SimpleNamespace(
        complete_rows=14,
        chapter_specific_contact_rows=11,
        nationals_only_contact_rows=1,
        inactive_validated_rows=4,
        confirmed_absent_website_rows=5,
        active_rows_with_chapter_specific_email=6,
        active_rows_with_chapter_specific_instagram=7,
        active_rows_with_any_contact=9,
        total_chapters=40,
    )

    payload = _accuracy_recovery_metrics_payload(before)
    assert payload["complete_rows"] == 10
    assert _accuracy_recovery_metrics_payload(None)["total_chapters"] == 0

    delta = _field_job_batch_delta_payload(before, after, processed=10)
    assert delta["new_complete_rows"] == 4
    assert delta["new_inactive_validated_rows"] == 2
    assert delta["new_confirmed_absent_website_rows"] == 2
    assert delta["productive_yield"] == 0.8


def test_throughput_helper_provider_window_logic_reorders_and_detects_degradation():
    settings = Settings(
        database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
        CRAWLER_SEARCH_PROVIDER="auto",
        CRAWLER_SEARCH_PROVIDER_ORDER_FREE="searxng_json,serper_api,bing_html,duckduckgo_html",
    )
    snapshot = {
        "healthy": True,
        "provider_health": {
            "searxng_json": {"attempts": 4, "success_rate": 0.0},
            "serper_api": {"attempts": 4, "success_rate": 0.1},
            "bing_html": {"attempts": 4, "success_rate": 1.0},
            "duckduckgo_html": {"attempts": 2, "success_rate": 1.0},
        },
    }

    assert _provider_order_from_settings(settings) == ["searxng_json", "bing_html", "duckduckgo_html"]
    reordered = _reorder_search_settings_from_window(settings, snapshot, min_success_rate=0.25)
    assert _provider_order_from_settings(reordered)[:2] == ["bing_html", "duckduckgo_html"]
    assert not _all_attempted_providers_below_threshold(snapshot, settings, min_success_rate=0.25)

    collapsed = {
        "provider_health": {
            "searxng_json": {"attempts": 4, "success_rate": 0.0},
            "serper_api": {"attempts": 4, "success_rate": 0.1},
            "bing_html": {"attempts": 4, "success_rate": 0.2},
        }
    }
    assert _all_attempted_providers_below_threshold(collapsed, settings, min_success_rate=0.25)
    assert _preflight_snapshot_is_healthy(snapshot) is True
    assert _preflight_snapshot_is_healthy(None) is False
    provider_window_state = _provider_window_state_from_preflight(snapshot)
    assert provider_window_state["general_web_search"]["attempt_count"] == 14
    assert provider_window_state["authoritative_fetch"]["healthy"] is True

    explicit_challenge_snapshot = {
        "healthy": False,
        "provider_health": {
            "duckduckgo_html": {
                "attempts": 4,
                "successes": 0,
                "unavailable": 4,
                "request_error": 0,
                "challenge_or_anomaly": 3,
                "success_rate": 0.0,
            }
        },
    }
    degraded_window = _provider_window_state_from_preflight(explicit_challenge_snapshot)
    assert degraded_window["general_web_search"]["challenge_or_anomaly_count"] == 3
    assert degraded_window["general_web_search"]["providers"][0]["challenge_or_anomaly_count"] == 3


def test_throughput_helper_merges_chunk_results_and_supporting_page_readiness():
    merged = _merge_field_job_chunk_results(
        {
            "processed": 1,
            "requeued": 1,
            "failed_terminal": 0,
            "runtime_fallback_count": 0,
            "runtime_mode_used": "legacy",
            "provider_degraded_deferred": 1,
            "dependency_wait_deferred": 0,
            "supporting_page_resolved": 1,
            "supporting_page_contact_resolved": 0,
            "external_search_contact_resolved": 0,
            "mid_batch_provider_rechecks": 1,
            "mid_batch_provider_reorders": 0,
            "preflight_probe_queries": ["probe-a"],
            "chapter_search_queries": ["query-a"],
        },
        {
            "processed": 2,
            "requeued": 0,
            "failed_terminal": 1,
            "runtime_fallback_count": 1,
            "runtime_mode_used": "langgraph_primary",
            "provider_degraded_deferred": 0,
            "dependency_wait_deferred": 1,
            "supporting_page_resolved": 0,
            "supporting_page_contact_resolved": 2,
            "external_search_contact_resolved": 1,
            "mid_batch_provider_rechecks": 2,
            "mid_batch_provider_reorders": 1,
            "preflight_probe_queries": ["probe-a", "probe-b"],
            "chapter_search_queries": ["query-b"],
        },
    )
    assert merged["processed"] == 3
    assert merged["failed_terminal"] == 1
    assert merged["runtime_fallback_count"] == 1
    assert merged["runtime_mode_used"] == "langgraph_primary"
    assert merged["preflight_probe_queries"] == ["probe-a", "probe-b"]
    assert merged["chapter_search_queries"] == ["query-a", "query-b"]

    ready_by_website = replace(
        _field_job(field_name="find_email", payload={"queue_state": "deferred"}, source_slug="alpha-main"),
        website_url="https://chapter.example.edu",
        field_states={"website_url": "found"},
        queue_state="deferred",
    )

    ready_by_supporting_page = _field_job(
        chapter_id="chapter-2",
        chapter_slug="chapter-2",
        field_name="find_instagram",
        payload={"queue_state": "deferred", "contactResolution": {"supportingPageUrl": "https://school.edu/greek/sigma", "supportingPageScope": "school_affiliation_page"}},
        source_slug="alpha-main",
    )
    ready_by_supporting_page = replace(ready_by_supporting_page, field_states={"website_url": "missing"}, queue_state="deferred")

    ready_by_confirmed_absent = _field_job(
        chapter_id="chapter-3",
        chapter_slug="chapter-3",
        field_name="find_instagram",
        payload={"queue_state": "deferred", "contactResolution": {"supportingPageUrl": "https://nationals.org/chapter/alpha", "supportingPageScope": "nationals_chapter_page"}},
        source_slug="alpha-main",
    )
    ready_by_confirmed_absent = replace(
        ready_by_confirmed_absent,
        field_states={"website_url": "confirmed_absent"},
        instagram_url="https://www.instagram.com/alphachapter",
        queue_state="deferred",
    )

    not_ready = replace(
        _field_job(chapter_id="chapter-4", chapter_slug="chapter-4", field_name="find_email", payload={"queue_state": "deferred"}, source_slug="alpha-main"),
        field_states={"website_url": "missing"},
        queue_state="deferred",
    )

    assert _job_supporting_page_ready(ready_by_website) is True
    assert _job_supporting_page_ready(ready_by_supporting_page) is True
    assert _job_supporting_page_ready(ready_by_confirmed_absent) is True
    assert _job_supporting_page_ready(not_ready) is False


def test_system_baseline_returns_accuracy_queue_and_preflight(monkeypatch):
    class FakeCursor:
        def __init__(self):
            self._execute_count = 0
            self._last_query = ""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            _ = params
            self._execute_count += 1
            self._last_query = str(query)

        def fetchone(self):
            if "FROM worker_processes" in self._last_query:
                return {
                    "active_workers": 0,
                    "stale_workers": 1,
                }
            return {
                "queued_jobs": 12,
                "actionable_jobs": 5,
                "deferred_jobs": 7,
                "running_jobs": 1,
                "done_jobs": 20,
                "failed_jobs": 2,
                "updated_jobs": 9,
                "review_jobs": 3,
                "terminal_no_signal_jobs": 4,
            }

        def fetchall(self):
            if "GROUP BY 1\n                    ORDER BY 1" in self._last_query:
                return [
                    {"field_name": "find_email", "actionable_jobs": 1, "deferred_jobs": 2, "done_jobs": 3, "failed_jobs": 0},
                    {"field_name": "find_website", "actionable_jobs": 4, "deferred_jobs": 5, "done_jobs": 6, "failed_jobs": 1},
                ]
            if "AS blocked_reason" in self._last_query and "GROUP BY 1, 3" in self._last_query:
                return [
                    {"blocked_reason": "provider_degraded", "count": 4, "queue_lane": "provider_dependent_search"},
                    {"blocked_reason": "dependency_wait", "count": 2, "queue_lane": "dependency_blocked"},
                    {"blocked_reason": "queued_for_entity_repair", "count": 1, "queue_lane": "repair_backlog"},
                ]
            if "field_name," in self._last_query and "AS blocked_reason" in self._last_query and "GROUP BY 1, 2, 4" in self._last_query:
                return [
                    {"field_name": "find_website", "blocked_reason": "provider_degraded", "count": 4, "queue_lane": "provider_dependent_search"},
                    {"field_name": "find_email", "blocked_reason": "dependency_wait", "count": 2, "queue_lane": "dependency_blocked"},
                ]
            if "COALESCE(s.slug, 'unknown') AS source_slug" in self._last_query:
                return [
                    {"source_slug": "alpha-main", "queue_lane": "provider_dependent_search", "count": 4},
                    {"source_slug": "beta-main", "queue_lane": "dependency_blocked", "count": 2},
                ]
            if "AS age_bucket" in self._last_query:
                return [
                    {"age_bucket": "1h_to_6h", "count": 5},
                    {"age_bucket": "lt_1h", "count": 2},
                ]
            if "FROM chapter_evidence" in self._last_query:
                return [{"reason_code": "chapter_page_match", "count": 11}]
            if "SELECT issue, COUNT(*)::int AS count" in self._last_query:
                return [
                    {"issue": "missing_reason_code", "count": 2},
                    {"issue": "matches_national_profile", "count": 1},
                ]
            return [
                {
                    "issue": "missing_reason_code",
                    "chapter_slug": "alpha-chapter",
                    "fraternity_slug": "alpha-main",
                    "field_name": "contact_email",
                    "field_value": "alpha@example.edu",
                    "reason_code": None,
                    "supporting_page_scope": "chapter_site",
                    "national_value": None,
                }
            ]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    class FakeRepository:
        def __init__(self, connection):
            self.connection = connection

        def get_accuracy_recovery_metrics(self):
            return SimpleNamespace(
                complete_rows=10,
                chapter_specific_contact_rows=9,
                nationals_only_contact_rows=0,
                inactive_validated_rows=2,
                confirmed_absent_website_rows=3,
                active_rows_with_chapter_specific_email=4,
                active_rows_with_chapter_specific_instagram=5,
                active_rows_with_any_contact=6,
                total_chapters=40,
            )

        def list_national_profiles(self, limit=1000):
            _ = limit
            return [
                SimpleNamespace(contact_email="hq@example.org", instagram_url=None, phone=None),
                SimpleNamespace(contact_email=None, instagram_url="https://instagram.com/example", phone="555-1234"),
            ]

        def load_latest_policy_snapshot(self, **kwargs):
            _ = kwargs
            return None

        def list_queued_field_jobs_for_triage(self, **kwargs):
            _ = kwargs
            return [
                replace(
                    _field_job(
                        chapter_slug="alpha-chapter",
                        chapter_name="Alpha Chapter",
                        field_name="find_email",
                        payload={"contactResolution": {"supportingPageUrl": "https://chapter.example.edu", "supportingPageScope": "chapter_site"}},
                        source_slug="alpha-main",
                        university_name="Example University",
                    ),
                    fraternity_slug="alpha-main",
                    queue_state="actionable",
                    field_states={"website_url": "found"},
                        website_url="https://chapter.example.edu",
                    )
                ]

        def export_enrichment_observations(self, **kwargs):
            _ = kwargs
            return [
                {
                    "chapter_slug": "alpha-chapter",
                    "source_slug": "alpha-main",
                    "field_name": "find_email",
                    "recommended_action": "parse_supporting_page",
                    "deterministic_action": "parse_supporting_page",
                    "context_features": {"supporting_page_present": True, "provider_window_healthy": True},
                    "outcome": {
                        "finalState": "processed",
                        "businessSignals": {"chapter_safe_email": 1.0, "complete_row": 1.0},
                    },
                }
            ]

    monkeypatch.setattr(pipeline_module, "get_connection", lambda settings: nullcontext(FakeConnection()))
    monkeypatch.setattr(pipeline_module, "CrawlerRepository", FakeRepository)

    service = CrawlService(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder"))
    monkeypatch.setattr(service, "search_preflight", lambda probes=None: {"healthy": True, "probes": probes or 3})

    baseline = service.system_baseline(probes=4)

    assert baseline["accuracy"]["complete_rows"] == 10
    assert baseline["queue"]["queued_jobs"] == 12
    assert baseline["queue_health"]["deferred_ratio"] == 0.5833
    assert baseline["queue_health"]["provider_degraded_ratio"] == 0.3333
    assert baseline["queue_health"]["worker_liveness_alert"]["open"] is False
    assert baseline["deferred_reason_breakdown"][0]["blocked_reason"] == "provider_degraded"
    assert baseline["national_profiles"]["total_profiles"] == 2
    assert baseline["search_preflight"]["probes"] == 4
    assert baseline["provenance_audit"]["accepted_rows_missing_reason_code"] == 2
    assert baseline["enrichment_shadow"]["jobs_considered"] == 1


def test_crawl_replay_policy_surfaces_terminal_business_signals(monkeypatch):
    class FakeConnection:
        pass

    class FakeRepository:
        def __init__(self, connection):
            self.connection = connection

        def export_reward_events(self, **kwargs):
            _ = kwargs
            return [
                {"action_type": "extract_table", "reward_value": 3.0, "reward_stage": "immediate", "reward_components": {"chapters": 3.0}},
                {"action_type": "extract_table", "reward_value": 1.2, "reward_stage": "delayed", "reward_components": {"path_credit": 1.2}},
                {
                    "action_type": "terminal_reward",
                    "reward_value": 2.0,
                    "reward_stage": "terminal",
                    "reward_components": {"canonical_validated": 1.5, "inline_contact_progress": 0.5},
                },
            ]

    monkeypatch.setattr(pipeline_module, "get_connection", lambda settings: nullcontext(FakeConnection()))
    monkeypatch.setattr(pipeline_module, "CrawlerRepository", FakeRepository)

    service = CrawlService(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder"))
    monkeypatch.setattr(
        service,
        "export_crawl_observations",
        lambda **kwargs: {
            "observations": [
                {"selected_action": "extract_table", "selected_action_score": 2.5, "risk_score": 0.2, "outcome": {"recordsExtracted": 2}},
                {"selected_action": "extract_table", "selected_action_score": 2.0, "risk_score": 0.4, "outcome": {"recordsExtracted": 1}},
            ]
        },
    )

    replay = service.crawl_replay_policy(source_slug="alpha-main")

    assert replay["actions"][0]["actionType"] == "extract_table"
    assert replay["actions"][0]["avgReward"] == 2.1
    assert replay["actions"][0]["totalReward"] == 4.2
    assert replay["terminalBusinessSignals"][0]["component"] == "canonical_validated"


def test_enrichment_shadow_context_prefers_supporting_page_and_tracks_provider_window():
    job = replace(
        _field_job(
            field_name="find_email",
            payload={"contactResolution": {"supportingPageUrl": "https://chapter.example.edu", "supportingPageScope": "chapter_site"}},
            source_slug="alpha-main",
        ),
        chapter_name="Alpha Chapter",
        university_name="Example University",
        fraternity_slug="alpha-main",
        website_url="https://chapter.example.edu",
        field_states={"website_url": "found"},
    )
    provider_window_state = _provider_window_state_from_preflight({"captured_at": "2026-04-13T00:00:00Z", "healthy": True, "provider_health": {}})
    context = _build_enrichment_shadow_context(job, provider_window_state)

    assert context["supporting_page_present"] is True
    assert context["provider_window_healthy"] is True
    assert context["website_prerequisite_unmet"] is False


def test_enrichment_replay_policy_summarizes_shadow_vs_deterministic_outcomes(monkeypatch):
    class FakeConnection:
        pass

    class FakeRepository:
        def __init__(self, connection):
            self.connection = connection

        def export_enrichment_observations(self, **kwargs):
            _ = kwargs
            return [
                {
                    "chapter_slug": "alpha-example",
                    "field_name": "find_email",
                    "recommended_action": "parse_supporting_page",
                    "deterministic_action": "parse_supporting_page",
                    "outcome": {
                        "finalState": "processed",
                        "businessSignals": {"chapter_safe_email": 1.0, "complete_row": 1.0},
                    },
                },
                {
                    "chapter_slug": "beta-example",
                    "field_name": "find_instagram",
                    "recommended_action": "defer",
                    "deterministic_action": "search_social",
                    "outcome": {
                        "finalState": "requeued",
                        "businessSignals": {"provider_waste": 1.0},
                    },
                },
            ]

    monkeypatch.setattr(pipeline_module, "get_connection", lambda settings: nullcontext(FakeConnection()))
    monkeypatch.setattr(pipeline_module, "CrawlerRepository", FakeRepository)

    service = CrawlService(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder"))
    replay = service.enrichment_replay_policy(source_slug="alpha-main")

    assert replay["count"] == 2
    assert replay["agreementRate"] == 0.5
    assert replay["recommendedActions"][0]["actionType"] == "defer"
    assert replay["deterministicActions"][0]["actionType"] == "parse_supporting_page"
    assert replay["businessSignals"][0]["signal"] in {"chapter_safe_email", "complete_row", "provider_waste"}
    assert replay["samples"][0]["chapterSlug"] in {"alpha-example", "beta-example"}


def test_enrichment_policy_compare_report_breaks_down_disagreements_and_opportunities(monkeypatch):
    class FakeConnection:
        pass

    class FakeRepository:
        def __init__(self, connection):
            self.connection = connection

        def export_enrichment_observations(self, **kwargs):
            _ = kwargs
            return [
                {
                    "chapter_slug": "alpha-example",
                    "source_slug": "alpha-main",
                    "field_name": "find_email",
                    "recommended_action": "parse_supporting_page",
                    "deterministic_action": "search_web",
                    "context_features": {"supporting_page_present": True, "provider_window_healthy": True},
                    "outcome": {
                        "finalState": "processed",
                        "businessSignals": {"chapter_safe_email": 1.0},
                    },
                },
                {
                    "chapter_slug": "beta-example",
                    "source_slug": "alpha-main",
                    "field_name": "find_instagram",
                    "recommended_action": "verify_school",
                    "deterministic_action": "defer",
                    "context_features": {"supporting_page_present": False, "provider_window_healthy": False},
                    "outcome": {
                        "finalState": "requeued",
                        "businessSignals": {"provider_waste": 1.0},
                    },
                },
                {
                    "chapter_slug": "gamma-example",
                    "source_slug": "beta-main",
                    "field_name": "find_website",
                    "recommended_action": "defer",
                    "deterministic_action": "search_web",
                    "context_features": {"supporting_page_present": False, "provider_window_healthy": False},
                    "outcome": {
                        "finalState": "requeued",
                        "businessSignals": {"provider_waste": 1.0},
                    },
                },
            ]

    monkeypatch.setattr(pipeline_module, "get_connection", lambda settings: nullcontext(FakeConnection()))
    monkeypatch.setattr(pipeline_module, "CrawlerRepository", FakeRepository)

    service = CrawlService(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder"))
    report = service.enrichment_policy_compare_report(source_slug="alpha-main")

    assert report["count"] == 3
    assert report["agreementRate"] == 0.0
    assert report["recommendedAuthoritativeRate"] == 0.6667
    assert report["deterministicProviderSearchRate"] == 0.6667
    assert report["providerWasteRate"] == 0.6667
    assert report["providerWasteAuthoritativeOpportunityRate"] == 0.3333
    assert report["providerWasteDelayOpportunityRate"] == 0.3333
    assert report["byField"][0]["fieldName"] in {"find_email", "find_instagram", "find_website"}
    assert report["bySource"][0]["sourceSlug"] == "alpha-main"
    assert report["disagreements"][0]["transition"] in {
        "search_web->parse_supporting_page",
        "defer->verify_school",
        "search_web->defer",
    }
    assert report["samples"][0]["chapterSlug"] in {"alpha-example", "beta-example", "gamma-example"}


def test_enrichment_promote_verify_school_candidates_reports_and_applies(monkeypatch):
    class FakeConnection:
        pass

    class FakeRepository:
        def __init__(self, connection):
            self.connection = connection
            self.promotions = []

        def load_latest_policy_snapshot(self, **kwargs):
            _ = kwargs
            return None

        def list_queued_field_jobs_for_triage(self, **kwargs):
            _ = kwargs
            return [
                replace(
                    replace(
                        _field_job(
                        chapter_slug="alpha-example",
                        chapter_name="Alpha Chapter",
                        field_name="find_website",
                        source_slug="alpha-main",
                        university_name="Example University",
                        ),
                        crawl_run_id=12,
                    ),
                    fraternity_slug="alpha-main",
                    queue_state="deferred",
                    payload={"contactResolution": {"reasonCode": "transient_network"}},
                ),
                replace(
                    replace(
                        _field_job(
                        chapter_slug="beta-example",
                        chapter_name="Beta Chapter",
                        field_name="find_email",
                        source_slug="alpha-main",
                        university_name="Example University",
                        ),
                        crawl_run_id=12,
                    ),
                    fraternity_slug="alpha-main",
                    queue_state="actionable",
                ),
            ]

        def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
            _ = chapter_id
            _ = field_name
            return False

        def create_field_jobs(self, chapter_id, crawl_run_id, chapter_slug, source_slug, missing_fields):
            self.promotions.append((chapter_id, crawl_run_id, chapter_slug, source_slug, tuple(missing_fields)))
            return 1

    monkeypatch.setattr(pipeline_module, "get_connection", lambda settings: nullcontext(FakeConnection()))
    repo = FakeRepository(FakeConnection())
    monkeypatch.setattr(pipeline_module, "CrawlerRepository", lambda connection: repo)

    service = CrawlService(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder"))

    dry_run = service.enrichment_promote_verify_school_candidates(limit=10, include_preflight=False)
    assert dry_run["jobsConsidered"] == 1
    assert dry_run["candidateCount"] == 1
    assert dry_run["promotedVerifySchoolJobs"] == 0
    assert dry_run["samples"][0]["recommendedAction"] == "verify_school"
    assert dry_run["samples"][0]["deterministicAction"] == "defer"

    applied = service.enrichment_promote_verify_school_candidates(limit=10, include_preflight=False, apply_changes=True)
    assert applied["promotedVerifySchoolJobs"] == 1
    assert repo.promotions[0][4] == ("verify_school_match",)


def test_enrichment_promote_verify_school_candidates_requires_cached_evidence_when_provider_window_degraded(monkeypatch):
    class FakeConnection:
        pass

    class FakeRepository:
        def __init__(self, connection):
            self.connection = connection
            self.promotions = []

        def load_latest_policy_snapshot(self, **kwargs):
            _ = kwargs
            return None

        def list_queued_field_jobs_for_triage(self, **kwargs):
            _ = kwargs
            return [
                replace(
                    replace(
                        _field_job(
                            chapter_slug="cached-school",
                            chapter_name="Alpha Chapter",
                            field_name="find_website",
                            source_slug="alpha-main",
                            university_name="Cached University",
                        ),
                        crawl_run_id=12,
                    ),
                    fraternity_slug="alpha-main",
                    queue_state="actionable",
                ),
                replace(
                    replace(
                        _field_job(
                            chapter_slug="unknown-school-policy",
                            chapter_name="Gamma Chapter",
                            field_name="find_website",
                            source_slug="alpha-main",
                            university_name="Unknown Policy University",
                        ),
                        crawl_run_id=12,
                    ),
                    fraternity_slug="alpha-main",
                    queue_state="actionable",
                ),
                replace(
                    replace(
                        _field_job(
                            chapter_slug="active-chapter-cache",
                            chapter_name="Delta Chapter",
                            field_name="find_instagram",
                            source_slug="alpha-main",
                            university_name="Active Chapter University",
                        ),
                        crawl_run_id=12,
                    ),
                    fraternity_slug="alpha-main",
                    queue_state="actionable",
                ),
                replace(
                    replace(
                        _field_job(
                            chapter_slug="uncached-school",
                            chapter_name="Beta Chapter",
                            field_name="find_website",
                            source_slug="alpha-main",
                            university_name="Uncached University",
                        ),
                        crawl_run_id=12,
                    ),
                    fraternity_slug="alpha-main",
                    queue_state="actionable",
                ),
            ]

        def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
            _ = chapter_id
            _ = field_name
            return False

        def create_field_jobs(self, chapter_id, crawl_run_id, chapter_slug, source_slug, missing_fields):
            self.promotions.append((chapter_id, crawl_run_id, chapter_slug, source_slug, tuple(missing_fields)))
            return 1

        def get_school_policy(self, school_name: str | None):
            if school_name == "Cached University":
                return SimpleNamespace(
                    school_name=school_name,
                    greek_life_status="allowed",
                    evidence_source_type="official_school",
                    reason_code="school_policy_allowed",
                )
            if school_name == "Unknown Policy University":
                return SimpleNamespace(
                    school_name=school_name,
                    greek_life_status="unknown",
                    evidence_source_type="official_school",
                    reason_code="school_policy_unknown",
                )
            return None

        def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None):
            _ = fraternity_slug
            if school_name == "Active Chapter University":
                return SimpleNamespace(
                    school_name=school_name,
                    chapter_activity_status="confirmed_active",
                    evidence_source_type="official_school",
                    reason_code="chapter_active",
                )
            return None

    monkeypatch.setattr(pipeline_module, "get_connection", lambda settings: nullcontext(FakeConnection()))
    repo = FakeRepository(FakeConnection())
    monkeypatch.setattr(pipeline_module, "CrawlerRepository", lambda connection: repo)

    service = CrawlService(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder"))
    degraded_window = {
        "general_web_search": {"healthy": False, "window_success_rate": 0.1},
        "social_search": {"healthy": False, "window_success_rate": 0.1},
        "authoritative_fetch": {"healthy": True, "window_success_rate": 1.0},
    }

    report = service.enrichment_promote_verify_school_candidates(
        limit=10,
        include_preflight=False,
        provider_window_state=degraded_window,
        apply_changes=True,
    )

    assert report["jobsConsidered"] == 4
    assert report["candidateCount"] == 2
    assert report["promotedVerifySchoolJobs"] == 2
    assert {sample["chapterSlug"] for sample in report["samples"]} == {
        "cached-school",
        "active-chapter-cache",
    }


def test_infer_repair_family_prioritizes_school_normalization_and_state_prefix():
    missing_school = replace(
        _field_job(field_name="find_website", source_slug="alpha-main"),
        chapter_name="Alpha Chapter",
        university_name=None,
    )
    state_prefix = replace(
        _field_job(field_name="find_website", source_slug="alpha-main"),
        chapter_name="Missouri Alpha",
        university_name="University of Missouri",
    )

    assert _infer_repair_family(missing_school) == "school_name_normalizer"
    assert _infer_repair_family(state_prefix) == "state_prefix_resolver"
