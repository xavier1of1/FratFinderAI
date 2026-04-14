from __future__ import annotations

import logging
from dataclasses import replace
from types import SimpleNamespace

import pytest
import requests

from fratfinder_crawler.adaptive.policy import AdaptivePolicy
from fratfinder_crawler.field_jobs import (
    ActivityValidationDecision,
    CandidateMatch,
    FieldJobEngine,
    NationalsChapterEntry,
    RetryableJobError,
    SearchDocument,
    _email_local_part_has_identity,
    _email_local_part_looks_generic_office,
    _fraternity_matches,
    _instagram_looks_relevant_to_job,
    _nationals_entry_match_score,
    _normalized_match_text,
    _search_result_is_useful,
)
from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.search import SearchResult, SearchUnavailableError
from fratfinder_crawler.models import ChapterActivityRecord, SchoolPolicyRecord


class FakeRepository:
    def __init__(
        self,
        jobs: list[FieldJob],
        snippets_by_chapter: dict[str, list[str]],
        pending_field_jobs: set[tuple[str, str]] | None = None,
        latest_provenance_by_chapter: dict[str, dict[str, object]] | None = None,
    ):
        self.jobs = jobs
        self.snippets_by_chapter = snippets_by_chapter
        self.pending_field_jobs = pending_field_jobs or set()
        self.latest_provenance_by_chapter = latest_provenance_by_chapter or {}
        self.completed: list[tuple[str, dict[str, str], dict[str, str], int]] = []
        self.requeued: list[str] = []
        self.requeue_details: list[tuple[str, int, str]] = []
        self.requeue_preserve_attempt_flags: list[bool] = []
        self.failed: list[str] = []
        self.review_items: list[tuple[str, object]] = []
        self.requeue_payload_patches: list[dict[str, object]] = []
        self.claimed_source_slugs: list[str | None] = []
        self.claimed_field_names: list[str | None] = []
        self.claimed_require_confident_website_for_email: list[bool] = []
        self.claim_order: list[str] = []
        self.discovery_upserts: list[object] = []
        self.discovery_field_jobs: list[tuple[str, int, str, str, list[str]]] = []
        self.school_policies: dict[str, SchoolPolicyRecord] = {}
        self.school_policy_upserts: list[dict[str, object]] = []
        self.chapter_activities: dict[tuple[str, str], ChapterActivityRecord] = {}
        self.inactive_applied: list[dict[str, object]] = []
        self.completed_siblings: list[dict[str, object]] = []
        self.enrichment_observations: list[object] = []

    def claim_next_field_job(self, worker_id: str, source_slug: str | None = None, field_name: str | None = None, require_confident_website_for_email: bool = False) -> FieldJob | None:
        self.claimed_source_slugs.append(source_slug)
        self.claimed_field_names.append(field_name)
        self.claimed_require_confident_website_for_email.append(require_confident_website_for_email)
        if not self.jobs:
            return None
        job = self.jobs.pop(0)
        self.claim_order.append(job.field_name)
        return job

    def fetch_provenance_snippets(self, chapter_id: str) -> list[str]:
        return self.snippets_by_chapter.get(chapter_id, [])

    def fetch_latest_provenance_context(self, chapter_id: str):
        return self.latest_provenance_by_chapter.get(chapter_id)

    def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
        return (chapter_id, field_name) in self.pending_field_jobs

    def has_recent_transient_website_failures(self, chapter_id: str, min_failures: int = 2) -> bool:
        return False


    def complete_field_job(
        self,
        job: FieldJob,
        chapter_updates: dict[str, str],
        completed_payload: dict[str, str],
        field_state_updates: dict[str, str] | None = None,
        provenance_records=None,
    ) -> None:
        self.completed.append((job.id, chapter_updates, field_state_updates or {}, len(provenance_records or [])))

    def requeue_field_job(
        self,
        job: FieldJob,
        error: str,
        delay_seconds: int,
        preserve_attempt: bool = False,
        payload_patch: dict[str, object] | None = None,
    ) -> None:
        self.requeued.append(job.id)
        self.requeue_details.append((job.id, delay_seconds, error))
        self.requeue_preserve_attempt_flags.append(preserve_attempt)
        self.requeue_payload_patches.append(payload_patch or {})

    def fail_field_job_terminal(self, job: FieldJob, error: str) -> None:
        self.failed.append(job.id)

    def create_field_job_review_item(self, job: FieldJob, candidate) -> None:
        self.review_items.append((job.id, candidate))

    def load_sources(self, source_slug: str | None = None):
        if source_slug is None:
            return []
        return [
            SimpleNamespace(
                id="source-1",
                fraternity_id="frat-1",
                fraternity_slug="sigma-chi",
                source_slug=source_slug,
                source_type="html_directory",
                parser_key="directory_v1",
                base_url="https://source.example.org",
                list_path=None,
                metadata={},
            )
        ]

    def upsert_chapter_discovery(self, source, chapter):
        self.discovery_upserts.append(chapter)
        return "discovered-chapter-1"

    def insert_provenance(self, chapter_id, source_id, crawl_run_id, records):
        return None

    def create_field_jobs(self, chapter_id, crawl_run_id, chapter_slug, source_slug, missing_fields):
        self.discovery_field_jobs.append((chapter_id, crawl_run_id, chapter_slug, source_slug, list(missing_fields)))
        return len(missing_fields)

    def get_school_policy(self, school_name: str | None):
        key = (school_name or "").strip().lower()
        return self.school_policies.get(key)

    def upsert_school_policy(
        self,
        *,
        school_name: str,
        greek_life_status: str,
        confidence: float,
        evidence_url: str | None = None,
        evidence_source_type: str | None = None,
        reason_code: str | None = None,
        metadata: dict | None = None,
    ):
        self.school_policy_upserts.append(
            {
                "school_name": school_name,
                "greek_life_status": greek_life_status,
                "confidence": confidence,
                "evidence_url": evidence_url,
                "evidence_source_type": evidence_source_type,
                "reason_code": reason_code,
                "metadata": metadata or {},
            }
        )
        record = SchoolPolicyRecord(
            school_slug=(school_name or "").strip().lower().replace(" ", "-"),
            school_name=school_name,
            greek_life_status=greek_life_status,
            confidence=confidence,
            evidence_url=evidence_url,
            evidence_source_type=evidence_source_type,
            reason_code=reason_code,
            metadata=metadata or {},
            last_verified_at="2026-04-08T00:00:00+00:00",
            created_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )
        self.school_policies[(school_name or "").strip().lower()] = record
        return record

    def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None):
        return self.chapter_activities.get(((fraternity_slug or "").strip(), (school_name or "").strip().lower()))

    def upsert_chapter_activity(
        self,
        *,
        fraternity_slug: str,
        school_name: str,
        chapter_activity_status: str,
        confidence: float,
        evidence_url: str | None = None,
        evidence_source_type: str | None = None,
        reason_code: str | None = None,
        metadata: dict | None = None,
    ):
        record = ChapterActivityRecord(
            fraternity_slug=fraternity_slug,
            school_slug=(school_name or "").strip().lower().replace(" ", "-"),
            school_name=school_name,
            chapter_activity_status=chapter_activity_status,
            confidence=confidence,
            evidence_url=evidence_url,
            evidence_source_type=evidence_source_type,
            reason_code=reason_code,
            metadata=metadata or {},
            last_verified_at="2026-04-08T00:00:00+00:00",
            created_at="2026-04-08T00:00:00+00:00",
            updated_at="2026-04-08T00:00:00+00:00",
        )
        self.chapter_activities[(fraternity_slug.strip(), school_name.strip().lower())] = record
        return record

    def apply_chapter_inactive_status(self, **kwargs):
        self.inactive_applied.append(dict(kwargs))

    def complete_pending_field_jobs_for_chapter(self, **kwargs):
        self.completed_siblings.append(dict(kwargs))
        field_names = list(kwargs.get("field_names") or [])
        return len(field_names)

    def append_enrichment_observation(self, observation):
        self.enrichment_observations.append(observation)
        return len(self.enrichment_observations)

    def get_chapter_completion_signal(self, chapter_id: str):
        _ = chapter_id
        return {
            "validated_active": True,
            "chapter_safe_email": False,
            "chapter_safe_instagram": False,
            "complete_row": False,
        }


def test_generic_office_email_markers_cover_school_office_aliases():
    assert _email_local_part_looks_generic_office("ifc@truman.edu")
    assert _email_local_part_looks_generic_office("osfl@iu.edu")
    assert _email_local_part_looks_generic_office("operator@wichita.edu")
    assert _email_local_part_looks_generic_office("admission@william.jewell.edu")


class FakeSearchClient:
    def __init__(self, results_by_query: dict[str, list[SearchResult]]):
        self.results_by_query = results_by_query
        self.queries: list[str] = []

    def search(self, query: str, max_results: int | None = None) -> list[SearchResult]:
        self.queries.append(query)
        return self.results_by_query.get(query, [])


class FailingSearchClient:
    def __init__(self, error: Exception):
        self.error = error
        self.queries: list[str] = []

    def search(self, query: str, max_results: int | None = None) -> list[SearchResult]:
        self.queries.append(query)
        raise self.error


def _job(
    field_name: str,
    *,
    attempts: int = 1,
    website_url: str | None = None,
    instagram_url: str | None = None,
    contact_email: str | None = None,
    university_name: str | None = None,
    chapter_name: str | None = None,
) -> FieldJob:
    return FieldJob(
        id=f"job-{field_name}-{attempts}",
        chapter_id="chapter-1",
        chapter_slug="chapter-one",
        chapter_name=chapter_name or "Alpha Test",
        field_name=field_name,
        payload={"candidateSchoolName": "Example University", "sourceSlug": "sigma-chi-main"},
        attempts=attempts,
        max_attempts=3,
        claim_token="claim-token",
        source_base_url="https://source.example.org",
        website_url=website_url,
        instagram_url=instagram_url,
        contact_email=contact_email,
        fraternity_slug="sigma-chi",
        source_id="source-1",
        source_slug="sigma-chi-main",
        university_name=university_name,
        crawl_run_id=11,
        field_states={},
    )



def test_field_job_engine_completes_supported_find_jobs():
    repo = FakeRepository(
        jobs=[_job("find_email"), _job("find_instagram"), _job("find_website")],
        snippets_by_chapter={
            "chapter-1": [
                "Reach us at chapter@example.edu",
                "Follow https://instagram.com/chapterhouse",
                "Website https://chapter.example.edu",
            ]
        },
    )
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=10)

    assert result["processed"] == 3
    assert result["requeued"] == 0
    assert result["failed_terminal"] == 0
    assert len(repo.completed) == 3
    assert repo.claimed_source_slugs[:3] == [None, None, None]



def test_field_job_engine_bounds_no_signal_jobs_after_negative_memory():
    retryable = _job("find_email", university_name="Example University")
    repo = FakeRepository(jobs=[retryable], snippets_by_chapter={"chapter-1": ["No email in this snippet"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", base_backoff_seconds=2)

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeue_payload_patches[0]["terminal_no_signal_count"] == 1

    bounded = replace(retryable, attempts=2, id="job-find_email-bounded", payload={**retryable.payload, "terminal_no_signal_count": 1})
    repo = FakeRepository(jobs=[bounded], snippets_by_chapter={"chapter-1": ["No email in this snippet"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", base_backoff_seconds=2)

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert len(repo.completed) == 1



def test_search_documents_aborts_early_when_all_providers_are_unavailable(monkeypatch: pytest.MonkeyPatch):
    repo = FakeRepository(jobs=[], snippets_by_chapter={"chapter-1": []})
    search_client = FailingSearchClient(SearchUnavailableError("all providers unavailable"))
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )
    provider_attempts = [
        {"provider": "searxng_json", "status": "unavailable", "circuit_open": True},
        {"provider": "serper_api", "status": "unavailable", "circuit_open": True},
        {"provider": "tavily_api", "status": "unavailable", "circuit_open": True},
        {"provider": "duckduckgo_html", "status": "unavailable", "circuit_open": True},
        {"provider": "bing_html", "status": "unavailable", "circuit_open": True},
        {"provider": "brave_html", "status": "unavailable", "circuit_open": True},
    ]
    monkeypatch.setattr(engine, "_consume_provider_attempts", lambda: list(provider_attempts))

    documents = engine._search_documents(
        _job("find_website", university_name="Vanderbilt University", chapter_name="Gamma Chapter"),
        "website_fallback",
        include_existing=False,
    )

    assert documents == []
    assert len(search_client.queries) == 2
    assert engine._search_fanout_aborted is True


def test_validation_document_search_aborts_early_when_all_providers_are_unavailable(monkeypatch: pytest.MonkeyPatch):
    repo = FakeRepository(jobs=[], snippets_by_chapter={"chapter-1": []})
    search_client = FailingSearchClient(SearchUnavailableError("all providers unavailable"))
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )
    provider_attempts = [
        {"provider": "searxng_json", "status": "unavailable", "circuit_open": True},
        {"provider": "serper_api", "status": "unavailable", "circuit_open": True},
        {"provider": "tavily_api", "status": "unavailable", "circuit_open": True},
        {"provider": "duckduckgo_html", "status": "unavailable", "circuit_open": True},
        {"provider": "bing_html", "status": "unavailable", "circuit_open": True},
        {"provider": "brave_html", "status": "unavailable", "circuit_open": True},
    ]
    monkeypatch.setattr(engine, "_consume_provider_attempts", lambda: list(provider_attempts))

    documents = engine._build_validation_documents(
        _job("find_website", university_name="Vanderbilt University", chapter_name="Gamma Chapter"),
        target="school_chapter_list",
        query_limit=10,
        page_limit=1,
        require_official_school=True,
    )

    assert documents == []
    assert len(search_client.queries) == 2
    assert engine._search_fanout_aborted is True


def test_provider_hard_block_skips_followup_validation_and_external_searches(monkeypatch: pytest.MonkeyPatch):
    repo = FakeRepository(jobs=[], snippets_by_chapter={"chapter-1": []})
    search_client = FailingSearchClient(SearchUnavailableError("all providers unavailable"))
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )
    provider_attempts = [
        {"provider": "searxng_json", "status": "unavailable", "circuit_open": True},
        {"provider": "serper_api", "status": "unavailable", "circuit_open": True},
        {"provider": "tavily_api", "status": "unavailable", "circuit_open": True},
        {"provider": "duckduckgo_html", "status": "unavailable", "circuit_open": True},
        {"provider": "bing_html", "status": "unavailable", "circuit_open": True},
        {"provider": "brave_html", "status": "unavailable", "circuit_open": True},
    ]
    monkeypatch.setattr(engine, "_consume_provider_attempts", lambda: list(provider_attempts))
    job = _job("find_website", university_name="Vanderbilt University", chapter_name="Gamma Chapter")

    first_documents = engine._build_validation_documents(
        job,
        target="school_chapter_list",
        query_limit=10,
        page_limit=1,
        require_official_school=True,
    )
    second_documents = engine._build_validation_documents(
        job,
        target="website_school",
        query_limit=10,
        page_limit=1,
        require_official_school=True,
    )
    external_documents = engine._search_documents(job, "website_school", include_existing=False)

    assert first_documents == []
    assert second_documents == []
    assert external_documents == []
    assert engine._provider_search_hard_blocked() is True
    assert len(search_client.queries) == 2


def test_field_job_engine_forwards_field_name_filter_to_repository():
    repo = FakeRepository(
        jobs=[_job("find_instagram")],
        snippets_by_chapter={"chapter-1": ["Follow https://instagram.com/chapterhouse"]},
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        field_name="find_instagram",
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.claimed_field_names == ["find_instagram"]



def test_non_bing_claim_does_not_require_confident_website_for_email():
    repo = FakeRepository(
        jobs=[_job("find_email", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="duckduckgo_html",
        negative_result_cooldown_days=0,
    )

    engine.process(limit=1)

    assert repo.claimed_require_confident_website_for_email == [True]


def test_bing_email_waits_for_pending_website_job_without_consuming_attempt():
    job = _job("find_email", university_name="Demo University")
    repo = FakeRepository(
        jobs=[job],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
        pending_field_jobs={("chapter-1", "find_website")},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        dependency_wait_seconds=90,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeue_preserve_attempt_flags == [True]
    assert repo.requeue_details[0][1] == 90
    assert "Waiting for confident website discovery" in repo.requeue_details[0][2]
    assert search_client.queries == []


def test_search_unavailable_preserves_attempts_for_instagram_jobs():
    job = _job("find_instagram", university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["No social links here"]})
    search_client = FailingSearchClient(SearchUnavailableError("provider unavailable"))
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        base_backoff_seconds=10,
        dependency_wait_seconds=45,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeue_preserve_attempt_flags == [True]
    assert repo.requeue_details[0][1] == 45
    assert "search provider or network unavailable" in repo.requeue_details[0][2]
    assert len(search_client.queries) > 2


def test_degraded_mode_skips_external_search_and_requeues_with_long_cooldown():
    job = _job("find_website", university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["No website in provenance"]})
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        transient_long_cooldown_seconds=600,
        dependency_wait_seconds=30,
        search_degraded_mode=True,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert search_client.queries == []
    assert repo.requeue_preserve_attempt_flags == [True]
    assert repo.requeue_details[0][1] == 600
    assert "search preflight degraded" in repo.requeue_details[0][2]
    assert repo.requeue_payload_patches[0]["contactResolution"]["reasonCode"] == "provider_degraded"


def test_degraded_mode_still_uses_existing_authoritative_context_before_requeue():
    job = _job("find_instagram", university_name="Demo University")
    repo = FakeRepository(
        jobs=[job],
        snippets_by_chapter={"chapter-1": ["Follow us at https://instagram.com/sigmachi_demochapter"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_degraded_mode=True,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert search_client.queries == []
    assert len(repo.completed) == 1


def test_degraded_mode_skips_trusted_school_email_search_helpers():
    job = _job("find_email", university_name="Demo University")
    repo = FakeRepository(jobs=[], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_degraded_mode=True,
    )

    matches = engine._extract_email_matches_from_trusted_school_pages(job)

    assert matches == []
    assert search_client.queries == []


def test_search_request_exception_preserves_attempts_for_instagram_jobs():
    job = _job("find_instagram", university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["No social links here"]})
    search_client = FailingSearchClient(requests.RequestException("socket blocked"))
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        base_backoff_seconds=12,
        dependency_wait_seconds=30,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeue_preserve_attempt_flags == [True]
    assert repo.requeue_details[0][1] == 30
    assert "search provider or network unavailable" in repo.requeue_details[0][2]
    assert len(search_client.queries) > 2



def test_preserve_attempt_error_does_not_terminal_fail_at_max_attempts():
    job = _job("find_website", attempts=3, university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["No website in provenance"]})
    search_client = FailingSearchClient(SearchUnavailableError("provider unavailable"))
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        base_backoff_seconds=6,
        dependency_wait_seconds=30,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.failed == []
    assert repo.requeue_preserve_attempt_flags == [True]
    assert repo.requeue_details[0][1] == 30
    assert len(search_client.queries) > 1


def test_transient_search_failures_escalate_to_long_cooldown():
    seeded_job = replace(
        _job("find_instagram", university_name="Demo University"),
        payload={
            "candidateSchoolName": "Example University",
            "sourceSlug": "sigma-chi-main",
            "transient_provider_failures": 2,
        },
    )
    repo = FakeRepository(jobs=[seeded_job], snippets_by_chapter={"chapter-1": ["No social links here"]})
    search_client = FailingSearchClient(SearchUnavailableError("provider unavailable"))
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        transient_short_retries=2,
        transient_long_cooldown_seconds=600,
        dependency_wait_seconds=30,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeue_details[0][1] == 600
    assert repo.requeue_preserve_attempt_flags == [True]
    assert repo.requeue_payload_patches[0]["transient_provider_failures"] == 3


def test_email_dependency_escape_allows_processing_when_website_blocked():
    class EscapeRepo(FakeRepository):
        def has_recent_transient_website_failures(self, chapter_id: str, min_failures: int = 2) -> bool:
            return True

    job = _job("find_email", university_name="Demo University")
    repo = EscapeRepo(
        jobs=[job],
        snippets_by_chapter={"chapter-1": ["Reach us at chapter@example.edu"]},
        pending_field_jobs={("chapter-1", "find_website")},
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_provider="bing_html",
        email_escape_on_provider_block=True,
        email_escape_min_website_failures=2,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.requeued == []


def test_field_job_engine_forwards_source_filter_to_repository():
    repo = FakeRepository(
        jobs=[_job("find_website")],
        snippets_by_chapter={"chapter-1": ["Website https://chapter.example.edu"]},
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        source_slug="sigma-chi-main",
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.claimed_source_slugs == ["sigma-chi-main"]


def test_extract_website_matches_rejects_cross_fraternity_candidate():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(
        _job("find_website", university_name="University of Rhode Island", chapter_name="Eta Chapter"),
        fraternity_slug="theta-chi",
    )
    document = SearchDocument(
        text="Kappa Kappa Psi official chapter listing for University of Rhode Island.",
        links=["https://www.kkpsi.org/eta"],
        url="https://www.kkpsi.org/about/chapters-districts/chapter-listing-2/",
        title="Kappa Kappa Psi Chapter Listing",
        provider="search_page",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_extract_website_matches_accepts_official_school_affiliation_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(
        _job("find_website", university_name="University of Rhode Island", chapter_name="Eta Chapter"),
        fraternity_slug="theta-chi",
    )
    document = SearchDocument(
        text="Recognized fraternity chapter profile for Theta Chi at the University of Rhode Island.",
        links=["https://fsl.uri.edu/theta-chi"],
        url="https://fsl.uri.edu/organizations",
        title="Theta Chi | University of Rhode Island Greek Life",
        provider="search_page",
    )

    matches = engine._extract_website_matches(document, job)

    assert len(matches) == 1
    assert matches[0].value == "https://fsl.uri.edu/theta-chi"



def test_verify_website_marks_job_done_on_http_200():
    repo = FakeRepository(jobs=[_job("verify_website", website_url="https://chapter.example.edu")], snippets_by_chapter={})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        head_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {}
    assert repo.completed[0][2] == {"website_url": "found"}



def test_verify_website_requeues_on_timeout():
    def raise_timeout(url, timeout, allow_redirects):
        raise requests.Timeout("timed out")

    job = _job("verify_website", website_url="https://chapter.example.edu")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", head_requester=raise_timeout)

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeued == [job.id]
    assert repo.completed == []



def test_verify_website_fails_terminal_on_max_attempts_after_server_error():
    job = _job("verify_website", attempts=3, website_url="https://chapter.example.edu")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        head_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=503),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 0, "failed_terminal": 1}
    assert repo.failed == [job.id]



def test_verify_website_clears_invalid_mailto_candidate_and_backfills_email():
    job = _job("verify_website", website_url="mailto:admin@example.org")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"website_url": None, "contact_email": "admin@example.org"}
    assert repo.completed[0][2] == {"website_url": "missing", "contact_email": "found"}


def test_process_normalizes_legacy_field_name_aliases_before_dispatch():
    job = _job("contact_email", website_url="https://chapter.example.edu")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["mailto:chapter@example.edu"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1]["contact_email"] == "chapter@example.edu"


def test_find_website_requeues_when_only_source_base_url_exists():
    job = _job("find_website", university_name="Example University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["No website in this snippet"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []



def test_field_job_reprocessing_does_not_corrupt_existing_chapter_value():
    job = _job("find_website", website_url="https://existing.example.edu")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["Website https://new.example.edu"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {}
    assert repo.completed[0][2] == {"website_url": "found"}


def test_find_website_ignores_invalid_existing_mailto_value():
    job = _job("find_website", website_url="mailto:admin@example.org", university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["Website https://chapter.example.edu"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"website_url": "https://chapter.example.edu"}
    assert repo.completed[0][2] == {"website_url": "found"}



def test_extract_website_matches_rejects_generic_school_roots_and_documents():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="The University of Texas", chapter_name="Beta Upsilon Chi"), fraternity_slug="beta-upsilon-chi")
    document = SearchDocument(
        text="Beta Upsilon Chi listed on the University of Texas site.",
        links=[
            "https://studentaffairs.uga.edu",
            "https://studentgovernment.web.baylor.edu/sites/g/files/example.pdf",
        ],
        url="https://studentaffairs.uga.edu",
        title="Beta Upsilon Chi | The University of Texas",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_extract_website_matches_keeps_specific_tier1_chapter_listing_pages():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="University of Kentucky", chapter_name="Beta Upsilon Chi"), fraternity_slug="beta-upsilon-chi")
    document = SearchDocument(
        text="Beta Upsilon Chi chapter listed by Fraternity and Sorority Life at the University of Kentucky.",
        links=["https://studentsuccess.uky.edu/fraternity-and-sorority-life/about-fraternity-and-sorority-life/chapters"],
        url="https://studentsuccess.uky.edu/fraternity-and-sorority-life/about-fraternity-and-sorority-life/chapters",
        title="Beta Upsilon Chi | University of Kentucky Fraternity and Sorority Life",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert [match.value for match in matches] == [
        "https://studentsuccess.uky.edu/fraternity-and-sorority-life/about-fraternity-and-sorority-life/chapters"
    ]


def test_extract_website_matches_rejects_ambiguous_school_generic_tier1_directory():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Denver", chapter_name="Denver Chapter"), fraternity_slug="delta-chi")
    document = SearchDocument(
        text="Delta Chi listed among student organizations at MSU Denver.",
        links=["https://www.msudenver.edu/gender-institute-teaching-advocacy/student-organizations/list-of-chapters/"],
        url="https://www.msudenver.edu/gender-institute-teaching-advocacy/student-organizations/list-of-chapters/",
        title="Delta Chi | Student Organizations | MSU Denver",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_extract_website_matches_keeps_ambiguous_school_tier1_page_with_fraternity_path_identity():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Denver", chapter_name="Denver Chapter"), fraternity_slug="delta-chi")
    document = SearchDocument(
        text="Delta Chi chapter page at Denver fraternity and sorority life.",
        links=["https://studentaffairs.du.edu/fraternity-sorority-life/chapters/delta-chi"],
        url="https://studentaffairs.du.edu/fraternity-sorority-life/chapters/delta-chi",
        title="Delta Chi | Fraternity and Sorority Life | Denver",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert [match.value for match in matches] == [
        "https://studentaffairs.du.edu/fraternity-sorority-life/chapters/delta-chi"
    ]


def test_extract_website_matches_rejects_low_signal_school_archive_pages():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Hamilton College", chapter_name="Alpha Delta Phi"), fraternity_slug="alpha-delta-phi")
    document = SearchDocument(
        text="Alpha Delta Phi at Hamilton College scholarship and prize history.",
        links=["https://www.hamilton.edu/scholarships-and-prizes/index?action=detail&id=1D296BD5-E521-DF54-2C6ACB17A8464AA3"],
        url="https://www.hamilton.edu/scholarships-and-prizes/index?action=detail&id=1D296BD5-E521-DF54-2C6ACB17A8464AA3",
        title="Alpha Delta Phi scholarship prize | Hamilton College",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_extract_website_matches_rejects_low_signal_external_shop_link_from_school_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Virginia Tech", chapter_name="Beta Upsilon Chi"), fraternity_slug="beta-upsilon-chi")
    document = SearchDocument(
        text="Beta Upsilon Chi Fraternity - Beta Alpha Chapter | Fraternity and Sorority Life | Virginia Tech",
        links=["https://shop.hokiesports.com/va-tech-hokies/hokie-gear"],
        url="https://fsl.vt.edu/organizations/chapters/BetaUpsilonChi.html",
        title="Beta Upsilon Chi Fraternity - Beta Alpha Chapter | Fraternity and Sorority Life | Virginia Tech",
        provider="search_page",
        html='<html><body><a href="https://shop.hokiesports.com/va-tech-hokies/hokie-gear">Shop Hokie Sports</a></body></html>',
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_extract_website_matches_keeps_external_link_with_website_anchor_from_school_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Virginia Tech", chapter_name="Beta Upsilon Chi"), fraternity_slug="beta-upsilon-chi")
    document = SearchDocument(
        text="Beta Upsilon Chi Fraternity - Beta Alpha Chapter | Fraternity and Sorority Life | Virginia Tech Website",
        links=["https://betaupsilonchi.example.org"],
        url="https://fsl.vt.edu/organizations/chapters/BetaUpsilonChi.html",
        title="Beta Upsilon Chi Fraternity - Beta Alpha Chapter | Fraternity and Sorority Life | Virginia Tech",
        provider="search_page",
        html='<html><body><p>Website: <a href="https://betaupsilonchi.example.org">Visit Website</a></p></body></html>',
    )

    matches = engine._extract_website_matches(document, job)

    assert [match.value for match in matches] == ["https://betaupsilonchi.example.org"]


def test_extract_website_matches_rejects_conflicting_org_national_site():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Florida State University", chapter_name="Alpha Delta Phi"), fraternity_slug="alpha-delta-phi")
    document = SearchDocument(
        text="Home - Alpha Phi Alpha Fraternity, Inc.",
        links=["https://apa1906.net/"],
        url="https://apa1906.net/",
        title="Home - Alpha Phi Alpha Fraternity, Inc.",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_fraternity_matches_requires_full_identity_for_three_letter_greek_orgs():
    job = replace(
        _job("find_website", university_name="West Texas A&M University", chapter_name="West Texas A&M Colony (active)"),
        fraternity_slug="alpha-gamma-rho",
        source_slug="alpha-gamma-rho-main",
        source_base_url="https://alphagammarho.org",
    )

    assert _fraternity_matches(job, _normalized_match_text("Alpha Gamma Rho at West Texas A&M")) is True
    assert _fraternity_matches(job, _normalized_match_text("AGR chapter at West Texas A&M")) is True
    assert _fraternity_matches(job, _normalized_match_text("Sigma Gamma Rho at West Texas A&M")) is False


def test_email_local_part_identity_rejects_two_letter_fraternity_initials_alone():
    sigma_chi_job = replace(
        _job("find_email", university_name="University of Tulsa", chapter_name="Delta Omega"),
        fraternity_slug="sigma-chi",
    )

    assert _email_local_part_has_identity("sga.ssc@utulsa.edu", sigma_chi_job) is False

    syracuse_job = replace(
        _job("find_email", university_name="Syracuse University", chapter_name="Psi Psi"),
        fraternity_slug="sigma-chi",
    )
    assert _email_local_part_has_identity("scpsscheduling@syr.edu", syracuse_job) is False


def test_email_local_part_identity_accepts_school_or_fraternity_specific_local_parts():
    agr_job = replace(
        _job("find_email", university_name="South Dakota State University", chapter_name="Alpha Phi"),
        fraternity_slug="alpha-gamma-rho",
    )
    assert _email_local_part_has_identity("alphagammarhosdsu@gmail.com", agr_job) is True

    byx_job = replace(
        _job("find_email", university_name="University of Central Oklahoma", chapter_name="University of Central Oklahoma"),
        fraternity_slug="beta-upsilon-chi",
    )
    assert _email_local_part_has_identity("ucobyx@gmail.com", byx_job) is True


def test_search_result_is_useful_rejects_partial_greek_token_overlap():
    job = replace(
        _job("find_website", university_name="West Texas A&M University", chapter_name="West Texas A&M Colony (active)"),
        fraternity_slug="alpha-gamma-rho",
        source_slug="alpha-gamma-rho-main",
        source_base_url="https://alphagammarho.org",
    )
    result = SearchResult(
        title="West Texas A&M | Sigma Gamma Rho Sorority",
        url="https://www.sigmaswregion.com/texas",
        snippet="Regional Sigma Gamma Rho sorority page for West Texas A&M.",
        provider="searxng_json",
        rank=1,
    )

    assert _search_result_is_useful(job, result, "website") is False


def test_extract_website_matches_rejects_conflicting_state_school_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="University of Oklahoma", chapter_name="Beta Upsilon Chi"), fraternity_slug="beta-upsilon-chi")
    document = SearchDocument(
        text="Beta Upsilon Chi appears in the Interfraternity Council directory for Oklahoma State University.",
        links=["https://campuslife.okstate.edu/fraternity-sorority-affairs/interfraternity-council"],
        url="https://campuslife.okstate.edu/fraternity-sorority-affairs/interfraternity-council",
        title="Interfraternity Council (IFC) | Fraternity & Sorority Life | Oklahoma State University",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_extract_website_matches_rejects_generic_school_department_page_when_chapter_name_is_school_shortname():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Northwestern University", chapter_name="Northwestern"), fraternity_slug="alpha-delta-phi")
    document = SearchDocument(
        text="Fraternity & Sorority Life is the office that represents the premiere collegiate experience at Northwestern University.",
        links=["https://www.northwestern.edu/campuslife/departments/"],
        url="https://www.northwestern.edu/campuslife/departments/",
        title="Departments of Campus Life - Northwestern University",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_extract_website_matches_rejects_trustees_statement_page_when_chapter_name_is_school_shortname():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_website", university_name="Amherst College", chapter_name="Amherst"), fraternity_slug="alpha-delta-phi")
    document = SearchDocument(
        text="Board statement and resolution on fraternities at Amherst College.",
        links=["https://www.amherst.edu/about/president-college-leadership/trustees/statements/node/546181"],
        url="https://www.amherst.edu/about/president-college-leadership/trustees/statements/node/546181",
        title="Board statement and resolution on fraternities | Trustees | Amherst College",
        provider="search_result",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_email_search_gate_rejects_generic_office_email_from_low_signal_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_email", university_name="Bowdoin College", chapter_name="Alpha Delta Phi"), fraternity_slug="alpha-delta-phi")
    document = SearchDocument(
        text="Alpha Delta Phi visiting writers series at Bowdoin College. Contact instagram@noorkhindi.event for event logistics.",
        url="https://calendar.bowdoin.edu/event/alpha-delta-phi-visiting-writers-series-presents-noor-hindi",
        title="Alpha Delta Phi visiting writers series | Bowdoin College calendar",
        provider="search_page",
    )

    assert engine._email_search_candidate_passes_gate("instagram@noorkhindi.event", document, job) is False


def test_email_search_gate_rejects_generic_office_email_without_identity_anchor():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_email", university_name="Columbia University", chapter_name="Alpha Delta Phi"), fraternity_slug="alpha-delta-phi")
    document = SearchDocument(
        text="Alpha Delta Phi listed in the Columbia IFC roster. General questions can go to reslife@columbia.edu.",
        url="https://www.cc-seas.columbia.edu/reslife/fraternity_sorority/ifc",
        title="Alpha Delta Phi | Columbia IFC",
        provider="search_page",
    )

    assert engine._email_search_candidate_passes_gate("reslife@columbia.edu", document, job) is False


def test_email_search_gate_rejects_graduate_program_office_email_on_school_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_email", university_name="Bryant University", chapter_name="Zeta Chi"), fraternity_slug="theta-chi")
    document = SearchDocument(
        text="Theta Chi appears on Bryant University student involvement pages. Contact graduateprograms@bryant.edu for graduate office support.",
        url="https://www.bryant.edu/student-life/student-involvement",
        title="Theta Chi | Bryant University Student Involvement",
        provider="search_page",
    )

    assert engine._email_search_candidate_passes_gate("graduateprograms@bryant.edu", document, job) is False


def test_email_search_gate_rejects_fsl_office_email_on_official_school_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(
        _job("find_email", university_name="University of Missouri", chapter_name="Missouri Alpha (A)"),
        fraternity_slug="sigma-alpha-epsilon",
    )
    document = SearchDocument(
        text="Fraternity and Sorority Life (FSL) lists Sigma Alpha Epsilon among fraternities. Questions? Email fsl@missouri.edu.",
        url="https://fsl.missouri.edu/chapters/sigma-alpha-epsilon",
        title="Sigma Alpha Epsilon | University of Missouri FSL",
        provider="search_page",
    )

    assert engine._email_search_candidate_passes_gate("fsl@missouri.edu", document, job) is False


def test_verify_school_match_creates_review_item_on_clear_mismatch():
    job = _job("verify_school_match", university_name="Ohio State University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.review_items[0][1].item_type == "school_match_mismatch"
    assert repo.completed[0][1] == {}


def test_verify_school_match_uses_authoritative_activity_when_candidate_school_missing():
    job = replace(
        _job("verify_school_match", university_name="Ohio State University"),
        payload={"sourceSlug": "sigma-chi-main"},
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    engine._get_or_resolve_school_policy = lambda _job: ActivityValidationDecision(school_policy_status="unknown")
    engine._get_or_resolve_chapter_activity = lambda _job: ActivityValidationDecision(
        chapter_activity_status="confirmed_active",
        evidence_url="https://osu.edu/greek-life/chapters",
        evidence_source_type="official_school",
        reason_code="chapter_active",
        source_snippet="Sigma Chi is listed as an active fraternity.",
        confidence=0.98,
        metadata={"decision": "confirmed_active"},
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][2]["university_name"] == "found"


def test_verify_school_match_marks_inactive_when_authoritative_policy_banned():
    job = replace(
        _job("verify_school_match", university_name="Norwich University"),
        payload={"sourceSlug": "theta-chi-main"},
        fraternity_slug="theta-chi",
        source_slug="theta-chi-main",
        chapter_name="Alpha Chapter",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    engine._get_or_resolve_school_policy = lambda _job: ActivityValidationDecision(
        school_policy_status="banned",
        evidence_url="https://archives.norwich.edu/example.pdf",
        evidence_source_type="official_school",
        reason_code="school_policy_banned",
        source_snippet="Fraternities are no longer fraternities by any definition.",
        confidence=0.99,
        metadata={"decision": "banned"},
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.inactive_applied[0]["reason_code"] == "school_policy_banned"


def test_verify_school_match_uses_transient_network_when_authoritative_search_failed():
    job = replace(
        _job("verify_school_match", university_name="Southern Methodist University"),
        payload={"sourceSlug": "delta-kappa-epsilon-main"},
        fraternity_slug="delta-kappa-epsilon",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    engine._get_or_resolve_school_policy = lambda _job: ActivityValidationDecision(school_policy_status="unknown")
    engine._get_or_resolve_chapter_activity = lambda _job: ActivityValidationDecision(chapter_activity_status="unknown")
    engine._search_errors_encountered = True
    engine._search_queries_attempted = 2
    engine._search_queries_failed = 2
    engine._last_search_failure_kind = "unavailable"
    engine._search_fanout_aborted = True

    with pytest.raises(RetryableJobError) as exc_info:
        engine._verify_school_match(job)

    assert exc_info.value.reason_code == "transient_network"
    assert exc_info.value.preserve_attempt is True



def test_field_job_engine_finds_obfuscated_email_from_chapter_website_html():
    job = _job("find_email", website_url="https://chapter.example.edu")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["Reach the chapter leadership team online"]})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(
            status_code=200,
            text='<html><body><a href="mailto:alpha.executive@example.edu">Email</a><p>backup alpha [at] example [dot] edu</p></body></html>',
        ),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"contact_email": "alpha.executive@example.edu", "website_url": "https://chapter.example.edu"}
    assert repo.completed[0][2] == {"contact_email": "found", "website_url": "found"}
    assert repo.completed[0][3] >= 1



def test_candidate_result_does_not_backfill_generic_source_directory_url_as_website():
    base_job = _job(
        "find_instagram",
        website_url=None,
        university_name="Mercer University",
    )
    job = replace(
        base_job,
        source_base_url="https://byx.org",
        payload={**base_job.payload, "sourceListUrl": "https://byx.org/join-a-chapter"},
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine._candidate_result(
        job,
        CandidateMatch(
            value="https://www.instagram.com/examplechapter/",
            confidence=0.93,
            source_url="https://byx.org/join-a-chapter",
            source_snippet="Instagram profile for chapter",
            field_name="instagram_url",
            source_provider="provenance",
            related_website_url="https://byx.org/join-a-chapter",
        ),
        "instagram_url",
    )

    assert result.chapter_updates.get("instagram_url") == "https://www.instagram.com/examplechapter"
    assert "website_url" not in result.chapter_updates


def test_candidate_result_rejects_provenance_map_export_as_website():
    job = replace(
        _job("find_website", university_name="Louisiana State University", chapter_name="Beta Rho Chapter"),
        fraternity_slug="phi-gamma-delta",
        source_base_url="https://phigam.org/about/overview/our-chapters/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    with pytest.raises(RetryableJobError):
        engine._candidate_result(
            job,
            CandidateMatch(
                value="https://www.google.com/maps/d/kml?mid=1497z-lFQzqOBrDnwB3z0r_qiqNU&forcekml=1",
                confidence=0.8,
                source_url="https://phigam.org/about/overview/our-chapters/",
                source_snippet="Chapter map export",
                field_name="website_url",
                source_provider="provenance",
            ),
            "website_url",
        )


def test_search_result_rejects_wrong_school_same_fraternity_instagram():
    job = replace(
        _job("find_instagram", university_name="University of Pennsylvania", chapter_name="Kappa Chapter"),
        fraternity_slug="theta-chi",
    )
    result = SearchResult(
        title="Theta Chi at IUP",
        url="https://www.instagram.com/thetachi_iup/",
        snippet="Indiana University of Pennsylvania Theta Chi official Instagram.",
        provider="searxng_json",
        rank=1,
    )

    assert _search_result_is_useful(job, result, "instagram") is False


def test_field_job_engine_finds_instagram_from_chapter_website_html_and_normalizes_url():
    job = _job("find_instagram", website_url="https://chapter.example.edu", university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["Follow the chapter online"]})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(
            status_code=200,
            text='<html><body><a href="https://www.instagram.com/alphatestchapter/">Instagram</a></body></html>',
        ),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/alphatestchapter", "website_url": "https://chapter.example.edu"}
    assert repo.completed[0][2] == {"instagram_url": "found", "website_url": "found"}



def test_instagram_normalizer_rejects_mailto_artifacts():
    repo = FakeRepository(jobs=[_job("find_instagram", university_name="Demo University")], snippets_by_chapter={"chapter-1": ["mailto:chapter@example.edu"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []


def test_field_job_engine_finds_instagram_from_handle_hint_when_no_full_url_exists():
    job = _job("find_instagram", university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["Instagram: @alphatestchapter"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/alphatestchapter"}


def test_instagram_direct_probe_finds_handle_without_search_results():
    job = _job("find_instagram", university_name="Florida State University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["No social links here"]})
    search_client = FailingSearchClient(SearchUnavailableError("provider unavailable"))

    def get_requester(url, timeout, allow_redirects):
        if "instagram.com/fsusigmachi/" in url:
            return SimpleNamespace(
                status_code=200,
                text="<html><head><title>Sigma Chi FSU (@fsusigmachi) Instagram</title></head>"
                "<body>Sigma Chi at Florida State University</body></html>",
            )
        return SimpleNamespace(status_code=404, text="")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=get_requester,
        instagram_direct_probe_enabled=True,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/fsusigmachi"}
    assert search_client.queries


def test_instagram_direct_probe_rejects_missing_profile_page():
    job = _job("find_instagram", university_name="Florida State University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["No social links here"]})
    search_client = FakeSearchClient({})

    def get_requester(url, timeout, allow_redirects):
        if "instagram.com/" in url:
            return SimpleNamespace(status_code=200, text="<html><title>Instagram</title><body>Sorry, this page isn't available.</body></html>")
        return SimpleNamespace(status_code=404, text="")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=get_requester,
        instagram_max_queries=1,
        negative_result_cooldown_days=0,
        instagram_direct_probe_enabled=True,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []
    assert repo.requeue_details
    assert "No candidate instagram URL found" in repo.requeue_details[0][2]


def test_field_job_engine_prioritizes_website_jobs_before_email_and_instagram_work():
    repo = FakeRepository(
        jobs=[_job("find_website"), _job("find_email"), _job("find_instagram")],
        snippets_by_chapter={
            "chapter-1": [
                "Website https://chapter.example.edu",
                "Reach us at chapter@example.edu",
                "Instagram: @chapterhouse",
            ]
        },
    )
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=3)

    assert result == {"processed": 3, "requeued": 0, "failed_terminal": 0}
    assert repo.claim_order == ["find_website", "find_email", "find_instagram"]



def test_field_job_engine_uses_search_results_for_website_email_and_instagram():
    repo = FakeRepository(
        jobs=[
            _job("find_website", university_name="Demo University"),
            _job("find_email", university_name="Demo University"),
            _job("find_instagram", university_name="Demo University"),
        ],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            '"sigma chi" Demo University student organization site:.edu': [
                SearchResult(
                    title="Sigma Chi | Demo University Student Organizations",
                    url="https://studentorgs.demo.edu/sigma-chi",
                    snippet="Official student organization listing for Sigma Chi at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            '"sigma chi" Alpha Test Demo University email': [
                SearchResult(
                    title="Contact Sigma Chi at Demo University",
                    url="https://demosigmachi.org/contact",
                    snippet="Contact the chapter leadership team.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            '"sigma chi" Demo University email': [
                SearchResult(
                    title="Contact Sigma Chi at Demo University",
                    url="https://demosigmachi.org/contact",
                    snippet="Contact the chapter leadership team.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            'site:instagram.com "Demo University" "sigma chi"': [
                SearchResult(
                    title="Sigma Chi Demo University Instagram",
                    url="https://www.instagram.com/demosigmachi",
                    snippet="Instagram profile for the Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            '"sigma chi" Demo University instagram': [
                SearchResult(
                    title="Sigma Chi Demo University Instagram",
                    url="https://www.instagram.com/demosigmachi",
                    snippet="Instagram profile for the Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            '"sigma chi" Alpha Test Demo University contact email': [
                SearchResult(
                    title="Contact Sigma Chi at Demo University",
                    url="https://demosigmachi.org/contact",
                    snippet="Contact the chapter leadership team.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            '"sigma chi" Demo University contact email': [
                SearchResult(
                    title="Contact Sigma Chi at Demo University",
                    url="https://demosigmachi.org/contact",
                    snippet="Contact the chapter leadership team.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
        }
    )

    def get_requester(url, timeout, allow_redirects):
        if url == "https://studentorgs.demo.edu/sigma-chi":
            return SimpleNamespace(status_code=200, text='<html><body><h1>Sigma Chi at Demo University</h1><p>Official student organization listing.</p><a href="https://demosigmachi.org">Visit chapter website</a></body></html>')
        if url == "https://demosigmachi.org":
            return SimpleNamespace(status_code=200, text='<html><body><h1>Sigma Chi at Demo University</h1><a href="https://demosigmachi.org/contact">Contact</a><a href="https://www.instagram.com/demosigmachi/">Instagram</a></body></html>')
        if url == "https://demosigmachi.org/contact":
            return SimpleNamespace(status_code=200, text='<html><body><a href="mailto:alphatest@demosigmachi.org">Email</a></body></html>')
        if url == "https://www.instagram.com/demosigmachi":
            return SimpleNamespace(status_code=200, text='<html><body>Instagram profile</body></html>')
        return SimpleNamespace(status_code=404, text='')

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        get_requester=get_requester,
        max_search_pages=2,
    )

    result = engine.process(limit=3)

    assert result == {"processed": 3, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"website_url": "https://demosigmachi.org"}
    assert repo.completed[1][1]["contact_email"] == "alphatest@demosigmachi.org"
    assert repo.completed[2][1]["instagram_url"] == "https://www.instagram.com/demosigmachi"


def test_field_job_engine_rejects_reddit_bing_redirect_for_website():
    repo = FakeRepository(
        jobs=[_job("find_website", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            '"sigma chi" Alpha Test Demo University chapter website': [
                SearchResult(
                    title="Sigma Chi Demo University",
                    url="https://www.reddit.com/r/examplechapter",
                    snippet="Discussion thread",
                    provider="bing_html",
                    rank=1,
                )
            ],
            '"sigma chi" Demo University chapter website': [],
            '"sigma chi" Alpha Test Demo University official chapter site': [],
            '"sigma chi" Demo University official chapter site': [],
        }
    )

    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_client=search_client)

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []


def test_field_job_engine_finds_chapter_website_via_university_directory_page():
    repo = FakeRepository(
        jobs=[_job("find_website", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            '"sigma chi" Alpha Test Demo University chapter website': [
                SearchResult(
                    title="Sigma Chi | Demo University Student Organizations",
                    url="https://studentorgs.demo.edu/sigma-chi",
                    snippet="Official student organization listing for Sigma Chi at Demo University.",
                    provider="bing_html",
                    rank=1,
                )
            ],
            '"sigma chi" Demo University student organization site:.edu': [
                SearchResult(
                    title="Sigma Chi | Demo University Student Organizations",
                    url="https://studentorgs.demo.edu/sigma-chi",
                    snippet="Official student organization listing for Sigma Chi at Demo University.",
                    provider="bing_html",
                    rank=1,
                )
            ],
        }
    )

    def get_requester(url, timeout, allow_redirects):
        if url == "https://studentorgs.demo.edu/sigma-chi":
            return SimpleNamespace(
                status_code=200,
                text='<html><body><h1>Sigma Chi at Demo University</h1><p>Official student organization page.</p><a href="https://demosigmachi.org">Visit chapter website</a></body></html>',
            )
        if url == "https://demosigmachi.org":
            return SimpleNamespace(status_code=200, text='<html><body><h1>Sigma Chi</h1></body></html>')
        return SimpleNamespace(status_code=404, text='')

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        get_requester=get_requester,
        max_search_pages=2,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"website_url": "https://demosigmachi.org"}


def test_build_search_queries_deemphasizes_greek_letter_chapter_names():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = _job("find_website", university_name="Willamette University", chapter_name="Delta Zeta")

    school_queries = engine._build_search_queries(job, target="website_school")
    fallback_queries = engine._build_search_queries(job, target="website_fallback")

    assert '"sigma chi" Willamette University student organization site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck' in school_queries
    assert '"sigma chi" "Willamette University" fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck' in school_queries
    assert '"sigma chi" Willamette University chapter website -"sigma aldrich" -sigmaaldrich -millipore -merck' in fallback_queries
    assert all("Delta Zeta Willamette University" not in query for query in school_queries + fallback_queries)


def test_build_search_queries_normalizes_polluted_school_names_before_validation_search():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = replace(
        _job(
            "verify_school_match",
            university_name="The Ohio State University - Delta Tau",
            chapter_name="The Ohio State University - Delta Tau",
        ),
        fraternity_slug="delta-kappa-epsilon",
        payload={"sourceSlug": "delta-kappa-epsilon-main"},
    )

    queries = engine._build_search_queries(job, target="campus_policy")

    assert any('"The Ohio State University" fraternities banned site:.edu' in query for query in queries)
    assert all("Delta Tau" not in query for query in queries)


def test_bing_negative_terms_only_apply_to_sigma_fraternities():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    non_sigma_job = replace(_job("find_website", university_name="Example University"), fraternity_slug="beta-theta-pi")

    queries = engine._build_search_queries(non_sigma_job, target="website_school")

    assert all('sigma aldrich' not in query.lower() for query in queries)


def test_bing_only_instagram_job_searches_without_website_gate():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="Western Connecticut State University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            'site:instagram.com "Western Connecticut State University" "sigma chi" -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Sigma Chi at Western Connecticut State University (@wcsu_sigma_chi)",
                    url="https://www.instagram.com/wcsu_sigma_chi",
                    snippet="Instagram profile for Sigma Chi at Western Connecticut State University.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/wcsu_sigma_chi"}
    assert search_client.queries


def test_bing_only_email_job_searches_even_without_confident_website():
    repo = FakeRepository(
        jobs=[_job("find_email", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        negative_result_cooldown_days=30,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert search_client.queries
    assert repo.requeue_details[0][1] == 30 * 24 * 60 * 60
    assert "No candidate email found" in repo.requeue_details[0][2]
    assert repo.requeue_preserve_attempt_flags == [False]
    assert repo.claimed_require_confident_website_for_email == [True]



def test_email_queries_follow_ordered_funnel_and_honor_cap():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_provider="bing_html",
        email_max_queries=3,
    )
    job = _job(
        "find_email",
        website_url="https://chapter.example.edu",
        university_name="Demo University",
        chapter_name="Alpha Test",
    )

    queries = engine._build_search_queries(job, target="email")

    assert len(queries) == 3
    assert queries[0].startswith('site:chapter.example.edu "sigma chi" contact email')
    assert queries[1].startswith('site:chapter.example.edu "sigma chi" officers email')


def test_bing_email_job_prefers_chapter_website_and_avoids_search_when_email_found():
    repo = FakeRepository(
        jobs=[_job("find_email", website_url="https://chapter.example.edu", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient({})

    def get_requester(url, timeout, allow_redirects):
        if url == "https://chapter.example.edu":
            return SimpleNamespace(
                status_code=200,
                text='<html><body><h1>Sigma Chi at Demo University</h1><a href="/contact">Contact</a></body></html>',
            )
        if url == "https://chapter.example.edu/contact":
            return SimpleNamespace(
                status_code=200,
                text='<html><body><a href="mailto:chapter@demo.edu">Email</a></body></html>',
            )
        return SimpleNamespace(status_code=404, text='')

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=get_requester,
        max_search_pages=2,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1]["contact_email"] == "chapter@demo.edu"
    assert search_client.queries == []


def test_bing_email_job_can_find_email_from_trusted_school_page_without_chapter_website():
    repo = FakeRepository(
        jobs=[_job("find_email", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        email_max_queries=1,
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(
            status_code=200,
            text='<html><head><title>Demo University IFC</title></head><body><h1>Sigma Chi at Demo University</h1><a href="mailto:sigmachi-ifc@demo.edu">Contact</a></body></html>',
        ),
    )
    job = _job("find_email", university_name="Demo University")
    website_school_query = engine._build_search_queries(job, target="website_school")[0]
    search_client.results_by_query[website_school_query] = [
        SearchResult(
            title="Sigma Chi | Demo University IFC",
            url="https://ifc.demo.edu/organizations/sigma-chi",
            snippet="Official IFC chapter listing.",
            provider="bing_html",
            rank=1,
        )
    ]

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1]["contact_email"] == "sigmachi-ifc@demo.edu"
    assert any("student organization" in query for query in search_client.queries)
    assert all("contact email" not in query for query in search_client.queries)


def test_run_search_uses_query_cache():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    search_client = FakeSearchClient(
        {
            "sigma chi demo university email": [
                SearchResult(
                    title="Sigma Chi contact",
                    url="https://example.edu/sigma-chi/contact",
                    snippet="Reach the chapter officers.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )

    first = engine._run_search("sigma chi demo university email")
    second = engine._run_search("sigma chi demo university email")

    assert first == second
    assert search_client.queries == ["sigma chi demo university email"]


def test_fetch_search_document_uses_cache():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    calls: list[str] = []

    def get_requester(url, timeout, allow_redirects):
        calls.append(url)
        return SimpleNamespace(status_code=200, text="<html><head><title>Demo</title></head><body><a href='/contact'>Contact</a></body></html>")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        get_requester=get_requester,
    )

    doc_one = engine._fetch_search_document("https://chapter.demo.edu", provider="search_page", query="query-1")
    doc_two = engine._fetch_search_document("https://chapter.demo.edu", provider="chapter_website", query="query-2")

    assert doc_one is not None
    assert doc_two is not None
    assert doc_one.text == doc_two.text
    assert calls == ["https://chapter.demo.edu"]


def test_bing_email_search_rejects_nationals_only_candidate_without_school_anchor():
    repo = FakeRepository(
        jobs=[_job("find_email", website_url="https://chapter.example.edu", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            'site:chapter.example.edu "sigma chi" contact email -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Sigma Chi Nationals Contact",
                    url="https://sigmachi.org/contact",
                    snippet="Reach our office at info@sigmachi.org for national support.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        email_max_queries=1,
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=404, text=''),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []


def test_email_search_documents_skip_low_signal_page_fetches():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    search_client = FakeSearchClient({})
    fetch_calls: list[str] = []

    def get_requester(url, timeout, allow_redirects):
        fetch_calls.append(url)
        return SimpleNamespace(status_code=200, text="<html><body>generic page</body></html>")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=get_requester,
        email_max_queries=1,
    )
    job = _job("find_email", university_name="Demo University")
    query = engine._build_search_queries(job, target="email")[0]
    search_client.results_by_query[query] = [
        SearchResult(
            title="Sigma Chi at Demo University events",
            url="https://example.com/sigma-chi-events",
            snippet="Sigma Chi chapter updates from Demo University students.",
            provider="bing_html",
            rank=1,
        )
    ]

    documents = engine._search_documents(job, target="email", include_existing=False)

    assert any(document.provider == "search_result" for document in documents)
    assert all(document.provider != "search_page" for document in documents)
    assert fetch_calls == []


def test_email_search_documents_fetch_tier1_school_pages():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    search_client = FakeSearchClient({})
    fetch_calls: list[str] = []

    def get_requester(url, timeout, allow_redirects):
        fetch_calls.append(url)
        return SimpleNamespace(
            status_code=200,
            text="<html><body><h1>Sigma Chi</h1><a href='mailto:chapter@demo.edu'>Email</a></body></html>",
        )

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=get_requester,
        email_max_queries=1,
    )
    job = _job("find_email", university_name="Demo University")
    query = engine._build_search_queries(job, target="email")[0]
    search_client.results_by_query[query] = [
        SearchResult(
            title="Sigma Chi | Demo University IFC",
            url="https://ifc.demo.edu/organizations/sigma-chi",
            snippet="Official chapter listing.",
            provider="bing_html",
            rank=1,
        )
    ]

    documents = engine._search_documents(job, target="email", include_existing=False)

    assert any(document.provider == "search_page" for document in documents)
    assert fetch_calls == ["https://ifc.demo.edu/organizations/sigma-chi"]
def test_bing_only_empty_website_search_uses_long_negative_cooldown():
    repo = FakeRepository(
        jobs=[_job("find_website", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        negative_result_cooldown_days=30,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeue_details[0][1] == 30 * 24 * 60 * 60
    assert repo.completed == []


def test_bing_only_medium_confidence_website_candidate_routes_to_review_not_write():
    repo = FakeRepository(
        jobs=[_job("find_website", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            '"sigma chi" Alpha Test Demo University chapter website -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Sigma Chi at Demo University",
                    url="https://demosigmachi.org",
                    snippet="Official Sigma Chi chapter at Demo University.",
                    provider="bing_html",
                    rank=1,
                )
            ],
            '"sigma chi" Demo University chapter website -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Alpha Test Demo University official chapter site -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University official chapter site -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University student organization site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University greek life site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            'Demo University "sigma chi" fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" "Demo University" fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=404, text=''),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {}
    assert len(repo.review_items) == 1


def test_build_search_queries_include_school_only_campus_discovery_variants():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = _job("find_website", university_name="Sam Houston State University")

    queries = engine._build_search_queries(job, target="website_school")

    assert '"Sam Houston State University" student organizations site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck' in queries
    assert '"Sam Houston State University" greek life site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck' in queries
    assert '"Sam Houston State University" fraternities site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck' in queries


def test_instagram_queries_follow_ordered_funnel_and_keep_handle_queries():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = _job("find_instagram", university_name="Western Connecticut State University")

    queries = engine._build_search_queries(job, target="instagram")

    assert queries[0] == 'site:instagram.com "Western Connecticut State University" "sigma chi" -"sigma aldrich" -sigmaaldrich -millipore -merck'
    assert queries[1] == '"sigma chi" Western Connecticut State University instagram -"sigma aldrich" -sigmaaldrich -millipore -merck'
    assert 'site:instagram.com wcsusigmachi -"sigma aldrich" -sigmaaldrich -millipore -merck' in queries
    assert 'site:instagram.com sigmachiwcsu -"sigma aldrich" -sigmaaldrich -millipore -merck' in queries


def test_instagram_query_count_honors_configured_cap():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_provider="bing_html",
        instagram_max_queries=4,
    )
    job = _job("find_instagram", university_name="Western Connecticut State University")

    queries = engine._build_search_queries(job, target="instagram")

    assert len(queries) == 4


def test_instagram_queries_skip_two_letter_school_initials_by_default():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = _job("find_instagram", university_name="Boston University")

    queries = engine._build_search_queries(job, target="instagram")

    assert all("busigmachi" not in query for query in queries)
    assert all("sigmachibu" not in query for query in queries)


def test_extract_instagram_matches_rejects_placeholder_handle_without_real_chapter_identity():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_instagram", university_name="Washington State University"), fraternity_slug="sigma-chi")
    document = SearchDocument(
        text="Housing page for Washington State University Fraternity & Sorority Life.",
        links=["https://www.instagram.com/Umbraco.Cms.Core.Models.Link"],
        url="https://gogreek.wsu.edu/joining-the-community/housing/",
        title="Housing",
        provider="search_page",
    )

    matches = engine._extract_instagram_matches(document, job)

    assert matches == []


def test_extract_relaxed_authoritative_matches_rejects_generic_school_contact_page_email():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(_job("find_email", university_name="University of Utah"), fraternity_slug="phi-gamma-delta", chapter_name="Sigma Lambda Provisional Chapter")
    document = SearchDocument(
        text="Contact Us Fraternity and Sorority Life University of Utah general office greeks@utah.edu",
        links=["mailto:greeks@utah.edu"],
        url="https://fraternityandsororitylife.utah.edu/contact-us/",
        title="Contact Us - Fraternity and Sorority Life",
        provider="search_page",
    )

    relaxed_email, relaxed_instagram = engine._extract_relaxed_authoritative_matches(document, job)

    assert relaxed_email is None
    assert relaxed_instagram is None


def test_instagram_queries_keep_strong_three_plus_character_initials():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = _job("find_instagram", university_name="Florida State University")

    queries = engine._build_search_queries(job, target="instagram")

    assert 'site:instagram.com fsusigmachi -"sigma aldrich" -sigmaaldrich -millipore -merck' in queries
    assert 'site:instagram.com sigmachifsu -"sigma aldrich" -sigmaaldrich -millipore -merck' in queries


def test_instagram_queries_skip_compact_fraternity_when_token_too_short():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = replace(_job("find_instagram", university_name="Florida State University"), fraternity_slug="du")

    queries = engine._build_search_queries(job, target="instagram")

    assert all('site:instagram.com "Florida State University" du' not in query for query in queries)
    assert all("site:instagram.com fsudu" not in query for query in queries)


def test_build_search_queries_prefers_known_campus_domains_when_present():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", search_provider="bing_html")
    job = replace(
        _job("find_website", university_name="Example University"),
        payload={"candidateSchoolName": "Example University", "sourceSlug": "sigma-chi-main", "campusDomains": ["greeklife.example.edu"]},
    )

    queries = engine._build_search_queries(job, target="website_school")

    assert queries[0].endswith('site:greeklife.example.edu -"sigma aldrich" -sigmaaldrich -millipore -merck')


def test_zero_negative_cooldown_still_honors_min_backoff_floor():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No social information available"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        negative_result_cooldown_days=0,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeue_details[0][1] == 60


def test_bing_only_low_signal_website_job_hits_retry_limit_and_terminal_fails():
    repo = FakeRepository(
        jobs=[_job("find_website", attempts=2, university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        negative_result_cooldown_days=30,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 0, "failed_terminal": 1}
    assert repo.failed == ["job-find_website-2"]


def test_instagram_search_skips_low_signal_hosts_and_avoids_fetching_direct_instagram_pages():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="Florida State University")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient(
        {
            'site:instagram.com "Florida State University" "sigma chi" -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Sigma discussion on Reddit",
                    url="https://www.reddit.com/r/frat/comments/12345/sigma_chi_fsu/",
                    snippet="Discussion thread only.",
                    provider="bing_html",
                    rank=1,
                ),
                SearchResult(
                    title="FSU Sigma Chi (@fsusigmachi)",
                    url="https://www.instagram.com/fsusigmachi/",
                    snippet="Florida State University Sigma Chi official Instagram.",
                    provider="bing_html",
                    rank=2,
                ),
            ]
        }
    )

    def fail_fetch(url, *args, **kwargs):
        if "instagram.com/" in url:
            raise AssertionError("direct instagram result should not trigger page fetch")
        return SimpleNamespace(status_code=404, text="")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=fail_fetch,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/fsusigmachi"}


def test_generic_instagram_handle_is_rejected_when_school_anchor_is_missing():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient(
        {
            'site:instagram.com "Demo University" "sigma chi" -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Sigma Chi (@sigmachi)",
                    url="https://www.instagram.com/sigmachi/",
                    snippet="Official Sigma Chi Instagram.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        instagram_max_queries=1,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []
    assert repo.review_items == []


def test_instagram_candidate_with_conflicting_org_signal_is_rejected():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="University of Virginia")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient(
        {
            'site:instagram.com "University of Virginia" "sigma chi" -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Tri Sigma UVA (@trisigmauva)",
                    url="https://www.instagram.com/trisigmauva/",
                    snippet="Delta Chi chapter at University of Virginia.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="duckduckgo_html",
        instagram_max_queries=1,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []



def test_trusted_website_instagram_match_short_circuits_search():
    repo = FakeRepository(
        jobs=[_job("find_instagram", website_url="https://chapter.example.edu", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(
            status_code=200,
            text='<html><head><title>Demo University Sigma Chi</title></head><body><h1>Demo University Sigma Chi</h1><a href="https://www.instagram.com/demosigmachi/">Instagram</a></body></html>',
        ),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/demosigmachi", "website_url": "https://chapter.example.edu"}
    assert search_client.queries == []



def test_untrusted_website_instagram_hint_does_not_auto_win():
    repo = FakeRepository(
        jobs=[_job("find_instagram", website_url="https://linktr.ee/demosigmachi", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(
            status_code=200,
            text='<html><body><a href="https://www.instagram.com/demosigmachi/">Instagram</a></body></html>',
        ),
        instagram_max_queries=1,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []
    assert search_client.queries


def test_instagram_rejects_institutional_school_account_on_chapter_directory_page():
    job = replace(
        _job(
            "find_instagram",
            website_url="https://fsaffairs.illinois.edu/organizations/fraternities/DeltaChi/",
            university_name="University of Illinois",
            chapter_name="Illinois",
        ),
        fraternity_slug="delta-chi",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})
    html = """
    <html><body>
      <h1>Delta Chi | Fraternity and Sorority Affairs</h1>
      <p>Delta Chi chapter at University of Illinois.</p>
      <a href="https://www.instagram.com/illinoisfsa/">Instagram</a>
    </body></html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        instagram_max_queries=1,
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []


def test_website_rejects_cross_school_edu_link_from_official_school_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(
        _job("find_website", university_name="Norwich University", chapter_name="Alpha Chapter"),
        fraternity_slug="theta-chi",
        source_slug="theta-chi-main",
        source_base_url="https://www.thetachi.org/chapters/",
        payload={"candidateSchoolName": "Norwich University", "sourceSlug": "theta-chi-main"},
    )
    document = SearchDocument(
        text="Theta Chi on a school page. Visit chapter website.",
        links=["https://web.uri.edu/greek/fraternities/theta-chi/"],
        url="https://fsaffairs.illinois.edu/organizations/fraternities/ThetaChi",
        title="Theta Chi | Fraternity and Sorority Affairs",
        provider="search_page",
    )

    matches = engine._extract_website_matches(document, job)

    assert matches == []


def test_instagram_rejects_wrong_greek_organization_results_like_tri_sigma_uva():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="University of Virginia", chapter_name="Psi")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient(
        {
            'site:instagram.com "University of Virginia" "sigma chi" -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Tri Sigma UVA (@trisigmauva)",
                    url="https://www.instagram.com/trisigmauva/",
                    snippet="Delta Chi chapter at the University of Virginia.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        instagram_max_queries=1,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []



def test_instagram_accepts_matching_greek_chapter_designation_from_profile_context():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="Gettysburg College", chapter_name="Theta")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient(
        {
            'site:instagram.com "Gettysburg College" "sigma chi" -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Theta Chapter of Sigma Chi (@sigmachitheta)",
                    url="https://www.instagram.com/sigmachitheta/",
                    snippet="Theta Chapter of Sigma Chi Gettysburg College.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        instagram_max_queries=1,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/sigmachitheta"}



def test_instagram_miss_marks_chapter_inactive_when_official_school_list_excludes_fraternity():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="Columbia University", chapter_name="Nu Nu")],
        snippets_by_chapter={"chapter-1": []},
    )
    search_client = FakeSearchClient(
        {
            '"Columbia University" fraternities site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Fraternities and Sororities | Columbia University",
                    url="https://www.cc-seas.columbia.edu/reslife/fsl/chapters",
                    snippet="Official chapter list for Columbia University fraternities and sororities.",
                    provider="bing_html",
                    rank=1,
                )
            ],
            '"sigma chi" Columbia University fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Fraternities and Sororities | Columbia University",
                    url="https://www.cc-seas.columbia.edu/reslife/fsl/chapters",
                    snippet="Official chapter list for Columbia University fraternities and sororities.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        instagram_max_queries=1,
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(
            status_code=200,
            text='<html><body><h1>Fraternities and Sororities</h1><h2>IFC Fraternities</h2><ul><li>Alpha Epsilon Pi</li><li>Phi Gamma Delta (FIJI)</li><li>Sigma Nu</li><li>Sigma Phi Epsilon</li></ul></body></html>',
        ),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"chapter_status": "inactive"}
    assert repo.completed[0][2]["instagram_url"] == "inactive"


def test_campus_policy_validation_runs_once_per_school_and_is_reused_for_sibling_jobs():
    school_policy = SchoolPolicyRecord(
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
    repo = FakeRepository(
        jobs=[
            _job("find_website", university_name="Norwich University", chapter_name="Alpha Chapter"),
            _job("find_email", university_name="Norwich University", chapter_name="Alpha Chapter"),
        ],
        snippets_by_chapter={"chapter-1": []},
    )
    repo.school_policies["norwich university"] = school_policy
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=2)

    assert result == {"processed": 2, "requeued": 0, "failed_terminal": 0}
    assert len(repo.inactive_applied) == 2
    assert repo.requeue_details == []
    assert repo.completed[0][2]["website_url"] == "inactive"
    assert repo.completed[1][2]["contact_email"] == "inactive"


def test_invalid_entity_gate_marks_wikipedia_seeded_junk_and_cancels_siblings():
    job = replace(
        _job(
            "find_website",
            university_name="4",
            chapter_name="Cardiac & Cardiovascular Systems",
        ),
        fraternity_slug="pi-kappa-alpha",
    )
    repo = FakeRepository(
        jobs=[job],
        snippets_by_chapter={"chapter-1": []},
        pending_field_jobs={("chapter-1", "find_email"), ("chapter-1", "find_instagram")},
        latest_provenance_by_chapter={
            "chapter-1": {
                "source_url": "https://en.wikipedia.org/wiki/Duke_University",
                "source_snippet": "Academic rankings and departments for Duke University.",
                "field_name": "name",
                "confidence": 0.72,
            }
        },
    )
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {}
    assert repo.completed[0][2]["website_url"] == "invalid_entity"
    assert repo.completed_siblings[0]["status"] == "invalid_entity_filtered"
    assert repo.completed_siblings[0]["reason_code"] == "ranking_or_report_row"
    assert repo.completed_siblings[0]["field_states"]["contact_email"] == "invalid_entity"
    assert repo.completed_siblings[0]["field_states"]["instagram_url"] == "invalid_entity"


def test_campus_policy_unknown_without_official_school_evidence_stays_process_local():
    school_name = "Mystery State University"
    job_one = _job("find_website", university_name=school_name, chapter_name="Alpha Chapter")
    repo = FakeRepository(jobs=[job_one], snippets_by_chapter={"chapter-1": []})
    search_client_one = FakeSearchClient({})
    engine_one = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker-one",
        search_client=search_client_one,
        search_provider="bing_html",
    )

    first_result = engine_one.process(limit=1)

    assert first_result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.school_policy_upserts == []
    assert any("fraternities banned" in query for query in search_client_one.queries)

    repo.jobs = [_job("find_website", university_name=school_name, chapter_name="Beta Chapter")]
    search_client_two = FakeSearchClient({})
    engine_two = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker-two",
        search_client=search_client_two,
        search_provider="bing_html",
    )

    second_result = engine_two.process(limit=1)

    assert second_result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.school_policy_upserts == []
    assert any("fraternities banned" in query for query in search_client_two.queries)


def test_legacy_nonofficial_unknown_school_policy_is_ignored_and_revalidated():
    school_name = "Norwich University"
    job = _job("find_website", university_name=school_name, chapter_name="Alpha Chapter")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    repo.school_policies[school_name.lower()] = SchoolPolicyRecord(
        school_slug="norwich-university",
        school_name=school_name,
        greek_life_status="unknown",
        confidence=0.0,
        evidence_url="https://example.com/not-official",
        evidence_source_type="official_school",
        reason_code="non_official_school_source",
        metadata={},
        last_verified_at="2026-04-08T00:00:00+00:00",
        created_at="2026-04-08T00:00:00+00:00",
        updated_at="2026-04-08T00:00:00+00:00",
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert any("fraternities banned" in query for query in search_client.queries)


def test_low_confidence_existing_website_does_not_block_inactive_revalidation():
    school_name = "University of Pennsylvania"
    job = replace(
        _job(
            "find_website",
            university_name=school_name,
            chapter_name="Kappa Chapter",
            website_url="https://drexel.edu/studentlife/activities-involvement/fraternity-sorority-life/councils-and-chapters/fraternities",
        ),
        fraternity_slug="theta-chi",
        field_states={"website_url": "low_confidence"},
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    repo.chapter_activities[("theta-chi", school_name.lower())] = ChapterActivityRecord(
        fraternity_slug="theta-chi",
        school_slug="university-of-pennsylvania",
        school_name=school_name,
        chapter_activity_status="confirmed_inactive",
        confidence=0.9,
        evidence_url="https://ofsl.universitylife.upenn.edu/chapters/",
        evidence_source_type="official_school",
        reason_code="fraternity_absent_from_official_school_list",
        metadata={"sourceSnippet": "Recognized chapters at Penn"},
        last_verified_at="2026-04-08T00:00:00+00:00",
        created_at="2026-04-08T00:00:00+00:00",
        updated_at="2026-04-08T00:00:00+00:00",
    )
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.inactive_applied[0]["reason_code"] == "fraternity_absent_from_official_school_list"
    assert repo.completed[0][2]["website_url"] == "inactive"


def test_found_school_directory_website_does_not_block_inactive_revalidation():
    school_name = "University of Pennsylvania"
    job = replace(
        _job(
            "find_website",
            university_name=school_name,
            chapter_name="Kappa Chapter",
            website_url="https://drexel.edu/studentlife/activities-involvement/fraternity-sorority-life/councils-and-chapters/fraternities",
        ),
        fraternity_slug="theta-chi",
        field_states={"website_url": "found"},
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    repo.chapter_activities[("theta-chi", school_name.lower())] = ChapterActivityRecord(
        fraternity_slug="theta-chi",
        school_slug="university-of-pennsylvania",
        school_name=school_name,
        chapter_activity_status="confirmed_inactive",
        confidence=0.9,
        evidence_url="https://ofsl.universitylife.upenn.edu/chapters/",
        evidence_source_type="official_school",
        reason_code="fraternity_absent_from_official_school_list",
        metadata={"sourceSnippet": "Recognized chapters at Penn"},
        last_verified_at="2026-04-08T00:00:00+00:00",
        created_at="2026-04-08T00:00:00+00:00",
        updated_at="2026-04-08T00:00:00+00:00",
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(
            status_code=200,
            text="<html><body><h1>Fraternities</h1><p>Official school list.</p></body></html>",
        ),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.inactive_applied[0]["reason_code"] == "fraternity_absent_from_official_school_list"
    assert repo.completed[0][2]["website_url"] == "inactive"


def test_found_wrong_school_instagram_does_not_short_circuit_rerun():
    school_name = "University of Pennsylvania"
    job = replace(
        _job(
            "find_instagram",
            university_name=school_name,
            chapter_name="Kappa Chapter",
            instagram_url="https://www.instagram.com/thetachi_iup",
        ),
        fraternity_slug="theta-chi",
        field_states={"instagram_url": "found"},
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    repo.chapter_activities[("theta-chi", school_name.lower())] = ChapterActivityRecord(
        fraternity_slug="theta-chi",
        school_slug="university-of-pennsylvania",
        school_name=school_name,
        chapter_activity_status="confirmed_inactive",
        confidence=0.9,
        evidence_url="https://ofsl.universitylife.upenn.edu/chapters/",
        evidence_source_type="official_school",
        reason_code="fraternity_absent_from_official_school_list",
        metadata={"sourceSnippet": "Recognized chapters at Penn"},
        last_verified_at="2026-04-08T00:00:00+00:00",
        created_at="2026-04-08T00:00:00+00:00",
        updated_at="2026-04-08T00:00:00+00:00",
    )
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.inactive_applied[0]["reason_code"] == "fraternity_absent_from_official_school_list"
    assert repo.completed[0][2]["instagram_url"] == "inactive"


def test_authoritative_school_page_can_mark_website_confirmed_absent_and_capture_instagram():
    website_job = replace(
        _job("find_website", university_name="William Woods College", chapter_name="Kappa Chi Chapter"),
        fraternity_slug="phi-gamma-delta",
        source_slug="phi-gamma-delta-main",
        source_base_url="https://phigam.org/about/overview/our-chapters/",
        payload={"candidateSchoolName": "William Woods College", "sourceSlug": "phi-gamma-delta-main"},
    )
    repo = FakeRepository(
        jobs=[website_job],
        snippets_by_chapter={"chapter-1": []},
        pending_field_jobs={("chapter-1", "find_instagram")},
    )
    repo.chapter_activities[("phi-gamma-delta", "william woods college")] = ChapterActivityRecord(
        fraternity_slug="phi-gamma-delta",
        school_slug="william-woods-college",
        school_name="William Woods College",
        chapter_activity_status="confirmed_active",
        confidence=0.93,
        evidence_url="https://www.williamwoods.edu/fraternity-chapters",
        evidence_source_type="official_school",
        reason_code="fraternity_present_on_official_school_list",
        metadata={"sourceSnippet": "Phi Gamma Delta"},
        last_verified_at="2026-04-08T00:00:00+00:00",
        created_at="2026-04-08T00:00:00+00:00",
        updated_at="2026-04-08T00:00:00+00:00",
    )
    search_client = FakeSearchClient(
        {
            '"William Woods College" fraternities site:.edu': [
                SearchResult(
                    title="Fraternity Chapters | William Woods University",
                    url="https://www.williamwoods.edu/student-experience/undergraduate-student-experience/clubs-and-organizations/fraternity-sorority/fraternity-chapters/",
                    snippet="Fraternity chapters with Fiji on Instagram and Fiji National Website.",
                    provider="bing_html",
                    rank=1,
                )
            ],
            '"William Woods College" fraternities site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Fraternity Chapters | William Woods University",
                    url="https://www.williamwoods.edu/student-experience/undergraduate-student-experience/clubs-and-organizations/fraternity-sorority/fraternity-chapters/",
                    snippet="Fraternity chapters with Fiji on Instagram and Fiji National Website.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    html = """
    <html><body>
      <h1>Fraternity Chapters</h1>
      <section>
        <h2>Phi Gamma Delta</h2>
        <a href="https://phigam.org/">Fiji National Website</a>
        <a href="https://www.instagram.com/wwufiji/">Fiji on Instagram</a>
      </section>
    </body></html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][2]["website_url"] == "confirmed_absent"
    assert repo.completed[0][1]["instagram_url"] == "https://www.instagram.com/wwufiji"
    assert repo.completed_siblings[-1]["field_names"] == ["find_instagram"]


def test_authoritative_school_page_does_not_capture_institutional_instagram_as_chapter_contact():
    website_job = replace(
        _job("find_website", university_name="Norwich University", chapter_name="Alpha Chapter"),
        fraternity_slug="theta-chi",
        source_slug="theta-chi-main",
        source_base_url="https://www.thetachi.org/chapters/",
        payload={"candidateSchoolName": "Norwich University", "sourceSlug": "theta-chi-main"},
    )
    repo = FakeRepository(
        jobs=[website_job],
        snippets_by_chapter={"chapter-1": []},
        pending_field_jobs={("chapter-1", "find_instagram")},
    )
    repo.chapter_activities[("theta-chi", "norwich university")] = ChapterActivityRecord(
        fraternity_slug="theta-chi",
        school_slug="norwich-university",
        school_name="Norwich University",
        chapter_activity_status="confirmed_active",
        confidence=0.93,
        evidence_url="https://www.norwich.edu/student-life/",
        evidence_source_type="official_school",
        reason_code="fraternity_present_on_official_school_list",
        metadata={"sourceSnippet": "Theta Chi"},
        last_verified_at="2026-04-08T00:00:00+00:00",
        created_at="2026-04-08T00:00:00+00:00",
        updated_at="2026-04-08T00:00:00+00:00",
    )
    search_client = FakeSearchClient(
        {
            '"Norwich University" chapters at site:.edu': [
                SearchResult(
                    title="Student Life | Norwich University",
                    url="https://www.norwich.edu/student-life/",
                    snippet="Student life resources and campus involvement at Norwich University.",
                    provider="bing_html",
                    rank=1,
                )
            ]
        }
    )
    html = """
    <html><body>
      <h1>Student Life</h1>
      <a href="https://www.instagram.com/norwichuniversity/">Instagram</a>
    </body></html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][2]["website_url"] == "confirmed_absent"
    assert "instagram_url" not in repo.completed[0][1]
    assert repo.completed_siblings == []


def test_instagram_search_gate_rejects_school_branded_handle_on_official_school_page():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(
        _job("find_instagram", university_name="Norwich University", chapter_name="Alpha Chapter"),
        fraternity_slug="theta-chi",
        source_slug="theta-chi-main",
        source_base_url="https://www.thetachi.org/chapters/",
        payload={"candidateSchoolName": "Norwich University", "sourceSlug": "theta-chi-main"},
    )
    document = SearchDocument(
        text="Student Life resources at Norwich University. Follow Norwich University on Instagram.",
        links=["https://www.instagram.com/norwichuniversity/"],
        url="https://www.norwich.edu/student-life/",
        title="Student Life | Norwich University",
        provider="search_page",
        query='"Norwich University" fraternities site:.edu',
    )

    matches = engine._extract_instagram_matches(document, job)

    assert matches == []


def test_instagram_search_gate_keeps_valid_school_page_chapter_handle():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(
        _job("find_instagram", university_name="William Woods College", chapter_name="Kappa Chi Chapter"),
        fraternity_slug="phi-gamma-delta",
        source_slug="phi-gamma-delta-main",
        source_base_url="https://phigam.org/about/overview/our-chapters/",
        payload={"candidateSchoolName": "William Woods College", "sourceSlug": "phi-gamma-delta-main"},
    )
    document = SearchDocument(
        text="Fraternity Chapters Phi Gamma Delta Fiji on Instagram William Woods College.",
        links=["https://www.instagram.com/wwufiji/"],
        url="https://www.williamwoods.edu/student-experience/undergraduate-student-experience/clubs-and-organizations/fraternity-sorority/fraternity-chapters/",
        title="Fraternity Chapters | William Woods College",
        provider="search_page",
        query='"phi gamma delta" William Woods College student organization site:.edu',
    )

    matches = engine._extract_instagram_matches(document, job)

    assert [match.value for match in matches] == ["https://www.instagram.com/wwufiji"]


def test_instagram_relevance_accepts_fiji_alias_handle_with_authoritative_context():
    job = replace(
        _job("find_instagram", university_name="William Woods College", chapter_name="Kappa Chi Chapter"),
        fraternity_slug="phi-gamma-delta",
        source_slug="phi-gamma-delta-main",
        source_base_url="https://phigam.org/about/overview/our-chapters/",
        payload={"candidateSchoolName": "William Woods College", "sourceSlug": "phi-gamma-delta-main"},
    )
    document = SearchDocument(
        text="William Woods College. Phi Gamma Delta. Instagram wwufiji.",
        links=[],
        url="https://phigam.org/about/overview/our-chapters/",
        title="Our Chapters | Phi Gamma Delta",
        provider="nationals_directory",
    )

    assert _instagram_looks_relevant_to_job("https://www.instagram.com/wwufiji/", job, document=document) is True


def test_instagram_source_page_rejects_hq_handle_without_local_identity():
    repo = FakeRepository(jobs=[], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = replace(
        _job("find_instagram", university_name="Norwich University", chapter_name="Alpha Chapter"),
        fraternity_slug="theta-chi",
        source_slug="theta-chi-main",
        source_base_url="https://www.thetachi.org/chapters/",
        payload={"candidateSchoolName": "Norwich University", "sourceSlug": "theta-chi-main"},
    )
    document = SearchDocument(
        text="Theta Chi chapters include Norwich University. Follow Theta Chi HQ on Instagram for updates.",
        links=["https://www.instagram.com/thetachiihq/"],
        url="https://www.thetachi.org/chapters/",
        title="Theta Chi Chapters",
        provider="source_page",
    )

    matches = engine._extract_instagram_matches(document, job)

    assert matches == []


def test_school_validation_follows_same_host_scorecard_link_before_marking_inactive():
    job = replace(
        _job("find_website", university_name="Louisiana State University", chapter_name="Beta Rho Chapter"),
        fraternity_slug="phi-gamma-delta",
        source_slug="phi-gamma-delta-main",
        source_base_url="https://phigam.org/about/overview/our-chapters/",
        payload={"candidateSchoolName": "Louisiana State University", "sourceSlug": "phi-gamma-delta-main"},
    )
    repo = FakeRepository(jobs=[], snippets_by_chapter={"chapter-1": []})

    class CommunitySearchClient:
        def __init__(self):
            self.queries: list[str] = []

        def search(self, query: str, max_results: int | None = None):
            self.queries.append(query)
            return [
                SearchResult(
                    title="Fraternity & Sorority Community",
                    url="https://www.lsu.edu/greeks/community/index.php",
                    snippet="Our community and councils for Greek Life at LSU.",
                    provider="bing_html",
                    rank=1,
                )
            ]

    community_html = """
    <html><body>
      <h1>Our Community</h1>
      <a href="/greeks/scorecard/index.php">Community Scorecard</a>
      <a href="/greeks/councils">Councils and Chapters</a>
    </body></html>
    """
    scorecard_html = """
    <html><body>
      <h1>Community Scorecard</h1>
      <p>Active Chapters</p>
      <a href="#fraternities">Fraternities</a>
      <a href="#sororities">Sororities</a>
      <a href="#suspended">Suspended Chapters</a>
      <a href="#closed">Closed Chapters</a>
      <h3>Phi Gamma Delta</h3>
      <p>Active</p>
      <p>FIJI</p>
      <a href="/greeks/scorecard/fiji">View Scorecard</a>
      <h3>Sigma Chi</h3>
      <p>Active</p>
      <a href="/greeks/scorecard/sigma-chi">View Scorecard</a>
      <h3>Delta Chi</h3>
      <p>Active</p>
      <a href="/greeks/scorecard/delta-chi">View Scorecard</a>
    </body></html>
    """

    def get_requester(url, timeout, allow_redirects):
        normalized = url.lower().rstrip("/")
        if normalized.endswith("/greeks/community/index.php"):
            return SimpleNamespace(status_code=200, text=community_html)
        if normalized.endswith("/greeks/scorecard/index.php"):
            return SimpleNamespace(status_code=200, text=scorecard_html)
        return SimpleNamespace(status_code=404, text="")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=CommunitySearchClient(),
        search_provider="bing_html",
        get_requester=get_requester,
    )

    decision = engine._get_or_resolve_chapter_activity(job)

    assert decision.chapter_activity_status == "confirmed_active"
    assert decision.reason_code == "fraternity_present_on_official_school_list"
    assert decision.evidence_url == "https://www.lsu.edu/greeks/scorecard/index.php"


def test_chapter_activity_falls_back_to_official_school_website_queries_when_roster_queries_miss():
    job = replace(
        _job("find_website", university_name="University of Pennsylvania", chapter_name="Kappa"),
        fraternity_slug="theta-chi",
        source_slug="theta-chi-main",
        source_base_url="https://www.thetachi.org/chapters",
        payload={"candidateSchoolName": "University of Pennsylvania", "sourceSlug": "theta-chi-main"},
    )
    repo = FakeRepository(jobs=[], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})

    seed_engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )
    for query in seed_engine._build_search_queries(job, target="website_school")[:2]:
        search_client.results_by_query[query] = [
            SearchResult(
                title="Penn Fraternity & Sorority Life",
                url="https://ofsl.universitylife.upenn.edu/",
                snippet="Official fraternity and sorority life page for Penn.",
                provider="bing_html",
                rank=1,
            )
        ]

    community_html = """
    <html><body>
      <h1>Penn Fraternity & Sorority Life</h1>
      <a href="/chapters/">Chapters at Penn</a>
      <a href="/about/">About</a>
    </body></html>
    """
    chapters_html = """
    <html><body>
      <h1>Chapters at Penn</h1>
      <ul>
        <li>Alpha Chi Rho</li>
        <li>Theta Chi</li>
        <li>Phi Gamma Delta</li>
      </ul>
    </body></html>
    """

    def get_requester(url, timeout, allow_redirects):
        normalized = url.lower().rstrip("/")
        if normalized == "https://ofsl.universitylife.upenn.edu":
            return SimpleNamespace(status_code=200, text=community_html)
        if normalized == "https://ofsl.universitylife.upenn.edu/chapters":
            return SimpleNamespace(status_code=200, text=chapters_html)
        return SimpleNamespace(status_code=404, text="")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        get_requester=get_requester,
    )

    decision = engine._get_or_resolve_chapter_activity(job)

    assert decision.chapter_activity_status == "confirmed_active"
    assert decision.reason_code == "fraternity_present_on_official_school_list"
    assert decision.evidence_url == "https://ofsl.universitylife.upenn.edu/chapters/"
    assert any("student organization" in query or "greek life" in query for query in search_client.queries)


def test_instagram_falls_back_to_nationals_provenance_when_search_is_weak():
    repo = FakeRepository(
        jobs=[_job("find_instagram", university_name="Austin Peay State University")],
        snippets_by_chapter={"chapter-1": ["Follow us on Instagram: @APSUSigmaChi"]},
    )
    search_client = FakeSearchClient({})
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/APSUSigmaChi"}


def test_bing_only_tier2_website_candidate_routes_to_review_not_write():
    repo = FakeRepository(
        jobs=[_job("find_website", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            '"sigma chi" Demo University student organization site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Sigma Chi chapter links",
                    url="https://linktr.ee/demosigmachi",
                    snippet="Official links for Sigma Chi at Demo University.",
                    provider="bing_html",
                    rank=1,
                )
            ],
            'Demo University "sigma chi" fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" "Demo University" fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University greek life site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University chapter profile site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University chapter website -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University official chapter site -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {}
    assert len(repo.review_items) == 1


def test_hard_blocklist_rejects_sigma_chemistry_domains():
    repo = FakeRepository(
        jobs=[_job("find_website", university_name="Demo University")],
        snippets_by_chapter={"chapter-1": ["No direct contact information available"]},
    )
    search_client = FakeSearchClient(
        {
            '"sigma chi" Demo University student organization site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [
                SearchResult(
                    title="Sigma chemical products",
                    url="https://www.sigmaaldrich.com/example",
                    snippet="Chemistry lab supplies.",
                    provider="bing_html",
                    rank=1,
                )
            ],
            'Demo University "sigma chi" fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" "Demo University" fraternity site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University greek life site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University chapter profile site:.edu -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University chapter website -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
            '"sigma chi" Demo University official chapter site -"sigma aldrich" -sigmaaldrich -millipore -merck': [],
        }
    )
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []


def test_greedy_collect_passive_finds_instagram_from_nationals_directory_page():
    job = replace(
        _job("find_instagram", university_name="Mississippi State University"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})

    html = """
    <html>
      <body>
        <h2>MISSISSIPPI STATE CHAPTER</h2>
        <p>
          Website: <a href="https://msstatedeltachi.com">Delta Chi Mississippi State</a>
          Instagram: @msstatedeltachi
        </p>
      </body>
    </html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        greedy_collect_mode="passive",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/msstatedeltachi"}


def test_greedy_collect_none_still_uses_nationals_for_target_chapter():
    job = replace(
        _job("find_instagram", university_name="Mississippi State University"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})

    html = """
    <html>
      <body>
        <h2>MISSISSIPPI STATE CHAPTER</h2>
        <p>
          Website: <a href="https://msstatedeltachi.com">Delta Chi Mississippi State</a>
          Instagram: @msstatedeltachi
        </p>
      </body>
    </html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        greedy_collect_mode="none",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/msstatedeltachi"}
    assert repo.discovery_upserts == []


def test_greedy_collect_bfs_ingests_additional_nationals_chapter_records():
    job = replace(
        _job("find_instagram", university_name="Mississippi State University"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})

    html = """
    <html>
      <body>
        <h2>MISSISSIPPI STATE CHAPTER</h2>
        <p>Website: <a href="https://msstatedeltachi.com">Delta Chi Mississippi State</a> Instagram: @msstatedeltachi</p>
        <h2>COLORADO STATE CHAPTER</h2>
        <p>Website: <a href="https://deltachicsu.org">Delta Chi Colorado State</a> Instagram: @deltachicsu</p>
      </body>
    </html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        greedy_collect_mode="bfs",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.discovery_upserts
    assert any(item.university_name == "Colorado State" for item in repo.discovery_upserts)


def test_greedy_collect_skips_follow_on_jobs_for_low_signal_one_token_school_names():
    job = replace(
        _job("find_instagram", university_name="Mississippi State University"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})
    html = """
    <html>
      <body>
        <h2>ALBERTA CHAPTER</h2>
        <p>Website: <a href="https://www.deltachi.ca">Delta Chi Alberta</a> Instagram: @deltachialberta</p>
      </body>
    </html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        greedy_collect_mode="bfs",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.discovery_upserts
    assert repo.discovery_field_jobs == []


def test_nationals_entry_match_score_requires_school_or_chapter_signal():
    job = replace(
        _job("find_website", university_name="Jacksonville State University", chapter_name="Jacksonville State"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    wrong_entry = NationalsChapterEntry(
        chapter_name="Alberta Chapter",
        university_name="Alberta",
        website_url="http://www.deltachi.ca/",
        instagram_url="https://www.instagram.com/deltachialberta",
        contact_email=None,
        source_url="https://deltachi.org/chapter-directory/alberta/",
        source_snippet="Website Delta Chi Alberta Instagram @deltachialberta",
        confidence=0.9,
    )
    right_entry = NationalsChapterEntry(
        chapter_name="Jacksonville State Chapter",
        university_name="Jacksonville State University",
        website_url="https://jsudeltachi.example.org/",
        instagram_url=None,
        contact_email=None,
        source_url="https://deltachi.org/chapter-directory/alabama/",
        source_snippet="Jacksonville State Chapter Website Delta Chi Jacksonville State",
        confidence=0.9,
    )

    assert _nationals_entry_match_score(job, wrong_entry) == 0
    assert _nationals_entry_match_score(job, right_entry) >= 4


def test_website_nationals_directory_does_not_accept_other_chapter_entry():
    job = replace(
        _job("find_website", university_name="Jacksonville State University", chapter_name="Jacksonville State"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})
    html = """
    <html>
      <body>
        <h2>ALBERTA CHAPTER</h2>
        <p>Website: <a href="https://www.deltachi.ca">Delta Chi Alberta</a> Instagram: @deltachialberta</p>
      </body>
    </html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        greedy_collect_mode="none",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.completed == []


def test_greedy_collect_passive_follows_map_config_state_urls_and_parses_elementor_blocks():
    job = replace(
        _job("find_instagram", university_name="Mississippi State University"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})

    directory_html = """
    <html>
      <body>
        <script>
          var uscanada_config = {
            'uscanada_1': {'url':'https://deltachi.org/chapter-directory/mississippi/'}
          };
        </script>
      </body>
    </html>
    """
    mississippi_html = """
    <html>
      <body>
        <div class="elementor-widget-heading"><h2>MISSISSIPPI STATE CHAPTER</h2></div>
        <div class="elementor-widget-heading"><h3>PO Box GK Mississippi State Mississippi State, MS 39762</h3></div>
        <div class="elementor-widget-container">
          Website: <a href="http://msstatedeltachi.com/">Delta Chi Mississippi State</a>
          Facebook: <a href="https://www.facebook.com/deltachi-msu/">Delta Chi Mississippi State</a>
          Instagram: <a href="https://www.instagram.com/msstatedeltachi">@msstatedeltachi</a>
        </div>
      </body>
    </html>
    """

    def fake_get(url, timeout, allow_redirects):
        lowered = url.lower().rstrip("/")
        if lowered.endswith("chapter-directory"):
            return SimpleNamespace(status_code=200, text=directory_html)
        if lowered.endswith("chapter-directory/mississippi"):
            return SimpleNamespace(status_code=200, text=mississippi_html)
        return SimpleNamespace(status_code=404, text="")

    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        greedy_collect_mode="passive",
        get_requester=fake_get,
    )

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/msstatedeltachi"}


def test_greedy_collect_rejects_navigation_noise_blocks():
    job = replace(
        _job("find_instagram", university_name="Mississippi State University"),
        fraternity_slug="delta-chi",
        source_slug="delta-chi-main",
        source_base_url="https://deltachi.org/chapter-directory/",
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": []})
    search_client = FakeSearchClient({})
    html = """
    <html>
      <body>
        <h2>CHAPTER DIRECTORY</h2>
        <p>Directory navigation. Facebook Instagram Twitter Contact</p>
        <a href="https://deltachi.org/chapter-directory/florida/">Florida</a>
      </body>
    </html>
    """
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="bing_html",
        greedy_collect_mode="bfs",
        get_requester=lambda url, timeout, allow_redirects: SimpleNamespace(status_code=200, text=html),
    )

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert not repo.discovery_upserts


def test_empty_search_results_are_not_cached_by_default():
    first_query = 'site:instagram.com "Demo University" "sigma chi"'
    job_one = replace(
        _job("find_instagram", university_name="Demo University"),
        chapter_id="chapter-1",
        chapter_slug="chapter-one",
    )
    job_two = replace(
        _job("find_instagram", university_name="Demo University"),
        id="job-find_instagram-2",
        chapter_id="chapter-2",
        chapter_slug="chapter-two",
    )
    repo = FakeRepository(jobs=[job_one, job_two], snippets_by_chapter={"chapter-1": [], "chapter-2": []})

    class OneTimeEmptySearchClient:
        def __init__(self):
            self.calls: dict[str, int] = {}

        def search(self, query: str, max_results: int | None = None):
            self.calls[query] = self.calls.get(query, 0) + 1
            if query != first_query:
                return []
            if self.calls[query] == 1:
                return []
            return [
                SearchResult(
                    title="Sigma Chi Demo University Instagram",
                    url="https://www.instagram.com/demosigmachi",
                    snippet="Official Sigma Chi chapter account at Demo University.",
                    provider="bing_html",
                    rank=1,
                )
            ]

    search_client = OneTimeEmptySearchClient()
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        search_client=search_client,
        search_provider="duckduckgo_html",
        instagram_max_queries=1,
    )

    result = engine.process(limit=2)

    assert search_client.calls[first_query] == 2
    assert result == {"processed": 1, "requeued": 1, "failed_terminal": 0}
    assert any(update.get("instagram_url") == "https://www.instagram.com/demosigmachi" for _, update, _, _ in repo.completed)


def test_engine_logs_enrichment_observation_with_shadow_recommendation():
    job = replace(
        _job("find_email", university_name="Demo University"),
        chapter_id="chapter-obs",
        chapter_slug="chapter-observation",
        chapter_name="Alpha Beta",
        fraternity_slug="alpha-main",
        website_url="https://chapter.example.edu",
        field_states={"website_url": "found"},
        payload={"contactResolution": {"supportingPageUrl": "https://chapter.example.edu", "supportingPageScope": "chapter_site"}},
    )
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-obs": ["Reach us at chapter@example.edu"]})
    policy = AdaptivePolicy(live_epsilon=0.0, train_epsilon=0.0)
    engine = FieldJobEngine(
        repo,
        logging.getLogger("test"),
        worker_id="worker",
        adaptive_policy=policy,
        adaptive_runtime_mode="langgraph_primary",
        adaptive_policy_version="adaptive-v1",
        provider_window_state={"general_web_search": {"healthy": True}},
    )

    result = engine.process(limit=1)

    assert result["processed"] == 1
    assert len(repo.enrichment_observations) == 1
    observation = repo.enrichment_observations[0]
    assert observation.recommended_action == "parse_supporting_page"
    assert observation.deterministic_action == "parse_supporting_page"
    assert observation.outcome["finalState"] == "processed"


