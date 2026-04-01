from __future__ import annotations

import logging
from dataclasses import replace
from types import SimpleNamespace

import requests

from fratfinder_crawler.field_jobs import CandidateMatch, FieldJobEngine
from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.search import SearchResult, SearchUnavailableError


class FakeRepository:
    def __init__(
        self,
        jobs: list[FieldJob],
        snippets_by_chapter: dict[str, list[str]],
        pending_field_jobs: set[tuple[str, str]] | None = None,
    ):
        self.jobs = jobs
        self.snippets_by_chapter = snippets_by_chapter
        self.pending_field_jobs = pending_field_jobs or set()
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



def test_field_job_engine_requeues_retryable_find_job_until_max_attempts():
    retryable = _job("find_email")
    repo = FakeRepository(jobs=[retryable], snippets_by_chapter={"chapter-1": ["No email in this snippet"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", base_backoff_seconds=2)

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 1, "failed_terminal": 0}
    assert repo.requeued == [retryable.id]

    terminal = replace(retryable, attempts=3, id="job-find_email-terminal")
    repo = FakeRepository(jobs=[terminal], snippets_by_chapter={"chapter-1": ["No email in this snippet"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker", base_backoff_seconds=2)

    result = engine.process(limit=1)

    assert result == {"processed": 0, "requeued": 0, "failed_terminal": 1}
    assert repo.failed == [terminal.id]



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

    assert repo.claimed_require_confident_website_for_email == [False]


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



def test_find_website_requeues_when_only_source_base_url_exists():
    job = _job("find_website")
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



def test_verify_school_match_creates_review_item_on_clear_mismatch():
    job = _job("verify_school_match", university_name="Ohio State University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.review_items[0][1].item_type == "school_match_mismatch"
    assert repo.completed[0][1] == {}



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
    assert repo.claimed_require_confident_website_for_email == [False]



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

    def fail_fetch(*args, **kwargs):
        raise AssertionError("direct instagram result should not trigger page fetch")

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
    assert repo.completed[0][2]["instagram_url"] == "missing"


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

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.discovery_upserts
    assert repo.discovery_field_jobs == []


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


