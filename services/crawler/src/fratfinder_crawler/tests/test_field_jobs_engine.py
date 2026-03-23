from __future__ import annotations

import logging
from dataclasses import replace
from types import SimpleNamespace

import requests

from fratfinder_crawler.field_jobs import FieldJobEngine
from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.search import SearchResult


class FakeRepository:
    def __init__(self, jobs: list[FieldJob], snippets_by_chapter: dict[str, list[str]]):
        self.jobs = jobs
        self.snippets_by_chapter = snippets_by_chapter
        self.completed: list[tuple[str, dict[str, str], dict[str, str], int]] = []
        self.requeued: list[str] = []
        self.failed: list[str] = []
        self.review_items: list[tuple[str, object]] = []
        self.claimed_source_slugs: list[str | None] = []
        self.claim_order: list[str] = []

    def claim_next_field_job(self, worker_id: str, source_slug: str | None = None) -> FieldJob | None:
        self.claimed_source_slugs.append(source_slug)
        if not self.jobs:
            return None
        job = self.jobs.pop(0)
        self.claim_order.append(job.field_name)
        return job

    def fetch_provenance_snippets(self, chapter_id: str) -> list[str]:
        return self.snippets_by_chapter.get(chapter_id, [])

    def complete_field_job(
        self,
        job: FieldJob,
        chapter_updates: dict[str, str],
        completed_payload: dict[str, str],
        field_state_updates: dict[str, str] | None = None,
        provenance_records=None,
    ) -> None:
        self.completed.append((job.id, chapter_updates, field_state_updates or {}, len(provenance_records or [])))

    def requeue_field_job(self, job: FieldJob, error: str, delay_seconds: int) -> None:
        self.requeued.append(job.id)

    def fail_field_job_terminal(self, job: FieldJob, error: str) -> None:
        self.failed.append(job.id)

    def create_field_job_review_item(self, job: FieldJob, candidate) -> None:
        self.review_items.append((job.id, candidate))


class FakeSearchClient:
    def __init__(self, results_by_query: dict[str, list[SearchResult]]):
        self.results_by_query = results_by_query
        self.queries: list[str] = []

    def search(self, query: str, max_results: int | None = None) -> list[SearchResult]:
        self.queries.append(query)
        return self.results_by_query.get(query, [])



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



def test_field_job_engine_finds_instagram_from_handle_hint_when_no_full_url_exists():
    job = _job("find_instagram", university_name="Demo University")
    repo = FakeRepository(jobs=[job], snippets_by_chapter={"chapter-1": ["Instagram: @alphatestchapter"]})
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")

    result = engine.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repo.completed[0][1] == {"instagram_url": "https://www.instagram.com/alphatestchapter"}



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
            "sigma chi Alpha Test Demo University chapter website": [
                SearchResult(
                    title="Sigma Chi at Demo University",
                    url="https://demosigmachi.org",
                    snippet="Official Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Demo University chapter website": [
                SearchResult(
                    title="Sigma Chi at Demo University",
                    url="https://demosigmachi.org",
                    snippet="Official Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Alpha Test Demo University email": [
                SearchResult(
                    title="Contact Sigma Chi at Demo University",
                    url="https://demosigmachi.org/contact",
                    snippet="Contact the chapter leadership team.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Demo University email": [
                SearchResult(
                    title="Contact Sigma Chi at Demo University",
                    url="https://demosigmachi.org/contact",
                    snippet="Contact the chapter leadership team.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Alpha Test Demo University instagram": [
                SearchResult(
                    title="Sigma Chi Demo University Instagram",
                    url="https://www.instagram.com/demosigmachi",
                    snippet="Instagram profile for the Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Demo University instagram": [
                SearchResult(
                    title="Sigma Chi Demo University Instagram",
                    url="https://www.instagram.com/demosigmachi",
                    snippet="Instagram profile for the Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Alpha Test Demo University instagram profile": [
                SearchResult(
                    title="Sigma Chi Demo University Instagram",
                    url="https://www.instagram.com/demosigmachi",
                    snippet="Instagram profile for the Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Demo University instagram profile": [
                SearchResult(
                    title="Sigma Chi Demo University Instagram",
                    url="https://www.instagram.com/demosigmachi",
                    snippet="Instagram profile for the Sigma Chi chapter at Demo University.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Alpha Test Demo University contact email": [
                SearchResult(
                    title="Contact Sigma Chi at Demo University",
                    url="https://demosigmachi.org/contact",
                    snippet="Contact the chapter leadership team.",
                    provider="duckduckgo_html",
                    rank=1,
                )
            ],
            "sigma chi Demo University contact email": [
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
            "sigma chi Alpha Test Demo University chapter website": [
                SearchResult(
                    title="Sigma Chi Demo University",
                    url="https://www.reddit.com/r/examplechapter",
                    snippet="Discussion thread",
                    provider="bing_html",
                    rank=1,
                )
            ],
            "sigma chi Demo University chapter website": [],
            "sigma chi Alpha Test Demo University official chapter site": [],
            "sigma chi Demo University official chapter site": [],
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
            "sigma chi Alpha Test Demo University chapter website": [
                SearchResult(
                    title="Sigma Chi | Demo University Student Organizations",
                    url="https://studentorgs.demo.edu/sigma-chi",
                    snippet="Official student organization listing for Sigma Chi at Demo University.",
                    provider="bing_html",
                    rank=1,
                )
            ],
            "sigma chi Demo University student organization site:.edu": [
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
    engine = FieldJobEngine(repo, logging.getLogger("test"), worker_id="worker")
    job = _job("find_website", university_name="Willamette University", chapter_name="Delta Zeta")

    queries = engine._build_search_queries(job, target="website")

    assert "sigma chi Willamette University chapter website" in queries
    assert '"sigma chi" "Willamette University" fraternity site:.edu' in queries
    assert all("Delta Zeta Willamette University" not in query for query in queries)
