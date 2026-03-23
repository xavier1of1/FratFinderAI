from __future__ import annotations

import logging
from dataclasses import replace
from types import SimpleNamespace

import requests

from fratfinder_crawler.field_jobs import FieldJobEngine
from fratfinder_crawler.models import FieldJob


class FakeRepository:
    def __init__(self, jobs: list[FieldJob], snippets_by_chapter: dict[str, list[str]]):
        self.jobs = jobs
        self.snippets_by_chapter = snippets_by_chapter
        self.completed: list[tuple[str, dict[str, str], dict[str, str]]] = []
        self.requeued: list[str] = []
        self.failed: list[str] = []
        self.review_items: list[tuple[str, object]] = []
        self.claimed_source_slugs: list[str | None] = []

    def claim_next_field_job(self, worker_id: str, source_slug: str | None = None) -> FieldJob | None:
        self.claimed_source_slugs.append(source_slug)
        if not self.jobs:
            return None
        return self.jobs.pop(0)

    def fetch_provenance_snippets(self, chapter_id: str) -> list[str]:
        return self.snippets_by_chapter.get(chapter_id, [])

    def complete_field_job(
        self,
        job: FieldJob,
        chapter_updates: dict[str, str],
        completed_payload: dict[str, str],
        field_state_updates: dict[str, str] | None = None,
    ) -> None:
        self.completed.append((job.id, chapter_updates, field_state_updates or {}))

    def requeue_field_job(self, job: FieldJob, error: str, delay_seconds: int) -> None:
        self.requeued.append(job.id)

    def fail_field_job_terminal(self, job: FieldJob, error: str) -> None:
        self.failed.append(job.id)

    def create_field_job_review_item(self, job: FieldJob, candidate) -> None:
        self.review_items.append((job.id, candidate))



def _job(
    field_name: str,
    *,
    attempts: int = 1,
    website_url: str | None = None,
    instagram_url: str | None = None,
    contact_email: str | None = None,
    university_name: str | None = None,
) -> FieldJob:
    return FieldJob(
        id=f"job-{field_name}-{attempts}",
        chapter_id="chapter-1",
        chapter_slug="chapter-one",
        field_name=field_name,
        payload={"candidateSchoolName": "Example University", "sourceSlug": "sigma-chi-main"},
        attempts=attempts,
        max_attempts=3,
        claim_token="claim-token",
        source_base_url="https://source.example.org",
        website_url=website_url,
        instagram_url=instagram_url,
        contact_email=contact_email,
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
