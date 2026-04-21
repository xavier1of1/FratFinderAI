from __future__ import annotations

import logging

import pytest

from fratfinder_crawler.field_jobs import FieldJobEngine, RetryableJobError
from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.status.models import ChapterStatusDecision


def _job(field_name: str) -> FieldJob:
    return FieldJob(
        id="job-1",
        chapter_id="chapter-1",
        chapter_slug="delta-chi-lsu",
        chapter_name="Delta Chi",
        field_name=field_name,
        payload={"sourceSlug": "delta-chi-main"},
        attempts=1,
        max_attempts=3,
        claim_token="claim-1",
        source_base_url="https://deltachi.org",
        website_url=None,
        instagram_url=None,
        contact_email=None,
        fraternity_slug="delta-chi",
        source_id="source-1",
        source_slug="delta-chi-main",
        university_name="Louisiana State University",
        crawl_run_id=1,
        field_states={},
    )


class _FakeRepository:
    def __init__(self, decision: ChapterStatusDecision | None = None):
        self.decision = decision
        self.created_field_jobs: list[list[str]] = []
        self.pending_field_jobs: set[tuple[str, str]] = set()
        self.inactive_calls: list[dict[str, object]] = []
        self.completed_pending_calls: list[dict[str, object]] = []

    def get_latest_chapter_status_decision(self, chapter_id: str):
        _ = chapter_id
        return self.decision

    def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
        return (chapter_id, field_name) in self.pending_field_jobs

    def create_field_jobs(self, chapter_id: str, crawl_run_id: int, chapter_slug: str, source_slug: str, missing_fields: list[str]) -> int:
        _ = chapter_id, crawl_run_id, chapter_slug, source_slug
        self.created_field_jobs.append(list(missing_fields))
        for field_name in missing_fields:
            self.pending_field_jobs.add((chapter_id, field_name))
        return len(missing_fields)

    def apply_chapter_inactive_status(self, **kwargs):
        self.inactive_calls.append(dict(kwargs))

    def complete_pending_field_jobs_for_chapter(self, **kwargs):
        self.completed_pending_calls.append(dict(kwargs))
        return len(list(kwargs.get("field_names") or []))

    def get_school_policy(self, school_name: str | None):
        _ = school_name
        return None

    def upsert_school_policy(self, **kwargs):
        _ = kwargs
        return None

    def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None):
        _ = fraternity_slug, school_name
        return None

    def upsert_chapter_activity(self, **kwargs):
        _ = kwargs
        return None

    def load_sources(self, source_slug: str | None = None):
        _ = source_slug
        return []

    def get_reusable_official_school_evidence_url(self, *, fraternity_slug: str | None, school_name: str | None):
        _ = fraternity_slug, school_name
        return None


def test_find_email_defers_when_status_unknown_and_no_status_decision_exists():
    repository = _FakeRepository()
    engine = FieldJobEngine(repository=repository, logger=logging.getLogger("status-gate"), worker_id="worker-1", search_degraded_mode=True)
    with pytest.raises(RetryableJobError) as exc:
        engine._resolve_activity_gate(_job("find_email"), target_field="contact_email")
    assert exc.value.reason_code == "status_dependency_unmet"
    assert repository.created_field_jobs == [["verify_school_match"]]


def test_find_website_defers_when_status_unknown_and_status_required():
    repository = _FakeRepository()
    engine = FieldJobEngine(repository=repository, logger=logging.getLogger("status-gate"), worker_id="worker-1", search_degraded_mode=True)
    with pytest.raises(RetryableJobError) as exc:
        engine._resolve_activity_gate(_job("find_website"), target_field="website_url")
    assert exc.value.reason_code == "status_dependency_unmet"


def test_find_instagram_defers_when_status_unknown_and_status_required():
    repository = _FakeRepository()
    engine = FieldJobEngine(repository=repository, logger=logging.getLogger("status-gate"), worker_id="worker-1", search_degraded_mode=True)
    with pytest.raises(RetryableJobError) as exc:
        engine._resolve_activity_gate(_job("find_instagram"), target_field="instagram_url")
    assert exc.value.reason_code == "status_dependency_unmet"


def test_contact_jobs_continue_after_confirmed_active_status():
    repository = _FakeRepository(
        ChapterStatusDecision(
            id="decision-1",
            chapter_id="chapter-1",
            final_status="active",
            school_recognition_status="recognized",
            national_status="active",
            reason_code="official_school_current_recognition",
            confidence=0.98,
            evidence_ids=["evidence-1"],
            decision_trace={},
        )
    )
    engine = FieldJobEngine(repository=repository, logger=logging.getLogger("status-gate"), worker_id="worker-1")
    assert engine._resolve_activity_gate(_job("find_email"), target_field="contact_email") is None


def test_contact_jobs_block_after_confirmed_inactive_status():
    repository = _FakeRepository(
        ChapterStatusDecision(
            id="decision-1",
            chapter_id="chapter-1",
            final_status="inactive",
            school_recognition_status="suspended",
            national_status="inactive",
            reason_code="official_school_negative_status",
            confidence=0.98,
            evidence_ids=["evidence-1"],
            decision_trace={"winning_evidence_id": "https://example.edu/status"},
        )
    )
    engine = FieldJobEngine(repository=repository, logger=logging.getLogger("status-gate"), worker_id="worker-1")
    result = engine._resolve_activity_gate(_job("find_email"), target_field="contact_email")
    assert result is not None
    assert result.chapter_updates["chapter_status"] == "inactive"


def test_inactive_status_completes_or_cancels_sibling_contact_jobs():
    repository = _FakeRepository(
        ChapterStatusDecision(
            id="decision-1",
            chapter_id="chapter-1",
            final_status="inactive",
            school_recognition_status="unrecognized",
            national_status="inactive",
            reason_code="official_school_negative_status",
            confidence=0.98,
            evidence_ids=["evidence-1"],
            decision_trace={"winning_evidence_id": "https://example.edu/status"},
        )
    )
    engine = FieldJobEngine(repository=repository, logger=logging.getLogger("status-gate"), worker_id="worker-1")
    engine._resolve_activity_gate(_job("find_instagram"), target_field="instagram_url")
    assert repository.completed_pending_calls
