from __future__ import annotations

import logging

import pytest

from fratfinder_crawler.field_jobs import FieldJobEngine
from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.status.models import ChapterStatusDecision


class _IntegrationRepository:
    def __init__(self):
        self.jobs: list[FieldJob] = []
        self.latest_status_decision: ChapterStatusDecision | None = None
        self.requeues: list[tuple[str, str, dict[str, object]]] = []
        self.completed: list[tuple[str, dict[str, str], dict[str, object]]] = []
        self.created_field_jobs: list[list[str]] = []
        self.pending: set[tuple[str, str]] = set()

    def claim_next_field_job(self, worker_id: str, source_slug=None, field_name=None, require_confident_website_for_email=False, degraded_mode=False):
        _ = worker_id, source_slug, field_name, require_confident_website_for_email, degraded_mode
        return self.jobs.pop(0) if self.jobs else None

    def get_latest_chapter_status_decision(self, chapter_id: str):
        _ = chapter_id
        return self.latest_status_decision

    def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
        return (chapter_id, field_name) in self.pending

    def create_field_jobs(self, chapter_id: str, crawl_run_id: int, chapter_slug: str, source_slug: str, missing_fields: list[str]) -> int:
        _ = crawl_run_id, chapter_slug, source_slug
        self.created_field_jobs.append(list(missing_fields))
        for field_name in missing_fields:
            self.pending.add((chapter_id, field_name))
        return len(missing_fields)

    def requeue_field_job(self, job, error, delay_seconds, preserve_attempt=False, payload_patch=None):
        _ = delay_seconds, preserve_attempt
        self.requeues.append((job.id, error, payload_patch or {}))

    def complete_field_job(self, job, chapter_updates, completed_payload, field_state_updates, provenance_records):
        _ = field_state_updates, provenance_records
        self.completed.append((job.id, chapter_updates, completed_payload))

    def fail_field_job_terminal(self, job, error):
        raise AssertionError(f"unexpected terminal failure: {job.id} {error}")

    def create_field_job_review_item(self, job, review_item):
        _ = job, review_item

    def apply_chapter_inactive_status(self, **kwargs):
        _ = kwargs

    def complete_pending_field_jobs_for_chapter(self, **kwargs):
        _ = kwargs
        return 0

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

    def fetch_provenance_snippets(self, chapter_id: str):
        _ = chapter_id
        return []

    def has_recent_transient_website_failures(self, chapter_id: str, min_failures: int = 2):
        _ = chapter_id, min_failures
        return False

    def append_enrichment_observation(self, observation):
        _ = observation
        return 1

    def get_chapter_completion_signal(self, chapter_id: str):
        _ = chapter_id
        return {
            "validated_active": bool(self.latest_status_decision and str(self.latest_status_decision.final_status) == "active"),
            "chapter_safe_email": False,
            "chapter_safe_instagram": False,
            "complete_row": False,
        }


def _job(field_name: str) -> FieldJob:
    return FieldJob(
        id=f"{field_name}-job",
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


@pytest.mark.integration
def test_status_first_queue_flow_defers_then_advances():
    repository = _IntegrationRepository()
    repository.jobs = [_job("find_email")]
    engine = FieldJobEngine(repository=repository, logger=logging.getLogger("integration-status-first"), worker_id="worker-1", search_degraded_mode=True)

    first = engine.process(limit=1)
    assert first["requeued"] == 1
    assert repository.requeues[0][2]["contactResolution"]["reasonCode"] == "status_dependency_unmet"

    repository.latest_status_decision = ChapterStatusDecision(
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
    repository.jobs = [_job("verify_school_match")]
    second = engine.process(limit=1)
    assert second["processed"] == 1
