from __future__ import annotations

from types import SimpleNamespace

from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.school_verification import resolve_cached_school_verification
from fratfinder_crawler.status.models import ChapterStatusDecision, ChapterStatusFinal, SchoolRecognitionStatus


class Repo:
    def __init__(self):
        self.status_decision = None
        self.activity = None
        self.policy = None

    def get_latest_chapter_status_decision(self, chapter_id: str):
        _ = chapter_id
        return self.status_decision

    def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None):
        _ = fraternity_slug, school_name
        return self.activity

    def get_school_policy(self, school_name: str | None):
        _ = school_name
        return self.policy


def _job(*, candidate_school_name: str | None = None, school_name: str = "Virginia Tech") -> FieldJob:
    return FieldJob(
        id="job-1",
        chapter_id="chapter-1",
        chapter_slug="chapter-one",
        chapter_name="Alpha Test",
        field_name="verify_school_match",
        payload={"candidateSchoolName": candidate_school_name} if candidate_school_name is not None else {},
        attempts=1,
        max_attempts=3,
        claim_token="claim-token",
        source_base_url=None,
        website_url=None,
        instagram_url=None,
        contact_email=None,
        fraternity_slug="delta-chi",
        source_id="source-1",
        source_slug="delta-chi-main",
        university_name=school_name,
        crawl_run_id=1,
    )


def test_exact_candidate_school_match_verifies_without_repository_evidence():
    result = resolve_cached_school_verification(job=_job(candidate_school_name="Virginia Tech"), repository=Repo())

    assert result.outcome == "verified"
    assert result.reason_code == "candidate_school_exact_match"


def test_candidate_school_mismatch_routes_review():
    result = resolve_cached_school_verification(job=_job(candidate_school_name="University of Virginia"), repository=Repo())

    assert result.outcome == "review_required"
    assert result.reason_code == "candidate_school_mismatch"


def test_official_active_status_decision_verifies():
    repo = Repo()
    repo.status_decision = ChapterStatusDecision(
        id="decision-1",
        chapter_id="chapter-1",
        final_status=ChapterStatusFinal.ACTIVE,
        school_recognition_status=SchoolRecognitionStatus.RECOGNIZED,
        national_status="unknown",
        confidence=0.97,
        reason_code="official_school_current_recognition",
        evidence_ids=["evidence-1"],
        decision_trace={"final_status_basis": "official_school_current_recognition"},
    )

    result = resolve_cached_school_verification(job=_job(), repository=repo)

    assert result.outcome == "verified"
    assert result.status_decision_id == "decision-1"


def test_official_inactive_status_decision_marks_inactive():
    repo = Repo()
    repo.status_decision = ChapterStatusDecision(
        id="decision-2",
        chapter_id="chapter-1",
        final_status=ChapterStatusFinal.INACTIVE,
        school_recognition_status=SchoolRecognitionStatus.UNRECOGNIZED,
        national_status="unknown",
        confidence=0.97,
        reason_code="official_school_negative_status",
        evidence_ids=["evidence-2"],
        decision_trace={"final_status_basis": "official_school_negative_status"},
    )

    result = resolve_cached_school_verification(job=_job(), repository=repo)

    assert result.outcome == "inactive"
    assert result.reason_code == "official_school_negative_status"


def test_activity_cache_active_verifies():
    repo = Repo()
    repo.activity = SimpleNamespace(
        chapter_activity_status="confirmed_active",
        evidence_source_type="official_school",
        evidence_url="https://vt.edu/fsl/chapters",
        reason_code="fraternity_present_on_official_school_list",
        confidence=0.98,
        metadata={"sourceSnippet": "Delta Chi is recognized."},
    )

    result = resolve_cached_school_verification(job=_job(), repository=repo)

    assert result.outcome == "verified"
    assert result.metadata["decisionSource"] == "fraternity_school_activity_cache"


def test_school_policy_allowed_is_identity_only_not_active_verification():
    repo = Repo()
    repo.policy = SimpleNamespace(
        greek_life_status="allowed",
        evidence_source_type="official_school",
        evidence_url="https://vt.edu/fsl",
        reason_code="school_policy_allowed",
        confidence=0.9,
        metadata={},
    )

    result = resolve_cached_school_verification(job=_job(), repository=repo)

    assert result.outcome == "school_identity_only"
    assert result.metadata["chapterActivityStatus"] == "unknown"


def test_cache_miss_blocks_for_school_evidence():
    result = resolve_cached_school_verification(job=_job(), repository=Repo())

    assert result.outcome == "evidence_missing"
    assert result.reason_code == "school_evidence_missing"
