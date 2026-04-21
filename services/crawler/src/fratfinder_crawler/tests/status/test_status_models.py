from __future__ import annotations

import pytest

from fratfinder_crawler.status.models import (
    ChapterStatusDecision,
    ChapterStatusFinal,
    NationalStatusValue,
    SchoolRecognitionStatus,
)


def test_final_status_values_are_limited_to_active_inactive_unknown_review():
    assert {item.value for item in ChapterStatusFinal} == {"active", "inactive", "unknown", "review"}


def test_school_recognition_status_values_are_exhaustive():
    assert {item.value for item in SchoolRecognitionStatus} == {
        "recognized",
        "probationary_recognition",
        "active_under_conduct_sanction",
        "seeking_recognition",
        "interim_suspension",
        "suspended",
        "closed",
        "dismissed",
        "expelled",
        "unrecognized",
        "banned_no_greek_life",
        "not_found_on_conclusive_roster",
        "unknown",
    }


def test_national_status_values_are_exhaustive():
    assert {item.value for item in NationalStatusValue} == {
        "active",
        "associate",
        "colony",
        "inactive",
        "dormant",
        "closed",
        "not_listed_on_active_only_directory",
        "not_listed_on_all_status_directory",
        "unknown",
    }


def test_status_decision_requires_evidence_for_active_or_inactive():
    with pytest.raises(ValueError):
        ChapterStatusDecision(
            final_status="active",
            school_recognition_status="recognized",
            national_status="active",
            reason_code="official_school_current_recognition",
            confidence=0.9,
            evidence_ids=[],
            decision_trace={},
        )


def test_status_decision_allows_unknown_without_final_evidence_but_requires_reason_code():
    decision = ChapterStatusDecision(
        final_status="unknown",
        school_recognition_status="unknown",
        national_status="unknown",
        reason_code="absence_not_conclusive",
        confidence=0.3,
        evidence_ids=[],
        decision_trace={},
    )
    assert decision.reason_code == "absence_not_conclusive"


def test_status_decision_serializes_and_deserializes_cleanly():
    decision = ChapterStatusDecision(
        id="decision-1",
        chapter_id="chapter-1",
        final_status="inactive",
        school_recognition_status="unrecognized",
        national_status="inactive",
        reason_code="official_school_negative_status",
        confidence=0.98,
        evidence_ids=["evidence-1"],
        decision_trace={"winning_evidence_id": "evidence-1"},
        conflict_flags=["school_inactive_national_active_conflict"],
        review_required=False,
    )
    parsed = ChapterStatusDecision.model_validate_json(decision.model_dump_json())
    assert parsed.id == "decision-1"
    assert parsed.conflict_flags == ["school_inactive_national_active_conflict"]
