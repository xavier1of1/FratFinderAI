from __future__ import annotations

from fratfinder_crawler.status.campus_discovery import CampusSourceDocument, build_campus_status_index
from fratfinder_crawler.status.decision_engine import decide_chapter_status
from fratfinder_crawler.status.models import NationalDirectoryCapability, NationalStatusEvidence, NationalStatusValue

from ._helpers import load_fixture


def test_school_active_overrides_national_inactive_with_conflict_flag():
    index = build_campus_status_index(
        school_name="Louisiana State University",
        documents=[CampusSourceDocument(page_url="https://lsu.edu/greeks/scorecard", title="Community Scorecard", text=load_fixture("status_pages", "lsu_scorecard_tabs.html"), html=load_fixture("status_pages", "lsu_scorecard_tabs.html"))],
    )
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Louisiana State University",
        index=index,
        national_evidence=NationalStatusEvidence(
            status=NationalStatusValue.INACTIVE,
            capability=NationalDirectoryCapability.ALL_STATUS,
            evidence_url="https://national.example.org",
            confidence=0.82,
            reason_code="national_directory_lists_inactive",
        ),
    )
    assert decision.final_status == "active"
    assert "school_active_national_inactive_conflict" in decision.conflict_flags


def test_school_inactive_overrides_national_active():
    index = build_campus_status_index(
        school_name="Penn State",
        documents=[CampusSourceDocument(page_url="https://studentaffairs.psu.edu/suspended-unrecognized", title="Suspended & Unrecognized Groups", text=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"), html=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"))],
    )
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Penn State",
        index=index,
        national_evidence=NationalStatusEvidence(status=NationalStatusValue.ACTIVE, capability=NationalDirectoryCapability.ACTIVE_ONLY, evidence_url="https://national.example.org", confidence=0.8),
    )
    assert decision.final_status == "inactive"
    assert "school_inactive_national_active_conflict" in decision.conflict_flags


def test_national_active_only_absence_sets_national_not_listed_but_final_unknown_without_school_evidence():
    empty_index = build_campus_status_index(school_name="Example University", documents=[])
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Example University",
        index=empty_index,
        national_evidence=NationalStatusEvidence(
            status=NationalStatusValue.NOT_LISTED_ON_ACTIVE_ONLY_DIRECTORY,
            capability=NationalDirectoryCapability.ACTIVE_ONLY,
            evidence_url="https://national.example.org",
            confidence=0.4,
            reason_code="national_active_only_absence_not_conclusive",
        ),
    )
    assert decision.final_status == "unknown"


def test_national_all_status_inactive_without_school_evidence_returns_review():
    empty_index = build_campus_status_index(school_name="Example University", documents=[])
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Example University",
        index=empty_index,
        national_evidence=NationalStatusEvidence(
            status=NationalStatusValue.DORMANT,
            capability=NationalDirectoryCapability.ALL_STATUS,
            evidence_url="https://national.example.org",
            confidence=0.84,
            reason_code="national_directory_lists_dormant",
        ),
    )
    assert decision.final_status == "review"


def test_national_history_roll_never_creates_final_active():
    empty_index = build_campus_status_index(school_name="Example University", documents=[])
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Example University",
        index=empty_index,
        national_evidence=NationalStatusEvidence(
            status=NationalStatusValue.UNKNOWN,
            capability=NationalDirectoryCapability.HISTORY_ROLL,
            evidence_url="https://history.example.org",
            confidence=0.2,
            reason_code="national_history_roll_not_current",
        ),
    )
    assert decision.final_status in {"unknown", "review"}
