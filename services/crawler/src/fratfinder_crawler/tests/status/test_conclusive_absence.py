from __future__ import annotations

from fratfinder_crawler.status.absence import (
    MAX_AUTHORITY_TIER_FOR_CONCLUSIVE_ABSENCE,
    MIN_CURRENTNESS_SCORE,
    MIN_ORG_COUNT_FOR_CONCLUSIVE_ROSTER,
    MIN_PARSE_COMPLETENESS_SCORE,
    infer_absence_status,
    is_conclusive_active_roster,
)
from fratfinder_crawler.status.campus_discovery import CampusSourceDocument, build_campus_status_index

from ._helpers import load_fixture


def test_absence_from_complete_official_recognized_roster_is_inactive():
    index = build_campus_status_index(
        school_name="University of Delaware",
        documents=[CampusSourceDocument(page_url="https://udel.edu/recognized", title="Recognized Chapters", text=load_fixture("status_pages", "delaware_recognized_chapters.html"), html=load_fixture("status_pages", "delaware_recognized_chapters.html"))],
    )
    decision = infer_absence_status(fraternity_name="Theta Chi", fraternity_slug="theta-chi", school_name="University of Delaware", index=index)
    assert decision.final_status == "inactive"
    assert decision.reason_code == "absent_from_current_official_complete_school_roster"


def test_absence_from_school_page_with_explicit_recognition_claim_is_inactive():
    index = build_campus_status_index(
        school_name="University of Delaware",
        documents=[CampusSourceDocument(page_url="https://udel.edu/recognized", title="Recognized Chapters", text=load_fixture("status_pages", "delaware_recognized_chapters.html"), html=load_fixture("status_pages", "delaware_recognized_chapters.html"))],
    )
    assert is_conclusive_active_roster(index) is True


def test_no_greek_policy_makes_absence_inactive_with_policy_reason():
    index = build_campus_status_index(
        school_name="Williams College",
        documents=[CampusSourceDocument(page_url="https://williams.edu/fraternities", title="Fraternities", text=load_fixture("status_pages", "williams_no_fraternities_policy.html"), html=load_fixture("status_pages", "williams_no_fraternities_policy.html"))],
    )
    decision = infer_absence_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="Williams College", index=index)
    assert decision.reason_code == "official_school_policy_prohibits_fraternities"


def test_absence_from_article_is_not_conclusive():
    index = build_campus_status_index(
        school_name="Example University",
        documents=[CampusSourceDocument(page_url="https://example.edu/news", title="Campus Update", text=load_fixture("status_pages", "thin_article_with_suspended_keyword.html"), html=load_fixture("status_pages", "thin_article_with_suspended_keyword.html"))],
    )
    decision = infer_absence_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="Example University", index=index)
    assert decision.final_status == "unknown"


def test_absence_from_dynamic_shell_without_rendered_orgs_is_not_conclusive():
    index = build_campus_status_index(
        school_name="University of California, Riverside",
        documents=[CampusSourceDocument(page_url="https://highlanderlink.ucr.edu/organizations", title="Organizations", text=load_fixture("status_pages", "dynamic_rso_shell_no_rendered_orgs.html"), html=load_fixture("status_pages", "dynamic_rso_shell_no_rendered_orgs.html"))],
    )
    decision = infer_absence_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="University of California, Riverside", index=index)
    assert decision.final_status == "unknown"


def test_threshold_constants_are_pinned():
    assert MIN_ORG_COUNT_FOR_CONCLUSIVE_ROSTER == 5
    assert MIN_CURRENTNESS_SCORE == 0.70
    assert MIN_PARSE_COMPLETENESS_SCORE == 0.85
    assert MAX_AUTHORITY_TIER_FOR_CONCLUSIVE_ABSENCE == 2
