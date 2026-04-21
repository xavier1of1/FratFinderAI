from __future__ import annotations

from fratfinder_crawler.status.campus_discovery import CampusSourceDocument, build_campus_status_index
from fratfinder_crawler.status.decision_engine import decide_chapter_status
from fratfinder_crawler.status.models import NationalDirectoryCapability, NationalStatusEvidence, NationalStatusValue

from ._helpers import load_fixture


def test_school_active_national_inactive_final_active_with_conflict_flag():
    index = build_campus_status_index(
        school_name="Louisiana State University",
        documents=[CampusSourceDocument(page_url="https://lsu.edu/greeks/scorecard", title="Community Scorecard", text=load_fixture("status_pages", "lsu_scorecard_tabs.html"), html=load_fixture("status_pages", "lsu_scorecard_tabs.html"))],
    )
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Louisiana State University",
        index=index,
        national_evidence=NationalStatusEvidence(status=NationalStatusValue.INACTIVE, capability=NationalDirectoryCapability.ALL_STATUS, evidence_url="https://national.example.org", confidence=0.88),
    )
    assert decision.final_status == "active"
    assert "school_active_national_inactive_conflict" in decision.conflict_flags


def test_school_inactive_national_active_final_inactive_with_conflict_flag():
    index = build_campus_status_index(
        school_name="Penn State",
        documents=[CampusSourceDocument(page_url="https://studentaffairs.psu.edu/suspended-unrecognized", title="Suspended Groups", text=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"), html=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"))],
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


def test_school_active_conduct_probation_final_active_under_sanction():
    index = build_campus_status_index(
        school_name="University of Florida",
        documents=[CampusSourceDocument(page_url="https://ufl.edu/ifc-conduct", title="Conduct Data", text=load_fixture("status_pages", "uf_ifc_conduct_page.html"), html=load_fixture("status_pages", "uf_ifc_conduct_page.html"))],
    )
    decision = decide_chapter_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="University of Florida", index=index)
    assert decision.final_status == "active"
    assert decision.school_recognition_status == "active_under_conduct_sanction"


def test_school_active_conduct_interim_suspension_routes_review_or_inactive_based_on_policy_text():
    index = build_campus_status_index(
        school_name="University of Florida",
        documents=[CampusSourceDocument(page_url="https://ufl.edu/ifc-conduct", title="Conduct Data", text=load_fixture("status_pages", "uf_ifc_conduct_page.html"), html=load_fixture("status_pages", "uf_ifc_conduct_page.html"))],
    )
    decision = decide_chapter_status(fraternity_name="Phi Delta Theta", fraternity_slug="phi-delta-theta", school_name="University of Florida", index=index)
    assert decision.final_status in {"inactive", "review"}


def test_recent_official_statement_loss_of_recognition_overrides_old_school_roster():
    index = build_campus_status_index(
        school_name="Virginia Commonwealth University",
        documents=[
            CampusSourceDocument(page_url="https://vcu.edu/news/delta-chi", title="VCU Statement on Delta Chi", text=load_fixture("status_pages", "vcu_delta_chi_statement.html"), html=load_fixture("status_pages", "vcu_delta_chi_statement.html")),
            CampusSourceDocument(page_url="https://vcu.edu/recognized", title="Recognized Chapters Archive", text="<html><body><h1>Recognized Chapters</h1><ul><li>Delta Chi</li></ul></body></html>", html="<html><body><h1>Recognized Chapters</h1><ul><li>Delta Chi</li></ul></body></html>"),
        ],
    )
    decision = decide_chapter_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="Virginia Commonwealth University", index=index)
    assert decision.final_status == "inactive"


def test_conflict_decision_stores_authority_ordering_trace():
    index = build_campus_status_index(
        school_name="Penn State",
        documents=[CampusSourceDocument(page_url="https://studentaffairs.psu.edu/suspended-unrecognized", title="Suspended Groups", text=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"), html=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"))],
    )
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Penn State",
        index=index,
        national_evidence=NationalStatusEvidence(status=NationalStatusValue.ACTIVE, capability=NationalDirectoryCapability.ACTIVE_ONLY, evidence_url="https://national.example.org", confidence=0.8),
    )
    assert decision.decision_trace["authority_order"] == ["school_status", "school_conduct", "national_directory", "chapter_site"]
