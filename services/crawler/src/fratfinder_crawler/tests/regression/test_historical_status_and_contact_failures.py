from __future__ import annotations

from fratfinder_crawler.field_jobs import _email_local_part_looks_generic_office
from fratfinder_crawler.status.campus_discovery import CampusSourceDocument, build_campus_status_index
from fratfinder_crawler.status.decision_engine import decide_chapter_status
from fratfinder_crawler.status.models import NationalDirectoryCapability, NationalStatusEvidence, NationalStatusValue

from fratfinder_crawler.tests.status._helpers import load_fixture


def test_lsu_status_tabs_do_not_misclassify_active_chapter_as_suspended():
    index = build_campus_status_index(
        school_name="Louisiana State University",
        documents=[CampusSourceDocument(page_url="https://lsu.edu/greeks/scorecard", title="Community Scorecard", text=load_fixture("status_pages", "lsu_scorecard_tabs.html"), html=load_fixture("status_pages", "lsu_scorecard_tabs.html"))],
    )
    active = decide_chapter_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="Louisiana State University", index=index)
    inactive = decide_chapter_status(fraternity_name="Theta Xi", fraternity_slug="theta-xi", school_name="Louisiana State University", index=index)
    assert active.final_status == "active"
    assert inactive.final_status == "inactive"


def test_probation_or_probationary_recognition_is_active_under_sanction_not_inactive():
    uf_index = build_campus_status_index(
        school_name="University of Florida",
        documents=[CampusSourceDocument(page_url="https://ufl.edu/ifc-conduct", title="Conduct Data", text=load_fixture("status_pages", "uf_ifc_conduct_page.html"), html=load_fixture("status_pages", "uf_ifc_conduct_page.html"))],
    )
    maryland_index = build_campus_status_index(
        school_name="University of Maryland",
        documents=[CampusSourceDocument(page_url="https://umd.edu/chapter-statuses", title="Chapter Statuses", text=load_fixture("status_pages", "maryland_chapter_statuses.html"), html=load_fixture("status_pages", "maryland_chapter_statuses.html"))],
    )
    uf_decision = decide_chapter_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="University of Florida", index=uf_index)
    md_decision = decide_chapter_status(fraternity_name="Sigma Phi Epsilon", fraternity_slug="sigma-phi-epsilon", school_name="University of Maryland", index=maryland_index)
    assert uf_decision.final_status == "active"
    assert md_decision.final_status == "active"


def test_interim_suspension_is_negative_status():
    index = build_campus_status_index(
        school_name="University of Florida",
        documents=[CampusSourceDocument(page_url="https://ufl.edu/ifc-conduct", title="Conduct Data", text=load_fixture("status_pages", "uf_ifc_conduct_page.html"), html=load_fixture("status_pages", "uf_ifc_conduct_page.html"))],
    )
    decision = decide_chapter_status(fraternity_name="Phi Delta Theta", fraternity_slug="phi-delta-theta", school_name="University of Florida", index=index)
    assert decision.final_status in {"inactive", "review"}


def test_no_greek_life_policy_marks_social_fraternity_inactive():
    index = build_campus_status_index(
        school_name="Williams College",
        documents=[CampusSourceDocument(page_url="https://williams.edu/fraternities", title="Fraternities", text=load_fixture("status_pages", "williams_no_fraternities_policy.html"), html=load_fixture("status_pages", "williams_no_fraternities_policy.html"))],
    )
    decision = decide_chapter_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="Williams College", index=index)
    assert decision.final_status == "inactive"


def test_absence_from_active_only_national_directory_does_not_alone_mark_inactive():
    decision = decide_chapter_status(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Virginia Commonwealth University",
        index=build_campus_status_index(school_name="Virginia Commonwealth University", documents=[]),
        national_evidence=NationalStatusEvidence(
            status=NationalStatusValue.NOT_LISTED_ON_ACTIVE_ONLY_DIRECTORY,
            capability=NationalDirectoryCapability.ACTIVE_ONLY,
            evidence_url="https://deltachi.org/chapter-directory/virginia/",
            confidence=0.5,
            reason_code="national_active_only_absence_not_conclusive",
        ),
    )
    assert decision.final_status == "unknown"


def test_school_office_email_not_written_as_chapter_contact():
    assert _email_local_part_looks_generic_office("studentengagement@du.edu") is True
