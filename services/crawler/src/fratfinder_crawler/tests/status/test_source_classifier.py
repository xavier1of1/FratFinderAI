from __future__ import annotations

from fratfinder_crawler.status.models import CampusSourceType
from fratfinder_crawler.status.source_classifier import classify_campus_source

from ._helpers import load_fixture


def test_recognized_roster_classified_as_recognized_roster():
    source = classify_campus_source(
        school_name="University of Delaware",
        page_url="https://udel.edu/students/involvement/fsll/chapters/",
        title="Recognized Chapters",
        text=load_fixture("status_pages", "delaware_recognized_chapters.html"),
        html=load_fixture("status_pages", "delaware_recognized_chapters.html"),
    )
    assert source.source_type == CampusSourceType.RECOGNIZED_ROSTER
    assert source.is_official_school_source is True


def test_scorecard_with_tabs_classified_as_chapter_status_page():
    source = classify_campus_source(
        school_name="Louisiana State University",
        page_url="https://lsu.edu/greeks/scorecard",
        title="Community Scorecard",
        text=load_fixture("status_pages", "lsu_scorecard_tabs.html"),
        html=load_fixture("status_pages", "lsu_scorecard_tabs.html"),
    )
    assert source.source_type == CampusSourceType.CHAPTER_STATUS_PAGE


def test_suspended_page_classified_as_suspended_unrecognized_page():
    source = classify_campus_source(
        school_name="Penn State",
        page_url="https://studentaffairs.psu.edu/suspended-unrecognized",
        title="Suspended and Unrecognized Groups",
        text=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"),
        html=load_fixture("status_pages", "penn_state_suspended_unrecognized.html"),
    )
    assert source.source_type == CampusSourceType.SUSPENDED_UNRECOGNIZED_PAGE


def test_conduct_page_classified_as_conduct_scorecard():
    source = classify_campus_source(
        school_name="University of Florida",
        page_url="https://ufl.edu/ifc-conduct",
        title="Interfraternity Council Conduct Data",
        text=load_fixture("status_pages", "uf_ifc_conduct_page.html"),
        html=load_fixture("status_pages", "uf_ifc_conduct_page.html"),
    )
    assert source.source_type == CampusSourceType.CONDUCT_SCORECARD


def test_no_greek_policy_classified_as_no_greek_policy():
    source = classify_campus_source(
        school_name="Williams College",
        page_url="https://dean.williams.edu/student-handbook/fraternities/",
        title="Fraternities",
        text=load_fixture("status_pages", "williams_no_fraternities_policy.html"),
        html=load_fixture("status_pages", "williams_no_fraternities_policy.html"),
    )
    assert source.source_type == CampusSourceType.NO_GREEK_POLICY


def test_news_article_not_classified_as_complete_roster():
    source = classify_campus_source(
        school_name="Virginia Commonwealth University",
        page_url="https://news.vcu.edu/article/vcu_statement_on_delta_chi",
        title="VCU Statement on Delta Chi",
        text=load_fixture("status_pages", "vcu_delta_chi_statement.html"),
        html=load_fixture("status_pages", "vcu_delta_chi_statement.html"),
    )
    assert source.source_type in {CampusSourceType.OFFICIAL_STATEMENT_LOSS_OF_RECOGNITION, CampusSourceType.ARTICLE_OR_NEWS}
    assert source.completeness_score < 0.9


def test_dynamic_shell_not_marked_complete_without_rendered_content():
    source = classify_campus_source(
        school_name="University of California, Riverside",
        page_url="https://highlanderlink.ucr.edu/organizations",
        title="Organizations",
        text=load_fixture("status_pages", "dynamic_rso_shell_no_rendered_orgs.html"),
        html=load_fixture("status_pages", "dynamic_rso_shell_no_rendered_orgs.html"),
    )
    assert source.source_type == CampusSourceType.DYNAMIC_SHELL
    assert source.parse_completeness_score < 0.5
