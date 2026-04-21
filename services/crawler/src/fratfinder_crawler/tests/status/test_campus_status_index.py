from __future__ import annotations

from fratfinder_crawler.status.campus_discovery import CampusSourceDocument, build_campus_status_index
from fratfinder_crawler.status.decision_engine import decide_chapter_status

from ._helpers import load_fixture


def test_index_contains_sources_zones_and_evidence():
    index = build_campus_status_index(
        school_name="Louisiana State University",
        documents=[
            CampusSourceDocument(
                page_url="https://lsu.edu/greeks/scorecard",
                title="Community Scorecard",
                text=load_fixture("status_pages", "lsu_scorecard_tabs.html"),
                html=load_fixture("status_pages", "lsu_scorecard_tabs.html"),
            )
        ],
    )
    assert index.metadata["sourceCount"] == 1
    assert index.metadata["zoneCount"] >= 3


def test_index_best_positive_match_returns_active_zone_match():
    index = build_campus_status_index(
        school_name="Louisiana State University",
        documents=[CampusSourceDocument(page_url="https://lsu.edu/greeks/scorecard", title="Community Scorecard", text=load_fixture("status_pages", "lsu_scorecard_tabs.html"), html=load_fixture("status_pages", "lsu_scorecard_tabs.html"))],
    )
    decision = decide_chapter_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="Louisiana State University", index=index)
    assert decision.final_status == "active"


def test_index_best_negative_match_returns_suspended_zone_match():
    index = build_campus_status_index(
        school_name="Louisiana State University",
        documents=[CampusSourceDocument(page_url="https://lsu.edu/greeks/scorecard", title="Community Scorecard", text=load_fixture("status_pages", "lsu_scorecard_tabs.html"), html=load_fixture("status_pages", "lsu_scorecard_tabs.html"))],
    )
    decision = decide_chapter_status(fraternity_name="Phi Delta Theta", fraternity_slug="phi-delta-theta", school_name="Louisiana State University", index=index)
    assert decision.final_status == "inactive"


def test_index_no_greek_policy_applies_to_all_social_fraternities():
    index = build_campus_status_index(
        school_name="Williams College",
        documents=[CampusSourceDocument(page_url="https://williams.edu/fraternities", title="Fraternities", text=load_fixture("status_pages", "williams_no_fraternities_policy.html"), html=load_fixture("status_pages", "williams_no_fraternities_policy.html"))],
    )
    decision = decide_chapter_status(fraternity_name="Delta Chi", fraternity_slug="delta-chi", school_name="Williams College", index=index)
    assert decision.school_recognition_status == "banned_no_greek_life"


def test_index_tracks_parse_completeness_score():
    index = build_campus_status_index(
        school_name="University of Delaware",
        documents=[CampusSourceDocument(page_url="https://udel.edu/recognized", title="Recognized Chapters", text=load_fixture("status_pages", "delaware_recognized_chapters.html"), html=load_fixture("status_pages", "delaware_recognized_chapters.html"))],
    )
    assert index.parse_completeness_score >= 0.85


def test_index_tracks_currentness_score():
    index = build_campus_status_index(
        school_name="University of Delaware",
        documents=[CampusSourceDocument(page_url="https://udel.edu/recognized", title="Recognized Chapters", text=load_fixture("status_pages", "delaware_recognized_chapters.html"), html=load_fixture("status_pages", "delaware_recognized_chapters.html"))],
    )
    assert index.currentness_score >= 0.7
