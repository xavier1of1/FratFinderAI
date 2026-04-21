from __future__ import annotations

from fratfinder_crawler.status.models import CampusStatusSource, CampusSourceType, StatusZoneType
from fratfinder_crawler.status.zone_parser import parse_status_zones

from ._helpers import load_fixture


def _source(url: str, html_name: str, title: str) -> CampusStatusSource:
    html = load_fixture("status_pages", html_name)
    return CampusStatusSource(
        school_name="Example University",
        source_url=url,
        source_host="example.edu",
        source_type=CampusSourceType.CHAPTER_STATUS_PAGE,
        authority_tier=1,
        currentness_score=0.95,
        completeness_score=0.95,
        parse_completeness_score=0.95,
        is_official_school_source=True,
        title=title,
        text=html,
        html=html,
    )


def test_lsu_active_suspended_closed_tabs_are_separate_zones():
    zones = parse_status_zones(_source("https://lsu.edu/greeks/scorecard", "lsu_scorecard_tabs.html", "Community Scorecard"))
    assert any(zone.zone_type == StatusZoneType.ACTIVE and "delta chi" in zone.text.lower() for zone in zones)
    assert any(zone.zone_type == StatusZoneType.SUSPENDED and "theta xi" in zone.text.lower() for zone in zones)
    assert any(zone.zone_type == StatusZoneType.CLOSED and "phi kappa psi" in zone.text.lower() for zone in zones)


def test_suspended_keyword_in_page_nav_does_not_poison_active_zone():
    zones = parse_status_zones(_source("https://lsu.edu/greeks/scorecard", "lsu_scorecard_tabs.html", "Community Scorecard"))
    active_zone = next(zone for zone in zones if zone.zone_type == StatusZoneType.ACTIVE)
    assert "delta chi" in active_zone.text.lower()
    assert "phi kappa psi" not in active_zone.text.lower()


def test_closed_keyword_in_other_tab_does_not_poison_active_fraternity_list():
    zones = parse_status_zones(_source("https://lsu.edu/greeks/scorecard", "lsu_scorecard_tabs.html", "Community Scorecard"))
    active_zone = next(zone for zone in zones if zone.zone_type == StatusZoneType.ACTIVE)
    assert "closed chapters" not in active_zone.text.lower()


def test_maryland_probationary_recognition_zone_is_positive_but_sanctioned():
    zones = parse_status_zones(_source("https://umd.edu/fsl/chapter-statuses", "maryland_chapter_statuses.html", "Chapter Statuses"))
    assert any(zone.zone_type == StatusZoneType.PROBATIONARY_RECOGNITION for zone in zones)


def test_uf_probation_zone_is_active_under_conduct_sanction():
    zones = parse_status_zones(_source("https://ufl.edu/ifc-conduct", "uf_ifc_conduct_page.html", "IFC Conduct"))
    assert any(zone.zone_type == StatusZoneType.CONDUCT_PROBATION for zone in zones)


def test_uf_interim_suspension_zone_is_negative():
    zones = parse_status_zones(_source("https://ufl.edu/ifc-conduct", "uf_ifc_conduct_page.html", "IFC Conduct"))
    assert any(zone.zone_type == StatusZoneType.INTERIM_SUSPENSION for zone in zones)


def test_williams_policy_creates_no_greek_policy_zone():
    zones = parse_status_zones(_source("https://williams.edu/fraternities", "williams_no_fraternities_policy.html", "Fraternities"))
    assert any(zone.zone_type == StatusZoneType.NO_GREEK_POLICY for zone in zones)


def test_article_statement_loss_of_recognition_is_negative_evidence_not_complete_roster():
    zones = parse_status_zones(_source("https://vcu.edu/news/delta-chi", "vcu_delta_chi_statement.html", "VCU Statement on Delta Chi"))
    assert any(zone.zone_type in {StatusZoneType.UNRECOGNIZED, StatusZoneType.UNKNOWN} for zone in zones)
