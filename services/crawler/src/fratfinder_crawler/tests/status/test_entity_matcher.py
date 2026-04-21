from __future__ import annotations

from fratfinder_crawler.status.entity_matcher import match_fraternity_in_zone
from fratfinder_crawler.status.models import StatusZone, StatusZoneType


def _zone(text: str) -> StatusZone:
    return StatusZone(source_url="https://example.edu", zone_type=StatusZoneType.ACTIVE, text=text, confidence=0.9)


def test_exact_fraternity_name_match():
    result = match_fraternity_in_zone(fraternity_name="Delta Chi", fraternity_slug="delta-chi", zone=_zone("Delta Chi fraternity recognized chapter"))
    assert result.matched is True
    assert result.match_method == "exact_alias"


def test_greek_letter_alias_match():
    result = match_fraternity_in_zone(fraternity_name="Delta Chi", fraternity_slug="delta-chi", zone=_zone("Δ Χ chapter recognized fraternity"))
    assert result.matched is True


def test_common_nickname_match_sigep_fiji_pike_ato():
    assert match_fraternity_in_zone(fraternity_name="Sigma Alpha Epsilon", fraternity_slug="sigma-alpha-epsilon", zone=_zone("SigEp chapter")).matched is True
    assert match_fraternity_in_zone(fraternity_name="Phi Gamma Delta", fraternity_slug="phi-gamma-delta", zone=_zone("FIJI fraternity")).matched is True
    assert match_fraternity_in_zone(fraternity_name="Pi Kappa Alpha", fraternity_slug="pi-kappa-alpha", zone=_zone("Pike recognized chapter")).matched is True
    assert match_fraternity_in_zone(fraternity_name="Alpha Tau Omega", fraternity_slug="alpha-tau-omega", zone=_zone("ATO chapter")).matched is True


def test_delta_alone_does_not_match_delta_chi():
    result = match_fraternity_in_zone(fraternity_name="Delta Chi", fraternity_slug="delta-chi", zone=_zone("Delta engineering students"))
    assert result.matched is False


def test_phi_kappa_does_not_match_phi_kappa_tau_or_phi_kappa_psi_without_full_context():
    result = match_fraternity_in_zone(fraternity_name="Phi Kappa Tau", fraternity_slug="phi-kappa-tau", zone=_zone("Phi Kappa alumni"))
    assert result.matched is False


def test_match_requires_fraternity_context_for_fuzzy_acceptance():
    result = match_fraternity_in_zone(fraternity_name="Delta Chi", fraternity_slug="delta-chi", zone=_zone("Delta Chi received campus honors in athletics"))
    assert result.matched is False


def test_school_alias_required_when_page_mentions_multiple_campuses():
    result = match_fraternity_in_zone(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Louisiana State University",
        zone=_zone("Delta Chi chapter at University of Delaware and William & Mary"),
    )
    assert result.matched is False
