from __future__ import annotations

from fratfinder_crawler.status.models import NationalDirectoryCapability
from fratfinder_crawler.status.national_capabilities import classify_national_directory_capability

from ._helpers import load_fixture


def test_delta_chi_state_page_classified_active_only():
    capability, _ = classify_national_directory_capability(
        page_url="https://deltachi.org/chapter-directory/virginia/",
        title="Virginia Chapters",
        text=load_fixture("national_directories", "delta_chi_active_only_state_page.html"),
        html=load_fixture("national_directories", "delta_chi_active_only_state_page.html"),
    )
    assert capability == NationalDirectoryCapability.ACTIVE_ONLY


def test_pi_kappa_phi_page_classified_all_status():
    capability, _ = classify_national_directory_capability(
        page_url="https://pikapp.org/about/chapters/",
        title="Chapters",
        text=load_fixture("national_directories", "pi_kappa_phi_all_status_page.html"),
        html=load_fixture("national_directories", "pi_kappa_phi_all_status_page.html"),
    )
    assert capability == NationalDirectoryCapability.ALL_STATUS


def test_phi_kappa_tau_page_classified_all_status():
    capability, _ = classify_national_directory_capability(
        page_url="https://phikappatau.org/find-a-chapter",
        title="Find a Chapter",
        text=load_fixture("national_directories", "phi_kappa_tau_all_status_page.html"),
        html=load_fixture("national_directories", "phi_kappa_tau_all_status_page.html"),
    )
    assert capability == NationalDirectoryCapability.ALL_STATUS


def test_sigma_tau_gamma_page_classified_all_status_when_active_and_inactive_labels_present():
    capability, _ = classify_national_directory_capability(
        page_url="https://sigtau.org/chapters/",
        title="Chapter Directory",
        text=load_fixture("national_directories", "sigma_tau_gamma_mixed_status_directory.html"),
        html=load_fixture("national_directories", "sigma_tau_gamma_mixed_status_directory.html"),
    )
    assert capability == NationalDirectoryCapability.ALL_STATUS


def test_sigma_nu_active_and_dormant_pages_classified_split_capability():
    capability, _ = classify_national_directory_capability(
        page_url="https://sigmanu.org/about-us/chapter-listing/dormant-chapters",
        title="Dormant Chapters",
        text=load_fixture("national_directories", "sigma_nu_dormant_chapters_page.html"),
        html=load_fixture("national_directories", "sigma_nu_dormant_chapters_page.html"),
    )
    assert capability == NationalDirectoryCapability.ACTIVE_DORMANT_SPLIT


def test_historical_roll_not_used_as_current_status_source():
    capability, _ = classify_national_directory_capability(
        page_url="https://example.org/chapter-roll",
        title="Historical Chapter Roll",
        text=load_fixture("national_directories", "generic_historical_chapter_roll.html"),
        html=load_fixture("national_directories", "generic_historical_chapter_roll.html"),
    )
    assert capability == NationalDirectoryCapability.HISTORY_ROLL


def test_map_locator_shell_requires_fetch_or_browser_fallback_before_absence_inference():
    capability, _ = classify_national_directory_capability(
        page_url="https://example.org/map-locator",
        title="Map Locator",
        text=load_fixture("national_directories", "map_locator_shell.html"),
        html=load_fixture("national_directories", "map_locator_shell.html"),
    )
    assert capability == NationalDirectoryCapability.MAP_WITH_STATUS_FILTER
