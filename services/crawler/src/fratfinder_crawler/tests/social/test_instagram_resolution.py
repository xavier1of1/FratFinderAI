from __future__ import annotations

from fratfinder_crawler.models import ChapterEvidenceRecord
from fratfinder_crawler.social.instagram_extractor import extract_instagram_from_verified_chapter_website
from fratfinder_crawler.social.instagram_candidate_bank import candidate_from_chapter_evidence
from fratfinder_crawler.social.instagram_auditor import audit_existing_instagram_candidate
from fratfinder_crawler.social.instagram_identity import build_chapter_instagram_identity
from fratfinder_crawler.social.instagram_models import InstagramCandidate, InstagramSourceType
from fratfinder_crawler.social.instagram_normalizer import canonicalize_instagram_profile, classify_instagram_url
from fratfinder_crawler.social.instagram_resolver import instagram_write_threshold, select_best_instagram_candidate
from fratfinder_crawler.social.instagram_scorer import score_instagram_candidate


def test_instagram_normalizer_rejects_reel_and_canonicalizes_profile():
    assert canonicalize_instagram_profile("@msstatedeltachi") == "https://www.instagram.com/msstatedeltachi/"
    assert classify_instagram_url("https://www.instagram.com/reel/abc123/").reject_reason == "non_profile_path"
    assert canonicalize_instagram_profile("https://www.instagram.com/manifest.json/") is None
    assert canonicalize_instagram_profile("https://www.instagram.com/1776260826/") is None


def test_verified_website_extractor_ignores_non_instagram_relative_assets():
    candidates = extract_instagram_from_verified_chapter_website(
        text="Follow us on Instagram @msstatedeltachi",
        links=["/manifest.json", "/style.css", "https://www.instagram.com/msstatedeltachi/"],
        html="""
        <html>
          <head>
            <link rel="manifest" href="/manifest.json" />
            <link rel="stylesheet" href="/style.css" />
          </head>
          <body>
            <a href="https://www.instagram.com/msstatedeltachi/">Instagram</a>
          </body>
        </html>
        """,
        source_url="https://www.ufdeltachi.org/",
        page_scope="chapter_website",
        contact_specificity="chapter_specific",
        source_title="Delta Chi at UF",
        source_snippet="Official chapter website",
        local_container_kind="homepage",
    )

    assert [candidate.profile_url for candidate in candidates] == ["https://www.instagram.com/msstatedeltachi/"]


def test_select_best_instagram_candidate_respects_source_thresholds():
    identity = build_chapter_instagram_identity(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Mississippi State University",
        chapter_name="Mississippi State Chapter",
    )
    search_candidate = score_instagram_candidate(
        InstagramCandidate(
            handle="msstatedeltachi",
            profile_url="https://www.instagram.com/msstatedeltachi/",
            source_type=InstagramSourceType.SEARCH_RESULT_PROFILE,
            source_url="https://www.instagram.com/msstatedeltachi/",
            evidence_url="https://www.instagram.com/msstatedeltachi/",
            source_snippet="Delta Chi Mississippi State Instagram",
        ),
        identity,
    )
    official_candidate = score_instagram_candidate(
        InstagramCandidate(
            handle="msstatedeltachi",
            profile_url="https://www.instagram.com/msstatedeltachi/",
            source_type=InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
            source_url="https://www.msstate.edu/greek-life/delta-chi",
            evidence_url="https://www.msstate.edu/greek-life/delta-chi",
            page_scope="school_affiliation_page",
            contact_specificity="school_specific",
            source_title="Delta Chi Mississippi State University Official Instagram",
            source_snippet="Official school chapter page for Delta Chi at Mississippi State University",
        ),
        identity,
    )

    assert instagram_write_threshold(search_candidate) == 0.94
    assert instagram_write_threshold(official_candidate) == 0.88
    assert instagram_write_threshold(
        InstagramCandidate(
            handle="msstatedeltachi",
            profile_url="https://www.instagram.com/msstatedeltachi/",
            source_type=InstagramSourceType.PROVENANCE_SUPPORTING_PAGE,
        )
    ) == 0.84
    assert select_best_instagram_candidate([search_candidate, official_candidate]) is official_candidate


def test_audit_existing_instagram_replaces_wrong_value_with_authoritative_candidate():
    identity = build_chapter_instagram_identity(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Mississippi State University",
        chapter_name="Mississippi State Chapter",
    )
    official_candidate = score_instagram_candidate(
        InstagramCandidate(
            handle="msstatedeltachi",
            profile_url="https://www.instagram.com/msstatedeltachi/",
            source_type=InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
            source_url="https://www.msstate.edu/greek-life/delta-chi",
            evidence_url="https://www.msstate.edu/greek-life/delta-chi",
            page_scope="school_affiliation_page",
            contact_specificity="school_specific",
            source_title="Delta Chi Mississippi State University Official Instagram",
            source_snippet="Official school chapter page for Delta Chi at Mississippi State University",
        ),
        identity,
    )

    decision = audit_existing_instagram_candidate(
        chapter_id="chapter-1",
        existing_url="https://www.instagram.com/sigmachi/",
        identity=identity,
        candidates=[official_candidate],
    )

    assert decision.outcome == "existing_value_replaced"
    assert decision.selected_url == "https://www.instagram.com/msstatedeltachi/"


def test_direct_instagram_provenance_requires_real_identity_binding():
    identity = build_chapter_instagram_identity(
        fraternity_name="Delta Chi",
        fraternity_slug="delta-chi",
        school_name="Mississippi State University",
        chapter_name="Mississippi State Chapter",
    )
    personal_candidate = score_instagram_candidate(
        InstagramCandidate(
            handle="lexiiii.hutch",
            profile_url="https://www.instagram.com/lexiiii.hutch/",
            source_type=InstagramSourceType.PROVENANCE_SUPPORTING_PAGE,
            source_url="https://www.instagram.com/lexiiii.hutch/",
            evidence_url="https://www.instagram.com/lexiiii.hutch/",
            page_scope="instagram_profile",
            contact_specificity="chapter_specific",
            source_snippet="Instagram profile",
        ),
        identity,
    )

    assert "rejected_wrong_fraternity_instagram" in personal_candidate.reject_reasons
    assert "rejected_wrong_school_instagram" in personal_candidate.reject_reasons


def test_candidate_bank_maps_legacy_provenance_and_school_metadata_to_higher_trust_types():
    provenance_candidate = candidate_from_chapter_evidence(
        ChapterEvidenceRecord(
            chapter_id="chapter-1",
            chapter_slug="chapter-one",
            fraternity_slug="delta-chi",
            source_slug="delta-chi-main",
            crawl_run_id=911,
            field_name="instagram_url",
            candidate_value="https://www.instagram.com/msstatedeltachi/",
            confidence=0.91,
            source_url="https://www.msstate.edu/greek-life/delta-chi",
            source_snippet="Official page",
            metadata={"sourceType": "provenance", "pageScope": "school_affiliation_page", "contactSpecificity": "school_specific"},
        )
    )
    school_candidate = candidate_from_chapter_evidence(
        ChapterEvidenceRecord(
            chapter_id="chapter-2",
            chapter_slug="chapter-two",
            fraternity_slug="delta-chi",
            source_slug="delta-chi-main",
            crawl_run_id=911,
            field_name="instagram_url",
            candidate_value="https://www.instagram.com/msstatedeltachi/",
            confidence=0.91,
            source_url="https://www.msstate.edu/greek-life/delta-chi",
            source_snippet="Official page",
            metadata={"pageScope": "school_affiliation_page", "contactSpecificity": "school_specific"},
        )
    )

    assert provenance_candidate is not None
    assert provenance_candidate.source_type is InstagramSourceType.PROVENANCE_SUPPORTING_PAGE
    assert school_candidate is not None
    assert school_candidate.source_type is InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE


def test_candidate_bank_defaults_unknown_search_like_rows_to_search_result_profile():
    search_candidate = candidate_from_chapter_evidence(
        ChapterEvidenceRecord(
            chapter_id="chapter-3",
            chapter_slug="chapter-three",
            fraternity_slug="delta-chi",
            source_slug="delta-chi-main",
            crawl_run_id=911,
            field_name="instagram_url",
            candidate_value="https://www.instagram.com/msstatedeltachi/",
            confidence=0.75,
            source_url="https://www.instagram.com/msstatedeltachi/",
            source_snippet="Instagram result",
            provider="searxng_json",
            query='site:instagram.com "Delta Chi" "Mississippi State"',
            metadata={},
        )
    )

    assert search_candidate is not None
    assert search_candidate.source_type is InstagramSourceType.SEARCH_RESULT_PROFILE
