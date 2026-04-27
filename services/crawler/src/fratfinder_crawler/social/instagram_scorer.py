from __future__ import annotations

import re
from urllib.parse import urlparse

from fratfinder_crawler.social.instagram_identity import ChapterInstagramIdentity
from fratfinder_crawler.social.instagram_models import InstagramCandidate, InstagramSourceType
from fratfinder_crawler.social.instagram_normalizer import classify_instagram_url


_NEGATIVE_ACCOUNT_MARKERS = {
    "alumni": "rejected_alumni_instagram",
    "ifc": "rejected_school_fsl_or_ifc_instagram",
    "panhellenic": "rejected_school_fsl_or_ifc_instagram",
    "greeklife": "rejected_school_fsl_or_ifc_instagram",
    "greek_life": "rejected_school_fsl_or_ifc_instagram",
    "fsl": "rejected_school_fsl_or_ifc_instagram",
    "hq": "rejected_national_hq_instagram",
    "ihq": "rejected_national_hq_instagram",
    "national": "rejected_generic_national_instagram",
    "international": "rejected_national_hq_instagram",
    "foundation": "rejected_generic_national_instagram",
    "office": "rejected_school_fsl_or_ifc_instagram",
}

_SOURCE_PRIORS = {
    InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE: 0.97,
    InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW: 0.97,
    InstagramSourceType.VERIFIED_CHAPTER_WEBSITE: 0.95,
    InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA: 0.92,
    InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK: 0.92,
    InstagramSourceType.NATIONALS_DIRECTORY_ROW: 0.93,
    InstagramSourceType.NATIONALS_CHAPTER_ENTRY: 0.93,
    InstagramSourceType.NATIONALS_CHAPTER_PAGE: 0.92,
    InstagramSourceType.AUTHORITATIVE_BUNDLE: 0.95,
    InstagramSourceType.PROVENANCE_SUPPORTING_PAGE: 0.9,
    InstagramSourceType.SEARCH_RESULT_PROFILE: 0.72,
    InstagramSourceType.GENERATED_HANDLE_SEARCH: 0.60,
    InstagramSourceType.EXISTING_DB_VALUE: 0.5,
    InstagramSourceType.NATIONAL_FOLLOWING_SEED: 0.35,
    InstagramSourceType.REVIEW_OVERRIDE: 1.0,
}


def _tokenize(text: str | None) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", str(text or "").lower()) if token}


def _handle_tokens(candidate: InstagramCandidate) -> set[str]:
    return _tokenize(candidate.handle.replace("_", " ").replace(".", " "))


def _identity_overlap(tokens: set[str], values: list[str]) -> tuple[float, list[str]]:
    best_score = 0.0
    matched: list[str] = []
    for value in values:
        value_tokens = _tokenize(value)
        if not value_tokens:
            continue
        overlap = value_tokens & tokens
        if not overlap:
            continue
        ratio = len(overlap) / max(1, len(value_tokens))
        if value_tokens.issubset(tokens):
            ratio = 1.0
        if ratio > best_score:
            best_score = ratio
        if ratio >= 0.6:
            matched.append(value)
    return min(1.0, best_score), matched


def _compact_substring_score(text: str, values: list[str]) -> tuple[float, list[str]]:
    normalized = "".join(ch for ch in str(text or "").lower() if ch.isalnum())
    if not normalized:
        return 0.0, []
    matched = [value for value in values if value and len(value) >= 3 and value in normalized]
    return (1.0 if matched else 0.0), matched


def _combined_text(candidate: InstagramCandidate) -> str:
    return " ".join(
        part
        for part in [
            candidate.handle,
            candidate.source_url,
            candidate.source_title,
            candidate.source_snippet,
            candidate.surrounding_text,
            candidate.local_container_text,
        ]
        if part
    )


def score_instagram_candidate(candidate: InstagramCandidate, identity: ChapterInstagramIdentity) -> InstagramCandidate:
    normalized = classify_instagram_url(candidate.profile_url)
    if normalized.kind != normalized.kind.PROFILE:
        candidate.is_profile_url = False
        candidate.reject_reasons.append("rejected_instagram_not_profile_url")
        return candidate

    candidate.source_trust_score = _SOURCE_PRIORS.get(candidate.source_type, 0.5)
    handle_tokens = _handle_tokens(candidate)
    combined_tokens = _tokenize(_combined_text(candidate))
    combined_text = _combined_text(candidate)
    fraternity_overlap_score, matched_fraternity = _identity_overlap(
        handle_tokens | combined_tokens,
        identity.fraternity_full_names + identity.fraternity_aliases + identity.fraternity_compact_tokens + identity.fraternity_initials,
    )
    fraternity_compact_score, matched_fraternity_compacts = _compact_substring_score(combined_text, identity.fraternity_compact_tokens)
    school_overlap_score, matched_school = _identity_overlap(
        handle_tokens | combined_tokens,
        identity.school_full_names + identity.school_aliases + identity.school_compact_tokens + identity.school_initials + identity.school_city_tokens + identity.school_state_tokens,
    )
    school_compact_score, matched_school_compacts = _compact_substring_score(combined_text, identity.school_compact_tokens)
    chapter_overlap_score, matched_chapter = _identity_overlap(
        handle_tokens | combined_tokens,
        identity.chapter_names + identity.chapter_compact_tokens + identity.chapter_greek_letters,
    )
    chapter_compact_score, matched_chapter_compacts = _compact_substring_score(combined_text, identity.chapter_compact_tokens)
    fraternity_score = max(fraternity_overlap_score, fraternity_compact_score)
    school_score = max(school_overlap_score, school_compact_score)
    chapter_score = max(chapter_overlap_score, chapter_compact_score)
    candidate.fraternity_identity_score = fraternity_score
    candidate.school_identity_score = school_score
    candidate.chapter_identity_score = chapter_score
    candidate.matched_fraternity_aliases = list(dict.fromkeys([*matched_fraternity, *matched_fraternity_compacts]))
    candidate.matched_school_aliases = list(dict.fromkeys([*matched_school, *matched_school_compacts]))
    candidate.matched_chapter_aliases = list(dict.fromkeys([*matched_chapter, *matched_chapter_compacts]))
    if candidate.source_type in {
        InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
        InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
    } and candidate.contact_specificity in {"school_specific", "chapter_specific"} and (fraternity_score > 0 or chapter_score > 0):
        candidate.school_identity_score = max(candidate.school_identity_score, 0.75)
        school_score = candidate.school_identity_score
    candidate.handle_pattern_score = 1.0 if fraternity_score > 0 and (school_score > 0 or chapter_score > 0) else 0.5 if fraternity_score > 0 else 0.0
    candidate.textual_binding_score = min(1.0, (fraternity_score + school_score + chapter_score) / 2.0)
    candidate.locality_score = 1.0 if candidate.contact_specificity in {"chapter_specific", "school_specific", "national_specific_to_chapter"} else 0.4
    candidate.duplicate_safety_score = 0.0 if candidate.already_assigned_to_other_chapter_ids else 1.0

    lowered = _combined_text(candidate).lower()
    for marker, reason in _NEGATIVE_ACCOUNT_MARKERS.items():
        if marker in candidate.handle.lower() or marker in lowered:
            candidate.reject_reasons.append(reason)
            if "hq" in marker or marker in {"national", "international", "foundation"}:
                candidate.is_national_hq_account = True
            if marker in {"ifc", "panhellenic", "greeklife", "greek_life", "fsl", "office"}:
                candidate.is_school_fsl_or_ifc_account = True
            if marker == "alumni":
                candidate.is_alumni_account = True

    if candidate.already_assigned_to_other_chapter_ids:
        candidate.reject_reasons.append("rejected_cross_school_instagram_reuse")
    if candidate.source_type == InstagramSourceType.GENERATED_HANDLE_SEARCH and candidate.source_trust_score < 0.94:
        candidate.reject_reasons.append("rejected_generated_handle_without_confirmation")
    if candidate.page_scope in {"nationals_generic", "school_affiliation_page"} and candidate.contact_specificity in {"national_generic", "ambiguous"}:
        candidate.reject_reasons.append("rejected_global_footer_instagram")
    source_host = (urlparse(str(candidate.source_url or "")).netloc or "").lower().removeprefix("www.")
    if candidate.source_type == InstagramSourceType.PROVENANCE_SUPPORTING_PAGE and source_host == "instagram.com":
        if candidate.fraternity_identity_score <= 0.0 and candidate.chapter_identity_score <= 0.0:
            candidate.reject_reasons.append("rejected_wrong_fraternity_instagram")
        if candidate.school_identity_score <= 0.0 and candidate.chapter_identity_score <= 0.0:
            candidate.reject_reasons.append("rejected_wrong_school_instagram")
    if candidate.school_identity_score <= 0.0 and candidate.chapter_identity_score <= 0.0 and candidate.source_type in {
        InstagramSourceType.SEARCH_RESULT_PROFILE,
        InstagramSourceType.GENERATED_HANDLE_SEARCH,
        InstagramSourceType.EXISTING_DB_VALUE,
    }:
        candidate.reject_reasons.append("rejected_wrong_school_instagram")
    if candidate.fraternity_identity_score <= 0.0 and candidate.source_type in {
        InstagramSourceType.SEARCH_RESULT_PROFILE,
        InstagramSourceType.GENERATED_HANDLE_SEARCH,
        InstagramSourceType.EXISTING_DB_VALUE,
    }:
        candidate.reject_reasons.append("rejected_wrong_fraternity_instagram")

    if candidate.reject_reasons:
        candidate.score = 0.0
        candidate.confidence = 0.0
        return candidate

    score = (
        candidate.source_trust_score * 0.35
        + candidate.fraternity_identity_score * 0.2
        + candidate.school_identity_score * 0.16
        + candidate.chapter_identity_score * 0.08
        + candidate.handle_pattern_score * 0.08
        + candidate.textual_binding_score * 0.06
        + candidate.locality_score * 0.04
        + candidate.duplicate_safety_score * 0.03
    )
    candidate.score = round(max(0.0, min(1.0, score)), 4)
    candidate.confidence = candidate.score
    if candidate.score >= 0.98:
        candidate.accept_reasons.append("strong_authoritative_match")
    elif candidate.score >= 0.94:
        candidate.accept_reasons.append("search_profile_with_identity_match")
    elif candidate.score >= 0.88:
        candidate.accept_reasons.append("trusted_local_match")
    return candidate
