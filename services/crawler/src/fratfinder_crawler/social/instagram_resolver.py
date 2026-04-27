from __future__ import annotations

from fratfinder_crawler.social.instagram_models import InstagramCandidate
from fratfinder_crawler.social.instagram_models import InstagramSourceType


_WRITE_THRESHOLDS = {
    InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE: 0.88,
    InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW: 0.88,
    InstagramSourceType.VERIFIED_CHAPTER_WEBSITE: 0.88,
    InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA: 0.88,
    InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK: 0.88,
    InstagramSourceType.AUTHORITATIVE_BUNDLE: 0.88,
    InstagramSourceType.PROVENANCE_SUPPORTING_PAGE: 0.84,
    InstagramSourceType.NATIONALS_DIRECTORY_ROW: 0.90,
    InstagramSourceType.NATIONALS_CHAPTER_ENTRY: 0.90,
    InstagramSourceType.NATIONALS_CHAPTER_PAGE: 0.90,
    InstagramSourceType.SEARCH_RESULT_PROFILE: 0.94,
    InstagramSourceType.GENERATED_HANDLE_SEARCH: 0.94,
}

_DEFAULT_WRITE_THRESHOLD = 0.90
_DEFAULT_REVIEW_THRESHOLD = 0.82


def instagram_write_threshold(candidate: InstagramCandidate) -> float:
    return _WRITE_THRESHOLDS.get(candidate.source_type, _DEFAULT_WRITE_THRESHOLD)


def instagram_review_threshold() -> float:
    return _DEFAULT_REVIEW_THRESHOLD


def select_best_instagram_candidate(
    candidates: list[InstagramCandidate],
    *,
    minimum_confidence: float | None = None,
) -> InstagramCandidate | None:
    accepted = [
        candidate
        for candidate in candidates
        if not candidate.reject_reasons
        and candidate.confidence >= (minimum_confidence if minimum_confidence is not None else instagram_write_threshold(candidate))
    ]
    accepted.sort(key=lambda item: item.confidence, reverse=True)
    return accepted[0] if accepted else None
