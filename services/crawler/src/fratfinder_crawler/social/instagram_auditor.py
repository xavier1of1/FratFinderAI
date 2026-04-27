from __future__ import annotations

from fratfinder_crawler.social.instagram_identity import ChapterInstagramIdentity
from fratfinder_crawler.social.instagram_models import InstagramCandidate, InstagramResolutionDecision, InstagramSourceType
from fratfinder_crawler.social.instagram_normalizer import canonicalize_instagram_profile
from fratfinder_crawler.social.instagram_resolver import instagram_review_threshold, instagram_write_threshold
from fratfinder_crawler.social.instagram_scorer import score_instagram_candidate


def audit_existing_instagram_candidate(
    *,
    chapter_id: str,
    existing_url: str | None,
    identity: ChapterInstagramIdentity,
    candidates: list[InstagramCandidate],
) -> InstagramResolutionDecision:
    current_url = canonicalize_instagram_profile(existing_url)
    decision = InstagramResolutionDecision(
        chapter_id=chapter_id,
        previous_url=current_url,
        outcome="unvalidated",
        reason_code="review_existing_instagram_ambiguous",
    )
    scored_candidates = [score_instagram_candidate(candidate, identity) for candidate in candidates]
    accepted_candidates = [candidate for candidate in scored_candidates if not candidate.reject_reasons]
    accepted_candidates.sort(key=lambda item: item.confidence, reverse=True)
    current_candidate = None
    if current_url:
        for candidate in accepted_candidates:
            if candidate.profile_url == current_url:
                current_candidate = candidate
                break
        if current_candidate is None:
            current_candidate = score_instagram_candidate(
                InstagramCandidate(
                    handle=current_url.rstrip("/").split("/")[-1],
                    profile_url=current_url,
                    source_type=InstagramSourceType.EXISTING_DB_VALUE,
                    source_url=current_url,
                    evidence_url=current_url,
                ),
                identity,
            )
    if current_candidate is not None and not current_candidate.reject_reasons and current_candidate.confidence >= 0.88:
        decision.outcome = "existing_value_confirmed"
        decision.reason_code = "existing_instagram_confirmed_by_authoritative_source"
        decision.selected_url = current_candidate.profile_url
        decision.selected_handle = current_candidate.handle
        decision.confidence = current_candidate.confidence
        decision.accepted_candidate = current_candidate
        return decision

    best_replacement = next((candidate for candidate in accepted_candidates if candidate.profile_url != current_url), None)
    if best_replacement is not None and best_replacement.confidence >= 0.98 and best_replacement.source_type in {
        InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
        InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
        InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
        InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA,
        InstagramSourceType.NATIONALS_DIRECTORY_ROW,
        InstagramSourceType.NATIONALS_CHAPTER_PAGE,
        InstagramSourceType.NATIONALS_CHAPTER_ENTRY,
    }:
        decision.outcome = "existing_value_replaced"
        decision.reason_code = "existing_instagram_replaced_by_higher_confidence_candidate"
        decision.selected_url = best_replacement.profile_url
        decision.selected_handle = best_replacement.handle
        decision.confidence = best_replacement.confidence
        decision.accepted_candidate = best_replacement
        return decision

    if current_candidate is not None and current_candidate.reject_reasons:
        decision.outcome = "review_required"
        decision.reason_code = current_candidate.reject_reasons[0]
        decision.confidence = 0.0
        decision.rejected_candidates = [current_candidate, *accepted_candidates[:3]]
        return decision

    if best_replacement is not None and best_replacement.confidence >= max(instagram_review_threshold(), min(0.88, instagram_write_threshold(best_replacement))):
        decision.outcome = "review_required"
        decision.reason_code = "review_existing_instagram_ambiguous"
        decision.accepted_candidate = best_replacement
        decision.confidence = best_replacement.confidence
        return decision

    return decision
