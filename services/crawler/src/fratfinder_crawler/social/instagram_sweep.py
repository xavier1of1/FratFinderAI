from __future__ import annotations

from typing import Any

from fratfinder_crawler.models import ReviewItemCandidate
from fratfinder_crawler.social.instagram_auditor import audit_existing_instagram_candidate
from fratfinder_crawler.social.instagram_candidate_bank import InstagramCandidateBank, candidate_from_chapter_evidence
from fratfinder_crawler.social.instagram_identity import build_chapter_instagram_identity
from fratfinder_crawler.social.instagram_models import InstagramCandidate
from fratfinder_crawler.social.instagram_resolver import select_best_instagram_candidate
from fratfinder_crawler.social.instagram_scorer import score_instagram_candidate


def run_instagram_sweep(
    *,
    repository,
    request_id: str,
    source_slug: str,
    crawl_run_id: int,
) -> dict[str, int]:
    chapters = repository.list_chapters_for_crawl_run(crawl_run_id)
    chapter_ids = [str(chapter["chapter_id"]) for chapter in chapters if chapter.get("chapter_id")]
    evidence_rows = repository.fetch_instagram_candidates_for_chapters(chapter_ids) if chapter_ids else []
    bank = InstagramCandidateBank()
    for row in evidence_rows:
        candidate = candidate_from_chapter_evidence(row)
        if candidate is not None and row.chapter_id is not None:
            candidate.metadata["fraternitySlug"] = row.fraternity_slug
            bank.add_candidate(str(row.chapter_id), candidate)
    bank.dedupe_by_handle_and_source()

    summary = {
        "candidates": 0,
        "resolved": 0,
        "review": 0,
        "jobsCanceled": 0,
        "existingConfirmed": 0,
        "existingReplaced": 0,
    }

    winners: dict[str, tuple[InstagramCandidate, dict[str, Any]]] = {}
    conflicts: set[str] = set()
    for chapter in chapters:
        chapter_id = str(chapter["chapter_id"])
        status = str(chapter.get("chapter_status") or "").strip().lower()
        if status and status != "active":
            continue
        identity = build_chapter_instagram_identity(
            fraternity_name=chapter.get("fraternity_slug"),
            fraternity_slug=chapter.get("fraternity_slug"),
            school_name=chapter.get("university_name"),
            chapter_name=chapter.get("chapter_name"),
        )
        chapter_candidates = [score_instagram_candidate(candidate, identity) for candidate in bank.get_candidates_for_chapter(chapter_id)]
        current_url = chapter.get("instagram_url")
        audit = audit_existing_instagram_candidate(
            chapter_id=chapter_id,
            existing_url=current_url,
            identity=identity,
            candidates=chapter_candidates,
        )
        summary["candidates"] += len(chapter_candidates)
        if audit.outcome == "existing_value_confirmed":
            summary["existingConfirmed"] += 1
            continue
        if audit.outcome == "existing_value_replaced" and audit.accepted_candidate is not None:
            winners[chapter_id] = (audit.accepted_candidate, chapter)
            continue
        if audit.outcome == "review_required":
            conflicts.add(chapter_id)
            continue
        best = select_best_instagram_candidate(chapter_candidates)
        if best is not None:
            if current_url and current_url != best.profile_url and best.confidence < 0.98:
                conflicts.add(chapter_id)
                continue
            winners[chapter_id] = (best, chapter)

    handle_claims: dict[str, list[str]] = {}
    for chapter_id, (candidate, _) in winners.items():
        handle_claims.setdefault(candidate.handle.lower(), []).append(chapter_id)
    for chapter_id, (candidate, chapter) in list(winners.items()):
        duplicated = handle_claims.get(candidate.handle.lower(), [])
        if len(duplicated) > 1:
            for duplicate_id in duplicated:
                conflicts.add(duplicate_id)
                winners.pop(duplicate_id, None)

    for chapter in chapters:
        chapter_id = str(chapter["chapter_id"])
        if chapter_id in conflicts:
            summary["review"] += 1
            repository.create_review_item(
                None,
                crawl_run_id,
                ReviewItemCandidate(
                    item_type="instagram_candidate",
                    reason="review_conflicting_instagram_candidates",
                    source_slug=source_slug,
                    chapter_slug=str(chapter.get("chapter_slug") or ""),
                    payload={"requestId": request_id, "reasonCode": "review_conflicting_instagram_candidates"},
                ),
                chapter_id=chapter_id,
            )

    for chapter_id, (candidate, chapter) in winners.items():
        current_url = str(chapter.get("instagram_url") or "").strip() or None
        allow_replace = bool(current_url and current_url != candidate.profile_url and candidate.confidence >= 0.98)
        applied = repository.apply_instagram_resolution(
            chapter_id=chapter_id,
            chapter_slug=str(chapter.get("chapter_slug") or ""),
            fraternity_slug=str(chapter.get("fraternity_slug") or ""),
            source_slug=source_slug,
            crawl_run_id=crawl_run_id,
            request_id=request_id,
            instagram_url=candidate.profile_url,
            confidence=candidate.confidence,
            source_url=candidate.source_url,
            source_snippet=candidate.source_snippet,
            reason_code="resolved_by_global_instagram_sweep" if not allow_replace else "existing_instagram_replaced_by_higher_confidence_candidate",
            page_scope=candidate.page_scope,
            contact_specificity=candidate.contact_specificity,
            source_type=str(candidate.source_type),
            decision_stage="global_instagram_sweep",
            allow_replace=allow_replace,
            previous_url=current_url,
        )
        if not applied:
            summary["review"] += 1
            repository.create_review_item(
                None,
                crawl_run_id,
                ReviewItemCandidate(
                    item_type="instagram_candidate",
                    reason="review_conflicting_instagram_candidates",
                    source_slug=source_slug,
                    chapter_slug=str(chapter.get("chapter_slug") or ""),
                    payload={"requestId": request_id, "reasonCode": "review_conflicting_instagram_candidates"},
                ),
                chapter_id=chapter_id,
            )
            continue
        summary["resolved"] += 1
        if allow_replace:
            summary["existingReplaced"] += 1
        summary["jobsCanceled"] += repository.complete_pending_field_jobs_for_chapter(
            chapter_id=chapter_id,
            reason_code="resolved_by_global_instagram_sweep",
            status="resolved_by_global_instagram_sweep",
            chapter_updates={"instagram_url": candidate.profile_url},
            field_states={"instagram_url": "found"},
            field_names=["find_instagram"],
        )
    return summary
