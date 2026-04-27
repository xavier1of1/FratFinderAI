from __future__ import annotations

from typing import Any

from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.social import InstagramSourceType, candidate_from_chapter_evidence


_INSTAGRAM_STATUS_SUPPORT_SOURCE_TYPES = {
    InstagramSourceType.PROVENANCE_SUPPORTING_PAGE,
    InstagramSourceType.NATIONALS_CHAPTER_ENTRY,
    InstagramSourceType.NATIONALS_CHAPTER_PAGE,
    InstagramSourceType.NATIONALS_DIRECTORY_ROW,
    InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
    InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
    InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
    InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA,
    InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK,
}


def job_supporting_page_ready(job: FieldJob) -> bool:
    field_states = dict(job.field_states or {})
    website_state = str(field_states.get("website_url") or "").strip().lower()
    if job.website_url and website_state not in {"", "missing", "low_confidence"}:
        return True

    contact_resolution = job.payload.get("contactResolution") if isinstance(job.payload.get("contactResolution"), dict) else {}
    supporting_page_url = str(contact_resolution.get("supportingPageUrl") or "").strip()
    supporting_page_scope = str(contact_resolution.get("supportingPageScope") or contact_resolution.get("pageScope") or "").strip().lower()
    if supporting_page_url and supporting_page_scope in {
        "chapter_site",
        "school_affiliation_page",
        "nationals_chapter_page",
    }:
        return True

    if website_state == "confirmed_absent":
        if job.contact_email or job.instagram_url:
            return True
        if supporting_page_url and supporting_page_scope in {"school_affiliation_page", "nationals_chapter_page"}:
            return True
    return False


def job_has_existing_instagram_support(job: FieldJob, repository: Any) -> bool:
    if job_supporting_page_ready(job):
        return True

    reusable_evidence_getter = getattr(repository, "get_reusable_official_school_evidence_url", None)
    if callable(reusable_evidence_getter):
        try:
            reusable_url = reusable_evidence_getter(
                fraternity_slug=job.fraternity_slug,
                school_name=job.university_name,
            )
        except Exception:
            reusable_url = None
        if str(reusable_url or "").strip():
            return True

    fetch_candidates = getattr(repository, "fetch_instagram_candidates_for_chapters", None)
    if not callable(fetch_candidates):
        return False

    try:
        rows = fetch_candidates([job.chapter_id]) or []
    except Exception:
        return False

    for row in rows:
        candidate = candidate_from_chapter_evidence(row)
        if candidate is None:
            continue
        if candidate.source_type not in _INSTAGRAM_STATUS_SUPPORT_SOURCE_TYPES:
            continue
        if float(candidate.confidence or 0.0) >= 0.75:
            return True
    return False


def job_has_canonical_active_status(job: FieldJob) -> bool:
    chapter_status = str(getattr(job, "chapter_status", "") or "").strip().lower()
    if chapter_status != "active":
        return False

    payload = job.payload if isinstance(job.payload, dict) else {}
    queue_triage = payload.get("queueTriage") if isinstance(payload.get("queueTriage"), dict) else {}
    contact_resolution = payload.get("contactResolution") if isinstance(payload.get("contactResolution"), dict) else {}
    validity_class = str(
        contact_resolution.get("validityClass")
        or queue_triage.get("validityClass")
        or payload.get("validityClass")
        or ""
    ).strip().lower()
    return validity_class == "canonical_valid"
