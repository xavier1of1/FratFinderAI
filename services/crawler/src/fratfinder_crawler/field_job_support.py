from __future__ import annotations

import re
from typing import Any

from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.social import InstagramSourceType, candidate_from_chapter_evidence


_SEMANTICALLY_INCOMPLETE_SCHOOL_SLUGS = {
    "at-the-university",
    "the-university",
    "university",
    "college",
    "the-college",
    "state-university",
    "university-campus",
}
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


def _normalized_text(value: object) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _slugify(value: object) -> str:
    import re

    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"^-+|-+$", "", text)


def school_name_is_semantically_incomplete(value: object) -> bool:
    normalized = _normalized_text(value)
    if not normalized:
        return True
    slug = _slugify(normalized)
    if slug in _SEMANTICALLY_INCOMPLETE_SCHOOL_SLUGS:
        return True
    tokens = normalized.split()
    generic_tokens = {"at", "the", "of", "and", "state", "university", "college", "school", "institute", "campus"}
    if tokens and all(token in generic_tokens for token in tokens) and any(
        token in {"university", "college", "school", "institute", "campus"} for token in tokens
    ):
        return True
    if normalized.startswith("at the ") and len(tokens) <= 4:
        return True
    if tokens and tokens[-1] in {"university", "college", "school", "institute"}:
        leading_tokens = tokens[:-1]
        if leading_tokens and all(token in {"at", "the", "of", "and", "state"} for token in leading_tokens):
            return True
    return False


def safe_school_name_repair_candidates(value: object) -> list[str]:
    """Return conservative school-name variants that are safe for repair matching.

    This helper intentionally does not assert that a candidate is canonical. Callers
    must still validate the returned variants against chapter-identity rules or
    official evidence before updating queue state or canonical data.
    """
    text = " ".join(str(value or "").replace("\u00a0", " ").split())
    if not text:
        return []

    cleaned = re.sub(r"\s*\((?:active|inactive|recognized|suspended|closed)\)\s*$", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+-\s+(?:active|inactive|recognized|suspended|closed)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:provisional|associate|colony|chapter)\s*$", "", cleaned, flags=re.IGNORECASE).strip(" -")

    variants: list[str] = []
    at_match = re.match(r"^(?:at|@)\s+(?P<school>.+)$", cleaned, flags=re.IGNORECASE)
    if at_match:
        school = at_match.group("school").strip(" -")
        if school.lower().startswith("the university of "):
            school = "University of " + school[len("the university of ") :]
        elif school.lower().startswith("the "):
            school = school[4:]
        if school and not school_name_is_semantically_incomplete(school):
            variants.append(school)

    if not at_match and cleaned and not school_name_is_semantically_incomplete(cleaned):
        variants.append(cleaned)

    seen: set[str] = set()
    deduped: list[str] = []
    for candidate in variants:
        normalized = candidate.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    return deduped


def school_identity_requires_repair_before_match(job: FieldJob, *, next_claim_attempt: bool = False) -> bool:
    school_name = str(job.university_name or "").strip()
    school_slug = _slugify(school_name)
    school_state = str((job.field_states or {}).get("university_name") or "").strip().lower()
    attempts = int(job.attempts or 0) + (1 if next_claim_attempt else 0)
    if not school_slug:
        return True
    if school_name_is_semantically_incomplete(school_name):
        return True
    if school_state in {"missing", "invalid_entity", "confirmed_absent", "inactive"}:
        return True
    if school_state == "low_confidence" and attempts >= 2:
        candidate_school = _slugify(job.payload.get("candidateSchoolName"))
        if candidate_school and candidate_school == school_slug:
            return False
        return True
    return False


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
