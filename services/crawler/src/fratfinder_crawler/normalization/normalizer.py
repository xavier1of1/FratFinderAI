from __future__ import annotations

import re

from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.models import (
    AmbiguousRecordError,
    ExtractedChapter,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_VERIFY_WEBSITE,
    NormalizedChapter,
    ProvenanceRecord,
    SourceRecord,
)


LOW_CONFIDENCE_THRESHOLD = 0.85
MAX_CHAPTER_NAME_LENGTH = 160
MAX_UNIVERSITY_NAME_LENGTH = 180
MAX_CHAPTER_SLUG_LENGTH = 120

_BLOCKED_CHAPTER_NAMES_EXACT = {
    "find a chapter",
    "our chapters",
    "chapter roll",
    "the byx",
    "the byx at your university",
}

_BLOCKED_CHAPTER_SLUGS_EXACT = {
    "find-a-chapter",
    "our-chapters",
    "chapter-roll",
    "the-byx-at-your-university",
}

_BLOCKED_CHAPTER_SLUG_PREFIXES = (
    "visit-page-",
    "society-chapters-",
)


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return re.sub(r"^-+|-+$", "", value)


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_label(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().lower()


def _looks_like_navigation_or_placeholder(chapter_name: str, university_name: str | None, slug: str) -> bool:
    normalized_name = _normalize_label(chapter_name)
    normalized_school = _normalize_label(university_name)

    if normalized_name in _BLOCKED_CHAPTER_NAMES_EXACT:
        return True
    if slug in _BLOCKED_CHAPTER_SLUGS_EXACT:
        return True
    if any(slug.startswith(prefix) for prefix in _BLOCKED_CHAPTER_SLUG_PREFIXES):
        return True
    if normalized_name == "visit" and normalized_school.startswith("page "):
        return True
    if normalized_school == "at your university" and normalized_name in {"the byx", "the byx at your university"}:
        return True
    return False


def _field_state(value: str | None, source_confidence: float) -> str:
    if value is None:
        return "missing"
    return "found" if source_confidence >= LOW_CONFIDENCE_THRESHOLD else "low_confidence"


def _build_field_states(record: ExtractedChapter) -> dict[str, str]:
    return {
        "name": _field_state(_clean(record.name), record.source_confidence),
        "university_name": _field_state(_clean(record.university_name), record.source_confidence),
        "city": _field_state(_clean(record.city), record.source_confidence),
        "state": _field_state(_clean(record.state), record.source_confidence),
        "website_url": _field_state(_clean(record.website_url), record.source_confidence),
        "instagram_url": _field_state(_clean(record.instagram_url), record.source_confidence),
        "contact_email": _field_state(_clean(record.contact_email), record.source_confidence),
    }


def _build_field_jobs(field_states: dict[str, str]) -> list[str]:
    jobs: list[str] = []
    if field_states["website_url"] == "missing":
        jobs.append(FIELD_JOB_FIND_WEBSITE)
    elif field_states["website_url"] == "low_confidence":
        jobs.append(FIELD_JOB_VERIFY_WEBSITE)

    if field_states["instagram_url"] == "missing":
        jobs.append(FIELD_JOB_FIND_INSTAGRAM)

    if field_states["contact_email"] == "missing":
        jobs.append(FIELD_JOB_FIND_EMAIL)

    return jobs


def normalize_record(source: SourceRecord, record: ExtractedChapter) -> tuple[NormalizedChapter, list[ProvenanceRecord]]:
    chapter_name = record.name.strip()
    if not chapter_name:
        raise AmbiguousRecordError("Chapter record is missing a name")
    if len(chapter_name) > MAX_CHAPTER_NAME_LENGTH:
        raise AmbiguousRecordError("Chapter record name exceeded max supported length")
    cleaned_university = _clean(record.university_name)
    if cleaned_university and len(cleaned_university) > MAX_UNIVERSITY_NAME_LENGTH:
        raise AmbiguousRecordError("Chapter record university exceeded max supported length")

    slug_input = record.external_id or f"{record.name}-{record.university_name or ''}"
    slug = _slugify(slug_input)
    if not slug:
        raise AmbiguousRecordError("Unable to derive deterministic chapter slug")
    if len(slug) > MAX_CHAPTER_SLUG_LENGTH:
        raise AmbiguousRecordError("Chapter record slug exceeded max supported length")
    if _looks_like_navigation_or_placeholder(chapter_name, cleaned_university, slug):
        raise AmbiguousRecordError("Chapter record appears to be navigation or placeholder text")

    sanitized_website = sanitize_as_website(record.website_url, base_url=record.source_url)
    sanitized_email = sanitize_as_email(record.contact_email)
    sanitized_instagram = sanitize_as_instagram(record.instagram_url)

    # Route obvious kind mismatches before field-state/job planning.
    if sanitized_email is None:
        routed_from_website = sanitize_as_email(record.website_url)
        if routed_from_website:
            sanitized_email = routed_from_website
            sanitized_website = None
    if sanitized_instagram is None:
        routed_from_website_instagram = sanitize_as_instagram(record.website_url)
        if routed_from_website_instagram:
            sanitized_instagram = routed_from_website_instagram
            sanitized_website = None

    normalized_input = ExtractedChapter(
        name=record.name,
        university_name=record.university_name,
        city=record.city,
        state=record.state,
        website_url=sanitized_website,
        instagram_url=sanitized_instagram,
        contact_email=sanitized_email,
        external_id=record.external_id,
        source_url=record.source_url,
        source_snippet=record.source_snippet,
        source_confidence=record.source_confidence,
    )

    field_states = _build_field_states(normalized_input)

    normalized = NormalizedChapter(
        fraternity_slug=source.fraternity_slug,
        source_slug=source.source_slug,
        slug=slug,
        name=chapter_name,
        university_name=cleaned_university,
        city=_clean(normalized_input.city),
        state=_clean(normalized_input.state),
        website_url=_clean(normalized_input.website_url),
        instagram_url=_clean(normalized_input.instagram_url),
        contact_email=_clean(normalized_input.contact_email),
        external_id=_clean(record.external_id),
        missing_optional_fields=_build_field_jobs(field_states),
        field_states=field_states,
    )

    provenance: list[ProvenanceRecord] = []
    for field_name, field_value in [
        ("name", normalized.name),
        ("university_name", normalized.university_name),
        ("city", normalized.city),
        ("state", normalized.state),
        ("website_url", normalized.website_url),
        ("instagram_url", normalized.instagram_url),
        ("contact_email", normalized.contact_email),
        ("external_id", normalized.external_id),
    ]:
        if field_value is None:
            continue
        provenance.append(
            ProvenanceRecord(
                source_slug=source.source_slug,
                source_url=record.source_url,
                field_name=field_name,
                field_value=field_value,
                source_snippet=record.source_snippet,
                confidence=record.source_confidence,
            )
        )

    return normalized, provenance
