from __future__ import annotations

import re

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


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return re.sub(r"^-+|-+$", "", value)



def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None



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
    if not record.name.strip():
        raise AmbiguousRecordError("Chapter record is missing a name")

    slug_input = record.external_id or f"{record.name}-{record.university_name or ''}"
    slug = _slugify(slug_input)
    if not slug:
        raise AmbiguousRecordError("Unable to derive deterministic chapter slug")

    field_states = _build_field_states(record)

    normalized = NormalizedChapter(
        fraternity_slug=source.fraternity_slug,
        source_slug=source.source_slug,
        slug=slug,
        name=record.name.strip(),
        university_name=_clean(record.university_name),
        city=_clean(record.city),
        state=_clean(record.state),
        website_url=_clean(record.website_url),
        instagram_url=_clean(record.instagram_url),
        contact_email=_clean(record.contact_email),
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
