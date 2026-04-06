from __future__ import annotations

import re

from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.models import (
    AmbiguousRecordError,
    ChapterValidityDecision,
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

_VALID_MISSING_MARKERS = (
    "suspended",
    "disbanded",
    "inactive chapter",
    "charter revoked",
    "no longer active",
    "closed chapter",
    "chapter closed",
)

_LOW_SIGNAL_UNIVERSITY_MARKERS = (
    "chapter",
    "colony",
    "associate",
    "interest group",
    "provisional",
    "inactive",
    "suspended",
    "unknown",
)

_AWARD_OR_HONOR_MARKERS = (
    "award",
    "awards",
    "outstanding",
    "delegate",
    "ombudsman",
    "scholarship",
    "honor",
    "honour",
    "convention",
    "alumnus",
    "alumna",
    "alumni",
)

_DEMOGRAPHIC_OR_STATISTIC_MARKERS = (
    "asian",
    "white",
    "black",
    "hispanic",
    "latino",
    "latina",
    "unknown",
    "other",
    "multiracial",
)

_SCHOOL_DIVISION_MARKERS = (
    "school of",
    "college of",
    "department of",
    "division of",
    "office of",
    "school ",
    "college ",
    "department ",
    "faculty of",
)

_HISTORY_OR_TIMELINE_MARKERS = (
    "history",
    "timeline",
    "founded",
    "since ",
    "centennial",
    "anniversary",
)

_RANKING_OR_REPORT_MARKERS = (
    "best ",
    "top ",
    "report",
    "ranking",
    "rankings",
    "statistics",
    "statistic",
    "survey",
    "dashboard",
)

_WIKIPEDIA_STAT_IDENTITY_RE = re.compile(r"^\d{1,3}%?$", re.IGNORECASE)
_WIKIPEDIA_TIER_IDENTITY_RE = re.compile(r"^tier\s+\d+$", re.IGNORECASE)
_RANKISH_IDENTITY_RE = re.compile(r"^(?:#\d+|\d+\s*(?:\(tie\)|\[[^\]]+\])?)$", re.IGNORECASE)

_CHAPTER_ENTITY_MARKERS = (
    "chapter",
    "colony",
    "associate chapter",
)

_GREEK_TOKENS = {
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "iota",
    "kappa",
    "lambda",
    "mu",
    "nu",
    "xi",
    "omicron",
    "pi",
    "rho",
    "sigma",
    "tau",
    "upsilon",
    "phi",
    "chi",
    "psi",
    "omega",
}


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


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _looks_like_year_or_percentage(value: str) -> bool:
    compact = value.strip()
    if re.fullmatch(r"(17|18|19|20)\d{2}", compact):
        return True
    if re.fullmatch(r"\d{1,3}%", compact):
        return True
    return False


def _contains_year_range(value: str) -> bool:
    compact = value.strip()
    return bool(re.search(r"(17|18|19|20)\d{2}\s*[-–—]\s*((17|18|19|20)\d{2}|present)", compact))


def _looks_like_rankish_identity(value: str) -> bool:
    compact = " ".join(value.strip().split())
    return bool(_RANKISH_IDENTITY_RE.fullmatch(compact))


def _looks_like_wikipedia_academic_or_ranking_row(record: ExtractedChapter) -> bool:
    source_url = (_clean(getattr(record, "source_url", None)) or "").lower()
    if "wikipedia.org/wiki/" not in source_url:
        return False
    chapter_name = _normalize_label(getattr(record, "name", None))
    university_name = _normalize_label(getattr(record, "university_name", None))
    if not chapter_name or _has_greek_style_name(chapter_name) or _contains_any(chapter_name, _CHAPTER_ENTITY_MARKERS):
        return False
    if (
        _WIKIPEDIA_STAT_IDENTITY_RE.fullmatch(university_name)
        or _WIKIPEDIA_TIER_IDENTITY_RE.fullmatch(university_name)
        or _looks_like_rankish_identity(university_name)
    ):
        return True
    snippet = _normalize_label(getattr(record, "source_snippet", None))
    if snippet and source_url and ("wikipedia.org/wiki/university_" in source_url or "wikipedia.org/wiki/" in source_url):
        if _WIKIPEDIA_STAT_IDENTITY_RE.search(snippet) and not _chapter_entity_signal_count(getattr(record, "name", ""), getattr(record, "university_name", None)):
            return True
    if not _chapter_entity_signal_count(getattr(record, "name", ""), getattr(record, "university_name", None)) and _institution_signal_count(getattr(record, "university_name", None)) == 0:
        return True
    return False


def _has_greek_style_name(name: str) -> bool:
    tokens = [token for token in re.split(r"[^a-z]+", name) if token]
    return any(token in _GREEK_TOKENS for token in tokens)


def _looks_like_person_name(name: str) -> bool:
    raw_tokens = [token.strip(" .,'\"") for token in re.split(r"\s+", name) if token.strip(" .,'\"")]
    if len(raw_tokens) < 2 or len(raw_tokens) > 4:
        return False
    lowered = [token.lower() for token in raw_tokens]
    if any(token in _GREEK_TOKENS for token in lowered):
        return False
    if any(token in {"chapter", "colony", "at", "university", "college", "school", "campus"} for token in lowered):
        return False
    return all(re.fullmatch(r"[A-Z][A-Za-z.-]{1,30}", token) for token in raw_tokens)


def _chapter_entity_signal_count(chapter_name: str, university_name: str | None) -> int:
    normalized_name = _normalize_label(chapter_name)
    count = 0
    if normalized_name and _has_greek_style_name(normalized_name):
        count += 1
    if normalized_name and _contains_any(normalized_name, _CHAPTER_ENTITY_MARKERS):
        count += 1
    return count


def _institution_signal_count(university_name: str | None) -> int:
    raw_school = _clean(university_name)
    normalized_school = _normalize_label(university_name)
    if not normalized_school:
        return 0
    if (
        _looks_like_year_or_percentage(normalized_school)
        or _contains_year_range(normalized_school)
        or _looks_like_rankish_identity(normalized_school)
    ):
        return 0
    if raw_school and _looks_like_person_name(raw_school):
        return 0
    score = 0
    if any(token in normalized_school for token in ("university", "college", "campus", "institute", "state", "school", "tech", "polytechnic", "academy")):
        score += 2
    elif len(normalized_school.split()) >= 2 and not _contains_any(normalized_school, _SCHOOL_DIVISION_MARKERS):
        score += 1
    return score


def _semantic_invalid_reason(record: ExtractedChapter) -> str | None:
    chapter_name = _normalize_label(getattr(record, "name", None))
    university_name = _normalize_label(getattr(record, "university_name", None))
    snippet = _normalize_label(getattr(record, "source_snippet", None))
    if not chapter_name:
        return "identity_semantically_invalid"
    if _looks_like_year_or_percentage(chapter_name) or _looks_like_year_or_percentage(university_name):
        return "year_or_percentage_as_identity"
    if _looks_like_rankish_identity(chapter_name) or _looks_like_rankish_identity(university_name):
        return "ranking_or_report_row"
    if _contains_year_range(chapter_name) and not _has_greek_style_name(chapter_name):
        return "history_or_timeline_row"
    if _contains_year_range(university_name) and not _has_greek_style_name(chapter_name):
        return "history_or_timeline_row"
    if _looks_like_person_name(getattr(record, "name", "") or "") and (
        _contains_year_range(university_name)
        or _looks_like_year_or_percentage(university_name)
        or _looks_like_rankish_identity(university_name)
    ):
        return "history_or_timeline_row"
    if _looks_like_wikipedia_academic_or_ranking_row(record):
        return "ranking_or_report_row"
    if snippet and _contains_year_range(snippet) and _looks_like_person_name(getattr(record, "name", "") or ""):
        return "history_or_timeline_row"
    if chapter_name in _DEMOGRAPHIC_OR_STATISTIC_MARKERS or university_name in _DEMOGRAPHIC_OR_STATISTIC_MARKERS:
        return "demographic_or_statistic_row"
    if _contains_any(chapter_name, _AWARD_OR_HONOR_MARKERS):
        return "award_or_honor_row"
    if _contains_any(chapter_name, _SCHOOL_DIVISION_MARKERS):
        return "school_division_or_department"
    if _contains_any(chapter_name, _HISTORY_OR_TIMELINE_MARKERS):
        return "history_or_timeline_row"
    if _contains_any(chapter_name, _RANKING_OR_REPORT_MARKERS):
        return "ranking_or_report_row"
    if _looks_like_navigation_or_placeholder(record.name, record.university_name, _slugify(f"{record.name}-{record.university_name or ''}")):
        return "navigation_or_chrome"
    if snippet and _contains_any(snippet, _RANKING_OR_REPORT_MARKERS) and not _chapter_entity_signal_count(record.name, record.university_name):
        return "ranking_or_report_row"
    return None


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


def _has_conservative_valid_missing_evidence(record: ExtractedChapter) -> bool:
    if record.website_url or record.instagram_url or record.contact_email:
        return False
    text = " ".join(
        value
        for value in (
            _normalize_label(record.source_snippet),
            _normalize_label(record.name),
            _normalize_label(record.university_name),
        )
        if value
    )
    if not text:
        return False
    return any(marker in text for marker in _VALID_MISSING_MARKERS)


def _field_state(value: str | None, source_confidence: float, *, valid_missing: bool = False) -> str:
    if value is None:
        if valid_missing:
            return "valid_missing"
        return "missing"
    return "found" if source_confidence >= LOW_CONFIDENCE_THRESHOLD else "low_confidence"


def _build_field_states(record: ExtractedChapter) -> dict[str, str]:
    valid_missing = _has_conservative_valid_missing_evidence(record)
    return {
        "name": _field_state(_clean(record.name), record.source_confidence),
        "university_name": _field_state(_clean(record.university_name), record.source_confidence),
        "city": _field_state(_clean(record.city), record.source_confidence),
        "state": _field_state(_clean(record.state), record.source_confidence),
        "website_url": _field_state(_clean(record.website_url), record.source_confidence, valid_missing=valid_missing),
        "instagram_url": _field_state(_clean(record.instagram_url), record.source_confidence, valid_missing=valid_missing),
        "contact_email": _field_state(_clean(record.contact_email), record.source_confidence, valid_missing=valid_missing),
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


def classify_chapter_validity(
    record: ExtractedChapter,
    *,
    source_class: str = "national",
    provenance: str | None = None,
    target_type: str | None = None,
) -> ChapterValidityDecision:
    chapter_name = _clean(getattr(record, "name", None)) or ""
    university_name = _clean(getattr(record, "university_name", None))
    invalid_reason = _semantic_invalid_reason(record)
    chapter_signal_count = _chapter_entity_signal_count(chapter_name, university_name)
    institution_signal_count = _institution_signal_count(university_name)
    semantic_signals = {
        "chapterEntitySignals": chapter_signal_count,
        "institutionSignals": institution_signal_count,
        "hasContacts": bool(
            _clean(getattr(record, "website_url", None))
            or _clean(getattr(record, "instagram_url", None))
            or _clean(getattr(record, "contact_email", None))
        ),
        "sourceConfidence": float(getattr(record, "source_confidence", 0.0) or 0.0),
    }

    if invalid_reason:
        return ChapterValidityDecision(
            chapter_name=chapter_name,
            university_name=university_name,
            source_class=source_class,
            validity_class="invalid_non_chapter",
            invalid_reason=invalid_reason,
            provenance=provenance,
            target_type=target_type,
            source_url=getattr(record, "source_url", None),
            next_action="quarantine_invalid_entities",
            semantic_signals=semantic_signals,
        )

    if source_class == "wider_web":
        if chapter_signal_count >= 1 and institution_signal_count >= 1:
            return ChapterValidityDecision(
                chapter_name=chapter_name,
                university_name=university_name,
                source_class=source_class,
                validity_class="provisional_candidate",
                repair_reason="broader_web_gated",
                provenance=provenance,
                target_type=target_type,
                source_url=getattr(record, "source_url", None),
                next_action="store_provisional_only",
                semantic_signals=semantic_signals,
            )
        return ChapterValidityDecision(
            chapter_name=chapter_name,
            university_name=university_name,
            source_class=source_class,
            validity_class="invalid_non_chapter",
            invalid_reason="identity_semantically_incomplete",
            provenance=provenance,
            target_type=target_type,
            source_url=getattr(record, "source_url", None),
            next_action="quarantine_invalid_entities",
            semantic_signals=semantic_signals,
        )

    if chapter_signal_count >= 1 and institution_signal_count >= 1:
        return ChapterValidityDecision(
            chapter_name=chapter_name,
            university_name=university_name,
            source_class=source_class,
            validity_class="canonical_valid",
            provenance=provenance,
            target_type=target_type,
            source_url=getattr(record, "source_url", None),
            next_action="canonicalize_valid_chapters",
            semantic_signals=semantic_signals,
        )

    if chapter_signal_count >= 1:
        return ChapterValidityDecision(
            chapter_name=chapter_name,
            university_name=university_name,
            source_class=source_class,
            validity_class="repairable_candidate",
            repair_reason="identity_semantically_incomplete",
            provenance=provenance,
            target_type=target_type,
            source_url=getattr(record, "source_url", None),
            next_action="institutional_repair",
            semantic_signals=semantic_signals,
        )

    if source_class in {"national", "institutional"} and chapter_name:
        return ChapterValidityDecision(
            chapter_name=chapter_name,
            university_name=university_name,
            source_class=source_class,
            validity_class="repairable_candidate",
            repair_reason="identity_semantically_incomplete",
            provenance=provenance,
            target_type=target_type,
            source_url=getattr(record, "source_url", None),
            next_action="institutional_repair",
            semantic_signals=semantic_signals,
        )

    return ChapterValidityDecision(
        chapter_name=chapter_name,
        university_name=university_name,
        source_class=source_class,
        validity_class="invalid_non_chapter",
        invalid_reason="identity_semantically_invalid",
        provenance=provenance,
        target_type=target_type,
        source_url=getattr(record, "source_url", None),
        next_action="quarantine_invalid_entities",
        semantic_signals=semantic_signals,
    )


def _identity_supports_contact_queue(record: ExtractedChapter, *, validity_class: str | None = None) -> bool:
    if validity_class is not None:
        return validity_class == "canonical_valid"
    chapter_name = _normalize_label(record.name)
    university_name = _normalize_label(record.university_name)
    if any((_clean(record.website_url), _clean(record.instagram_url), _clean(record.contact_email))):
        return True
    if not chapter_name or not university_name:
        return False
    if any(marker in university_name for marker in _LOW_SIGNAL_UNIVERSITY_MARKERS):
        return False
    return record.source_confidence >= LOW_CONFIDENCE_THRESHOLD


def normalize_record(
    source: SourceRecord,
    record: ExtractedChapter,
    *,
    validity_class: str | None = None,
) -> tuple[NormalizedChapter, list[ProvenanceRecord]]:
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

    if validity_class is not None and validity_class != "canonical_valid":
        missing_optional_fields = []
    else:
        missing_optional_fields = _build_field_jobs(field_states) if _identity_supports_contact_queue(normalized_input, validity_class=validity_class) else [
            job for job in _build_field_jobs(field_states) if job == FIELD_JOB_VERIFY_WEBSITE
        ]

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
        missing_optional_fields=missing_optional_fields,
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
