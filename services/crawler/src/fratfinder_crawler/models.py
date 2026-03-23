from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

FIELD_JOB_FIND_WEBSITE = "find_website"
FIELD_JOB_FIND_INSTAGRAM = "find_instagram"
FIELD_JOB_FIND_EMAIL = "find_email"
FIELD_JOB_VERIFY_WEBSITE = "verify_website"
FIELD_JOB_VERIFY_SCHOOL = "verify_school_match"

FIELD_JOB_TYPES = (
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_VERIFY_WEBSITE,
    FIELD_JOB_VERIFY_SCHOOL,
)

FIELD_TO_CHAPTER_COLUMN = {
    FIELD_JOB_FIND_WEBSITE: "website_url",
    FIELD_JOB_FIND_INSTAGRAM: "instagram_url",
    FIELD_JOB_FIND_EMAIL: "contact_email",
    FIELD_JOB_VERIFY_WEBSITE: "website_url",
    FIELD_JOB_VERIFY_SCHOOL: "university_name",
}

FIELD_JOB_TO_STATE_KEY = {
    FIELD_JOB_FIND_WEBSITE: "website_url",
    FIELD_JOB_FIND_INSTAGRAM: "instagram_url",
    FIELD_JOB_FIND_EMAIL: "contact_email",
    FIELD_JOB_VERIFY_WEBSITE: "website_url",
    FIELD_JOB_VERIFY_SCHOOL: "university_name",
}


@dataclass(slots=True)
class SourceRecord:
    id: str
    fraternity_id: str
    fraternity_slug: str
    source_slug: str
    source_type: str
    parser_key: str
    base_url: str
    list_path: str | None
    metadata: dict[str, Any]

    @property
    def list_url(self) -> str:
        if self.list_path and self.list_path.startswith("http"):
            return self.list_path
        suffix = self.list_path or ""
        return f"{self.base_url.rstrip('/')}/{suffix.lstrip('/')}" if suffix else self.base_url


@dataclass(slots=True)
class ExtractedChapter:
    name: str
    university_name: str | None = None
    city: str | None = None
    state: str | None = None
    website_url: str | None = None
    instagram_url: str | None = None
    contact_email: str | None = None
    external_id: str | None = None
    source_url: str = ""
    source_snippet: str | None = None
    source_confidence: float = 1.0


@dataclass(slots=True)
class NormalizedChapter:
    fraternity_slug: str
    source_slug: str
    slug: str
    name: str
    university_name: str | None = None
    city: str | None = None
    state: str | None = None
    country: str = "USA"
    website_url: str | None = None
    instagram_url: str | None = None
    contact_email: str | None = None
    external_id: str | None = None
    chapter_status: str = "active"
    missing_optional_fields: list[str] = field(default_factory=list)
    field_states: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ProvenanceRecord:
    source_slug: str
    source_url: str
    field_name: str
    field_value: str | None
    source_snippet: str | None = None
    confidence: float = 1.0


@dataclass(slots=True)
class ReviewItemCandidate:
    item_type: str
    reason: str
    source_slug: str | None
    chapter_slug: str | None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CrawlMetrics:
    pages_processed: int = 0
    records_seen: int = 0
    records_upserted: int = 0
    review_items_created: int = 0
    field_jobs_created: int = 0


@dataclass(slots=True)
class PageAnalysis:
    title: str | None
    headings: list[str]
    table_count: int
    repeated_block_count: int
    link_count: int
    has_json_ld: bool
    has_script_json: bool
    has_map_widget: bool
    has_pagination: bool
    probable_page_role: str
    text_sample: str


@dataclass(slots=True)
class SourceClassification:
    page_type: str
    confidence: float
    recommended_strategy: str
    needs_follow_links: bool
    possible_data_locations: list[str]
    classified_by: str


@dataclass(slots=True)
class EmbeddedDataResult:
    found: bool
    data_type: str | None
    raw_data: list[dict[str, Any]] | None
    api_url: str | None


@dataclass(slots=True)
class ExtractionPlan:
    primary_strategy: str
    fallback_strategies: list[str]
    max_attempts: int = 2
    llm_allowed: bool = True


@dataclass(slots=True)
class FieldJob:
    id: str
    chapter_id: str
    chapter_slug: str
    chapter_name: str
    field_name: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    claim_token: str
    source_base_url: str | None
    website_url: str | None
    instagram_url: str | None
    contact_email: str | None
    fraternity_slug: str | None = None
    source_id: str | None = None
    source_slug: str | None = None
    university_name: str | None = None
    crawl_run_id: int | None = None
    field_states: dict[str, str] = field(default_factory=dict)


class UnsupportedSourceError(Exception):
    pass


class AmbiguousRecordError(Exception):
    pass
