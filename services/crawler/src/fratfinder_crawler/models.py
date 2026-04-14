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

PAGE_SCOPE_CHAPTER_SITE = "chapter_site"
PAGE_SCOPE_SCHOOL_AFFILIATION = "school_affiliation_page"
PAGE_SCOPE_NATIONALS_CHAPTER = "nationals_chapter_page"
PAGE_SCOPE_NATIONALS_GENERIC = "nationals_generic"
PAGE_SCOPE_DIRECTORY = "directory_page"
PAGE_SCOPE_UNRELATED = "unrelated"

PAGE_SCOPE_VALUES = (
    PAGE_SCOPE_CHAPTER_SITE,
    PAGE_SCOPE_SCHOOL_AFFILIATION,
    PAGE_SCOPE_NATIONALS_CHAPTER,
    PAGE_SCOPE_NATIONALS_GENERIC,
    PAGE_SCOPE_DIRECTORY,
    PAGE_SCOPE_UNRELATED,
)

CONTACT_SPECIFICITY_CHAPTER = "chapter_specific"
CONTACT_SPECIFICITY_SCHOOL = "school_specific"
CONTACT_SPECIFICITY_NATIONAL_CHAPTER = "national_specific_to_chapter"
CONTACT_SPECIFICITY_NATIONAL_GENERIC = "national_generic"
CONTACT_SPECIFICITY_AMBIGUOUS = "ambiguous"

CONTACT_SPECIFICITY_VALUES = (
    CONTACT_SPECIFICITY_CHAPTER,
    CONTACT_SPECIFICITY_SCHOOL,
    CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
    CONTACT_SPECIFICITY_NATIONAL_GENERIC,
    CONTACT_SPECIFICITY_AMBIGUOUS,
)

CHAPTER_STATUS_ACTIVE = "active"
CHAPTER_STATUS_INACTIVE = "inactive"
CHAPTER_STATUS_UNKNOWN = "unknown"

CHAPTER_STATUS_VALUES = (
    CHAPTER_STATUS_ACTIVE,
    CHAPTER_STATUS_INACTIVE,
    CHAPTER_STATUS_UNKNOWN,
)

FIELD_RESOLUTION_MISSING = "missing"
FIELD_RESOLUTION_RESOLVED = "resolved"
FIELD_RESOLUTION_INACTIVE = "inactive"
FIELD_RESOLUTION_CONFIRMED_ABSENT = "confirmed_absent"
FIELD_RESOLUTION_DEFERRED = "deferred"

FIELD_RESOLUTION_STATE_VALUES = (
    FIELD_RESOLUTION_MISSING,
    FIELD_RESOLUTION_RESOLVED,
    FIELD_RESOLUTION_INACTIVE,
    FIELD_RESOLUTION_CONFIRMED_ABSENT,
    FIELD_RESOLUTION_DEFERRED,
)

DECISION_OUTCOME_ACCEPTED = "accepted"
DECISION_OUTCOME_REJECTED = "rejected"
DECISION_OUTCOME_DEFERRED = "deferred"
DECISION_OUTCOME_REVIEW_REQUIRED = "review_required"

DECISION_OUTCOME_VALUES = (
    DECISION_OUTCOME_ACCEPTED,
    DECISION_OUTCOME_REJECTED,
    DECISION_OUTCOME_DEFERRED,
    DECISION_OUTCOME_REVIEW_REQUIRED,
)


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
class VerifiedSourceRecord:
    fraternity_slug: str
    fraternity_name: str
    national_url: str
    origin: str
    confidence: float
    http_status: int | None
    checked_at: str | None = None
    is_active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NationalProfileRecord:
    fraternity_slug: str
    fraternity_name: str
    national_url: str
    national_url_confidence: float = 0.0
    national_url_provenance_type: str | None = None
    national_url_reason_code: str | None = None
    contact_email: str | None = None
    contact_email_confidence: float = 0.0
    contact_email_provenance_type: str | None = None
    contact_email_reason_code: str | None = None
    instagram_url: str | None = None
    instagram_confidence: float = 0.0
    instagram_provenance_type: str | None = None
    instagram_reason_code: str | None = None
    phone: str | None = None
    phone_confidence: float = 0.0
    phone_provenance_type: str | None = None
    phone_reason_code: str | None = None
    address_text: str | None = None
    address_confidence: float = 0.0
    address_provenance_type: str | None = None
    address_reason_code: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class ExistingSourceCandidate:
    source_slug: str
    list_url: str
    base_url: str
    source_type: str
    parser_key: str
    active: bool
    last_run_status: str | None
    last_success_at: str | None
    confidence: float


@dataclass(slots=True)
class ChapterStub:
    chapter_name: str
    university_name: str | None
    detail_url: str | None
    outbound_chapter_url_candidate: str | None
    confidence: float
    provenance: str


@dataclass(slots=True)
class ChapterTarget:
    url: str | None
    target_type: str
    source_class: str
    follow_allowed: bool
    rejection_reason: str | None = None
    host: str | None = None


@dataclass(slots=True)
class ChapterIdentity:
    chapter_name: str
    university_name: str | None
    source_class: str
    chapter_intent_signals: int
    identity_complete: bool


@dataclass(slots=True)
class ChapterValidityDecision:
    chapter_name: str
    university_name: str | None
    source_class: str
    validity_class: str
    invalid_reason: str | None = None
    repair_reason: str | None = None
    target_type: str | None = None
    provenance: str | None = None
    source_url: str | None = None
    next_action: str | None = None
    semantic_signals: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ChapterCandidate:
    chapter_name: str
    university_name: str | None
    confidence: float
    provenance: str
    source_class: str
    identity: ChapterIdentity
    targets: list[ChapterTarget] = field(default_factory=list)
    validity_class: str = "repairable_candidate"
    invalid_reason: str | None = None
    repair_reason: str | None = None
    semantic_signals: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ChapterSearchDecision:
    chapter_name: str
    university_name: str | None
    source_class: str
    decision: str
    validity_class: str = "repairable_candidate"
    reason: str | None = None
    target_type: str | None = None
    provenance: str | None = None
    source_url: str | None = None
    invalid_reason: str | None = None
    repair_reason: str | None = None
    next_action: str | None = None


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
class DecisionEvidence:
    decision_stage: str
    evidence_url: str | None = None
    source_type: str | None = None
    page_scope: str | None = None
    contact_specificity: str | None = None
    confidence: float | None = None
    reason_code: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


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
class FrontierItem:
    id: str | None
    url: str
    canonical_url: str
    parent_url: str | None
    depth: int
    anchor_text: str | None
    discovered_from: str
    state: str = "queued"
    score_total: float = 0.0
    score_components: dict[str, float] = field(default_factory=dict)
    selected_count: int = 0


@dataclass(slots=True)
class PolicyDecision:
    action_type: str
    score: float
    score_components: dict[str, float] = field(default_factory=dict)
    predicted_reward: float = 0.0
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PageObservation:
    id: int | None
    crawl_session_id: str
    url: str
    template_signature: str
    http_status: int | None
    latency_ms: int
    page_analysis: dict[str, Any]
    classification: dict[str, Any]
    embedded_data: dict[str, Any]
    structural_template_signature: str | None = None
    candidate_actions: list[dict[str, Any]] = field(default_factory=list)
    selected_action: str | None = None
    selected_action_score: float | None = None
    selected_action_score_components: dict[str, float] = field(default_factory=dict)
    parent_observation_id: int | None = None
    path_depth: int = 0
    risk_score: float = 0.0
    guardrail_flags: list[str] = field(default_factory=list)
    context_bucket: str | None = None
    outcome: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EnrichmentObservation:
    id: int | None
    field_job_id: str | None
    chapter_id: str
    chapter_slug: str
    fraternity_slug: str | None
    source_slug: str | None
    field_name: str
    queue_state: str
    runtime_mode: str
    policy_version: str | None = None
    policy_mode: str = "shadow"
    recommended_action: str | None = None
    deterministic_action: str | None = None
    recommended_actions: list[dict[str, Any]] = field(default_factory=list)
    context_features: dict[str, Any] = field(default_factory=dict)
    provider_window_state: dict[str, Any] = field(default_factory=dict)
    outcome: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RewardEvent:
    action_type: str
    reward_value: float
    reward_components: dict[str, float] = field(default_factory=dict)
    delayed: bool = False
    reward_stage: str = "immediate"
    attributed_observation_id: int | None = None
    discount_factor: float = 1.0


@dataclass(slots=True)
class TemplateProfile:
    template_signature: str
    host_family: str
    page_role_guess: str | None = None
    best_action_family: str | None = None
    best_extraction_family: str | None = None
    visit_count: int = 0
    chapter_yield: float = 0.0
    contact_yield: float = 0.0
    empty_rate: float = 0.0
    timeout_rate: float = 0.0
    updated_at: str | None = None


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
    source_hint_applied: str | None = None
    strategy_overrides: dict[str, Any] = field(default_factory=dict)


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
    chapter_status: str = "active"
    field_states: dict[str, str] = field(default_factory=dict)
    priority: int = 0
    queue_state: str = "actionable"
    validity_class: str | None = None
    repair_state: str | None = None
    blocked_reason: str | None = None
    terminal_outcome: str | None = None


@dataclass(slots=True)
class ChapterRepairJob:
    id: str
    chapter_id: str
    chapter_slug: str
    chapter_name: str
    source_slug: str | None
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    priority: int
    claim_token: str
    repair_state: str
    university_name: str | None = None
    website_url: str | None = None
    instagram_url: str | None = None
    contact_email: str | None = None


class UnsupportedSourceError(Exception):
    pass


class AmbiguousRecordError(Exception):
    pass

@dataclass(slots=True)
class EpochMetric:
    epoch: int
    policy_version: str
    runtime_mode: str
    train_sources: list[str]
    eval_sources: list[str]
    kpis: dict[str, float]
    deltas: dict[str, float]
    slopes: dict[str, float]
    cohort_label: str = "default"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FieldJobDecision:
    status: str
    confidence: float | None = None
    candidate_kind: str | None = None
    candidate_value: str | None = None
    reason_codes: list[str] = field(default_factory=list)
    write_allowed: bool = False
    requires_review: bool = False


@dataclass(slots=True)
class FraternityCrawlRequestRecord:
    id: str
    fraternity_name: str
    fraternity_slug: str
    source_slug: str | None
    source_url: str | None
    source_confidence: float | None
    status: str
    stage: str
    scheduled_for: str
    started_at: str | None
    finished_at: str | None
    priority: int
    runtime_worker_id: str | None = None
    runtime_lease_expires_at: str | None = None
    runtime_last_heartbeat_at: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    progress: dict[str, Any] = field(default_factory=dict)
    last_error: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class RequestGraphRunRecord:
    id: int
    request_id: str
    worker_id: str
    runtime_mode: str
    status: str
    active_node: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None


@dataclass(slots=True)
class ChapterEvidenceRecord:
    chapter_slug: str
    field_name: str
    candidate_value: str | None
    confidence: float | None
    source_url: str | None = None
    source_snippet: str | None = None
    fraternity_slug: str | None = None
    source_slug: str | None = None
    request_id: str | None = None
    crawl_run_id: int | None = None
    provider: str | None = None
    query: str | None = None
    related_website_url: str | None = None
    chapter_id: str | None = None
    trust_tier: str = "medium"
    evidence_status: str = "observed"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ProvisionalChapterRecord:
    id: str
    fraternity_id: str
    slug: str
    name: str
    status: str
    source_id: str | None = None
    request_id: str | None = None
    promoted_chapter_id: str | None = None
    university_name: str | None = None
    city: str | None = None
    state: str | None = None
    country: str = "USA"
    website_url: str | None = None
    instagram_url: str | None = None
    contact_email: str | None = None
    promotion_reason: str | None = None
    evidence_payload: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class SchoolPolicyRecord:
    school_slug: str
    school_name: str
    greek_life_status: str
    confidence: float = 0.0
    evidence_url: str | None = None
    evidence_source_type: str | None = None
    reason_code: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    last_verified_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class ChapterActivityRecord:
    fraternity_slug: str
    school_slug: str
    school_name: str
    chapter_activity_status: str
    confidence: float = 0.0
    evidence_url: str | None = None
    evidence_source_type: str | None = None
    reason_code: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    last_verified_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class AccuracyRecoveryMetrics:
    complete_rows: int = 0
    chapter_specific_contact_rows: int = 0
    nationals_only_contact_rows: int = 0
    inactive_validated_rows: int = 0
    confirmed_absent_website_rows: int = 0
    active_rows_with_chapter_specific_email: int = 0
    active_rows_with_chapter_specific_instagram: int = 0
    active_rows_with_any_contact: int = 0
    total_chapters: int = 0
