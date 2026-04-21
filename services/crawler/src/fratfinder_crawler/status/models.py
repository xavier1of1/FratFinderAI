from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class ChapterStatusFinal(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    UNKNOWN = "unknown"
    REVIEW = "review"


class SchoolRecognitionStatus(StrEnum):
    RECOGNIZED = "recognized"
    PROBATIONARY_RECOGNITION = "probationary_recognition"
    ACTIVE_UNDER_CONDUCT_SANCTION = "active_under_conduct_sanction"
    SEEKING_RECOGNITION = "seeking_recognition"
    INTERIM_SUSPENSION = "interim_suspension"
    SUSPENDED = "suspended"
    CLOSED = "closed"
    DISMISSED = "dismissed"
    EXPELLED = "expelled"
    UNRECOGNIZED = "unrecognized"
    BANNED_NO_GREEK_LIFE = "banned_no_greek_life"
    NOT_FOUND_ON_CONCLUSIVE_ROSTER = "not_found_on_conclusive_roster"
    UNKNOWN = "unknown"


class NationalStatusValue(StrEnum):
    ACTIVE = "active"
    ASSOCIATE = "associate"
    COLONY = "colony"
    INACTIVE = "inactive"
    DORMANT = "dormant"
    CLOSED = "closed"
    NOT_LISTED_ON_ACTIVE_ONLY_DIRECTORY = "not_listed_on_active_only_directory"
    NOT_LISTED_ON_ALL_STATUS_DIRECTORY = "not_listed_on_all_status_directory"
    UNKNOWN = "unknown"


class CampusSourceType(StrEnum):
    RECOGNIZED_ROSTER = "recognized_roster"
    CHAPTER_STATUS_PAGE = "chapter_status_page"
    SUSPENDED_UNRECOGNIZED_PAGE = "suspended_unrecognized_page"
    CONDUCT_SCORECARD = "conduct_scorecard"
    HAZING_TRANSPARENCY_REPORT = "hazing_transparency_report"
    RSO_DIRECTORY = "rso_directory"
    NO_GREEK_POLICY = "no_greek_policy"
    OFFICIAL_STATEMENT_LOSS_OF_RECOGNITION = "official_statement_loss_of_recognition"
    ARTICLE_OR_NEWS = "article_or_news"
    HISTORICAL_ARCHIVE = "historical_archive"
    DYNAMIC_SHELL = "dynamic_shell"
    UNKNOWN = "unknown"


class StatusZoneType(StrEnum):
    ACTIVE = "active"
    RECOGNIZED = "recognized"
    PROBATIONARY_RECOGNITION = "probationary_recognition"
    SEEKING_RECOGNITION = "seeking_recognition"
    CONDUCT_GOOD = "conduct_good"
    CONDUCT_PROBATION = "conduct_probation"
    DEFERRED_SUSPENSION = "deferred_suspension"
    INTERIM_SUSPENSION = "interim_suspension"
    SUSPENDED = "suspended"
    CLOSED = "closed"
    DISMISSED = "dismissed"
    EXPELLED = "expelled"
    UNRECOGNIZED = "unrecognized"
    NO_GREEK_POLICY = "no_greek_policy"
    HISTORICAL = "historical"
    NAVIGATION = "navigation"
    FOOTER = "footer"
    UNKNOWN = "unknown"


class NationalDirectoryCapability(StrEnum):
    ACTIVE_ONLY = "active_only_directory"
    ALL_STATUS = "all_status_directory"
    ACTIVE_DORMANT_SPLIT = "active_and_dormant_separate_pages"
    MAP_WITH_STATUS_FILTER = "map_locator_with_status_filter"
    HISTORY_ROLL = "history_roll_not_current_status"
    UNKNOWN = "unknown_capability"


POSITIVE_ZONE_TYPES = {
    StatusZoneType.ACTIVE,
    StatusZoneType.RECOGNIZED,
    StatusZoneType.PROBATIONARY_RECOGNITION,
    StatusZoneType.CONDUCT_GOOD,
    StatusZoneType.CONDUCT_PROBATION,
    StatusZoneType.DEFERRED_SUSPENSION,
}

NEGATIVE_ZONE_TYPES = {
    StatusZoneType.INTERIM_SUSPENSION,
    StatusZoneType.SUSPENDED,
    StatusZoneType.CLOSED,
    StatusZoneType.DISMISSED,
    StatusZoneType.EXPELLED,
    StatusZoneType.UNRECOGNIZED,
    StatusZoneType.NO_GREEK_POLICY,
}

ACTIVE_SCHOOL_RECOGNITION = {
    SchoolRecognitionStatus.RECOGNIZED,
    SchoolRecognitionStatus.PROBATIONARY_RECOGNITION,
    SchoolRecognitionStatus.ACTIVE_UNDER_CONDUCT_SANCTION,
}

INACTIVE_SCHOOL_RECOGNITION = {
    SchoolRecognitionStatus.INTERIM_SUSPENSION,
    SchoolRecognitionStatus.SUSPENDED,
    SchoolRecognitionStatus.CLOSED,
    SchoolRecognitionStatus.DISMISSED,
    SchoolRecognitionStatus.EXPELLED,
    SchoolRecognitionStatus.UNRECOGNIZED,
    SchoolRecognitionStatus.BANNED_NO_GREEK_LIFE,
    SchoolRecognitionStatus.NOT_FOUND_ON_CONCLUSIVE_ROSTER,
}


class CampusStatusSource(BaseModel):
    school_name: str
    source_url: str
    source_host: str
    source_type: CampusSourceType
    authority_tier: int = Field(ge=0, le=9)
    currentness_score: float = Field(ge=0.0, le=1.0)
    completeness_score: float = Field(ge=0.0, le=1.0)
    parse_completeness_score: float = Field(default=0.0, ge=0.0, le=1.0)
    is_official_school_source: bool = False
    last_fetched_at: str | None = None
    content_hash: str | None = None
    title: str = ""
    text: str = ""
    html: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class StatusZone(BaseModel):
    source_url: str
    zone_type: StatusZoneType
    heading: str | None = None
    dom_path: str | None = None
    text: str = ""
    links: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    parser_version: str = "campus_status_v1"
    metadata: dict[str, Any] = Field(default_factory=dict)


class StatusMatch(BaseModel):
    source_url: str
    zone_type: StatusZoneType
    matched_text: str
    matched_alias: str | None = None
    match_method: str = "exact"
    match_confidence: float = Field(ge=0.0, le=1.0)
    evidence_confidence: float = Field(ge=0.0, le=1.0)
    authority_tier: int = Field(ge=0, le=9)
    source_type: CampusSourceType = CampusSourceType.UNKNOWN
    currentness_score: float = Field(default=0.0, ge=0.0, le=1.0)
    completeness_score: float = Field(default=0.0, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChapterStatusEvidence(BaseModel):
    id: str | None = None
    chapter_id: str | None = None
    fraternity_name: str
    school_name: str
    source_url: str
    authority_tier: int
    evidence_type: str
    status_signal: str
    matched_text: str | None = None
    matched_alias: str | None = None
    zone_type: str | None = None
    match_confidence: float = Field(ge=0.0, le=1.0)
    evidence_confidence: float = Field(ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)
    observed_at: str | None = None


class NationalStatusEvidence(BaseModel):
    status: NationalStatusValue = NationalStatusValue.UNKNOWN
    capability: NationalDirectoryCapability = NationalDirectoryCapability.UNKNOWN
    evidence_url: str | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    reason_code: str | None = None
    evidence_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChapterStatusDecision(BaseModel):
    id: str | None = None
    chapter_id: str | None = None
    final_status: ChapterStatusFinal
    school_recognition_status: SchoolRecognitionStatus
    national_status: NationalStatusValue = NationalStatusValue.UNKNOWN
    reason_code: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_ids: list[str] = Field(default_factory=list)
    decision_trace: dict[str, Any] = Field(default_factory=dict)
    conflict_flags: list[str] = Field(default_factory=list)
    review_required: bool = False
    decided_at: str | None = None

    @model_validator(mode="after")
    def _validate_evidence_requirements(self) -> "ChapterStatusDecision":
        if self.final_status in {ChapterStatusFinal.ACTIVE, ChapterStatusFinal.INACTIVE} and not self.evidence_ids:
            raise ValueError("active/inactive decisions require evidence_ids")
        if not self.reason_code:
            raise ValueError("reason_code is required")
        return self


class CampusStatusIndex(BaseModel):
    school_name: str
    sources: list[CampusStatusSource] = Field(default_factory=list)
    zones: list[StatusZone] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def source_by_url(self) -> dict[str, CampusStatusSource]:
        return {source.source_url: source for source in self.sources}

    @property
    def parse_completeness_score(self) -> float:
        if not self.sources:
            return 0.0
        return round(max(source.parse_completeness_score for source in self.sources), 4)

    @property
    def currentness_score(self) -> float:
        if not self.sources:
            return 0.0
        return round(max(source.currentness_score for source in self.sources), 4)

    @property
    def no_greek_policy(self) -> CampusStatusSource | None:
        for source in self.sources:
            if source.source_type == CampusSourceType.NO_GREEK_POLICY:
                return source
        return None

    def zones_for_types(self, zone_types: set[StatusZoneType]) -> list[StatusZone]:
        return [zone for zone in self.zones if zone.zone_type in zone_types]


class CampusSourceExpectation(BaseModel):
    source_type: CampusSourceType
    is_official_school_source: bool = True
    expected_zones: list[dict[str, Any]] = Field(default_factory=list)


class LegacyStatusMappingResult(BaseModel):
    final_status: ChapterStatusFinal
    school_recognition_status: SchoolRecognitionStatus
    reason_code: str
    evidence_ids: list[str] = Field(default_factory=list)
    decision_trace: dict[str, Any] = Field(default_factory=dict)


class ZoneClassificationResult(BaseModel):
    zone_type: StatusZoneType
    confidence: float = Field(ge=0.0, le=1.0)
    reason_code: str
    metadata: dict[str, Any] = Field(default_factory=dict)

