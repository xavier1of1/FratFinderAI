from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class InstagramSourceType(StrEnum):
    EXISTING_DB_VALUE = "existing_db_value"
    PROVENANCE_SUPPORTING_PAGE = "provenance_supporting_page"
    AUTHORITATIVE_BUNDLE = "authoritative_bundle"
    NATIONALS_CHAPTER_ENTRY = "nationals_chapter_entry"
    NATIONALS_CHAPTER_PAGE = "nationals_chapter_page"
    NATIONALS_DIRECTORY_ROW = "nationals_directory_row"
    OFFICIAL_SCHOOL_CHAPTER_PAGE = "official_school_chapter_page"
    OFFICIAL_SCHOOL_DIRECTORY_ROW = "official_school_directory_row"
    VERIFIED_CHAPTER_WEBSITE = "verified_chapter_website"
    CHAPTER_WEBSITE_STRUCTURED_DATA = "chapter_website_structured_data"
    CHAPTER_WEBSITE_SOCIAL_LINK = "chapter_website_social_link"
    SEARCH_RESULT_PROFILE = "search_result_profile"
    GENERATED_HANDLE_SEARCH = "generated_handle_search"
    NATIONAL_FOLLOWING_SEED = "national_following_seed"
    REVIEW_OVERRIDE = "review_override"


@dataclass(slots=True)
class ChapterInstagramIdentity:
    fraternity_full_names: list[str] = field(default_factory=list)
    fraternity_aliases: list[str] = field(default_factory=list)
    fraternity_nicknames: list[str] = field(default_factory=list)
    fraternity_greek_letters: list[str] = field(default_factory=list)
    fraternity_initials: list[str] = field(default_factory=list)
    fraternity_compact_tokens: list[str] = field(default_factory=list)
    school_full_names: list[str] = field(default_factory=list)
    school_aliases: list[str] = field(default_factory=list)
    school_initials: list[str] = field(default_factory=list)
    school_compact_tokens: list[str] = field(default_factory=list)
    school_city_tokens: list[str] = field(default_factory=list)
    school_state_tokens: list[str] = field(default_factory=list)
    chapter_names: list[str] = field(default_factory=list)
    chapter_greek_letters: list[str] = field(default_factory=list)
    chapter_compact_tokens: list[str] = field(default_factory=list)
    negative_generic_terms: list[str] = field(default_factory=list)


@dataclass(slots=True)
class InstagramCandidate:
    handle: str
    profile_url: str
    source_type: InstagramSourceType
    source_url: str | None = None
    evidence_url: str | None = None
    page_scope: str | None = None
    contact_specificity: str | None = None
    source_title: str | None = None
    source_snippet: str | None = None
    surrounding_text: str | None = None
    local_container_text: str | None = None
    local_container_kind: str | None = None
    matched_fraternity_aliases: list[str] = field(default_factory=list)
    matched_school_aliases: list[str] = field(default_factory=list)
    matched_school_initials: list[str] = field(default_factory=list)
    matched_chapter_aliases: list[str] = field(default_factory=list)
    matched_handle_patterns: list[str] = field(default_factory=list)
    fraternity_identity_score: float = 0.0
    school_identity_score: float = 0.0
    chapter_identity_score: float = 0.0
    handle_pattern_score: float = 0.0
    textual_binding_score: float = 0.0
    source_trust_score: float = 0.0
    locality_score: float = 0.0
    duplicate_safety_score: float = 0.0
    is_profile_url: bool = True
    is_post_url: bool = False
    is_reel_url: bool = False
    is_story_url: bool = False
    is_location_url: bool = False
    is_explore_url: bool = False
    is_national_hq_account: bool = False
    is_school_fsl_or_ifc_account: bool = False
    is_alumni_account: bool = False
    is_generic_fraternity_account: bool = False
    is_generic_school_account: bool = False
    already_assigned_to_other_chapter_ids: list[str] = field(default_factory=list)
    score: float = 0.0
    confidence: float = 0.0
    reject_reasons: list[str] = field(default_factory=list)
    accept_reasons: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InstagramResolutionDecision:
    chapter_id: str
    field_name: str = "instagram_url"
    outcome: str = "terminal_no_candidate"
    selected_url: str | None = None
    selected_handle: str | None = None
    previous_url: str | None = None
    confidence: float = 0.0
    reason_code: str = "terminal_no_chapter_instagram_candidate"
    evidence_url: str | None = None
    source_type: InstagramSourceType | None = None
    page_scope: str | None = None
    contact_specificity: str | None = None
    accepted_candidate: InstagramCandidate | None = None
    rejected_candidates: list[InstagramCandidate] = field(default_factory=list)
    decision_trace: dict[str, Any] = field(default_factory=dict)
