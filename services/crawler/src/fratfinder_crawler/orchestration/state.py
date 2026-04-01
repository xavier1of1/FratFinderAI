from __future__ import annotations

from typing import Any, TypedDict

from fratfinder_crawler.models import (
    CrawlMetrics,
    ChapterStub,
    EmbeddedDataResult,
    ExtractedChapter,
    ExtractionPlan,
    FrontierItem,
    PageAnalysis,
    PolicyDecision,
    RewardEvent,
    ReviewItemCandidate,
    SourceClassification,
    SourceRecord,
)


class CrawlGraphState(TypedDict, total=False):
    source: SourceRecord
    run_id: int
    html: str
    page_analysis: PageAnalysis
    classification: SourceClassification
    embedded_data: EmbeddedDataResult
    extraction_plan: ExtractionPlan
    extracted: list[ExtractedChapter]
    normalized: list[dict[str, Any]]
    review_items: list[ReviewItemCandidate]
    metrics: CrawlMetrics
    error: str
    final_status: str
    strategy_attempts: int
    llm_calls_used: int
    page_level_confidence: float
    extraction_notes: str
    chapter_index_mode: str
    chapter_index_mode_confidence: float
    chapter_index_mode_reason: str
    chapter_stubs: list[ChapterStub]
    chapter_follow_pages: dict[str, list[tuple[str, str]]]
    chapter_contact_hints: dict[str, dict[str, str]]
    navigation_stats: dict[str, int]


class AdaptiveCrawlState(TypedDict, total=False):
    source: SourceRecord
    run_id: int
    crawl_session_id: str
    runtime_mode: str
    seed_urls: list[str]
    frontier_items: list[FrontierItem]
    visited_urls: list[str]
    current_frontier_item: FrontierItem
    current_page_html: str
    current_page_url: str
    current_page_status: int
    current_fetch_latency_ms: int
    page_analysis: PageAnalysis
    classification: SourceClassification
    embedded_data: EmbeddedDataResult
    template_signature: str
    candidate_actions: list[PolicyDecision]
    selected_action: str
    selected_action_score: float
    selected_action_score_components: dict[str, float]
    policy_mode: str
    policy_features: dict[str, Any]
    reward_events: list[RewardEvent]
    saturation_state: dict[str, Any]
    budget_state: dict[str, Any]
    extracted: list[ExtractedChapter]
    extracted_from_current: list[ExtractedChapter]
    normalized: list[dict[str, Any]]
    review_items: list[ReviewItemCandidate]
    metrics: CrawlMetrics
    error: str
    final_status: str
    stop_reason: str
    persisted_observation_id: int
    current_links: list[dict[str, Any]]
    navigation_stats: dict[str, int]
    page_level_confidence: float
    extraction_notes: str
