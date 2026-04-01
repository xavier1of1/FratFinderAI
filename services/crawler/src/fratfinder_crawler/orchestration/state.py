from __future__ import annotations

from typing import Any, TypedDict

from fratfinder_crawler.models import (
    CrawlMetrics,
    ChapterStub,
    EmbeddedDataResult,
    ExtractedChapter,
    ExtractionPlan,
    PageAnalysis,
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
