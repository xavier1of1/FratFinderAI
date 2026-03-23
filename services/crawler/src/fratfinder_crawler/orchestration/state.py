from __future__ import annotations

from typing import Any, TypedDict

from fratfinder_crawler.models import (
    CrawlMetrics,
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
