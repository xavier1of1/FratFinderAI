from __future__ import annotations

from fratfinder_crawler.models import EmbeddedDataResult, ExtractionPlan, PageAnalysis, SourceClassification


def select_extraction_plan(
    page_analysis: PageAnalysis,
    classification: SourceClassification,
    embedded_data: EmbeddedDataResult,
    llm_enabled: bool = False,
) -> ExtractionPlan:
    if classification.page_type == "static_directory" and classification.confidence >= 0.75:
        strategy = "table" if page_analysis.table_count > 0 else "repeated_block"
        fallback_strategies = ["repeated_block" if strategy == "table" else "table"]
        if embedded_data.found:
            fallback_strategies.append("script_json")
        fallback_strategies.append("review")
        return ExtractionPlan(
            primary_strategy=strategy,
            fallback_strategies=fallback_strategies,
            max_attempts=2,
            llm_allowed=llm_enabled,
        )

    if embedded_data.found:
        if embedded_data.api_url:
            return ExtractionPlan(
                primary_strategy="locator_api",
                fallback_strategies=["script_json", "review"],
                max_attempts=2,
                llm_allowed=False,
            )
        return ExtractionPlan(
            primary_strategy="script_json",
            fallback_strategies=["review"],
            max_attempts=2,
            llm_allowed=False,
        )

    if classification.page_type == "static_directory":
        strategy = "table" if page_analysis.table_count > 0 else "repeated_block"
        return ExtractionPlan(
            primary_strategy=strategy,
            fallback_strategies=["repeated_block" if strategy == "table" else "table", "review"],
            max_attempts=2,
            llm_allowed=llm_enabled,
        )

    if classification.confidence >= 0.75:
        return ExtractionPlan(
            primary_strategy=classification.recommended_strategy,
            fallback_strategies=["review"],
            max_attempts=2,
            llm_allowed=llm_enabled,
        )

    if llm_enabled and classification.recommended_strategy == "llm":
        return ExtractionPlan(
            primary_strategy="llm",
            fallback_strategies=["review"],
            max_attempts=1,
            llm_allowed=True,
        )

    return ExtractionPlan(
        primary_strategy="review",
        fallback_strategies=[],
        max_attempts=1,
        llm_allowed=False,
    )
