from __future__ import annotations

from fratfinder_crawler.models import EmbeddedDataResult, ExtractionPlan, PageAnalysis, SourceClassification


def _apply_source_overrides(plan: ExtractionPlan, source_metadata: dict[str, object] | None) -> ExtractionPlan:
    hints = ((source_metadata or {}).get("extractionHints") or {})
    if not isinstance(hints, dict):
        return plan

    primary = hints.get("primaryStrategy")
    fallbacks = hints.get("fallbackStrategies")
    normalized_fallbacks = [value for value in fallbacks if isinstance(value, str) and value] if isinstance(fallbacks, list) else []

    if isinstance(primary, str) and primary:
        return ExtractionPlan(
            primary_strategy=primary,
            fallback_strategies=normalized_fallbacks or list(plan.fallback_strategies),
            max_attempts=plan.max_attempts,
            llm_allowed=plan.llm_allowed,
            source_hint_applied="primaryStrategy",
            strategy_overrides=dict(hints),
        )

    if normalized_fallbacks:
        return ExtractionPlan(
            primary_strategy=plan.primary_strategy,
            fallback_strategies=normalized_fallbacks,
            max_attempts=plan.max_attempts,
            llm_allowed=plan.llm_allowed,
            source_hint_applied="fallbackStrategies",
            strategy_overrides=dict(hints),
        )

    if hints:
        return ExtractionPlan(
            primary_strategy=plan.primary_strategy,
            fallback_strategies=list(plan.fallback_strategies),
            max_attempts=plan.max_attempts,
            llm_allowed=plan.llm_allowed,
            source_hint_applied="metadata_only",
            strategy_overrides=dict(hints),
        )

    return plan


def select_extraction_plan(
    page_analysis: PageAnalysis,
    classification: SourceClassification,
    embedded_data: EmbeddedDataResult,
    llm_enabled: bool = False,
    source_metadata: dict[str, object] | None = None,
) -> ExtractionPlan:
    if classification.page_type == "static_directory" and classification.confidence >= 0.75:
        strategy = "table" if page_analysis.table_count > 0 else "repeated_block"
        fallback_strategies = ["repeated_block" if strategy == "table" else "table"]
        if embedded_data.found:
            fallback_strategies.append("script_json")
        fallback_strategies.append("review")
        return _apply_source_overrides(
            ExtractionPlan(
                primary_strategy=strategy,
                fallback_strategies=fallback_strategies,
                max_attempts=2,
                llm_allowed=llm_enabled,
            ),
            source_metadata,
        )

    if embedded_data.found:
        if embedded_data.api_url:
            return _apply_source_overrides(
                ExtractionPlan(
                    primary_strategy="locator_api",
                    fallback_strategies=["script_json", "review"],
                    max_attempts=2,
                    llm_allowed=False,
                ),
                source_metadata,
            )
        return _apply_source_overrides(
            ExtractionPlan(
                primary_strategy="script_json",
                fallback_strategies=["review"],
                max_attempts=2,
                llm_allowed=False,
            ),
            source_metadata,
        )

    if classification.page_type == "static_directory":
        strategy = "table" if page_analysis.table_count > 0 else "repeated_block"
        return _apply_source_overrides(
            ExtractionPlan(
                primary_strategy=strategy,
                fallback_strategies=["repeated_block" if strategy == "table" else "table", "review"],
                max_attempts=2,
                llm_allowed=llm_enabled,
            ),
            source_metadata,
        )

    if classification.confidence >= 0.75:
        return _apply_source_overrides(
            ExtractionPlan(
                primary_strategy=classification.recommended_strategy,
                fallback_strategies=["review"],
                max_attempts=2,
                llm_allowed=llm_enabled,
            ),
            source_metadata,
        )

    if llm_enabled and classification.recommended_strategy == "llm":
        return _apply_source_overrides(
            ExtractionPlan(
                primary_strategy="llm",
                fallback_strategies=["review"],
                max_attempts=1,
                llm_allowed=True,
            ),
            source_metadata,
        )

    return _apply_source_overrides(
        ExtractionPlan(
            primary_strategy="review",
            fallback_strategies=[],
            max_attempts=1,
            llm_allowed=False,
        ),
        source_metadata,
    )
