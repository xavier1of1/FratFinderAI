from __future__ import annotations


def evaluate_stop_conditions(*, budget_state: dict[str, object], frontier_remaining: int, current_score: float | None = None) -> tuple[bool, str | None]:
    pages_processed = int(budget_state.get("pages_processed", 0))
    records_seen = int(budget_state.get("records_seen", 0))
    max_pages = int(budget_state.get("max_pages", 0))
    empty_streak = int(budget_state.get("empty_streak", 0))
    max_empty_streak = int(budget_state.get("max_empty_streak", 0))
    low_yield_streak = int(budget_state.get("low_yield_streak", 0))
    saturation_threshold = int(budget_state.get("saturation_threshold", 0))
    high_yield_record_threshold = int(budget_state.get("high_yield_record_threshold", 0))
    min_pages_for_high_yield_stop = int(budget_state.get("min_pages_for_high_yield_stop", 2))

    if max_pages and pages_processed >= max_pages:
        return True, "page_budget_exhausted"
    if frontier_remaining <= 0:
        return True, "frontier_empty"
    if max_empty_streak and empty_streak >= max_empty_streak:
        return True, "empty_streak_exhausted"
    if (
        high_yield_record_threshold
        and records_seen >= high_yield_record_threshold
        and pages_processed >= max(1, min_pages_for_high_yield_stop)
        and low_yield_streak >= 1
    ):
        return True, "high_yield_saturated"
    if saturation_threshold and low_yield_streak >= saturation_threshold:
        return True, "saturation_reached"
    if current_score is not None and current_score < float(budget_state.get("min_score", 0.0)):
        return True, "min_score_floor"
    return False, None
