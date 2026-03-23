from __future__ import annotations

from fratfinder_crawler.models import PageAnalysis, SourceClassification


def classify_source(page_analysis: PageAnalysis, llm_enabled: bool = False) -> SourceClassification:
    if page_analysis.has_json_ld or page_analysis.has_script_json:
        return SourceClassification(
            page_type="script_embedded_data",
            confidence=0.9 if page_analysis.has_json_ld else 0.8,
            recommended_strategy="script_json",
            needs_follow_links=False,
            possible_data_locations=["script[type='application/ld+json']", "inline_script_json"],
            classified_by="heuristic",
        )

    if page_analysis.has_map_widget:
        return SourceClassification(
            page_type="locator_map",
            confidence=0.8,
            recommended_strategy="locator_api",
            needs_follow_links=False,
            possible_data_locations=["map_widget", "api_hint"],
            classified_by="heuristic",
        )

    if page_analysis.table_count > 0 and page_analysis.probable_page_role == "directory":
        return SourceClassification(
            page_type="static_directory",
            confidence=0.9,
            recommended_strategy="table",
            needs_follow_links=False,
            possible_data_locations=["table"],
            classified_by="heuristic",
        )

    if page_analysis.repeated_block_count >= 1 and page_analysis.probable_page_role == "directory":
        return SourceClassification(
            page_type="static_directory",
            confidence=0.85 if page_analysis.repeated_block_count >= 2 else 0.7,
            recommended_strategy="repeated_block",
            needs_follow_links=False,
            possible_data_locations=["[data-chapter-card]", ".chapter-card", "li.chapter-item"],
            classified_by="heuristic",
        )

    if page_analysis.probable_page_role == "directory" and page_analysis.link_count >= 10:
        return SourceClassification(
            page_type="static_directory",
            confidence=0.65,
            recommended_strategy="repeated_block",
            needs_follow_links=True,
            possible_data_locations=["anchor_lists"],
            classified_by="heuristic",
        )

    if not llm_enabled:
        return SourceClassification(
            page_type="unsupported_or_unclear",
            confidence=0.0,
            recommended_strategy="review",
            needs_follow_links=False,
            possible_data_locations=[],
            classified_by="heuristic",
        )

    return SourceClassification(
        page_type="unsupported_or_unclear",
        confidence=0.25,
        recommended_strategy="llm",
        needs_follow_links=False,
        possible_data_locations=[],
        classified_by="heuristic",
    )
