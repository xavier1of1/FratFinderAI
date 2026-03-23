from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator

from fratfinder_crawler.llm.client import LLMClient
from fratfinder_crawler.models import PageAnalysis, SourceClassification

_CLASSIFICATION_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "page_type",
        "confidence",
        "recommended_strategy",
        "needs_follow_links",
        "possible_data_locations",
    ],
    "properties": {
        "page_type": {"type": "string", "minLength": 1},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "recommended_strategy": {
            "type": "string",
            "enum": ["repeated_block", "table", "script_json", "locator_api", "llm", "review"],
        },
        "needs_follow_links": {"type": "boolean"},
        "possible_data_locations": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
}

_CLASSIFICATION_VALIDATOR = Draft202012Validator(_CLASSIFICATION_SCHEMA)



def classify_source_with_llm(
    page_analysis: PageAnalysis,
    llm_client: LLMClient | None = None,
) -> SourceClassification:
    client = llm_client or LLMClient()
    payload = client.create_json_completion(
        system_prompt=(
            "Classify a fraternity chapter website page using only the provided page summary. "
            "Return a conservative recommendation for the best extraction strategy."
        ),
        user_prompt=_build_classifier_prompt(page_analysis),
        schema_name="source_classification",
        schema=_CLASSIFICATION_SCHEMA,
    )
    _validate_payload(payload)
    return SourceClassification(
        page_type=payload["page_type"],
        confidence=float(payload["confidence"]),
        recommended_strategy=payload["recommended_strategy"],
        needs_follow_links=bool(payload["needs_follow_links"]),
        possible_data_locations=[str(item) for item in payload["possible_data_locations"]],
        classified_by="llm",
    )



def _build_classifier_prompt(page_analysis: PageAnalysis) -> str:
    return (
        f"Title: {page_analysis.title or 'n/a'}\n"
        f"Headings: {', '.join(page_analysis.headings) or 'n/a'}\n"
        f"Probable role: {page_analysis.probable_page_role}\n"
        f"Tables: {page_analysis.table_count}\n"
        f"Repeated blocks: {page_analysis.repeated_block_count}\n"
        f"Links: {page_analysis.link_count}\n"
        f"Has JSON-LD: {page_analysis.has_json_ld}\n"
        f"Has inline script JSON: {page_analysis.has_script_json}\n"
        f"Has map widget: {page_analysis.has_map_widget}\n"
        f"Has pagination: {page_analysis.has_pagination}\n"
        f"Visible text sample:\n{page_analysis.text_sample}"
    )



def _validate_payload(payload: dict[str, Any]) -> None:
    errors = sorted(_CLASSIFICATION_VALIDATOR.iter_errors(payload), key=lambda error: list(error.path))
    if not errors:
        return
    joined = "; ".join(error.message for error in errors)
    raise ValueError(f"LLM classification response failed validation: {joined}")
