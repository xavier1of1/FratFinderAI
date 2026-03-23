from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator

from fratfinder_crawler.llm.client import LLMClient
from fratfinder_crawler.models import ExtractedChapter, PageAnalysis

_EXTRACTION_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["records", "page_level_confidence", "extraction_notes"],
    "properties": {
        "records": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "chapter_name",
                    "school_name",
                    "city",
                    "state",
                    "address",
                    "website_url",
                    "instagram_url",
                    "email",
                    "source_confidence",
                ],
                "properties": {
                    "chapter_name": {"type": "string", "minLength": 1},
                    "school_name": {"type": ["string", "null"]},
                    "city": {"type": ["string", "null"]},
                    "state": {"type": ["string", "null"]},
                    "address": {"type": ["string", "null"]},
                    "website_url": {"type": ["string", "null"]},
                    "instagram_url": {"type": ["string", "null"]},
                    "email": {"type": ["string", "null"]},
                    "source_confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                },
            },
        },
        "page_level_confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "extraction_notes": {"type": "string"},
    },
}

_EXTRACTION_VALIDATOR = Draft202012Validator(_EXTRACTION_SCHEMA)


class ExtractionValidationError(ValueError):
    pass


@dataclass(slots=True)
class LLMExtractionResult:
    records: list[ExtractedChapter]
    page_level_confidence: float
    extraction_notes: str



def extract_records(
    page_analysis: PageAnalysis,
    source_url: str,
    llm_client: LLMClient | None = None,
) -> list[ExtractedChapter]:
    return extract_records_with_metadata(page_analysis, source_url, llm_client=llm_client).records



def extract_records_with_metadata(
    page_analysis: PageAnalysis,
    source_url: str,
    llm_client: LLMClient | None = None,
) -> LLMExtractionResult:
    client = llm_client or LLMClient()
    payload = client.create_json_completion(
        system_prompt=(
            "Extract fraternity chapter records from the provided page summary only. "
            "Return only chapters that are actually supported by the summary and never invent URLs, schools, or locations."
        ),
        user_prompt=_build_extractor_prompt(page_analysis),
        schema_name="chapter_extraction",
        schema=_EXTRACTION_SCHEMA,
    )
    _validate_payload(payload)
    records = [
        ExtractedChapter(
            name=record["chapter_name"].strip(),
            university_name=_clean_optional(record.get("school_name")),
            city=_clean_optional(record.get("city")),
            state=_clean_optional(record.get("state")),
            website_url=_clean_optional(record.get("website_url")),
            instagram_url=_clean_optional(record.get("instagram_url")),
            contact_email=_clean_optional(record.get("email")),
            source_url=source_url,
            source_snippet=page_analysis.text_sample[:400],
            source_confidence=float(record["source_confidence"]),
        )
        for record in payload["records"]
    ]
    return LLMExtractionResult(
        records=records,
        page_level_confidence=float(payload["page_level_confidence"]),
        extraction_notes=payload["extraction_notes"],
    )



def _build_extractor_prompt(page_analysis: PageAnalysis) -> str:
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
    errors = sorted(_EXTRACTION_VALIDATOR.iter_errors(payload), key=lambda error: list(error.path))
    if not errors:
        return
    joined = "; ".join(error.message for error in errors)
    raise ExtractionValidationError(f"LLM extraction response failed validation: {joined}")



def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
