from __future__ import annotations

from pydantic import BaseModel, Field

from .models import CampusStatusIndex
from .source_classifier import classify_campus_source
from .zone_parser import parse_status_zones


class CampusSourceDocument(BaseModel):
    page_url: str
    title: str = ""
    text: str = ""
    html: str = ""
    fetched_at: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


def build_campus_status_index(*, school_name: str, documents: list[CampusSourceDocument]) -> CampusStatusIndex:
    sources = []
    zones = []
    for document in documents:
        source = classify_campus_source(
            school_name=school_name,
            page_url=document.page_url,
            title=document.title,
            text=document.text,
            html=document.html,
        )
        if document.fetched_at:
            source.last_fetched_at = document.fetched_at
        if document.metadata:
            source.metadata.update(document.metadata)
        parsed_zones = parse_status_zones(source)
        if parsed_zones:
            source.parse_completeness_score = max(source.parse_completeness_score, 0.88 if len(parsed_zones) > 1 else 0.65)
        sources.append(source)
        zones.extend(parsed_zones)
    return CampusStatusIndex(
        school_name=school_name,
        sources=sources,
        zones=zones,
        metadata={
            "sourceCount": len(sources),
            "zoneCount": len(zones),
        },
    )
