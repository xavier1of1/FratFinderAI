from __future__ import annotations

import re
from urllib.parse import urlparse

from fratfinder_crawler.precision_tools import (
    _contains_strong_ban_phrase,
    _looks_historical_or_archival_context,
    _looks_like_official_chapter_list_page,
    _looks_like_school_article_context,
    _looks_like_tabbed_chapter_status_page,
    _official_school_source,
)

from .models import CampusSourceType, CampusStatusSource


def _normalize(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _org_density_score(text: str) -> float:
    normalized = _normalize(text)
    tokens = (
        "fraternity",
        "sorority",
        "chapter",
        "chapters",
        "recognized",
        "suspended",
        "closed",
        "unrecognized",
        "interfraternity",
        "ifc",
    )
    hits = sum(normalized.count(token) for token in tokens)
    if hits >= 18:
        return 0.98
    if hits >= 10:
        return 0.9
    if hits >= 5:
        return 0.78
    if hits >= 2:
        return 0.58
    return 0.28


def classify_campus_source(
    *,
    school_name: str,
    page_url: str,
    title: str = "",
    text: str = "",
    html: str = "",
) -> CampusStatusSource:
    parsed = urlparse(page_url)
    host = (parsed.netloc or "").lower()
    path = _normalize(parsed.path or "")
    combined = _normalize(" ".join(part for part in [title, text[:8000], page_url] if part))

    official = _official_school_source(host, school_name) or host.endswith(".edu")
    org_density = _org_density_score(text)
    dynamic_shell = (
        ("__next" in html.lower() or "root" in html.lower() or "app" in html.lower())
        and org_density < 0.45
        and any(marker in combined for marker in ("organization", "organizations", "engage", "highlanderlink", "campuslabs"))
    )
    historical = _looks_historical_or_archival_context(combined, page_url=page_url)
    article = _looks_like_school_article_context(combined, page_url=page_url)

    source_type = CampusSourceType.UNKNOWN
    authority_tier = 6
    currentness = 0.7
    completeness = org_density

    if _contains_strong_ban_phrase(combined) or any(
        marker in combined
        for marker in (
            "may neither join nor participate in fraternities",
            "students may neither join nor participate in fraternities",
            "no fraternities on campus",
        )
    ):
        source_type = CampusSourceType.NO_GREEK_POLICY
        authority_tier = 0
        currentness = 0.98
        completeness = 0.98
    elif "lost university recognition" in combined or "permanently lost university recognition" in combined:
        source_type = CampusSourceType.OFFICIAL_STATEMENT_LOSS_OF_RECOGNITION
        authority_tier = 2
        currentness = 0.96
        completeness = max(completeness, 0.8)
    elif historical:
        source_type = CampusSourceType.HISTORICAL_ARCHIVE
        authority_tier = 7
        currentness = 0.25
        completeness = min(completeness, 0.45)
    elif dynamic_shell:
        source_type = CampusSourceType.DYNAMIC_SHELL
        authority_tier = 5
        currentness = 0.75
        completeness = min(completeness, 0.3)
    elif any(marker in combined for marker in ("unrecognized organizations", "unrecognized groups", "suspended and unrecognized", "dismissed organizations")):
        source_type = CampusSourceType.SUSPENDED_UNRECOGNIZED_PAGE
        authority_tier = 2
        currentness = 0.95
        completeness = max(completeness, 0.78)
    elif any(marker in combined for marker in ("hazing transparency", "campus hazing")):
        source_type = CampusSourceType.HAZING_TRANSPARENCY_REPORT
        authority_tier = 4
        currentness = 0.9
    elif any(marker in combined for marker in ("conduct status", "good conduct standing", "probation", "interim suspension", "deferred suspension")):
        source_type = CampusSourceType.CONDUCT_SCORECARD
        authority_tier = 4
        currentness = 0.9
    elif _looks_like_tabbed_chapter_status_page(combined) or any(marker in combined for marker in ("community scorecard", "chapter status", "suspended chapters", "closed chapters")):
        source_type = CampusSourceType.CHAPTER_STATUS_PAGE
        authority_tier = 1
        currentness = 0.95
        completeness = max(completeness, 0.9)
    elif any(marker in combined for marker in ("recognized chapters", "chapters below are recognized", "rights and privileges of recognized fraternities")):
        source_type = CampusSourceType.RECOGNIZED_ROSTER
        authority_tier = 1
        currentness = 0.96
        completeness = max(completeness, 0.92)
    elif any(marker in combined for marker in ("student organizations", "student organization", "organization profile", "organization directory", "highlanderlink", "engage")):
        source_type = CampusSourceType.RSO_DIRECTORY
        authority_tier = 3
        currentness = 0.82
        completeness = max(completeness, 0.72)
    elif article:
        source_type = CampusSourceType.ARTICLE_OR_NEWS
        authority_tier = 5 if official else 7
        currentness = 0.62 if official else 0.4
        completeness = min(completeness, 0.55)
    elif _looks_like_official_chapter_list_page(combined):
        source_type = CampusSourceType.RECOGNIZED_ROSTER if official else CampusSourceType.UNKNOWN
        authority_tier = 1 if official else 6
        currentness = 0.88 if official else 0.45
        completeness = max(completeness, 0.8 if official else 0.4)

    if not official and source_type not in {
        CampusSourceType.HISTORICAL_ARCHIVE,
        CampusSourceType.ARTICLE_OR_NEWS,
        CampusSourceType.DYNAMIC_SHELL,
        CampusSourceType.UNKNOWN,
    }:
        authority_tier = max(authority_tier, 5)
        currentness = min(currentness, 0.55)
        completeness = min(completeness, 0.65)

    return CampusStatusSource(
        school_name=school_name,
        source_url=page_url,
        source_host=host,
        source_type=source_type,
        authority_tier=authority_tier,
        currentness_score=round(max(0.0, min(currentness, 1.0)), 4),
        completeness_score=round(max(0.0, min(completeness, 1.0)), 4),
        parse_completeness_score=round(max(0.0, min(completeness if not dynamic_shell else 0.2, 1.0)), 4),
        is_official_school_source=official,
        title=title or "",
        text=text or "",
        html=html or "",
        metadata={
            "path": path,
            "dynamicShell": dynamic_shell,
            "historicalContext": historical,
            "articleContext": article,
            "organizationDensityScore": round(org_density, 4),
        },
    )
