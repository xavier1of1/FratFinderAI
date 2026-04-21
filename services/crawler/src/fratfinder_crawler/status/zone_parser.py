from __future__ import annotations

import re

from bs4 import BeautifulSoup

from fratfinder_crawler.precision_tools import _tabbed_roster_section_texts

from .models import CampusStatusSource, StatusZone, StatusZoneType

_NEGATIVE_ZONE_TOKENS = {
    "suspended chapters": StatusZoneType.SUSPENDED,
    "currently suspended": StatusZoneType.SUSPENDED,
    "closed chapters": StatusZoneType.CLOSED,
    "unrecognized organizations": StatusZoneType.UNRECOGNIZED,
    "unrecognized chapters": StatusZoneType.UNRECOGNIZED,
    "lost university recognition": StatusZoneType.UNRECOGNIZED,
    "permanently lost university recognition": StatusZoneType.UNRECOGNIZED,
    "no longer authorized to operate": StatusZoneType.UNRECOGNIZED,
    "dismissed organizations": StatusZoneType.DISMISSED,
    "expelled": StatusZoneType.EXPELLED,
    "interim suspension": StatusZoneType.INTERIM_SUSPENSION,
}

_POSITIVE_ZONE_TOKENS = {
    "recognized chapters": StatusZoneType.RECOGNIZED,
    "active chapters": StatusZoneType.ACTIVE,
    "current chapters": StatusZoneType.ACTIVE,
    "interfraternity council": StatusZoneType.RECOGNIZED,
    "chapter scorecards": StatusZoneType.ACTIVE,
    "probationary recognition": StatusZoneType.PROBATIONARY_RECOGNITION,
    "seeking recognition": StatusZoneType.SEEKING_RECOGNITION,
    "probation": StatusZoneType.CONDUCT_PROBATION,
    "good conduct standing": StatusZoneType.CONDUCT_GOOD,
    "deferred suspension": StatusZoneType.DEFERRED_SUSPENSION,
}


def _normalize(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _zone_type_for_text(*, heading: str, text: str) -> StatusZoneType:
    normalized = _normalize(f"{heading} {text[:2000]}")
    for token, zone_type in _NEGATIVE_ZONE_TOKENS.items():
        if token in normalized:
            return zone_type
    if any(
        marker in normalized
        for marker in (
            "may neither join nor participate in fraternities",
            "no fraternities on campus",
            "students may neither join nor participate in fraternities",
        )
    ):
        return StatusZoneType.NO_GREEK_POLICY
    for token, zone_type in _POSITIVE_ZONE_TOKENS.items():
        if token in normalized:
            return zone_type
    if "fraternities" in normalized and any(marker in normalized for marker in ("view scorecard", "chapter", "active")):
        return StatusZoneType.ACTIVE
    if any(marker in normalized for marker in ("footer", "contact us", "privacy policy", "breadcrumb")):
        return StatusZoneType.FOOTER
    if any(marker in normalized for marker in ("menu", "search", "home", "skip navigation")):
        return StatusZoneType.NAVIGATION
    return StatusZoneType.UNKNOWN


def _block_text(block) -> str:
    return " ".join(text.strip() for text in block.stripped_strings)


def parse_status_zones(source: CampusStatusSource) -> list[StatusZone]:
    html = source.html or ""
    text = source.text or ""
    zones: list[StatusZone] = []
    seen_payloads: set[tuple[str, str]] = set()

    if html:
        soup = BeautifulSoup(html, "html.parser")
        for selector_key, section_texts in _tabbed_roster_section_texts(soup).items():
            zone_type = {
                "fraternities": StatusZoneType.ACTIVE,
                "suspended": StatusZoneType.SUSPENDED,
                "closed": StatusZoneType.CLOSED,
                "sororities": StatusZoneType.UNKNOWN,
            }.get(selector_key, StatusZoneType.UNKNOWN)
            zone_text = " ".join(section_texts)
            key = (selector_key, zone_text[:400])
            if zone_text and key not in seen_payloads:
                seen_payloads.add(key)
                zones.append(
                    StatusZone(
                        source_url=source.source_url,
                        zone_type=zone_type,
                        heading=selector_key,
                        dom_path=f"tab:{selector_key}",
                        text=zone_text,
                        confidence=0.94 if zone_type != StatusZoneType.UNKNOWN else 0.6,
                        metadata={"strategy": "tabbed_section"},
                    )
                )

        for heading in soup.select("h1, h2, h3, h4"):
            if heading.name == "h1" and soup.select("h2, h3"):
                continue
            heading_text = heading.get_text(" ", strip=True)
            collected_parts: list[str] = []
            node = heading.next_sibling
            steps = 0
            while node is not None and steps < 12:
                name = getattr(node, "name", None)
                if name in {"h1", "h2", "h3", "h4"}:
                    break
                if getattr(node, "get_text", None):
                    block_text = _block_text(node)
                    if block_text:
                        collected_parts.append(block_text)
                node = node.next_sibling
                steps += 1
            joined = " ".join(collected_parts).strip()
            if not joined:
                continue
            zone_text = f"{heading_text} {joined}".strip()
            zone_type = _zone_type_for_text(heading=heading_text, text=zone_text)
            key = (heading_text, zone_text[:400])
            if key in seen_payloads:
                continue
            seen_payloads.add(key)
            zones.append(
                StatusZone(
                    source_url=source.source_url,
                    zone_type=zone_type,
                    heading=heading_text,
                    dom_path=heading.name,
                    text=zone_text,
                    confidence=0.9 if zone_type != StatusZoneType.UNKNOWN else 0.55,
                    metadata={"strategy": "heading_block"},
                )
            )

    fallback_zone_type = _zone_type_for_text(heading=source.title, text=text)
    if text.strip() and not zones:
        key = ("document", text[:400])
        if key not in seen_payloads:
            zones.append(
                StatusZone(
                    source_url=source.source_url,
                    zone_type=fallback_zone_type,
                    heading=source.title or None,
                    dom_path="document",
                    text=text,
                    confidence=0.72 if fallback_zone_type != StatusZoneType.UNKNOWN else 0.45,
                    metadata={"strategy": "document_fallback"},
                )
            )

    if not zones:
        zones.append(
            StatusZone(
                source_url=source.source_url,
                zone_type=StatusZoneType.UNKNOWN,
                heading=source.title or None,
                dom_path="document",
                text=text,
                confidence=0.1,
                metadata={"strategy": "empty_fallback"},
            )
        )
    return zones
