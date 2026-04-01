from __future__ import annotations

import json
import re
from typing import Any

from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.models import ChapterStub, ExtractedChapter

_JSON_LD_TYPES = {"educationalorganization"}
_SCRIPT_ARRAY_PATTERNS = (
    re.compile(r"(?:window\.)?chapters\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"var\s+chapters\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"(?:window\.)?locations\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"var\s+locations\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"storepoint\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"wpsl_settings\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
)


class ScriptJsonAdapter:
    def parse_stubs(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client: Any | None = None,
    ) -> list[ChapterStub]:
        payloads = _extract_json_ld_payloads(html)
        payloads.extend(_extract_inline_array_payloads(html))
        stubs: list[ChapterStub] = []
        for payload in payloads:
            stub = _payload_to_stub(payload, source_url)
            if stub is not None:
                stubs.append(stub)
        return stubs

    def parse(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client: Any | None = None,
    ) -> list[ExtractedChapter]:
        payloads = _extract_json_ld_payloads(html)
        payloads.extend(_extract_inline_array_payloads(html))
        return _payloads_to_chapters(payloads, source_url)


def _extract_json_ld_payloads(html: str) -> list[dict[str, Any]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    payloads: list[dict[str, Any]] = []
    for script in soup.select('script[type="application/ld+json"]'):
        text = script.get_text(" ", strip=True)
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        for payload in _coerce_dict_list(data):
            if _is_supported_json_ld(payload):
                payloads.append(payload)
    return payloads


def _extract_inline_array_payloads(html: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for pattern in _SCRIPT_ARRAY_PATTERNS:
        for match in pattern.finditer(html):
            try:
                data = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue
            payloads.extend(_coerce_dict_list(data))
    return payloads


def _payloads_to_chapters(payloads: list[dict[str, Any]], source_url: str) -> list[ExtractedChapter]:
    records: list[ExtractedChapter] = []
    for payload in payloads:
        record = _payload_to_chapter(payload, source_url)
        if record is not None:
            records.append(record)
    return records


def _payload_to_stub(payload: dict[str, Any], source_url: str) -> ChapterStub | None:
    chapter = _payload_to_chapter(payload, source_url)
    if chapter is None:
        return None
    return ChapterStub(
        chapter_name=chapter.name,
        university_name=chapter.university_name,
        detail_url=chapter.website_url,
        outbound_chapter_url_candidate=chapter.website_url,
        confidence=chapter.source_confidence,
        provenance=f"script_json:{chapter.source_url}",
    )


def _payload_to_chapter(payload: dict[str, Any], source_url: str) -> ExtractedChapter | None:
    name = _get_first_string(payload, ["chapter_name", "chapterName", "name", "title"])
    university_name = _get_first_string(payload, ["school_name", "schoolName", "university_name", "universityName", "college", "institution"])
    city = _get_first_string(payload, ["city", "town"])
    state = _get_first_string(payload, ["state", "stateProvince", "province", "region"])
    website_url = sanitize_as_website(_get_first_string(payload, ["website_url", "websiteUrl", "website", "url"]), base_url=source_url)
    contact_email = sanitize_as_email(_get_first_string(payload, ["email", "contact_email", "contactEmail", "mail", "mailto"]))
    instagram_url = sanitize_as_instagram(
        _get_first_string(payload, ["instagram_url", "instagramUrl", "instagram", "instagram_handle", "instagramHandle"])
    )
    external_id = _get_first_string(payload, ["chapter_id", "chapterId", "external_id", "externalId", "slug", "id", "@id"])

    address = payload.get("address")
    if isinstance(address, dict):
        city = city or _get_first_string(address, ["addressLocality", "city"])
        state = state or _get_first_string(address, ["addressRegion", "state", "province"])

    if not _looks_like_chapter_payload(payload, name, university_name, city, state, website_url):
        return None

    confidence = _calculate_confidence(name, university_name, city, state, website_url, payload)
    snippet = json.dumps(payload, sort_keys=True)[:400]
    return ExtractedChapter(
        name=name.strip(),
        university_name=university_name,
        city=city,
        state=state,
        website_url=website_url,
        instagram_url=instagram_url,
        contact_email=contact_email,
        external_id=external_id,
        source_url=source_url,
        source_snippet=snippet,
        source_confidence=confidence,
    )


def _looks_like_chapter_payload(
    payload: dict[str, Any],
    name: str | None,
    university_name: str | None,
    city: str | None,
    state: str | None,
    website_url: str | None,
) -> bool:
    if not name:
        return False
    if _is_supported_json_ld(payload):
        return True
    return any(value is not None for value in (university_name, city, state, website_url))


def _is_supported_json_ld(payload: dict[str, Any]) -> bool:
    type_value = payload.get("@type")
    if isinstance(type_value, list):
        lowered = {str(item).lower() for item in type_value}
        return not _JSON_LD_TYPES.isdisjoint(lowered)
    if isinstance(type_value, str):
        return type_value.lower() in _JSON_LD_TYPES
    return False


def _calculate_confidence(
    name: str | None,
    university_name: str | None,
    city: str | None,
    state: str | None,
    website_url: str | None,
    payload: dict[str, Any],
) -> float:
    score = 0.45 if name else 0.0
    for value in (university_name, city, state, website_url):
        if value:
            score += 0.1
    if _is_supported_json_ld(payload):
        score += 0.1
    return min(round(score, 2), 1.0)


def _get_first_string(payload: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            value = str(value)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _coerce_dict_list(data: object) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        graph = data.get("@graph") if isinstance(data.get("@graph"), list) else None
        if graph is not None:
            return [item for item in graph if isinstance(item, dict)]
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []
