from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from typing import Any

from fratfinder_crawler.adapters.script_json import _coerce_dict_list, _payloads_to_chapters
from fratfinder_crawler.models import ExtractedChapter


class LocatorApiAdapter:
    def parse(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client: Any | None = None,
    ) -> list[ExtractedChapter]:
        if not api_url or http_client is None:
            return []

        try:
            payload_text = http_client.get(api_url)
            stripped = payload_text.lstrip()
            if stripped.startswith("<"):
                return _kml_to_chapters(payload_text, api_url)
            data = json.loads(payload_text)
        except Exception:
            return []

        payloads = _coerce_dict_list(data)
        return _payloads_to_chapters(payloads, api_url)


_KML_NAMESPACE = {"k": "http://www.opengis.net/kml/2.2"}


def _parse_description_fields(description: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in re.split(r"<br\s*/?>", description, flags=re.IGNORECASE):
        plain = re.sub(r"<[^>]+>", "", line).strip()
        if not plain or ":" not in plain:
            continue
        key, value = plain.split(":", 1)
        fields[key.strip().lower()] = value.strip()
    return fields


def _normalize_instagram(value: str | None) -> str | None:
    if not value:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    if trimmed.startswith("http://") or trimmed.startswith("https://"):
        return trimmed
    handle = trimmed[1:] if trimmed.startswith("@") else trimmed
    return f"https://www.instagram.com/{handle}/"


def _parse_city_state(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return (None, None)
    parts = [part.strip() for part in value.split(",") if part.strip()]
    if len(parts) >= 2:
        return (parts[0], parts[-1])
    return (value.strip() or None, None)


def _kml_to_chapters(kml_text: str, source_url: str) -> list[ExtractedChapter]:
    try:
        root = ET.fromstring(kml_text)
    except ET.ParseError:
        return []

    chapters: list[ExtractedChapter] = []
    seen_keys: set[tuple[str, str]] = set()

    for placemark in root.findall(".//k:Placemark", _KML_NAMESPACE):
        name_node = placemark.find("k:name", _KML_NAMESPACE)
        description_node = placemark.find("k:description", _KML_NAMESPACE)

        university_name = (name_node.text or "").strip() if name_node is not None and name_node.text else ""
        description = (description_node.text or "") if description_node is not None and description_node.text else ""
        fields = _parse_description_fields(description)

        chapter_name = fields.get("alias") or university_name
        website_url = fields.get("website") or None
        instagram_url = _normalize_instagram(fields.get("instagram"))
        city, state = _parse_city_state(fields.get("preferred city_ state"))

        if not chapter_name:
            continue

        dedupe_key = (chapter_name.lower(), university_name.lower())
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        chapters.append(
            ExtractedChapter(
                name=chapter_name,
                university_name=university_name or None,
                city=city,
                state=state,
                website_url=website_url,
                instagram_url=instagram_url,
                source_url=source_url,
                source_snippet=fields.get("preferred address lines"),
                source_confidence=0.95,
            )
        )

    return chapters
