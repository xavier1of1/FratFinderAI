from __future__ import annotations

import json
import re
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from fratfinder_crawler.models import EmbeddedDataResult

_SCRIPT_ARRAY_PATTERNS = (
    re.compile(r"(?:window\.)?chapters\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"(?:window\.)?locations\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"var\s+chapters\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
    re.compile(r"var\s+locations\s*=\s*(\[[\s\S]*?\])\s*;", re.IGNORECASE),
)

_API_HINT_PATTERNS = (
    re.compile(r"fetch\(\s*['\"]([^'\"]+)['\"]", re.IGNORECASE),
    re.compile(r"['\"]((?:https?:)?//[^'\"]+(?:api|graphql|wp-json)[^'\"]*)['\"]", re.IGNORECASE),
    re.compile(r"['\"]((?:/[^'\"]*)?(?:api|graphql|wp-json)[^'\"]*)['\"]", re.IGNORECASE),
)


def detect_embedded_data(html: str, source_url: str | None = None) -> EmbeddedDataResult:
    soup = BeautifulSoup(html, "html.parser")

    google_maps_api_url = _detect_google_maps_kml_url(soup)
    if google_maps_api_url:
        return EmbeddedDataResult(found=True, data_type="api_hint", raw_data=None, api_url=google_maps_api_url)

    json_ld_payloads = _extract_json_ld_payloads(soup)
    if json_ld_payloads:
        return EmbeddedDataResult(found=True, data_type="json_ld", raw_data=json_ld_payloads, api_url=None)

    data_attr_payloads = _extract_data_attribute_payloads(soup)
    if data_attr_payloads:
        return EmbeddedDataResult(found=True, data_type="script_json", raw_data=data_attr_payloads, api_url=None)

    for script in soup.select("script"):
        script_text = script.get_text(" ", strip=True)
        if not script_text:
            continue

        script_payloads = _extract_script_payloads(script_text)
        if script_payloads:
            return EmbeddedDataResult(found=True, data_type="script_json", raw_data=script_payloads, api_url=None)

        api_url = _detect_api_url(script_text, source_url)
        if api_url:
            return EmbeddedDataResult(found=True, data_type="api_hint", raw_data=None, api_url=api_url)

        if "storepoint" in script_text.lower() or "wpsl_settings" in script_text.lower():
            return EmbeddedDataResult(found=True, data_type="script_json", raw_data=[], api_url=None)

    return EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None)


def _detect_google_maps_kml_url(soup: BeautifulSoup) -> str | None:
    for iframe in soup.select("iframe[src]"):
        src = iframe.get("src")
        if not src:
            continue
        lowered = src.lower()
        if "google.com/maps/d/" not in lowered and "maps.google.com/maps/d/" not in lowered:
            continue
        parsed = urlparse(src)
        mid = parse_qs(parsed.query).get("mid", [None])[0]
        if not mid:
            continue
        return f"https://www.google.com/maps/d/kml?mid={mid}&forcekml=1"
    return None


def _extract_json_ld_payloads(soup: BeautifulSoup) -> list[dict]:
    payloads: list[dict] = []
    for script in soup.select('script[type="application/ld+json"]'):
        text = script.get_text(" ", strip=True)
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        payloads.extend(_coerce_dict_list(data))
    return payloads


def _extract_data_attribute_payloads(soup: BeautifulSoup) -> list[dict]:
    payloads: list[dict] = []
    for node in soup.find_all(True):
        for attr_name, attr_value in node.attrs.items():
            if not str(attr_name).startswith("data-"):
                continue
            if not isinstance(attr_value, str):
                continue
            value = attr_value.strip()
            if not value or value[0] not in "[{":
                continue
            try:
                data = json.loads(value)
            except json.JSONDecodeError:
                continue
            payloads.extend(_coerce_dict_list(data))
    return payloads


def _extract_script_payloads(script_text: str) -> list[dict]:
    payloads: list[dict] = []
    for pattern in _SCRIPT_ARRAY_PATTERNS:
        for match in pattern.finditer(script_text):
            try:
                data = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue
            payloads.extend(_coerce_dict_list(data))
    return payloads


def _detect_api_url(script_text: str, source_url: str | None) -> str | None:
    for pattern in _API_HINT_PATTERNS:
        match = pattern.search(script_text)
        if not match:
            continue
        candidate = match.group(1)
        if candidate.startswith("//"):
            return f"https:{candidate}"
        if candidate.startswith("http"):
            return candidate
        if source_url:
            return urljoin(source_url, candidate)
        return candidate
    return None


def _coerce_dict_list(data: object) -> list[dict]:
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []
