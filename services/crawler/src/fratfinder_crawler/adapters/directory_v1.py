from __future__ import annotations

from html import unescape
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.analysis import has_dom_neighborhood, score_chapter_link
from fratfinder_crawler.models import ChapterStub, ExtractedChapter


TABLE_HEADER_LABELS = {
    "greek-letter chapter name",
    "alpha",
    "symbol",
    "college",
    "college/university",
    "city",
    "state/province",
    "country",
    "founded",
    "active since",
    "dormant since",
    "fraternity province",
}

_ARCHIVE_CONTACT_LABELS = ("website:", "instagram:", "facebook:", "twitter:", "email:")
_ADDRESS_PATTERN = re.compile(r"\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b")
_LABELED_LINK_PATTERN = re.compile(r"(?P<label>Website|Instagram|Facebook|Twitter|Email)\s*:\s*<a[^>]+href=[\"'](?P<href>[^\"']+)", re.IGNORECASE)
_TITLE_SPLIT_PATTERN = re.compile(r"^(?P<chapter>.+?)\s+[–—-]\s+(?P<university>.+)$")
_UNIVERSITY_CHAPTER_PATTERN = re.compile(r"^(?P<university>.+?)\s+[–—-]\s+(?P<chapter>.+?)\s+Chapter$", re.IGNORECASE)

_DEFAULT_CARD_SELECTORS = (
    "[data-chapter-card]",
    ".chapter-card",
    ".chapter-item",
    "li.chapter-item",
    "a.chapter-link",
    ".grid-item .card",
    ".card.h-100",
)

_DEFAULT_CARD_NAME_SELECTORS = (
    "[data-chapter-name]",
    "h1",
    "h2",
    "h3",
    ".chapter-name",
    ".card-title a",
    ".card-title",
    "a",
)

_DEFAULT_CARD_UNIVERSITY_SELECTORS = (
    "[data-university]",
    ".university",
)


def _parse_city_state(location: str | None) -> tuple[str | None, str | None]:
    if not location:
        return None, None
    if "," not in location:
        return location.strip() or None, None
    city, state = location.split(",", 1)
    return city.strip() or None, state.strip() or None


def _is_header_row(cells: list) -> bool:
    if any(cell.name == "th" for cell in cells):
        return True

    normalized_labels = {
        cell.get_text(" ", strip=True).strip().lower()
        for cell in cells
        if cell.get_text(" ", strip=True).strip()
    }
    return bool(normalized_labels) and normalized_labels.issubset(TABLE_HEADER_LABELS)


def _normalize_table_value(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.replace(chr(173), "").strip()
    return cleaned or None


def _table_header_map(table) -> dict[str, int]:
    for row in table.select("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2 or not _is_header_row(cells):
            continue
        headers: dict[str, int] = {}
        for index, cell in enumerate(cells):
            label = _normalize_table_value(cell.get_text(" ", strip=True))
            if label:
                headers[label.lower()] = index
        if headers:
            return headers
    return {}


def _table_indexes(headers: dict[str, int], values: list[str]) -> tuple[int, int | None, int | None, int | None]:
    def _first(*keys: str) -> int | None:
        for key in keys:
            if key in headers:
                return headers[key]
        return None

    chapter_idx = _first("greek-letter chapter name", "alpha", "chapter", "chapter name", "name", "alias")
    university_idx = _first("college/university", "college", "university", "school", "campus", "institution")
    city_idx = _first("city", "town")
    state_idx = _first("state/province", "state", "province")

    if chapter_idx is None:
        chapter_idx = 0
    if university_idx is None and len(values) > 1:
        university_idx = 1 if chapter_idx != 1 else (2 if len(values) > 2 else None)

    return chapter_idx, university_idx, city_idx, state_idx


def _title_case_if_loud(value: str | None) -> str | None:
    if not value:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    alpha_chars = [char for char in trimmed if char.isalpha()]
    uppercase_ratio = (
        sum(1 for char in alpha_chars if char.isupper()) / len(alpha_chars)
        if alpha_chars
        else 0.0
    )
    if uppercase_ratio >= 0.7:
        return trimmed.title()
    return trimmed


def _looks_like_address(value: str | None) -> bool:
    if not value:
        return False
    lowered = value.lower()
    return bool(_ADDRESS_PATTERN.search(value) or re.search(r"\b(po box|suite|ave|street|st\.|road|rd\.|blvd|lane|ln\.|drive|dr\.)\b", lowered))


def _find_labeled_links(raw_html: str) -> dict[str, str]:
    matches: dict[str, str] = {}
    for match in _LABELED_LINK_PATTERN.finditer(raw_html):
        matches[match.group("label").lower()] = unescape(match.group("href"))
    return matches


def _record_richness(record: ExtractedChapter) -> int:
    return sum(
        1
        for value in (
            record.university_name,
            record.city,
            record.state,
            record.website_url,
            record.instagram_url,
            record.contact_email,
        )
        if value
    )


def _split_combined_heading(title: str | None) -> tuple[str | None, str | None]:
    if not title:
        return None, None
    match = _TITLE_SPLIT_PATTERN.match(title.strip())
    if not match:
        return title.strip() or None, None
    return match.group("chapter").strip() or None, match.group("university").strip() or None


def _extract_repeated_list_stubs(soup: BeautifulSoup, source_url: str) -> list[ChapterStub]:
    stubs: list[ChapterStub] = []
    seen: set[tuple[str, str]] = set()
    matched_items = 0
    for item in soup.find_all("li"):
        text = item.get_text(" ", strip=True).replace("\xa0", " ")
        match = _UNIVERSITY_CHAPTER_PATTERN.match(text)
        if not match:
            continue

        matched_items += 1
        university_name = _title_case_if_loud(match.group("university"))
        chapter_name = _title_case_if_loud(match.group("chapter"))
        if not university_name or not chapter_name:
            continue

        key = (chapter_name.lower(), university_name.lower())
        if key in seen:
            continue
        seen.add(key)

        detail_or_outbound, link_score = _pick_best_link(
            item,
            source_url,
            chapter_name=chapter_name,
            university_name=university_name,
        )
        confidence = 0.8 + min(0.12, link_score * 0.12)
        stubs.append(
            ChapterStub(
                chapter_name=chapter_name,
                university_name=university_name,
                detail_url=detail_or_outbound or source_url,
                outbound_chapter_url_candidate=detail_or_outbound,
                confidence=min(0.99, confidence),
                provenance="directory_v1:repeated_list",
            )
        )

    return stubs if matched_items >= 5 else []


def _selector_list(
    source_metadata: dict | None,
    key: str,
    defaults: tuple[str, ...],
) -> list[str]:
    hints = ((source_metadata or {}).get("extractionHints") or {})
    directory_selectors = hints.get("directorySelectors") or {}
    configured = directory_selectors.get(key)
    merged: list[str] = []
    for selector in [*(configured or []), *defaults]:
        if isinstance(selector, str) and selector.strip() and selector not in merged:
            merged.append(selector)
    return merged


def _extract_structured_card_identity(card) -> tuple[str | None, str | None]:
    classes = {str(value).lower() for value in card.get("class", [])}
    has_chapter_structure = (
        "chapter-item" in classes
        or "chapter-link" in classes
        or any("chapter" in value for value in classes)
        or card.select_one(".chapter-logo") is not None
    )
    if not has_chapter_structure:
        return None, None

    chapter_heading = card.select_one("h2")
    university_heading = card.select_one("h3")
    if chapter_heading is None or university_heading is None:
        return None, None

    chapter_name = _title_case_if_loud(chapter_heading.get_text(" ", strip=True))
    university_name = _title_case_if_loud(university_heading.get_text(" ", strip=True))
    if not chapter_name or not university_name:
        return None, None
    if chapter_name.strip().casefold() == university_name.strip().casefold():
        return None, None
    return chapter_name, university_name


def _extract_card_identity(
    card,
    *,
    name_selectors: list[str],
    university_selectors: list[str],
) -> tuple[str | None, str | None]:
    chapter_name, university_name = _extract_structured_card_identity(card)
    if chapter_name:
        return chapter_name, university_name

    name_node = next((card.select_one(selector) for selector in name_selectors if card.select_one(selector)), None)
    if not name_node:
        return None, None

    raw_name = _title_case_if_loud(name_node.get_text(" ", strip=True))
    if not raw_name:
        return None, None

    university_node = next(
        (card.select_one(selector) for selector in university_selectors if card.select_one(selector)),
        None,
    )
    split_chapter_name, fallback_university = _split_combined_heading(raw_name)
    university_text = (
        _title_case_if_loud(university_node.get_text(" ", strip=True))
        if university_node
        else _title_case_if_loud(fallback_university)
    )
    return split_chapter_name or raw_name, university_text


def _archive_contact_entries(soup: BeautifulSoup, source_url: str) -> list[dict[str, str | None]]:
    entries: list[dict[str, str | None]] = []
    seen: set[tuple[str, str]] = set()
    for container in soup.select(".elementor-widget-text-editor .elementor-widget-container"):
        raw_html = str(container)
        lowered = raw_html.lower()
        if not any(label in lowered for label in _ARCHIVE_CONTACT_LABELS):
            continue

        article = container.find_parent("article") or container.find_parent("section") or container.find_parent("div")
        if article is None:
            continue

        headings = [_title_case_if_loud(node.get_text(" ", strip=True)) for node in article.find_all(["h1", "h2", "h3"])]
        headings = [heading for heading in headings if heading]
        chapter_heading = next((heading for heading in headings if "chapter" in heading.lower()), None)
        if not chapter_heading:
            continue

        address_heading = next((heading for heading in headings if heading != chapter_heading and _looks_like_address(heading)), None)
        links = _find_labeled_links(raw_html)
        website_url = sanitize_as_website(links.get("website"), base_url=source_url)
        instagram_url = sanitize_as_instagram(links.get("instagram"))
        contact_email = sanitize_as_email(links.get("email"))
        if not any((website_url, instagram_url, contact_email)):
            continue

        university_name = re.sub(r"\s+chapter$", "", chapter_heading, flags=re.IGNORECASE).strip() or None
        key = ((chapter_heading or "").lower(), (university_name or "").lower())
        if key in seen:
            continue
        seen.add(key)
        entries.append(
            {
                "chapter_name": chapter_heading,
                "university_name": university_name,
                "address": address_heading,
                "website_url": website_url,
                "instagram_url": instagram_url,
                "contact_email": contact_email,
                "snippet": article.get_text(" ", strip=True)[:500],
            }
        )
    return entries


def _pick_best_link(container, source_url: str, chapter_name: str | None, university_name: str | None) -> tuple[str | None, float]:
    anchors = []
    if getattr(container, "name", None) == "a" and container.get("href"):
        anchors.append(container)
    anchors.extend(container.select("a[href]"))
    if not anchors:
        return None, 0.0

    context_text = container.get_text(" ", strip=True)
    best_url: str | None = None
    best_score = 0.0
    for anchor in anchors:
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        score = score_chapter_link(anchor.get_text(" ", strip=True), href, context_text)
        if "go to site" in anchor.get_text(" ", strip=True).lower() and not has_dom_neighborhood(chapter_name, university_name, context_text):
            continue
        if score.score > best_score:
            best_score = score.score
            best_url = urljoin(source_url, href)
    return best_url, best_score


class DirectoryV1Adapter:
    """Parses list-based chapter directories from predictable card/table patterns."""

    def parse_stubs(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client=None,
        source_metadata: dict | None = None,
    ) -> list[ChapterStub]:
        soup = BeautifulSoup(html, "html.parser")
        records: list[ChapterStub] = []

        card_selectors = _selector_list(source_metadata, "cardSelectors", _DEFAULT_CARD_SELECTORS)
        name_selectors = _selector_list(source_metadata, "nameSelectors", _DEFAULT_CARD_NAME_SELECTORS)
        university_selectors = _selector_list(source_metadata, "universitySelectors", _DEFAULT_CARD_UNIVERSITY_SELECTORS)

        cards = []
        seen_card_ids: set[int] = set()
        for selector in card_selectors:
            for node in soup.select(selector):
                if id(node) in seen_card_ids:
                    continue
                seen_card_ids.add(id(node))
                cards.append(node)
        for card in cards:
            chapter_name, university_text = _extract_card_identity(
                card,
                name_selectors=name_selectors,
                university_selectors=university_selectors,
            )
            if not chapter_name:
                continue

            detail_or_outbound, link_score = _pick_best_link(
                card,
                source_url,
                chapter_name=chapter_name,
                university_name=university_text,
            )

            confidence = 0.75 + min(0.2, link_score * 0.2)
            records.append(
                ChapterStub(
                    chapter_name=chapter_name,
                    university_name=university_text,
                    detail_url=detail_or_outbound or source_url,
                    outbound_chapter_url_candidate=detail_or_outbound,
                    confidence=min(0.99, confidence),
                    provenance="directory_v1:card",
                )
            )

        if records:
            return records

        for table in soup.select("table"):
            header_map = _table_header_map(table)
            for row in table.select("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                if _is_header_row(cells):
                    continue

                values = [_normalize_table_value(cell.get_text(" ", strip=True)) or "" for cell in cells]
                chapter_idx, university_idx, _, _ = _table_indexes(header_map, values)
                name = values[chapter_idx] if chapter_idx < len(values) else ""
                university = values[university_idx] if university_idx is not None and university_idx < len(values) else None
                if not name:
                    continue
                detail_or_outbound, link_score = _pick_best_link(row, source_url, chapter_name=name, university_name=university)
                confidence = 0.72 + min(0.2, link_score * 0.2)
                records.append(
                    ChapterStub(
                        chapter_name=name,
                        university_name=university or None,
                        detail_url=detail_or_outbound,
                        outbound_chapter_url_candidate=detail_or_outbound,
                        confidence=min(0.99, confidence),
                        provenance="directory_v1:table",
                    )
                )
        if records:
            return records

        records = _extract_repeated_list_stubs(soup, source_url)
        if records:
            return records

        for entry in _archive_contact_entries(soup, source_url):
            outbound = entry["website_url"] or entry["instagram_url"] or entry["contact_email"]
            records.append(
                ChapterStub(
                    chapter_name=str(entry["chapter_name"]),
                    university_name=str(entry["university_name"]) if entry["university_name"] else None,
                    detail_url=source_url,
                    outbound_chapter_url_candidate=str(outbound) if outbound else None,
                    confidence=0.93,
                    provenance="directory_v1:archive_entry",
                )
            )
        return records

    def parse(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client=None,
        source_metadata: dict | None = None,
    ) -> list[ExtractedChapter]:
        soup = BeautifulSoup(html, "html.parser")
        stubs = self.parse_stubs(
            html,
            source_url,
            api_url=api_url,
            http_client=http_client,
            source_metadata=source_metadata,
        )
        records: list[ExtractedChapter] = []

        card_selectors = _selector_list(source_metadata, "cardSelectors", _DEFAULT_CARD_SELECTORS)
        name_selectors = _selector_list(source_metadata, "nameSelectors", _DEFAULT_CARD_NAME_SELECTORS)
        university_selectors = _selector_list(source_metadata, "universitySelectors", _DEFAULT_CARD_UNIVERSITY_SELECTORS)
        cards = []
        seen_card_ids: set[int] = set()
        for selector in card_selectors:
            for node in soup.select(selector):
                if id(node) in seen_card_ids:
                    continue
                seen_card_ids.add(id(node))
                cards.append(node)
        rows = soup.select("table tr")
        card_lookup: dict[tuple[str, str], tuple[str | None, str | None, str | None]] = {}
        if cards:
            for card in cards:
                chapter_name, university_name = _extract_card_identity(
                    card,
                    name_selectors=name_selectors,
                    university_selectors=university_selectors,
                )
                if chapter_name is None:
                    continue
                key = ((chapter_name or "").strip().lower(), (university_name or "").strip().lower())
                location = card.select_one("[data-location], .location")
                city, state = _parse_city_state(location.get_text(" ", strip=True) if location else None)
                card_lookup[key] = (city, state, card.get_text(" ", strip=True)[:400])

        table_lookup: dict[tuple[str, str], tuple[str | None, str | None, str | None]] = {}
        if rows:
            for table in soup.select("table"):
                header_map = _table_header_map(table)
                for row in table.select("tr"):
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2 or _is_header_row(cells):
                        continue
                    values = [_normalize_table_value(cell.get_text(" ", strip=True)) or "" for cell in cells]
                    chapter_idx, university_idx, city_idx, state_idx = _table_indexes(header_map, values)
                    name = values[chapter_idx] if chapter_idx < len(values) else ""
                    university_name = values[university_idx] if university_idx is not None and university_idx < len(values) else None
                    if not name:
                        continue
                    city = values[city_idx] if city_idx is not None and city_idx < len(values) else None
                    state = values[state_idx] if state_idx is not None and state_idx < len(values) else None
                    if city_idx is None and state_idx is None:
                        if len(values) >= 4:
                            city = values[2] or None
                            state = values[3] or None
                        elif len(values) > 2:
                            city, state = _parse_city_state(values[2])
                    key = ((name or "").strip().lower(), (university_name or "").strip().lower())
                    table_lookup[key] = (city, state, row.get_text(" ", strip=True).replace(chr(173), "")[:400])

        for stub in stubs:
            key = ((stub.chapter_name or "").strip().lower(), (stub.university_name or "").strip().lower())
            snippet = None
            city = None
            state = None
            if key in card_lookup:
                city, state, snippet = card_lookup[key]
            elif key in table_lookup:
                city, state, snippet = table_lookup[key]

            records.append(
                ExtractedChapter(
                    name=stub.chapter_name,
                    university_name=stub.university_name,
                    city=city,
                    state=state,
                    website_url=stub.outbound_chapter_url_candidate,
                    source_url=source_url,
                    source_snippet=snippet,
                    source_confidence=stub.confidence,
                )
            )

        for entry in _archive_contact_entries(soup, source_url):
            records.append(
                ExtractedChapter(
                    name=str(entry["chapter_name"]),
                    university_name=str(entry["university_name"]) if entry["university_name"] else None,
                    city=None,
                    state=None,
                    website_url=str(entry["website_url"]) if entry["website_url"] else None,
                    instagram_url=str(entry["instagram_url"]) if entry["instagram_url"] else None,
                    contact_email=str(entry["contact_email"]) if entry["contact_email"] else None,
                    source_url=source_url,
                    source_snippet=str(entry["snippet"]) if entry["snippet"] else None,
                    source_confidence=0.93,
                )
            )

        deduped: dict[tuple[str, str], ExtractedChapter] = {}
        for record in records:
            key = ((record.name or "").strip().lower(), (record.university_name or "").strip().lower())
            current = deduped.get(key)
            if current is None or (
                record.source_confidence,
                _record_richness(record),
            ) > (
                current.source_confidence,
                _record_richness(current),
            ):
                deduped[key] = record
        return list(deduped.values())
