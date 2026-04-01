from __future__ import annotations

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from fratfinder_crawler.analysis import has_dom_neighborhood, score_chapter_link
from fratfinder_crawler.models import ChapterStub, ExtractedChapter


TABLE_HEADER_LABELS = {
    "greek-letter chapter name",
    "college/university",
    "city",
    "state/province",
    "country",
    "fraternity province",
}


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


def _pick_best_link(container, source_url: str, chapter_name: str | None, university_name: str | None) -> tuple[str | None, float]:
    anchors = container.select("a[href]")
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
    ) -> list[ChapterStub]:
        soup = BeautifulSoup(html, "html.parser")
        records: list[ChapterStub] = []

        cards = soup.select("[data-chapter-card], .chapter-card, li.chapter-item")
        for card in cards:
            name_node = card.select_one("[data-chapter-name], h1, h2, h3, .chapter-name, a")
            if not name_node:
                continue
            name = name_node.get_text(" ", strip=True)
            if not name:
                continue

            university = card.select_one("[data-university], .university")
            detail_or_outbound, link_score = _pick_best_link(
                card,
                source_url,
                chapter_name=name,
                university_name=university.get_text(" ", strip=True) if university else None,
            )

            confidence = 0.75 + min(0.2, link_score * 0.2)
            records.append(
                ChapterStub(
                    chapter_name=name,
                    university_name=university.get_text(" ", strip=True) if university else None,
                    detail_url=detail_or_outbound,
                    outbound_chapter_url_candidate=detail_or_outbound,
                    confidence=min(0.99, confidence),
                    provenance="directory_v1:card",
                )
            )

        if records:
            return records

        rows = soup.select("table tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            if _is_header_row(cells):
                continue

            values = [cell.get_text(" ", strip=True) for cell in cells]
            name = values[0]
            university = values[1] if len(values) > 1 else None
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
        return records

    def parse(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client=None,
    ) -> list[ExtractedChapter]:
        soup = BeautifulSoup(html, "html.parser")
        stubs = self.parse_stubs(html, source_url, api_url=api_url, http_client=http_client)
        records: list[ExtractedChapter] = []

        cards = soup.select("[data-chapter-card], .chapter-card, li.chapter-item")
        rows = soup.select("table tr")
        card_lookup: dict[tuple[str, str], tuple[str | None, str | None, str | None]] = {}
        if cards:
            for card in cards:
                name_node = card.select_one("[data-chapter-name], h1, h2, h3, .chapter-name, a")
                if name_node is None:
                    continue
                name = name_node.get_text(" ", strip=True)
                university = card.select_one("[data-university], .university")
                university_name = university.get_text(" ", strip=True) if university else None
                key = ((name or "").strip().lower(), (university_name or "").strip().lower())
                location = card.select_one("[data-location], .location")
                city, state = _parse_city_state(location.get_text(" ", strip=True) if location else None)
                card_lookup[key] = (city, state, card.get_text(" ", strip=True)[:400])

        table_lookup: dict[tuple[str, str], tuple[str | None, str | None, str | None]] = {}
        if rows:
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2 or _is_header_row(cells):
                    continue
                values = [cell.get_text(" ", strip=True) for cell in cells]
                name = values[0]
                university_name = values[1] if len(values) > 1 else None
                if not name:
                    continue
                city = None
                state = None
                if len(values) >= 4:
                    city = values[2] or None
                    state = values[3] or None
                elif len(values) > 2:
                    city, state = _parse_city_state(values[2])
                key = ((name or "").strip().lower(), (university_name or "").strip().lower())
                table_lookup[key] = (city, state, row.get_text(" ", strip=True)[:400])

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
        return records
