from __future__ import annotations

from bs4 import BeautifulSoup

from fratfinder_crawler.models import ExtractedChapter


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


class DirectoryV1Adapter:
    """Parses list-based chapter directories from predictable card/table patterns."""

    def parse(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client=None,
    ) -> list[ExtractedChapter]:
        soup = BeautifulSoup(html, "html.parser")
        records: list[ExtractedChapter] = []

        cards = soup.select("[data-chapter-card], .chapter-card, li.chapter-item")
        for card in cards:
            name_node = card.select_one("[data-chapter-name], h1, h2, h3, .chapter-name, a")
            if not name_node:
                continue

            name = name_node.get_text(" ", strip=True)
            university = card.select_one("[data-university], .university")
            location = card.select_one("[data-location], .location")
            link = card.select_one("[data-chapter-link], a[href]")

            city, state = _parse_city_state(location.get_text(" ", strip=True) if location else None)
            snippet = card.get_text(" ", strip=True)

            records.append(
                ExtractedChapter(
                    name=name,
                    university_name=university.get_text(" ", strip=True) if university else None,
                    city=city,
                    state=state,
                    website_url=link.get("href") if link else None,
                    external_id=card.get("data-id"),
                    source_url=source_url,
                    source_snippet=snippet[:400],
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
            university = values[1]
            if len(values) >= 4:
                city = values[2] or None
                state = values[3] or None
            else:
                location = values[2] if len(values) > 2 else None
                city, state = _parse_city_state(location)
            link = row.select_one("a[href]")

            if not name:
                continue

            records.append(
                ExtractedChapter(
                    name=name,
                    university_name=university or None,
                    city=city,
                    state=state,
                    website_url=link.get("href") if link else None,
                    source_url=source_url,
                    source_snippet=row.get_text(" ", strip=True)[:400],
                )
            )

        return records
