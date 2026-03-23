from __future__ import annotations

from collections import Counter

from bs4 import BeautifulSoup

from fratfinder_crawler.models import PageAnalysis

_INLINE_JSON_HINTS = (
    "window.chapters",
    "var chapters",
    "window.locations",
    "var locations",
    "storepoint",
    "wpsl_settings",
)

_MAP_HINTS = (
    "storepoint",
    "locator",
    "leaflet",
    "google.maps",
    "mapbox",
    "wpsl",
)


def analyze_page(html: str) -> PageAnalysis:
    soup = BeautifulSoup(html, "html.parser")
    headings = [node.get_text(" ", strip=True) for node in soup.select("h1, h2, h3") if node.get_text(" ", strip=True)]
    table_count = len(soup.select("table"))
    repeated_block_count = _count_repeated_blocks(soup)
    link_count = len(soup.select("a[href]"))
    has_json_ld = any(node.get_text(strip=True) for node in soup.select('script[type="application/ld+json"]'))
    script_text = "\n".join(node.get_text(" ", strip=True) for node in soup.select("script") if node.get_text(" ", strip=True))
    has_script_json = any(hint in script_text for hint in _INLINE_JSON_HINTS)
    has_map_widget = _has_map_widget(soup, script_text)
    has_pagination = _has_pagination(soup)

    probable_page_role = "unknown"
    if table_count > 0 or repeated_block_count >= 1:
        probable_page_role = "directory"
    elif has_map_widget:
        probable_page_role = "search"
    elif has_pagination:
        probable_page_role = "index"
    elif headings or link_count <= 5:
        probable_page_role = "detail"

    title_node = soup.title.get_text(" ", strip=True) if soup.title else None
    text_sample = " ".join(soup.stripped_strings)[:2000]

    return PageAnalysis(
        title=title_node or None,
        headings=headings,
        table_count=table_count,
        repeated_block_count=repeated_block_count,
        link_count=link_count,
        has_json_ld=has_json_ld,
        has_script_json=has_script_json,
        has_map_widget=has_map_widget,
        has_pagination=has_pagination,
        probable_page_role=probable_page_role,
        text_sample=text_sample,
    )


def _count_repeated_blocks(soup: BeautifulSoup) -> int:
    explicit_blocks = soup.select("[data-chapter-card], .chapter-card, li.chapter-item")
    if explicit_blocks:
        return len(explicit_blocks)

    repeated_class_counts: Counter[tuple[str, tuple[str, ...]]] = Counter()
    for element in soup.find_all(["article", "div", "li", "section"]):
        classes = tuple(sorted(element.get("class", [])))
        if not classes:
            continue
        repeated_class_counts[(element.name, classes)] += 1

    repeated_candidates = [count for count in repeated_class_counts.values() if count > 1]
    if repeated_candidates:
        return max(repeated_candidates)

    return 0


def _has_map_widget(soup: BeautifulSoup, script_text: str) -> bool:
    attr_values: list[str] = []
    for node in soup.find_all(True):
        attr_values.extend(str(value).lower() for value in node.attrs.values())

    joined_attrs = " ".join(attr_values)
    lowered_script = script_text.lower()
    return any(hint in joined_attrs or hint in lowered_script for hint in _MAP_HINTS)


def _has_pagination(soup: BeautifulSoup) -> bool:
    if soup.select('[rel="next"], .pagination, nav[aria-label*="pagination" i]'):
        return True

    pagination_texts = {
        node.get_text(" ", strip=True).lower()
        for node in soup.select("a, button")
        if node.get_text(" ", strip=True)
    }
    return any(text in pagination_texts for text in {"next", "next page", "older"})
