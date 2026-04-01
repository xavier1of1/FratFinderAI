from __future__ import annotations

from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from fratfinder_crawler.models import FrontierItem, PageAnalysis


KEYWORD_WEIGHTS = {
    "chapter": 2.0,
    "chapters": 2.2,
    "directory": 1.8,
    "find": 0.5,
    "locator": 1.7,
    "map": 1.8,
    "collegiate": 1.5,
    "undergraduate": 1.1,
    "campus": 0.6,
    "university": 0.5,
    "contact": 0.6,
}

NEGATIVE_KEYWORDS = {
    "alumni": -1.2,
    "news": -0.8,
    "blog": -0.8,
    "donate": -0.9,
    "store": -0.9,
    "login": -1.1,
    "event": -0.5,
    "staff": -0.7,
}


def canonicalize_url(url: str, base_url: str | None = None) -> str:
    resolved = urljoin(base_url, url) if base_url else url
    parsed = urlparse(resolved)
    path = parsed.path or "/"
    normalized_path = path.rstrip("/") or "/"
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), normalized_path, "", parsed.query, ""))


def _keyword_score(text: str) -> float:
    lowered = text.lower()
    score = 0.0
    for keyword, value in KEYWORD_WEIGHTS.items():
        if keyword in lowered:
            score += value
    for keyword, value in NEGATIVE_KEYWORDS.items():
        if keyword in lowered:
            score += value
    return score


def score_frontier_item(
    url: str,
    *,
    anchor_text: str | None,
    depth: int,
    source_url: str,
    page_analysis: PageAnalysis | None = None,
    template_bonus: float = 0.0,
    parent_success_bonus: float = 0.0,
    selected_count: int = 0,
) -> tuple[float, dict[str, float]]:
    parsed = urlparse(url)
    source_parsed = urlparse(source_url)
    same_host = 1.0 if parsed.netloc.lower() == source_parsed.netloc.lower() else -3.0
    text = f"{parsed.path} {parsed.query} {anchor_text or ''}".strip()
    keyword_score = _keyword_score(text)
    map_bonus = 0.0
    repeated_bonus = 0.0
    if page_analysis is not None:
        map_bonus = 0.6 if page_analysis.has_map_widget else 0.0
        repeated_bonus = min(page_analysis.repeated_block_count, 4) * 0.15
    depth_penalty = depth * 0.45
    duplicate_penalty = selected_count * 0.8
    score_components = {
        "same_host": same_host,
        "keyword": keyword_score,
        "map_bonus": map_bonus,
        "repeated_bonus": repeated_bonus,
        "template_bonus": template_bonus,
        "parent_success_bonus": parent_success_bonus,
        "depth_penalty": -depth_penalty,
        "duplicate_penalty": -duplicate_penalty,
    }
    score = round(sum(score_components.values()), 4)
    return score, score_components


def discover_frontier_links(html: str, page_url: str, *, max_links: int = 40) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    discovered: list[dict[str, object]] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = anchor.get("href")
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
            continue
        canonical = canonicalize_url(href, page_url)
        if canonical in seen:
            continue
        seen.add(canonical)
        discovered.append(
            {
                "url": canonical,
                "anchor_text": anchor.get_text(" ", strip=True)[:180] or None,
            }
        )
        if len(discovered) >= max_links:
            break
    return discovered


def make_frontier_item(payload: dict[str, object]) -> FrontierItem:
    return FrontierItem(
        id=str(payload.get("id")) if payload.get("id") is not None else None,
        url=str(payload["url"]),
        canonical_url=str(payload.get("canonical_url") or payload["url"]),
        parent_url=str(payload.get("parent_url")) if payload.get("parent_url") is not None else None,
        depth=int(payload.get("depth", 0)),
        anchor_text=str(payload.get("anchor_text")) if payload.get("anchor_text") is not None else None,
        discovered_from=str(payload.get("discovered_from") or "seed"),
        state=str(payload.get("state") or "queued"),
        score_total=float(payload.get("score_total", 0.0)),
        score_components=dict(payload.get("score_components") or {}),
        selected_count=int(payload.get("selected_count", 0)),
    )
