from __future__ import annotations

from dataclasses import asdict, dataclass
from urllib.parse import urlparse
import re

from fratfinder_crawler.search import SearchClient, SearchResult

_BLOCKED_HOSTS = {
    "wikipedia.org",
    "www.wikipedia.org",
    "reddit.com",
    "www.reddit.com",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "linkedin.com",
    "www.linkedin.com",
    "x.com",
    "twitter.com",
    "stackoverflow.com",
    "stackexchange.com",
    "github.com",
    "medium.com",
    "quora.com",
    "wiktionary.org",
}

_DIRECTORY_MARKERS = (
    "chapter-directory",
    "chapters",
    "chapter",
    "directory",
    "find-a-chapter",
    "findachapter",
    "locations",
    "locator",
)

_FRATERNITY_ALIASES = {
    "phi-gamma-delta": ("fiji",),
}

_FRATERNITY_HOST_HINTS = {
    "phi-gamma-delta": ("phigam.org",),
}

_FRATERNITY_SOURCE_HINTS = {
    "phi-gamma-delta": "https://phigam.org/about/overview/our-chapters/",
}

_FRATERNITY_CONTEXT_MARKERS = (
    "fraternity",
    "chapter",
    "chapters",
    "greek life",
    "brotherhood",
    "interfraternity",
    "ifc",
)

_NON_ORG_CONTEXT_MARKERS = (
    "function",
    "programming",
    "software",
    "tutorial",
    "question",
    "forum",
    "how to",
    "travel",
    "vacation",
    "resort",
    "island",
    "hotel",
    "airline",
)


def _slugify(value: str) -> str:
    lowered = value.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", lowered)
    return slug.strip("-")


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _display_tokens(value: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]


def _root_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    return f"{parsed.scheme}://{parsed.netloc}"


def _contains_phrase(text: str, phrase: str) -> bool:
    normalized_text = _compact(text)
    normalized_phrase = _compact(phrase)
    return bool(normalized_phrase and normalized_phrase in normalized_text)


@dataclass(slots=True)
class DiscoveryCandidate:
    title: str
    url: str
    snippet: str
    provider: str
    rank: int
    score: float


@dataclass(slots=True)
class DiscoveryResult:
    fraternity_name: str
    fraternity_slug: str
    selected_url: str | None
    selected_confidence: float
    confidence_tier: str
    candidates: list[DiscoveryCandidate]

    def as_dict(self) -> dict:
        return {
            "fraternity_name": self.fraternity_name,
            "fraternity_slug": self.fraternity_slug,
            "selected_url": self.selected_url,
            "selected_confidence": self.selected_confidence,
            "confidence_tier": self.confidence_tier,
            "candidates": [asdict(candidate) for candidate in self.candidates],
        }


def _score_candidate(fraternity_name: str, fraternity_slug: str, result: SearchResult) -> float:
    score = 0.36
    lowered_title = (result.title or "").lower()
    lowered_snippet = (result.snippet or "").lower()
    parsed = urlparse(result.url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    combined = f"{lowered_title} {lowered_snippet} {path}"

    if _contains_phrase(lowered_title, fraternity_name):
        score += 0.2
    if _contains_phrase(lowered_snippet, fraternity_name):
        score += 0.1

    fraternity_compact = _compact(fraternity_name)
    if fraternity_compact and fraternity_compact in _compact(host):
        score += 0.16

    trusted_host_hints = _FRATERNITY_HOST_HINTS.get(fraternity_slug, ())
    if trusted_host_hints and any(hint in host for hint in trusted_host_hints):
        score += 0.35

    tokens = _display_tokens(fraternity_name)
    host_text = _compact(host)
    token_hits = sum(1 for token in tokens if token and token in host_text)
    if token_hits >= 2:
        score += 0.12
    elif token_hits == 1:
        score += 0.06

    if any(marker in path for marker in _DIRECTORY_MARKERS):
        score += 0.12

    if any(marker in lowered_title for marker in ("official", "international", "fraternity")):
        score += 0.08

    if any(marker in lowered_snippet for marker in ("official", "chapter", "directory", "fraternity")):
        score += 0.06

    context_hits = sum(1 for marker in _FRATERNITY_CONTEXT_MARKERS if marker in combined)
    if context_hits == 0:
        score -= 0.2
    elif context_hits >= 2:
        score += 0.08

    if any(marker in combined for marker in _NON_ORG_CONTEXT_MARKERS) and context_hits == 0:
        score -= 0.18

    if any(marker in combined for marker in ("travel", "vacation", "resort", "island")) and context_hits == 0:
        score -= 0.2

    if host in _BLOCKED_HOSTS:
        score -= 0.45

    score += max(0.0, 0.08 - (max(result.rank, 1) - 1) * 0.01)
    return max(0.0, min(0.99, score))


def _confidence_tier(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def discover_source(
    fraternity_name: str,
    search_client: SearchClient,
    *,
    max_candidates: int = 5,
) -> DiscoveryResult:
    name = fraternity_name.strip()
    slug = _slugify(name)

    if not name:
        return DiscoveryResult(
            fraternity_name=fraternity_name,
            fraternity_slug="",
            selected_url=None,
            selected_confidence=0.0,
            confidence_tier="low",
            candidates=[],
        )

    queries = [
        f'"{name}" national fraternity website',
        f'"{name}" chapter directory',
        f'"{name}" official fraternity',
        f'"{name}" find a chapter',
    ]
    host_hints = _FRATERNITY_HOST_HINTS.get(slug, ())
    for hint in host_hints:
        queries.extend(
            [
                f'"{name}" "{hint}" fraternity',
                f'"{hint}" chapter directory fraternity',
            ]
        )
    for alias in _FRATERNITY_ALIASES.get(slug, ()):
        queries.extend(
            [
                f'"{alias}" fraternity national website',
                f'"{alias}" chapter directory',
            ]
        )

    raw_results: list[SearchResult] = []
    for query in queries:
        raw_results.extend(search_client.search(query, max_results=max_candidates))

    deduped: dict[str, SearchResult] = {}
    for result in raw_results:
        key = _root_url(result.url)
        if key not in deduped or result.rank < deduped[key].rank:
            deduped[key] = result

    ranked: list[DiscoveryCandidate] = []
    for result in deduped.values():
        ranked.append(
            DiscoveryCandidate(
                title=result.title,
                url=result.url,
                snippet=result.snippet,
                provider=result.provider,
                rank=result.rank,
                score=_score_candidate(name, slug, result),
            )
        )

    ranked.sort(key=lambda item: item.score, reverse=True)
    top = ranked[:max_candidates]
    top_score = top[0].score if top else 0.0
    selected = top[0] if top and top_score >= 0.6 else None
    selected_score = selected.score if selected else 0.0

    if not selected:
        hinted_source = _FRATERNITY_SOURCE_HINTS.get(slug)
        if hinted_source:
            hinted_candidate = DiscoveryCandidate(
                title=f"{name} Official Chapter Directory",
                url=hinted_source,
                snippet="Curated official source hint for fraternity chapter discovery.",
                provider="curated_hint",
                rank=1,
                score=0.85,
            )
            top = [hinted_candidate, *top][:max_candidates]
            selected = hinted_candidate
            selected_score = hinted_candidate.score

    return DiscoveryResult(
        fraternity_name=name,
        fraternity_slug=slug,
        selected_url=selected.url if selected else None,
        selected_confidence=selected_score,
        confidence_tier=_confidence_tier(selected_score),
        candidates=top,
    )
