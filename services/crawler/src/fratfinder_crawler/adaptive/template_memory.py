from __future__ import annotations

from collections import Counter
from urllib.parse import urlparse

from fratfinder_crawler.models import PageAnalysis


US_STATE_TOKENS = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware",
    "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri",
    "montana", "nebraska", "nevada", "new-hampshire", "new-jersey", "new-mexico", "new-york",
    "north-carolina", "north-dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode-island",
    "south-carolina", "south-dakota", "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west-virginia", "wisconsin", "wyoming", "dc",
}


def host_family(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    parts = [part for part in host.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def compute_template_signature(url: str, analysis: PageAnalysis) -> str:
    return f"{host_family(url)}|{compute_structural_template_signature(url, analysis)}"


def compute_structural_template_signature(url: str, analysis: PageAnalysis) -> str:
    parsed = urlparse(url)
    path_tokens = [token for token in parsed.path.lower().split("/") if token]
    role = analysis.probable_page_role or "unknown"
    table_bucket = min(analysis.table_count, 3)
    repeated_bucket = min(analysis.repeated_block_count, 4)
    link_bucket = min(max(analysis.link_count // 10, 0), 5)
    route_class = _route_class_from_tokens(path_tokens)
    flags = [
        "json" if analysis.has_script_json or analysis.has_json_ld else "nojson",
        "map" if analysis.has_map_widget else "nomap",
        "page" if analysis.has_pagination else "single",
    ]
    return f"{role}|t{table_bucket}|r{repeated_bucket}|l{link_bucket}|{route_class}|{'-'.join(flags)}"


def to_structural_signature(template_signature: str) -> str:
    if "|" not in template_signature:
        return template_signature
    first, rest = template_signature.split("|", 1)
    if "." in first or first == "localhost":
        return rest
    return template_signature


def _normalize_token(token: str) -> str:
    clean = token.replace("_", "-")
    if clean in US_STATE_TOKENS:
        return "state"
    if clean.isdigit():
        return "num"
    if len(clean) > 20:
        return clean[:20]
    return clean


def _route_class_from_tokens(path_tokens: list[str]) -> str:
    if not path_tokens:
        return "root"

    family_keywords = {
        "chapter-directory": ("chapter", "chapters", "directory", "find", "where-we-are", "locations", "locator"),
        "chapter-detail": ("school", "campus", "colony", "chapter-detail", "brothers"),
        "contact": ("contact", "contact-us", "connect"),
        "join": ("join", "recruit", "membership", "rush"),
        "about": ("about", "history", "mission", "values", "leadership", "team"),
        "events-news": ("event", "events", "news", "blog", "stories"),
        "alumni": ("alumni", "graduates"),
        "giving": ("foundation", "donate", "giving"),
    }

    normalized = [_normalize_token(token) for token in path_tokens]
    token_counter = Counter(normalized)

    best_family = "generic"
    best_score = 0
    for family, keywords in family_keywords.items():
        score = 0
        for token, count in token_counter.items():
            if any(keyword in token for keyword in keywords):
                score += count
        if score > best_score:
            best_score = score
            best_family = family

    if best_score <= 0:
        unique_shape = "-".join(normalized[:3]) if normalized else "generic"
        return f"generic:{unique_shape}"
    return best_family