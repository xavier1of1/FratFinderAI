from __future__ import annotations

from collections import Counter
from urllib.parse import urlparse

from fratfinder_crawler.models import PageAnalysis


def host_family(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    parts = [part for part in host.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def compute_template_signature(url: str, analysis: PageAnalysis) -> str:
    parsed = urlparse(url)
    path_tokens = [token for token in parsed.path.lower().split("/") if token]
    role = analysis.probable_page_role or "unknown"
    table_bucket = min(analysis.table_count, 3)
    repeated_bucket = min(analysis.repeated_block_count, 4)
    route_class = _route_class_from_tokens(path_tokens)
    flags = [
        "json" if analysis.has_script_json or analysis.has_json_ld else "nojson",
        "map" if analysis.has_map_widget else "nomap",
        "page" if analysis.has_pagination else "single",
    ]
    return f"{host_family(url)}|{role}|t{table_bucket}|r{repeated_bucket}|{route_class}|{'-'.join(flags)}"


def _route_class_from_tokens(path_tokens: list[str]) -> str:
    if not path_tokens:
        return "root"

    family_keywords = {
        "chapter-directory": ("chapter", "chapters", "directory", "find", "where-we-are", "locations"),
        "contact": ("contact", "contact-us", "connect"),
        "join": ("join", "recruit", "membership", "rush"),
        "about": ("about", "history", "mission", "values", "leadership", "team"),
        "events-news": ("event", "events", "news", "blog", "stories"),
        "alumni": ("alumni", "graduates"),
        "giving": ("foundation", "donate", "giving"),
    }

    normalized = [token.replace("_", "-") for token in path_tokens]
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
        return "generic"
    return best_family
