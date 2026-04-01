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
    path_tokens = [token for token in parsed.path.lower().split("/") if token][:3]
    token_counter = Counter(path_tokens)
    token_fragment = "-".join(sorted(token_counter.keys())[:3]) or "root"
    role = analysis.probable_page_role or "unknown"
    table_bucket = min(analysis.table_count, 3)
    repeated_bucket = min(analysis.repeated_block_count, 4)
    flags = [
        "json" if analysis.has_script_json or analysis.has_json_ld else "nojson",
        "map" if analysis.has_map_widget else "nomap",
        "page" if analysis.has_pagination else "single",
    ]
    return f"{host_family(url)}|{role}|t{table_bucket}|r{repeated_bucket}|{token_fragment}|{'-'.join(flags)}"
