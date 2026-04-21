from __future__ import annotations


def build_campus_status_queries(*, school_name: str, school_domain: str, fraternity_aliases: list[str] | None = None) -> list[str]:
    aliases = fraternity_aliases or []
    queries = [
        f'site:{school_domain} "fraternity and sorority life" "recognized chapters"',
        f'site:{school_domain} "greek life" "chapter status"',
        f'site:{school_domain} "fraternity" "suspended chapters"',
        f'site:{school_domain} "unrecognized organizations" fraternity',
        f'site:{school_domain} "closed chapters" fraternity',
        f'site:{school_domain} "student organizations" fraternity',
        f'site:{school_domain} "hazing transparency report" fraternity',
    ]
    for alias in aliases[:4]:
        queries.extend(
            [
                f'site:{school_domain} "{alias}" "recognized"',
                f'site:{school_domain} "{alias}" "suspended"',
                f'site:{school_domain} "{alias}" "lost recognition"',
                f'site:{school_domain} "{alias}" "unrecognized"',
            ]
        )
    seen: set[str] = set()
    deduped: list[str] = []
    for query in queries:
        normalized = query.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped
