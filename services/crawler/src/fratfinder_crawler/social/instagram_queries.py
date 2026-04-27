from __future__ import annotations

from urllib.parse import urlparse

from fratfinder_crawler.social.instagram_identity import ChapterInstagramIdentity


def build_instagram_search_queries(
    *,
    identity: ChapterInstagramIdentity,
    school_domains: list[str] | None = None,
    chapter_website_url: str | None = None,
    max_queries: int = 5,
) -> list[str]:
    school_domains = [domain for domain in (school_domains or []) if domain]
    chapter_website_host = (urlparse(chapter_website_url or "").netloc or "").lower()
    fraternity = next(iter(identity.fraternity_full_names), "")
    fraternity_alias = next(iter(identity.fraternity_aliases), fraternity)
    school = next(iter(identity.school_full_names), "")
    chapter = next(iter(identity.chapter_names), "")
    handle_hypotheses = [
        *identity.fraternity_compact_tokens,
        *identity.school_compact_tokens,
        *identity.chapter_compact_tokens,
    ]
    query_parts: list[str] = []
    for domain in school_domains:
        query_parts.extend(
            [
                f'site:{domain} "{fraternity}" Instagram',
                f'site:{domain} "{fraternity_alias}" "Chapter Instagram"',
                f'site:{domain} "{fraternity}" "social media"',
            ]
        )
    if chapter_website_host:
        query_parts.extend(
            [
                f'site:{chapter_website_host} Instagram',
                f'site:{chapter_website_host} "{fraternity}" "{school}"',
            ]
        )
    if school:
        query_parts.extend(
            [
                f'site:instagram.com "{fraternity}" "{school}"',
                f'"{fraternity}" "{school}" Instagram',
            ]
        )
    if chapter:
        query_parts.append(f'"{fraternity}" "{chapter}" Instagram')
    for handle in handle_hypotheses:
        if len(handle) >= 4:
            query_parts.extend(
                [
                    f'site:instagram.com "{handle}" "{fraternity}"',
                    f'site:instagram.com "{handle}" "{school}"',
                ]
            )
    deduped = list(dict.fromkeys(query for query in query_parts if query.strip()))
    return deduped[: max(1, max_queries)]
