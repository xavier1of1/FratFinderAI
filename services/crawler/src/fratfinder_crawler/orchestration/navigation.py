from __future__ import annotations

from collections import defaultdict
from typing import Any
from urllib.parse import urljoin, urlparse
import re

from bs4 import BeautifulSoup

from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.analysis import score_chapter_link
from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.models import ChapterStub, EmbeddedDataResult, PageAnalysis, SourceClassification

_GENERIC_LINK_LABELS = {
    "go to site",
    "chapter website",
    "view chapter",
    "learn more",
    "details",
}

_BLOCKED_HOST_FRAGMENTS = ("facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
_INSTAGRAM_URL_PATTERN = re.compile(r"https?://(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?", re.IGNORECASE)
_INSTAGRAM_HANDLE_PATTERN = re.compile(r"@([A-Za-z0-9_.]{3,30})")


def detect_chapter_index_mode(
    html: str,
    page_analysis: PageAnalysis,
    classification: SourceClassification,
    embedded_data: EmbeddedDataResult,
) -> tuple[str, float, str]:
    lowered_html = html.lower()
    if any(marker in lowered_html for marker in ("member portal", "member login", "sign in to view", "directory login")):
        return "member_portal_gated", 0.9, "member_portal_keywords"
    if classification.page_type == "locator_map" or (embedded_data.found and embedded_data.api_url):
        return "map_or_api_locator", 0.9, "locator_or_api_hint"
    if page_analysis.table_count > 0 or page_analysis.repeated_block_count >= 2:
        return "direct_chapter_list", 0.85, "table_or_repeated_blocks"
    if page_analysis.probable_page_role == "directory" and page_analysis.link_count >= 12:
        return "internal_detail_pages", 0.7, "directory_with_many_links"
    if page_analysis.probable_page_role in {"directory", "index"}:
        return "mixed", 0.55, "mixed_directory_indicators"
    return "mixed", 0.35, "fallback_mixed"


def extract_chapter_stubs(
    *,
    registry: AdapterRegistry,
    html: str,
    source_url: str,
    mode: str,
    embedded_data: EmbeddedDataResult,
    http_client: Any,
) -> list[ChapterStub]:
    stubs: list[ChapterStub] = []
    strategies: list[str] = ["repeated_block", "table"]
    if embedded_data.found and embedded_data.api_url:
        strategies.insert(0, "locator_api")
    if embedded_data.found and embedded_data.data_type in {"json_ld", "script_json"}:
        strategies.insert(0, "script_json")

    seen_strategies: set[str] = set()
    for strategy in strategies:
        if strategy in seen_strategies:
            continue
        seen_strategies.add(strategy)
        adapter = registry.get(strategy)
        if adapter is None:
            continue
        try:
            adapter_stubs = adapter.parse_stubs(
                html,
                source_url,
                api_url=embedded_data.api_url,
                http_client=http_client,
            )
        except Exception:
            adapter_stubs = []
        stubs.extend(adapter_stubs)

    stubs.extend(_extract_anchor_stubs(html, source_url))
    stubs.extend(_extract_wix_chapter_link_stubs(html, source_url))
    stubs.extend(_extract_elementor_chaptername_stubs(html, source_url))
    stubs.extend(_extract_chapter_roll_text_stubs(html, source_url))

    return _dedupe_stubs(stubs)


def follow_chapter_detail_or_outbound(
    *,
    stubs: list[ChapterStub],
    source_url: str,
    http_client: Any,
    max_hops_per_stub: int,
    max_pages_per_run: int,
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, int]]:
    pages_by_stub: dict[str, list[tuple[str, str]]] = defaultdict(list)
    fetched_pages = 0
    skipped_by_domain = 0
    errors = 0
    cache_hits = 0

    national_host = (urlparse(source_url).netloc or "").lower()
    chapter_hosts = {
        (urlparse(stub.outbound_chapter_url_candidate or "").netloc or "").lower()
        for stub in stubs
        if stub.outbound_chapter_url_candidate
    }
    allowed_hosts = {host for host in {national_host, *chapter_hosts} if host}
    fetched_cache: dict[str, str] = {}

    for stub in stubs:
        if fetched_pages >= max_pages_per_run:
            break
        key = _stub_key(stub)
        candidates: list[str] = []
        if stub.detail_url:
            candidates.append(stub.detail_url)
        if stub.outbound_chapter_url_candidate and stub.outbound_chapter_url_candidate not in candidates:
            candidates.append(stub.outbound_chapter_url_candidate)

        for candidate_url in candidates[: max(1, max_hops_per_stub)]:
            if fetched_pages >= max_pages_per_run:
                break
            host = (urlparse(candidate_url).netloc or "").lower()
            if not _host_allowed(host, allowed_hosts):
                skipped_by_domain += 1
                continue
            cache_key = _normalize_fetch_url(candidate_url)
            if cache_key in fetched_cache:
                pages_by_stub[key].append((candidate_url, fetched_cache[cache_key]))
                cache_hits += 1
                continue
            try:
                html = http_client.get(candidate_url)
            except Exception:
                errors += 1
                continue
            fetched_cache[cache_key] = html
            pages_by_stub[key].append((candidate_url, html))
            fetched_pages += 1

    return pages_by_stub, {
        "fetched_pages": fetched_pages,
        "skipped_by_domain": skipped_by_domain,
        "errors": errors,
        "cache_hits": cache_hits,
    }


def extract_contacts_from_chapter_site(
    stubs: list[ChapterStub],
    pages_by_stub: dict[str, list[tuple[str, str]]],
) -> dict[str, dict[str, str]]:
    hints: dict[str, dict[str, str]] = {}
    for stub in stubs:
        key = _stub_key(stub)
        prefilled: dict[str, str] = {}
        outbound = (stub.outbound_chapter_url_candidate or "").strip()
        prefilled_email = sanitize_as_email(outbound)
        prefilled_instagram = sanitize_as_instagram(outbound)
        prefilled_website = sanitize_as_website(outbound, base_url=stub.detail_url)
        if prefilled_email:
            prefilled["email"] = prefilled_email
        elif prefilled_instagram:
            prefilled["instagram_url"] = _ensure_instagram_trailing_slash(prefilled_instagram)
        elif prefilled_website and not any(blocked in prefilled_website.lower() for blocked in _BLOCKED_HOST_FRAGMENTS):
            prefilled["website_url"] = prefilled_website

        pages = pages_by_stub.get(key, [])
        candidates: dict[str, str] = dict(prefilled)
        if not pages and candidates:
            hints[key] = candidates
            continue
        elif not pages:
            continue
        for url, html in pages:
            if _should_skip_shared_listing_contact_extraction(url, stub):
                continue
            text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
            email_match = _EMAIL_PATTERN.search(text)
            if email_match and "email" not in candidates:
                sanitized_email = sanitize_as_email(email_match.group(0))
                if sanitized_email:
                    candidates["email"] = sanitized_email

            instagram_match = _INSTAGRAM_URL_PATTERN.search(text)
            if instagram_match and "instagram_url" not in candidates:
                sanitized_instagram = sanitize_as_instagram(f"https://www.instagram.com/{instagram_match.group(1)}/")
                if sanitized_instagram:
                    candidates["instagram_url"] = _ensure_instagram_trailing_slash(sanitized_instagram)
            elif "instagram_url" not in candidates:
                handle_match = _INSTAGRAM_HANDLE_PATTERN.search(text)
                if handle_match and "instagram" in text.lower():
                    sanitized_instagram = sanitize_as_instagram(f"https://www.instagram.com/{handle_match.group(1)}/")
                    if sanitized_instagram:
                        candidates["instagram_url"] = _ensure_instagram_trailing_slash(sanitized_instagram)

            host = (urlparse(url).netloc or "").lower()
            if (
                "website_url" not in candidates
                and host
                and not any(blocked in host for blocked in _BLOCKED_HOST_FRAGMENTS)
            ):
                sanitized_website = sanitize_as_website(url, base_url=stub.detail_url)
                if sanitized_website:
                    candidates["website_url"] = sanitized_website

        if candidates:
            hints[key] = candidates
    return hints


def _extract_anchor_stubs(html: str, source_url: str) -> list[ChapterStub]:
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[ChapterStub] = []
    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        label = anchor.get_text(" ", strip=True)
        if not label:
            continue
        context = anchor.parent.get_text(" ", strip=True) if anchor.parent else label
        score = score_chapter_link(label, href, context)
        if score.score < 0.65:
            continue

        chapter_name = label
        if chapter_name.strip().lower() in _GENERIC_LINK_LABELS:
            heading = anchor.find_previous(["h1", "h2", "h3", "h4"])
            chapter_name = heading.get_text(" ", strip=True) if heading else ""
        if len(chapter_name.strip()) < 3:
            continue

        university_name = _extract_university_name(context)
        absolute_url = urljoin(source_url, href)
        stubs.append(
            ChapterStub(
                chapter_name=chapter_name.strip(),
                university_name=university_name,
                detail_url=absolute_url,
                outbound_chapter_url_candidate=absolute_url,
                confidence=score.score,
                provenance="anchor_list",
            )
        )
    return stubs


def _extract_wix_chapter_link_stubs(html: str, source_url: str) -> list[ChapterStub]:
    if not _looks_like_wix_page(html, source_url):
        return []
    soup = BeautifulSoup(html, "html.parser")
    root_host = (urlparse(source_url).netloc or "").lower()
    stubs: list[ChapterStub] = []
    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        absolute_url = urljoin(source_url, href)
        parsed = urlparse(absolute_url)
        if not parsed.netloc or root_host not in parsed.netloc.lower():
            continue
        path = (parsed.path or "").lower()
        if "/chapters/" not in path:
            continue
        if path.rstrip("/") == "/chapters":
            continue
        container = anchor.find_parent(["article", "section", "div", "li"]) or anchor
        context = container.get_text(" ", strip=True)
        university_name = _extract_university_name(context)
        chapter_name = university_name
        if not chapter_name:
            slug_token = path.split("/chapters/")[-1].strip("/").replace("-", " ")
            chapter_name = " ".join(part.capitalize() for part in slug_token.split()) or anchor.get_text(" ", strip=True)
        if not chapter_name or len(chapter_name.strip()) < 3:
            continue
        stubs.append(
            ChapterStub(
                chapter_name=chapter_name.strip(),
                university_name=university_name,
                detail_url=absolute_url,
                outbound_chapter_url_candidate=absolute_url,
                confidence=0.78,
                provenance="wix_chapter_link",
            )
        )
    return stubs


def _looks_like_wix_page(html: str, source_url: str) -> bool:
    host = (urlparse(source_url).netloc or "").lower()
    lowered_html = html.lower()
    return (
        "wixsite.com" in host
        or "wix.com" in host
        or "wix-image" in lowered_html
        or "wix-code" in lowered_html
        or "wix-data" in lowered_html
    )


def _extract_elementor_chaptername_stubs(html: str, source_url: str) -> list[ChapterStub]:
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[ChapterStub] = []
    for node in soup.select(".chaptername"):
        school = node.get_text(" ", strip=True)
        if not school or len(school) < 5:
            continue
        container = node.find_parent(["section", "article", "div"]) or node
        links = [urljoin(source_url, a.get("href")) for a in container.select("a[href]") if a.get("href")]
        mailto_candidate = next((link for link in links if link.lower().startswith("mailto:")), None)
        website_candidate = next(
            (
                link
                for link in links
                if link.startswith("http")
                and not any(blocked in link.lower() for blocked in _BLOCKED_HOST_FRAGMENTS)
            ),
            None,
        )
        outbound_candidate = mailto_candidate or website_candidate
        stubs.append(
            ChapterStub(
                chapter_name=school,
                university_name=school,
                detail_url=source_url,
                outbound_chapter_url_candidate=outbound_candidate,
                confidence=0.82,
                provenance="elementor_chaptername",
            )
        )
    return stubs


def _extract_chapter_roll_text_stubs(html: str, source_url: str) -> list[ChapterStub]:
    soup = BeautifulSoup(html, "html.parser")
    stubs: list[ChapterStub] = []
    pattern = re.compile(
        r"(?P<chapter>[A-Z][A-Z\s\-']+?)\s+CHAPTER\s+(?P<school>[A-Z][A-Z\s\-'.&]+(?:UNIVERSITY|COLLEGE|INSTITUTE|SCHOOL))"
    )
    seen: set[tuple[str, str]] = set()
    for node in soup.select("h1, h2, h3, h4, p, div, span"):
        text = " ".join(node.get_text(" ", strip=True).split())
        if len(text) < 20 or " CHAPTER " not in text.upper():
            continue
        normalized = text.upper()
        match = pattern.search(normalized)
        if not match:
            continue
        chapter_name = " ".join(match.group("chapter").title().split())
        school_name = " ".join(match.group("school").title().split())
        key = (chapter_name.lower(), school_name.lower())
        if key in seen:
            continue
        seen.add(key)
        stubs.append(
            ChapterStub(
                chapter_name=chapter_name,
                university_name=school_name,
                detail_url=source_url,
                outbound_chapter_url_candidate=None,
                confidence=0.74,
                provenance="chapter_roll_text",
            )
        )
    return stubs


def _extract_university_name(context: str) -> str | None:
    compact = " ".join((context or "").split())
    match = re.search(
        r"([A-Z][A-Za-z&.'\-]+\s(?:University|College|Institute|State University|School|Campus))",
        compact,
    )
    return match.group(1) if match else None


def _stub_key(stub: ChapterStub) -> str:
    name = re.sub(r"[^a-z0-9]+", "-", stub.chapter_name.lower()).strip("-")
    school = re.sub(r"[^a-z0-9]+", "-", (stub.university_name or "").lower()).strip("-")
    return f"{name}:{school}"


def _dedupe_stubs(stubs: list[ChapterStub]) -> list[ChapterStub]:
    deduped: dict[str, ChapterStub] = {}
    for stub in stubs:
        key = _stub_key(stub)
        current = deduped.get(key)
        if current is None or stub.confidence > current.confidence:
            deduped[key] = stub
    return sorted(deduped.values(), key=lambda item: item.confidence, reverse=True)


def _host_allowed(host: str, allowed_hosts: set[str]) -> bool:
    if not host:
        return False
    for allowed in allowed_hosts:
        if host == allowed or host.endswith(f".{allowed}"):
            return True
    return False


def _normalize_fetch_url(url: str) -> str:
    parsed = urlparse(url)
    clean = parsed._replace(fragment="")
    return clean.geturl()


def _normalize_instagram_url(url: str) -> str:
    match = _INSTAGRAM_URL_PATTERN.search(url)
    if not match:
        return url
    return f"https://www.instagram.com/{match.group(1)}/"


def _ensure_instagram_trailing_slash(url: str) -> str:
    return f"{url.rstrip('/')}/"


def _should_skip_shared_listing_contact_extraction(url: str, stub: ChapterStub) -> bool:
    if stub.provenance not in {"elementor_chaptername", "chapter_roll_text"}:
        return False
    parsed = urlparse(url)
    path = (parsed.path or "").lower().rstrip("/")
    return path.endswith("/join-a-chapter") or path.endswith("/join-a-chapter/") or path.endswith("/chapter-roll")
