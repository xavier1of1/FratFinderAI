from __future__ import annotations

from collections import defaultdict
from typing import Any
from urllib.parse import urljoin, urlparse
import re

from bs4 import BeautifulSoup

from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.analysis import score_chapter_link
from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.models import ChapterCandidate, ChapterIdentity, ChapterStub, ChapterTarget, EmbeddedDataResult, ExtractedChapter, PageAnalysis, SourceClassification
from fratfinder_crawler.normalization import classify_chapter_validity

_GENERIC_LINK_LABELS = {
    "go to site",
    "chapter website",
    "view chapter",
    "learn more",
    "details",
}

_BLOCKED_HOST_FRAGMENTS = ("facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com")
_SOCIAL_HOST_FRAGMENTS = ("facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com", "youtube.com", "tiktok.com")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
_INSTAGRAM_URL_PATTERN = re.compile(r"https?://(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?", re.IGNORECASE)
_INSTAGRAM_HANDLE_PATTERN = re.compile(r"@([A-Za-z0-9_.]{3,30})")
_MAP_CONFIG_LINK_PATTERN = re.compile(
    r"'hover'\s*:\s*'<p>(?P<label>[^<]+)</p>'\s*,\s*'url'\s*:\s*'(?P<url>https?://[^']+)'",
    re.IGNORECASE,
)
_CHAPTER_ROLL_BLOCKED_MARKERS = (
    "org quick links",
    "quick links",
    "donate",
    "careers",
    "privacy statement",
    "contact us",
    "member development",
    "leadership institute",
    "officer training academy",
    "request a program",
    "our history",
    "scholarships",
    "alumni",
    "foundation",
    "policies",
    "history",
    "system",
)

_IRRELEVANT_CHAPTER_TARGET_MARKERS = (
    "/start-a-chapter",
    "start a chapter",
    "/careers",
    "/career",
    "/about-us",
    "/about/",
    "/history",
    "/notable",
    "/news",
    "/blog",
    "/events",
    "/event",
    "/foundation",
    "/donate",
    "/scholarship",
    "/alumni",
    "/leadership",
    "/staff",
    "/privacy",
    "/terms",
    "/contact-us",
)



def detect_chapter_index_mode(
    html: str,
    page_analysis: PageAnalysis,
    classification: SourceClassification,
    embedded_data: EmbeddedDataResult,
    source_metadata: dict[str, Any] | None = None,
) -> tuple[str, float, str]:
    hints = ((source_metadata or {}).get("extractionHints") or {})
    if isinstance(hints, dict):
        override_mode = hints.get("chapterIndexMode")
        if isinstance(override_mode, str) and override_mode:
            return override_mode, 0.98, "source_metadata_override"

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
    source_metadata: dict[str, Any] | None = None,
) -> list[ChapterStub]:
    stubs: list[ChapterStub] = []
    hints = ((source_metadata or {}).get("extractionHints") or {})
    configured_stub_strategies = [value for value in (hints.get("stubStrategies") or []) if isinstance(value, str) and value] if isinstance(hints, dict) else []
    strategies: list[str] = configured_stub_strategies or ["repeated_block", "table"]
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
                source_metadata=source_metadata,
            )
        except Exception:
            adapter_stubs = []
        stubs.extend(adapter_stubs)

    stubs.extend(_extract_anchor_stubs(html, source_url))
    stubs.extend(_extract_map_config_state_stubs(html, source_url))
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
    follow_external_chapter_sites: bool = True,
    allow_institutional_follow: bool = True,
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, int]]:
    pages_by_stub: dict[str, list[tuple[str, str]]] = defaultdict(list)
    fetched_pages = 0
    skipped_by_domain = 0
    errors = 0
    cache_hits = 0
    followed_by_target_type: dict[str, int] = defaultdict(int)
    skipped_by_target_type: dict[str, int] = defaultdict(int)
    target_decisions: list[dict[str, str | bool | None]] = []

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
            target = classify_chapter_target(source_url=source_url, candidate_url=candidate_url)
            follow_allowed = target.follow_allowed
            rejection_reason = target.rejection_reason
            if (
                target.target_type == "institutional_page"
                and allow_institutional_follow
                and _stub_identity_complete(stub)
            ):
                follow_allowed = False
                rejection_reason = "institutional_completion_not_needed"
            target_decisions.append(
                {
                    "url": target.url,
                    "targetType": target.target_type,
                    "sourceClass": target.source_class,
                    "followAllowed": follow_allowed,
                    "rejectionReason": rejection_reason,
                }
            )
            if target.target_type == "institutional_page" and not allow_institutional_follow:
                skipped_by_target_type[target.target_type] += 1
                continue
            if target.target_type == "chapter_owned_site" and not follow_external_chapter_sites:
                skipped_by_target_type[target.target_type] += 1
                continue
            if not follow_allowed:
                skipped_by_target_type[target.target_type] += 1
                skipped_by_domain += 1
                continue
            cache_key = _normalize_fetch_url(candidate_url)
            if cache_key in fetched_cache:
                pages_by_stub[key].append((candidate_url, fetched_cache[cache_key]))
                cache_hits += 1
                followed_by_target_type[target.target_type] += 1
                continue
            try:
                html = http_client.get(candidate_url)
            except Exception:
                errors += 1
                continue
            fetched_cache[cache_key] = html
            pages_by_stub[key].append((candidate_url, html))
            fetched_pages += 1
            followed_by_target_type[target.target_type] += 1

    return pages_by_stub, {
        "fetched_pages": fetched_pages,
        "skipped_by_domain": skipped_by_domain,
        "errors": errors,
        "cache_hits": cache_hits,
        "followed_by_target_type": dict(followed_by_target_type),
        "skipped_by_target_type": dict(skipped_by_target_type),
        "target_decisions": target_decisions,
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
        if _looks_like_irrelevant_chapter_target(absolute_url, label, context):
            continue
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


def _extract_map_config_state_stubs(html: str, source_url: str) -> list[ChapterStub]:
    stubs: list[ChapterStub] = []
    seen: set[str] = set()
    for match in _MAP_CONFIG_LINK_PATTERN.finditer(html):
        label = match.group("label").strip()
        href = match.group("url").strip()
        if not label or not href:
            continue
        absolute_url = urljoin(source_url, href)
        if absolute_url in seen:
            continue
        seen.add(absolute_url)
        stubs.append(
            ChapterStub(
                chapter_name=label.title(),
                university_name=None,
                detail_url=absolute_url,
                outbound_chapter_url_candidate=absolute_url,
                confidence=0.78,
                provenance="map_config",
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
        if len(text) > 180:
            continue
        normalized = text.upper()
        if any(marker.upper() in normalized for marker in _CHAPTER_ROLL_BLOCKED_MARKERS):
            continue
        match = pattern.search(normalized)
        if not match:
            continue
        chapter_name = " ".join(match.group("chapter").title().split())
        school_name = " ".join(match.group("school").title().split())
        if len(chapter_name) > 80 or len(school_name) > 120:
            continue
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


def classify_chapter_target(*, source_url: str, candidate_url: str | None) -> ChapterTarget:
    normalized_url = (candidate_url or "").strip() or None
    source_host = (urlparse(source_url).netloc or "").lower()
    parsed = urlparse(normalized_url or "")
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    lowered = (normalized_url or "").lower()
    if normalized_url is None or not host:
        return ChapterTarget(url=normalized_url, target_type="unknown", source_class="unknown", follow_allowed=False, rejection_reason="missing_target_url", host=host or None)
    if any(fragment in host for fragment in _SOCIAL_HOST_FRAGMENTS):
        return ChapterTarget(url=normalized_url, target_type="social_page", source_class="broader_web", follow_allowed=False, rejection_reason="social_page", host=host)
    if host == source_host or host.endswith(f".{source_host}"):
        target_type = "national_listing" if normalized_url.rstrip("/") == source_url.rstrip("/") else "national_detail"
        return ChapterTarget(url=normalized_url, target_type=target_type, source_class="national", follow_allowed=True, host=host)
    if host.endswith(".edu") or ".edu." in host:
        if "/~" in path or path.startswith("/users/") or "/people/" in path:
            return ChapterTarget(
                url=normalized_url,
                target_type="institutional_page",
                source_class="institutional",
                follow_allowed=False,
                rejection_reason="external_target_timeout_risk",
                host=host,
            )
        return ChapterTarget(url=normalized_url, target_type="institutional_page", source_class="institutional", follow_allowed=True, host=host)
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return ChapterTarget(url=normalized_url, target_type="chapter_owned_site", source_class="wider_web", follow_allowed=False, rejection_reason="chapter_site_only", host=host)
    return ChapterTarget(url=normalized_url, target_type="unknown", source_class="unknown", follow_allowed=False, rejection_reason="unknown_target_type", host=host)


def build_chapter_candidates(*, stubs: list[ChapterStub], source_url: str) -> list[ChapterCandidate]:
    candidates: list[ChapterCandidate] = []
    for stub in stubs:
        targets: list[ChapterTarget] = []
        for candidate_url in (stub.detail_url, stub.outbound_chapter_url_candidate):
            if not candidate_url:
                continue
            target = classify_chapter_target(source_url=source_url, candidate_url=candidate_url)
            if not any(existing.url == target.url for existing in targets):
                targets.append(target)
        source_classes = {target.source_class for target in targets if target.source_class != "unknown"}
        source_class = "national" if "national" in source_classes else next(iter(source_classes), "national")
        identity = ChapterIdentity(
            chapter_name=stub.chapter_name,
            university_name=stub.university_name,
            source_class=source_class,
            chapter_intent_signals=sum(1 for value in (stub.chapter_name, stub.university_name) if value),
            identity_complete=bool((stub.chapter_name or "").strip()) and bool((stub.university_name or "").strip()),
        )
        validity = classify_chapter_validity(
            ExtractedChapter(
                name=stub.chapter_name,
                university_name=stub.university_name,
                source_url=stub.detail_url or stub.outbound_chapter_url_candidate or source_url,
                source_confidence=float(stub.confidence or 0.0),
            ),
            source_class=source_class,
            provenance=stub.provenance,
            target_type=targets[0].target_type if targets else None,
        )
        candidates.append(
            ChapterCandidate(
                chapter_name=stub.chapter_name,
                university_name=stub.university_name,
                confidence=float(stub.confidence or 0.0),
                provenance=stub.provenance,
                source_class=source_class,
                identity=identity,
                targets=targets,
                validity_class=validity.validity_class,
                invalid_reason=validity.invalid_reason,
                repair_reason=validity.repair_reason,
                semantic_signals=validity.semantic_signals,
            )
        )
    return candidates


def _dedupe_stubs(stubs: list[ChapterStub]) -> list[ChapterStub]:
    deduped: dict[str, ChapterStub] = {}
    for stub in stubs:
        key = _stub_key(stub)
        current = deduped.get(key)
        if current is None or stub.confidence > current.confidence:
            deduped[key] = stub
    return sorted(deduped.values(), key=lambda item: item.confidence, reverse=True)


def _stub_identity_complete(stub: ChapterStub) -> bool:
    return bool((stub.chapter_name or "").strip()) and bool((stub.university_name or "").strip())


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


def _looks_like_irrelevant_chapter_target(url: str, label: str, context: str) -> bool:
    lowered = " ".join(part for part in (url, label, context) if part).lower()
    return any(marker in lowered for marker in _IRRELEVANT_CHAPTER_TARGET_MARKERS)


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



