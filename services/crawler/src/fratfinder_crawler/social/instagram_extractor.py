from __future__ import annotations

import json
import re
from typing import Iterable

from bs4 import BeautifulSoup

from fratfinder_crawler.social.instagram_models import InstagramCandidate, InstagramSourceType
from fratfinder_crawler.social.instagram_normalizer import canonicalize_instagram_profile, extract_instagram_handle, is_instagram_profile_url


_INSTAGRAM_URL_RE = re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_./-]+", re.IGNORECASE)
_INSTAGRAM_HANDLE_HINT_RE = re.compile(r"(?:instagram|insta|ig)[^@A-Za-z0-9]{0,12}@([A-Za-z0-9_.]{3,30})", re.IGNORECASE)


def _looks_like_instagram_link(value: str | None) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    return "instagram.com/" in lowered or lowered.startswith("//instagram.com/") or lowered.startswith("//www.instagram.com/")


def _candidate(
    value: str,
    *,
    source_type: InstagramSourceType,
    source_url: str | None,
    evidence_url: str | None,
    page_scope: str | None,
    contact_specificity: str | None,
    source_title: str | None,
    source_snippet: str | None,
    surrounding_text: str | None,
    local_container_text: str | None,
    local_container_kind: str | None,
) -> InstagramCandidate | None:
    normalized = canonicalize_instagram_profile(value)
    handle = extract_instagram_handle(value)
    if not normalized or not handle or not is_instagram_profile_url(normalized):
        return None
    return InstagramCandidate(
        handle=handle,
        profile_url=normalized,
        source_type=source_type,
        source_url=source_url,
        evidence_url=evidence_url or source_url,
        page_scope=page_scope,
        contact_specificity=contact_specificity,
        source_title=source_title,
        source_snippet=source_snippet,
        surrounding_text=surrounding_text,
        local_container_text=local_container_text,
        local_container_kind=local_container_kind,
    )


def _json_ld_sameas_values(html: str | None) -> list[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    values: list[str] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.get_text(" ", strip=True)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        values.extend(_collect_sameas(payload))
    return values


def _collect_sameas(payload: object) -> list[str]:
    values: list[str] = []
    if isinstance(payload, dict):
        same_as = payload.get("sameAs")
        if isinstance(same_as, list):
            values.extend(str(item) for item in same_as if item)
        elif isinstance(same_as, str):
            values.append(same_as)
        for value in payload.values():
            values.extend(_collect_sameas(value))
    elif isinstance(payload, list):
        for item in payload:
            values.extend(_collect_sameas(item))
    return values


def extract_instagram_candidates_from_document(
    *,
    text: str,
    links: Iterable[str] | None,
    html: str | None,
    source_type: InstagramSourceType,
    source_url: str | None,
    page_scope: str | None,
    contact_specificity: str | None,
    source_title: str | None = None,
    source_snippet: str | None = None,
    local_container_kind: str | None = None,
) -> list[InstagramCandidate]:
    candidates: list[InstagramCandidate] = []
    seen: set[str] = set()
    for link in list(links or []):
        if not _looks_like_instagram_link(link):
            continue
        candidate = _candidate(
            link,
            source_type=source_type,
            source_url=source_url,
            evidence_url=source_url,
            page_scope=page_scope,
            contact_specificity=contact_specificity,
            source_title=source_title,
            source_snippet=source_snippet,
            surrounding_text=text[:400],
            local_container_text=text[:800],
            local_container_kind=local_container_kind,
        )
        if candidate is not None and candidate.profile_url not in seen:
            seen.add(candidate.profile_url)
            candidates.append(candidate)
    for match in _INSTAGRAM_URL_RE.findall(text):
        candidate = _candidate(
            match,
            source_type=source_type,
            source_url=source_url,
            evidence_url=source_url,
            page_scope=page_scope,
            contact_specificity=contact_specificity,
            source_title=source_title,
            source_snippet=source_snippet,
            surrounding_text=text[:400],
            local_container_text=text[:800],
            local_container_kind=local_container_kind,
        )
        if candidate is not None and candidate.profile_url not in seen:
            seen.add(candidate.profile_url)
            candidates.append(candidate)
    for handle_match in _INSTAGRAM_HANDLE_HINT_RE.finditer(text):
        candidate = _candidate(
            handle_match.group(1),
            source_type=source_type,
            source_url=source_url,
            evidence_url=source_url,
            page_scope=page_scope,
            contact_specificity=contact_specificity,
            source_title=source_title,
            source_snippet=source_snippet,
            surrounding_text=text[:400],
            local_container_text=text[:800],
            local_container_kind=local_container_kind,
        )
        if candidate is not None and candidate.profile_url not in seen:
            seen.add(candidate.profile_url)
            candidates.append(candidate)
    for same_as in _json_ld_sameas_values(html):
        candidate = _candidate(
            same_as,
            source_type=source_type,
            source_url=source_url,
            evidence_url=source_url,
            page_scope=page_scope,
            contact_specificity=contact_specificity,
            source_title=source_title,
            source_snippet=source_snippet,
            surrounding_text=text[:400],
            local_container_text=text[:800],
            local_container_kind="json_ld_sameas",
        )
        if candidate is not None and candidate.profile_url not in seen:
            seen.add(candidate.profile_url)
            candidates.append(candidate)
    return candidates


def extract_instagram_from_national_directory_context(**kwargs) -> list[InstagramCandidate]:
    return extract_instagram_candidates_from_document(source_type=InstagramSourceType.NATIONALS_DIRECTORY_ROW, **kwargs)


def extract_instagram_from_nationals_chapter_page(**kwargs) -> list[InstagramCandidate]:
    return extract_instagram_candidates_from_document(source_type=InstagramSourceType.NATIONALS_CHAPTER_PAGE, **kwargs)


def extract_instagram_from_school_page_context(**kwargs) -> list[InstagramCandidate]:
    return extract_instagram_candidates_from_document(source_type=InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE, **kwargs)


def extract_instagram_from_verified_chapter_website(**kwargs) -> list[InstagramCandidate]:
    return extract_instagram_candidates_from_document(source_type=InstagramSourceType.VERIFIED_CHAPTER_WEBSITE, **kwargs)
