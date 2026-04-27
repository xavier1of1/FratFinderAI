from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlparse

from fratfinder_crawler.candidate_sanitizer import sanitize_as_instagram


_PROFILE_RE = re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.]{1,30})(?:[/?#].*)?$", re.IGNORECASE)
_PROFILE_PATH_RE = re.compile(r"^/([A-Za-z0-9_.]{1,30})/?$", re.IGNORECASE)
_INVALID_HANDLE_RE = re.compile(r"^[A-Za-z0-9_.]{3,30}$")
_FILE_LIKE_HANDLE_RE = re.compile(r"^\d+w$", re.IGNORECASE)
_REJECTED_SEGMENTS = {
    "p",
    "reel",
    "tv",
    "stories",
    "explore",
    "accounts",
    "location",
    "locations",
    "direct",
    "about",
    "developer",
    "graphql",
}
_REJECTED_HANDLE_VALUES = {
    "index",
    "manifest",
    "style",
    "xmlrpc",
    "feed",
}
_REJECTED_HANDLE_SUFFIXES = (
    ".json",
    ".css",
    ".js",
    ".php",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".xml",
    ".txt",
    ".map",
    ".html",
    ".htm",
)


class InstagramUrlKind(StrEnum):
    PROFILE = "profile"
    POST = "post"
    REEL = "reel"
    STORY = "story"
    LOCATION = "location"
    EXPLORE = "explore"
    OTHER = "other"


@dataclass(slots=True)
class InstagramNormalizationResult:
    handle: str | None
    profile_url: str | None
    kind: InstagramUrlKind
    reject_reason: str | None = None


def extract_instagram_handle(value: str | None) -> str | None:
    result = classify_instagram_url(value)
    return result.handle


def is_instagram_profile_url(value: str | None) -> bool:
    return classify_instagram_url(value).kind == InstagramUrlKind.PROFILE


def canonicalize_instagram_profile(value: str | None) -> str | None:
    result = classify_instagram_url(value)
    return result.profile_url


def classify_instagram_url(value: str | None) -> InstagramNormalizationResult:
    raw = str(value or "").strip()
    if not raw:
        return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.OTHER, reject_reason="empty")
    sanitized = sanitize_as_instagram(raw)
    parsed = urlparse(raw if raw.startswith("http") else f"https://www.instagram.com/{raw.lstrip('@')}")
    path_segments = [segment for segment in (parsed.path or "").split("/") if segment]
    if path_segments:
        head = path_segments[0].lower()
        if head in {"p", "reel", "tv"}:
            kind = InstagramUrlKind.POST if head == "p" else InstagramUrlKind.REEL
            return InstagramNormalizationResult(handle=None, profile_url=None, kind=kind, reject_reason="non_profile_path")
        if head == "stories":
            return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.STORY, reject_reason="non_profile_path")
        if head in {"location", "locations"}:
            return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.LOCATION, reject_reason="non_profile_path")
        if head == "explore":
            return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.EXPLORE, reject_reason="non_profile_path")
        if head in _REJECTED_SEGMENTS:
            return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.OTHER, reject_reason="non_profile_path")
    def _is_rejected_handle(handle: str) -> bool:
        lowered = handle.lower()
        return (
            lowered in _REJECTED_SEGMENTS
            or lowered in _REJECTED_HANDLE_VALUES
            or lowered.endswith(_REJECTED_HANDLE_SUFFIXES)
            or _FILE_LIKE_HANDLE_RE.match(lowered) is not None
            or (lowered.isdigit() and len(lowered) >= 6)
        )

    if sanitized:
        match = _PROFILE_RE.search(sanitized)
        if match:
            handle = match.group(1)
            if _INVALID_HANDLE_RE.match(handle) and not _is_rejected_handle(handle):
                return InstagramNormalizationResult(
                    handle=handle,
                    profile_url=f"https://www.instagram.com/{handle}/",
                    kind=InstagramUrlKind.PROFILE,
                )
    handle_match = _PROFILE_RE.search(raw) or _PROFILE_PATH_RE.search(raw) or re.search(r"@?([A-Za-z0-9_.]{3,30})$", raw)
    if not handle_match:
        return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.OTHER, reject_reason="not_instagram")
    handle = handle_match.group(1).lstrip("@")
    if not _INVALID_HANDLE_RE.match(handle):
        return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.OTHER, reject_reason="invalid_handle")
    if _is_rejected_handle(handle):
        return InstagramNormalizationResult(handle=None, profile_url=None, kind=InstagramUrlKind.OTHER, reject_reason="non_profile_path")
    return InstagramNormalizationResult(
        handle=handle,
        profile_url=f"https://www.instagram.com/{handle}/",
        kind=InstagramUrlKind.PROFILE,
    )
