from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import unquote, urljoin, urlparse
import re

_EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)
_INSTAGRAM_PATH_RE = re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.-]+)", re.IGNORECASE)
_IGNORED_INSTAGRAM_SEGMENTS = {
    "p",
    "reel",
    "tv",
    "stories",
    "explore",
    "accounts",
    "mailto",
    "tel",
    "node",
    "umbraco.cms.core.models.link",
    "index.html",
    "index.php",
    "home",
    "default",
    "default.aspx",
}
_IGNORED_INSTAGRAM_HANDLE_SUFFIXES = {
    ".htm",
    ".html",
    ".php",
    ".aspx",
    ".jsp",
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
}


class CandidateKind(StrEnum):
    WEBSITE = "website"
    EMAIL = "email"
    INSTAGRAM = "instagram"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class SanitizedCandidate:
    kind: CandidateKind
    value: str
    original_kind: CandidateKind
    coerced: bool = False


def classify_candidate_kind(value: str | None) -> CandidateKind:
    if not value:
        return CandidateKind.UNKNOWN
    raw = value.strip()
    lowered = raw.lower()
    if lowered.startswith("mailto:") or _EMAIL_RE.match(raw):
        return CandidateKind.EMAIL
    if "instagram.com/" in lowered or raw.startswith("@"):
        return CandidateKind.INSTAGRAM
    if lowered.startswith("http://") or lowered.startswith("https://") or lowered.startswith("//") or lowered.startswith("/"):
        return CandidateKind.WEBSITE
    return CandidateKind.UNKNOWN


def sanitize_as_website(value: str | None, *, base_url: str | None = None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    lowered = raw.lower()
    if lowered.startswith("mailto:"):
        return None
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith("/") and base_url:
        raw = urljoin(base_url, raw)
    if not raw.lower().startswith(("http://", "https://")):
        return None
    parsed = urlparse(raw)
    if not parsed.netloc:
        return None
    return parsed._replace(fragment="").geturl()


def sanitize_as_email(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if raw.lower().startswith("mailto:"):
        raw = raw.split(":", 1)[1]
    raw = unquote(raw).split("?", 1)[0].strip()
    if not raw:
        return None
    if _EMAIL_RE.match(raw):
        return raw.lower()
    return None


def sanitize_as_instagram(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if raw.startswith("@"):
        raw = raw[1:]
    if not raw.lower().startswith("http"):
        if "instagram.com/" in raw.lower():
            raw = f"https://{raw.lstrip('/')}"
        else:
            raw = f"https://www.instagram.com/{raw}"
    match = _INSTAGRAM_PATH_RE.search(raw)
    if not match:
        return None
    handle = match.group(1).strip("/").split("/")[0].split("?")[0].split("#")[0].lstrip("@")
    if not handle or handle.lower() in _IGNORED_INSTAGRAM_SEGMENTS:
        return None
    lowered_handle = handle.lower()
    if any(lowered_handle.endswith(suffix) for suffix in _IGNORED_INSTAGRAM_HANDLE_SUFFIXES):
        return None
    return f"https://www.instagram.com/{handle}"


def sanitize_candidate(
    value: str | None,
    *,
    expected: CandidateKind,
    base_url: str | None = None,
) -> SanitizedCandidate | None:
    if not value:
        return None
    original_kind = classify_candidate_kind(value)

    if expected == CandidateKind.WEBSITE:
        website = sanitize_as_website(value, base_url=base_url)
        if website:
            return SanitizedCandidate(kind=CandidateKind.WEBSITE, value=website, original_kind=original_kind, coerced=original_kind != CandidateKind.WEBSITE)
        email = sanitize_as_email(value)
        if email:
            return SanitizedCandidate(kind=CandidateKind.EMAIL, value=email, original_kind=original_kind, coerced=True)
        instagram = sanitize_as_instagram(value)
        if instagram:
            return SanitizedCandidate(kind=CandidateKind.INSTAGRAM, value=instagram, original_kind=original_kind, coerced=True)
        return None

    if expected == CandidateKind.EMAIL:
        email = sanitize_as_email(value)
        if email:
            return SanitizedCandidate(kind=CandidateKind.EMAIL, value=email, original_kind=original_kind, coerced=original_kind != CandidateKind.EMAIL)
        return None

    if expected == CandidateKind.INSTAGRAM:
        instagram = sanitize_as_instagram(value)
        if instagram:
            return SanitizedCandidate(kind=CandidateKind.INSTAGRAM, value=instagram, original_kind=original_kind, coerced=original_kind != CandidateKind.INSTAGRAM)
        return None

    return None
