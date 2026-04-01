from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlparse


_POSITIVE_LABEL_MARKERS = (
    "go to site",
    "chapter website",
    "find a chapter",
    "our chapters",
    "chapter directory",
    "chapter roll",
    "chapter map",
    "chapter profile",
)

_NEGATIVE_LABEL_MARKERS = (
    "news",
    "blog",
    "resource",
    "toolkit",
    "award",
    "staff directory",
    "member directory",
)

_POSITIVE_URL_MARKERS = (
    "chapters",
    "chapter-directory",
    "find-a-chapter",
    "chapter-roll",
    "chapter",
    "directory",
)

_NEGATIVE_URL_MARKERS = (
    "news",
    "blog",
    "resource",
    "toolkit",
    "award",
    "staff",
)


@dataclass(slots=True)
class ChapterLinkScore:
    score: float
    positive_reasons: list[str] = field(default_factory=list)
    negative_reasons: list[str] = field(default_factory=list)


def score_chapter_link(label: str, url: str, context: str = "") -> ChapterLinkScore:
    lowered_label = (label or "").strip().lower()
    lowered_url = (url or "").strip().lower()
    lowered_context = (context or "").strip().lower()
    score = 0.0
    positives: list[str] = []
    negatives: list[str] = []

    if lowered_url.startswith("mailto:") or lowered_url.startswith("tel:"):
        negatives.append("contact_scheme_not_chapter_link")
        return ChapterLinkScore(score=0.0, positive_reasons=positives, negative_reasons=negatives)

    for marker in _POSITIVE_LABEL_MARKERS:
        if marker in lowered_label:
            score += 0.5
            positives.append(f"label:{marker}")

    for marker in _NEGATIVE_LABEL_MARKERS:
        if marker in lowered_label:
            score -= 0.6
            negatives.append(f"label:{marker}")

    parsed = urlparse(lowered_url)
    path = parsed.path
    for marker in _POSITIVE_URL_MARKERS:
        if marker in path:
            score += 0.25
            positives.append(f"url:{marker}")
            break

    for marker in _NEGATIVE_URL_MARKERS:
        if marker in path:
            score -= 0.35
            negatives.append(f"url:{marker}")
            break

    if "chapter" in lowered_context or "university" in lowered_context or "college" in lowered_context:
        score += 0.2
        positives.append("context:chapter_or_school")

    if "instagram.com" in lowered_url or "facebook.com" in lowered_url or "x.com" in lowered_url:
        score -= 0.2
        negatives.append("social_profile_url")

    return ChapterLinkScore(
        score=max(0.0, min(1.0, score)),
        positive_reasons=positives,
        negative_reasons=negatives,
    )


def has_dom_neighborhood(chapter_name: str | None, university_name: str | None, context: str) -> bool:
    if not chapter_name:
        return False
    if university_name and university_name.strip():
        return True
    lowered_context = (context or "").lower()
    return "university" in lowered_context or "college" in lowered_context or "chapter" in lowered_context
