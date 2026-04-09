from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse
import re
import unicodedata

from bs4 import BeautifulSoup

_BLOCKED_HOSTS = {
    "wikipedia.org",
    "www.wikipedia.org",
    "reddit.com",
    "www.reddit.com",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "linkedin.com",
    "www.linkedin.com",
    "x.com",
    "twitter.com",
    "stackoverflow.com",
    "stackexchange.com",
    "github.com",
    "medium.com",
    "quora.com",
    "wiktionary.org",
}

_BLOCKED_HOST_SUFFIXES = tuple(sorted(_BLOCKED_HOSTS))

_DIRECTORY_MARKERS = (
    "chapter-directory",
    "chapters",
    "chapter list",
    "chapter",
    "directory",
    "find a chapter",
    "find-a-chapter",
    "findachapter",
    "locations",
    "locator",
    "chapter-roll",
    "our chapters",
    "active chapters",
    "colonies",
)

_GENERIC_INFO_PATH_SEGMENTS = {
    "about",
    "history",
    "ideals",
    "mission",
    "values",
    "recruitment",
    "join",
    "news",
    "events",
    "careers",
    "contact",
    "staff",
    "board",
    "foundation",
    "housing",
    "merchandise",
    "giving",
}

_NON_ORG_CONTEXT_MARKERS = (
    "function",
    "programming",
    "software",
    "tutorial",
    "question",
    "forum",
    "how to",
    "travel",
    "vacation",
    "resort",
    "island",
    "hotel",
    "airline",
    "store",
    "shop",
    "merchandise",
)

_CHAPTER_CONTEXT_MARKERS = (
    "fraternity",
    "chapter",
    "chapters",
    "colony",
    "colonies",
    "greek life",
    "interfraternity",
    "ifc",
    "brotherhood",
)

_SAME_HOST_DIRECTORY_BLOCKLIST = (
    "staff-directory",
    "staff directory",
    "member directory",
    "members directory",
    "alumni directory",
    "alumni",
    "staff",
    "foundation",
    "donate",
    "giving",
)

_SAME_HOST_DIRECTORY_PENALTIES = (
    "expansion",
    "start-a-chapter",
    "join",
    "recruitment",
)

_LOW_SIGNAL_WEBSITE_PATH_MARKERS = (
    "apparel",
    "article",
    "articles",
    "award",
    "awards",
    "blog",
    "bookstore",
    "calendar",
    "event",
    "events",
    "grade-report",
    "grade_report",
    "history",
    "journalism",
    "merch",
    "news",
    "post",
    "posts",
    "prize",
    "prizes",
    "profile",
    "profiles",
    "report",
    "reports",
    "resource",
    "resources",
    "scholarship",
    "scholarships",
    "shop",
    "statistics",
    "store",
    "story",
    "stories",
    "terminology",
    "statement",
    "statements",
)

_OFFICIAL_AFFILIATION_MARKERS = (
    "chapter profile",
    "chapter profiles",
    "chapters",
    "clubs organizations",
    "council",
    "find a student org",
    "fraternities",
    "fraternity and sorority",
    "fraternity chapters",
    "fraternity sorority life",
    "greek life",
    "greek organizations",
    "ifc",
    "interfraternity",
    "organization profile",
    "organization scorecard",
    "recognized chapters",
    "student org",
    "student organization",
    "student organizations",
)

_GREEK_SYMBOLS = {
    "\u0391": "alpha",
    "\u03b1": "alpha",
    "\u0392": "beta",
    "\u03b2": "beta",
    "\u0393": "gamma",
    "\u03b3": "gamma",
    "\u0394": "delta",
    "\u03b4": "delta",
    "\u0395": "epsilon",
    "\u03b5": "epsilon",
    "\u0396": "zeta",
    "\u03b6": "zeta",
    "\u0397": "eta",
    "\u03b7": "eta",
    "\u0398": "theta",
    "\u03b8": "theta",
    "\u0399": "iota",
    "\u03b9": "iota",
    "\u039a": "kappa",
    "\u03ba": "kappa",
    "\u039b": "lambda",
    "\u03bb": "lambda",
    "\u039c": "mu",
    "\u03bc": "mu",
    "\u039d": "nu",
    "\u03bd": "nu",
    "\u039e": "xi",
    "\u03be": "xi",
    "\u039f": "omicron",
    "\u03bf": "omicron",
    "\u03a0": "pi",
    "\u03c0": "pi",
    "\u03a1": "rho",
    "\u03c1": "rho",
    "\u03a3": "sigma",
    "\u03c3": "sigma",
    "\u03c2": "sigma",
    "\u03a4": "tau",
    "\u03c4": "tau",
    "\u03a5": "upsilon",
    "\u03c5": "upsilon",
    "\u03a6": "phi",
    "\u03c6": "phi",
    "\u03a7": "chi",
    "\u03c7": "chi",
    "\u03a8": "psi",
    "\u03c8": "psi",
    "\u03a9": "omega",
    "\u03c9": "omega",
}

_GREEK_TOKENS = {
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "iota",
    "kappa",
    "lambda",
    "mu",
    "nu",
    "xi",
    "omicron",
    "pi",
    "rho",
    "sigma",
    "tau",
    "upsilon",
    "phi",
    "chi",
    "psi",
    "omega",
}

_IDENTITY_STOPWORDS = {
    "chapter",
    "chapters",
    "colony",
    "colonies",
    "fraternity",
    "international",
    "official",
    "national",
    "nationals",
    "the",
    "and",
    "for",
    "at",
    "of",
    "university",
    "college",
    "campus",
}


@dataclass(slots=True)
class PrecisionDecision:
    decision: str
    confidence: float
    reason_codes: list[str] = field(default_factory=list)
    evidence_urls: list[str] = field(default_factory=list)
    next_action: str = "continue"
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "confidence": self.confidence,
            "reasonCodes": list(self.reason_codes),
            "evidenceUrls": list(self.evidence_urls),
            "nextAction": self.next_action,
            "metadata": dict(self.metadata),
        }


def _replace_greek_symbols(value: str) -> str:
    translated = unicodedata.normalize("NFKC", value or "")
    for symbol, replacement in _GREEK_SYMBOLS.items():
        translated = translated.replace(symbol, f" {replacement} ")
    return translated


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = _replace_greek_symbols(value.lower())
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _compact_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", _normalize_text(value))


def _initialism(value: str | None) -> str:
    tokens = [token for token in re.split(r"[^a-z0-9]+", _normalize_text(value)) if token]
    return "".join(token[0] for token in tokens if token)


def _significant_tokens(value: str | None) -> list[str]:
    tokens = [token for token in re.split(r"[^a-z0-9]+", _normalize_text(value)) if token]
    return [token for token in tokens if token not in _IDENTITY_STOPWORDS]


def _path_segments(path: str) -> list[str]:
    return [segment for segment in path.lower().split("/") if segment]


def _contains_phrase(text: str, phrase: str) -> bool:
    normalized_text = _compact_text(text)
    normalized_phrase = _compact_text(phrase)
    return bool(normalized_phrase and normalized_phrase in normalized_text)


def _host_is_blocked(host: str) -> bool:
    normalized = host.lower().strip(".")
    return normalized in _BLOCKED_HOSTS or any(normalized.endswith(f".{suffix}") for suffix in _BLOCKED_HOST_SUFFIXES)


def _fraternity_phrase_candidates(fraternity_name: str, fraternity_slug: str) -> list[str]:
    phrases = [fraternity_name, fraternity_slug.replace("-", " ")]
    initials = _initialism(fraternity_name)
    if len(initials) >= 2:
        phrases.append(initials)
    compact_slug = fraternity_slug.replace("-", "")
    if compact_slug:
        phrases.append(compact_slug)
    alias_map = {
        "phi gamma delta": ["fiji"],
        "alpha tau omega": ["ato"],
        "sigma alpha epsilon": ["sae"],
        "beta upsilon chi": ["byx"],
        "pi kappa alpha": ["pike"],
    }
    canonical = _normalize_text(fraternity_name)
    phrases.extend(alias_map.get(canonical, []))
    return [phrase for phrase in phrases if phrase]


def _tabbed_roster_section_texts(soup: BeautifulSoup | None) -> dict[str, list[str]]:
    if soup is None:
        return {}
    targets: dict[str, list[str]] = {"fraternities": [], "sororities": [], "suspended": [], "closed": []}
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href.startswith("#"):
            continue
        target = href[1:].strip()
        if not target:
            continue
        text = _normalize_text(anchor.get_text(" ", strip=True))
        if "fraternit" in text:
            targets["fraternities"].append(target)
        elif "sororit" in text:
            targets["sororities"].append(target)
        elif "suspended" in text:
            targets["suspended"].append(target)
        elif "closed" in text:
            targets["closed"].append(target)
    collected: dict[str, list[str]] = {}
    for key, ids in targets.items():
        texts: list[str] = []
        seen: set[str] = set()
        for target in ids:
            if target in seen:
                continue
            seen.add(target)
            section = soup.find(id=target)
            if section is None:
                continue
            texts.append(_normalize_text(section.get_text(" ", strip=True)))
            texts.extend(_normalize_text(node.get_text(" ", strip=True)) for node in section.select("a[href]"))
            texts.extend(_normalize_text(node.get_text(" ", strip=True)) for node in section.select("li"))
            texts.extend(_normalize_text(node.get_text(" ", strip=True)) for node in section.select("h1, h2, h3, h4, h5"))
        if texts:
            collected[key] = [text for text in texts if text]
    return collected


def _extract_greek_org_phrases(text: str) -> set[str]:
    greek = "|".join(sorted(_GREEK_TOKENS, key=len, reverse=True))
    pattern = re.compile(rf"\b(?:{greek})(?:\s+(?:{greek})){{1,3}}\b")
    return {match.group(0).strip() for match in pattern.finditer(_normalize_text(text))}


def _text_has_conflicting_org_phrase(fraternity_name: str, fraternity_slug: str, text: str) -> bool:
    normalized = _normalize_text(text)
    target_phrases = {_normalize_text(candidate) for candidate in _fraternity_phrase_candidates(fraternity_name, fraternity_slug)}
    target_phrases.discard("")
    if not normalized or not target_phrases:
        return False
    if any(phrase in normalized for phrase in target_phrases):
        return False
    for phrase in _extract_greek_org_phrases(normalized):
        if phrase not in target_phrases:
            return True
    return False


def _page_has_directory_signal(*values: str) -> bool:
    combined = " ".join(_normalize_text(value) for value in values if value)
    return any(marker in combined for marker in _DIRECTORY_MARKERS)


def tool_source_identity_guard(
    *,
    fraternity_name: str,
    fraternity_slug: str,
    candidate_url: str,
    title: str = "",
    snippet: str = "",
) -> PrecisionDecision:
    parsed = urlparse(candidate_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    combined = " ".join(part for part in [title, snippet, host, path] if part)
    normalized_combined = _normalize_text(combined)
    reasons: list[str] = []
    confidence = 0.18

    if not host:
        return PrecisionDecision(
            decision="reject",
            confidence=0.0,
            reason_codes=["missing_host"],
            evidence_urls=[candidate_url] if candidate_url else [],
            next_action="reject",
        )

    if _host_is_blocked(host):
        return PrecisionDecision(
            decision="reject",
            confidence=0.0,
            reason_codes=["blocked_host"],
            evidence_urls=[candidate_url],
            next_action="reject",
        )

    if _text_has_conflicting_org_phrase(fraternity_name, fraternity_slug, combined):
        return PrecisionDecision(
            decision="reject",
            confidence=0.05,
            reason_codes=["cross_fraternity_conflict"],
            evidence_urls=[candidate_url],
            next_action="reject",
        )

    fraternity_phrase_match = any(_contains_phrase(part, fraternity_name) for part in [title, snippet, host, path])
    slug_phrase_match = bool(fraternity_slug and fraternity_slug.replace("-", "") in _compact_text(f"{host} {path}"))
    token_hits = sum(1 for token in _significant_tokens(fraternity_name) if token in _compact_text(host))

    if fraternity_phrase_match:
        confidence += 0.3
        reasons.append("fraternity_phrase_match")
    if slug_phrase_match:
        confidence += 0.18
        reasons.append("slug_identity_match")
    if token_hits >= 2:
        confidence += 0.14
        reasons.append("host_token_match")
    elif token_hits == 1:
        confidence += 0.08
        reasons.append("host_partial_token_match")

    if _page_has_directory_signal(title, snippet, path):
        confidence += 0.16
        reasons.append("directory_signal")

    chapter_context_hits = sum(1 for marker in _CHAPTER_CONTEXT_MARKERS if marker in normalized_combined)
    if chapter_context_hits >= 2:
        confidence += 0.12
        reasons.append("strong_chapter_context")
    elif chapter_context_hits == 1:
        confidence += 0.06
        reasons.append("chapter_context")

    normalized_path = path.strip("/")
    if not normalized_path:
        reasons.append("generic_root_path")
        confidence -= 0.05
    else:
        segments = _path_segments(path)
        if len(segments) == 1 and segments[0] in _GENERIC_INFO_PATH_SEGMENTS and not _page_has_directory_signal(path, title, snippet):
            reasons.append("generic_info_path")
            confidence -= 0.18

    if any(marker in normalized_combined for marker in _NON_ORG_CONTEXT_MARKERS) and chapter_context_hits == 0:
        return PrecisionDecision(
            decision="reject",
            confidence=max(0.0, min(confidence, 0.2)),
            reason_codes=[*reasons, "non_org_context"],
            evidence_urls=[candidate_url],
            next_action="reject",
        )

    confidence = max(0.0, min(0.99, confidence))
    if confidence >= 0.72:
        decision = "match"
        next_action = "continue"
    elif confidence >= 0.48:
        decision = "weak_match"
        next_action = "review"
    else:
        decision = "reject"
        next_action = "reject"
        reasons.append("low_identity_confidence")

    return PrecisionDecision(
        decision=decision,
        confidence=round(confidence, 4),
        reason_codes=reasons,
        evidence_urls=[candidate_url],
        next_action=next_action,
        metadata={
            "host": host,
            "path": path,
            "chapterContextHits": chapter_context_hits,
        },
    )


def tool_same_host_directory_ranker(
    *,
    source_url: str,
    html: str,
) -> PrecisionDecision:
    if not html:
        return PrecisionDecision(decision="no_directory_candidate", confidence=0.0, reason_codes=["empty_html"], next_action="review")

    parsed = urlparse(source_url)
    host = (parsed.netloc or "").lower()
    if not host:
        return PrecisionDecision(decision="no_directory_candidate", confidence=0.0, reason_codes=["missing_host"], next_action="review")

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[dict[str, Any]] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        candidate_url = urljoin(source_url, href)
        candidate_parsed = urlparse(candidate_url)
        if (candidate_parsed.netloc or "").lower() != host:
            continue
        candidate_path = (candidate_parsed.path or "").lower()
        if not candidate_path or candidate_path == parsed.path:
            continue

        anchor_text = " ".join(anchor.stripped_strings)
        parent = getattr(anchor, "parent", None)
        parent_text = ""
        if parent is not None and getattr(parent, "name", None) not in {"body", "html"}:
            parent_text = parent.get_text(" ", strip=True)[:300]
        combined_text = _normalize_text(f"{candidate_path} {anchor_text} {parent_text}")
        blocklist_text = _normalize_text(f"{candidate_path} {anchor_text}")
        if any(marker in blocklist_text for marker in _SAME_HOST_DIRECTORY_BLOCKLIST):
            continue
        if not _page_has_directory_signal(candidate_path, anchor_text, parent_text):
            continue

        score = 0
        reasons: list[str] = []
        if "chapters" in candidate_path:
            score += 5
            reasons.append("chapters_path")
        elif "chapter" in candidate_path:
            score += 4
            reasons.append("chapter_path")
        elif "colony" in candidate_path or "colonies" in candidate_path:
            score += 3
            reasons.append("colonies_path")
        if _page_has_directory_signal(anchor_text):
            score += 2
            reasons.append("anchor_directory_signal")
        if _page_has_directory_signal(parent_text):
            score += 1
            reasons.append("parent_directory_signal")
        if len(_path_segments(candidate_path)) <= 2:
            score += 1
            reasons.append("shallow_path")
        if any(marker in candidate_path for marker in _SAME_HOST_DIRECTORY_PENALTIES):
            score -= 3
            reasons.append("penalized_path")

        if score > 0:
            candidates.append(
                {
                    "url": candidate_url,
                    "score": score,
                    "anchorText": anchor_text,
                    "reasonCodes": reasons,
                }
            )

    if not candidates:
        return PrecisionDecision(
            decision="no_directory_candidate",
            confidence=0.0,
            reason_codes=["no_same_host_directory_link"],
            evidence_urls=[source_url],
            next_action="review",
        )

    candidates.sort(key=lambda item: (int(item["score"]), str(item["url"])), reverse=True)
    selected = candidates[0]
    confidence = min(0.95, 0.55 + (0.05 * int(selected["score"])))
    return PrecisionDecision(
        decision="ranked_directory_link",
        confidence=round(confidence, 4),
        reason_codes=list(selected["reasonCodes"]),
        evidence_urls=[source_url, str(selected["url"])],
        next_action="continue",
        metadata={
            "selectedUrl": selected["url"],
            "selectedAnchorText": selected["anchorText"],
            "candidates": candidates[:8],
        },
    )


def tool_directory_layout_profiler(
    *,
    html: str,
    page_url: str | None = None,
) -> PrecisionDecision:
    soup = BeautifulSoup(html, "html.parser")
    table_count = len(soup.select("table"))
    explicit_cards = soup.select("[data-chapter-card], .chapter-card, .chapter-item, li.chapter-item")
    anchor_cards = soup.select("a.chapter-link, a.chapter-card, a[data-chapter-card]")

    linked_directory_items = 0
    for node in soup.select("li, article, div, section"):
        href_node = node.select_one("a[href]")
        if href_node is None:
            continue
        headings = node.select("h1, h2, h3, h4, strong")
        text = node.get_text(" ", strip=True)
        if (headings or _page_has_directory_signal(text, href_node.get_text(" ", strip=True))) and (
            _page_has_directory_signal(text, href_node.get_text(" ", strip=True))
            or any(token in _normalize_text(text) for token in ("university", "college", "institute", "state"))
        ):
            linked_directory_items += 1

    repeated_classes: dict[tuple[str, tuple[str, ...]], int] = {}
    for element in soup.find_all(["article", "div", "li", "section"]):
        classes = tuple(sorted(element.get("class", [])))
        if not classes:
            continue
        key = (element.name, classes)
        repeated_classes[key] = repeated_classes.get(key, 0) + 1
    repeated_block_count = max([count for count in repeated_classes.values() if count > 1] or [0])

    reasons: list[str] = []
    possible_data_locations: list[str] = []
    layout_family = "unclassified"
    recommended_strategy = "review"
    confidence = 0.2

    if table_count > 0:
        layout_family = "table_directory"
        recommended_strategy = "table"
        confidence = 0.92
        reasons.append("html_table_present")
        possible_data_locations.append("table")
    elif len(explicit_cards) >= 1 and len(anchor_cards) >= 1 and (len(explicit_cards) + len(anchor_cards)) >= 2:
        layout_family = "mixed_card_grid"
        recommended_strategy = "repeated_block"
        confidence = 0.9
        reasons.extend(["explicit_cards", "anchor_cards"])
        possible_data_locations.extend([".chapter-item", ".chapter-card", "a.chapter-link"])
    elif len(explicit_cards) >= 2:
        layout_family = "explicit_card_grid"
        recommended_strategy = "repeated_block"
        confidence = 0.88
        reasons.append("explicit_cards")
        possible_data_locations.extend([".chapter-item", ".chapter-card", "[data-chapter-card]"])
    elif len(anchor_cards) >= 2:
        layout_family = "anchor_card_grid"
        recommended_strategy = "repeated_block"
        confidence = 0.86
        reasons.append("anchor_cards")
        possible_data_locations.append("a.chapter-link")
    elif linked_directory_items >= 6:
        layout_family = "linked_directory"
        recommended_strategy = "repeated_block"
        confidence = 0.78
        reasons.append("linked_directory_items")
        possible_data_locations.append("li a[href]")
    elif repeated_block_count >= 2:
        layout_family = "repeated_blocks"
        recommended_strategy = "repeated_block"
        confidence = 0.72
        reasons.append("repeated_block_classes")
        possible_data_locations.append("repeated_class_blocks")

    return PrecisionDecision(
        decision="directory_layout_profiled" if layout_family != "unclassified" else "layout_unclear",
        confidence=round(confidence, 4),
        reason_codes=reasons or ["unclassified_layout"],
        evidence_urls=[page_url] if page_url else [],
        next_action="continue" if layout_family != "unclassified" else "review",
        metadata={
            "layoutFamily": layout_family,
            "recommendedStrategy": recommended_strategy,
            "tableCount": table_count,
            "explicitCardCount": len(explicit_cards),
            "anchorCardCount": len(anchor_cards),
            "linkedDirectoryItemCount": linked_directory_items,
            "repeatedBlockCount": repeated_block_count,
            "possibleDataLocations": possible_data_locations,
        },
    )


def tool_official_domain_verifier(
    *,
    candidate_url: str,
    fraternity_name: str,
    fraternity_slug: str,
    chapter_name: str,
    university_name: str | None,
    source_url: str | None = None,
    document_url: str | None = None,
    document_title: str = "",
    document_text: str = "",
) -> PrecisionDecision:
    parsed = urlparse(candidate_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    normalized_context = _normalize_text(" ".join(part for part in [document_title, document_text[:1600], document_url or "", candidate_url] if part))

    if not host:
        return PrecisionDecision(decision="reject", confidence=0.0, reason_codes=["missing_host"], evidence_urls=[candidate_url], next_action="reject")
    if _host_is_blocked(host):
        return PrecisionDecision(decision="reject", confidence=0.0, reason_codes=["blocked_host"], evidence_urls=[candidate_url], next_action="reject")
    candidate_identity = _normalize_text(f"{host} {path} {parsed.query or ''}")
    if (
        ("google." in host and ("/maps/" in path or "/maps/d/" in path))
        or "forcekml" in candidate_identity
        or path.endswith(".kml")
    ):
        return PrecisionDecision(decision="reject", confidence=0.0, reason_codes=["map_export_url"], evidence_urls=[candidate_url], next_action="reject")
    if any(marker in candidate_identity for marker in ("archive", "archives", "digital api collection", " download")):
        return PrecisionDecision(decision="reject", confidence=0.0, reason_codes=["archival_url"], evidence_urls=[candidate_url], next_action="reject")
    if source_url and urlparse(source_url).netloc.lower() == host and not path.strip("/"):
        return PrecisionDecision(decision="reject", confidence=0.0, reason_codes=["source_domain_root"], evidence_urls=[candidate_url], next_action="reject")
    if any(marker in _normalize_text(f"{host} {path}") for marker in _LOW_SIGNAL_WEBSITE_PATH_MARKERS):
        return PrecisionDecision(decision="reject", confidence=0.1, reason_codes=["low_signal_path"], evidence_urls=[candidate_url], next_action="reject")
    if _text_has_conflicting_org_phrase(fraternity_name, fraternity_slug, normalized_context):
        return PrecisionDecision(decision="reject", confidence=0.05, reason_codes=["cross_fraternity_conflict"], evidence_urls=[candidate_url], next_action="reject")

    fraternity_match = any(_contains_phrase(value, fraternity_name) for value in [candidate_url, document_title, document_text])
    fraternity_slug_match = bool(fraternity_slug and fraternity_slug.replace("-", "") in _compact_text(candidate_url))
    chapter_tokens = _significant_tokens(chapter_name)
    school_tokens = _significant_tokens(university_name)
    candidate_identity_text = _normalize_text(f"{host} {path}")
    chapter_hits = sum(1 for token in chapter_tokens if token in candidate_identity_text or token in normalized_context)
    school_hits = sum(1 for token in school_tokens if token in normalized_context)
    official_affiliation_hits = sum(1 for marker in _OFFICIAL_AFFILIATION_MARKERS if marker in normalized_context)

    confidence = 0.22
    reasons: list[str] = []
    if fraternity_match:
        confidence += 0.24
        reasons.append("fraternity_identity_match")
    if fraternity_slug_match:
        confidence += 0.14
        reasons.append("fraternity_slug_match")
    if chapter_hits > 0:
        confidence += min(0.18, chapter_hits * 0.08)
        reasons.append("chapter_identity_match")
    if school_hits > 0:
        confidence += min(0.16, school_hits * 0.08)
        reasons.append("school_identity_match")
    if official_affiliation_hits > 0:
        confidence += min(0.16, official_affiliation_hits * 0.05)
        reasons.append("official_affiliation_context")
    if host.endswith(".edu"):
        confidence += 0.08
        reasons.append("school_domain")
    elif not _page_has_directory_signal(path) and path.strip("/"):
        confidence += 0.06
        reasons.append("non_directory_path")

    if _page_has_directory_signal(path) and official_affiliation_hits == 0 and chapter_hits == 0:
        confidence -= 0.2
        reasons.append("generic_directory_path")
    if host.endswith(".edu") and official_affiliation_hits > 0 and school_hits == 0:
        return PrecisionDecision(
            decision="reject",
            confidence=0.15,
            reason_codes=["missing_target_school_context"],
            evidence_urls=[url for url in [candidate_url, document_url] if url],
            next_action="reject",
            metadata={
                "host": host,
                "path": path,
                "chapterHits": chapter_hits,
                "schoolHits": school_hits,
                "officialAffiliationHits": official_affiliation_hits,
            },
        )

    confidence = max(0.0, min(0.99, confidence))
    if confidence >= 0.76 and not host.endswith(".edu"):
        decision = "official_chapter_domain"
    elif confidence >= 0.72 and host.endswith(".edu"):
        decision = "official_affiliation_page"
    elif confidence >= 0.54:
        decision = "weak_match"
    else:
        decision = "reject"
        reasons.append("low_official_confidence")

    return PrecisionDecision(
        decision=decision,
        confidence=round(confidence, 4),
        reason_codes=reasons,
        evidence_urls=[url for url in [candidate_url, document_url] if url],
        next_action="continue" if decision != "reject" else "reject",
        metadata={
            "host": host,
            "path": path,
            "chapterHits": chapter_hits,
            "schoolHits": school_hits,
            "officialAffiliationHits": official_affiliation_hits,
        },
    )


def _school_phrase_candidates(school_name: str | None) -> list[str]:
    if not school_name:
        return []
    normalized = _normalize_text(school_name)
    candidates = [school_name, normalized]
    trailing_core = re.sub(r"\b(university|college|institute|school)\b", "", normalized).strip()
    trailing_core = re.sub(r"\s+", " ", trailing_core).strip()
    if trailing_core and trailing_core != normalized:
        candidates.append(trailing_core)
    if normalized.startswith("university of "):
        core = normalized.removeprefix("university of ").strip()
        if core:
            candidates.append(core)
    return [candidate for candidate in candidates if candidate]


def _school_host_alias_candidates(school_name: str | None) -> list[str]:
    normalized = _normalize_text(school_name)
    if not normalized:
        return []
    tokens = [token for token in re.split(r"\s+", normalized) if token]
    broad_tokens = [token for token in tokens if token not in {"of", "the", "at"}]
    filtered = [token for token in tokens if token not in {"university", "college", "institute", "school", "of", "the", "at"}]
    aliases: list[str] = []
    if filtered:
        aliases.extend(token[:6] for token in filtered if len(token) >= 4)
    if normalized.startswith("university of ") and filtered:
        aliases.append(f"u{filtered[0][:4]}")
    if len(filtered) >= 2:
        aliases.append("".join(token[0] for token in filtered[:4]))
    if len(broad_tokens) >= 2:
        aliases.append("".join(token[0] for token in broad_tokens[:4]))
    return [alias for alias in dict.fromkeys(alias for alias in aliases if len(alias) >= 3)]


def _text_contains_any(text: str, phrases: list[str]) -> bool:
    return any(_contains_phrase(text, phrase) for phrase in phrases if phrase)


def _official_school_source(host: str, school_name: str | None) -> bool:
    normalized_host = host.lower().strip(".")
    school_tokens = [token for token in _significant_tokens(school_name) if len(token) >= 4]
    host_text = _normalize_text(normalized_host.replace(".", " "))
    host_aliases = _school_host_alias_candidates(school_name)
    token_hits = sum(1 for token in school_tokens if token in host_text)
    alias_hits = sum(1 for alias in host_aliases if alias in host_text)
    if normalized_host.endswith(".edu"):
        return token_hits >= 1 or alias_hits >= 1
    if not school_tokens and not host_aliases:
        return False
    return token_hits >= min(2, len(school_tokens)) or alias_hits >= 1


def _contains_strong_ban_phrase(text: str) -> bool:
    strong_markers = (
        "banned fraternities",
        "banned from campus",
        "abolish the fraternity system",
        "abolished the fraternity system",
        "no active greek life chapters",
        "there are no fraternities",
        "fraternities are no longer",
        "no greek life",
        "no fraternities on campus",
    )
    normalized = _normalize_text(text)
    return any(marker in normalized for marker in strong_markers)


def _contains_soft_ban_context(text: str) -> bool:
    normalized = _normalize_text(text)
    signals = (
        "fraternit",
        "greek life",
        "greek organizations",
        "board of trustees",
        "abolish",
        "banned",
        "suspended",
        "discontinued",
        "no longer",
    )
    return sum(1 for signal in signals if signal in normalized) >= 2


def tool_greek_detection(*, value: str) -> PrecisionDecision:
    normalized = _normalize_text(value)
    found = [token for token in _GREEK_TOKENS if re.search(rf"\b{re.escape(token)}\b", normalized)]
    found = list(dict.fromkeys(found))
    return PrecisionDecision(
        decision="greek_tokens_found" if found else "no_greek_tokens",
        confidence=0.9 if found else 0.2,
        reason_codes=["greek_token_present"] if found else ["no_greek_token_present"],
        metadata={"tokens": found},
        next_action="continue" if found else "review",
    )


def tool_site_scope_classifier(
    *,
    page_url: str,
    title: str = "",
    text: str = "",
    fraternity_name: str = "",
    school_name: str | None = None,
    chapter_name: str | None = None,
) -> PrecisionDecision:
    parsed = urlparse(page_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    combined = _normalize_text(" ".join(part for part in [title, text[:1600], page_url] if part))
    school_match = _text_contains_any(combined, _school_phrase_candidates(school_name))
    fraternity_match = _text_contains_any(combined, _fraternity_phrase_candidates(fraternity_name, fraternity_name.replace(" ", "-")))
    chapter_match = _text_contains_any(combined, [chapter_name or ""])

    decision = "other"
    reasons: list[str] = []
    confidence = 0.35
    if _official_school_source(host, school_name):
        if any(marker in combined for marker in _OFFICIAL_AFFILIATION_MARKERS) or _looks_like_official_chapter_list_page(combined) or _looks_like_tabbed_chapter_status_page(combined):
            decision = "school_affiliation"
            confidence = 0.88 if school_match else 0.72
            reasons.append("official_school_affiliation_markers")
        elif school_match:
            decision = "school_affiliation"
            confidence = 0.7
            reasons.append("school_owned_source")
    elif _page_has_directory_signal(path, title, text):
        decision = "nationals"
        confidence = 0.78 if fraternity_match else 0.58
        reasons.append("directory_markers")
    elif fraternity_match and (school_match or chapter_match):
        decision = "chapter_site"
        confidence = 0.82
        reasons.append("chapter_identity_context")

    return PrecisionDecision(
        decision=decision,
        confidence=round(confidence, 4),
        reason_codes=reasons or ["scope_unclear"],
        evidence_urls=[page_url] if page_url else [],
        next_action="continue" if decision != "other" else "review",
        metadata={"host": host, "path": path, "schoolMatch": school_match, "fraternityMatch": fraternity_match, "chapterMatch": chapter_match},
    )


def tool_campus_greek_life_policy(
    *,
    school_name: str,
    page_url: str,
    title: str = "",
    text: str = "",
) -> PrecisionDecision:
    parsed = urlparse(page_url)
    host = (parsed.netloc or "").lower()
    combined = _normalize_text(" ".join(part for part in [title, text[:2400], page_url] if part))
    school_phrases = _school_phrase_candidates(school_name)

    if not _official_school_source(host, school_name):
        return PrecisionDecision(
            decision="unknown",
            confidence=0.0,
            reason_codes=["non_official_school_source"],
            evidence_urls=[page_url] if page_url else [],
            next_action="review",
        )
    if not _text_contains_any(combined, school_phrases) and not _contains_strong_ban_phrase(combined):
        return PrecisionDecision(
            decision="unknown",
            confidence=0.0,
            reason_codes=["school_identity_missing"],
            evidence_urls=[page_url] if page_url else [],
            next_action="review",
        )
    if _contains_strong_ban_phrase(combined) and _contains_soft_ban_context(combined):
        return PrecisionDecision(
            decision="banned",
            confidence=0.97,
            reason_codes=["strong_ban_phrase", "official_school_source"],
            evidence_urls=[page_url] if page_url else [],
            next_action="continue",
            metadata={"sourceType": "official_school"},
        )
    current_allowed_markers = (
        "fraternity and sorority life",
        "recognized chapters",
        "interfraternity council",
        "fraternity chapters",
        "student organizations",
        "fraternities",
    )
    weak_or_historical_markers = (
        "unrecognized orgs",
        "unrecognized organizations",
        "hazing",
        "violations",
        "conduct",
        "archive",
        "archives",
        "history",
        "historical",
    )
    if (
        any(marker in combined for marker in current_allowed_markers)
        and _looks_like_official_chapter_list_page(combined)
        and not any(marker in combined for marker in weak_or_historical_markers)
    ):
        return PrecisionDecision(
            decision="allowed",
            confidence=0.9,
            reason_codes=["current_official_greek_life_context"],
            evidence_urls=[page_url] if page_url else [],
            next_action="continue",
            metadata={"sourceType": "official_school"},
        )
    return PrecisionDecision(
        decision="unknown",
        confidence=0.28,
        reason_codes=["no_conclusive_policy_signal"],
        evidence_urls=[page_url] if page_url else [],
        next_action="review",
        metadata={"sourceType": "official_school"},
    )


def _extract_anchor_texts_from_html(html: str) -> list[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    texts: list[str] = []
    for anchor in soup.select("a[href]"):
        text = anchor.get_text(" ", strip=True)
        if text:
            texts.append(text)
    return texts


def _looks_like_official_chapter_list_page(text: str) -> bool:
    normalized = _normalize_text(text)
    strong_markers = (
        "chapters at",
        "fraternity chapters",
        "recognized chapters",
        "chapter scorecards",
        "active chapters",
        "community scorecard",
        "chapter list",
        "councils and chapters",
    )
    return any(marker in normalized for marker in strong_markers)


def _looks_like_tabbed_chapter_status_page(text: str) -> bool:
    normalized = _normalize_text(text)
    has_tabs = "fraternities" in normalized and any(
        marker in normalized for marker in ("sororities", "suspended chapters", "closed chapters")
    )
    has_status_entries = "active chapters" in normalized or (
        normalized.count("view scorecard") >= 3 and normalized.count("active") >= 3
    )
    return has_tabs and has_status_entries


def _looks_like_chapter_name_entry(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if any(
        marker in normalized
        for marker in (
            "view scorecard",
            "learn more",
            "join",
            "community scorecard",
            "interfraternity council",
            "panhellenic council",
            "councils and chapters",
            "home",
            "contact",
            "about",
            "our community",
            "active chapters",
            "suspended chapters",
            "closed chapters",
        )
    ):
        return False
    words = normalized.split()
    if len(words) > 8:
        return False
    return bool(words)


def _looks_historical_or_archival_context(text: str, *, page_url: str = "") -> bool:
    normalized = _normalize_text(" ".join(part for part in [text, page_url] if part))
    if _looks_like_tabbed_chapter_status_page(normalized) or "active chapters" in normalized or "chapter scorecards" in normalized:
        return False
    markers = (
        "archive",
        "archives",
        "historical",
        "history",
        "closed chapter",
        "closed 56 years ago",
        "former chapter",
        "discontinued",
        "yearbook",
    )
    return any(marker in normalized for marker in markers)


def _looks_like_school_article_context(text: str, *, page_url: str = "") -> bool:
    normalized = _normalize_text(" ".join(part for part in [text, page_url] if part))
    if _looks_like_tabbed_chapter_status_page(normalized):
        return False
    parsed = urlparse(page_url)
    path = _normalize_text(parsed.path or "")
    markers = (
        "announcement",
        "announcements",
        "article",
        "articles",
        "blog",
        "blogs",
        "department spotlight",
        "feature story",
        "news",
        "post",
        "posts",
        "spotlight",
        "story",
        "stories",
    )
    return any(marker in normalized or marker in path for marker in markers)


def tool_school_chapter_list_validator(
    *,
    school_name: str,
    fraternity_name: str,
    fraternity_slug: str,
    page_url: str,
    title: str = "",
    text: str = "",
    html: str = "",
) -> PrecisionDecision:
    parsed = urlparse(page_url)
    host = (parsed.netloc or "").lower()
    combined = _normalize_text(" ".join(part for part in [title, text[:4000], page_url] if part))
    soup = BeautifulSoup(html, "html.parser") if html else None
    anchor_texts = [_normalize_text(value) for value in _extract_anchor_texts_from_html(html)]
    list_texts = [_normalize_text(node.get_text(" ", strip=True)) for node in (soup.select("li") if soup is not None else [])]
    heading_texts = [_normalize_text(node.get_text(" ", strip=True)) for node in (soup.select("h1, h2, h3, h4, h5") if soup is not None else [])]
    tabbed_sections = _tabbed_roster_section_texts(soup)
    fraternity_section_texts = tabbed_sections.get("fraternities", [])
    suspended_section_texts = tabbed_sections.get("suspended", [])
    closed_section_texts = tabbed_sections.get("closed", [])
    fraternity_phrases = _fraternity_phrase_candidates(fraternity_name, fraternity_slug)
    school_phrases = _school_phrase_candidates(school_name)
    school_match = _text_contains_any(combined, school_phrases)
    fraternity_match = any(
        _text_contains_any(part, fraternity_phrases)
        for part in [combined, *anchor_texts, *list_texts, *heading_texts, *fraternity_section_texts]
    )
    org_anchor_count = sum(1 for anchor_text in anchor_texts if any(token in anchor_text for token in ("fraternity", "sorority", "chapter")) or len(anchor_text.split()) <= 8)
    org_anchor_count += sum(1 for item_text in list_texts if item_text and len(item_text.split()) <= 10)
    official_list_page = (
        _looks_like_official_chapter_list_page(combined)
        or _looks_like_official_chapter_list_page(" ".join(anchor_texts))
        or _looks_like_tabbed_chapter_status_page(" ".join([combined, " ".join(anchor_texts), " ".join(heading_texts)]))
    )
    fraternity_roster_parts = fraternity_section_texts or [combined, *anchor_texts, *list_texts, *heading_texts]
    fraternity_roster_text = " ".join(part for part in fraternity_roster_parts if part)
    historical_context = _looks_historical_or_archival_context(
        " ".join([combined, fraternity_roster_text, " ".join(anchor_texts), " ".join(list_texts), " ".join(heading_texts)]),
        page_url=page_url,
    )
    article_context = _looks_like_school_article_context(
        " ".join([combined, fraternity_roster_text, " ".join(anchor_texts), " ".join(list_texts), " ".join(heading_texts)]),
        page_url=page_url,
    )
    scorecard_count = sum(1 for anchor_text in anchor_texts if "view scorecard" in anchor_text)
    active_marker_count = sum(1 for part in fraternity_roster_parts if "active" in part)
    chapter_name_entry_count = sum(
        1 for part in fraternity_roster_parts if _looks_like_chapter_name_entry(part)
    )
    active_roster_signal = bool(fraternity_section_texts) or scorecard_count >= 3 or chapter_name_entry_count >= 3 or org_anchor_count >= 3
    conclusive_roster_page = official_list_page and (
        "chapters at" in combined
        or "recognized chapters" in combined
        or "chapter scorecards" in combined
        or "active chapters" in combined
        or _looks_like_tabbed_chapter_status_page(" ".join([combined, " ".join(anchor_texts), " ".join(heading_texts)]))
        or scorecard_count >= 3
        or active_marker_count >= 5
        or (("fraternities" in fraternity_roster_text or "fraternities and sororities" in fraternity_roster_text) and chapter_name_entry_count >= 3)
    )
    roster_excludes_target = not any(
        _text_contains_any(part, fraternity_phrases)
        for part in [fraternity_roster_text, *suspended_section_texts, *closed_section_texts]
        if part
    )

    if not _official_school_source(host, school_name):
        return PrecisionDecision(
            decision="unknown",
            confidence=0.0,
            reason_codes=["non_official_school_source"],
            evidence_urls=[page_url] if page_url else [],
            next_action="review",
        )
    if not school_match and not official_list_page:
        return PrecisionDecision(
            decision="unknown",
            confidence=0.0,
            reason_codes=["school_identity_missing"],
            evidence_urls=[page_url] if page_url else [],
            next_action="review",
        )
    if historical_context:
        return PrecisionDecision(
            decision="unknown",
            confidence=0.15,
            reason_codes=["historical_school_context"],
            evidence_urls=[page_url] if page_url else [],
            next_action="review",
            metadata={"organizationAnchorCount": org_anchor_count, "sourceType": "official_school"},
        )
    if article_context and not conclusive_roster_page:
        return PrecisionDecision(
            decision="unknown",
            confidence=0.2,
            reason_codes=["school_article_context"],
            evidence_urls=[page_url] if page_url else [],
            next_action="review",
            metadata={"organizationAnchorCount": org_anchor_count, "sourceType": "official_school"},
        )
    if official_list_page and active_roster_signal and fraternity_match:
        return PrecisionDecision(
            decision="confirmed_active",
            confidence=0.93,
            reason_codes=["fraternity_present_on_official_school_list"],
            evidence_urls=[page_url] if page_url else [],
            next_action="continue",
            metadata={
                "organizationAnchorCount": org_anchor_count,
                "sourceType": "official_school",
                "activeRosterSection": bool(fraternity_section_texts),
            },
        )
    if conclusive_roster_page and org_anchor_count >= 3 and roster_excludes_target:
        return PrecisionDecision(
            decision="confirmed_inactive",
            confidence=0.9,
            reason_codes=["fraternity_absent_from_official_school_list"],
            evidence_urls=[page_url] if page_url else [],
            next_action="continue",
            metadata={
                "organizationAnchorCount": org_anchor_count,
                "sourceType": "official_school",
                "activeRosterSection": bool(fraternity_section_texts),
            },
        )
    return PrecisionDecision(
        decision="unknown",
        confidence=0.3,
        reason_codes=["official_page_not_conclusive"],
        evidence_urls=[page_url] if page_url else [],
        next_action="review",
        metadata={"organizationAnchorCount": org_anchor_count, "sourceType": "official_school"},
    )


def tool_directory_block_matcher(
    *,
    html: str,
    page_url: str,
    school_name: str,
    fraternity_name: str = "",
    chapter_name: str | None = None,
) -> PrecisionDecision:
    if not html:
        return PrecisionDecision(decision="no_match", confidence=0.0, reason_codes=["empty_html"], evidence_urls=[page_url], next_action="review")
    soup = BeautifulSoup(html, "html.parser")
    school_phrases = _school_phrase_candidates(school_name)
    selected_text = ""
    selected_links: list[str] = []
    selected_greek_tokens: list[str] = []
    best_score = 0

    for element in soup.select("div, li, article, section, a, td"):
        text = element.get_text(" ", strip=True)
        normalized = _normalize_text(text)
        if not normalized:
            continue
        if not _text_contains_any(normalized, school_phrases):
            continue
        score = 3
        greek = tool_greek_detection(value=text).metadata.get("tokens") or []
        if greek:
            score += min(2, len(greek))
        if fraternity_name and _contains_phrase(normalized, fraternity_name):
            score += 1
        links = [urljoin(page_url, str(node.get("href") or "").strip()) for node in element.select("a[href]")]
        links = [link for link in links if link]
        if links:
            score += 1
        if score > best_score:
            best_score = score
            selected_text = text
            selected_links = links[:10]
            selected_greek_tokens = list(greek)

    if best_score <= 0 or not selected_text:
        return PrecisionDecision(
            decision="no_match",
            confidence=0.0,
            reason_codes=["school_block_not_found"],
            evidence_urls=[page_url],
            next_action="review",
        )
    confidence = min(0.96, 0.55 + (best_score * 0.08))
    return PrecisionDecision(
        decision="matched_block",
        confidence=round(confidence, 4),
        reason_codes=["school_block_match", "directory_block_match"],
        evidence_urls=[page_url, *selected_links[:3]],
        next_action="continue",
        metadata={
            "blockText": selected_text[:1200],
            "links": selected_links,
            "greekTokens": selected_greek_tokens,
            "chapterName": chapter_name,
        },
    )


__all__ = [
    "PrecisionDecision",
    "tool_campus_greek_life_policy",
    "tool_directory_block_matcher",
    "tool_directory_layout_profiler",
    "tool_greek_detection",
    "tool_official_domain_verifier",
    "tool_school_chapter_list_validator",
    "tool_same_host_directory_ranker",
    "tool_site_scope_classifier",
    "tool_source_identity_guard",
]
