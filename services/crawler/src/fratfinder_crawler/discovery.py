from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urlparse
import re

from fratfinder_crawler.models import ExistingSourceCandidate, VerifiedSourceRecord
from fratfinder_crawler.search import SearchClient, SearchResult

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
    "chapter",
    "directory",
    "find-a-chapter",
    "findachapter",
    "locations",
    "locator",
    "chapter-roll",
    "our-chapters",
)

_FRATERNITY_ALIASES = {
    "phi-gamma-delta": ("fiji",),
}

_ALIAS_CANONICALS = {
    "fiji": ("phi-gamma-delta", "Phi Gamma Delta"),
}

_FRATERNITY_HOST_HINTS = {
    "phi-gamma-delta": ("phigam.org",),
    "sigma-chi": ("sigmachi.org",),
    "alpha-tau-omega": ("ato.org",),
}

_FRATERNITY_SOURCE_HINTS = {
    "phi-gamma-delta": "https://phigam.org/about/overview/our-chapters/",
    "sigma-chi": "https://sigmachi.org/chapters/",
    "alpha-tau-omega": "https://ato.org/home-2/ato-map/",
}

_FRATERNITY_CONTEXT_MARKERS = (
    "fraternity",
    "chapter",
    "chapters",
    "greek life",
    "brotherhood",
    "interfraternity",
    "ifc",
)

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
)

_INVALID_EXISTING_SOURCE_PARSER_KEYS = {
    "unsupported",
}

_WEAK_SOURCE_PATH_MARKERS = (
    "alumni",
    "alumni-groups",
    "alumnigroups",
    "members",
    "member",
    "memberhub",
    "portal",
    "login",
    "account",
)

_GREEK_SYMBOLS = {
    "Α": "alpha",
    "α": "alpha",
    "Β": "beta",
    "β": "beta",
    "Γ": "gamma",
    "γ": "gamma",
    "Δ": "delta",
    "δ": "delta",
    "Ε": "epsilon",
    "ε": "epsilon",
    "Ζ": "zeta",
    "ζ": "zeta",
    "Η": "eta",
    "η": "eta",
    "Θ": "theta",
    "θ": "theta",
    "Ι": "iota",
    "ι": "iota",
    "Κ": "kappa",
    "κ": "kappa",
    "Λ": "lambda",
    "λ": "lambda",
    "Μ": "mu",
    "μ": "mu",
    "Ν": "nu",
    "ν": "nu",
    "Ξ": "xi",
    "ξ": "xi",
    "Ο": "omicron",
    "ο": "omicron",
    "Π": "pi",
    "π": "pi",
    "Ρ": "rho",
    "ρ": "rho",
    "Σ": "sigma",
    "σ": "sigma",
    "ς": "sigma",
    "Τ": "tau",
    "τ": "tau",
    "Υ": "upsilon",
    "υ": "upsilon",
    "Φ": "phi",
    "φ": "phi",
    "Χ": "chi",
    "χ": "chi",
    "Ψ": "psi",
    "ψ": "psi",
    "Ω": "omega",
    "ω": "omega",
}


class DiscoveryRepository(Protocol):
    def get_verified_source_by_slug(self, fraternity_slug: str) -> VerifiedSourceRecord | None:
        ...

    def get_existing_source_candidates(self, fraternity_slug: str) -> list[ExistingSourceCandidate]:
        ...


def _replace_greek_symbols(value: str) -> str:
    translated = value
    for symbol, replacement in _GREEK_SYMBOLS.items():
        translated = translated.replace(symbol, f" {replacement} ")
    return translated


def _slugify(value: str) -> str:
    lowered = _replace_greek_symbols(value.strip().lower()).replace("&", " and ")
    slug = re.sub(r"[^a-z0-9]+", "-", lowered)
    return slug.strip("-")


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", _replace_greek_symbols(value.lower()))


def _display_tokens(value: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", _replace_greek_symbols(value.lower())) if token]


def _root_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    return f"{parsed.scheme}://{parsed.netloc}"


def _host_is_blocked(host: str) -> bool:
    if not host:
        return False
    normalized = host.lower().strip(".")
    return normalized in _BLOCKED_HOSTS or any(normalized.endswith(f".{suffix}") for suffix in _BLOCKED_HOST_SUFFIXES)


def _host_matches_hint(host: str, hint: str) -> bool:
    normalized_host = host.lower().strip(".")
    normalized_hint = hint.lower().strip(".")
    return normalized_host == normalized_hint or normalized_host.endswith(f".{normalized_hint}")


def _host_matches_any_hint(host: str, hints: tuple[str, ...]) -> bool:
    return any(_host_matches_hint(host, hint) for hint in hints)


def _contains_phrase(text: str, phrase: str) -> bool:
    normalized_text = _compact(text)
    normalized_phrase = _compact(phrase)
    return bool(normalized_phrase and normalized_phrase in normalized_text)


def _normalize_fraternity_identity(fraternity_name: str) -> tuple[str, str, list[dict[str, str]]]:
    trimmed = fraternity_name.strip()
    slug = _slugify(trimmed)
    trace: list[dict[str, str]] = [
        {
            "step": "identity_normalization",
            "input_name": fraternity_name,
            "normalized_name": trimmed,
            "normalized_slug": slug,
        }
    ]

    compact = _compact(trimmed)
    alias_mapping = _ALIAS_CANONICALS.get(compact)
    if alias_mapping:
        canonical_slug, canonical_name = alias_mapping
        trace.append(
            {
                "step": "alias_resolution",
                "alias": trimmed,
                "canonical_name": canonical_name,
                "canonical_slug": canonical_slug,
            }
        )
        return canonical_name, canonical_slug, trace
    return trimmed, slug, trace


def _name_variants(fraternity_name: str, fraternity_slug: str) -> list[str]:
    variants: list[str] = [fraternity_name]
    plain = re.sub(r"[^\w\s]", " ", fraternity_name).strip()
    if plain and plain not in variants:
        variants.append(plain)

    ampersand = fraternity_name.replace(" and ", " & ")
    if ampersand != fraternity_name:
        variants.append(ampersand)

    if fraternity_slug in _FRATERNITY_ALIASES:
        variants.extend(_FRATERNITY_ALIASES[fraternity_slug])

    deduped: list[str] = []
    seen: set[str] = set()
    for item in variants:
        token = item.strip()
        if not token:
            continue
        key = _compact(token)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(token)
    return deduped


@dataclass(slots=True)
class DiscoveryCandidate:
    title: str
    url: str
    snippet: str
    provider: str
    rank: int
    score: float


@dataclass(slots=True)
class DiscoveryResult:
    fraternity_name: str
    fraternity_slug: str
    selected_url: str | None
    selected_confidence: float
    confidence_tier: str
    candidates: list[DiscoveryCandidate]
    source_provenance: str | None
    fallback_reason: str | None
    resolution_trace: list[dict[str, Any]]

    def as_dict(self) -> dict:
        return {
            "fraternity_name": self.fraternity_name,
            "fraternity_slug": self.fraternity_slug,
            "selected_url": self.selected_url,
            "selected_confidence": self.selected_confidence,
            "confidence_tier": self.confidence_tier,
            "candidates": [asdict(candidate) for candidate in self.candidates],
            "source_provenance": self.source_provenance,
            "fallback_reason": self.fallback_reason,
            "resolution_trace": self.resolution_trace,
        }


def _score_candidate(fraternity_name: str, fraternity_slug: str, result: SearchResult) -> float:
    score = 0.36
    lowered_title = (result.title or "").lower()
    lowered_snippet = (result.snippet or "").lower()
    parsed = urlparse(result.url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    combined = f"{lowered_title} {lowered_snippet} {path}"

    if _contains_phrase(lowered_title, fraternity_name):
        score += 0.2
    if _contains_phrase(lowered_snippet, fraternity_name):
        score += 0.1

    fraternity_compact = _compact(fraternity_name)
    if fraternity_compact and fraternity_compact in _compact(host):
        score += 0.16

    trusted_host_hints = _FRATERNITY_HOST_HINTS.get(fraternity_slug, ())
    if trusted_host_hints and _host_matches_any_hint(host, trusted_host_hints):
        score += 0.35

    tokens = _display_tokens(fraternity_name)
    host_text = _compact(host)
    token_hits = sum(1 for token in tokens if token and token in host_text)
    if token_hits >= 2:
        score += 0.12
    elif token_hits == 1:
        score += 0.06

    if any(marker in path for marker in _DIRECTORY_MARKERS):
        score += 0.12

    if any(marker in lowered_title for marker in ("official", "international", "fraternity")):
        score += 0.08

    if any(marker in lowered_snippet for marker in ("official", "chapter", "directory", "fraternity")):
        score += 0.06

    if any(marker in combined for marker in ("alumni chapter", "alumni association", "alumni club", "alumni")):
        score -= 0.3
    if path.endswith(".pdf"):
        score -= 0.25

    context_hits = sum(1 for marker in _FRATERNITY_CONTEXT_MARKERS if marker in combined)
    if context_hits == 0:
        score -= 0.2
    elif context_hits >= 2:
        score += 0.08

    if any(marker in combined for marker in _NON_ORG_CONTEXT_MARKERS) and context_hits == 0:
        score -= 0.18

    if any(marker in combined for marker in ("travel", "vacation", "resort", "island")) and context_hits == 0:
        score -= 0.2

    if host in _BLOCKED_HOSTS:
        score -= 0.45

    score += max(0.0, 0.08 - (max(result.rank, 1) - 1) * 0.01)
    return max(0.0, min(0.99, score))


def _text_has_directory_signal(*values: str) -> bool:
    combined = " ".join(value.lower() for value in values if value)
    return any(marker in combined for marker in _DIRECTORY_MARKERS)


def _confidence_tier(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def _is_healthy_http_status(http_status: int | None) -> bool:
    return http_status is not None and 200 <= http_status < 400


def _parse_ts(value: str | None) -> datetime:
    if not value:
        return datetime.min
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def _verified_health_rank(candidate: VerifiedSourceRecord | None, min_confidence: float) -> int:
    if candidate is None:
        return 0
    if candidate.is_active and _is_healthy_http_status(candidate.http_status) and candidate.confidence >= min_confidence:
        return 3
    if candidate.is_active and candidate.confidence >= min_confidence:
        return 2
    if candidate.is_active:
        return 1
    return 0


def _evaluate_verified_source_candidate(
    candidate: VerifiedSourceRecord | None,
    fraternity_name: str,
    fraternity_slug: str,
) -> tuple[bool, list[str]]:
    if candidate is None:
        return False, ["missing"]

    parsed = urlparse(candidate.national_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    combined = f"{host} {path}"
    reasons: list[str] = []

    if parsed.scheme not in {"http", "https"}:
        reasons.append("non_http_url")
    if _host_is_blocked(host):
        reasons.append("blocked_host")
    if any(marker in path for marker in _WEAK_SOURCE_PATH_MARKERS):
        reasons.append("member_or_alumni_path")

    context_hits = sum(1 for marker in _DIRECTORY_MARKERS if marker in combined)
    if _contains_phrase(combined, fraternity_name):
        context_hits += 1

    trusted_host_hints = _FRATERNITY_HOST_HINTS.get(fraternity_slug, ())
    hinted_source = _FRATERNITY_SOURCE_HINTS.get(fraternity_slug)
    hinted_path = (urlparse(hinted_source).path or "").strip("/").lower() if hinted_source else ""
    if trusted_host_hints and _host_matches_any_hint(host, trusted_host_hints):
        context_hits += 1

    if path.strip("/") == "" and hinted_source and hinted_path and hinted_path != path.strip("/"):
        reasons.append("generic_root_path")
    elif context_hits == 0 and path.strip("/") == "":
        reasons.append("generic_root_path")

    if trusted_host_hints and _host_matches_any_hint(host, trusted_host_hints):
        if not (hinted_source and hinted_path and hinted_path != path.strip("/")):
            reasons = [reason for reason in reasons if reason != "generic_root_path"]

    return not reasons, reasons


def _existing_health_rank(candidate: ExistingSourceCandidate | None) -> int:
    if candidate is None:
        return 0
    if candidate.last_run_status == "succeeded":
        return 3
    if candidate.active and candidate.last_run_status in {"partial", "running"}:
        return 2
    if candidate.active:
        return 1
    return 0


def _choose_existing_candidate(candidates: list[ExistingSourceCandidate]) -> ExistingSourceCandidate | None:
    if not candidates:
        return None
    ranked = sorted(
        candidates,
        key=lambda item: (
            _existing_health_rank(item),
            _parse_ts(item.last_success_at),
            item.confidence,
            item.source_slug,
        ),
        reverse=True,
    )
    return ranked[0]


def _build_search_queries(fraternity_name: str, fraternity_slug: str) -> list[str]:
    variants = _name_variants(fraternity_name, fraternity_slug)
    queries: list[str] = []
    for variant in variants:
        queries.extend(
            [
                f'"{variant}" national fraternity website',
                f'"{variant}" fraternity national website',
                f'"{variant}" chapter directory',
                f'"{variant}" official fraternity',
                f'"{variant}" find a chapter',
                f'"{variant}" chapter roll',
            ]
        )

    host_hints = _FRATERNITY_HOST_HINTS.get(fraternity_slug, ())
    for hint in host_hints:
        queries.extend(
            [
                f'"{fraternity_name}" "{hint}" fraternity',
                f'"{hint}" chapter directory fraternity',
            ]
        )

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        key = query.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(query)
    return deduped


def _candidate_from_verified_source(candidate: VerifiedSourceRecord) -> DiscoveryCandidate:
    snippet = f"Verified source registry ({candidate.origin})"
    if candidate.http_status is not None:
        snippet = f"{snippet}; http_status={candidate.http_status}"
    return DiscoveryCandidate(
        title=f"{candidate.fraternity_name} verified source",
        url=candidate.national_url,
        snippet=snippet,
        provider="verified_registry",
        rank=1,
        score=max(0.0, min(0.99, candidate.confidence)),
    )


def _candidate_from_existing_source(candidate: ExistingSourceCandidate, fraternity_name: str) -> DiscoveryCandidate:
    status_text = candidate.last_run_status or "unknown"
    return DiscoveryCandidate(
        title=f"{fraternity_name} existing configured source ({candidate.source_slug})",
        url=candidate.list_url,
        snippet=f"Existing source with last_run_status={status_text}; parser_key={candidate.parser_key}",
        provider="existing_source",
        rank=1,
        score=max(0.0, min(0.99, candidate.confidence)),
    )


def _evaluate_existing_source_candidate(
    candidate: ExistingSourceCandidate | None,
    fraternity_name: str,
    fraternity_slug: str,
) -> tuple[bool, list[str]]:
    if candidate is None:
        return False, ["missing"]

    parsed = urlparse(candidate.list_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    combined = f"{host} {path}"
    reasons: list[str] = []

    if parsed.scheme not in {"http", "https"}:
        reasons.append("non_http_url")
    if _host_is_blocked(host):
        reasons.append("blocked_host")
    if candidate.parser_key in _INVALID_EXISTING_SOURCE_PARSER_KEYS:
        reasons.append("unsupported_parser")
    if any(marker in path for marker in _WEAK_SOURCE_PATH_MARKERS):
        reasons.append("member_or_alumni_path")

    fraternity_compact = _compact(fraternity_name)
    host_text = _compact(host)
    context_hits = sum(1 for marker in _FRATERNITY_CONTEXT_MARKERS if marker in combined)
    if fraternity_compact and fraternity_compact in host_text:
        context_hits += 1
    if any(marker in combined for marker in _DIRECTORY_MARKERS):
        context_hits += 1
    if any(marker in combined for marker in _NON_ORG_CONTEXT_MARKERS):
        reasons.append("non_org_context")
    if context_hits == 0:
        reasons.append("missing_fraternity_context")

    trusted_host_hints = _FRATERNITY_HOST_HINTS.get(fraternity_slug, ())
    hinted_source = _FRATERNITY_SOURCE_HINTS.get(fraternity_slug)
    hinted_path = (urlparse(hinted_source).path or "").strip("/").lower() if hinted_source else ""
    if trusted_host_hints and _host_matches_any_hint(host, trusted_host_hints):
        reasons = [reason for reason in reasons if reason != "missing_fraternity_context"]
        if path.strip("/") == "" and not (hinted_source and hinted_path and hinted_path != path.strip("/")):
            reasons = [reason for reason in reasons if reason != "generic_root_path"]

    if path.strip("/") == "" and hinted_source and hinted_path and hinted_path != path.strip("/"):
        reasons.append("generic_root_path")
    elif context_hits == 0 and path.strip("/") == "":
        reasons.append("generic_root_path")

    return not reasons, reasons


def discover_source(
    fraternity_name: str,
    search_client: SearchClient,
    repository: DiscoveryRepository | None = None,
    *,
    max_candidates: int = 5,
    verified_min_confidence: float = 0.65,
) -> DiscoveryResult:
    normalized_name, slug, trace = _normalize_fraternity_identity(fraternity_name)

    if not normalized_name:
        return DiscoveryResult(
            fraternity_name=fraternity_name,
            fraternity_slug="",
            selected_url=None,
            selected_confidence=0.0,
            confidence_tier="low",
            candidates=[],
            source_provenance=None,
            fallback_reason="empty_fraternity_name",
            resolution_trace=trace,
        )

    selected_url: str | None = None
    selected_confidence = 0.0
    source_provenance: str | None = None
    fallback_reason: str | None = None
    registry_candidate: VerifiedSourceRecord | None = None
    existing_candidate: ExistingSourceCandidate | None = None
    synthetic_candidates: list[DiscoveryCandidate] = []

    if repository is not None:
        registry_candidate = repository.get_verified_source_by_slug(slug)
        existing_candidates = repository.get_existing_source_candidates(slug)
        existing_candidate = _choose_existing_candidate(existing_candidates)
        verified_candidate_valid, verified_candidate_reasons = _evaluate_verified_source_candidate(
            registry_candidate,
            normalized_name,
            slug,
        )
        existing_candidate_valid, existing_candidate_reasons = _evaluate_existing_source_candidate(
            existing_candidate,
            normalized_name,
            slug,
        )

        if registry_candidate is not None:
            trace.append(
                {
                    "step": "checked_verified_registry",
                    "slug": slug,
                    "found": True,
                    "is_active": registry_candidate.is_active,
                    "http_status": registry_candidate.http_status,
                    "confidence": registry_candidate.confidence,
                    "national_url": registry_candidate.national_url,
                    "candidate_valid": verified_candidate_valid,
                    "candidate_reasons": verified_candidate_reasons,
                }
            )
            if verified_candidate_valid:
                synthetic_candidates.append(_candidate_from_verified_source(registry_candidate))
        else:
            trace.append({"step": "checked_verified_registry", "slug": slug, "found": False})

        if existing_candidate is not None:
            trace.append(
                {
                    "step": "checked_existing_sources",
                    "slug": slug,
                    "source_slug": existing_candidate.source_slug,
                    "list_url": existing_candidate.list_url,
                    "active": existing_candidate.active,
                    "source_type": existing_candidate.source_type,
                    "parser_key": existing_candidate.parser_key,
                    "last_run_status": existing_candidate.last_run_status,
                    "last_success_at": existing_candidate.last_success_at,
                    "confidence": existing_candidate.confidence,
                    "candidate_valid": existing_candidate_valid,
                    "candidate_reasons": existing_candidate_reasons,
                }
            )
            if existing_candidate_valid:
                synthetic_candidates.append(_candidate_from_existing_source(existing_candidate, normalized_name))
        else:
            trace.append({"step": "checked_existing_sources", "slug": slug, "found": False})
            existing_candidate_valid = False
            existing_candidate_reasons = ["missing"]
    else:
        verified_candidate_valid = False
        verified_candidate_reasons: list[str] = ["repository_unavailable"]
        existing_candidate_valid = False
        existing_candidate_reasons: list[str] = ["repository_unavailable"]

    verified_rank = _verified_health_rank(registry_candidate, verified_min_confidence)
    existing_rank = _existing_health_rank(existing_candidate) if existing_candidate_valid else 0
    if registry_candidate is not None and verified_rank >= 2 and verified_candidate_valid:
        selected_url = registry_candidate.national_url
        selected_confidence = registry_candidate.confidence
        source_provenance = "verified_registry"
        trace.append(
            {
                "step": "selected_verified_registry_candidate",
                "url": selected_url,
                "health_rank": verified_rank,
                "confidence": selected_confidence,
            }
        )
    elif registry_candidate is not None and not verified_candidate_valid:
        fallback_reason = fallback_reason or "verified_source_invalid"
        trace.append(
            {
                "step": "rejected_verified_registry_candidate",
                "url": registry_candidate.national_url,
                "reasons": verified_candidate_reasons,
            }
        )

    if existing_candidate is not None and selected_url is None:
        if existing_candidate_valid:
            selected_url = existing_candidate.list_url
            selected_confidence = existing_candidate.confidence
            source_provenance = "existing_source"
            trace.append(
                {
                    "step": "selected_existing_source_candidate",
                    "url": selected_url,
                    "health_rank": existing_rank,
                    "confidence": selected_confidence,
                }
            )
        else:
            fallback_reason = fallback_reason or "existing_source_invalid"
            trace.append(
                {
                    "step": "rejected_existing_source_candidate",
                    "url": existing_candidate.list_url,
                    "reasons": existing_candidate_reasons,
                }
            )

    if (
        selected_url is not None
        and source_provenance == "verified_registry"
        and existing_candidate is not None
        and existing_candidate.list_url != selected_url
    ):
        verified_tuple = (verified_rank, _parse_ts(registry_candidate.checked_at if registry_candidate else None), selected_confidence)
        existing_tuple = (existing_rank, _parse_ts(existing_candidate.last_success_at), existing_candidate.confidence)
        if existing_tuple > verified_tuple:
            selected_url = existing_candidate.list_url
            selected_confidence = existing_candidate.confidence
            source_provenance = "existing_source"
            fallback_reason = "registry_disagreed_preferred_existing_source"
            trace.append(
                {
                    "step": "resolved_registry_existing_conflict",
                    "decision": "existing_source",
                    "reason": fallback_reason,
                    "selected_url": selected_url,
                }
            )
        else:
            trace.append(
                {
                    "step": "resolved_registry_existing_conflict",
                    "decision": "verified_registry",
                    "selected_url": selected_url,
                }
            )

    search_candidates: list[DiscoveryCandidate] = []
    if selected_url is None:
        if registry_candidate is not None or existing_candidate is not None:
            fallback_reason = fallback_reason or "registry_or_existing_not_healthy_enough"
        queries = _build_search_queries(normalized_name, slug)
        raw_results: list[SearchResult] = []
        for query in queries:
            results = search_client.search(query, max_results=max_candidates)
            raw_results.extend(results)

        deduped: dict[str, SearchResult] = {}
        for result in raw_results:
            key = _root_url(result.url)
            if key not in deduped or result.rank < deduped[key].rank:
                deduped[key] = result

        for result in deduped.values():
            search_candidates.append(
                DiscoveryCandidate(
                    title=result.title,
                    url=result.url,
                    snippet=result.snippet,
                    provider=result.provider,
                    rank=result.rank,
                    score=_score_candidate(normalized_name, slug, result),
                )
            )

        search_candidates.sort(key=lambda item: (item.score, -item.rank, item.url), reverse=True)
        if search_candidates and search_candidates[0].score >= 0.6:
            top_candidate = search_candidates[0]
            top_parsed = urlparse(top_candidate.url)
            top_combined = f"{(top_candidate.title or '').lower()} {(top_candidate.snippet or '').lower()} {(top_parsed.path or '').lower()}"
            trusted_host_hints = _FRATERNITY_HOST_HINTS.get(slug, ())
            hinted_source = _FRATERNITY_SOURCE_HINTS.get(slug)
            top_matches_trusted_host = _host_matches_any_hint((top_parsed.netloc or "").lower(), trusted_host_hints)
            top_is_noisy = any(marker in top_combined for marker in ("alumni chapter", "alumni association", "alumni club", "alumni")) or (
                (top_parsed.path or "").lower().endswith(".pdf")
            )
            top_has_directory_signal = _text_has_directory_signal(top_candidate.title or "", top_candidate.snippet or "", top_parsed.path or "")
            hinted_path = (urlparse(hinted_source).path or "").lower() if hinted_source else ""
            hinted_has_directory_signal = _text_has_directory_signal(hinted_path)
            should_use_hinted_directory = bool(
                hinted_source
                and hinted_has_directory_signal
                and top_matches_trusted_host
                and not top_has_directory_signal
            )

            if hinted_source and ((top_is_noisy and not top_matches_trusted_host) or should_use_hinted_directory):
                selected_url = hinted_source
                selected_confidence = max(top_candidate.score, 0.7)
                source_provenance = "search"
                fallback_reason = fallback_reason or (
                    "used_curated_source_hint_over_noisy_search"
                    if top_is_noisy and not top_matches_trusted_host
                    else "used_curated_source_hint_over_generic_same_host_page"
                )
                trace.append(
                    {
                        "step": (
                            "selected_curated_source_hint_over_noisy_search"
                            if top_is_noisy and not top_matches_trusted_host
                            else "selected_curated_source_hint_over_generic_same_host_page"
                        ),
                        "hinted_url": hinted_source,
                        "rejected_url": top_candidate.url,
                        "rejected_score": top_candidate.score,
                    }
                )
            else:
                selected_url = top_candidate.url
                selected_confidence = top_candidate.score
                source_provenance = "search"
                trace.append(
                    {
                        "step": "selected_search_candidate",
                        "url": selected_url,
                        "score": selected_confidence,
                        "provider": top_candidate.provider,
                    }
                )
        else:
            hinted_source = _FRATERNITY_SOURCE_HINTS.get(slug)
            if hinted_source:
                hinted_candidate = DiscoveryCandidate(
                    title=f"{normalized_name} Official Chapter Directory",
                    url=hinted_source,
                    snippet="Curated official source hint for fraternity chapter discovery.",
                    provider="curated_hint",
                    rank=1,
                    score=0.85,
                )
                search_candidates.insert(0, hinted_candidate)
                selected_url = hinted_source
                selected_confidence = hinted_candidate.score
                source_provenance = "search"
                fallback_reason = fallback_reason or "used_curated_source_hint_after_search"
                trace.append(
                    {
                        "step": "selected_curated_hint",
                        "url": selected_url,
                        "score": selected_confidence,
                    }
                )
            else:
                trace.append({"step": "search_fallback_exhausted", "queries_executed": len(queries)})

    combined_candidates = [*synthetic_candidates, *search_candidates]
    deduped_candidates: list[DiscoveryCandidate] = []
    seen_candidate_urls: set[str] = set()
    for candidate in sorted(combined_candidates, key=lambda item: (item.score, -item.rank, item.url), reverse=True):
        root = _root_url(candidate.url)
        if root in seen_candidate_urls:
            continue
        seen_candidate_urls.add(root)
        deduped_candidates.append(candidate)
        if len(deduped_candidates) >= max_candidates:
            break

    return DiscoveryResult(
        fraternity_name=normalized_name,
        fraternity_slug=slug,
        selected_url=selected_url,
        selected_confidence=selected_confidence if selected_url else 0.0,
        confidence_tier=_confidence_tier(selected_confidence if selected_url else 0.0),
        candidates=deduped_candidates,
        source_provenance=source_provenance,
        fallback_reason=fallback_reason,
        resolution_trace=trace,
    )

