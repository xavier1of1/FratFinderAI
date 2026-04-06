from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urlparse
import re
import unicodedata

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
    "pi-kappa-alpha": ("pike", "pka"),
    "tau-kappa-epsilon": ("tke", "tekes"),
    "sigma-alpha-epsilon": ("sae",),
    "sigma-phi-epsilon": ("sigep",),
    "alpha-tau-omega": ("ato",),
    "kappa-sigma": ("ksig",),
}

_ALIAS_CANONICALS = {
    "fiji": ("phi-gamma-delta", "Phi Gamma Delta"),
    "pike": ("pi-kappa-alpha", "Pi Kappa Alpha"),
    "pka": ("pi-kappa-alpha", "Pi Kappa Alpha"),
    "pikappaalpha": ("pi-kappa-alpha", "Pi Kappa Alpha"),
    "tke": ("tau-kappa-epsilon", "Tau Kappa Epsilon"),
    "tekes": ("tau-kappa-epsilon", "Tau Kappa Epsilon"),
    "taukappaepsilon": ("tau-kappa-epsilon", "Tau Kappa Epsilon"),
    "sae": ("sigma-alpha-epsilon", "Sigma Alpha Epsilon"),
    "sigep": ("sigma-phi-epsilon", "Sigma Phi Epsilon"),
    "ato": ("alpha-tau-omega", "Alpha Tau Omega"),
    "ksig": ("kappa-sigma", "Kappa Sigma"),
}

_FRATERNITY_HOST_HINTS = {
    "phi-gamma-delta": ("phigam.org",),
    "sigma-chi": ("sigmachi.org",),
    "alpha-tau-omega": ("ato.org",),
    "pi-kappa-alpha": ("pikes.org", "pikapp.org"),
    "tau-kappa-epsilon": ("tke.org",),
    "kappa-sigma": ("kappasigma.org",),
    "theta-xi": ("thetaxi.org",),
}

_FRATERNITY_SOURCE_HINTS = {
    "phi-gamma-delta": "https://phigam.org/about/overview/our-chapters/",
    "sigma-chi": "https://sigmachi.org/chapters/",
    "alpha-tau-omega": "https://ato.org/home-2/ato-map/",
    "pi-kappa-alpha": "https://pikes.org/chapters/",
    "tau-kappa-epsilon": "https://www.tke.org/chapters",
    "theta-xi": "https://www.thetaxi.org/chapters-and-colonies/",
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

_WEAK_SOURCE_HOST_MARKERS = (
    "dynamic.omegafi.com",
    "omegafi.com",
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

_ADDITIONAL_GREEK_SYMBOLS = {
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

_ADDITIONAL_GREEK_SYMBOLS = {
    "\u00ce\u0091": "alpha",
    "\u00ce\u00b1": "alpha",
    "\u00ce\u00a0": "pi",
    "\u00cf\u0080": "pi",
    "\u00ce\u00a4": "tau",
    "\u00cf\u0084": "tau",
    "\u00ce\u009a": "kappa",
    "\u00ce\u00ba": "kappa",
    "\u00ce\u0095": "epsilon",
    "\u00ce\u00b5": "epsilon",
    "\u00ce\u00a7": "chi",
    "\u00cf\u0087": "chi",
    "\u00ce\u00a8": "psi",
    "\u00cf\u0088": "psi",
}


class DiscoveryRepository(Protocol):
    def get_verified_source_by_slug(self, fraternity_slug: str) -> VerifiedSourceRecord | None:
        ...

    def get_existing_source_candidates(self, fraternity_slug: str) -> list[ExistingSourceCandidate]:
        ...

    def list_verified_sources(self, limit: int = 200) -> list[VerifiedSourceRecord]:
        ...


def _replace_greek_symbols(value: str) -> str:
    translated = unicodedata.normalize("NFKC", value)
    for symbol, replacement in _GREEK_SYMBOLS.items():
        translated = translated.replace(symbol, f" {replacement} ")
    for symbol, replacement in _ADDITIONAL_GREEK_SYMBOLS.items():
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


def _fraternity_acronym(fraternity_name: str) -> str:
    tokens = _display_tokens(fraternity_name)
    return "".join(token[0] for token in tokens if token)


def _resolve_alias_from_repository(
    fraternity_name: str,
    fraternity_slug: str,
    repository: DiscoveryRepository | None,
    trace: list[dict[str, Any]],
) -> tuple[str, str]:
    if repository is None:
        return fraternity_name, fraternity_slug

    list_fn = getattr(repository, "list_verified_sources", None)
    if not callable(list_fn):
        return fraternity_name, fraternity_slug

    target_compact = _compact(fraternity_name)
    target_tokens = set(_display_tokens(fraternity_name))
    target_acronym = _fraternity_acronym(fraternity_name)
    if not target_compact:
        return fraternity_name, fraternity_slug

    try:
        verified_sources = list_fn(limit=250)
    except Exception as exc:  # pragma: no cover - defensive repository path
        trace.append({"step": "repository_alias_resolution_failed", "error": str(exc)})
        return fraternity_name, fraternity_slug

    best_score = 0
    best_match: VerifiedSourceRecord | None = None
    for candidate in verified_sources:
        candidate_name = str(candidate.fraternity_name or "").strip()
        candidate_slug = str(candidate.fraternity_slug or "").strip()
        if not candidate_name or not candidate_slug:
            continue

        candidate_compact = _compact(candidate_name)
        candidate_slug_compact = _compact(candidate_slug)
        candidate_tokens = set(_display_tokens(candidate_name))
        candidate_acronym = _fraternity_acronym(candidate_name)

        score = 0
        if target_compact == candidate_compact or target_compact == candidate_slug_compact:
            score += 6
        overlap = len(target_tokens.intersection(candidate_tokens))
        if overlap > 0:
            score += overlap
        if target_acronym and target_acronym == candidate_acronym:
            score += 4

        if score > best_score:
            best_score = score
            best_match = candidate

    if best_match is None or best_match.fraternity_slug == fraternity_slug or best_score < 4:
        return fraternity_name, fraternity_slug

    trace.append(
        {
            "step": "alias_resolution",
            "alias": fraternity_name,
            "canonical_name": best_match.fraternity_name,
            "canonical_slug": best_match.fraternity_slug,
            "reason": "repository_alias_match",
            "score": best_score,
        }
    )
    return best_match.fraternity_name, best_match.fraternity_slug


def _normalize_fraternity_identity(fraternity_name: str) -> tuple[str, str, list[dict[str, str]]]:
    trimmed = _replace_greek_symbols(fraternity_name).strip()
    slug = _slugify(trimmed)
    trace: list[dict[str, str]] = [
        {
            "step": "identity_normalization",
            "input_name": fraternity_name,
            "normalized_name": trimmed,
            "normalized_slug": slug,
            "acronym": _fraternity_acronym(trimmed),
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

    acronym = _fraternity_acronym(fraternity_name)
    if acronym:
        variants.append(acronym.upper())

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
class DiscoverySourceQuality:
    score: float
    is_weak: bool
    is_blocked: bool
    reasons: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "is_weak": self.is_weak,
            "is_blocked": self.is_blocked,
            "reasons": self.reasons,
        }


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
    source_quality: DiscoverySourceQuality | None
    selected_candidate_rationale: str | None
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
            "source_quality": self.source_quality.as_dict() if self.source_quality else None,
            "selected_candidate_rationale": self.selected_candidate_rationale,
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



def _source_quality_from_url(url: str | None) -> DiscoverySourceQuality:
    if not url:
        return DiscoverySourceQuality(score=0.0, is_weak=True, is_blocked=False, reasons=["missing_url"])

    try:
        parsed = urlparse(url)
    except Exception:
        return DiscoverySourceQuality(score=0.0, is_weak=True, is_blocked=False, reasons=["invalid_url"])

    host = (parsed.netloc or "").lower().strip(".")
    path = (parsed.path or "").lower()
    reasons: list[str] = []
    score = 0.56

    if parsed.scheme not in {"http", "https"}:
        reasons.append("non_http_url")
        score -= 0.4

    is_blocked = _host_is_blocked(host)
    if is_blocked:
        reasons.append("blocked_host")
        score -= 0.72

    if any(marker in path for marker in _DIRECTORY_MARKERS):
        reasons.append("directory_path")
        score += 0.24

    weak_markers = [marker for marker in _WEAK_SOURCE_PATH_MARKERS if marker in path]
    if weak_markers:
        reasons.append("weak_path")
        score -= min(0.45, 0.18 * len(weak_markers))

    if path.strip("/") == "":
        reasons.append("generic_root_path")
        score -= 0.08

    score = max(0.0, min(1.0, score))
    return DiscoverySourceQuality(
        score=round(score, 4),
        is_weak=is_blocked or score < 0.4 or "weak_path" in reasons,
        is_blocked=is_blocked,
        reasons=reasons,
    )


def _search_candidate_is_runnable(candidate: DiscoveryCandidate) -> tuple[bool, DiscoverySourceQuality, list[str]]:
    quality = _source_quality_from_url(candidate.url)
    reasons = list(quality.reasons)
    combined = f"{candidate.title} {candidate.snippet}".lower()
    if any(marker in combined for marker in _NON_ORG_CONTEXT_MARKERS):
        reasons.append("non_org_context")
    if "alumni" in combined and "international" not in combined and "headquarters" not in combined:
        reasons.append("noisy_alumni_context")
    if candidate.score < 0.5:
        reasons.append("low_candidate_score")
    runnable = not quality.is_weak and "non_org_context" not in reasons and "noisy_alumni_context" not in reasons
    return runnable, quality, reasons

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
    if any(marker in host for marker in _WEAK_SOURCE_HOST_MARKERS):
        reasons.append("hosted_member_portal")
    if any(marker in path for marker in _WEAK_SOURCE_PATH_MARKERS):
        reasons.append("member_or_alumni_path")
    if candidate.last_run_status in {"partial", "failed"} and not candidate.last_success_at:
        reasons.append("no_success_history")

    fraternity_compact = _compact(fraternity_name)
    host_text = _compact(host)
    context_hits = sum(1 for marker in _FRATERNITY_CONTEXT_MARKERS if marker in combined)
    if fraternity_compact and fraternity_compact in host_text:
        context_hits += 1
    if any(marker in combined for marker in _DIRECTORY_MARKERS):
        context_hits += 1
    if any(marker in combined for marker in _NON_ORG_CONTEXT_MARKERS):
        reasons.append("non_org_context")
    if "alumni" in combined and "international" not in combined and "headquarters" not in combined:
        reasons.append("noisy_alumni_context")
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
    normalized_name, slug = _resolve_alias_from_repository(normalized_name, slug, repository, trace)

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
            source_quality=_source_quality_from_url(None),
            selected_candidate_rationale=None,
            resolution_trace=trace,
        )

    selected_url: str | None = None
    selected_confidence = 0.0
    source_provenance: str | None = None
    fallback_reason: str | None = None
    source_quality: DiscoverySourceQuality | None = None
    selected_candidate_rationale: str | None = None
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
        source_quality = _source_quality_from_url(selected_url)
        selected_candidate_rationale = "verified_registry_healthy"
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
            source_quality = _source_quality_from_url(selected_url)
            selected_candidate_rationale = "existing_source_candidate"
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
            source_quality = _source_quality_from_url(selected_url)
            selected_candidate_rationale = "existing_source_newer_and_healthier"
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
            try:
                results = search_client.search(query, max_results=max_candidates)
                raw_results.extend(results)
            except Exception as exc:
                provider_attempts: list[dict[str, Any]] = []
                consume_attempts = getattr(search_client, "consume_last_provider_attempts", None)
                if callable(consume_attempts):
                    try:
                        provider_attempts = list(consume_attempts())
                    except Exception:
                        provider_attempts = []
                trace.append(
                    {
                        "step": "search_query_error",
                        "query": query,
                        "error": str(exc),
                        "provider_attempts": provider_attempts,
                    }
                )

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
        if search_candidates:
            selected_search_candidate: DiscoveryCandidate | None = None
            selected_search_quality: DiscoverySourceQuality | None = None
            rejected_candidates: list[dict[str, Any]] = []
            top_candidate = search_candidates[0]

            for candidate in search_candidates:
                runnable, candidate_quality, candidate_reasons = _search_candidate_is_runnable(candidate)
                if runnable and candidate.score >= 0.6:
                    selected_search_candidate = candidate
                    selected_search_quality = candidate_quality
                    break
                rejected_candidates.append(
                    {
                        "url": candidate.url,
                        "provider": candidate.provider,
                        "score": candidate.score,
                        "reasons": candidate_reasons,
                        "quality": candidate_quality.as_dict(),
                    }
                )

            if rejected_candidates:
                trace.append({"step": "rejected_search_candidates", "rejected": rejected_candidates})

            if selected_search_candidate is not None:
                hinted_source = _FRATERNITY_SOURCE_HINTS.get(slug)
                hinted_quality = _source_quality_from_url(hinted_source) if hinted_source else _source_quality_from_url(None)
                selected_parsed = urlparse(selected_search_candidate.url)
                selected_has_directory_signal = _text_has_directory_signal(
                    selected_search_candidate.title,
                    selected_search_candidate.snippet,
                    selected_parsed.path,
                )
                hinted_path = (urlparse(hinted_source).path or "").lower() if hinted_source else ""
                hinted_has_directory_signal = _text_has_directory_signal(hinted_path)
                selected_matches_hinted_host = bool(
                    hinted_source and _host_matches_hint((selected_parsed.netloc or "").lower(), (urlparse(hinted_source).netloc or "").lower())
                )

                if hinted_source and not hinted_quality.is_weak and selected_matches_hinted_host and not selected_has_directory_signal and hinted_has_directory_signal:
                    selected_url = hinted_source
                    selected_confidence = max(selected_search_candidate.score, 0.7)
                    source_provenance = "search"
                    source_quality = hinted_quality
                    selected_candidate_rationale = "curated_hint_over_generic_same_host_page"
                    fallback_reason = fallback_reason or "used_curated_source_hint_over_generic_same_host_page"
                    trace.append(
                        {
                            "step": "selected_curated_source_hint_over_generic_same_host_page",
                            "hinted_url": hinted_source,
                            "rejected_url": selected_search_candidate.url,
                            "rejected_score": selected_search_candidate.score,
                        }
                    )
                else:
                    selected_url = selected_search_candidate.url
                    selected_confidence = selected_search_candidate.score
                    source_provenance = "search"
                    source_quality = selected_search_quality
                    if selected_search_candidate.url != top_candidate.url:
                        selected_candidate_rationale = "promoted_safe_candidate_over_top_rejected"
                        fallback_reason = fallback_reason or "promoted_non_blocked_candidate"
                    else:
                        selected_candidate_rationale = "selected_search_candidate"
                    trace.append(
                        {
                            "step": "selected_search_candidate",
                            "url": selected_url,
                            "score": selected_confidence,
                            "provider": selected_search_candidate.provider,
                            "rationale": selected_candidate_rationale,
                        }
                    )
            else:
                hinted_source = _FRATERNITY_SOURCE_HINTS.get(slug)
                hinted_quality = _source_quality_from_url(hinted_source) if hinted_source else _source_quality_from_url(None)
                if hinted_source and not hinted_quality.is_weak:
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
                    source_quality = hinted_quality
                    selected_candidate_rationale = "curated_hint_safe_fallback"
                    fallback_reason = fallback_reason or "used_curated_source_hint_after_search"
                    noisy_rejected = any("noisy_alumni_context" in (entry.get("reasons") or []) for entry in rejected_candidates)
                    trace.append(
                        {
                            "step": "selected_curated_source_hint_over_noisy_search" if noisy_rejected else "selected_curated_hint",
                            "url": selected_url,
                            "score": selected_confidence,
                        }
                    )
                else:
                    fallback_reason = fallback_reason or "no_safe_candidate"
        else:
            hinted_source = _FRATERNITY_SOURCE_HINTS.get(slug)
            hinted_quality = _source_quality_from_url(hinted_source) if hinted_source else _source_quality_from_url(None)
            if hinted_source and not hinted_quality.is_weak:
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
                source_quality = hinted_quality
                selected_candidate_rationale = "curated_hint_safe_fallback"
                fallback_reason = fallback_reason or "used_curated_source_hint_after_search"
                trace.append(
                    {
                        "step": "selected_curated_hint",
                        "url": selected_url,
                        "score": selected_confidence,
                    }
                )
            else:
                fallback_reason = fallback_reason or "no_safe_candidate"
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

    if selected_url is not None:
        selected_quality = source_quality or _source_quality_from_url(selected_url)
        if selected_quality.is_weak:
            trace.append(
                {
                    "step": "final_source_quality_gate_rejected",
                    "url": selected_url,
                    "reasons": selected_quality.reasons,
                }
            )
            fallback_reason = fallback_reason or "no_safe_candidate"
            selected_url = None
            selected_confidence = 0.0
            source_provenance = None
            selected_candidate_rationale = None
            source_quality = selected_quality
        else:
            source_quality = selected_quality

    if selected_url is None and source_quality is None:
        source_quality = _source_quality_from_url(None)

    return DiscoveryResult(
        fraternity_name=normalized_name,
        fraternity_slug=slug,
        selected_url=selected_url,
        selected_confidence=selected_confidence if selected_url else 0.0,
        confidence_tier=_confidence_tier(selected_confidence if selected_url else 0.0),
        candidates=deduped_candidates,
        source_provenance=source_provenance,
        fallback_reason=fallback_reason,
        source_quality=source_quality,
        selected_candidate_rationale=selected_candidate_rationale,
        resolution_trace=trace,
    )

