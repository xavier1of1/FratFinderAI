from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher

from .models import StatusZone

_AMBIGUOUS_SHORT_ALIASES = {"delta", "phi", "phi kappa", "sigma", "alpha"}
_NICKNAME_ALIASES = {
    "phi gamma delta": ["fiji"],
    "alpha tau omega": ["ato"],
    "sigma alpha epsilon": ["sae", "sig ep", "sigep"],
    "beta upsilon chi": ["byx"],
    "pi kappa alpha": ["pike"],
    "delta chi": ["d chi", "dchi"],
}
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


def _normalize(value: str | None) -> str:
    text = value or ""
    for symbol, replacement in _GREEK_SYMBOLS.items():
        text = text.replace(symbol, f" {replacement} ")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()


def _initialism(value: str | None) -> str:
    tokens = [token for token in _normalize(value).split() if token not in {"of", "the", "and", "at"}]
    return "".join(token[0] for token in tokens if token)


def _fraternity_aliases(fraternity_name: str, fraternity_slug: str | None, aliases: list[str] | None) -> list[str]:
    normalized = _normalize(fraternity_name)
    candidate_aliases = [fraternity_name, fraternity_slug or "", fraternity_slug.replace("-", " ") if fraternity_slug else ""]
    candidate_aliases.extend(aliases or [])
    candidate_aliases.extend(_NICKNAME_ALIASES.get(normalized, []))
    deduped: list[str] = []
    seen: set[str] = set()
    for alias in candidate_aliases:
        normalized_alias = _normalize(alias)
        if not normalized_alias or normalized_alias in seen:
            continue
        seen.add(normalized_alias)
        deduped.append(normalized_alias)
    return deduped


def _has_fraternity_context(text: str) -> bool:
    return any(
        marker in text
        for marker in (
            "fraternity",
            "chapter",
            "greek",
            "ifc",
            "interfraternity",
            "recognized",
            "suspended",
            "active",
            "probation",
            "suspension",
            "scorecard",
            "organization",
        )
    )


def _mentions_other_campus(text: str, school_name: str | None) -> bool:
    school = _normalize(school_name)
    if not school:
        return False
    initials = _initialism(school_name)
    if school in text or (initials and re.search(rf"\b{re.escape(initials)}\b", text)):
        return False
    return any(
        marker in text
        for marker in (
            "university of",
            "college",
            "state university",
            "william mary",
            "university",
        )
    )


@dataclass(slots=True)
class MatchResult:
    matched: bool
    matched_text: str | None
    matched_alias: str | None
    match_method: str
    confidence: float


def match_fraternity_in_zone(
    *,
    fraternity_name: str,
    fraternity_slug: str | None,
    zone: StatusZone,
    aliases: list[str] | None = None,
    school_name: str | None = None,
) -> MatchResult:
    normalized_text = _normalize(zone.text)
    school = _normalize(school_name)
    alias_candidates = _fraternity_aliases(fraternity_name, fraternity_slug, aliases)

    if school and (normalized_text.count("university") >= 2 and school not in normalized_text or _mentions_other_campus(normalized_text, school_name)):
        return MatchResult(False, None, None, "school_context_missing", 0.0)

    for alias in alias_candidates:
        if alias in _AMBIGUOUS_SHORT_ALIASES:
            continue
        if re.search(rf"\b{re.escape(alias)}\b", normalized_text) and _has_fraternity_context(normalized_text):
            return MatchResult(True, alias, alias, "exact_alias", 0.99)

    for alias in alias_candidates:
        if alias in _AMBIGUOUS_SHORT_ALIASES:
            continue
        tokens = alias.split()
        if len(tokens) >= 2 and re.search(rf"\b{re.escape(' '.join(tokens))}\b", normalized_text) and _has_fraternity_context(normalized_text):
            return MatchResult(True, alias, alias, "greek_letters", 0.97)

    for alias in alias_candidates:
        if alias in _AMBIGUOUS_SHORT_ALIASES:
            continue
        if alias in _NICKNAME_ALIASES.get(_normalize(fraternity_name), []):
            if re.search(rf"\b{re.escape(alias)}\b", normalized_text):
                return MatchResult(True, alias, alias, "nickname", 0.94)

    if not _has_fraternity_context(normalized_text):
        return MatchResult(False, None, None, "no_fraternity_context", 0.0)

    best_ratio = 0.0
    best_alias: str | None = None
    for alias in alias_candidates:
        if alias in _AMBIGUOUS_SHORT_ALIASES or len(alias) < 6:
            continue
        ratio = SequenceMatcher(None, alias, normalized_text).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_alias = alias
    if best_alias is not None and best_ratio >= 0.68:
        return MatchResult(True, best_alias, best_alias, "fuzzy_contextual", round(best_ratio, 4))
    return MatchResult(False, None, None, "no_match", 0.0)
