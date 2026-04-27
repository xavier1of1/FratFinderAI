from __future__ import annotations

import re

from fratfinder_crawler.social.instagram_models import ChapterInstagramIdentity


_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_STOPWORDS = {
    "the",
    "of",
    "and",
    "at",
    "for",
    "chapter",
    "university",
    "college",
    "campus",
    "school",
    "state",
    "fraternity",
}
_GREEK = {
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
_NEGATIVE_TERMS = [
    "alumni",
    "alumnae",
    "foundation",
    "headquarters",
    "international",
    "national",
    "office",
    "ifc",
    "greeklife",
    "greek_life",
    "greek",
    "fsl",
    "studentlife",
]


def _clean(text: str | None) -> str:
    return " ".join(_TOKEN_RE.findall(str(text or "").strip().lower()))


def _tokens(text: str | None) -> list[str]:
    return [token for token in _clean(text).split() if token]


def _compact(text: str | None) -> str:
    return "".join(_TOKEN_RE.findall(str(text or "").lower()))


def _initials(text: str | None) -> str | None:
    tokens = [token for token in _tokens(text) if token not in _STOPWORDS]
    if len(tokens) < 2:
        return None
    return "".join(token[0] for token in tokens if token)


def _display_from_slug(slug: str | None) -> str:
    return " ".join(part for part in str(slug or "").replace("_", "-").split("-") if part)


def build_chapter_instagram_identity(
    *,
    fraternity_name: str | None,
    fraternity_slug: str | None = None,
    school_name: str | None = None,
    chapter_name: str | None = None,
    school_aliases: list[str] | None = None,
    fraternity_aliases: list[str] | None = None,
    city: str | None = None,
    state: str | None = None,
) -> ChapterInstagramIdentity:
    fraternity_display = fraternity_name or _display_from_slug(fraternity_slug)
    fraternity_alias_values = [value for value in [fraternity_display, _display_from_slug(fraternity_slug), *(fraternity_aliases or [])] if value]
    school_alias_values = [value for value in [school_name, *(school_aliases or [])] if value]
    chapter_display = chapter_name or ""
    fraternity_initials = [value for value in {_initials(alias) for alias in fraternity_alias_values} if value]
    school_initials = [value for value in {_initials(alias) for alias in school_alias_values} if value]
    fraternity_compacts = [value for value in {_compact(alias) for alias in fraternity_alias_values} if len(value) >= 3]
    school_compacts = [value for value in {_compact(alias) for alias in school_alias_values} if len(value) >= 3]
    chapter_compacts = [value for value in {_compact(chapter_display)} if value and len(value) >= 2]
    fraternity_greek = [token for alias in fraternity_alias_values for token in _tokens(alias) if token in _GREEK]
    chapter_greek = [token for token in _tokens(chapter_display) if token in _GREEK]
    fraternity_nicknames = [
        cleaned
        for cleaned in [_clean(value) for value in (fraternity_aliases or [])]
        if cleaned and cleaned not in {_clean(fraternity_display), _clean(_display_from_slug(fraternity_slug))}
    ]
    return ChapterInstagramIdentity(
        fraternity_full_names=list(dict.fromkeys([_clean(value) for value in fraternity_alias_values if _clean(value)])),
        fraternity_aliases=list(dict.fromkeys([_clean(value) for value in (fraternity_aliases or []) if _clean(value)])),
        fraternity_nicknames=list(dict.fromkeys(fraternity_nicknames)),
        fraternity_greek_letters=list(dict.fromkeys(fraternity_greek)),
        fraternity_initials=list(dict.fromkeys(fraternity_initials)),
        fraternity_compact_tokens=list(dict.fromkeys(fraternity_compacts)),
        school_full_names=list(dict.fromkeys([_clean(value) for value in school_alias_values if _clean(value)])),
        school_aliases=list(dict.fromkeys([_clean(value) for value in (school_aliases or []) if _clean(value)])),
        school_initials=list(dict.fromkeys(school_initials)),
        school_compact_tokens=list(dict.fromkeys(school_compacts)),
        school_city_tokens=[_clean(city)] if _clean(city) else [],
        school_state_tokens=[_clean(state)] if _clean(state) else [],
        chapter_names=list(dict.fromkeys([_clean(chapter_display)] if _clean(chapter_display) else [])),
        chapter_greek_letters=list(dict.fromkeys(chapter_greek)),
        chapter_compact_tokens=list(dict.fromkeys(chapter_compacts)),
        negative_generic_terms=list(_NEGATIVE_TERMS),
    )
