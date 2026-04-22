from __future__ import annotations

from dataclasses import dataclass


PROVIDER_KIND_FREE_HTML = "free_html"
PROVIDER_KIND_SELF_HOSTED_API = "self_hosted_api"
PROVIDER_KIND_MANAGED_API = "managed_api"


@dataclass(frozen=True, slots=True)
class ProviderMetadata:
    provider_name: str
    provider_kind: str
    default_enabled: bool
    automatic_chain_allowed: bool
    deprecated: bool = False
    requires_key: bool = False


PROVIDER_CATALOG: dict[str, ProviderMetadata] = {
    "searxng_json": ProviderMetadata(
        provider_name="searxng_json",
        provider_kind=PROVIDER_KIND_SELF_HOSTED_API,
        default_enabled=True,
        automatic_chain_allowed=True,
    ),
    "bing_html": ProviderMetadata(
        provider_name="bing_html",
        provider_kind=PROVIDER_KIND_FREE_HTML,
        default_enabled=True,
        automatic_chain_allowed=True,
    ),
    "duckduckgo_html": ProviderMetadata(
        provider_name="duckduckgo_html",
        provider_kind=PROVIDER_KIND_FREE_HTML,
        default_enabled=True,
        automatic_chain_allowed=True,
    ),
    "tavily_api": ProviderMetadata(
        provider_name="tavily_api",
        provider_kind=PROVIDER_KIND_MANAGED_API,
        default_enabled=False,
        automatic_chain_allowed=False,
        requires_key=True,
    ),
    "serper_api": ProviderMetadata(
        provider_name="serper_api",
        provider_kind=PROVIDER_KIND_MANAGED_API,
        default_enabled=False,
        automatic_chain_allowed=False,
        requires_key=True,
    ),
    "brave_api": ProviderMetadata(
        provider_name="brave_api",
        provider_kind=PROVIDER_KIND_MANAGED_API,
        default_enabled=False,
        automatic_chain_allowed=False,
        requires_key=True,
    ),
    "dataforseo_api": ProviderMetadata(
        provider_name="dataforseo_api",
        provider_kind=PROVIDER_KIND_MANAGED_API,
        default_enabled=False,
        automatic_chain_allowed=False,
        requires_key=True,
    ),
    "brave_html": ProviderMetadata(
        provider_name="brave_html",
        provider_kind=PROVIDER_KIND_FREE_HTML,
        default_enabled=False,
        automatic_chain_allowed=False,
        deprecated=True,
    ),
}


CANONICAL_AUTOMATIC_FREE_PROVIDER_ORDER = [
    "searxng_json",
    "bing_html",
    "duckduckgo_html",
]

SUPPORTED_PROVIDER_CHOICES = {
    "auto",
    "auto_free",
    *PROVIDER_CATALOG.keys(),
}


def provider_metadata(provider_name: str) -> ProviderMetadata | None:
    return PROVIDER_CATALOG.get(str(provider_name or "").strip().lower())


def provider_names(*, automatic_chain_allowed: bool | None = None, include_deprecated: bool = True) -> list[str]:
    names: list[str] = []
    for provider_name, metadata in PROVIDER_CATALOG.items():
        if automatic_chain_allowed is not None and metadata.automatic_chain_allowed != automatic_chain_allowed:
            continue
        if not include_deprecated and metadata.deprecated:
            continue
        names.append(provider_name)
    return names


def canonical_free_provider_order() -> list[str]:
    return list(CANONICAL_AUTOMATIC_FREE_PROVIDER_ORDER)


def normalize_free_provider_order(raw_order: str | None) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    order: list[str] = []
    seen: set[str] = set()

    tokens: list[str]
    if str(raw_order or "").strip():
        tokens = [part.strip().lower() for part in str(raw_order or "").split(",")]
    else:
        tokens = canonical_free_provider_order()

    for token in tokens:
        if not token or token in seen:
            continue
        metadata = provider_metadata(token)
        if metadata is None:
            warnings.append(f"Ignoring unknown search provider `{token}` in automatic free-chain configuration.")
            continue
        if not metadata.automatic_chain_allowed:
            reason = "deprecated" if metadata.deprecated else "opt-in only"
            warnings.append(f"Ignoring {reason} search provider `{token}` in automatic free-chain configuration.")
            continue
        seen.add(token)
        order.append(token)

    if not order:
        order = canonical_free_provider_order()

    return order, warnings


def canonical_free_provider_order_csv() -> str:
    return ",".join(canonical_free_provider_order())
