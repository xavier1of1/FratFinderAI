from __future__ import annotations

import base64
import json
import re
import time
from dataclasses import dataclass
from typing import Callable
from urllib.parse import parse_qs, unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

from fratfinder_crawler.config import Settings

_DDG_HTML_ENDPOINT = "https://html.duckduckgo.com/html/"
_DDG_LITE_ENDPOINT = "https://lite.duckduckgo.com/lite/"
_BING_HTML_ENDPOINT = "https://www.bing.com/search"
_BRAVE_SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
_BRAVE_HTML_ENDPOINT = "https://search.brave.com/search"
_TAVILY_SEARCH_ENDPOINT = "https://api.tavily.com/search"
_SERPER_SEARCH_ENDPOINT = "https://google.serper.dev/search"
_LOW_SIGNAL_BING_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com"}
_BRAVE_INTERNAL_HOSTS = {
    "search.brave.com",
    "cdn.search.brave.com",
    "imgs.search.brave.com",
    "tiles.search.brave.com",
    "brave.com",
    "www.brave.com",
}
_LOW_SIGNAL_QUERY_STOPWORDS = {
    "site",
    "instagram",
    "email",
    "contact",
    "chapter",
    "official",
    "fraternity",
    "find",
    "website",
    "profile",
    "edu",
    "com",
    "org",
    "www",
    "http",
    "https",
}
_SUPPORTED_PROVIDERS = {
    "auto",
    "auto_free",
    "searxng_json",
    "tavily_api",
    "serper_api",
    "brave_api",
    "bing_html",
    "duckduckgo_html",
    "brave_html",
}


@dataclass(slots=True)
class SearchResult:
    title: str
    url: str
    snippet: str
    provider: str
    rank: int


class SearchUnavailableError(RuntimeError):
    pass


class SearchClient:
    def __init__(
        self,
        settings: Settings,
        get_requester: Callable[..., object] | None = None,
        post_requester: Callable[..., object] | None = None,
    ):
        self._settings = settings
        self._query_cache: dict[tuple[str, str, int], list[SearchResult]] = {}
        self._provider_failure_streak: dict[str, int] = {}
        self._provider_circuit_open_until: dict[str, float] = {}
        self._provider_last_request_at: dict[str, float] = {}
        self._last_provider_attempts: list[dict[str, object]] = []
        self._global_last_request_at: float = 0.0
        self._min_request_interval_seconds = max(0.0, float(settings.crawler_search_min_request_interval_ms) / 1000.0)
        self._provider_min_request_interval_seconds: dict[str, float] = {
            "searxng_json": max(0.0, float(settings.crawler_search_provider_pacing_ms_searxng_json) / 1000.0),
            "tavily_api": max(0.0, float(settings.crawler_search_provider_pacing_ms_tavily_api) / 1000.0),
            "serper_api": max(0.0, float(settings.crawler_search_provider_pacing_ms_serper_api) / 1000.0),
            "bing_html": max(0.0, float(settings.crawler_search_provider_pacing_ms_bing_html) / 1000.0),
            "duckduckgo_html": max(0.0, float(settings.crawler_search_provider_pacing_ms_duckduckgo_html) / 1000.0),
            "brave_html": max(0.0, float(settings.crawler_search_provider_pacing_ms_brave_html) / 1000.0),
        }
        self._session: requests.Session | None = None
        if get_requester is None and post_requester is None:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._session = session
            self._get_requester = session.get
            self._post_requester = session.post
        else:
            self._get_requester = get_requester or requests.get
            self._post_requester = post_requester or requests.post

    def search(self, query: str, max_results: int | None = None) -> list[SearchResult]:
        if not self._settings.crawler_search_enabled:
            return []
        self._last_provider_attempts = []

        provider = self._settings.crawler_search_provider.lower()
        limit = max_results or self._settings.crawler_search_max_results
        cache_key = (provider, query.strip().lower(), limit)
        cached = self._query_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        if provider not in _SUPPORTED_PROVIDERS:
            raise SearchUnavailableError(f"Unsupported search provider: {self._settings.crawler_search_provider}")

        if provider == "auto":
            results = self._search_auto(query, limit)
            self._cache_query_results(cache_key, results)
            return results
        if provider == "auto_free":
            results = self._search_auto_free(query, limit)
            self._cache_query_results(cache_key, results)
            return results
        if provider == "searxng_json":
            results = self._run_provider_call("searxng_json", lambda: self._search_searxng_json(query, limit))
            self._cache_query_results(cache_key, results)
            return results
        if provider == "tavily_api":
            results = self._run_provider_call("tavily_api", lambda: self._search_tavily_api(query, limit))
            self._cache_query_results(cache_key, results)
            return results
        if provider == "serper_api":
            results = self._run_provider_call("serper_api", lambda: self._search_serper_api(query, limit))
            self._cache_query_results(cache_key, results)
            return results
        if provider == "brave_api":
            if not self._settings.crawler_search_brave_api_key:
                raise SearchUnavailableError("Brave Search API key is required when CRAWLER_SEARCH_PROVIDER=brave_api")
            try:
                results = self._run_provider_call("brave_api", lambda: self._search_brave_api(query, limit))
            except (SearchUnavailableError, requests.RequestException):
                results = self._run_provider_call("bing_html", lambda: self._search_bing_html(query, limit), fallback_taken=True)
            self._cache_query_results(cache_key, results)
            return results
        if provider == "bing_html":
            results = self._search_bing_with_free_fallback(query, limit)
            self._cache_query_results(cache_key, results)
            return results
        if provider == "duckduckgo_html":
            try:
                results = self._run_provider_call("duckduckgo_html", lambda: self._search_duckduckgo_html(query, limit))
            except (SearchUnavailableError, requests.RequestException):
                results = self._run_provider_call("bing_html", lambda: self._search_bing_html(query, limit), fallback_taken=True)
            self._cache_query_results(cache_key, results)
            return results
        if provider == "brave_html":
            results = self._run_provider_call("brave_html", lambda: self._search_brave_html(query, limit))
            self._cache_query_results(cache_key, results)
            return results
        raise SearchUnavailableError(f"Unsupported search provider: {self._settings.crawler_search_provider}")

    def _search_auto(self, query: str, max_results: int) -> list[SearchResult]:
        providers = self._free_provider_order()
        brave_api_key = (self._settings.crawler_search_brave_api_key or "").strip()
        if brave_api_key:
            if "searxng_json" in providers:
                insert_at = providers.index("searxng_json") + 1
                providers.insert(insert_at, "brave_api")
            else:
                providers.insert(0, "brave_api")
        deduped: list[str] = []
        for provider in providers:
            if provider not in deduped:
                deduped.append(provider)
        return self._search_with_provider_chain(query, max_results, deduped)

    def _search_auto_free(self, query: str, max_results: int) -> list[SearchResult]:
        providers = self._free_provider_order()
        return self._search_with_provider_chain(query, max_results, providers)

    def _free_provider_order(self) -> list[str]:
        raw_order = (self._settings.crawler_search_provider_order_free or "").strip()
        if not raw_order:
            raw_order = "searxng_json,serper_api,tavily_api,duckduckgo_html,bing_html,brave_html"
        deduped: list[str] = []
        for token in (part.strip().lower() for part in raw_order.split(",")):
            if not token or token in deduped:
                continue
            if token not in _SUPPORTED_PROVIDERS or token in {"auto", "auto_free", "brave_api"}:
                continue
            deduped.append(token)
        return deduped or ["duckduckgo_html", "bing_html", "brave_html"]

    def _search_with_provider_chain(self, query: str, max_results: int, providers: list[str]) -> list[SearchResult]:
        last_error: Exception | None = None
        had_successful_provider_call = False
        last_successful_results: list[SearchResult] = []
        for index, provider in enumerate(providers):
            if not self._provider_configured(provider):
                self._record_provider_attempt(provider, "skipped", failure_type="not_configured", fallback_taken=index > 0)
                continue
            try:
                results = self._search_with_single_provider(provider, query, max_results, fallback_taken=index > 0)
            except (SearchUnavailableError, requests.RequestException) as exc:
                last_error = exc
                continue
            had_successful_provider_call = True
            last_successful_results = results
            if provider == "bing_html" and _should_fallback_from_bing(query, results):
                self._record_provider_attempt(
                    provider,
                    "low_signal",
                    result_count=len(results),
                    failure_type="low_signal_fallback",
                    fallback_taken=index > 0,
                )
                last_error = SearchUnavailableError("bing_html low-signal result set")
                continue
            if results:
                return results
            last_error = SearchUnavailableError(f"{provider} returned no results")
        if had_successful_provider_call:
            return last_successful_results
        if last_error is not None:
            raise last_error
        raise SearchUnavailableError("No configured providers available")

    def _provider_configured(self, provider: str) -> bool:
        if provider == "searxng_json":
            return bool((self._settings.crawler_search_searxng_base_url or "").strip())
        if provider == "tavily_api":
            return bool((self._settings.crawler_search_tavily_api_key or "").strip())
        if provider == "serper_api":
            return bool((self._settings.crawler_search_serper_api_key or "").strip())
        if provider == "brave_api":
            return bool((self._settings.crawler_search_brave_api_key or "").strip())
        return True

    def _search_with_single_provider(self, provider: str, query: str, max_results: int, *, fallback_taken: bool) -> list[SearchResult]:
        if provider == "searxng_json":
            return self._run_provider_call("searxng_json", lambda: self._search_searxng_json(query, max_results), fallback_taken=fallback_taken)
        if provider == "tavily_api":
            return self._run_provider_call("tavily_api", lambda: self._search_tavily_api(query, max_results), fallback_taken=fallback_taken)
        if provider == "serper_api":
            return self._run_provider_call("serper_api", lambda: self._search_serper_api(query, max_results), fallback_taken=fallback_taken)
        if provider == "bing_html":
            return self._run_provider_call("bing_html", lambda: self._search_bing_html(query, max_results), fallback_taken=fallback_taken)
        if provider == "duckduckgo_html":
            return self._run_provider_call("duckduckgo_html", lambda: self._search_duckduckgo_html(query, max_results), fallback_taken=fallback_taken)
        if provider == "brave_html":
            return self._run_provider_call("brave_html", lambda: self._search_brave_html(query, max_results), fallback_taken=fallback_taken)
        if provider == "brave_api":
            return self._run_provider_call("brave_api", lambda: self._search_brave_api(query, max_results), fallback_taken=fallback_taken)
        raise SearchUnavailableError(f"Unsupported provider in chain: {provider}")

    def _search_bing_with_free_fallback(self, query: str, max_results: int) -> list[SearchResult]:
        providers = ["bing_html", "duckduckgo_html", "brave_html"]
        return self._search_with_provider_chain(query, max_results, providers)

    def _cache_query_results(self, cache_key: tuple[str, str, int], results: list[SearchResult]) -> None:
        if results or self._settings.crawler_search_cache_empty_results:
            self._query_cache[cache_key] = list(results)

    def consume_last_provider_attempts(self) -> list[dict[str, object]]:
        attempts = list(self._last_provider_attempts)
        self._last_provider_attempts = []
        return attempts

    def _run_provider_call(self, provider: str, fn: Callable[[], list[SearchResult]], *, fallback_taken: bool = False) -> list[SearchResult]:
        try:
            self._ensure_provider_available(provider)
        except SearchUnavailableError as exc:
            self._record_provider_attempt(
                provider,
                "unavailable",
                failure_type=self._classify_failure_type(exc),
                circuit_open="circuit open" in str(exc).lower(),
                fallback_taken=fallback_taken,
            )
            raise

        self._apply_request_spacing(provider)
        try:
            results = fn()
        except SearchUnavailableError as exc:
            self._record_provider_failure(provider)
            self._record_provider_attempt(
                provider,
                "unavailable",
                failure_type=self._classify_failure_type(exc),
                circuit_open="circuit open" in str(exc).lower(),
                fallback_taken=fallback_taken,
            )
            raise
        except requests.RequestException as exc:
            self._record_provider_failure(provider)
            self._record_provider_attempt(
                provider,
                "request_error",
                failure_type=type(exc).__name__,
                fallback_taken=fallback_taken,
            )
            raise
        self._record_provider_success(provider)
        self._record_provider_attempt(provider, "success", result_count=len(results), fallback_taken=fallback_taken)
        return results

    def _record_provider_attempt(
        self,
        provider: str,
        status: str,
        *,
        result_count: int | None = None,
        failure_type: str | None = None,
        circuit_open: bool = False,
        fallback_taken: bool = False,
    ) -> None:
        self._last_provider_attempts.append(
            {
                "provider": provider,
                "status": status,
                "result_count": result_count,
                "failure_type": failure_type,
                "circuit_open": circuit_open,
                "fallback_taken": fallback_taken,
            }
        )

    def _classify_failure_type(self, error: Exception) -> str:
        message = str(error).lower()
        if "circuit open" in message:
            return "circuit_open"
        if "anomaly" in message or "challenge" in message or "captcha" in message:
            return "challenge_or_anomaly"
        if "temporarily unavailable" in message:
            return "provider_unavailable"
        return type(error).__name__

    def _apply_request_spacing(self, provider: str) -> None:
        provider_interval = self._provider_min_request_interval_seconds.get(provider, 0.0)
        interval = max(self._min_request_interval_seconds, provider_interval)
        if interval <= 0:
            return
        now = time.monotonic()
        next_allowed_global = self._global_last_request_at + interval
        next_allowed_provider = self._provider_last_request_at.get(provider, 0.0) + interval
        wait_seconds = max(0.0, max(next_allowed_global, next_allowed_provider) - now)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        stamped = time.monotonic()
        self._global_last_request_at = stamped
        self._provider_last_request_at[provider] = stamped

    def _ensure_provider_available(self, provider: str) -> None:
        open_until = self._provider_circuit_open_until.get(provider, 0.0)
        if open_until > time.monotonic():
            raise SearchUnavailableError(f"{provider} temporarily unavailable (circuit open)")

    def _record_provider_success(self, provider: str) -> None:
        self._provider_failure_streak[provider] = 0
        self._provider_circuit_open_until.pop(provider, None)

    def _record_provider_failure(self, provider: str) -> None:
        threshold = max(1, self._settings.crawler_search_circuit_breaker_failures)
        cooldown_seconds = max(0, self._settings.crawler_search_circuit_breaker_cooldown_seconds)
        streak = self._provider_failure_streak.get(provider, 0) + 1
        self._provider_failure_streak[provider] = streak
        if cooldown_seconds > 0 and streak >= threshold:
            self._provider_circuit_open_until[provider] = time.monotonic() + cooldown_seconds

    def _search_duckduckgo_html(self, query: str, max_results: int) -> list[SearchResult]:
        response = self._get_requester(
            _DDG_LITE_ENDPOINT,
            params={"q": query},
            timeout=min(self._settings.crawler_http_timeout_seconds, 5.0),
            verify=self._settings.crawler_http_verify_ssl,
            headers=self._search_headers(referer="https://duckduckgo.com/"),
        )
        status_code = getattr(response, "status_code", None)
        text = getattr(response, "text", "") or ""
        if status_code in {202, 403, 429} or _looks_like_duckduckgo_anomaly_page(text):
            raise SearchUnavailableError("DuckDuckGo HTML returned an anomaly or blocked response")
        response.raise_for_status()
        soup = BeautifulSoup(text, "html.parser")
        results: list[SearchResult] = []
        rank = 1
        for link in soup.select("a[href]"):
            href_raw = link.get("href") or ""
            if "/l/?uddg=" not in href_raw and "duckduckgo.com/l/?" not in href_raw:
                continue
            href = _normalize_search_result_url(href_raw)
            title = link.get_text(" ", strip=True)
            if not href or not title:
                continue
            snippet = ""
            parent = link.parent
            if parent is not None:
                snippet = parent.get_text(" ", strip=True)
            results.append(SearchResult(title=title, url=href, snippet=snippet, provider="duckduckgo_html", rank=rank))
            rank += 1
            if len(results) >= max_results:
                break
        if not results:
            raise SearchUnavailableError("DuckDuckGo HTML returned no parseable search results")
        return results

    def _search_bing_html(self, query: str, max_results: int) -> list[SearchResult]:
        response = self._get_requester(
            _BING_HTML_ENDPOINT,
            params={"q": query},
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
            headers=self._search_headers(referer="https://www.bing.com/"),
        )
        response.raise_for_status()
        text = getattr(response, "text", "") or ""
        soup = BeautifulSoup(text, "html.parser")
        result_nodes = [node for node in soup.select("li.b_algo") if node.select_one("h2 a") is not None]
        if _looks_like_bing_anomaly_page(text, has_parseable_results=bool(result_nodes)):
            raise SearchUnavailableError("Bing HTML returned a challenge or anomaly page")
        results: list[SearchResult] = []
        for rank, node in enumerate(result_nodes, start=1):
            link = node.select_one("h2 a")
            href = _normalize_search_result_url(link.get("href") or "")
            title = link.get_text(" ", strip=True)
            snippet_node = node.select_one(".b_caption p, .b_snippet, p")
            snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
            if not href or not title:
                continue
            results.append(SearchResult(title=title, url=href, snippet=snippet, provider="bing_html", rank=rank))
            if len(results) >= max_results:
                break
        if not results and _looks_like_bing_empty_anomaly(text):
            raise SearchUnavailableError("Bing HTML returned no parseable results (likely anti-bot challenge)")
        return results

    def _search_headers(self, *, referer: str) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
            "Accept-Encoding": "identity",
        }

    def _search_brave_html(self, query: str, max_results: int) -> list[SearchResult]:
        response = self._get_requester(
            _BRAVE_HTML_ENDPOINT,
            params={"q": query, "source": "web"},
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
            headers=self._search_headers(referer="https://search.brave.com/"),
        )
        response.raise_for_status()
        text = getattr(response, "text", "") or ""
        if _looks_like_brave_anomaly_page(text):
            raise SearchUnavailableError("Brave HTML returned a challenge or anomaly page")
        soup = BeautifulSoup(text, "html.parser")
        results: list[SearchResult] = []
        seen_urls: set[str] = set()
        rank = 1
        for link in soup.select("a[href]"):
            href_raw = (link.get("href") or "").strip()
            if not href_raw.startswith(("http://", "https://")):
                continue
            href = _normalize_search_result_url(href_raw)
            parsed = urlparse(href)
            host = (parsed.netloc or "").lower()
            if not host or host in _BRAVE_INTERNAL_HOSTS or any(host.endswith(f".{blocked}") for blocked in _BRAVE_INTERNAL_HOSTS):
                continue
            if href in seen_urls:
                continue
            title = link.get_text(" ", strip=True)
            parent = link.find_parent(class_=re.compile("result-wrapper|snippet", re.IGNORECASE)) or link.parent
            snippet = parent.get_text(" ", strip=True) if parent is not None else ""
            if not title:
                title = snippet[:120] or href
            results.append(
                SearchResult(
                    title=title,
                    url=href,
                    snippet=snippet[:600],
                    provider="brave_html",
                    rank=rank,
                )
            )
            seen_urls.add(href)
            rank += 1
            if len(results) >= max_results:
                break
        if not results:
            raise SearchUnavailableError("Brave HTML returned no parseable search results")
        return results

    def _search_searxng_json(self, query: str, max_results: int) -> list[SearchResult]:
        base_url = (self._settings.crawler_search_searxng_base_url or "").strip().rstrip("/")
        if not base_url:
            raise SearchUnavailableError("SearXNG base URL is required when provider is searxng_json")
        endpoint = f"{base_url}/search"
        params: dict[str, str | int] = {"q": query, "format": "json"}
        engines = (self._settings.crawler_search_searxng_engines or "").strip()
        if engines:
            params["engines"] = engines
        response = self._get_requester(
            endpoint,
            params=params,
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
            headers=self._search_headers(referer=f"{base_url}/"),
        )
        response.raise_for_status()
        payload = response.json() if hasattr(response, "json") else json.loads(getattr(response, "text", "{}") or "{}")
        results: list[SearchResult] = []
        for rank, item in enumerate(payload.get("results", []), start=1):
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or item.get("link") or "").strip()
            if not url:
                continue
            title = str(item.get("title") or "").strip() or url
            snippet = str(item.get("content") or item.get("snippet") or "").strip()
            results.append(SearchResult(title=title, url=url, snippet=snippet, provider="searxng_json", rank=rank))
            if len(results) >= max_results:
                break
        if not results:
            raise SearchUnavailableError("SearXNG returned no parseable search results")
        return results

    def _search_tavily_api(self, query: str, max_results: int) -> list[SearchResult]:
        api_key = (self._settings.crawler_search_tavily_api_key or "").strip()
        if not api_key:
            raise SearchUnavailableError("Tavily API key is required when provider is tavily_api")
        response = self._post_requester(
            _TAVILY_SEARCH_ENDPOINT,
            json={
                "api_key": api_key,
                "query": query,
                "max_results": max_results,
                "search_depth": "basic",
                "include_answer": False,
                "include_raw_content": False,
            },
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
            headers={
                "User-Agent": self._settings.crawler_http_user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        payload = response.json() if hasattr(response, "json") else json.loads(getattr(response, "text", "{}") or "{}")
        results: list[SearchResult] = []
        for rank, item in enumerate(payload.get("results", []), start=1):
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            title = str(item.get("title") or "").strip()
            snippet = str(item.get("content") or item.get("snippet") or "").strip()
            if not url or not title:
                continue
            results.append(SearchResult(title=title, url=url, snippet=snippet, provider="tavily_api", rank=rank))
            if len(results) >= max_results:
                break
        if not results:
            raise SearchUnavailableError("Tavily returned no parseable search results")
        return results

    def _search_serper_api(self, query: str, max_results: int) -> list[SearchResult]:
        api_key = (self._settings.crawler_search_serper_api_key or "").strip()
        if not api_key:
            raise SearchUnavailableError("Serper API key is required when provider is serper_api")
        response = self._post_requester(
            _SERPER_SEARCH_ENDPOINT,
            json={"q": query, "num": max_results},
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
            headers={
                "User-Agent": self._settings.crawler_http_user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-API-KEY": api_key,
            },
        )
        response.raise_for_status()
        payload = response.json() if hasattr(response, "json") else json.loads(getattr(response, "text", "{}") or "{}")
        results: list[SearchResult] = []
        for rank, item in enumerate(payload.get("organic", []), start=1):
            if not isinstance(item, dict):
                continue
            url = str(item.get("link") or item.get("url") or "").strip()
            title = str(item.get("title") or "").strip()
            snippet = str(item.get("snippet") or item.get("description") or "").strip()
            if not url or not title:
                continue
            results.append(SearchResult(title=title, url=url, snippet=snippet, provider="serper_api", rank=rank))
            if len(results) >= max_results:
                break
        if not results:
            raise SearchUnavailableError("Serper returned no parseable search results")
        return results

    def _search_brave_api(self, query: str, max_results: int) -> list[SearchResult]:
        response = self._get_requester(
            _BRAVE_SEARCH_ENDPOINT,
            params={"q": query, "count": max_results},
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
            headers={
                "User-Agent": self._settings.crawler_http_user_agent,
                "Accept": "application/json",
                "X-Subscription-Token": self._settings.crawler_search_brave_api_key or "",
            },
        )
        response.raise_for_status()
        payload = response.json() if hasattr(response, "json") else json.loads(response.text)
        results: list[SearchResult] = []
        for rank, item in enumerate(payload.get("web", {}).get("results", []), start=1):
            url = item.get("url")
            title = item.get("title")
            snippet = item.get("description") or ""
            if not url or not title:
                continue
            results.append(SearchResult(title=title, url=url, snippet=snippet, provider="brave_api", rank=rank))
            if len(results) >= max_results:
                break
        return results


def _normalize_search_result_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        url = f"https:{url}"
    parsed = urlparse(url)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        uddg = parse_qs(parsed.query).get("uddg")
        if uddg:
            return unquote(uddg[0])
    if "bing.com" in parsed.netloc and parsed.path.startswith("/ck/a"):
        redirect_target = parse_qs(parsed.query).get("u")
        if redirect_target:
            decoded = _decode_bing_redirect_target(redirect_target[0])
            if decoded:
                return decoded
    return url


def _looks_like_duckduckgo_anomaly_page(html: str) -> bool:
    lowered = html.lower()
    return "anomaly" in lowered or "automated traffic" in lowered or "detected unusual traffic" in lowered


def _looks_like_bing_anomaly_page(html: str, *, has_parseable_results: bool = False) -> bool:
    if has_parseable_results:
        return False
    lowered = html.lower()
    markers = (
        "challenge/verify",
        "id=\"b_captcha\"",
        "enter the characters you see below",
        "unusual traffic",
        "verify you are a human",
    )
    return any(marker in lowered for marker in markers)


def _looks_like_bing_empty_anomaly(html: str) -> bool:
    lowered = html.lower()
    if "li class=\"b_algo\"" in lowered:
        return False
    anomaly_hints = (
        "challenge",
        "captcha",
        "verify",
        "unusual traffic",
    )
    return any(hint in lowered for hint in anomaly_hints)


def _looks_like_brave_anomaly_page(html: str) -> bool:
    lowered = html.lower()
    if "are you a robot" in lowered or "captcha" in lowered:
        if "result-wrapper" not in lowered and "snippet.fdb" not in lowered:
            return True
    if "verify you are human" in lowered and "result-wrapper" not in lowered:
        return True
    return False


def _decode_bing_redirect_target(value: str) -> str | None:
    candidate = value
    if candidate.startswith("a1"):
        candidate = candidate[2:]
    padding = "=" * (-len(candidate) % 4)
    try:
        decoded = base64.urlsafe_b64decode(candidate + padding).decode("utf-8", errors="ignore")
    except Exception:
        return None
    decoded = decoded.strip()
    return decoded if decoded.startswith(("http://", "https://")) else None


def _should_fallback_from_bing(query: str, results: list[SearchResult]) -> bool:
    if not results:
        return True

    query_text = re.sub(r"[^a-z0-9]+", " ", query.lower()).strip()
    query_tokens = [
        token
        for token in query_text.split()
        if len(token) >= 4 and token not in _LOW_SIGNAL_QUERY_STOPWORDS
    ]
    result_texts = [
        re.sub(r"[^a-z0-9]+", " ", f"{result.title} {result.snippet} {result.url}".lower()).strip()
        for result in results
    ]

    if "sigma chi" in query_text:
        has_sigma_chi_anchor = any((" sigmachi " in f" {text} " or " chi " in f" {text} ") for text in result_texts)
        if not has_sigma_chi_anchor:
            return True

    if query_tokens:
        has_token_overlap = any(sum(1 for token in query_tokens if token in text) >= 2 for text in result_texts)
        if not has_token_overlap:
            return True

    low_signal_host_count = 0
    for result in results:
        host = (urlparse(result.url).netloc or "").lower()
        if host in _LOW_SIGNAL_BING_HOSTS:
            low_signal_host_count += 1
    if low_signal_host_count >= max(2, len(results) // 2):
        return True

    return False
