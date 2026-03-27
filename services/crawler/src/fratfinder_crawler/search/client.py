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
_LOW_SIGNAL_BING_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com"}
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
    def __init__(self, settings: Settings, get_requester: Callable[..., object] | None = None):
        self._settings = settings
        self._query_cache: dict[tuple[str, str, int], list[SearchResult]] = {}
        self._provider_failure_streak: dict[str, int] = {}
        self._provider_circuit_open_until: dict[str, float] = {}
        self._session: requests.Session | None = None
        if get_requester is None:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._session = session
            self._get_requester = session.get
        else:
            self._get_requester = get_requester

    def search(self, query: str, max_results: int | None = None) -> list[SearchResult]:
        if not self._settings.crawler_search_enabled:
            return []

        provider = self._settings.crawler_search_provider.lower()
        limit = max_results or self._settings.crawler_search_max_results
        cache_key = (provider, query.strip().lower(), limit)
        cached = self._query_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        if provider == "auto":
            results = self._search_auto(query, limit)
            self._cache_query_results(cache_key, results)
            return results
        if provider == "brave_api":
            if not self._settings.crawler_search_brave_api_key:
                raise SearchUnavailableError("Brave Search API key is required when CRAWLER_SEARCH_PROVIDER=brave_api")
            try:
                results = self._run_provider_call("brave_api", lambda: self._search_brave_api(query, limit))
            except (SearchUnavailableError, requests.RequestException):
                results = self._run_provider_call("bing_html", lambda: self._search_bing_html(query, limit))
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
                results = self._run_provider_call("bing_html", lambda: self._search_bing_html(query, limit))
            self._cache_query_results(cache_key, results)
            return results
        raise SearchUnavailableError(f"Unsupported search provider: {self._settings.crawler_search_provider}")

    def _search_auto(self, query: str, max_results: int) -> list[SearchResult]:
        if self._settings.crawler_search_brave_api_key:
            try:
                return self._run_provider_call("brave_api", lambda: self._search_brave_api(query, max_results))
            except (SearchUnavailableError, requests.RequestException):
                pass
        return self._search_bing_with_free_fallback(query, max_results)

    def _search_bing_with_free_fallback(self, query: str, max_results: int) -> list[SearchResult]:
        results = self._run_provider_call("bing_html", lambda: self._search_bing_html(query, max_results))
        if not _should_fallback_from_bing(query, results):
            return results
        try:
            fallback = self._run_provider_call("duckduckgo_html", lambda: self._search_duckduckgo_html(query, max_results))
        except (SearchUnavailableError, requests.RequestException):
            return results
        return fallback or results

    def _cache_query_results(self, cache_key: tuple[str, str, int], results: list[SearchResult]) -> None:
        if results or self._settings.crawler_search_cache_empty_results:
            self._query_cache[cache_key] = list(results)

    def _run_provider_call(self, provider: str, fn: Callable[[], list[SearchResult]]) -> list[SearchResult]:
        self._ensure_provider_available(provider)
        try:
            results = fn()
        except (SearchUnavailableError, requests.RequestException):
            self._record_provider_failure(provider)
            raise
        self._record_provider_success(provider)
        return results

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
        soup = BeautifulSoup(response.text, "html.parser")
        results: list[SearchResult] = []
        for rank, node in enumerate(soup.select("li.b_algo"), start=1):
            link = node.select_one("h2 a")
            if link is None:
                continue
            href = _normalize_search_result_url(link.get("href") or "")
            title = link.get_text(" ", strip=True)
            snippet_node = node.select_one(".b_caption p, .b_snippet, p")
            snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
            if not href or not title:
                continue
            results.append(SearchResult(title=title, url=href, snippet=snippet, provider="bing_html", rank=rank))
            if len(results) >= max_results:
                break
        return results

    def _search_headers(self, *, referer: str) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
        }

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
