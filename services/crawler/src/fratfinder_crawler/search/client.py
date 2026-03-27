from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Callable
from urllib.parse import parse_qs, unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

from fratfinder_crawler.config import Settings

_DDG_HTML_ENDPOINT = "https://html.duckduckgo.com/html/"
_BING_HTML_ENDPOINT = "https://www.bing.com/search"
_BRAVE_SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"


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
            self._query_cache[cache_key] = list(results)
            return results
        if provider == "brave_api":
            if not self._settings.crawler_search_brave_api_key:
                raise SearchUnavailableError("Brave Search API key is required when CRAWLER_SEARCH_PROVIDER=brave_api")
            try:
                results = self._search_brave_api(query, limit)
            except (SearchUnavailableError, requests.RequestException):
                results = self._search_bing_html(query, limit)
            self._query_cache[cache_key] = list(results)
            return results
        if provider == "bing_html":
            results = self._search_bing_html(query, limit)
            self._query_cache[cache_key] = list(results)
            return results
        if provider == "duckduckgo_html":
            try:
                results = self._search_duckduckgo_html(query, limit)
            except (SearchUnavailableError, requests.RequestException):
                results = self._search_bing_html(query, limit)
            self._query_cache[cache_key] = list(results)
            return results
        raise SearchUnavailableError(f"Unsupported search provider: {self._settings.crawler_search_provider}")

    def _search_auto(self, query: str, max_results: int) -> list[SearchResult]:
        if self._settings.crawler_search_brave_api_key:
            try:
                return self._search_brave_api(query, max_results)
            except (SearchUnavailableError, requests.RequestException):
                pass
        return self._search_bing_html(query, max_results)

    def _search_duckduckgo_html(self, query: str, max_results: int) -> list[SearchResult]:
        response = self._get_requester(
            _DDG_HTML_ENDPOINT,
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
        for node in soup.select(".result, .web-result"):
            link = node.select_one(".result__title a, .result__a, a.result-link")
            if link is None:
                continue
            href = _normalize_search_result_url(link.get("href") or "")
            title = link.get_text(" ", strip=True)
            snippet_node = node.select_one(".result__snippet, .result__extras__url + a, .result-snippet")
            snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
            if not href or not title:
                continue
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
