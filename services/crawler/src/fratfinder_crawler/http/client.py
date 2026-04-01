from __future__ import annotations

from urllib.parse import urlparse

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from fratfinder_crawler.config import Settings

_DEFAULT_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
_BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}


def _effective_user_agent(configured_user_agent: str | None) -> str:
    value = (configured_user_agent or "").strip()
    if not value or value.startswith("FratFinderAI/"):
        return _DEFAULT_BROWSER_USER_AGENT
    return value


def _origin_referer(url: str) -> str | None:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/"


class HttpClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._session = Session()
        self._session.headers.update({
            "User-Agent": _effective_user_agent(settings.crawler_http_user_agent),
            **_BROWSER_HEADERS,
        })
        retry = Retry(
            total=settings.crawler_max_retries,
            backoff_factor=settings.crawler_retry_backoff_seconds,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def get(self, url: str) -> str:
        request_headers: dict[str, str] = {}
        referer = _origin_referer(url)
        if referer:
            request_headers["Referer"] = referer

        response = self._session.get(
            url,
            headers=request_headers or None,
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
        )
        response.raise_for_status()
        return response.text
