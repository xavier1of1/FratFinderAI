from __future__ import annotations

from urllib.parse import urlparse

from fratfinder_crawler.config import Settings
from fratfinder_crawler.security.url_safety import safe_untrusted_get

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
        self._session = _SafeBrowserSession(
            {
                **_BROWSER_HEADERS,
                "User-Agent": _effective_user_agent(settings.crawler_http_user_agent),
            },
            max_body_bytes=settings.crawler_http_max_body_bytes,
            max_redirects=settings.crawler_http_max_redirects,
            allowed_content_types=[
                item.strip()
                for item in settings.crawler_http_allowed_content_types.split(",")
                if item.strip()
            ],
        )

    def get(self, url: str) -> str:
        referer = _origin_referer(url)
        request_headers = {"Referer": referer} if referer else {}
        response = self._session.get(
            url,
            headers=request_headers,
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
        )
        response.raise_for_status()
        return response.text


class _SafeBrowserSession:
    def __init__(
        self,
        headers: dict[str, str],
        *,
        max_body_bytes: int,
        max_redirects: int,
        allowed_content_types: list[str],
    ):
        self.headers = dict(headers)
        self._max_body_bytes = max_body_bytes
        self._max_redirects = max_redirects
        self._allowed_content_types = list(allowed_content_types)

    def get(self, url: str, *, headers=None, timeout=None, verify=None):
        return safe_untrusted_get(
            url,
            headers={**self.headers, **(headers or {})},
            timeout=timeout or 20.0,
            verify=True if verify is None else bool(verify),
            max_body_bytes=self._max_body_bytes,
            max_redirects=self._max_redirects,
            allowed_content_types=self._allowed_content_types,
        )
