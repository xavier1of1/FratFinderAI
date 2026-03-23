from __future__ import annotations

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from fratfinder_crawler.config import Settings


class HttpClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._session = Session()
        self._session.headers.update({"User-Agent": settings.crawler_http_user_agent})
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
        response = self._session.get(
            url,
            timeout=self._settings.crawler_http_timeout_seconds,
            verify=self._settings.crawler_http_verify_ssl,
        )
        response.raise_for_status()
        return response.text
