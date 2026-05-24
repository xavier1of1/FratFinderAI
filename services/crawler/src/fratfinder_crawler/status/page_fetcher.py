from __future__ import annotations

from typing import Callable

from fratfinder_crawler.security.url_safety import UrlSafetyError, safe_untrusted_get

from .campus_discovery import CampusSourceDocument


def fetch_page_document(
    *,
    url: str,
    requester: Callable[..., object] | None = None,
    timeout: float = 15,
) -> CampusSourceDocument | None:
    try:
        response = (
            requester(url, timeout=timeout)
            if requester is not None
            else safe_untrusted_get(url, timeout=timeout)
        )
    except (Exception, UrlSafetyError):
        return None
    status_code = getattr(response, "status_code", None)
    if status_code is None or int(status_code) >= 400:
        return None
    text = getattr(response, "text", "") or ""
    return CampusSourceDocument(page_url=str(getattr(response, "url", url) or url), text=text, html=text)
