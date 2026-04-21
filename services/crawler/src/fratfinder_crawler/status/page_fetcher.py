from __future__ import annotations

from typing import Callable

import requests

from .campus_discovery import CampusSourceDocument


def fetch_page_document(
    *,
    url: str,
    requester: Callable[..., object] | None = None,
    timeout: float = 15,
) -> CampusSourceDocument | None:
    get_request = requester or requests.get
    try:
        response = get_request(url, timeout=timeout)
    except Exception:
        return None
    status_code = getattr(response, "status_code", None)
    if status_code is None or int(status_code) >= 400:
        return None
    text = getattr(response, "text", "") or ""
    return CampusSourceDocument(page_url=str(getattr(response, "url", url) or url), text=text, html=text)
