from __future__ import annotations

import json
from typing import Any

from fratfinder_crawler.adapters.script_json import _coerce_dict_list, _payloads_to_chapters
from fratfinder_crawler.models import ExtractedChapter


class LocatorApiAdapter:
    def parse(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client: Any | None = None,
    ) -> list[ExtractedChapter]:
        if not api_url or http_client is None:
            return []

        try:
            payload_text = http_client.get(api_url)
            data = json.loads(payload_text)
        except Exception:
            return []

        payloads = _coerce_dict_list(data)
        return _payloads_to_chapters(payloads, api_url)
