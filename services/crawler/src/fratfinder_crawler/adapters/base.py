from __future__ import annotations

from typing import Any, Protocol

from fratfinder_crawler.models import ChapterStub, ExtractedChapter


class ParserAdapter(Protocol):
    def parse_stubs(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client: Any | None = None,
        source_metadata: dict[str, Any] | None = None,
    ) -> list[ChapterStub]:
        ...

    def parse(
        self,
        html: str,
        source_url: str,
        *,
        api_url: str | None = None,
        http_client: Any | None = None,
        source_metadata: dict[str, Any] | None = None,
    ) -> list[ExtractedChapter]:
        ...
