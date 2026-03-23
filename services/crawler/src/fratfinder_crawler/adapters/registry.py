from __future__ import annotations

from fratfinder_crawler.adapters.base import ParserAdapter
from fratfinder_crawler.adapters.directory_v1 import DirectoryV1Adapter
from fratfinder_crawler.adapters.locator_api import LocatorApiAdapter
from fratfinder_crawler.adapters.script_json import ScriptJsonAdapter


class AdapterRegistry:
    def __init__(self) -> None:
        directory_adapter = DirectoryV1Adapter()
        self._adapters: dict[str, ParserAdapter] = {
            "repeated_block": directory_adapter,
            "table": directory_adapter,
            "script_json": ScriptJsonAdapter(),
            "locator_api": LocatorApiAdapter(),
        }

    def get(self, strategy_family: str) -> ParserAdapter | None:
        return self._adapters.get(strategy_family)
