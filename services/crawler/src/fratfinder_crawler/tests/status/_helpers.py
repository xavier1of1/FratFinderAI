from __future__ import annotations

from pathlib import Path


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures"


def load_fixture(*parts: str) -> str:
    return (FIXTURE_ROOT.joinpath(*parts)).read_text(encoding="utf-8")
