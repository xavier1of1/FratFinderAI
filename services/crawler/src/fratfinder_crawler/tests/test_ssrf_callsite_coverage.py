from __future__ import annotations

import ast
from pathlib import Path


APPROVED_RAW_REQUESTS_PATHS = {
    Path("security/url_safety.py"),
    Path("search/client.py"),
    Path("search/searxng_health.py"),
}


def test_untrusted_crawler_code_does_not_use_raw_requests_calls():
    src_root = Path(__file__).resolve().parents[1]
    violations: list[str] = []
    for path in src_root.rglob("*.py"):
        relative = path.relative_to(src_root)
        if relative.parts[0] == "tests" or relative in APPROVED_RAW_REQUESTS_PATHS:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                if func.value.id == "requests" and func.attr in {"get", "head", "post", "request", "Session"}:
                    violations.append(f"{relative}:{node.lineno} uses requests.{func.attr}")
    assert violations == []
