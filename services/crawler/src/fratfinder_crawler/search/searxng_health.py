from __future__ import annotations

import json
import statistics
import subprocess
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlparse

import requests

from fratfinder_crawler.config import Settings


DEFAULT_HEALTH_QUERIES = [
    "delta chi chapter directory",
    'site:vt.edu "Delta Chi" "Fraternity and Sorority Life"',
    "sigma chi official chapter directory",
]

DEFAULT_ENGINE_SMOKE_QUERIES = [
    "delta chi chapter directory",
    'site:vt.edu "Delta Chi" "Fraternity and Sorority Life"',
    "lambda chi alpha virginia tech instagram",
    "sigma chi official chapter directory",
]

DEFAULT_ENGINE_CANDIDATES = [
    "startpage",
    "mojeek",
    "bing",
    "google",
    "duckduckgo",
    "brave",
    "qwant",
    "aol",
    "karmasearch",
]

SEARXNG_PROVIDER = "searxng_json"
LOW_SIGNAL_SMOKE_HOSTS = {
    "reddit.com",
    "www.reddit.com",
    "stackoverflow.com",
    "www.stackoverflow.com",
    "duckhuntingchat.com",
    "www.duckhuntingchat.com",
    "commons.wikimedia.org",
    "wikipedia.org",
    "en.wikipedia.org",
}

_ENGINE_BLOCKED_MARKERS = (
    "captcha",
    "suspended",
    "access denied",
    "forbidden",
    "blocked",
)
_ENGINE_RATE_LIMIT_MARKERS = (
    "too many request",
    "too many requests",
    "rate limit",
    "rate-limit",
    "429",
)


@dataclass(frozen=True, slots=True)
class SearxngProbe:
    base_url: str
    query: str
    engines: str | None
    http_status: int | None
    latency_ms: int
    reachable: bool
    json_parseable: bool
    result_bearing: bool
    result_count: int
    health_reason: str
    unresponsive_engines: list[object]
    top_urls: list[str]
    detail: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "baseUrl": self.base_url,
            "query": self.query,
            "engines": self.engines,
            "httpStatus": self.http_status,
            "latencyMs": self.latency_ms,
            "reachable": self.reachable,
            "jsonParseable": self.json_parseable,
            "resultBearing": self.result_bearing,
            "resultCount": self.result_count,
            "healthReason": self.health_reason,
            "unresponsiveEngines": self.unresponsive_engines,
            "topUrls": self.top_urls,
            "detail": self.detail,
        }


def searxng_endpoints_from_settings(settings: Settings) -> list[str]:
    endpoints: list[str] = []
    raw_multi = str(getattr(settings, "crawler_search_searxng_base_urls", "") or "").strip()
    if raw_multi:
        for token in raw_multi.split(","):
            value = str(token or "").strip().rstrip("/")
            if value and value not in endpoints:
                endpoints.append(value)
    single = str(getattr(settings, "crawler_search_searxng_base_url", "") or "").strip().rstrip("/")
    if not endpoints and single:
        endpoints.append(single)
    return endpoints


def effective_searxng_engines(settings: Settings) -> str:
    explicit = str(getattr(settings, "crawler_search_searxng_engines", "") or "").strip()
    if explicit:
        return explicit

    profile = str(getattr(settings, "crawler_search_searxng_engine_profile", "") or "auto").strip().lower()
    stable = str(getattr(settings, "crawler_search_searxng_stable_engines", "") or "").strip()
    rescue = str(getattr(settings, "crawler_search_searxng_rescue_engines", "") or "").strip()

    if profile == "stable":
        return stable
    if profile == "rescue":
        return rescue
    if profile == "auto" and stable:
        return stable
    return ""


def classify_unresponsive_engine_failure(unresponsive_engines: object) -> str | None:
    """Distinguish provider-level engine blocks from query-level empty results."""

    combined = json.dumps(unresponsive_engines or [], default=str).lower()
    if any(marker in combined for marker in _ENGINE_RATE_LIMIT_MARKERS):
        return "rate_limited_429"
    if any(marker in combined for marker in _ENGINE_BLOCKED_MARKERS):
        return "challenge_or_anomaly"
    return None


def classify_searxng_payload(
    *,
    http_status: int | None,
    payload: object | None,
    json_parseable: bool,
    request_error: str | None = None,
) -> tuple[str, bool, bool, int, list[object], list[str]]:
    if request_error:
        lowered = request_error.lower()
        if "timed out" in lowered or "timeout" in lowered:
            return "timeout", False, False, 0, [], []
        if "name or service not known" in lowered or "getaddrinfo" in lowered or "dns" in lowered:
            return "dns_error", False, False, 0, [], []
        if "refused" in lowered:
            return "connection_refused", False, False, 0, [], []
        return "endpoint_down", False, False, 0, [], []

    if http_status == 403:
        return "json_disabled", False, False, 0, [], []
    if http_status is not None and http_status >= 500:
        return "endpoint_down", False, False, 0, [], []
    if not json_parseable or not isinstance(payload, dict):
        return "json_disabled", False, False, 0, [], []

    raw_results = payload.get("results")
    results = raw_results if isinstance(raw_results, list) else []
    result_count = len(results)
    unresponsive = payload.get("unresponsive_engines")
    unresponsive_engines = unresponsive if isinstance(unresponsive, list) else []
    top_urls = [
        str(item.get("url") or item.get("link") or "").strip()
        for item in results[:3]
        if isinstance(item, dict) and str(item.get("url") or item.get("link") or "").strip()
    ]

    if result_count > 0 and unresponsive_engines:
        return "healthy_degraded", True, True, result_count, unresponsive_engines, top_urls
    if result_count > 0:
        return "healthy", True, True, result_count, unresponsive_engines, top_urls
    if unresponsive_engines:
        provider_failure = classify_unresponsive_engine_failure(unresponsive_engines)
        return provider_failure or "engine_unresponsive", True, False, result_count, unresponsive_engines, top_urls
    return "empty_results", True, False, result_count, unresponsive_engines, top_urls


def probe_searxng_endpoint(
    *,
    endpoint: str,
    query: str,
    engines: str | None,
    timeout: float,
    verify_ssl: bool,
    user_agent: str,
    requester: Callable[..., object] | None = None,
) -> SearxngProbe:
    started_at = time.monotonic()
    request_fn = requester or requests.get
    params: dict[str, str] = {"q": query, "format": "json"}
    if engines:
        params["engines"] = engines

    try:
        response = request_fn(
            f"{endpoint.rstrip('/')}/search",
            params=params,
            timeout=(min(2.0, timeout), max(2.0, timeout)),
            verify=verify_ssl,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )
        latency_ms = _elapsed_ms(started_at)
        status_code = int(getattr(response, "status_code", 0) or 0)
        payload: object | None = None
        json_parseable = False
        if status_code != 403:
            try:
                payload = response.json() if hasattr(response, "json") else json.loads(getattr(response, "text", "{}") or "{}")
                json_parseable = isinstance(payload, dict)
            except (ValueError, json.JSONDecodeError):
                json_parseable = False
        reason, reachable, result_bearing, result_count, unresponsive, top_urls = classify_searxng_payload(
            http_status=status_code,
            payload=payload,
            json_parseable=json_parseable,
        )
        return SearxngProbe(
            base_url=endpoint,
            query=query,
            engines=engines,
            http_status=status_code,
            latency_ms=latency_ms,
            reachable=reachable,
            json_parseable=json_parseable,
            result_bearing=result_bearing,
            result_count=result_count,
            health_reason=reason,
            unresponsive_engines=unresponsive,
            top_urls=top_urls,
        )
    except requests.RequestException as exc:
        latency_ms = _elapsed_ms(started_at)
        reason, reachable, result_bearing, result_count, unresponsive, top_urls = classify_searxng_payload(
            http_status=None,
            payload=None,
            json_parseable=False,
            request_error=str(exc),
        )
        return SearxngProbe(
            base_url=endpoint,
            query=query,
            engines=engines,
            http_status=None,
            latency_ms=latency_ms,
            reachable=reachable,
            json_parseable=False,
            result_bearing=result_bearing,
            result_count=result_count,
            health_reason=reason,
            unresponsive_engines=unresponsive,
            top_urls=top_urls,
            detail=str(exc),
        )


def run_searxng_health(
    settings: Settings,
    *,
    endpoints: list[str] | None = None,
    queries: list[str] | None = None,
    engines: str | None = None,
    timeout: float | None = None,
    include_docker_logs: bool = False,
    container_name: str = "searxng",
    log_tail: int = 80,
) -> dict[str, object]:
    resolved_endpoints = endpoints or searxng_endpoints_from_settings(settings)
    resolved_queries = queries or list(DEFAULT_HEALTH_QUERIES)
    resolved_engines = engines if engines is not None else effective_searxng_engines(settings)
    bounded_timeout = float(timeout or min(settings.crawler_http_timeout_seconds, 8.0))

    endpoint_reports: list[dict[str, object]] = []
    for endpoint in resolved_endpoints:
        probes = [
            probe_searxng_endpoint(
                endpoint=endpoint,
                query=query,
                engines=resolved_engines or None,
                timeout=bounded_timeout,
                verify_ssl=settings.crawler_http_verify_ssl,
                user_agent=settings.crawler_http_user_agent,
            ).as_dict()
            for query in resolved_queries
        ]
        reasons = Counter(str(probe.get("healthReason") or "unknown") for probe in probes)
        endpoint_reports.append(
            {
                "baseUrl": endpoint,
                "configuredEngines": resolved_engines,
                "probeCount": len(probes),
                "resultBearingRate": _ratio(sum(1 for probe in probes if probe.get("resultBearing")), len(probes)),
                "healthReasons": dict(reasons),
                "medianLatencyMs": _median([int(probe.get("latencyMs") or 0) for probe in probes]),
                "probes": probes,
            }
        )

    report: dict[str, object] = {
        "capturedAt": _utc_now_iso(),
        "provider": SEARXNG_PROVIDER,
        "endpoints": endpoint_reports,
        "summary": _summarize_endpoint_reports(endpoint_reports),
    }
    if include_docker_logs:
        report["dockerLogs"] = _tail_docker_logs(container_name=container_name, tail=log_tail)
    return report


def run_searxng_engine_smoke(
    settings: Settings,
    *,
    endpoint: str | None = None,
    engines: list[str] | None = None,
    queries: list[str] | None = None,
    max_queries: int | None = None,
    delay_ms: int | None = None,
    timeout: float | None = None,
) -> dict[str, object]:
    endpoints = searxng_endpoints_from_settings(settings)
    selected_endpoint = (endpoint or (endpoints[0] if endpoints else "")).strip().rstrip("/")
    if not selected_endpoint:
        raise ValueError("SearXNG endpoint is required for engine smoke testing")

    engine_candidates = engines or list(DEFAULT_ENGINE_CANDIDATES)
    query_candidates = queries or list(DEFAULT_ENGINE_SMOKE_QUERIES)
    if max_queries is not None:
        query_candidates = query_candidates[: max(1, int(max_queries))]
    bounded_timeout = float(timeout or min(settings.crawler_http_timeout_seconds, 8.0))

    rows: list[dict[str, object]] = []
    for engine in engine_candidates:
        for query in query_candidates:
            probe = probe_searxng_endpoint(
                endpoint=selected_endpoint,
                query=query,
                engines=engine,
                timeout=bounded_timeout,
                verify_ssl=settings.crawler_http_verify_ssl,
                user_agent=settings.crawler_http_user_agent,
            )
            rows.append({**probe.as_dict(), "engine": engine})
            if delay_ms and delay_ms > 0:
                time.sleep(max(0.0, float(delay_ms) / 1000.0))

    by_engine: dict[str, dict[str, object]] = {}
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("engine") or "")].append(row)
    for engine, items in grouped.items():
        reasons = Counter(str(item.get("healthReason") or "unknown") for item in items)
        result_bearing = sum(1 for item in items if item.get("resultBearing"))
        by_engine[engine] = {
            "engine": engine,
            "probeCount": len(items),
            "resultBearingRate": _ratio(result_bearing, len(items)),
            "healthyRate": _ratio(
                sum(1 for item in items if str(item.get("healthReason")) in {"healthy", "healthy_degraded"}),
                len(items),
            ),
            "lowSignalTopUrlRate": _low_signal_top_url_rate(items),
            "healthReasons": dict(reasons),
            "medianLatencyMs": _median([int(item.get("latencyMs") or 0) for item in items]),
            "p95LatencyMs": _p95([int(item.get("latencyMs") or 0) for item in items]),
            "sampleTopUrls": [url for item in items for url in (item.get("topUrls") or [])][:5],
        }

    allowlist = [
        engine
        for engine, summary in by_engine.items()
        if float(summary.get("resultBearingRate") or 0.0) >= 0.8
        and float(summary.get("lowSignalTopUrlRate") or 0.0) <= 0.25
        and int(summary.get("medianLatencyMs") or 0) <= 3000
        and not set(dict(summary.get("healthReasons") or {}).keys()).issubset({"engine_unresponsive", "json_disabled", "endpoint_down"})
        and not _engine_summary_has_suspended_or_challenge(items=grouped.get(engine, []))
    ]
    return {
        "capturedAt": _utc_now_iso(),
        "provider": SEARXNG_PROVIDER,
        "endpoint": selected_endpoint,
        "queryCountPerEngine": len(query_candidates),
        "engineCount": len(engine_candidates),
        "recommendedAllowlist": allowlist,
        "engines": by_engine,
        "probes": rows,
    }


def _summarize_endpoint_reports(endpoint_reports: list[dict[str, object]]) -> dict[str, object]:
    total_probes = sum(int(endpoint.get("probeCount") or 0) for endpoint in endpoint_reports)
    result_bearing = sum(
        sum(1 for probe in (endpoint.get("probes") or []) if isinstance(probe, dict) and probe.get("resultBearing"))
        for endpoint in endpoint_reports
    )
    reasons = Counter()
    for endpoint in endpoint_reports:
        reasons.update(dict(endpoint.get("healthReasons") or {}))
    return {
        "endpointCount": len(endpoint_reports),
        "totalProbes": total_probes,
        "resultBearingRate": _ratio(result_bearing, total_probes),
        "healthReasons": dict(reasons),
        "anyHealthyEndpoint": any(
            any(
                isinstance(probe, dict) and str(probe.get("healthReason")) in {"healthy", "healthy_degraded"}
                for probe in (endpoint.get("probes") or [])
            )
            for endpoint in endpoint_reports
        ),
    }


def _tail_docker_logs(*, container_name: str, tail: int) -> dict[str, object]:
    try:
        completed = subprocess.run(
            ["docker", "logs", "--tail", str(max(1, tail)), container_name],
            capture_output=True,
            text=True,
            timeout=12,
            check=False,
        )
    except Exception as exc:  # pragma: no cover - local diagnostic convenience
        return {"ok": False, "error": str(exc)}
    return {
        "ok": completed.returncode == 0,
        "returnCode": completed.returncode,
        "stdout": completed.stdout[-12000:],
        "stderr": completed.stderr[-12000:],
    }


def _low_signal_top_url_rate(items: list[dict[str, object]]) -> float:
    urls = [str(url or "") for item in items for url in (item.get("topUrls") or [])]
    if not urls:
        return 0.0
    low_signal = 0
    for url in urls:
        host = urlparse(url).netloc.lower()
        if host in LOW_SIGNAL_SMOKE_HOSTS or any(host.endswith(f".{blocked}") for blocked in LOW_SIGNAL_SMOKE_HOSTS):
            low_signal += 1
    return _ratio(low_signal, len(urls))


def _engine_summary_has_suspended_or_challenge(*, items: list[dict[str, object]]) -> bool:
    bad_markers = (
        "captcha",
        "suspended",
        "too many requests",
        "access denied",
        "forbidden",
        "blocked",
    )
    for item in items:
        reason = str(item.get("healthReason") or "").lower()
        detail = str(item.get("detail") or "").lower()
        unresponsive = json.dumps(item.get("unresponsiveEngines") or item.get("unresponsive_engines") or [], default=str).lower()
        combined = " ".join([reason, detail, unresponsive])
        if any(marker in combined for marker in bad_markers):
            return True
    return False


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def _median(values: list[int]) -> int:
    if not values:
        return 0
    return int(round(statistics.median(values)))


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * 0.95))))
    return int(ordered[index])


def _elapsed_ms(started_at: float) -> int:
    return max(0, int(round((time.monotonic() - started_at) * 1000.0)))


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
