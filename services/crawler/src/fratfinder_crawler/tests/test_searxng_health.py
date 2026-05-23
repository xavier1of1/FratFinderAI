from __future__ import annotations

from types import SimpleNamespace

import requests

from fratfinder_crawler.config import Settings
from fratfinder_crawler.search.searxng_health import (
    classify_searxng_payload,
    effective_searxng_engines,
    probe_searxng_endpoint,
    run_searxng_engine_smoke,
)


def test_searxng_payload_classifies_result_bearing_with_unresponsive_engines_as_degraded():
    reason, reachable, result_bearing, result_count, unresponsive, top_urls = classify_searxng_payload(
        http_status=200,
        payload={
            "results": [{"url": "https://example.org", "title": "Example"}],
            "unresponsive_engines": [["duckduckgo", "timeout"]],
        },
        json_parseable=True,
    )

    assert reason == "healthy_degraded"
    assert reachable is True
    assert result_bearing is True
    assert result_count == 1
    assert unresponsive == [["duckduckgo", "timeout"]]
    assert top_urls == ["https://example.org"]


def test_searxng_payload_classifies_empty_unresponsive_as_engine_unresponsive():
    reason, reachable, result_bearing, result_count, unresponsive, _top_urls = classify_searxng_payload(
        http_status=200,
        payload={"results": [], "unresponsive_engines": [["duckduckgo", "timeout"]]},
        json_parseable=True,
    )

    assert reason == "engine_unresponsive"
    assert reachable is True
    assert result_bearing is False
    assert result_count == 0
    assert unresponsive == [["duckduckgo", "timeout"]]


def test_searxng_payload_classifies_suspended_engine_as_provider_failure():
    reason, reachable, result_bearing, result_count, unresponsive, _top_urls = classify_searxng_payload(
        http_status=200,
        payload={"results": [], "unresponsive_engines": [["brave", "Suspended: too many requests"]]},
        json_parseable=True,
    )

    assert reason == "rate_limited_429"
    assert reachable is True
    assert result_bearing is False
    assert result_count == 0
    assert unresponsive == [["brave", "Suspended: too many requests"]]


def test_probe_searxng_endpoint_reports_json_disabled():
    def requester(url, params, timeout, verify, headers):
        return SimpleNamespace(status_code=403, text="forbidden")

    probe = probe_searxng_endpoint(
        endpoint="http://localhost:8888",
        query="delta chi chapter directory",
        engines=None,
        timeout=4.0,
        verify_ssl=True,
        user_agent="test",
        requester=requester,
    )

    assert probe.health_reason == "json_disabled"
    assert probe.reachable is False
    assert probe.json_parseable is False


def test_probe_searxng_endpoint_reports_connection_refused():
    def requester(url, params, timeout, verify, headers):
        raise requests.ConnectionError("connection refused")

    probe = probe_searxng_endpoint(
        endpoint="http://localhost:8888",
        query="delta chi chapter directory",
        engines=None,
        timeout=4.0,
        verify_ssl=True,
        user_agent="test",
        requester=requester,
    )

    assert probe.health_reason == "connection_refused"
    assert probe.reachable is False


def test_effective_searxng_engines_prefers_explicit_then_profile():
    settings = Settings(
        database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
        CRAWLER_SEARCH_SEARXNG_ENGINES="startpage",
        CRAWLER_SEARCH_SEARXNG_ENGINE_PROFILE="stable",
        CRAWLER_SEARCH_SEARXNG_STABLE_ENGINES="mojeek",
    )
    assert effective_searxng_engines(settings) == "startpage"

    settings = settings.model_copy(update={"crawler_search_searxng_engines": "", "crawler_search_searxng_engine_profile": "stable"})
    assert effective_searxng_engines(settings) == "mojeek"


def test_searxng_engine_smoke_summarizes_per_engine(monkeypatch):
    from fratfinder_crawler.search import searxng_health

    def fake_probe(*, endpoint, query, engines, timeout, verify_ssl, user_agent, requester=None):
        result = bool(engines == "startpage")
        return searxng_health.SearxngProbe(
            base_url=endpoint,
            query=query,
            engines=engines,
            http_status=200,
            latency_ms=25,
            reachable=True,
            json_parseable=True,
            result_bearing=result,
            result_count=1 if result else 0,
            health_reason="healthy" if result else "empty_results",
            unresponsive_engines=[],
            top_urls=["https://example.org"] if result else [],
        )

    monkeypatch.setattr(searxng_health, "probe_searxng_endpoint", fake_probe)
    report = run_searxng_engine_smoke(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
        ),
        engines=["startpage", "google"],
        queries=["delta chi"],
    )

    assert report["recommendedAllowlist"] == ["startpage"]
    assert report["engines"]["startpage"]["resultBearingRate"] == 1.0
    assert report["engines"]["google"]["resultBearingRate"] == 0.0


def test_searxng_engine_smoke_excludes_captcha_or_suspended_engines(monkeypatch):
    from fratfinder_crawler.search import searxng_health

    def fake_probe(*, endpoint, query, engines, timeout, verify_ssl, user_agent, requester=None):
        return searxng_health.SearxngProbe(
            base_url=endpoint,
            query=query,
            engines=engines,
            http_status=200,
            latency_ms=25,
            reachable=True,
            json_parseable=True,
            result_bearing=True,
            result_count=1,
            health_reason="healthy_degraded",
            unresponsive_engines=[[engines, "Suspended: too many requests"]],
            top_urls=["https://example.org"],
        )

    monkeypatch.setattr(searxng_health, "probe_searxng_endpoint", fake_probe)
    report = run_searxng_engine_smoke(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
        ),
        engines=["brave"],
        queries=["delta chi"],
    )

    assert report["recommendedAllowlist"] == []
