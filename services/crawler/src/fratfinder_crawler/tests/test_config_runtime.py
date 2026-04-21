from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from fratfinder_crawler.config import get_settings, resolve_env_file_path
from fratfinder_crawler.pipeline import CrawlService


def test_get_settings_loads_env_file_independent_of_current_working_directory(monkeypatch, tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("DATABASE_URL=postgresql://example/test\n", encoding="utf-8")
    nested = tmp_path / "nested" / "cwd"
    nested.mkdir(parents=True)

    monkeypatch.chdir(nested)
    monkeypatch.setenv("CRAWLER_ENV_FILE", str(env_file))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()

    settings = get_settings()

    assert settings.database_url == "postgresql://example/test"
    assert resolve_env_file_path() == env_file.resolve()


def test_resolve_runtime_mode_defaults_to_adaptive_assisted_even_when_legacy_config_is_present():
    service = CrawlService(SimpleNamespace(crawler_runtime_mode="legacy"))
    assert service._resolve_runtime_mode(None) == "adaptive_assisted"


def test_crawler_env_keys_override_legacy_agent_aliases(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://example/test")
    monkeypatch.setenv("CRAWLER_FIELD_JOB_RUNTIME_MODE", "langgraph_primary")
    monkeypatch.setenv("Agent:FIELD_JOB_RUNTIME_MODE", "legacy")
    get_settings.cache_clear()

    settings = get_settings()

    assert settings.crawler_field_job_runtime_mode == "langgraph_primary"
    get_settings.cache_clear()


def test_doctor_reports_deprecated_runtime_and_agent_env_warning(monkeypatch):
    monkeypatch.setenv("Agent:FIELD_JOB_RUNTIME_MODE", "langgraph_primary")

    class FakeRepository:
        def __init__(self, connection):
            self.connection = connection

        def get_field_job_queue_counts(self):
            return {"queued_jobs": 4, "actionable_jobs": 2, "deferred_jobs": 0, "blocked_provider_jobs": 0, "blocked_dependency_jobs": 0, "blocked_repairable_jobs": 0, "running_jobs": 0}

        def get_field_job_worker_process_stats(self, workload_lane: str = "contact_resolution"):
            return {"active_workers": 0 if workload_lane == "contact_resolution" else 1, "stale_workers": 0}

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("fratfinder_crawler.pipeline.get_connection", lambda settings: FakeConnection())
    monkeypatch.setattr("fratfinder_crawler.pipeline.CrawlerRepository", FakeRepository)

    service = CrawlService(
        SimpleNamespace(
            app_env="development",
            crawler_search_provider="auto",
            crawler_search_provider_order_free="searxng_json,duckduckgo_html,bing_html",
            crawler_runtime_mode="legacy",
            crawler_v3_crawl_runtime_mode="adaptive_primary",
            crawler_field_job_runtime_mode="langgraph_shadow",
            crawler_field_job_graph_durability="sync",
            crawler_v3_enabled=True,
            crawler_adaptive_enabled=True,
            crawler_search_searxng_base_url=None,
            crawler_search_serper_api_key=None,
            crawler_search_tavily_api_key=None,
            crawler_search_brave_api_key=None,
            crawler_http_timeout_seconds=2,
            crawler_http_verify_ssl=True,
        )
    )

    report = service.doctor()

    assert report["effectiveSettings"]["crawlRuntimeMode"] == "adaptive_assisted"
    assert report["effectiveSettings"]["liveRequestCrawlRuntimeMode"] == "adaptive_assisted"
    assert report["effectiveSettings"]["fieldJobRuntimeMode"] == "langgraph_primary"
    assert report["ok"] is False
    assert report["workerLiveness"]["alertOpen"] is True
    assert report["runtimeCompatibility"]["blockingIssues"]
    assert any("no longer controls new live executions" in warning for warning in report["warnings"])


def test_live_runtime_configuration_fails_fast_for_unsupported_modes():
    service = CrawlService(
        SimpleNamespace(
            crawler_v3_crawl_runtime_mode="adaptive_primary",
            crawler_field_job_runtime_mode="langgraph_shadow",
            crawler_field_job_graph_durability="sync",
        )
    )

    with pytest.raises(ValueError, match="Invalid live runtime configuration"):
        service._assert_live_runtime_configuration()
