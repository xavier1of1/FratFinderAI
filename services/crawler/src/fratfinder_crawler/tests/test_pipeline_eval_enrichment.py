from types import SimpleNamespace

from fratfinder_crawler.pipeline import CrawlService


class _ServiceWithHooks(CrawlService):
    def __init__(self, settings: SimpleNamespace):
        self._settings = settings
        self.preflight_calls = 0
        self.job_calls = []
        self._preflight_payload = {"healthy": True}

    def search_preflight(self, probes: int | None = None) -> dict[str, object]:
        self.preflight_calls += 1
        return dict(self._preflight_payload)

    def process_field_jobs(
        self,
        limit: int = 25,
        source_slug: str | None = None,
        field_name: str | None = None,
        workers: int | None = None,
        require_healthy_search: bool = False,
        run_preflight: bool | None = None,
        runtime_mode: str | None = None,
        graph_durability: str | None = None,
    ) -> dict[str, int]:
        self.job_calls.append(
            {
                "limit": limit,
                "source_slug": source_slug,
                "field_name": field_name,
                "workers": workers,
                "require_healthy_search": require_healthy_search,
                "run_preflight": run_preflight,
                "runtime_mode": runtime_mode,
                "graph_durability": graph_durability,
            }
        )
        return {"processed": 2, "requeued": 1, "failed_terminal": 0}


def _settings(*, run_preflight: bool = True, require_healthy: bool = True, search_enabled: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        crawler_adaptive_eval_enrichment_run_preflight=run_preflight,
        crawler_adaptive_eval_enrichment_require_healthy_search=require_healthy,
        crawler_search_enabled=search_enabled,
    )


def test_eval_enrichment_short_circuits_when_limit_is_zero():
    svc = _ServiceWithHooks(_settings())
    result = svc._run_eval_enrichment_for_sources(["a", "b"], limit_per_source=0, workers=4)
    assert result == {"processed": 0, "requeued": 0, "failed_terminal": 0, "skipped_provider_degraded": 0}
    assert svc.preflight_calls == 0
    assert svc.job_calls == []


def test_eval_enrichment_skips_all_sources_when_preflight_unhealthy():
    svc = _ServiceWithHooks(_settings(run_preflight=True, require_healthy=True))
    svc._preflight_payload = {"healthy": False, "success_rate": 0.0}
    should_skip, preflight = svc._should_skip_eval_enrichment(["alpha", "beta", ""])
    result = svc._run_eval_enrichment_for_sources(
        ["alpha", "beta", ""],
        limit_per_source=20,
        workers=2,
        skip_provider_degraded=should_skip,
        preflight_snapshot=preflight,
    )
    assert should_skip is True
    assert result["processed"] == 0
    assert result["requeued"] == 0
    assert result["failed_terminal"] == 0
    assert result["skipped_provider_degraded"] == 2
    assert svc.preflight_calls == 1
    assert svc.job_calls == []


def test_eval_enrichment_runs_jobs_with_health_guard_enabled():
    svc = _ServiceWithHooks(_settings(run_preflight=True, require_healthy=True))
    should_skip, preflight = svc._should_skip_eval_enrichment(["alpha", "beta"])
    result = svc._run_eval_enrichment_for_sources(
        ["alpha", "beta"],
        limit_per_source=5,
        workers=3,
        skip_provider_degraded=should_skip,
        preflight_snapshot=preflight,
    )
    assert should_skip is False
    assert result == {"processed": 4, "requeued": 2, "failed_terminal": 0, "skipped_provider_degraded": 0}
    assert svc.preflight_calls == 1
    assert len(svc.job_calls) == 2
    assert all(call["require_healthy_search"] is True for call in svc.job_calls)
    assert all(call["run_preflight"] is False for call in svc.job_calls)
