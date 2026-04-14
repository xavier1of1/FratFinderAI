from fratfinder_crawler.orchestration.field_job_supervisor_graph import FieldJobSupervisorGraphRuntime


def test_supervisor_returns_zero_when_no_chunks():
    calls = []

    def _runner(*args, **kwargs):
        calls.append((args, kwargs))
        return {"processed": 1, "requeued": 0, "failed_terminal": 0}

    runtime = FieldJobSupervisorGraphRuntime(
        worker_limits=[],
        runtime_mode="legacy",
        graph_durability="sync",
        source_slug=None,
        field_name=None,
        degraded_mode=False,
        chunk_processor=_runner,
    )

    result = runtime.run()

    assert result["processed"] == 0
    assert result["requeued"] == 0
    assert result["failed_terminal"] == 0
    assert result["runtime_fallback_count"] == 0
    assert result["runtime_mode_used"] == "legacy"
    assert result["provider_degraded_deferred"] == 0
    assert result["dependency_wait_deferred"] == 0
    assert result["supporting_page_resolved"] == 0
    assert result["supporting_page_contact_resolved"] == 0
    assert result["external_search_contact_resolved"] == 0
    assert result["mid_batch_provider_rechecks"] == 0
    assert result["mid_batch_provider_reorders"] == 0
    assert result["preflight_probe_queries"] == []
    assert result["chapter_search_queries"] == []
    assert calls == []


def test_supervisor_aggregates_chunk_results():
    calls = []

    def _runner(limit, source_slug, field_name, worker_index, total_workers, degraded_mode, runtime_mode, graph_durability):
        calls.append(
            {
                "limit": limit,
                "source_slug": source_slug,
                "field_name": field_name,
                "worker_index": worker_index,
                "total_workers": total_workers,
                "degraded_mode": degraded_mode,
                "runtime_mode": runtime_mode,
                "graph_durability": graph_durability,
            }
        )
        return {
            "processed": limit,
            "requeued": worker_index,
            "failed_terminal": 0,
        }

    runtime = FieldJobSupervisorGraphRuntime(
        worker_limits=[2, 1],
        runtime_mode="langgraph_shadow",
        graph_durability="async",
        source_slug="sigma-chi-main",
        field_name="find_email",
        degraded_mode=True,
        chunk_processor=_runner,
    )

    result = runtime.run()

    assert result["processed"] == 3
    assert result["requeued"] == 3
    assert result["failed_terminal"] == 0
    assert result["runtime_fallback_count"] == 0
    assert result["runtime_mode_used"] == "langgraph_shadow"
    assert result["provider_degraded_deferred"] == 0
    assert result["dependency_wait_deferred"] == 0
    assert result["supporting_page_resolved"] == 0
    assert result["supporting_page_contact_resolved"] == 0
    assert result["external_search_contact_resolved"] == 0
    assert result["mid_batch_provider_rechecks"] == 0
    assert result["mid_batch_provider_reorders"] == 0
    assert result["preflight_probe_queries"] == []
    assert result["chapter_search_queries"] == []
    assert len(calls) == 2
    assert sorted(call["worker_index"] for call in calls) == [1, 2]
    assert all(call["total_workers"] == 2 for call in calls)
    assert all(call["runtime_mode"] == "langgraph_shadow" for call in calls)
    assert all(call["graph_durability"] == "async" for call in calls)
