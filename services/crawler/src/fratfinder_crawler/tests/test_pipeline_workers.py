from types import SimpleNamespace

from fratfinder_crawler.pipeline import CrawlService, _distribute_limit, _worker_id


def test_distribute_limit_caps_workers_to_limit():
    assert _distribute_limit(3, 8) == [1, 1, 1]


def test_distribute_limit_balances_work_evenly():
    assert _distribute_limit(10, 4) == [3, 3, 2, 2]


def test_distribute_limit_handles_zero_limit():
    assert _distribute_limit(0, 8) == []


def test_worker_id_suffixes_only_for_multi_worker_runs():
    assert _worker_id("local-crawler-worker", 1, 1) == "local-crawler-worker"
    assert _worker_id("local-crawler-worker", 3, 8) == "local-crawler-worker-3"


def test_resolve_field_job_runtime_mode_defaults_to_legacy_for_unknown():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="legacy", crawler_field_job_graph_durability="sync"))
    assert service._resolve_field_job_runtime_mode("unsupported") == "legacy"


def test_resolve_field_job_runtime_mode_uses_settings_default():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="langgraph_shadow", crawler_field_job_graph_durability="sync"))
    assert service._resolve_field_job_runtime_mode(None) == "langgraph_shadow"


def test_resolve_field_job_graph_durability_defaults_to_sync_for_unknown():
    service = CrawlService(SimpleNamespace(crawler_field_job_runtime_mode="legacy", crawler_field_job_graph_durability="sync"))
    assert service._resolve_field_job_graph_durability("invalid") == "sync"
