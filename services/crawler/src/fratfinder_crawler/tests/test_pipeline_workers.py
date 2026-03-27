from fratfinder_crawler.pipeline import _distribute_limit, _worker_id


def test_distribute_limit_caps_workers_to_limit():
    assert _distribute_limit(3, 8) == [1, 1, 1]


def test_distribute_limit_balances_work_evenly():
    assert _distribute_limit(10, 4) == [3, 3, 2, 2]


def test_distribute_limit_handles_zero_limit():
    assert _distribute_limit(0, 8) == []


def test_worker_id_suffixes_only_for_multi_worker_runs():
    assert _worker_id("local-crawler-worker", 1, 1) == "local-crawler-worker"
    assert _worker_id("local-crawler-worker", 3, 8) == "local-crawler-worker-3"
