# Provider Telemetry Fix

Date: `2026-04-20`

## What Changed

This change fixes a major observability flaw in provider-health telemetry.

Before this change:

- `request_graph.py` wrote one `provider_health_snapshots` row per provider.
- Each row used the overall batch-level `preflight_snapshot["healthy"]` boolean.
- That meant a degraded provider could be stored as `healthy=true` whenever some other provider kept the batch healthy.

After this change:

- preflight now preserves provider-specific failure detail:
  - `low_signal`
  - `challenge_or_anomaly`
  - `failure_types`
  - provider-level `healthy`
  - provider-level `health_reason`
- request-graph snapshot writes now persist provider-specific health instead of the overall batch verdict.
- each persisted payload now includes:
  - `provider_healthy`
  - `provider_health_reason`
  - `batch_healthy`
  - `preflight_min_success_rate`
  - `viable_provider`

## Why It Matters

This has reliable operational impact because it removes a source of false confidence in diagnostics and future provider research.

Concretely:

- we can now distinguish "batch healthy because Bing worked" from "SearXNG healthy"
- provider audits can trust snapshot rows more directly
- future reordering and fallback tuning can use the persisted provider truth
- challenge/anomaly and low-signal behavior is now visible instead of being flattened away

## Files Changed

- [pipeline.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\pipeline.py)
- [request_graph.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\orchestration\request_graph.py)
- [test_pipeline_workers.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\tests\test_pipeline_workers.py)
- [test_request_graph_runtime.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\tests\test_request_graph_runtime.py)

## Validation

Focused validation:

- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py -k "search_preflight or provider_window_logic"`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py -k "provider_specific_health or runs_enrichment_cycle_until_queue_drains"`

Broader validation:

- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py`

Results:

- `41/41` tests passed in `test_pipeline_workers.py`
- `13/13` tests passed in `test_request_graph_runtime.py`

## Practical Outcome

This does not directly raise crawl throughput by itself.

It does something more foundational:

- it makes provider diagnosis trustworthy
- it improves future provider comparison work
- it removes one misleading metric path that could cause bad provider decisions

This is a safe, validated improvement that supports the next round of provider hardening and search-stack tuning.
