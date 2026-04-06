# Evidence Index

This index lists the major sources used in the April 6, 2026 system audit.

## Design Docs Reviewed

- `docs/Diagrams/V3_SYSTEM_OVERVIEW.md`
- `docs/Diagrams/V3_DECISION_TREES.md`
- `docs/Diagrams/V3_QUEUE_PROCESSES.md`
- `docs/Diagrams/V4_PLATFORM_ARCHITECTURE.md`
- `docs/Diagrams/V4_LANGGRAPH_LOGIC_MAP.md`

## Crawler Runtime Files Reviewed

- `services/crawler/src/fratfinder_crawler/orchestration/request_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/adaptive_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/field_job_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/field_job_supervisor_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/navigation.py`
- `services/crawler/src/fratfinder_crawler/pipeline.py`
- `services/crawler/src/fratfinder_crawler/field_jobs.py`
- `services/crawler/src/fratfinder_crawler/models.py`
- `services/crawler/src/fratfinder_crawler/normalization/normalizer.py`
- `services/crawler/src/fratfinder_crawler/db/repository.py`
- `services/crawler/src/fratfinder_crawler/db/request_repository.py`
- `services/crawler/src/fratfinder_crawler/http/client.py`
- `services/crawler/src/fratfinder_crawler/search/client.py`
- `services/crawler/src/fratfinder_crawler/cli.py`

## Web / Control-Plane Files Reviewed

- `apps/web/src/lib/benchmark-runner.ts`
- `apps/web/src/lib/campaign-runner.ts`
- `apps/web/src/lib/fraternity-crawl-request-runner.ts`
- `apps/web/src/lib/runtime-comparison.ts`
- `apps/web/src/lib/types.ts`
- `apps/web/src/lib/repositories/benchmark-repository.ts`
- `apps/web/src/lib/repositories/campaign-run-repository.ts`
- `apps/web/src/lib/repositories/field-job-repository.ts`
- `apps/web/src/lib/repositories/field-job-graph-repository.ts`
- `apps/web/src/lib/repositories/agent-ops-repository.ts`
- `apps/web/src/lib/repositories/fraternity-crawl-request-repository.ts`
- `apps/web/src/app/api/benchmarks/route.ts`
- `apps/web/src/app/api/campaign-runs/route.ts`
- `apps/web/src/app/api/agent-ops/route.ts`
- `apps/web/src/app/api/runs/route.ts`
- `apps/web/src/app/api/field-jobs/route.ts`

## Migrations Reviewed

- `infra/supabase/migrations/0004_chapter_field_states.sql`
- `infra/supabase/migrations/0006_benchmark_runs.sql`
- `infra/supabase/migrations/0009_campaign_runs.sql`
- `infra/supabase/migrations/0013_adaptive_crawl_runtime.sql`
- `infra/supabase/migrations/0015_adaptive_epoch_metrics.sql`
- `infra/supabase/migrations/0017_field_job_langgraph_runtime.sql`
- `infra/supabase/migrations/0019_benchmark_alerts.sql`
- `infra/supabase/migrations/0020_v3_request_runtime.sql`

## Logs And Prior Reports Reviewed

- `docs/reports/V3_MVP_VALIDATION_2026-04-04.md`
- `docs/reports/SYSTEM_IMPROVEMENT_REPORT_2026-04-06.md`
- `docs/reports/cohort_runtime_summary_2026-04-01.json`
- `logs/campaign-watch/7f5d54cb-997d-4e1d-b080-ef24138d7059.log`
- `logs/campaign-watch/7f5d54cb-997d-4e1d-b080-ef24138d7059.alerts.log`
- `logs/campaign-watch/1077f82c-59f3-4257-92d1-790a6f7b0981.log`
- `logs/campaign-watch/1077f82c-59f3-4257-92d1-790a6f7b0981.alerts.log`

## Live API Snapshots Used

The audit used live reads from the running web app, especially:

- `/api/agent-ops`
- `/api/benchmarks`
- `/api/campaign-runs`
- `/api/runs`

Key observed values included:

- `fieldJobsQueued: 10823`
- `fieldJobsDeferred: 288`
- `fieldJobsTerminalNoSignal: 128`
- `fieldJobsReviewRequired: 314`
- `fieldJobsUpdated: 1920`
- `provisionalOpen: 287`
- `provisionalPromoted: 0`
- `chapterSearchCanonical: 633`
- `chapterSearchProvisional: 441`

## Direct Database Snapshots Used

Direct PostgreSQL inspection was used for:

- request status and stage distributions
- request graph run counts
- field-job graph run counts
- benchmark and campaign outcome counts
- queue-state distributions by source and field
- review reason counts
- evidence and provisional counts
- benchmark alert presence

Important observed values included:

- `request_graph_runs: 4 succeeded`
- `field_job_graph_runs: 104 succeeded, 6 failed`
- `benchmark_runs: 38 succeeded, 16 failed`
- `campaign_runs: 3 succeeded, 7 failed, 1 canceled`
- `crawl_runs: 157 succeeded, 54 partial, 44 failed`
- `provisional_open: 287`
- `provisional_promoted: 0`
- `benchmark_alerts: 0 rows`

## Why These Sources Matter

The audit deliberately combined:

- design intent
- code truth
- operational truth
- historical incident evidence

That combination is what allowed the report to compare the conceptual fraternity-chapter discovery strategy with the actual queue-processing implementation rather than judging the system from docs or code alone.
