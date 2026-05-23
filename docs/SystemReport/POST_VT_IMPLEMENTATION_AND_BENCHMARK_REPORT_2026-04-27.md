# Post-VT Implementation And Benchmark Report

Date: 2026-04-27

Primary scope: work completed after the Virginia Tech fraternity accuracy benchmark exposed school-scoped crawl and enrichment failures.

## Executive Summary

The VT benchmark failure was not caused by one isolated bug. It exposed a set of handoff failures between source recovery, chapter promotion, status-gated enrichment, Instagram resolution, and queue scheduling. Since that audit, the platform has been changed in three important ways:

1. School-scoped requests now carry school/chapter context into source recovery instead of falling back to fraternity-only discovery.
2. Crawls that see chapter-like records but promote no rows no longer silently succeed; they enter a `promotion_recovery_needed` stage for explicit recovery/review.
3. Field-job scheduling and status gates now prioritize prerequisite/status work and avoid burning provider-dependent jobs during degraded search windows.

The strongest measured improvement was the queue-throughput recovery benchmark: the bounded field-job run improved from `0 processed / 60 requeued` before the scheduler-priority fix to `56 processed / 3 requeued / 1 failed_terminal`, while `nationals_only_contact_rows` stayed at `0`.

The latest full benchmark on 2026-04-27 shows a different current bottleneck: the code paths validated, but live throughput dropped back to `0 processed` because every automatic search provider was unhealthy in that window. This is now primarily a search dependency availability issue, not a broad scheduler crash or unsafe-write regression.

## Starting Point: VT Benchmark Failure Modes

The VT audit identified these major failure classes:

- Source recovery was school-blind. `_recover_source()` rediscovered with fraternity-only context even when the task clearly implied a target school.
- Crawls could complete successfully after seeing records but promoting no target chapter rows.
- Instagram resolution was too search-first and did not consistently elevate trusted source-native evidence first.
- Search matching relied too heavily on brittle token overlap and special cases.
- The pipeline lacked a school-roster rescue anchor for school-scoped tasks.

The recommendation accepted for implementation was: official school roster evidence may create or promote a provisional chapter row when national evidence is weak, as long as provenance clearly records that the row was school-anchored.

## Implemented Changes

### 1. School-Conditioned Source Recovery

Main files:

- `services/crawler/src/fratfinder_crawler/orchestration/request_graph.py`
- `services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py`

What changed:

- Request source recovery now uses target school and chapter context when available.
- Recovery attempts include school roster, council, and school-specific chapter evidence before falling back to fraternity-only discovery.
- Snake-case discovery results such as `is_weak` are normalized before quality decisions, preventing recovered sources from being misread as weak.

Why it matters:

- This directly addresses the VT failure class where weak/dynamic national sources caused the request to revert to confirmation even though school evidence could have rescued the run.

### 2. Zero-Promotion Crawl Recovery

Main files:

- `infra/supabase/migrations/0035_request_stage_promotion_recovery.sql`
- `services/crawler/src/fratfinder_crawler/orchestration/request_graph.py`
- `apps/web/src/lib/types.ts`
- `apps/web/src/components/fraternity-intake-dashboard.tsx`
- `apps/web/src/app/api/fraternity-crawl-requests/[id]/expedite/route.ts`

What changed:

- Added the `promotion_recovery_needed` request stage.
- Crawls with records seen but no promoted rows or actionable enrichment work no longer report success by default.
- Web/operator types and intake UI now understand this stage.

Why it matters:

- This prevents false-success outcomes like `crawl_completed_without_promoting_vt_row`.
- Operators can now distinguish "the crawl ran and produced useful work" from "the crawler saw something but failed to bind/promote it."

### 3. Status-First Field-Job Scheduling

Main files:

- `services/crawler/src/fratfinder_crawler/field_jobs.py`
- `services/crawler/src/fratfinder_crawler/pipeline.py`
- `services/crawler/src/fratfinder_crawler/db/repository.py`
- `services/crawler/src/fratfinder_crawler/tests/status/test_status_first_field_jobs.py`
- `services/crawler/src/fratfinder_crawler/tests/test_repository_queue_state.py`
- `services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py`

What changed:

- Prerequisite/status work is claimed before contact enrichment.
- Stale `blocked_reason` values are cleared when jobs become truly actionable.
- Unresolved or review-required status decisions stay dependency-blocked instead of duplicating verification work.
- Email jobs respect the confident-website prerequisite consistently in both the main engine claim loop and LangGraph runtime.
- Provider hard-blocks are respected mid-job so degraded provider windows do not create churn.

Why it matters:

- This is the change that produced the strongest measured throughput recovery: `0/60` to `56/60` processed in the bounded live field-job benchmark.

### 4. Instagram And Contact Safety Improvements

Main files:

- `services/crawler/src/fratfinder_crawler/field_jobs.py`
- `services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py`
- `services/crawler/src/fratfinder_crawler/tests/status/test_status_first_field_jobs.py`

What changed:

- Instagram extraction remains permissive for real chapter-website, national-chapter, and provenance evidence when local chapter/school identity is present.
- School-branded Instagram handles are rejected unless the handle or local context carries chapter/fraternity identity.
- HQ-only, institutional, wrong-school, and garbage-handle matches remain rejected.

Why it matters:

- This addresses the VT observation that trusted evidence existed but broad residual search was still overused.
- It also preserves the safety rule that no generic national or school-office contact should be treated as chapter contact.

### 5. Provider And Runtime Hardening

Main files:

- `services/crawler/src/fratfinder_crawler/search/client.py`
- `services/crawler/src/fratfinder_crawler/pipeline.py`
- `services/crawler/src/fratfinder_crawler/tests/test_search_client.py`
- `services/crawler/src/fratfinder_crawler/tests/test_config_runtime.py`

What changed:

- Managed providers remain explicit opt-in and do not enter the automatic chain just because keys exist.
- `doctor` provider diagnostics now use bounded connect/read timeouts and avoid inherited proxy/environment behavior.
- Provider failure classes are preserved more explicitly in preflight and attempt logs.

Why it matters:

- Search degradation is now easier to diagnose as SearXNG engine unresponsive, Bing challenge/anomaly, DuckDuckGo timeout, quota exceeded, circuit open, and so on.

## Validation Results

### Deterministic Test Validation

Targeted benchmark validation command:

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests/test_config_runtime.py services/crawler/src/fratfinder_crawler/tests/test_search_client.py services/crawler/src/fratfinder_crawler/tests/test_repository_queue_state.py services/crawler/src/fratfinder_crawler/tests/test_field_job_graph_runtime.py services/crawler/src/fratfinder_crawler/tests/test_field_job_supervisor_graph.py services/crawler/src/fratfinder_crawler/tests/status services/crawler/src/fratfinder_crawler/tests/regression/test_historical_status_and_contact_failures.py -q
```

Result:

```text
118 passed
```

Additional validation recorded in `CHANGELOG.md`:

- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py services/crawler/src/fratfinder_crawler/tests/social/test_instagram_resolution.py services/crawler/src/fratfinder_crawler/tests/status services/crawler/src/fratfinder_crawler/tests/regression/test_historical_status_and_contact_failures.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests -q`
- `pnpm.cmd --filter @fratfinder/web typecheck`

### Queue-Throughput Recovery Benchmark

Before scheduler/status-priority fix:

```text
processed = 0
requeued = 60
failed_terminal = 0
productive_yield = 0.0
```

After scheduler/status-priority fix:

```text
processed = 56
requeued = 3
failed_terminal = 1
nationals_only_contact_rows = 0
```

Interpretation:

- The field-job scheduler and status gate no longer repeatedly claim jobs that should remain blocked.
- The system made measurable progress without weakening contact safety.

### Latest Full Benchmark: 2026-04-27

Artifacts:

- `docs/reports/stress/full-benchmark-doctor-20260427-$(Get-Date -Format HHmmss).json`
- `docs/reports/stress/full-benchmark-baseline-20260427-$(Get-Date -Format HHmmss).json`
- `docs/reports/stress/full-benchmark-preflight-20260427-$(Get-Date -Format HHmmss).json`
- `docs/reports/stress/full-benchmark-field-jobs-20260427-192520.json`
- `docs/reports/stress/full-benchmark-smoke-searxng-20260427-192625.json`
- `docs/reports/stress/full-benchmark-smoke-bing-20260427-192625.json`
- `docs/reports/stress/full-benchmark-db-analysis-20260427-193023.json`
- `docs/reports/stress/full-benchmark-targeted-tests-20260427-193128.txt`

Baseline queue:

```text
queued_jobs = 5,828
actionable_jobs = 1,204
deferred_jobs = 0
blocked_provider_jobs = 1,321
blocked_dependency_jobs = 1,826
blocked_repairable_jobs = 1,477
running_jobs = 7
```

Queue health:

```text
deferred_ratio = 0.0
provider_degraded_ratio = 0.2267
dependency_blocked_ratio = 0.3133
repair_backlog_ratio = 0.2534
worker_liveness_ratio = 1.0
worker_liveness_alert.open = false
```

Accuracy/safety snapshot:

```text
complete_rows = 1,615
chapter_specific_contact_rows = 1,615
nationals_only_contact_rows = 0
inactive_validated_rows = 177
confirmed_absent_website_rows = 27
active_rows_with_chapter_specific_email = 142
active_rows_with_chapter_specific_instagram = 1,572
active_rows_with_any_contact = 2,067
total_chapters = 3,410
```

Provider preflight:

```text
healthy = false
successes = 0 / 4 probes
viable_providers = []
reason = probe_success_below_threshold
```

Provider-specific failures:

```text
bing_html: 4 attempts, 0 successes, challenge_or_anomaly = 4
searxng_json: 4 attempts, 0 successes, engine_unresponsive = 4
duckduckgo_html: 4 attempts, 0 successes, timeout = 4
```

Bounded field-job run:

```text
processed = 0
requeued = 0
failed_terminal = 0
productive_yield = 0.0
runtime_mode_used = langgraph_primary
field_job_workers_active = 4
field_job_worker_alert_open = false
```

Interpretation:

- The runtime did not crash.
- Worker liveness was acceptable.
- The scheduler correctly avoided burning jobs during a fully failed search window.
- The current bottleneck is provider availability and upstream search health, not a broad field-job execution failure.

### Provider Smoke Tests

SearXNG smoke test:

```text
provider = searxng_json
query_count = 20
raw_success_rate = 0.0
unavailable_rate = 1.0
accepted_evidence_rate = 0.0
unsafe_candidate_rate = 0.0
promotion_gates.passed = false
primary failure = engine_unresponsive / circuit_open
```

Bing HTML smoke test:

```text
provider = bing_html
query_count = 20
raw_success_rate = 0.0
unavailable_rate = 0.9333
request_error_rate = 0.0667
challenge_anomaly_rate = 0.0667
accepted_evidence_rate = 0.0
unsafe_candidate_rate = 0.0
promotion_gates.passed = false
```

Interpretation:

- Current provider reliability is not sufficient for search-heavy live throughput.
- SearXNG is reachable as a local dependency but its upstream engines are suspended, timing out, blocked, or CAPTCHA-gated.
- Bing and DuckDuckGo are fallback-only at best in the current environment.

## Current Top Bottlenecks

| Rank | Bottleneck | Evidence | Impact |
| ---: | --- | --- | --- |
| 1 | Search provider health | Preflight `0/4`, SearXNG `engine_unresponsive`, Bing `challenge_or_anomaly`, DuckDuckGo `timeout` | Search-heavy jobs cannot safely run |
| 2 | Dependency/status backlog | `blocked_dependency_jobs = 2,165` in DB analysis; `status_dependency_unmet` and `dependency_wait` dominate | Contact enrichment waits behind status and website prerequisites |
| 3 | Repair/identity backlog | `blocked_repairable_jobs = 1,477`; `identity_semantically_incomplete` dominates | Many chapters cannot enter mainline execution until identity is repaired |
| 4 | Actionable jobs with blocked reasons | 525 actionable jobs still carried dependency-style `blocked_reason` values in DB analysis | Hot pool remains dirtier than desired |
| 5 | Historical provenance reason-code gap | `accepted_rows_missing_reason_code = 3,589` in DB analysis | KPI/audit quality is weaker for older completed rows |

## Improvements Since VT

### Functional Improvements

- School-scoped recovery now uses school/chapter context.
- Zero-promotion crawls enter a recovery state instead of silently succeeding.
- Status prerequisites are enforced before contact enrichment.
- Queue triage better separates provider, dependency, and repair-blocked work.
- Instagram/contact writes are safer around school and national generic sources.
- Runtime diagnostics now distinguish worker liveness from provider health.

### Measurable Improvements

- Field-job throughput recovered from `0 processed / 60 requeued` to `56 processed / 3 requeued / 1 failed_terminal` under a healthier provider window.
- Unsafe national-contact pollution remained at `0`.
- Worker liveness alert stayed closed during the latest benchmark despite poor provider health.
- Targeted deterministic validation passed: `118/118`.

### Observability Improvements

- Provider failures are now visible by class: `engine_unresponsive`, `challenge_or_anomaly`, `timeout`, `quota_exceeded`, `circuit_open`, and related categories.
- Request stages now surface `promotion_recovery_needed`.
- Queue state is more typed and operator-readable than before.

## What Did Not Improve Enough Yet

### Provider Reliability

The provider layer remains the largest live bottleneck. The latest benchmark had no viable automatic search provider. Because many jobs still require search, the scheduler correctly stopped rather than producing unsafe or meaningless work.

Recommended next work:

- Stabilize SearXNG upstream engine configuration.
- Add or validate a secondary SearXNG endpoint.
- Keep Bing HTML opportunistic only.
- Run isolated smoke cohorts for managed providers before promotion.

### Dependency Backlog

The system still has a large blocked dependency population. This is partly correct because contact jobs should not bypass status and website prerequisites, but the volume is high enough to affect throughput.

Recommended next work:

- Continue prerequisite-first scheduling.
- Focus on status verification and website/supporting-page unlockers.
- Reduce actionable jobs that still carry blocked reasons.

### Historical Provenance Gaps

Older completed rows still lack reason codes in `completed_payload`. Newer evidence contracts are stronger, but backfill is incomplete.

Recommended next work:

- Add a provenance reason-code backfill pass for historical completed rows where the reason can be reconstructed safely.
- Do not infer reason codes where evidence is insufficient.

## Risk Assessment

Low regression risk:

- The deterministic test suite is passing.
- Contact safety did not regress in latest KPI snapshots.
- Worker liveness is healthy.

Medium operational risk:

- Provider health is unstable enough to make live throughput intermittent.
- SearXNG is locally reachable but upstream engines can collectively fail.
- Historical queue state still has some actionable/blocked drift.

High product risk if ignored:

- If search-provider reliability is not fixed, new fraternity intake and broad enrichment will remain inconsistent even though the scheduling logic is much better.

## Conclusion

The post-VT work materially improved the platform architecture. The system now handles school-scoped recovery more intelligently, avoids false-success crawl completion, protects contact writes behind status/prerequisite gates, and has stronger queue/provider observability.

The most important measured win was queue execution under the fixed scheduler: `56/60` jobs processed versus the previous `0/60` failure mode, with no national-contact pollution.

The latest full benchmark shows the next limiting factor clearly: provider reliability. When SearXNG, Bing, and DuckDuckGo are all unhealthy, the scheduler correctly stops rather than wasting jobs or writing unsafe data. The next improvement cycle should therefore focus on SearXNG hardening, managed-provider smoke validation, and dependency-backlog reduction rather than another broad rewrite of the field-job runtime.
