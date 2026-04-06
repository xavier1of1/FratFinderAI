# Queue Architecture Recovery Program: Phases, Deliverables, and Acceptance Criteria

This document turns the queue-architecture recovery plan into an implementation program with explicit deliverables, phase gates, and end-state acceptance criteria.

The program is mission-first. Every phase is judged against the real product goal:

- find real fraternity chapters
- validate those entities correctly
- recover trustworthy website, email, and Instagram data
- preserve evidence
- avoid wasting queue and provider budget on invalid work

## Final Program Exit Criteria

The overall program is only complete when all of these are true:

1. no production `GET` route mutates queue or run state
2. benchmark and campaign execution survive web restarts because the web process no longer owns runtime state
3. request, repair, contact, and evaluation workloads are separately claimable and observable
4. repair is a durable queue lane, not embedded side logic only
5. queue-critical ordering and state do not depend on nested JSON payload fields
6. benchmark and campaign alerts are actually emitted and visible
7. provisional chapters can leave `open` through explicit promotion/review/reject workflows
8. dashboards distinguish actionable, blocked, repair, deferred, and historical queue work
9. run summaries distinguish runtime success from business success
10. docs clearly separate implemented, transitional, and target architecture

## Phase 1: Read Paths Become Pure Reads

Status: `Completed on 2026-04-06`

### Deliverables

- All `GET` API routes used for operations dashboards become read-only.
- Runtime mutations currently hidden in read routes move to explicit write endpoints or remain temporarily in existing write actions.
- A temporary explicit maintenance endpoint exists for manual/staged operational actions that were previously hidden in reads.
- The health endpoint becomes observational only.

### Acceptance Criteria

- `GET /api/campaign-runs` does not reconcile, schedule, or resume runs.
- `GET /api/campaign-runs/[id]` does not schedule or resume a run.
- `GET /api/benchmarks` and benchmark detail/export reads do not fail stale runs.
- `GET /api/runs` and `GET /api/agent-ops` do not reconcile stale crawl runs.
- `GET /api/health` does not schedule campaigns or scan alerts.
- A test suite exists proving these GET routes do not call the old mutating functions.

### Validation Completed

- Added explicit regression coverage in `apps/web/src/app/api/read-only-routes.test.ts`.
- Added `apps/web/vitest.config.ts` so route tests can resolve app aliases.
- Validated with:
  - `pnpm --filter @fratfinder/web test`
  - `pnpm --filter @fratfinder/web typecheck`
  - `pnpm --filter @fratfinder/web build`

## Phase 2: Durable Worker Ownership

Status: `Completed on 2026-04-06`

### Deliverables

- Add worker lease and heartbeat model for long-running queue ownership.
- Add a backend-owned worker supervisor concept and durable worker identity.
- Replace in-memory active-run state as the source of truth for production work ownership.

### Acceptance Criteria

- Web process restart does not lose benchmark/campaign/request ownership state.
- Lease expiry can recover abandoned work safely.
- At least one worker table or equivalent persisted ownership record exists and is used by production workers.

### Validation Completed

- Added `infra/supabase/migrations/0021_runtime_worker_leases.sql` to introduce `worker_processes` plus runtime lease columns for:
  - `benchmark_runs`
  - `campaign_runs`
  - `fraternity_crawl_requests`
- Added web runtime-worker repository support in `apps/web/src/lib/repositories/runtime-worker-repository.ts`.
- Updated benchmark and campaign runners to claim durable DB leases and heartbeat them while runs are active.
- Extended the Python request worker to:
  - register a durable request-lane worker record
  - claim request leases
  - heartbeat request leases during long-running graph execution
  - release request leases on completion
- Exposed request, benchmark, and campaign runtime lease metadata through the web repositories/types.
- Added request-worker regression coverage updates in `services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py`.
- Validated with:
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py -q`
  - `pnpm --filter @fratfinder/web typecheck`
  - `pnpm --filter @fratfinder/web test`
  - `pnpm --filter @fratfinder/web build`
- Applied the lease migration against the configured local development database and verified:
  - `worker_processes` exists
  - `benchmark_runs` has runtime lease columns
  - `fraternity_crawl_requests` has runtime lease columns

## Phase 3: Typed Queue-State Foundation

Status: `Completed on 2026-04-06`

### Deliverables

- Add typed workflow columns for hot queue state.
- Backfill existing queue rows.
- Convert claim/order queries to typed state fields.
- Keep JSON for diagnostics only during transition.

### Acceptance Criteria

- Queue claim queries no longer depend on nested JSON payload fields for primary ordering/state.
- Dashboards and repositories can answer queue-state questions without JSON reconstruction for hot paths.
- Existing queue rows are backfilled sufficiently for mixed old/new operation.

### Validation Completed

- Added `infra/supabase/migrations/0022_field_job_typed_queue_state.sql` with typed queue-state columns on `field_jobs`:
  - `queue_state`
  - `validity_class`
  - `repair_state`
  - `blocked_reason`
  - `terminal_outcome`
- Backfilled existing `field_jobs` rows from legacy JSON payload/completed payload state in the migration.
- Converted hot claim/order paths in `services/crawler/src/fratfinder_crawler/db/repository.py` to use typed queue-state columns instead of nested JSON.
- Updated request/runtime queue snapshots and Agent Ops aggregates to use typed queue-state and terminal-outcome columns.
- Extended the `FieldJob` model so runtime code can consume typed queue state directly.
- Validated with:
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py -q`
  - `pnpm --filter @fratfinder/web typecheck`
  - `pnpm --filter @fratfinder/web test`
  - `pnpm --filter @fratfinder/web build`
- Applied the typed queue-state migration locally and verified:
  - typed `field_jobs` columns exist in the development database
  - backfilled queue-state distribution is queryable directly from `field_jobs`
  - terminal outcomes are queryable directly from `field_jobs`

## Phase 4: First-Class Repair Queue

Status: `Completed on 2026-04-06`

### Deliverables

- Introduce `chapter_repair_jobs`.
- Route repairable candidates into that queue instead of inline-only repair logic.
- Add repair worker processing, repair outcomes, and repair metrics.

### Acceptance Criteria

- Repairable candidates do not enter contact resolution before repair completes.
- Repair lane is separately visible in dashboards and summaries.
- Repair outcomes include promote, downgrade, confirm invalid, and exhausted.

### Validation Completed

- Added `infra/supabase/migrations/0023_chapter_repair_jobs.sql` to create the durable `chapter_repair_jobs` lane.
- Added backend repository support for:
  - enqueueing repair jobs
  - claiming repair jobs
  - completing repair jobs with explicit outcomes
  - listing related queued field jobs by chapter
- Changed field-job queue triage so repairable candidates enqueue repair work instead of performing inline-only repair.
- Added backend repair-queue processing to `process_field_jobs`, preserving the existing chapter-repair analytics contract while making repair operational.
- Added Agent Ops visibility for:
  - queued repair jobs
  - running repair jobs
- Added repair-lane regression coverage in `services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py`, including:
  - repair job enqueue behavior
  - promote-to-canonical repair processing
- Validated with:
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py -q`
  - `pnpm --filter @fratfinder/web typecheck`
  - `pnpm --filter @fratfinder/web test`
  - `pnpm --filter @fratfinder/web build`
- Applied the repair-lane migration locally and verified the `chapter_repair_jobs` table exists in the development database.

## Phase 5: Graph-Native Contact Resolution

Status: `Completed on 2026-04-06`

### Deliverables

- Contact resolution becomes graph-native in production.
- Imperative field-job engine becomes rollback-only.
- Business progress semantics are added to contact execution summaries.

### Acceptance Criteria

- Contact queue processing uses the graph-native path by default in production.
- Runs with zero business progress are visible as such even if runtime succeeds.
- Hard caps prevent unbounded low-signal churn and requeue storms.

### Validation Completed

- Preserved `langgraph_primary` as the production-default contact runtime path while keeping the imperative path available only as a fallback/rollback surface.
- Added business-progress semantics to field-job graph run summaries in `services/crawler/src/fratfinder_crawler/orchestration/field_job_graph.py`:
  - `businessStatus = progressed | no_business_progress`
  - `businessProgressCount`
- Added focused graph-runtime coverage in `services/crawler/src/fratfinder_crawler/tests/test_field_job_graph_runtime.py` for:
  - a clean no-job/no-progress run
  - a real progressed run
- Revalidated supervisor/runtime behavior with:
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_field_job_graph_runtime.py services/crawler/src/fratfinder_crawler/tests/test_field_job_supervisor_graph.py -q`
  - `pnpm --filter @fratfinder/web build`
  - `pnpm --filter @fratfinder/web typecheck`
  - `pnpm --filter @fratfinder/web test`

## Phase 6: Durable Evaluation Lane

Status: `Completed on 2026-04-06`

### Deliverables

- Benchmarks and campaigns move to backend-owned evaluation jobs.
- Per-source isolated work units replace monolithic long-running rounds.
- Benchmark preconditions and isolation mode are persisted.

### Acceptance Criteria

- One pathological source cannot kill an entire training or campaign round.
- Authoritative benchmarks can run in an explicitly isolated mode.
- Campaign and benchmark runs survive web restart and remain resumable.

### Validation Completed

- Added `infra/supabase/migrations/0024_evaluation_jobs.sql` to create the durable `evaluation_jobs` lane with:
  - benchmark/campaign job ownership
  - lease fields
  - isolation mode
  - persisted preconditions/results
- Added `apps/web/src/lib/repositories/evaluation-job-repository.ts` for durable evaluation job enqueue, claim, heartbeat, completion, failure, and precondition capture.
- Added the separate evaluation worker process:
  - `apps/web/src/lib/evaluation-worker.ts`
  - `apps/web/scripts/evaluation-worker.ts`
  - `apps/web/package.json` script: `worker:evaluation`
- Changed benchmark and campaign API writes so they enqueue durable evaluation jobs instead of directly scheduling in-process execution:
  - `apps/web/src/app/api/benchmarks/route.ts`
  - `apps/web/src/app/api/campaign-runs/route.ts`
  - `apps/web/src/app/api/campaign-runs/[id]/resume/route.ts`
- Updated campaign and benchmark execution to run under worker-owned execution paths and to persist benchmark isolation metadata in summaries.
- Reworked V4 campaign training rounds so adaptive train/eval executes in isolated per-source units instead of one monolithic source batch, preventing one failing source unit from killing the entire round by default.
- Added route regression coverage proving benchmark/campaign POSTs now enqueue evaluation work instead of relying on the old web-owned scheduler path.
- Validated with:
  - `pnpm --filter @fratfinder/web test`
  - `pnpm --filter @fratfinder/web typecheck`
  - `pnpm --filter @fratfinder/web build`
- Applied the evaluation-jobs migration locally and verified the table/indexes exist.
- Ran real worker smoke tests through the new evaluation lane:
  - shared-mode benchmark job completed as `succeeded` through `evaluation_jobs`
  - strict-isolation benchmark job completed as `succeeded` with:
    - `summary.isolationMode = strict_live_isolated`
    - `summary.contaminationStatus = isolated`

## Phase 7: Alerting and Provisional Closure

Status: `Completed on 2026-04-06`

### Deliverables

- Benchmark/campaign/queue/repair alerts are emitted through a real operational loop.
- Provisional workflow can leave `open`.
- Operator-visible backlog and age are available for provisionals and alerts.

### Acceptance Criteria

- Failed benchmark and campaign incidents create alerts.
- Provisional rows can become `promoted`, `review_required`, or `rejected`.
- Open operational problems are visible without reading raw logs or DB tables.

### Validation Completed

- Added `infra/supabase/migrations/0025_ops_alerts.sql` to create the durable `ops_alerts` table for benchmark, campaign, queue, repair, provider, and system incidents.
- Added `apps/web/src/lib/repositories/ops-alert-repository.ts` for:
  - open-alert upsert
  - fingerprint-based resolution
  - summary aggregation
  - recent-alert listing
- Integrated evaluation-worker failure handling with real alert emission and resolution in:
  - `apps/web/src/lib/evaluation-worker.ts`
- Extended Agent Ops with visible alert and provisional workflow state:
  - open/critical/warning/resolved alert counts
  - oldest open alert age
  - oldest open provisional age
  - recent ops-alert table
  - provisional review/rejected counts
- Implemented provisional exit workflow in the request graph so provisional rows can now leave `open` through:
  - `promoted`
  - `review`
  - `rejected`
- Added regression coverage for provisional promotion/review/reject behavior in:
  - `services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py`
- Applied the ops-alert migration locally and validated real smoke scenarios:
  - benchmark strict-isolation failure created a warning ops alert
  - campaign strict-isolation failure created a warning ops alert
  - Agent Ops summary reflected the open alerts
  - a live provisional smoke request produced one `promoted`, one `review`, and one `rejected` provisional row
- Validated with:
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py -q`
  - `pnpm --filter @fratfinder/web test`
  - `pnpm --filter @fratfinder/web build`
  - `pnpm --filter @fratfinder/web typecheck`

## Phase 8: Dashboard and Documentation Convergence

Status: `Completed on 2026-04-06`

### Deliverables

- Lightweight summary read models for campaigns, benchmarks, and queue health.
- Heavy detail loads move to drill-down endpoints.
- Architecture docs explicitly distinguish implemented, transitional, and target state.

### Acceptance Criteria

- Campaign list loads only summary data by default.
- Agent Ops and queue dashboards reflect the new workload-lane model.
- Docs no longer present the target graph-native architecture as if it were already fully implemented.

### Validation Completed

- Changed `listCampaignRuns()` so campaign list reads load summary rows only by default, while drill-down detail remains available through `getCampaignRun()` and the campaign detail API.
- Verified summary-vs-detail behavior live:
  - campaign list rows now return `items=[]` and `events=[]`
  - campaign detail continues to return full item/event payloads for drill-down
- Expanded Agent Ops queue-health metrics so the dashboard now distinguishes:
  - actionable queued contact work
  - deferred queued contact work
  - blocked invalid queued work
  - blocked repairable queued work
  - queued/running/completed repair work
  - historical queue reconciliation work
- Added explicit architecture-status separation in `docs/Diagrams`:
  - `CURRENT_IMPLEMENTED_QUEUE_ARCHITECTURE.md` as the implementation-accurate view
  - `V4_PLATFORM_ARCHITECTURE.md` marked as `Transitional`
  - `V3_SYSTEM_OVERVIEW.md` marked as `Target`
  - `docs/Diagrams/README.md` updated with the implemented/transitional/target status guide
- Added business-progress semantics to benchmark, campaign, and request summaries so run summaries distinguish runtime completion from useful progress.
- Stabilized the web TypeScript validation command by updating the web package scripts to run `tsc --noEmit --incremental false`, eliminating the recurring `.next/types` incremental-state failure mode.
- Finalized web typecheck stability with `apps/web/tsconfig.typecheck.json`, so the web `typecheck` command no longer depends on Next-generated `.next/types` state.
- Validated with:
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py -q`
  - `pnpm --filter @fratfinder/web test`
  - `pnpm --filter @fratfinder/web build`
  - `pnpm --filter @fratfinder/web typecheck`
  - live summary/detail smoke confirming campaign summary lists are lightweight by default
  - live Agent Ops summary smoke confirming actionable/deferred/blocked/repair lane counters are queryable

## Program Closeout

Status: `Completed on 2026-04-06`

All final program exit criteria at the top of this document have now been implemented and validated in the repository:

1. production GET routes are observational only
2. benchmark/campaign execution is backend-owned and durable
3. request, repair, contact, and evaluation lanes are separately claimable and observable
4. repair is a durable queue lane
5. queue-critical field-job ordering uses typed relational state rather than nested JSON
6. benchmark and campaign alerts are emitted and visible
7. provisional chapters can leave `open`
8. dashboards distinguish actionable, blocked, repair, deferred, and historical queue work
9. run summaries distinguish runtime completion from business progress
10. docs clearly separate implemented, transitional, and target architecture
