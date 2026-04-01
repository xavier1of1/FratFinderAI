# Production Readiness Program: Website Recovery + Long-Run Campaign Benchmark

## Summary
Stabilize the website first, then add a campaign-grade full-stack benchmark system that can run a 1.5-2 hour, 20-fraternity end-to-end test with live website visibility, queue-progress monitoring, degradation detection, tuning actions, resumability, and post-run analysis.

Chosen defaults:
- Benchmark model: Campaign + Bench
- Fraternity mix: 18 new fraternities + 2 controls
- Headline success metric: chapter has any contact field found
- Stretch metrics: all-3-fields coverage, field-level coverage, throughput, provider efficiency

This program has two equally important goals:
1. restore website/runtime stability so operators can trust the dashboard during long runs
2. make the benchmark itself a durable product capability, not a one-off experiment

## Phase 0: Website Stability Recovery
Observed current failure:
- root page is 404
- static asset routes under `/_next/static/...` are 404
- logs show resolution failures for generated app/api route bundles under `apps/web/.next/server/...`
- prior pages compiled and worked, then degraded into route-level and asset-level misses

### Scope
Fix the web app so it is consistently usable before any long-run campaign work begins.

### Implementation changes
- Add a web runtime stabilization pass focused on the Next dev/build lifecycle:
  - ensure `.next` artifact generation and resolution are deterministic
  - remove any code or process assumptions that depend on stale generated files surviving hot reloads
  - verify route compilation and asset serving across `/`, `/chapters`, `/review-items`, `/benchmarks`, `/fraternity-intake`
- Add explicit startup/runtime guards:
  - health endpoint or startup check for app route availability
  - optional dashboard-visible `web runtime healthy/unhealthy` signal
- Add better web diagnostics:
  - structured logging for page/asset route failures
  - clearer capture of app route bundle resolution failures
  - distinguish between:
    - component render error
    - API error
    - Next build artifact error
    - static asset serving error
- Add a reproducible recovery path:
  - a documented/dev-safe restart strategy for local operator use
  - optionally a small server-side sanity self-check that verifies a minimal set of route endpoints after startup

### Acceptance criteria
- `/` returns 200
- `/chapters`, `/review-items`, `/benchmarks`, `/fraternity-intake` all return 200
- `_next/static` asset 404s are eliminated in steady-state
- route bundle resolution errors stop recurring during normal operator usage
- web app remains stable while a benchmark/campaign is running

## Phase 1: Campaign Benchmark Architecture
Current system truth:
- the website already has queue benchmarks
- the intake workflow already tracks per-fraternity discovery/crawl/enrichment progress
- benchmarks are queue-cycle oriented, not multi-fraternity campaign oriented

### Decision
Do not overload the existing benchmark system alone. Add a campaign layer and link it to benchmark summaries.

### New entities
Add:
- `campaign_runs`
  - `id`, `name`, `status`, `scheduled_for`, `started_at`, `finished_at`, `config`, `summary`, `telemetry`, `last_error`, timestamps
- `campaign_run_items`
  - `campaign_run_id`, `fraternity_name`, `fraternity_slug`, `request_id`, `cohort`, `status`, `scorecard`, `selection_reason`, `notes`, timestamps
- `campaign_run_events`
  - timeline entries for launch, preflight, degradation, tuning actions, checkpoints, completion/failure

### Status model
Campaign:
- `draft -> queued -> running -> succeeded | failed | canceled`

Campaign item:
- `planned -> request_created -> crawling -> enriching -> completed | failed | skipped`

### Relationships
- Each campaign item links to exactly one `fraternity_crawl_request`
- Campaign summaries may also create a linked benchmark-style rollup record for the Benchmarks page

## Phase 2: Website-Visible Campaign UI
Add a new operator surface instead of hiding this inside raw logs or queue benchmarks.

### New pages/components
- New nav item and page: `/campaigns`
- Campaign list view:
  - status
  - elapsed time
  - active items
  - queue delta
  - any-contact success rate
  - degradation badge
- Campaign detail view:
  - live fraternity roster
  - linked crawl request status for each fraternity
  - discovery/crawl/enrichment stage progress
  - provider health snapshot
  - queue depth over time
  - throughput over time
  - failure-mode histogram
  - tuning-action timeline
  - final scorecards and comparisons
- Benchmarks page enhancements:
  - support `benchmarkKind = queue | campaign`
  - show linked campaign summaries
  - keep current queue-only benchmark flow intact

### Campaign launch UX
Form should support:
- campaign name
- fraternity count default 20
- active concurrency default 4
- max duration default 120 min
- controls list override
- selection strategy preview
- optional source-scope restrictions
- optional `preflight required` toggle
- optional `auto-tuning enabled` toggle

### Acceptance criteria
- operator can launch, observe, and revisit a long-run campaign entirely from the website
- campaign remains visible during web refreshes/reloads
- benchmark and campaign summaries are readable without terminal access

## Phase 3: 20-Fraternity Selection and Launch Logic
### Default cohort policy
- 18 new fraternities:
  - from `verified_sources`
  - active only
  - not already represented in `chapters`
  - deterministic ordering
- 2 controls:
  - from previously-seen fraternities with historical comparison value
  - fixed in config or chosen deterministically from prior baseline set

### Selection metadata
Store for each selected fraternity:
- source provenance
- reason for inclusion (`new_pool` or `control_pool`)
- prior baseline if control exists
- source confidence / verified-source info

### Request creation behavior
For each item:
- create `fraternity_crawl_request`
- use discovery result or verified source
- queue automatically when confidence/source are sufficient
- store the linked request id and initial discovery metadata on the campaign item

### Batch admission logic
Do not launch all 20 at once.
Use:
- default active batch width 4
- admit new fraternity items only when prior items finish or stall
- this prevents queue flooding and makes tuning meaningful

## Phase 4: Long-Run Monitoring and Checkpoints
### Monitoring loop
Every 5 minutes:
- run provider health snapshot
- collect field-job queue counts
- collect request-stage progress
- collect per-provider attempt/success metrics
- append `campaign_run_event`
- update `campaign_runs.telemetry`

Every 15 minutes:
- compute trend windows:
  - throughput
  - requeue rate
  - provider-unavailable rate
  - queue burn-down
  - coverage growth
- determine whether the run is healthy, degraded, or stalled
- either:
  - continue
  - tune runtime knobs
  - pause admission of new items

### Resumability
Campaign state must survive:
- Next server restart
- operator browser refresh
- web route failure recovery
- benchmark-page revisit

Reconciliation source of truth:
- DB campaign row
- DB campaign item rows
- linked `fraternity_crawl_requests`
- field-job state
- crawl run state

No in-memory-only campaign state may be required to continue.

## Phase 5: Metrics, Scoring, and Successful Habits
The benchmark should teach the system something, not just produce a pass/fail.

### Core success metrics
Campaign-level:
- any-contact success rate
- all-3-fields success rate
- website coverage
- email coverage
- instagram coverage
- jobs/min
- queue depth delta
- fraternity completion rate
- average time per fraternity
- average time from discovery to first found contact

Per-fraternity:
- chapters discovered
- chapters enriched
- chapters with any contact
- chapters with all 3
- provider usage totals
- review-routed counts
- requeue counts
- terminal failures
- source-native yield

### Successful habits scoring
Keep deterministic, not ML.

Add a `CampaignScorecard` model with:
- `source_native_yield`
  - how much was resolved from nationals/school pages before broad search
- `search_efficiency`
  - contacts found per query/provider attempt
- `retry_efficiency`
  - useful retries vs wasted retries
- `confidence_quality`
  - high-confidence accepted writes vs review/low-confidence
- `provider_resilience`
  - success by provider and fallback depth used
- `queue_efficiency`
  - processed vs requeued vs terminal by time window

### Failure-mode classification
Persist counts and examples for:
- provider unavailable
- search degraded
- dependency wait
- source parse weak
- no candidate
- review-routed
- confidence rejected
- website missing blocks email
- source discovery weak
- request stalled
- route/web UI unavailable during run

### Reporting
Final campaign report should include:
- overall summary
- controls before/after delta
- new-fraternity yield
- provider comparison
- top failure modes
- top successful habits
- recommended config changes for next run

## Phase 6: Safe Auto-Tuning During Campaign
User asked for iterative improvement during the run. That should happen through safe runtime adjustments, not mid-run code edits.

### Allowed automatic tuning actions
Per campaign only:
- lower active fraternity concurrency
- lower worker count for search-heavy cycles
- reduce search budgets when provider degradation rises
- increase cooldown/backoff under challenge/unavailable spikes
- pause admission of new fraternity items
- resume normal admission after healthy checkpoints

### Tuning decision inputs
- provider health success rate
- requeue/processed ratio
- long cooldown concentration
- zero-progress windows
- request backlog age
- query-to-success efficiency

### Logging
Every tuning action must capture:
- reason
- before metrics
- knob changed
- new runtime values
- expected impact
- later outcome comparison

### Acceptance criteria
- tuning is visible on the website timeline
- tuning is campaign-local, not a silent global config mutation
- campaign can show whether a tuning action improved or worsened throughput

## Phase 7: Queue and Provider Safety for 2-Hour Runs
### Search/provider controls
Use current free-provider stack but make campaign-specific protections explicit:
- require preflight at campaign start
- require periodic provider health checks
- degrade gracefully instead of flooding the queue
- keep rate-limiting pressure visible in campaign telemetry

### Queue protections
- cap active admission breadth
- prevent requeue storms from dominating all workers
- track queue aging buckets
- surface starvation conditions explicitly
- preserve resumable progress even when providers degrade

### Campaign stop conditions
Campaign ends when:
- all items completed/failed/skipped, or
- max duration reached, or
- repeated provider degradation produces a configured safe-stop condition

Safe stop should still persist a partial report.

## Phase 8: Public Interfaces and Types
### New APIs
- `GET/POST /api/campaign-runs`
- `GET /api/campaign-runs/[id]`
- `POST /api/campaign-runs/[id]/resume`
- `POST /api/campaign-runs/[id]/cancel`

### New types
- `CampaignRun`
- `CampaignRunItem`
- `CampaignRunEvent`
- `CampaignRunConfig`
- `CampaignSummary`
- `CampaignScorecard`
- `CampaignProviderHealthSnapshot`

### Existing interface changes
- benchmark types gain:
  - `benchmarkKind`
  - optional `campaignRunId`
- intake request detail UI should expose linkage back to a campaign item when applicable

## Phase 9: Testing and Validation
### Website stability tests
- startup route smoke:
  - `/`
  - `/chapters`
  - `/review-items`
  - `/benchmarks`
  - `/fraternity-intake`
- static asset route validation
- restart/reload resilience validation
- no recurring `.next/server` route-resolution failures

### Backend campaign tests
- 18 new + 2 control deterministic selection
- request creation and linkage
- reconciliation after restart
- safe stop behavior
- tuning event persistence
- scoring calculation correctness

### UI tests
- launch campaign
- view live campaign progress
- benchmark linkage renders correctly
- degradation/tuning events visible
- final summary visible after completion

### End-to-end validation sequence
1. Short smoke campaign with 3 fraternities
2. Medium campaign with 6 fraternities and forced degraded window
3. Full 20-fraternity campaign, 1.5-2 hour runtime

### Final acceptance criteria
- website stays usable during the benchmark
- campaign is fully visible on the site
- queue progresses throughout the campaign, unless safely paused by explicit degradation logic
- campaign is resumable after restart
- final report is data-rich enough to guide the next iteration
- output proves whether throughput and coverage improved versus controls

## Phase 10: Scope Expansion and Product Hardening
To match the broader product vision, this plan also includes product-level hardening beyond the immediate benchmark.

### Additional useful features
- campaign export:
  - JSON report
  - CSV summary
- operator notes on campaign items
- ability to replay only failed fraternities from a finished campaign
- campaign comparison view:
  - current vs previous
  - control delta trend
- campaign-derived config recommendation panel
- website-visible provider health dashboard shared across campaigns and benchmarks
- dashboard banner when web runtime is degraded or route build artifacts are unstable

### Documentation updates
Update:
- README operational runbook
- CHANGELOG
- benchmark/campaign operator docs
- website recovery troubleshooting notes
- definition of success metrics and scorecards

## Assumptions and Defaults
- Website recovery is blocking and must happen first
- Campaign duration default is 120 minutes
- Checkpoint cadence is 5 minutes
- Tuning cadence is 15 minutes
- Active concurrency default is 4 fraternities
- Headline success = any contact found
- Stretch = all 3 contact fields found
- Runtime tuning is allowed only through safe campaign-local knobs
- The benchmark must be visible on the website, not just terminal logs
