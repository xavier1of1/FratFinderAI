## [3.0.0] - 2026-04-04

### Added
- Added the V3 request-worker runtime and LangGraph request supervisor flow across `services/crawler`, including request graph runs, events, checkpoints, evidence writes, provisional chapter persistence, and provider-health snapshots.
- Added the Agent Ops web surface and API for V3 runtime inspection:
  - `GET /api/agent-ops`
  - `/agent-ops`
- Added request-level queue health and graph metadata visibility to the website, including:
  - V3 queue/runtime summary cards in Overview
  - request graph metadata in Fraternity Intake
  - provisional/evidence summary cards in Agent Ops
- Added live V3 validation documentation in `docs/reports/V3_MVP_VALIDATION_2026-04-04.md`.

### Changed
- Production crawl-request execution now routes through the dedicated Python V3 worker path instead of the web-owned shell-out scheduler when `CRAWLER_V3_ENABLED=true`.
- The default V3 crawl core remains `legacy` while request orchestration, field-job continuation, checkpoints, and observability are LangGraph-native.
- Website metadata, release framing, and package versions now identify this release as `V3.0.0`.

### Validated
- Validated a live V3 request against the latest comparable V2 request path, with V3 completing materially faster while matching useful output.
- Validated a multi-request queue-drain run where the V3 worker processed `2` fresh queued requests, completed both successfully, and left `0` pending request-level backlog.
- Validated the website through `next build`, `typecheck`, `vitest`, and targeted crawler pytest coverage for the request worker/runtime.

## [2.1.2] - 2026-04-04

### Added
- Added demo recovery report artifact: `docs/reports/DEMO_READINESS_RECOVERY_2026-04-04.md`.
- Added source-quality and selection-rationale payload contract fields in discovery output (`source_quality`, `selected_candidate_rationale`).

### Changed
- Hardened source discovery policy to avoid auto-selecting blocked/weak hosts and to emit explicit reject reasons and per-query provider failure traces.
- Expanded fraternity alias/normalization handling for common NIC aliases and Greek-letter variants, including `???` and `???` inputs.
- Request intake now only auto-queues when source quality is safe; weak sources are held in `awaiting_confirmation`.
- Crawl request runner now performs deterministic pre-run source validation/recovery and moves zero-chapter weak-source outcomes to `awaiting_confirmation` instead of silent terminal dead-ends.
- Enrichment execution now supports LangGraph-forward runtime with explicit legacy fallback on runtime errors, including observable `runtime_fallback` events.
- `process-field-jobs` now returns runtime execution metadata (`runtime_mode_used`, `runtime_fallback_count`) for benchmark/request analytics.

### Fixed
- Fixed potential mis-selection of unsafe discovery candidates by adding final source-quality gating and curated-safe fallback behavior.
- Fixed brittle field-job output parsing fallback logic and improved trailing-JSON parsing reliability in the request runner.

## [2.1.1] - 2026-04-03

### Added
- Added `0017_field_job_langgraph_runtime.sql` with LangGraph field-job runtime telemetry tables: `field_job_graph_runs`, `field_job_graph_events`, `field_job_graph_checkpoints`, and `field_job_graph_decisions`.
- Added `FieldJobDecision` model and repository APIs for field-job graph run lifecycle, event emission, checkpoint persistence, and decision logging.
- Added `FieldJobGraphRuntime` (`services/crawler/src/fratfinder_crawler/orchestration/field_job_graph.py`) with explicit node orchestration (`load_job`, `evaluate_preconditions`, `resolve_job`, `decide_outcome`, `persist_outcome`, `finalize`).
- Added field-job runtime controls to benchmark configuration and UI: `fieldJobRuntimeMode` and `fieldJobGraphDurability`.
- Added `0018_benchmark_shadow_diffs.sql` to persist per-cycle LangGraph shadow-diff artifacts for benchmark runs.
- Added new web APIs for LangGraph field-job telemetry and exports:
  - `GET /api/field-jobs/graph-runs`
  - `GET /api/field-jobs/graph-runs/[id]`
  - `GET /api/benchmarks/[id]/export?format=json|md`
- Added `0019_benchmark_alerts.sql` for persistent benchmark drift alerts with fingerprint dedupe, severity/status state, and resolution timestamps.
- Added `GET /api/benchmarks/alerts` with optional forced scan (`scan=1`), severity filtering, and benchmark scoping for operator workflows.
- Added `GET /api/benchmarks/alerts/summary` to expose global open/resolved drift-alert aggregates for dashboard KPI cards.

### Changed
- `process-field-jobs` now supports runtime selection (`legacy`, `langgraph_shadow`, `langgraph_primary`) and checkpoint durability control (`exit`, `async`, `sync`) end-to-end from CLI -> pipeline -> benchmark/campaign runners.
- Field-job chunk processing now routes through LangGraph runtime when enabled while preserving existing repository write contracts for safe transition.
- Added `FieldJobSupervisorGraphRuntime` to orchestrate batch chunk preparation/dispatch/aggregation through LangGraph state transitions instead of ad-hoc threadpool control-flow.
- Benchmark and crawl-request runners now use more robust trailing-JSON parsing to reduce stdout-coupling fragility during long runs.
- Benchmark cycle timeout handling now uses workload-aware estimation (field-aware per-job cost + worker parallelism + fixed overhead) with a safe clamp for long-running website batches.
- Added `BENCHMARK_CYCLE_TIMEOUT_MS` env override for operators who need a fixed benchmark cycle timeout in constrained or high-latency environments.
- Benchmark gate baseline selection now requires protocol parity (workers, limit/cycle count, pause, warmup flag) before comparing treatment vs legacy.
- Benchmark gate report now includes an explicit `Comparison quality` check based on queue-start drift and full-cycle completion.
- Benchmark timeout estimation now applies a LangGraph runtime multiplier so `langgraph_shadow`/`langgraph_primary` cycles are less likely to fail from artificial clipping.
- Benchmark runner now computes and stores cycle-window shadow diff diagnostics (`observed_jobs`, decision/status mismatches, mismatch-rate, sample mismatches) whenever field jobs run in a `langgraph_*` runtime.
- Chapters map rendering now falls back to normalized chapter row state aggregation when map summary API data is empty, restoring visible state dots.
- Benchmarks detail view now includes explicit LangGraph cutover gate checks (throughput, retry-waste reduction, p95 latency, queue burn retention, and terminal-rate safety) against the latest matching legacy baseline.
- Benchmarks detail view now also surfaces shadow-diff artifact rows and a field-job graph run timeline with node-level events.
- Benchmarks detail view now includes drift-alert controls (severity/status filters, manual scan trigger, alert summaries, and resolved-by-scan timestamps) for faster rollout triage.
- Benchmarks snapshot now surfaces global drift-alert KPI cards (open critical/warning, resolved 24h) plus a one-click "Scan All Benchmarks" operator action.
- Health endpoint now runs scheduled benchmark drift scan checks and reports scan stats in runtime health payloads.

### Fixed
- Fixed frontend production verification path by removing stale Node/Next build-process interference and validating successful `next build` output.
- Fixed chapter map “all zeros/no dots” behavior when state summary responses are empty or stale.
- Fixed prior changelog gap by documenting the LangGraph field-job runtime migration work completed in this phase.
## [2.1.0] - 2026-04-01

### Added
- Added migrations:
  - `0014_adaptive_credit_assignment.sql`
  - `0015_adaptive_epoch_metrics.sql`
  - `0016_adaptive_policy_indexes.sql`
- Added adaptive replay/learning surfaces in crawler CLI:
  - `adaptive-train-loop`
  - `adaptive-replay-window`
  - `adaptive-policy-diff`
- Added delayed-credit reward plumbing with reward stages (`immediate`, `delayed`, `terminal`) and attribution metadata.
- Added adaptive epoch metric persistence (`crawl_epoch_metrics`) and policy diff/report repository helpers.
- Added web adaptive observability APIs:
  - `GET /api/adaptive/epochs`
  - `GET /api/adaptive/insights`
- Added benchmark dashboard learning-curve panel from adaptive epoch metrics.
- Added campaign adaptive-insights panel (action leaderboard, delayed-attribution table, guardrail hit-rate, valid-missing and verified-website counts).
- Added focused V2.1 crawler tests for delayed-credit rewards, live/train guardrail behavior, structural signature generalization, and conservative valid-missing normalization.

### Changed
- Adaptive runtime now supports split policy behavior in live vs train usage with safer default control (`live epsilon` vs `train epsilon`) and risk-aware penalties.
- Adaptive session observations now persist richer context (structural signatures, parent observation links, path depth, risk score, guardrail flags, context buckets).
- Normalization now supports conservative `valid_missing` handling so jobs are not requeued when evidence indicates true absence.
- `adaptive-train-eval` now performs train -> replay-update -> legacy/adaptive eval per epoch, computes richer KPI deltas, and stores per-epoch slope snapshots.
- Environment examples now include the full Agentic RL variable set for V2.1 tuning.
- Version bumped to `2.1.0` in root package, web app package, and crawler package metadata.

### Fixed
- Fixed dataclass field ordering regressions in adaptive models that could break crawler imports.
- Fixed adaptive graph helper gaps (missing context bucket, ancestor traversal, valid-missing counting, queue-efficiency terminal reward).
- Fixed adaptive over-scanning on high-yield sources by adding a high_yield_saturated early-stop path based on records discovered vs low-yield streak.
- Fixed eval-time enrichment churn during search/provider outages by adding adaptive eval preflight health gating and provider-degraded skip reporting.
- Fixed adaptive persistence contract mismatches between runtime payloads and repository insert columns.
## [2.0.1] - 2026-04-01

### Added
- Added adaptive policy snapshot resume support so new adaptive runs can hydrate prior action/reward state before executing.
- Added `adaptive-train-eval` CLI command to run repeated train/eval epochs and publish per-epoch KPI slope reports.
- Added benchmark runtime controls (`crawlRuntimeMode`, `runAdaptiveCrawlBeforeCycles`) so benchmark runs can execute adaptive warmup in `adaptive_assisted` mode by default.

### Changed
- Coarsened adaptive template signatures to improve template-memory reuse across structurally similar pages.
- Adaptive orchestration now stores both coarse and raw template signature context in telemetry metadata for easier diagnostics.
- Benchmark config normalization now defaults crawl runtime to `adaptive_assisted` and enables pre-cycle adaptive warmup when a source is provided.
- Version bumped to `2.0.1` in root package, web app package, and crawler package metadata.

## [2.0.0] - 2026-04-01

### Added
- Added `infra/supabase/migrations/0013_adaptive_crawl_runtime.sql` introducing adaptive crawl sessions, frontier storage, page observations, reward events, template profiles, and policy snapshots.
- Added a new adaptive crawler foundation with:
  - persistent crawl sessions
  - weighted-BFS frontier scoring
  - template-signature memory
  - contextual-bandit style policy decisions
  - adaptive observation export, replay summary, and policy report CLI commands
- Added explicit CLI runtime commands:
  - `run-legacy`
  - `run-adaptive`
  - `crawl-export-observations`
  - `crawl-replay-policy`
  - `crawl-policy-report`
- Added `services/crawler/src/fratfinder_crawler/tests/test_adaptive_runtime.py` covering adaptive frontier, template, policy, and stop-condition behavior.
- Added runtime-mode comparison panels to the Benchmarks and Campaigns dashboards (legacy vs adaptive scope cards + adaptive delta metrics).
- Added cohort benchmark artifacts:
  - `docs/reports/cohort_runtime_commands_2026-04-01.log`
  - `docs/reports/cohort_runtime_summary_2026-04-01.json`
  - `docs/reports/COHORT_RUNTIME_COMPARISON_2026-04-01.md`

### Changed
- `CrawlService` now supports dual-track runtime dispatch while preserving the existing crawl-run, chapter, provenance, review, and field-job contracts.
- Crawl runs now persist adaptive runtime metadata such as runtime mode, stop reason, and policy snapshot inside `crawl_runs.extraction_metadata`.
- The Runs and Overview web surfaces now expose adaptive runtime metadata so operators can compare legacy vs adaptive executions without leaving the dashboard.
- Benchmarks and Campaigns pages now hydrate crawl-run telemetry (`/api/runs`) to calculate runtime-mode deltas inside the operator flow.
- `.env.example` and `README.md` now document the adaptive runtime controls and inspection commands.

### Fixed
- Fixed the transition risk of an immediate crawl-core rewrite by implementing the adaptive runtime as a side-by-side path instead of mutating the legacy LangGraph in place.
## [0.10.9] - 2026-04-01

### Added
- Added `infra/supabase/migrations/0011_targeted_source_backfills_and_http_hardening.sql` and `0012_alpha_tau_omega_map_backfill.sql` to preserve the latest benchmark-response source recoveries in database state.
- Added new crawler regressions covering:
  - Chi Psi-style header-aware table extraction
  - Alpha Tau Omega Mapplic `data-mapdata` extraction
  - curated ATO source-hint preference over a generic official root URL
- Added `docs/reports/BENCHMARK_RESPONSE_ITERATION_2026-04-01.md` follow-up coverage documenting the live targeted validation tranche for Sigma Chi, KDR, DKE, Chi Psi, and ATO.

### Changed
- Discovery now treats a generic same-host root as weak when a curated deeper official chapter-directory path is available for that fraternity.
- The script-json adapter now parses inline Mapplic map payloads, allowing official chapter-map pages to become source-native chapter inventories.
- The HTTP client now uses a safer browser-like request posture by default, reducing avoidable 403s on official fraternity sites.
- The directory adapter now uses header-aware table extraction for live chapter tables that label columns with `ALPHA`, `SYMBOL`, `COLLEGE`, and similar variants.
- The locator adapter now splits combined KML chapter-school names into separate chapter and university fields.

### Fixed
- Fixed Sigma Chi chapter-roll extraction producing bogus navigation-derived stubs from the official chapters page.
- Fixed Delta Kappa Epsilon failing at the transport layer on its official chapters feed.
- Fixed Chi Psi using the wrong official source and misreading the live table column layout.
- Fixed Alpha Tau Omega remaining in `unsupported_or_unclear_source` despite having recoverable official chapter data embedded in the map page.

## [0.10.8] - 2026-04-01

### Added
- Added metadata-driven extraction overrides in the crawler so `sources.metadata.extractionHints` can now steer:
  - chapter index mode detection
  - extraction strategy selection
  - stub strategy ordering
  - directory selector overrides
- Added `infra/supabase/migrations/0010_benchmark_source_hint_backfills.sql` with conservative source-record backfills for the benchmark response pass.
- Added new crawler regressions covering:
  - metadata-forced extraction strategy selection
  - Bootstrap-style chapter card parsing and chapter/university title splitting

### Changed
- The directory adapter now understands Bootstrap-style card archives such as Kappa Delta Rho's chapter grid instead of relying only on `.chapter-card` and table patterns.
- Extraction orchestration now passes source metadata all the way through mode detection, strategy choice, and adapter execution, making source-specific parser hints operational instead of passive.
- Source-hint backfills now promote Sigma Chi toward the known chapter-directory URL and add KDR card-selector hints directly in migration state.

### Fixed
- Fixed source-native extraction blind spots where valid chapter-card layouts were present but missed by the generic directory parser.
- Fixed the gap between benchmark learnings and runtime behavior by making source metadata affect real extraction plans during crawl execution.
- Fixed the KDR-style combined title pattern (`Beta - Cornell University`) so chapter names and schools are separated cleanly during extraction.
## [0.10.7] - 2026-04-01

### Added
- Added `apps/web/src/lib/source-selection.ts` to score discovered source URLs, flag weak intake candidates, and upgrade discovery results toward stronger chapter-directory sources before request creation.
- Added `apps/web/src/lib/source-selection.test.ts` covering weak-source detection and post-discovery source upgrades for benchmark-problem fraternities.
- Added source/enrichment analytics to fraternity request progress so the Intake dashboard can now show:
  - source quality score and weak-source reasons
  - recovery attempts and recovered source URLs
  - adaptive enrichment workers/limits/cycle strategy
  - low-progress and degraded cycle counts
- Added new discovery regressions in `services/crawler/src/fratfinder_crawler/tests/test_discovery.py` for:
  - weak existing-source rejection
  - curated hint preference over noisy alumni search results
  - curated hint preference over same-host non-directory pages

### Changed
- Campaign request creation now re-runs discovery when a preferred source looks weak and upgrades to a materially better discovered source before launching the crawl request.
- Fraternity request execution now adapts enrichment workers, per-cycle limits, and max cycle budgets using live queue pressure and chapter volume instead of fixed campaign-era defaults.
- Zero-chapter crawl requests can now attempt a one-time source recovery by rediscovering and swapping to a better national source before failing the request.
- Intake request details now surface benchmark-response diagnostics directly in the website so operators can see whether a weak source or exhausted budget is driving poor results.
- Discovery host-hint matching is now domain-aware instead of substring-based, preventing false trust matches like `sandiegosigmachi.org` being treated as the canonical `sigmachi.org`.

### Fixed
- Fixed the benchmark-dominant Sigma Chi source-selection failure where both verified and existing sources pointed to a member/alumni portal and search could still fall into alumni or generic informational pages.
- Fixed discovery fallback leakage where weak existing configured sources could bypass the new verified-source validation logic and keep the crawler pinned to bad nationals URLs.
- Fixed source discovery for known curated fraternities so chapter-directory hints can override noisy same-host informational pages when those pages are not actually directory-like.
- Fixed long-run request execution to apply search preflight during enrichment cycles, so campaign-era degraded-provider protections are used by real crawl requests instead of only by standalone field-job runs.

## [0.10.6] - 2026-04-01

### Added
- Added `GET /api/health` for lightweight runtime visibility into campaign scheduling state during long operator sessions.
- Added campaign runtime attachment signals to campaign API responses and the Campaigns dashboard so operators can see when a DB-`running` campaign has lost its in-memory runner and needs reattachment.
- Added `docs/reports/CAMPAIGN_LIVE_RUN_2026-04-01.md` documenting the real 20-fraternity campaign, the live bottlenecks observed, and the fixes applied during execution.

### Changed
- Campaign APIs now auto-reattach detached `running` campaigns during normal dashboard polling, making long runs more resilient to Next.js dev reloads.
- Free-provider search order now prefers `serper_api` ahead of `tavily_api` after live campaign evidence showed Serper succeeding consistently while Tavily was consuming failed attempts.
- Campaign duration and throughput calculations now anchor to the first true `campaign_started` event, preventing resume/reattach actions from inflating jobs-per-minute analytics.

### Fixed
- Fixed long-running campaign detachment in local dev by reattaching active campaigns automatically instead of requiring manual operator resumes after every reload.
- Fixed enrichment-cycle timeout handling so productive long field-job passes can continue instead of hard-failing a fraternity request the moment a single cycle exceeds the old timeout ceiling.
- Fixed crawl-run timeout handling so source ingests that already discovered chapters or created field jobs can continue into enrichment instead of being discarded as total failures.
- Fixed runtime config drift between source defaults and local env by updating the live provider order setting to match the new measured fallback priority.
- Fixed the Chapters state map showing zero dots by using normalized dataset-wide state summaries instead of relying on the latest 500 loaded chapter rows to contain clean state codes.

## [0.10.5] - 2026-04-01

### Added
- Added `docs/plans/PRODUCTION_READINESS_PROGRAM_2026-04-01.md` capturing the full website-recovery + campaign-benchmark roadmap for future implementation guidance.
- Added a new campaign benchmark product surface:
  - database tables for `campaign_runs`, `campaign_run_items`, and `campaign_run_events`
  - campaign orchestration runner with resumable admission, checkpoints, and safe concurrency tuning hooks
  - API routes:
    - `GET/POST /api/campaign-runs`
    - `GET /api/campaign-runs/[id]`
    - `POST /api/campaign-runs/[id]/resume`
    - `POST /api/campaign-runs/[id]/cancel`
  - new `/campaigns` dashboard page with launch controls, live status, item scorecards, provider-health visibility, and event timeline
- Added `docs/reports/CAMPAIGN_FOUNDATION_VALIDATION_2026-04-01.md` documenting the smoke validations and the fixes discovered during rollout.
- Added baseline-aware campaign scorecards so control fraternities can measure delta from pre-existing coverage instead of inflating campaign success with historical data.

### Changed
- Local web development now starts through a guarded dev wrapper that clears stale `.next` artifacts before launching Next.js, reducing the route/static-asset corruption pattern that was causing dashboard 404s.
- Next.js dev webpack caching is disabled for local development to reduce stale route bundle lookups during reload cycles on Windows.
- Benchmarks now acknowledge active long-run campaigns and link operators toward the Campaigns workspace for broader validation work.
- Overview navigation and workspace structure now include the new Campaigns surface directly.

### Fixed
- Fixed the current website 404/runtime instability caused by stale `.next` dev artifacts and route-bundle resolution failures.
- Fixed campaign selection SQL and startup error handling so failed launches now persist an explicit failed state instead of stalling in `running`.
- Fixed campaign scorecards for control fraternities so pre-existing chapter coverage no longer appears as campaign-earned success.

## [0.10.4] - 2026-04-01

### Added
- Added chapter-operator controls directly to the Chapters dashboard:
  - row selection and filtered select-all
  - bulk rerun requests for `find_website`, `find_email`, and `find_instagram`
  - bulk delete for selected chapter records
  - single-chapter edit form for core text/contact fields
- Added new chapter operator API routes:
  - `POST /api/chapters/actions`
  - `PATCH /api/chapters/[id]`
- Added source provenance visibility to the chapters table via a new `sourceSlug` field in chapter list responses.

### Changed
- Chapters now use a more operator-focused workflow with a dedicated action panel and single-edit mode alongside the existing filters and map/table browsing views.
- Chapter repository queries now return latest source provenance for each chapter to support safer reruns and clearer operator context.
- Web database configuration loading is more resilient during Next.js reloads, preventing intermittent `DATABASE_URL is not set` failures in local development.

### Fixed
- Fixed the new chapter edit API SQL path so single-record saves no longer fail on invalid `UPDATE ... FROM` references.
- Fixed nullable contact-field updates by making Postgres parameter typing explicit for chapter edits.
- Fixed the Chapters page instability caused by one-shot env loading during hot reloads.
- Fixed the remaining `align-items: end` CSS warnings in the modernized dashboard styles.

## [0.10.3] - 2026-04-01

### Added
- Added a richer operator-console shell for the web dashboard with:
  - persistent workspace rail navigation
  - operator notes
  - top-level capability chips
- Added reusable progress meters for staged crawl requests and benchmark progress visualisation.
- Added benchmark cycle sparklines to make per-cycle output easier to compare visually.

### Changed
- Fraternity Intake now uses a more structured launch layout with a checklist, stage rail, and field-progress meters.
- Benchmarks now surface best-throughput/high-level performance context directly in the page hero instead of only in tabular detail.
- Internal dashboard navigation now uses simple anchor navigation instead of server-rendered `next/link` wrappers in the app shell and overview guide cards.

### Fixed
- Fixed the current website create-request flow after reproducing the failure path and restarting the dev server with the corrected environment configuration.
- Removed the most likely source of the `PathnameContext` render failure by hardening server-rendered navigation usage.

## [0.10.2] - 2026-04-01

### Added
- Added ambiguous-school website regressions covering:
  - rejection of generic tier-1 `.edu` directory leads for one-token school names
  - retention of stronger fraternity-specific school paths for those same ambiguous schools
- Added richer review-queue diagnostics in the web app:
  - triggering query
  - rejection summary histogram
  - candidate/source context for low-confidence review rows

### Changed
- Website enrichment now applies a stricter ambiguous-school rule for generic tier-1 school pages, preventing one-token school names such as `Denver` from surfacing weak campus-directory candidates as if they were viable chapter leads.
- Review queue entries now carry enough context to explain why a candidate reached review, including top rejection reasons from the search pass.

### Fixed
- Fixed a real Delta Chi ambiguous-school case where `denver-chapter-denver` could still surface a generic `msudenver.edu` organization directory as the active review outcome.

## [0.10.1] - 2026-04-01

### Added
- Added website-precision regressions covering:
  - nationals-directory chapter mismatch rejection
  - stricter Greek-letter fraternity identity matching
  - safer handling of ambiguous external website candidates
- Added `docs/reports/VALIDATION_REPORT_2026-04-01_PRECISION_HARDENING.md` documenting the live before/after validation pass for AGR, Delta Chi, and ADP control chapters.

### Changed
- Website enrichment now requires stronger school/chapter evidence before using nationals-directory entries for a target chapter.
- Fraternity identity matching is stricter for multi-token Greek organizations, reducing false positives such as `Alpha Gamma Rho` vs `Sigma Gamma Rho`.
- Generic chapter slug tokens such as `chapter`, `colony`, `active`, and `provisional` no longer count as chapter identity evidence in website matching.
- Short fraternity aliases/initialisms are handled more conservatively so they do not match arbitrary URL substrings.

### Fixed
- Fixed unsafe website auto-writes from over-broad nationals-directory matches, including the Delta Chi Canada misassignment pattern.
- Fixed website false positives caused by partial Greek-token overlap across different organizations.
- Fixed chapter matching leakage from generic slug tokens that could inflate relevance scoring on wrong school pages.

## [0.10.0] - 2026-03-31

### Added
- Added `0008_verified_sources.sql` with a dedicated `verified_sources` registry table (health/provenance metadata, active flag, timestamps, indexes, updated-at trigger).
- Added manual operator CLI commands for registry lifecycle:
  - `bootstrap-nic-sources --input <json> [--dry-run]`
  - `revalidate-verified-source --fraternity-slug <slug>`
  - `revalidate-verified-sources --limit <n>`
- Added discovery provenance metadata end-to-end:
  - `sourceProvenance`
  - `fallbackReason`
  - `resolutionTrace`
- Added orchestration navigation stages before extraction:
  - `detect_chapter_index_mode`
  - `extract_chapter_stubs`
  - `follow_chapter_detail_or_outbound` (bounded)
  - `extract_contacts_from_chapter_site`
- Added shared chapter-link scoring utility and adapter stub contract support (`parse_stubs`) across `directory_v1`, `script_json`, and `locator_api`.
- Added crawler tests for registry-first discovery and navigation/stub/contact extraction behaviors.
- Added no-cost search provider support:
  - `searxng_json`
  - `tavily_api`
  - `serper_api`
  - `auto_free` provider routing mode.
- Added provider-order and provider-specific pacing settings for free-provider orchestration.
- Added search preflight provider-health snapshots (`provider_health`) with per-provider attempt/success metrics.

### Changed
- Discovery resolution is now registry-first: `verified_sources` -> existing configured sources -> search fallback.
- Discovery now applies stronger identity normalization (including alias handling such as `fiji -> phi-gamma-delta`) and deterministic conflict resolution when registry and existing sources disagree.
- Fraternity Intake now surfaces discovery provenance and decision trace in request details for operator auditability.
- Crawl finalization metadata now includes navigation mode, stub counts, and navigation stats.
- Bing-first free fallback routing now uses a provider-chain execution path that can continue across provider failures without aborting on the first failed query.
- Field-job search fanout no longer aborts after the first provider-unavailable query; jobs now exhaust bounded query budgets before classifying provider outage outcomes.

## [0.9.1] - 2026-03-27

### Added
- Added Fraternity Intake `Discovery Review` UI controls so operators can inspect ranked source candidates, choose a candidate URL, or paste a manual source override before confirm.
- Added request detail metadata cards for `Chapters Discovered` and `Field Jobs Created` so intake triage can quickly distinguish valid crawls from zero-yield runs.
- Added curated source hint support in source discovery for high-ambiguity fraternity names (initially `Phi Gamma Delta` -> `https://phigam.org/about/overview/our-chapters/`).
- Added discovery tests for travel/PHI noise rejection, host-hint query generation, and curated source fallback behavior.

### Changed
- Intake confirm now uses a safer source resolution chain: explicit override -> stored source URL -> discovered candidate URL.
- Intake create flow now auto-queues requests for both `high` and `medium` confidence discoveries (when a source URL exists), reducing unnecessary manual confirmations.
- Intake runner now fails fast when crawl ingest discovers zero chapters instead of allowing a false `succeeded` request.
- Embedded-data detection and locator extraction now support Google My Maps KML-based chapter directories, improving chapter ingestion from map-backed national directory pages.

### Fixed
- Fixed a legacy false-success intake outcome where requests could complete with `recordsSeen = 0` and `fieldJobsCreated = 0`.
- Fixed a Fraternity Intake runtime error on confidence rendering by normalizing confidence formatting in the UI.

## [0.9.0] - 2026-03-27

### Added
- Added a new Fraternity Intake workflow with a dedicated dashboard tab (`/fraternity-intake`) for scheduling staged crawl requests from a fraternity name.
- Added `fraternity_crawl_requests` and `fraternity_crawl_request_events` tables, plus `field_jobs.priority` support, via `0007_fraternity_crawl_requests.sql`.
- Added crawler CLI source discovery command: `python -m fratfinder_crawler.cli discover-source --fraternity-name "<name>"`.
- Added web APIs for intake lifecycle management: list/create requests, request detail/confirm/cancel/reschedule, and expedite.
- Added an in-app staged request runner that executes crawl + bounded enrichment, records timeline events, and updates live progress snapshots.

### Changed
- Field-job claiming now prioritizes high-priority queued jobs (`priority DESC`) before scheduled time ordering, enabling source-level expedite behavior.
- Request execution now reconciles stale running states and supports scheduled start times with immediate expedite override.

## [0.8.0] - 2026-03-23

### Added
- Added a simple U.S. state-tile chapter map to the chapters dashboard, with one marker per loaded chapter and live counts that respond to the current table filters.
- Added per-column chapter filters for name, fraternity, university, state, status, website, Instagram, and email directly in the chapters overview table.
- Added a Bing-only operating profile to the crawler settings and runbook, including a configurable negative-result cooldown for search-backed enrichment.

### Changed
- The chapters page now loads up to 500 rows for the operator view instead of truncating at 200, which fixes silent omission of loaded chapter data in the dashboard.
- Refactored the chapters dashboard into a small client component so filtering stays instant without moving crawl or database logic into the frontend.
- Bing-backed field jobs now behave more conservatively: website searches cool down for 30 days after a clean miss, email and Instagram searches wait for a confident website first, and medium-confidence Bing search matches are routed to review instead of being written directly.
- Bing-only website discovery now runs school-domain-first (`site:.edu` and optional known campus domains), treats generic web search as fallback, blocks Sigma chemistry domains outright, and caps low-signal website jobs at one retry before terminal failure.
- Instagram enrichment now uses a dedicated search strategy: it searches broad Instagram/web results before provenance fallback, supports school-initial and handle-shape queries like `fsusigmachi` / `wcsu_sigma_chi`, and no longer waits on website discovery before attempting a chapter Instagram match.
- Instagram-only batches can now be targeted directly from the CLI, and Instagram search skips low-signal result hosts plus search-page fetches so focused runs spend more time on likely profile hits and less time on junk results.
- Instagram hardening now uses a bounded query funnel with configurable Instagram-specific caps, keeps only strong school-initial handle searches, checks trusted chapter websites before broad search, and rejects weak generic Instagram candidates before they can write or enter review.
- Instagram matching now avoids Greek-letter chapter-name search terms, rejects wrong-organization results like `Tri Sigma UVA`, treats matching chapter designations like `Theta Chapter` / `Omicron Omicron Chapter` as strong evidence, and can mark chapters inactive when official school-affiliation pages exclude the fraternity.
- Field-job processing now supports concurrent workers, with an explicit worker cap in settings/CLI so large source batches can scale out to multiple `SKIP LOCKED` workers safely.
- Queue operations now support field-type targeting end-to-end, which lets us run Instagram-only throughput batches without spending slots on website/email jobs.
- Field-job processing can now target a single job type from the CLI, and Instagram search skips low-signal result hosts plus unnecessary page fetches for direct Instagram hits so focused batches run faster and waste fewer searches.
- Search reliability now includes a provider circuit breaker (configurable failure threshold/cooldown) plus optional empty-result cache control so workers avoid stale-miss amplification and fail fast during provider/network outages.
- Search enrichment now emits structured candidate-rejection summaries per no-candidate job, adds a minimum no-candidate backoff guard to prevent zero-cooldown hot loops, broadens Instagram query formats (while preserving handle-based wins), and strengthens trusted school/IFC directory website extraction for safer higher-yield candidate acceptance.

## [0.7.0] - 2026-03-22

### Added
- Added a new crawler `search/` package with a provider abstraction, a DuckDuckGo HTML client for local development, and optional Brave API support for search-backed enrichment.
- Added crawler tests covering search-client parsing plus search-driven website, email, and Instagram enrichment behavior.

### Changed
- Field-job enrichment now falls back to bounded public web search when provenance and chapter-page evidence do not contain chapter website, email, or Instagram data.
- Search-driven field jobs now preserve provenance for discovered values, use chapter/fraternity/school-aware query generation, and keep low-confidence matches in review instead of writing them directly.
- Search-backed enrichment can be configured through `CRAWLER_SEARCH_*` environment variables documented in the README and `.env.example`.
- The local search default is now `bing_html`, and DuckDuckGo HTML now falls back to Bing on anomaly pages and request-level failures instead of stalling jobs on repeated timeout/requeue loops.
- Search enrichment now applies stricter fraternity/school/chapter relevance checks before fetching or writing search-derived candidates, which keeps low-quality Bing/Reddit/Stack Overflow matches from polluting chapter contact fields.
- Website enrichment now follows relevant university directory pages and prefers linked chapter sites over directory listing URLs when both appear credible.
- Search query generation now de-emphasizes generic Greek-letter chapter names and adds school/domain-focused variants so fraternity web search is less likely to be poisoned by unrelated `sigma` slang results.
- Search provider selection now supports an `auto` mode that prefers Brave Search API when a key is configured and otherwise falls back to Bing HTML, keeping local enrichment runnable while improving production search quality.

## [0.6.0] - 2026-03-22

### Added
- Added `0005_crawl_run_intelligence.sql` so crawl runs can store page analysis, classification, and extraction metadata in Postgres.
- Added dashboard rendering for strategy badges, chapter field-state labels, and review extraction notes.
- Added crawler test coverage proving crawl-run metadata persists the selected extraction strategy and classification payload.

### Changed
- Crawl finalization now persists page-level intelligence metadata, including `strategy_used`, `page_level_confidence`, and `llm_calls_used`.
- Web API responses for runs, chapters, and review items now expose strategy metadata, chapter `fieldStates`, and review `extractionNotes`.
- Shared contracts now accept chapter `fieldStates` and review-item `extractionNotes`, keeping crawler and dashboard schemas aligned.
- Local Docker Postgres now defaults to port `5433` in the example configuration to avoid silently colliding with an existing host Postgres on `5432`.
- Adaptive source analysis now treats explicit single chapter-card pages as valid directory inputs, which keeps one-record local demo crawls from being routed to review by mistake.
- Field-job enrichment now deobfuscates emails, scans chapter website HTML for `mailto:` and Instagram links, and prioritizes website discovery before downstream contact/social jobs.
- `find_website` no longer falls back to the fraternity base URL, preventing bad chapter website writes when a source lacks chapter-specific contact evidence.
## [0.5.0] - 2026-03-22

### Added
- Added the Phase 2-D verification job types `verify_website` and `verify_school_match` to the crawler field-job model and engine.
- Added `0004_chapter_field_states.sql` so chapter records can persist field-level confidence states in Postgres.
- Expanded crawler tests to cover confidence-aware job queueing, website verification success/retry/terminal-failure paths, and review routing for clear school mismatches.

### Changed
- Normalization now queues `find_*` jobs only for fields that are truly missing and queues `verify_website` when a website is present at low confidence.
- Chapter upserts now persist `instagram_url`, `contact_email`, and `field_states`, and completed field jobs update the resolved field state in the chapter row.
- Field job processing now marks verified fields as `found`, avoids overwriting already-populated chapter values, and keeps failed verification attempts from mutating chapter records.

## [0.4.0] - 2026-03-22

### Added
- Introduced the bounded Phase 2-C LLM integration under `services/crawler/src/fratfinder_crawler/llm/`:
  - `client.py` for settings-gated OpenAI access with immediate failure when LLM is disabled or the API key is missing.
  - `classifier.py` for mocked, schema-validated LLM source classification.
  - `extractor.py` for mocked, schema-validated structured chapter extraction using JSON Schema responses.
- Added mocked crawler tests covering extractor success, extractor validation failure, LLM client safety guards, call-budget enforcement, and low-confidence chapter persistence behavior.

### Changed
- Extended crawler settings and graph state with LLM model, token budget, max-call budget, API key support, and `llm_calls_used` tracking.
- Updated the crawl graph so heuristic classification can fall back to the LLM only within budget, never when embedded data is present, and LLM extraction routes invalid or low-confidence output into review instead of unsafe writes.
- Normalization now marks medium-confidence extracted fields as `low_confidence` and preserves optional Instagram/email values when present in the extracted record.

## [0.3.0] - 2026-03-22

### Added
- Implemented the Phase 2-B adaptive adapter families:
  - `adapters/script_json.py` for JSON-LD and inline `window.chapters`-style extraction.
  - `adapters/locator_api.py` for API-backed locator extraction through the shared HTTP client.
- Added fixture-backed crawler tests covering script-embedded extraction, JSON-LD extraction, mocked locator API extraction, and review routing when either adaptive family returns no usable records.
- Added `0003_adaptive_source_types.sql` so the database accepts `script_embedded` and `locator_api` source types for Phase 2 crawling.

### Changed
- Updated the adapter protocol and extraction graph so strategy-family adapters can receive `api_url` hints and the shared HTTP client while preserving the existing `directory_v1` behavior unchanged.
- Replaced the Phase 2-A placeholder registry entries for `script_json` and `locator_api` with real adapters.

## [0.2.0] - 2026-03-22

### Added
- Introduced the Phase 2 analysis foundation for adaptive crawling:
  - `analysis/page_analyzer.py` for deterministic DOM summaries.
  - `analysis/source_classifier.py` for heuristic page-type classification.
  - `analysis/embedded_data_detector.py` for JSON-LD, inline JSON, and API-hint discovery.
  - `analysis/strategy_selector.py` for extraction-plan routing without LLM usage.
- Expanded the crawler graph to the 11-node Phase 2-A flow with typed state for page analysis, classification, embedded data, extraction planning, and strategy attempts.
- Added Phase 2-A fixture coverage for static directory classification, JSON-LD detection, unknown-page review routing, and full graph execution against the existing sample directory fixture.

### Changed
- Redesigned the adapter registry around strategy families (`repeated_block`, `table`, `script_json`, `locator_api`) while keeping `directory_v1` behavior unchanged for known directory sources.
- Extended crawler models with extraction confidence, field states, and analysis dataclasses needed for adaptive routing.
- Moved field-job creation out of record persistence into a dedicated `spawn_followup_jobs` graph node.
- Normalization now records field-state metadata while preserving Phase 1 missing-field job behavior for existing sources.
- Added `CRAWLER_LLM_ENABLED` to settings with a default of `false`; strategy selection does not route to LLM when disabled.

## [0.1.0] - 2026-03-22

### Added
- Bootstrapped a production-oriented monorepo structure:
  - `apps/web` for the Next.js operator dashboard.
  - `services/crawler` for the Python ingestion pipeline.
  - `packages/contracts` for shared runtime contracts and typed payloads.
  - `infra` for Docker, migrations, seeds, and database smoke validation.
- Added environment and repository guardrails:
  - root `.env.example` with all required configuration variables.
  - strict `.gitignore` coverage for secret-bearing `.env` files.
  - root workspace scripts for linting, typing, testing, DB lifecycle, migrations, and seeds.
- Implemented canonical relational schema and seed data for:
  - fraternities, sources, chapters, chapter_provenance, crawl_runs, review_items, field_jobs.
- Implemented crawler architecture with:
  - deterministic adapter registry.
  - strict normalization path.
  - repository layer for DB writes/reads.
  - LangGraph orchestration nodes that coordinate but do not parse HTML.
  - retry-aware HTTP session client and failure routing.
- Implemented operator dashboard backend and frontend:
  - server routes for chapters, crawl runs, review items, and field jobs.
  - dashboard pages for operational inspection and triage queues.
- Added tests and fixtures:
  - contract validation tests.
  - crawler adapter and normalization tests.
  - SQL smoke test file for schema sanity checks.
- Added local-operations hardening for the next milestone:
  - structured crawler event logging with run/job context.
  - field-job engine for `find_website`, `find_instagram`, and `find_email`.
  - health/readiness endpoints and CLI probes.
  - consistent API envelope helpers for success/error responses.
  - integration flow test covering crawl-to-dashboard visibility when local Postgres is available.

### Changed
- Review workflow server logic now enforces valid transitions in application code and records operator audit entries.
- Dashboard pages now display enriched chapter contact fields, review audit context, and field-job worker/error details.
- README expanded with local workflow commands for field-job processing, health checks, and integration testing.
- Converted repository from planning-only state to executable project scaffold with clear runbooks and stage-aligned deliverables.

### Fixed
- Corrected the seeded Sigma Chi source path to the live undergraduate groups directory and hardened the `directory_v1` table adapter to skip header rows and parse split city/state columns correctly.
- Fixed field-job transaction persistence for local processing and added source-scoped field-job execution so integration checks and local demos can process only the intended job queue.

















