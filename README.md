# Frat Finder AI

Frat Finder AI is a source-aware chapter discovery and data platform for NIC fraternities.

It ingests chapter data from national fraternity websites, normalizes records into a canonical relational model, preserves provenance, tracks crawl runs/failures, creates review items for ambiguous records, queues follow-up jobs for missing fields, and exposes everything in a local operator dashboard.

## Architecture

- `apps/web`: Next.js TypeScript operator dashboard and server routes.
- `services/crawler`: Python ingestion service with adapter-based parsing and LangGraph orchestration.
- `packages/contracts`: shared schemas/types for payload and storage contracts.
- `infra`: Docker, SQL migrations, seeds, and DB smoke tests.

Separation rules:

- No crawling/parsig/db business logic in React components.
- No low-level HTML parsing inside LangGraph nodes.
- No scattered raw SQL writes; use repository modules.

## Repository Layout

```text
apps/
  web/
docs/
  plans/
  reports/
artifacts/
services/
  crawler/
packages/
  contracts/
infra/
  docker/
  supabase/
tests/
  integration/
```

## Prerequisites

- Docker + Docker Compose
- Node.js 20+
- pnpm 9+
- Python 3.11+

## Local Setup

1. Copy env template:

```bash
copy .env.example .env
```

2. Start local database stack:

```bash
pnpm db:up
```

Docker Desktop must be running before `pnpm db:up` will succeed.

3. Apply migrations and seed data:

```bash
pnpm db:migrate
pnpm db:seed
pnpm db:smoke
```

4. Install web dependencies:

```bash
pnpm install
```

5. Install crawler dependencies:

```bash
python -m pip install -e "services/crawler[dev]"
```

## Run The Web App

```bash
pnpm dev:web
```

Open `http://localhost:3000`.

## Run The Crawler

Run a crawl for all active sources:

```bash
python -m fratfinder_crawler.cli run
```

Run a crawl for one source slug:

```bash
python -m fratfinder_crawler.cli run --source-slug beta-theta-pi-main
```

Process missing-field jobs locally:

```bash
python -m fratfinder_crawler.cli process-field-jobs --limit 25
```

Process missing-field jobs for one source only:

```bash
python -m fratfinder_crawler.cli process-field-jobs --source-slug sigma-chi-main --field-name find_instagram --workers 8 --limit 25
```

Probe search-provider health before launching a large batch:

```bash
python -m fratfinder_crawler.cli search-preflight --probes 4
python -m fratfinder_crawler.cli process-field-jobs --field-name find_website --workers 8 --limit 200 --run-preflight --require-healthy-search
```

Discover a likely national source URL for a fraternity name:

```bash
python -m fratfinder_crawler.cli discover-source --fraternity-name "Lambda Chi Alpha"
```

Bootstrap verified registry seeds from manual research:

```bash
python -m fratfinder_crawler.cli bootstrap-nic-sources --input research_nav_21.json
```

Revalidate one verified seed (manual/operator-driven):

```bash
python -m fratfinder_crawler.cli revalidate-verified-source --fraternity-slug sigma-chi
```

Run the new dual-track crawl runtimes explicitly:

```bash
python -m fratfinder_crawler.cli run-legacy --source-slug sigma-chi-main
python -m fratfinder_crawler.cli run-adaptive --source-slug sigma-chi-main --runtime-mode adaptive_shadow --policy-mode live
python -m fratfinder_crawler.cli run-adaptive --source-slug sigma-chi-main --runtime-mode adaptive_assisted --policy-mode train
```

Inspect adaptive crawl telemetry and policy summaries:

```bash
python -m fratfinder_crawler.cli crawl-export-observations --source-slug sigma-chi-main --runtime-mode adaptive_assisted --window-days 14 --limit 50
python -m fratfinder_crawler.cli crawl-replay-policy --source-slug sigma-chi-main --runtime-mode adaptive_assisted --window-days 14 --limit 200
python -m fratfinder_crawler.cli crawl-policy-report --limit 25
python -m fratfinder_crawler.cli adaptive-replay-window --source-slugs "sigma-chi-main,chi-psi-main" --runtime-mode adaptive_assisted --window-days 14 --limit 300
python -m fratfinder_crawler.cli adaptive-train-eval --epochs 3 --runtime-mode adaptive_assisted --cohort-label target-cohort --train-sources "sigma-chi-main,chi-psi-main" --eval-sources "kappa-delta-rho-main,delta-kappa-epsilon-main"
python -m fratfinder_crawler.cli adaptive-train-loop --rounds 2 --epochs-per-round 3 --runtime-mode adaptive_assisted --train-sources "sigma-chi-main,chi-psi-main" --eval-sources "alpha-tau-omega-main,delta-sigma-phi-main" --report-dir docs/reports
python -m fratfinder_crawler.cli adaptive-policy-diff --snapshot-a 101 --snapshot-b 122
```

Agentic RL tuning env vars (V2.1):

- `Agent:ADAPTIVE_LIVE_EPSILON` (default `0.02`)
- `Agent:ADAPTIVE_TRAIN_EPSILON` (default `0.12`)
- `Agent:ADAPTIVE_REWARD_GAMMA` (default `0.85`)
- `Agent:ADAPTIVE_TRACE_HOPS` (default `4`)
- `Agent:ADAPTIVE_REPLAY_WINDOW_DAYS` (default `7`)
- `Agent:ADAPTIVE_REPLAY_BATCH_SIZE` (default `500`)
- `Agent:ADAPTIVE_RISK_TIMEOUT_WEIGHT` (default `0.75`)
- `Agent:ADAPTIVE_RISK_REQUEUE_WEIGHT` (default `0.35`)
- `Agent:ADAPTIVE_BALANCED_KPI_WEIGHTS` (default `{"coverage":0.45,"throughput":0.2,"queue":0.2,"reliability":0.15}`)

Revalidate the newest N verified seeds:

```bash
python -m fratfinder_crawler.cli revalidate-verified-sources --limit 20
```

### Search-Backed Enrichment

When chapter website, email, or Instagram data is not present on the national source page, field jobs can now fall back to public web search.

Relevant env settings:

- `CRAWLER_SEARCH_ENABLED=true` enables search-backed enrichment for missing fields.
- `CRAWLER_SEARCH_PROVIDER=auto` is now the recommended default: it starts with SearXNG (`searxng_json`) when configured, then uses the free-provider order (`tavily_api -> serper_api -> duckduckgo_html -> bing_html -> brave_html`), and inserts `brave_api` early when a Brave API key is present.
- `CRAWLER_SEARCH_PROVIDER=auto_free` remains available for a strict free-only chain (`searxng_json -> tavily_api -> serper_api -> duckduckgo_html -> bing_html -> brave_html`) and skips providers that are not configured.
- `CRAWLER_SEARCH_PROVIDER=bing_html` remains available for explicit local-only testing when you want to bypass Brave.
- `CRAWLER_SEARCH_PROVIDER_ORDER_FREE` overrides the `auto_free` chain when you need custom ordering.
- `CRAWLER_SEARCH_SEARXNG_BASE_URL` points to a SearXNG JSON endpoint (for example `http://localhost:8888`).
- `CRAWLER_SEARCH_SEARXNG_ENGINES` optionally pins SearXNG engines per query.
- `CRAWLER_SEARCH_TAVILY_API_KEY` and `CRAWLER_SEARCH_SERPER_API_KEY` enable free-tier API fallbacks behind SearXNG.
- `CRAWLER_SEARCH_MIN_REQUEST_INTERVAL_MS` enables lightweight per-worker pacing between search requests (set this above `0` when providers start challenging high-frequency traffic).
- `CRAWLER_SEARCH_PROVIDER_PACING_MS_*` applies provider-specific pacing overrides (`SEARXNG_JSON`, `TAVILY_API`, `SERPER_API`, `DUCKDUCKGO_HTML`, `BING_HTML`, `BRAVE_HTML`) without changing global pacing.
- `CRAWLER_SEARCH_NEGATIVE_COOLDOWN_DAYS` controls how long Bing-backed jobs cool down after a clean no-result pass so hopeless chapters do not get reprocessed every day.
- `CRAWLER_SEARCH_DEPENDENCY_WAIT_SECONDS` controls backoff for dependency-blocked jobs (for example, Bing email jobs waiting on confident website discovery) without consuming retry budget.
- `CRAWLER_SEARCH_TRANSIENT_SHORT_RETRIES` and `CRAWLER_SEARCH_TRANSIENT_LONG_COOLDOWN_SECONDS` split transient provider failure handling into short retries followed by long cooldowns, preventing queue hot-loop churn.
- `CRAWLER_SEARCH_PREFLIGHT_ENABLED`, `CRAWLER_SEARCH_PREFLIGHT_PROBE_COUNT`, and `CRAWLER_SEARCH_PREFLIGHT_MIN_SUCCESS_RATE` gate batches on provider health before large runs.
- `CRAWLER_SEARCH_DEGRADED_WORKER_CAP`, `CRAWLER_SEARCH_DEGRADED_MAX_RESULTS`, `CRAWLER_SEARCH_DEGRADED_MAX_PAGES_PER_JOB`, `CRAWLER_SEARCH_DEGRADED_EMAIL_MAX_QUERIES`, `CRAWLER_SEARCH_DEGRADED_INSTAGRAM_MAX_QUERIES`, and `CRAWLER_SEARCH_DEGRADED_DEPENDENCY_WAIT_SECONDS` tune degraded-mode behavior when preflight detects weak provider availability.
- `CRAWLER_SEARCH_MIN_NO_CANDIDATE_BACKOFF_SECONDS` enforces a minimum retry delay for no-candidate outcomes so zero-day cooldown settings do not hot-loop the same jobs in one batch.
- `CRAWLER_FIELD_JOB_MAX_WORKERS` controls how many field-job workers can run concurrently in one batch.
- `CRAWLER_SEARCH_EMAIL_MAX_QUERIES` caps the email query funnel so contact-email jobs stay bounded and do not fan out unnecessarily.
- `CRAWLER_SEARCH_REQUIRE_CONFIDENT_WEBSITE_FOR_EMAIL` keeps website-first email safety on by default, while `CRAWLER_SEARCH_EMAIL_ESCAPE_ON_PROVIDER_BLOCK` and `CRAWLER_SEARCH_EMAIL_ESCAPE_MIN_WEBSITE_FAILURES` let email jobs proceed from trusted non-website evidence after repeated provider-blocked website failures.
- `CRAWLER_SEARCH_INSTAGRAM_MAX_QUERIES` caps the Instagram query funnel so Bing-backed Instagram jobs stay bounded by default.
- `CRAWLER_SEARCH_ENABLE_SCHOOL_INITIALS`, `CRAWLER_SEARCH_MIN_SCHOOL_INITIAL_LENGTH`, `CRAWLER_SEARCH_ENABLE_COMPACT_FRATERNITY`, and `CRAWLER_SEARCH_INSTAGRAM_ENABLE_HANDLE_QUERIES` tune the Instagram-specific handle/query strategy without changing the broader search stack.
- `CRAWLER_SEARCH_INSTAGRAM_DIRECT_PROBE_ENABLED` is an experimental fallback for direct Instagram handle probing when search providers are unavailable; keep this `false` unless you are actively validating probe quality.
- `CRAWLER_DISCOVERY_VERIFIED_MIN_CONFIDENCE` is the confidence floor for using `verified_sources` before falling back to existing source rows or search.
- `CRAWLER_NAV_MAX_HOPS_PER_STUB` and `CRAWLER_NAV_MAX_PAGES_PER_RUN` bound chapter-stub navigation so directory expansion stays fast and deterministic.
- `GREEDY_COLLECT` controls nationals-site opportunistic ingestion: `none` keeps target-only enrichment, `passive` collects nearby chapter directory evidence with low crawl fanout, and `bfs` performs deeper same-domain traversal on nationals sites to ingest additional chapter contact signals.
- `CRAWLER_SEARCH_PROVIDER=duckduckgo_html` remains available and now auto-falls back to Bing when DuckDuckGo returns anomaly pages or transport-level failures.
- `CRAWLER_SEARCH_PROVIDER=brave_api` can be used explicitly if you want Brave only; when Brave errors, the crawler falls back to Bing instead of stalling jobs.
- `CRAWLER_SEARCH_MAX_RESULTS` and `CRAWLER_SEARCH_MAX_PAGES_PER_JOB` bound how aggressively each job searches and fetches candidate pages.
- `CRAWLER_SEARCH_CACHE_EMPTY_RESULTS=false` avoids reusing zero-result search responses within the same worker process, which helps prevent stale-miss amplification on transient query/provider noise.
- `CRAWLER_SEARCH_CIRCUIT_BREAKER_FAILURES` and `CRAWLER_SEARCH_CIRCUIT_BREAKER_COOLDOWN_SECONDS` open a short provider circuit after repeated transport/provider failures so workers fail fast instead of spending full timeout windows on every query.

The crawler only writes high-confidence matches directly. Lower-confidence search candidates are routed into review instead of silently mutating chapter records.

For a cost-first Bing-only run, use a conservative profile:

- `CRAWLER_SEARCH_PROVIDER=bing_html`
- `CRAWLER_SEARCH_MAX_RESULTS=3`
- `CRAWLER_SEARCH_MAX_PAGES_PER_JOB=1`
- `CRAWLER_SEARCH_EMAIL_MAX_QUERIES=5`
- `CRAWLER_SEARCH_CACHE_EMPTY_RESULTS=false`
- `CRAWLER_SEARCH_NEGATIVE_COOLDOWN_DAYS=30`
- `CRAWLER_SEARCH_DEPENDENCY_WAIT_SECONDS=300`
- `CRAWLER_SEARCH_MIN_NO_CANDIDATE_BACKOFF_SECONDS=60`
- `CRAWLER_SEARCH_MIN_REQUEST_INTERVAL_MS=250`
- `CRAWLER_SEARCH_PREFLIGHT_ENABLED=true`
- `CRAWLER_SEARCH_PREFLIGHT_MIN_SUCCESS_RATE=0.34`

For a no-cost recovery profile with local SearXNG + free API fallbacks:

- `CRAWLER_SEARCH_PROVIDER=auto_free`
- `CRAWLER_SEARCH_SEARXNG_BASE_URL=http://localhost:8888`
- `CRAWLER_SEARCH_TAVILY_API_KEY=<optional>`
- `CRAWLER_SEARCH_SERPER_API_KEY=<optional>`
- `CRAWLER_SEARCH_MIN_REQUEST_INTERVAL_MS=200`
- `CRAWLER_SEARCH_PREFLIGHT_ENABLED=true`

In Bing-only mode, the crawler now:

- searches school-owned domains first for `find_website` jobs using `site:.edu` and any known `campusDomains` carried in the job payload
- treats generic web search as fallback only when the campus-domain pass yields no usable website candidate
- waits for a confident chapter website before running Bing-backed email searches, which keeps request volume lower and reduces false positives
- prioritizes chapter website + same-domain contact/officer pages for `find_email` before broad web search, improving precision and lowering query load
- lets Instagram jobs run independently with an Instagram-specific funnel that starts with `site:instagram.com`, searches using fraternity name plus school, preserves high-yield handle-shape searches like `fsusigmachi`, and cuts weak short school aliases by default
- uses stricter Instagram relevance gates so wrong organizations like `Tri Sigma` / weak generic handles are rejected instead of written or reviewed automatically
- falls back to trusted school/IFC affiliation pages when Instagram discovery fails; if an official campus chapter list excludes the fraternity, the crawler marks that chapter inactive instead of requeueing forever
- auto-writes only tier-1 website evidence from campus `.edu` pages or known fraternity domains; lower-trust aggregator/profile candidates are routed to review
- caps low-signal Bing website jobs at one retry so the queue does not churn on hopeless chapters

Check crawler probes:

```bash
python -m fratfinder_crawler.cli health --probe liveness
python -m fratfinder_crawler.cli health --probe readiness
```

## Run Tests

All tests:

```bash
pnpm test
```

Crawler tests only:

```bash
pytest services/crawler/src/fratfinder_crawler/tests
```

Contracts tests only:

```bash
pnpm test:contracts
```

Integration flow test:

```bash
pnpm test:integration
```

This test requires local Postgres to be reachable and skips itself when the database is not running.

## Inspect Database And Dashboard

- Adminer (DB UI): `http://localhost:8080`
  - System: `PostgreSQL`
  - Server: `postgres`
  - Username/password/database from `.env`
- Dashboard:
  - Overview: `/`
  - Chapters: `/chapters`
  - Crawl Runs: `/runs`
  - Review Queue: `/review-items`
  - Benchmarks: `/benchmarks`
- Fraternity Intake: `/fraternity-intake`
  - Health: `/api/health`, `/api/health/liveness`, `/api/health/readiness`

### Dashboard Signals

- Crawl runs now surface extraction strategy, page-level confidence, and LLM calls used.
- Chapter rows now include field-state labels so operators can distinguish `found`, `missing`, and `low_confidence` values.
- Review items now surface extraction notes when the crawler or LLM records them in the review payload.

### Fraternity Intake Workflow (Current)

The intake runner is a staged workflow that creates a request from a fraternity name, discovers a likely national source, and then runs crawl + enrichment.

High-level behavior:

1. Create request (`/fraternity-intake` form)
- Backend calls `discover-source` and stores ranked candidates in `progress.discovery.candidates`.
- Discovery now resolves in deterministic order: `verified_sources` registry -> existing configured source -> search fallback.
- Request progress stores `sourceProvenance`, `fallbackReason`, and `resolutionTrace` so operators can audit selection decisions.
- If discovery confidence is `high` or `medium` and a source URL is present, the request is queued automatically.
- If discovery confidence is `low`, request stays in `draft/awaiting_confirmation`.

2. Confirm or override source
- Request details now include a `Discovery Review` section with:
  - ranked discovery candidates (score, provider, URL, title)
  - `Use URL` action to populate `Source URL Override`
  - manual URL override input for operator-provided source
- Confirm sends the selected/override URL.
- If no override is provided, confirm falls back to the stored discovered URL/candidate when available.

3. Crawl stage safety gate
- If crawl ingestion discovers zero chapters (`recordsSeen = 0`), the request is marked `failed` and does not continue into enrichment.
- This prevents false `succeeded` outcomes when a source URL is wrong or parser strategy cannot extract chapters.

4. Enrichment stage
- When crawl discovers chapters, field jobs are created and processed in bounded cycles.
- Request progress tracks per-field queue counts (`find_website`, `find_email`, `find_instagram`) and totals.

Operator notes:
- `Chapters Discovered` and `Field Jobs Created` are shown in request details for fast triage.
- `Expedite` prioritizes request/job execution by moving schedule to now and raising queue priority.

### Discovery Notes

- Discovery is deterministic and LLM-optional.
- Alias-aware discovery is implemented (for example `Phi Gamma Delta` also queries `fiji`).
- Registry-first source resolution is implemented with conflict policy: health first, then recent successful crawl evidence, then confidence.
- `verified_sources` is intentionally manual-bootstrap/revalidate only (no periodic refresh scheduler).
- Additional fraternity-specific host/source hints can be defined in `services/crawler/src/fratfinder_crawler/discovery.py` when search providers are noisy.
- Graph orchestration now includes navigation stages (`detect_chapter_index_mode`, `extract_chapter_stubs`, bounded follow, contact hint extraction) and then gracefully falls back to existing extraction strategies when navigation signals are weak.
- For map-backed chapter directories (for example Google My Maps embeds), embedded-data detection can emit a KML API hint and `locator_api` can parse KML placemarks into canonical chapter records.

## Migrations and Seeds

- Schema migrations: `infra/supabase/migrations`
- Seed files: `infra/supabase/seeds`
- SQL smoke test: `infra/supabase/tests/schema_smoke.sql`

Apply manually if needed:

```bash
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0001_init.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0002_workflow_hardening.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0003_adaptive_source_types.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0004_chapter_field_states.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0005_crawl_run_intelligence.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0006_benchmark_runs.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0007_fraternity_crawl_requests.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/migrations/0008_verified_sources.sql
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/seeds/0001_seed.sql
```

## Notes

- All secrets must stay in environment variables or `.env` files.
- `.env` files are gitignored by default.
- Review items support strict lifecycle transitions with audit logging through the dashboard API.
- Field jobs are processed by the crawler service with claim/start/complete/fail/requeue semantics and exponential backoff.
- Crawl runs persist page-analysis, classification, and extraction metadata for operator inspection.
- This README is intended to remain an operational runbook as the project evolves.





