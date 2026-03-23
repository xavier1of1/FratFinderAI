# Frat Finder AI

Frat Finder AI is a source-aware chapter discovery and data platform for NIC fraternities.

It ingests chapter data from national fraternity websites, normalizes records into a canonical relational model, preserves provenance, tracks crawl runs/failures, creates review items for ambiguous records, queues follow-up jobs for missing fields, and exposes everything in a local operator dashboard.

## Architecture

- `apps/web`: Next.js TypeScript operator dashboard and server routes.
- `services/crawler`: Python ingestion service with adapter-based parsing and LangGraph orchestration.
- `packages/contracts`: shared schemas/types for payload and storage contracts.
- `infra`: Docker, SQL migrations, seeds, and DB smoke tests.

Separation rules:

- No crawling/parsing/db business logic in React components.
- No low-level HTML parsing inside LangGraph nodes.
- No scattered raw SQL writes; use repository modules.

## Repository Layout

```text
apps/
  web/
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
python -m fratfinder_crawler.cli process-field-jobs --source-slug sigma-chi-main --limit 25
```

### Search-Backed Enrichment

When chapter website, email, or Instagram data is not present on the national source page, field jobs can now fall back to public web search.

Relevant env settings:

- `CRAWLER_SEARCH_ENABLED=true` enables search-backed enrichment for missing fields.
- `CRAWLER_SEARCH_PROVIDER=auto` is now the recommended default: it prefers Brave Search API when `CRAWLER_SEARCH_BRAVE_API_KEY` is set and otherwise falls back to Bing HTML.
- `CRAWLER_SEARCH_PROVIDER=bing_html` remains available for explicit local-only testing when you want to bypass Brave.
- `CRAWLER_SEARCH_PROVIDER=duckduckgo_html` remains available and now auto-falls back to Bing when DuckDuckGo returns anomaly pages or transport-level failures.
- `CRAWLER_SEARCH_PROVIDER=brave_api` can be used explicitly if you want Brave only; when Brave errors, the crawler falls back to Bing instead of stalling jobs.
- `CRAWLER_SEARCH_MAX_RESULTS` and `CRAWLER_SEARCH_MAX_PAGES_PER_JOB` bound how aggressively each job searches and fetches candidate pages.

The crawler only writes high-confidence matches directly. Lower-confidence search candidates are routed into review instead of silently mutating chapter records.

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
  - Health: `/api/health`, `/api/health/liveness`, `/api/health/readiness`

### Dashboard Signals

- Crawl runs now surface extraction strategy, page-level confidence, and LLM calls used.
- Chapter rows now include field-state labels so operators can distinguish `found`, `missing`, and `low_confidence` values.
- Review items now surface extraction notes when the crawler or LLM records them in the review payload.

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
docker exec -i fratfinder-postgres psql -U postgres -d fratfinder < infra/supabase/seeds/0001_seed.sql
```

## Notes

- All secrets must stay in environment variables or `.env` files.
- `.env` files are gitignored by default.
- Review items support strict lifecycle transitions with audit logging through the dashboard API.
- Field jobs are processed by the crawler service with claim/start/complete/fail/requeue semantics and exponential backoff.
- Crawl runs persist page-analysis, classification, and extraction metadata for operator inspection.
- This README is intended to remain an operational runbook as the project evolves.
