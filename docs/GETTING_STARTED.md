# Getting Started

This guide gets FratFinderAI running locally for development and demo use.

## Prerequisites

- Docker Desktop
- Node.js 20+
- pnpm 9+
- Python 3.11+

## Setup

```powershell
Copy-Item .env.example .env
pnpm install
python -m pip install -e "services/crawler[dev]"
```

The default environment is development-oriented. Do not use `WEB_AUTH_DISABLED=true` in production.

## Database

```powershell
pnpm db:up
pnpm db:migrate
pnpm db:seed
pnpm db:smoke
```

This starts the local Postgres/Adminer stack, applies migrations, loads demo seed data, and runs the schema smoke test.

## Web App

```powershell
pnpm dev:web
```

Open `http://localhost:3000`.

Useful routes:

- `/` - overview dashboard
- `/fraternity-intake` - request/intake workflow
- `/chapters` - canonical chapter data
- `/review-items` - human review queue
- `/benchmarks` - validation and KPI surfaces
- `/agent-ops` - worker, queue, and provider operations
- `/nationals` - national-source profiles
- `/crm` - outreach/campaign workflows

## Optional Search Provider

The crawler can use public HTML fallbacks, but local SearXNG is the preferred controllable search provider for demos and development.

```powershell
docker compose -f infra/docker/docker-compose.yml --profile search up -d searxng-rescue
python -m fratfinder_crawler.cli searxng-health --query "delta chi fraternity" --engines bing
```

If an engine is CAPTCHA-blocked or unresponsive, run the engine smoke command and update local `.env` engine settings:

```powershell
python -m fratfinder_crawler.cli searxng-engine-smoke --endpoint http://localhost:8889 --engines bing,duckduckgo,brave,qwant,wikipedia
```

## Crawler Commands

Run the diagnostic doctor:

```powershell
python -m fratfinder_crawler.cli doctor
```

Run a bounded field-job batch:

```powershell
python -m fratfinder_crawler.cli process-field-jobs --limit 30 --workers 2 --runtime-mode langgraph_primary --graph-durability sync --run-preflight
```

Run a continuous field-job worker:

```powershell
python -m fratfinder_crawler.cli run-field-job-worker --limit 30 --workers 2 --runtime-mode langgraph_primary --graph-durability sync
```

Discover a national source candidate:

```powershell
python -m fratfinder_crawler.cli discover-source --fraternity-name "Lambda Chi Alpha"
```

Run search preflight:

```powershell
python -m fratfinder_crawler.cli search-preflight --probes 4
```

## Validation

Run the web and contract checks:

```powershell
pnpm lint
pnpm typecheck
pnpm test:contracts
pnpm test:web
```

Run crawler tests:

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70
```

Run integration tests:

```powershell
python -m pytest tests/integration -m integration
```

Run focused security tests:

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests -k "url_safety or ssrf or security_sca"
```

## Troubleshooting

- If `/api/health` reports no active workers while jobs are queued, start `run-field-job-worker`.
- If SearXNG is reachable but resultless, smoke-test engines and pin a healthy engine in `.env`.
- If web auth blocks local development, confirm `APP_ENV=development` and `WEB_AUTH_DISABLED=true`. Production must not disable auth.
- If Docker commands fail, verify Docker Desktop is running.
