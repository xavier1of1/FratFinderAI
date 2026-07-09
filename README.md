# FratFinderAI

FratFinderAI is a source-aware data automation platform for discovering, verifying, and enriching fraternity chapter records.

The project is intentionally more than a scraper. It combines a Next.js operator console, a Python crawler/orchestration service, PostgreSQL-backed queues, source provenance, status-first contact safety gates, search-provider reliability controls, and security evidence automation.

## What It Does

FratFinderAI helps an operator answer:

- Which chapters exist for a fraternity?
- Which school recognizes each chapter?
- Which national, school, chapter-site, or search evidence supports that answer?
- Which website, Instagram, or email fields are safe enough to write?
- Which ambiguous records need human review instead of automatic mutation?

The system favors precision and explainability over blind automation. Unsafe or weak evidence is routed to review, blocked, or deferred rather than silently written into canonical data.

## Architecture At A Glance

```text
apps/web/          Next.js operator dashboard and API routes
services/crawler/  Python crawler, status engine, search, and worker orchestration
packages/contracts Shared JSON schemas and TypeScript contract helpers
infra/             Docker, migrations, seeds, and local database tooling
docs/              Public architecture, setup, reports, and security documentation
tests/             Integration tests
```

Core subsystems:

- **Operator console:** intake, chapters, review items, benchmarks, agent operations, national source profiles, and CRM surfaces.
- **Crawler service:** national-source discovery, chapter extraction, campus recognition verification, contact enrichment, queue triage, and evidence persistence.
- **Status engine:** school-recognition-first decisions with official school evidence, national evidence, conclusive absence rules, conflict flags, and review routing.
- **Search reliability layer:** SearXNG-first provider chain with endpoint health checks, provider attempts, circuit breakers, degraded-mode handling, and managed-provider smoke tests.
- **Security controls:** SBOM/SCA evidence, SSRF-safe outbound fetch policy, operator RBAC, and audit logging.

## Quick Start

### Prerequisites

- Docker Desktop
- Node.js 20+
- pnpm 9+
- Python 3.11+

### 1. Configure Environment

```powershell
Copy-Item .env.example .env
```

The default `.env.example` is for local development. Production deployments must set real operator tokens/secrets and must not use `WEB_AUTH_DISABLED=true`.

### 2. Install Dependencies

```powershell
pnpm install
python -m pip install -e "services/crawler[dev]"
```

### 3. Start Database

```powershell
pnpm db:up
pnpm db:migrate
pnpm db:seed
pnpm db:smoke
```

### 4. Start Web App

```powershell
pnpm dev:web
```

Open `http://localhost:3000`.

### 5. Check Crawler Health

```powershell
python -m fratfinder_crawler.cli doctor
```

Optional local search provider:

```powershell
docker compose -f infra/docker/docker-compose.yml --profile search up -d searxng-rescue
python -m fratfinder_crawler.cli searxng-health --query "delta chi fraternity" --engines bing
```

### 6. Run Workers

```powershell
python -m fratfinder_crawler.cli run-field-job-worker --limit 30 --workers 2 --runtime-mode langgraph_primary --graph-durability sync
```

For a bounded one-shot batch:

```powershell
python -m fratfinder_crawler.cli process-field-jobs --limit 30 --workers 2 --runtime-mode langgraph_primary --graph-durability sync --run-preflight
```

## Validation

Recommended local validation before opening a PR:

```powershell
pnpm lint
pnpm typecheck
pnpm test:contracts
pnpm test:web
python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70
python -m pytest tests/integration -m integration
```

Security-specific checks:

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests -k "url_safety or ssrf or security_sca"
python scripts/security/security_sca.py validate-ignores --ignore-file .security/vulnerability-ignores.yml
```

## Documentation

Start here:

- [Documentation index](docs/README.md)
- [Getting started](docs/GETTING_STARTED.md)
- [Architecture overview](docs/architecture/README.md)
- [Project report](docs/reports/PROJECT_REPORT.md)
- [Security controls](docs/security/security-sprint-comprehensive-report.md)

## Public Release Notes

This repository is being prepared for public/open-source viewing. Local artifacts, interview-prep files, logs, generated SBOM output, coverage files, runtime scripts, and stress-test dumps are intentionally ignored or removed from the public tree.

Before publishing, review:

- [Public release checklist](docs/release/PUBLIC_RELEASE_CHECKLIST.md)

## License

No open-source license is currently included. Add a license before accepting outside contributions.
