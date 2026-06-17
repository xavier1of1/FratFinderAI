# FratFinderAI Senior Engineer Demo Readiness

> Prepared: 2026-06-16 | Version: 3.0.4 | Local demo URL: http://127.0.0.1:3000

## Executive Readiness Summary

FratFinderAI is ready for a senior-engineer walkthrough as a working product demo, not just a static portfolio artifact. The local web app is running, the database and SearXNG rescue service are healthy, the field-job worker is active, and the core validation suite passed after correcting two demo-critical issues found during readiness prep.

Issues found and fixed during this pass:

- SearXNG was pinned to `startpage`, which returned CAPTCHA/unresponsive-engine signals locally. The demo config now uses the healthy SearXNG rescue endpoint on `http://localhost:8889` with the `bing` engine.
- `node_modules` was stale and still resolved Next.js `14.2.15` even though package manifests and lockfile required `14.2.25`. Running `pnpm install` reconciled the workspace, and the dev server now starts with Next.js `14.2.25`.
- An SSRF test leaked local `.env` endpoint settings into a unit assertion. The test now explicitly isolates `CRAWLER_SEARCH_SEARXNG_BASE_URLS`.

## Role-Relevant Feature Map

| Customer Strategist Need | FratFinderAI Demo Feature | What To Emphasize |
| --- | --- | --- |
| Build and deliver demos | Next.js operator console, intake flow, dashboards, PDF overview | You can tell the product story, operate the system, and explain tradeoffs live. |
| Work with intense customers | Status-first verification, provenance, review queues, SSRF/RBAC/SBOM controls | The platform prefers safe unresolved work over false-positive writes. |
| Data analysis and KPIs | Benchmarks, Agent Ops, queue health, worker phases, search-provider diagnostics | You measure performance by throughput, safety, evidence, and user-visible outcomes. |
| Voice of user to roadmap | Review items, blocked-reason families, failure audits, validation reports | You turned recurring user pain into productized workflow and observability. |
| Shape opportunities and use cases | CRM, chapter/contact enrichment, official-source discovery | The core engine can support data operations, compliance review, and outreach workflows. |
| Cross-functional collaboration | Security sprint reports, two-page technical PDF, project report | The project has engineer-readable evidence and non-engineer-readable narrative. |

## Demo Walkthrough

Recommended 10-minute flow:

1. Open `http://127.0.0.1:3000`.
2. Start with the overview dashboard and explain the pipeline: intake -> discovery -> status verification -> enrichment -> review/CRM.
3. Open `/fraternity-intake` to show how an operator scopes work.
4. Open `/agent-ops` to show queue health, worker liveness, provider health, and operational telemetry.
5. Open `/chapters` to show canonical data and evidence-backed enrichment.
6. Open `/review-items` to show safety-first ambiguity handling.
7. Open `/benchmarks` to show KPI and validation thinking.
8. Mention `docs/reports/FratFinderAI_Two_Page_Overview.pdf` and `docs/reports/PROJECT_REPORT.md` as the leave-behind artifacts.

Recommended 30-minute engineering flow:

1. Show the web console and live health endpoint.
2. Show `doctor` output: runtime compatibility, provider chain, SearXNG endpoint health, queue state, worker phase metadata.
3. Show the field-job worker log summary: `processed=30`, `requeued=0`, `failed_terminal=0`, `runtime_fallback_count=0`, `verify_school_provider_search_attempted=0`.
4. Show security evidence: SBOM/SCA, SSRF-safe fetch policy, RBAC/audit docs and tests.
5. Show the status model report or project report to explain why school recognition is the highest authority.
6. Close with product judgment: this is not just scraping; it is evidence orchestration with operator trust gates.

## Live Runtime Snapshot

Validated local services:

| Component | Status |
| --- | --- |
| PostgreSQL | Healthy Docker container on port `5433` |
| Adminer | Running on port `8080` |
| SearXNG rescue | Healthy Docker container on port `8889` |
| SearXNG primary | Stopped for demo because local `8888` was unhealthy/json-disabled |
| Web app | Running on `http://127.0.0.1:3000` |
| Field-job worker | Active with `langgraph_primary` runtime |

Search/provider proof:

```text
searxng_json endpoint: http://localhost:8889
engine: bing
healthReason: healthy
resultBearingRate: 1.0
sample query: delta chi fraternity
resultCount: 10
```

Queue/worker proof:

```text
health.ok: true
activeFieldJobWorkers: 1
queueWorkerAlert: false
requestWorkerAlert: false
SearXNG reachable: true
```

Recent worker batch proof:

```text
processed: 30
requeued: 0
failed_terminal: 0
provider_degraded_deferred: 0
runtime_fallback_count: 0
verify_school_provider_search_attempted: 0
nationals_only_contact_rows: 0
```

## Route Smoke Test

All role-relevant UI/API routes returned `200`.

| Route | Status |
| --- | --- |
| `/` | 200 |
| `/fraternity-intake` | 200 |
| `/chapters` | 200 |
| `/review-items` | 200 |
| `/benchmarks` | 200 |
| `/agent-ops` | 200 |
| `/nationals` | 200 |
| `/crm` | 200 |
| `/api/health` | 200 |
| `/api/health/liveness` | 200 |
| `/api/health/readiness` | 200 |
| `/api/chapters?limit=3` | 200 |
| `/api/field-jobs?limit=3` | 200 |
| `/api/fraternity-crawl-requests/summary` | 200 |

## Validation Evidence

Commands run successfully on 2026-06-16:

```powershell
pnpm.cmd lint
pnpm.cmd typecheck
pnpm.cmd test:contracts
pnpm.cmd test:web
python -m pytest services/crawler/src/fratfinder_crawler/tests/test_security_sca_policy.py services/crawler/src/fratfinder_crawler/tests/test_url_safety.py services/crawler/src/fratfinder_crawler/tests/test_ssrf_callsite_coverage.py services/crawler/src/fratfinder_crawler/tests/test_searxng_health.py services/crawler/src/fratfinder_crawler/tests/test_school_verification.py
python -m pytest tests/integration -m integration
python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70
```

Results:

| Validation | Result |
| --- | --- |
| Lint | PASS |
| Typecheck | PASS |
| Contract tests | 5 passed |
| Web tests | 47 passed |
| Focused crawler/security tests | 51 passed |
| Integration tests | 2 passed |
| Full crawler suite | 589 passed, coverage 70.84% |

## Demo Startup Commands

If anything needs to be restarted tomorrow:

```powershell
docker compose -f infra/docker/docker-compose.yml up -d postgres adminer
docker compose -f infra/docker/docker-compose.yml --profile search up -d searxng-rescue
powershell -ExecutionPolicy Bypass -File .runtime\start-web-demo.ps1
powershell -ExecutionPolicy Bypass -File .runtime\start-field-worker-demo.ps1
```

Health checks:

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:3000/api/health
python -m fratfinder_crawler.cli doctor
python -m fratfinder_crawler.cli searxng-health --query "delta chi fraternity" --engines bing
```

## Known Caveats To Frame Honestly

- The dashboard is currently running with `WEB_AUTH_DISABLED=true` for local demo convenience. The RBAC system is implemented and tested; production mode fails closed if auth is disabled or required secrets are missing.
- The local primary SearXNG container on `8888` was unhealthy, so the demo is intentionally pinned to the healthy rescue endpoint on `8889`.
- DuckDuckGo HTML remains in the fallback chain but is currently challenge-prone. SearXNG and Bing HTML were viable in the validation window.
- Some queue backlog remains by design; blocked dependency and repairable states are part of the safety model, not a signal that writes are bypassing validation.

## Leave-Behind Artifacts

- `docs/reports/PROJECT_REPORT.md`
- `docs/reports/FratFinderAI_Two_Page_Overview.pdf`
- `docs/security/security-sprint-comprehensive-report.md`
- `docs/security/security-sprint-validation.md`
- `docs/SystemReport/SEARCH_PROVIDER_RELIABILITY_AUDIT_2026-04-20.md`
- `docs/SystemReport/FRATERNITY_CHAPTER_VERIFICATION_TECHNICAL_MODEL_2026-04-19.md`
