# FratFinderAI — Comprehensive Project Report

> Generated: 2026-03-31 | Current version: 3.0.0

---

## 1. What Is This Project?

**FratFinderAI** is a production-grade, source-aware chapter discovery and data enrichment platform for NIC (North-American Interfraternity Conference) fraternities. Its core mission: automatically find, extract, normalize, and continuously enrich contact data (website, email, Instagram) for every chapter of every major fraternity in the U.S. — starting from national fraternity websites and falling back to public web search.

The system is designed for an "operator" persona — someone managing the pipeline, triaging ambiguous data, and making high-stakes sourcing decisions — exposed through a rich Next.js dashboard. It is built with a strong emphasis on **data safety**, **provenance**, **idempotent writes**, and **explainable decisions**.

---

## 2. Architecture Overview

The project is a **pnpm monorepo** with clear separation of concerns across four top-level workspaces:

```
apps/web          →  Next.js 14 operator dashboard + REST API routes
services/crawler  →  Python 3.11 ingestion engine (LangGraph-orchestrated)
packages/contracts → Shared JSON Schema / TypeScript contracts
infra/            →  Docker Compose, PostgreSQL 16 migrations, seeds, smoke tests
```

### Architectural Rules (non-negotiable)
- No crawling/parsing/DB logic in React components
- No raw HTML parsing inside LangGraph graph nodes — parsing is always inside adapter classes
- No scattered raw SQL writes — all DB access is through repository modules
- Secrets only via `.env` files, never hardcoded

---

## 3. Data Pipeline Flow

The end-to-end data flow has two major phases:

### Phase 1 — National Site Crawl
```
CLI / Intake API
    → CrawlService.run()
        → CrawlerRepository.load_sources()
        → For each source: CrawlOrchestrator.run_for_source()
            → LangGraph Graph (CrawlGraphState)
                [fetch_page]
                    → [analyze_page_structure]
                        → [classify_source_type]
                            → [detect_embedded_data]
                                → [detect_chapter_index_mode]
                                    → [extract_chapter_stubs]
                                        → [follow_chapter_detail_or_outbound]
                                            → [extract_contacts_from_chapter_site]
                                → [choose_extraction_strategy]
                                    → [extract_records]
                                        → [validate_records]
                                            → [normalize_records]
                                                → [persist_records]
                                                    → [finalize_run]
```

### Phase 2 — Field Job Enrichment (Search-Backed)
```
CLI process-field-jobs
    → FieldJobEngine (SKIP LOCKED queue claim)
        → ThreadPoolExecutor (up to 10 workers)
            → Per job: search provider (Brave API / Bing HTML / DuckDuckGo HTML)
                → Candidate extraction & relevance scoring
                    → High-confidence → direct chapter write
                    → Low-confidence → review_items queue
                    → No candidate → cooldown backoff + requeue
```

---

## 4. Tech Stack — Deep Dive

### 4.1 Python Crawler (`services/crawler`)

| Technology | Role |
|---|---|
| **Python 3.11+** | Crawler runtime |
| **LangGraph 0.2+** | Orchestration of the multi-step crawl pipeline as a stateful directed graph |
| **Pydantic v2 + pydantic-settings** | Settings validation (40+ env vars), data model validation |
| **psycopg v3** | PostgreSQL access with `SKIP LOCKED` for concurrent job claiming |
| **BeautifulSoup 4** | HTML parsing within adapters |
| **requests** | HTTP client with retry/backoff |
| **OpenAI SDK** | Optional LLM classification and structured extraction (gpt-4o-mini) |
| **jsonschema (Draft 2020-12)** | Validates LLM JSON outputs against strict schemas |
| **pytest + pytest-mock** | Unit and fixture-backed adapter tests |

#### LangGraph Usage
LangGraph is used as the **orchestration backbone** of the crawl pipeline. Each step of the crawl is a named graph node:

- `fetch_page` → HTTP fetch with retry
- `analyze_page_structure` → heuristic DOM analysis (table count, repeated blocks, page role)
- `classify_source_type` → deterministic rules first; LLM fallback when heuristics are inconclusive (budget-gated to `crawler_llm_max_calls_per_run`)
- `detect_embedded_data` → JSON-LD, `window.chapters`-style scripts, KML locator APIs
- `detect_chapter_index_mode` → decides between `direct_chapter_list`, `internal_detail_pages`, `map_or_api_locator`, etc.
- `extract_chapter_stubs` → multi-strategy stub extraction (table, repeated block, script JSON, locator API, anchor fallback)
- `follow_chapter_detail_or_outbound` → bounded hop navigation (configurable `NAV_MAX_HOPS_PER_STUB`, `NAV_MAX_PAGES_PER_RUN`)
- `extract_contacts_from_chapter_site` → chapter website scraping for email/Instagram signals
- `choose_extraction_strategy`, `extract_records`, `validate_records`, `normalize_records`, `persist_records`, `finalize_run`

The **`CrawlGraphState`** TypedDict carries all inter-node shared state: `source`, `run_id`, `html`, `page_analysis`, `classification`, `embedded_data`, `extraction_plan`, chapter stubs, contact hints, navigation stats, LLM call budget, and final metrics.

Each node is wrapped in `_with_error_boundary()` so a single node failure sets `error` state and routes to run finalization without corrupting other records.

#### Adapter System
Three pluggable adapter families handle different source formats:
- **`DirectoryV1Adapter`** — table/card-based chapter directory HTML
- **`ScriptJsonAdapter`** — JSON-LD and inline `window.chapters`-style embedded data
- **`LocatorApiAdapter`** — REST API-backed chapter locators (including KML/Google My Maps)

Each adapter implements `parse_stubs()` returning `ChapterStub` objects. The `AdapterRegistry` maps strategy names to adapter instances.

#### LLM Integration (bounded)
- **Classifier** (`llm/classifier.py`): Uses OpenAI structured outputs with strict JSON Schema validation to classify page type and recommend extraction strategy
- **Extractor** (`llm/extractor.py`): Structured chapter extraction when heuristic adapters yield no results
- Both are guarded by `CRAWLER_LLM_ENABLED` flag and `crawler_llm_max_calls_per_run` budget; disabled by default in production to control costs
- LLM output is validated against `Draft202012Validator` before use — invalid or low-confidence LLM output routes to `review_items`, never writes directly

#### Search-Backed Enrichment
A multi-provider search stack handles missing contact fields post-crawl:
- **Providers**: Brave Search API (preferred when key present), Bing HTML, DuckDuckGo HTML (with Bing fallback on anomaly pages)
- **`auto` mode**: selects the best available provider at runtime
- **Circuit breaker**: opens after `CRAWLER_SEARCH_CIRCUIT_BREAKER_FAILURES` consecutive transport failures, cools down for `CRAWLER_SEARCH_CIRCUIT_BREAKER_COOLDOWN_SECONDS`
- **Dependency enforcement**: email search waits for confident website discovery first (configurable)
- **Relevance gates**: school, fraternity, and chapter name matching filters; Instagram-specific handle-shape queries (`fsusigmachi`); wrong-organization rejection (`Tri Sigma`, chemistry companies)
- **Confidence tiers**: tier-1 (`.edu` campus pages, known fraternity domains) → auto-write; tier-2 aggregators → review; no candidate → cooldown requeue

#### Data Safety Mechanisms
- **`SKIP LOCKED`** on `field_jobs` table enables safe multi-worker parallel job claiming with zero double-processing
- **Idempotent upserts** — chapters are upserted by `(fraternity_id, slug)` unique constraint
- **Normalization safety gate** — placeholder/navigation slugs (`find-a-chapter`, `our-chapters`, `visit-page-*`) are intercepted and routed to `ambiguous_record` review
- **Provenance records** — every field value is linked to its source URL, source snippet, and confidence score in `chapter_provenance`
- **`field_states`** — per-field confidence state (`found`, `low_confidence`, `missing`) stored in JSONB column on `chapters`

---

### 4.2 Next.js Web App (`apps/web`)

| Technology | Role |
|---|---|
| **Next.js 14 (App Router)** | Server-rendered dashboard with server components and API routes |
| **TypeScript 5** | Full type safety; contracts shared with `packages/contracts` |
| **React 18** | Client component interactivity (chapter filters, benchmark dashboard) |
| **`pg` (node-postgres)** | Direct PostgreSQL connection pool from Next.js API routes |
| **Zod 3** | Input validation on API route handlers |
| **Space Grotesk + IBM Plex Mono** | Typography (Google Fonts) |

#### Dashboard Pages
| Route | Purpose |
|---|---|
| `/` | System overview: chapter count, latest run, open reviews, queued jobs |
| `/chapters` | Filterable table (500 rows) + U.S. state tile map for chapter coverage |
| `/runs` | Crawl run history with strategy badges, confidence scores, LLM call counts |
| `/review-items` | Triage queue for ambiguous/failed extractions |
| `/benchmarks` | Multi-cycle field-job benchmark runner with throughput metrics |
| `/fraternity-intake` | Staged intake workflow: submit name → auto-discover source → confirm → crawl → enrich |

#### API Surface
REST API routes under `/api/` backed by repository modules:
- `GET /api/chapters` — filterable chapter list
- `GET/POST /api/runs` — crawl run history and detail
- `GET /api/review-items`, `PATCH /api/review-items/[id]` — triage workflow
- `GET/POST /api/field-jobs` — job queue status
- `GET/POST /api/benchmarks` — benchmark run management
- `GET/POST /api/fraternity-crawl-requests` — intake lifecycle (create/confirm/cancel/reschedule/expedite)
- `GET /api/health` — liveness/readiness probes

#### Repository Pattern (Web)
Six typed repository modules in `apps/web/src/lib/repositories/`:
- `chapter-repository.ts`
- `crawl-run-repository.ts`
- `review-item-repository.ts`
- `field-job-repository.ts`
- `benchmark-repository.ts`
- `fraternity-crawl-request-repository.ts`

All use the shared `getDbPool()` singleton from `lib/db.ts` (connection pool capped at 10, lazy-loaded from `.env`).

#### API Envelope
All API responses follow a consistent `{ success: true, data: T }` / `{ success: false, error: { code, message, requestId } }` envelope pattern (`lib/api-envelope.ts`).

---

### 4.3 Shared Contracts (`packages/contracts`)

- JSON Schema files define canonical shapes for chapter records, provenance payloads, field-job payloads, and review-item payloads
- TypeScript types exported for web consumption
- Schema validation used by the Python crawler via `jsonschema`
- Vitest test suite prevents schema/type drift between services

---

### 4.4 Database — PostgreSQL 16 (`infra/supabase/`)

8 versioned migration files implement the full schema:

| Migration | What it adds |
|---|---|
| `0001_init.sql` | Core schema: `fraternities`, `sources`, `crawl_runs`, `chapters`, `chapter_provenance`, `review_items`, `field_jobs` |
| `0002_workflow_hardening.sql` | Additional constraints and audit fields |
| `0003_adaptive_source_types.sql` | `script_embedded` and `locator_api` source types |
| `0004_chapter_field_states.sql` | `field_states` JSONB column on `chapters` |
| `0005_crawl_run_intelligence.sql` | `strategy_used`, `page_level_confidence`, `llm_calls_used` on `crawl_runs` |
| `0006_benchmark_runs.sql` | `benchmark_runs` table for throughput benchmarking |
| `0007_fraternity_crawl_requests.sql` | `fraternity_crawl_requests`, `fraternity_crawl_request_events`, `field_jobs.priority` |
| `0008_verified_sources.sql` | `verified_sources` registry (health metadata, confidence, provenance, active flag) |

Key design patterns:
- All PKs are UUIDs via `gen_random_uuid()` (pgcrypto)
- `BIGSERIAL` for `crawl_runs.id` (append-only log)
- `set_updated_at()` trigger function applied across all mutable tables
- `JSONB` columns for extensible metadata (`sources.metadata`, `chapters.field_states`, `chapter_provenance`, `crawl_runs.metadata`)
- `SKIP LOCKED` pattern for concurrent field-job claiming
- `CHECK` constraints on all status/type enum columns

---

### 4.5 Infrastructure (`infra/docker/`)

- **Docker Compose** with named profiles: `postgres` + `adminer` run always; `web` and `crawler` containers under `--profile app`
- PostgreSQL mapped to port `5433` (avoids collision with host Postgres on 5432)
- `Dockerfile.web` and `Dockerfile.crawler` for containerized deployment
- `apply-migrations.ps1` and `apply-seed.ps1` PowerShell scripts for Windows dev environments

---

## 5. Fraternity Intake Workflow (Key Feature)

The **Fraternity Intake** system (`/fraternity-intake`) is the highest-level operator-facing feature. It represents the full lifecycle of onboarding a new fraternity from just a name:

```
1. Operator submits fraternity name (e.g., "Chi Psi")
2. Discovery stage:
   - Checks verified_sources registry first (registry-first resolution)
   - Falls back to existing configured sources
   - Falls back to search-backed discovery
   - Returns: sourceUrl, confidence tier, provenance, fallback reason, resolution trace
3. Awaiting confirmation:
   - Operator reviews ranked candidates
   - Can approve auto-discovered URL, select a candidate, or paste manual override
4. Crawl run stage:
   - Full crawl pipeline executes against confirmed URL
   - Safety gate: zero-chapter result = terminal failure (no false success)
5. Enrichment stage:
   - Bounded field-job cycles for find_website, find_email, find_instagram
   - Configurable workers, cycles, and pause between cycles
6. Completed / Failed:
   - Timeline events persisted end-to-end
   - Progress snapshot queryable at any time
```

Discovery is **registry-first**: the `verified_sources` table (21+ rows bootstrapped from `research_nav_21.json`) provides pre-validated, high-confidence source URLs with sub-30ms lookup latency.

---

## 6. Verified Sources Registry

`verified_sources` is a separately managed registry of known-good national fraternity website URLs:
- Bootstrapped via `bootstrap-nic-sources --input research_nav_21.json`
- Each row stores: `fraternity_slug`, `national_url`, `origin`, `confidence`, `http_status`, `checked_at`, `is_active`, `metadata` (JSONB for mode hints, selectors, etc.)
- Revalidated on demand via `revalidate-verified-source` and `revalidate-verified-sources` CLI commands
- Health check probes each URL and updates `http_status` + `is_active`
- When a registry seed is unhealthy (e.g., `410 Gone`), discovery falls back explicitly with `fallbackReason` recorded

---

## 7. Observability & Quality Controls

| Feature | Implementation |
|---|---|
| Structured logging | `log_event()` utility emitting JSON-compatible key-value log entries |
| Correlation IDs | `crawl_runs.correlation_id` UUID for end-to-end tracing |
| Crawl run intelligence | `strategy_used`, `page_level_confidence`, `llm_calls_used`, `navigation_stats` persisted per run |
| Provenance | Every field write linked to source URL + confidence in `chapter_provenance` |
| Review queue | All ambiguous, low-confidence, or failed records land in `review_items` with `reason` and `extractionNotes` |
| Field states | Per-field confidence state (`found`/`low_confidence`/`missing`) on every chapter record |
| Benchmark runner | Multi-cycle throughput benchmarking with `jobs/min`, `avgCycleMs`, queue depth delta |
| Health probes | `GET /api/health?probe=liveness` and `?probe=readiness` (also available via CLI) |
| Schema smoke tests | `infra/supabase/tests/schema_smoke.sql` validates schema assumptions post-migration |
| Integration tests | `tests/integration/test_local_demo_flow.py` end-to-end flow validation |

---

## 8. Key Engineering Decisions

1. **LangGraph for orchestration, not parsing** — graph nodes handle routing, retry, and state transitions; all HTML parsing stays in adapter classes. This keeps the graph readable and decoupled from source-specific logic.

2. **Registry-first discovery** — pre-validated `verified_sources` provides sub-30ms high-confidence lookups before any search is attempted, dramatically reducing discovery latency and false positives for known fraternities.

3. **Confidence-tiered writes** — nothing is written directly below a confidence threshold. Medium/low confidence outcomes go to review, never silently mutate chapter records.

4. **`SKIP LOCKED` concurrency** — field-job workers use PostgreSQL advisory-style `SKIP LOCKED` in their claiming query, enabling safe scale-out to multiple concurrent workers without a queueing middleware (no Redis, no Celery).

5. **Search circuit breaker** — configurable failure-count circuit breaker prevents workers from spending full timeout windows on every query during provider outages; fails fast and cools down.

6. **Placeholder/navigation record gate** — the normalizer intercepts known noisy chapter slugs (`find-a-chapter`, `our-chapters`, `visit-page-*`) before they can persist as fake chapter records.

7. **Monorepo with shared contracts** — `packages/contracts` is the single source of truth for inter-service schema definitions. The Python crawler and TypeScript web app both validate against the same JSON Schema files.

8. **Greedy collect modes** — `GREEDY_COLLECT` has three modes (`none`, `passive`, `bfs`) controlling how aggressively the crawler follows same-domain links on nationals sites, giving operators cost vs. coverage control.

---

## 9. Real-World Benchmark Results (2026-03-31)

From the validation report on the new-fraternity cohort:

| Fraternity | Chapters | Website | Instagram | Email |
|---|---|---|---|---|
| Alpha Gamma Rho | 74 | 61 | 19 | 13 |
| Alpha Delta Gamma | 31 | 31 | 31 | 31 |
| Alpha Delta Phi | 40 | 0 | 1 | 0 |
| Beta Upsilon Chi | 36 | 36 | 0 | 35 |

- 4/5 test fraternities resolved directly from `verified_sources` registry in ~20–26ms
- 1/5 (`chi-phi`) failed cleanly with `410 Gone` — no unsafe writes, explicit fallback reason recorded
- Zero-chapter safety gate correctly halted false-success intake flows

---

## 10. Project Maturity

The project is at **version 0.10.0** with 10 release cycles. It has progressed through:
- Phase 1: Deterministic ingestion spine (stages 0–8)
- Phase 2: Adaptive crawler intelligence (LLM integration, search-backed enrichment, navigation, registry)
- Current focus: source-specific extraction hints, intake observability metrics, multi-cycle throughput optimization

All tests pass as of 2026-03-31 (`pytest` for crawler, `tsc --noEmit` for web).
