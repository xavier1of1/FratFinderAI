# Changelog

All notable changes to this project will be documented in this file.

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



