# Frat Finder AI - Staged Implementation Plan

## 1) Purpose
This plan translates the master requirements into an execution sequence Codex can implement in controlled stages. Each stage has scope, outputs, and acceptance gates so progress is measurable and handoff-friendly.

## 2) Non-Negotiable Architecture Rules
- Monorepo layout:
  - `apps/web` -> Next.js TypeScript operator dashboard.
  - `services/crawler` -> Python ingestion service.
  - `packages/contracts` -> shared schemas/types/contracts.
  - `infra` -> Docker, Supabase/Postgres migrations, seed scripts, ops files.
- No crawling/parsing/DB business logic in React components.
- No low-level HTML parsing inside LangGraph nodes.
- DB access through repository/data-access layer (not scattered raw SQL).
- Secrets only via env vars and `.env` files; never hardcoded.

## 3) Stage-by-Stage Delivery

### Stage 0 - Repository Bootstrap and Standards
**Goal:** Create production-ready project skeleton and engineering guardrails.

**Scope**
- Initialize monorepo with package/workspace management.
- Create folder structure:
  - `apps/web`
  - `services/crawler`
  - `packages/contracts`
  - `infra/docker`
  - `infra/supabase/migrations`
  - `tests` (integration/e2e organization)
- Add root docs:
  - `README.md` (operational doc with setup/run/test/db inspection flows).
  - `CHANGELOG.md` (initial entry with Stage 0 summary).
  - `.env.example` (all required variables with placeholders).
  - `.gitignore` including `.env*` protections.
- Add formatter/linter config and baseline CI checks.

**Deliverables**
- Bootstrapped repository with clear scripts for lint/typecheck/test.
- Developer can run one command to validate baseline health.

**Acceptance Gates**
- Directory structure matches architecture.
- `README.md` and `.env.example` are complete and coherent.
- CI baseline passes on clean clone.

---

### Stage 1 - Infrastructure and Canonical Data Model
**Goal:** Establish Supabase/Postgres as authoritative system of record.

**Scope**
- Add local Docker composition (web, crawler, postgres, optional pgadmin/admin UI).
- Define canonical relational schema in SQL migrations:
  - `fraternities`
  - `sources`
  - `chapters`
  - `chapter_provenance`
  - `crawl_runs`
  - `review_items`
  - `field_jobs`
- Add indexes, foreign keys, uniqueness constraints, and timestamps.
- Add `crawl_run` status model (`pending`, `running`, `succeeded`, `failed`, `partial`).
- Add seed data for initial fraternity/source set.

**Deliverables**
- Reproducible local database setup via Docker.
- Versioned migration files + seed scripts.

**Acceptance Gates**
- Fresh boot + migration + seed works end-to-end.
- Referential integrity and uniqueness constraints enforce expected behavior.
- At least one SQL smoke test validates schema assumptions.

---

### Stage 2 - Shared Contracts Package
**Goal:** Create source of truth for cross-service typing and validation.

**Scope**
- Implement `packages/contracts` with schemas/types for:
  - canonical chapter record
  - provenance payload
  - run status/result envelopes
  - review item payloads
  - field job requests/results
- Use runtime validation (e.g., Zod/Pydantic-compatible JSON contracts).
- Publish contracts for web + crawler consumption.

**Deliverables**
- Versioned contract package imported by both apps.
- Contract tests to prevent drift.

**Acceptance Gates**
- Contract validation fails on malformed payloads.
- Web/crawler compile with shared contracts and no duplicated schema definitions.

---

### Stage 3 - Crawler Core (Without LangGraph Orchestration Yet)
**Goal:** Build deterministic ingestion engine components in isolation.

**Scope**
- Implement source loader from DB.
- HTTP client with `requests.Session`, retry policy, timeout, and user-agent config.
- Adapter registry by source type:
  - one adapter per supported source pattern.
  - unsupported type -> structured review item creation path.
- Parsing modules with deterministic extraction logic.
- Normalization layer mapping extracted fields to canonical chapter model.
- Repository layer for DB upsert/write operations:
  - chapter upsert
  - provenance insert
  - crawl run updates
  - review item insert
  - field jobs insert for missing optional fields
- Unit tests for adapters, normalization, and repository behaviors.

**Deliverables**
- CLI entrypoint to run a crawl job locally for selected sources.
- Deterministic parser adapter framework with fixtures.

**Acceptance Gates**
- Sample run ingests fixture or live test source into local DB.
- Ambiguous/unsupported records land in `review_items`.
- Missing optional data creates `field_jobs`.

---

### Stage 4 - LangGraph Pipeline Orchestration
**Goal:** Add resilient orchestration around already-built ingestion modules.

**Scope**
- Implement graph nodes strictly for orchestration steps:
  - load sources
  - fetch page
  - select adapter
  - parse + normalize
  - persist
  - enqueue jobs/review items
  - retry/route failures
- Add retry/backoff and failure routing rules at graph level.
- Ensure parser logic remains outside graph nodes.
- Add state typing and traceable run context IDs.

**Deliverables**
- Operational LangGraph pipeline invoking core modules.
- Structured logs and run metadata persisted to `crawl_runs`.

**Acceptance Gates**
- Retries happen on transient failures.
- Non-recoverable failures are logged and surfaced to review/failure tracking.
- Graph execution is reproducible and observable by run ID.

---

### Stage 5 - Operator API Surface and Dashboard Foundation
**Goal:** Expose crawler results and review workflows via Next.js dashboard.

**Scope**
- Build server-side API routes in `apps/web` for:
  - chapter listing + filters
  - run history + run detail
  - review queue list/detail
  - field job queue status
- Keep business logic in server/data layers, not React components.
- Build dashboard pages:
  - Overview metrics
  - Chapters table + detail drawer/page
  - Crawl runs timeline/detail
  - Review queue worklist
- Add authentication placeholder strategy (local dev mode acceptable first).

**Deliverables**
- Usable local operator dashboard connected to DB-backed API routes.

**Acceptance Gates**
- Dashboard renders real DB data from local crawl runs.
- No crawler/parsing logic exists in UI components.
- Error/loading/empty states are handled cleanly.

---

### Stage 6 - Review and Follow-Up Workflows
**Goal:** Complete ambiguity resolution and missing-field lifecycle.

**Scope**
- Add review item state transitions (`open`, `triaged`, `resolved`, `ignored`).
- Add UI actions for triage and resolution notes.
- Add field job processing model (`queued`, `running`, `done`, `failed`).
- Add optional follow-up worker endpoint/CLI to process queued `field_jobs`.
- Add audit trail fields for operator actions.

**Deliverables**
- End-to-end review and job lifecycle from ingestion to resolution.

**Acceptance Gates**
- Operator can resolve a review item and persist rationale.
- Field job lifecycle is queryable and visible in dashboard.
- Lifecycle transitions are validated and tested.

---

### Stage 7 - Reliability, Observability, and Hardening
**Goal:** Make system stable and production-minded.

**Scope**
- Structured logging and correlation IDs across crawler + web.
- Health endpoints and readiness checks.
- Idempotency checks for repeated crawl runs.
- Add integration tests for key happy and failure paths.
- Add security hygiene:
  - strict env loading
  - input validation on all API routes
  - safe error handling without secret leakage.

**Deliverables**
- Reliability baseline with repeatable test suite and diagnostics.

**Acceptance Gates**
- Integration suite passes in CI with local Docker services.
- Duplicate crawl attempts do not corrupt canonical data.
- Failure telemetry is actionable.

---

### Stage 8 - Documentation, Release Readiness, and Handoff
**Goal:** Finalize operational maturity for ongoing implementation and support.

**Scope**
- Expand `README.md` into full runbook:
  - setup
  - local run commands
  - migration lifecycle
  - troubleshooting
  - dashboard usage.
- Update `CHANGELOG.md` with stage-level entries.
- Add architecture decision records (optional but recommended).
- Add a final "definition of done" checklist.

**Deliverables**
- Handoff-ready repository and operator documentation.

**Acceptance Gates**
- New contributor can get system running from docs alone.
- Stage history is traceable in changelog.

## 4) Implementation Sequencing Rules for Codex
- Do not start a stage until all acceptance gates of prior stage are met.
- Open a changelog entry at completion of each stage.
- Keep PRs/slices small and reviewable.
- Prefer schema-first and contract-first changes before feature UI.
- Add tests in the same stage as the behavior they protect.

## 5) Definition of Done (Program Level — Phase 1)
- Local stack runs via Docker with reproducible setup.
- Crawler can ingest from configured sources into canonical schema.
- Provenance is preserved for chapter fields and source URLs/content references.
- Crawl runs, failures, and retries are queryable and visible.
- Ambiguous records appear in review queue.
- Missing optional fields generate and track field jobs.
- Dashboard supports operational inspection and triage workflows.
- Docs and changelog are current and useful for handoff.

## 6) Suggested Stage Ticket Breakdown (Phase 1 Execution Granularity)
- Ticket A: Stage 0 scaffolding/docs/tooling.
- Ticket B: Stage 1 schema/migrations/seeds/docker.
- Ticket C: Stage 2 contracts package + tests.
- Ticket D: Stage 3 crawler adapters/normalizer/repository layer.
- Ticket E: Stage 4 LangGraph orchestration + retry/failure routing.
- Ticket F: Stage 5 web APIs + dashboard pages.
- Ticket G: Stage 6 review/field job lifecycle.
- Ticket H: Stage 7 reliability/observability/integration tests.
- Ticket I: Stage 8 docs hardening and final release checklist.

---

# Phase 2 — Adaptive Crawler Intelligence Upgrade

> **Context:** Phase 1 built a working deterministic ingestion spine. Phase 2 converts the crawler into an adaptive, source-aware system capable of handling unfamiliar fraternity websites without requiring hand-coded layout preconfiguration. The existing strengths — safe persistence, idempotent upserts, provenance, review queues, field jobs, and LangGraph orchestration — must be preserved and extended, not replaced.

---

## P2-1) Gap Analysis: Current vs Target

### Current Graph (6 nodes)
```
fetch_page → parse_page → normalize_records → persist_records → finalize_success | handle_failure
```

### Target Graph (11 nodes)
```
fetch_page → analyze_page_structure → classify_source_type → detect_embedded_data
→ choose_extraction_strategy → extract_records → validate_records
→ normalize_records → persist_records → spawn_followup_jobs → finalize
```

### Specific Gaps (by file)

| Area | Current State | Gap |
|---|---|---|
| `adapters/registry.py` | Only `directory_v1` registered | No adapter routing; unsupported source always becomes review item immediately |
| `adapters/base.py` | `parse(html, url) -> list[ExtractedChapter]` | No confidence output; no embedded-data path; no strategy awareness |
| `adapters/directory_v1.py` | Card/table CSS pattern matching | Breaks on CMS variants, locator pages, script-embedded data, paginated indices |
| `orchestration/graph.py` `_parse_page` node | Looks up adapter by `parser_key` from DB, calls parse directly | No page inspection before adapter selection; `parser_key` hardcoded in seed |
| `models.py` `ExtractedChapter` | No `source_confidence` field | Cannot express extraction certainty |
| `models.py` `NormalizedChapter` | No field-level states | Cannot distinguish "field missing" from "field not found" from "field unverified" |
| `normalization/normalizer.py` | Always queues `find_instagram` and `find_email` | Too aggressive; should queue only for fields that are genuinely missing/uncertain |
| `http/client.py` | Returns raw HTML text only | No embedded-data detection; no endpoint discovery |
| `field_jobs.py` | Regex-only extraction from provenance snippets | Cannot use structured hints from page analysis; no LLM path |
| `config.py` | No LLM configuration | OpenAI key exists in `.env` but is not consumed by crawler |
| `CrawlGraphState` | No analysis, classification, or strategy fields | Graph cannot carry intelligence layer state between nodes |

---

## P2-2) Target Technical Architecture

### Guiding Principle

> Fetch content deterministically. Inspect structure. Classify source. Detect embedded data. Choose extraction strategy. Extract deterministically when possible. Use LLM only when structure is unclear. Validate all output against strict schema. Persist with provenance and confidence. Queue only genuinely missing fields.

### New Module Layout

```
services/crawler/src/fratfinder_crawler/
  analysis/
    __init__.py
    page_analyzer.py         # DOM/text structural summary (deterministic)
    source_classifier.py     # page type + confidence (heuristics → LLM fallback)
    embedded_data_detector.py # JSON-LD, script JSON, API endpoint hints
    strategy_selector.py     # ranked extraction plan from analysis results
  llm/
    __init__.py
    client.py                # OpenAI client; settings-controlled model/tokens
    classifier.py            # LLM fallback for classify_source_type node
    extractor.py             # LLM structured-output extraction (json_schema mode)
  adapters/
    __init__.py
    base.py                  # (update protocol to include source_confidence)
    registry.py              # (now keyed by StrategyFamily, not parser_key)
    directory_v1.py          # (unchanged)
    script_json.py           # NEW: extracts from inline JSON/JSON-LD
    locator_api.py           # NEW: detects and requests backing API endpoints
  orchestration/
    graph.py                 # (expanded to 11 nodes)
    state.py                 # (extended CrawlGraphState)
  models.py                  # (extended with analysis/classification/field-state models)
  config.py                  # (add LLM settings)
```

### New Models (add to `models.py`)

```python
@dataclass(slots=True)
class PageAnalysis:
    title: str | None
    headings: list[str]
    table_count: int
    repeated_block_count: int
    link_count: int
    has_json_ld: bool
    has_script_json: bool
    has_map_widget: bool
    has_pagination: bool
    probable_page_role: str        # "directory" | "detail" | "index" | "search" | "unknown"
    text_sample: str               # first 2000 chars of visible text for LLM context

@dataclass(slots=True)
class SourceClassification:
    page_type: str                 # "static_directory" | "locator_map" | "script_embedded_data" | ...
    confidence: float              # 0.0 – 1.0
    recommended_strategy: str      # "repeated_block" | "table" | "script_json" | "locator_api" | "llm" | "review"
    needs_follow_links: bool
    possible_data_locations: list[str]   # CSS selectors / script IDs / URL hints
    classified_by: str             # "heuristic" | "llm"

@dataclass(slots=True)
class EmbeddedDataResult:
    found: bool
    data_type: str | None          # "json_ld" | "script_json" | "api_hint"
    raw_data: list[dict] | None
    api_url: str | None

@dataclass(slots=True)
class ExtractionPlan:
    primary_strategy: str
    fallback_strategies: list[str]
    max_attempts: int = 2
    llm_allowed: bool = True       # controlled by settings.crawler_llm_enabled
```

Update `ExtractedChapter`:
- Add `source_confidence: float = 1.0`

Update `NormalizedChapter`:
- Add `field_states: dict[str, str]` mapping field name → `"found"` | `"missing"` | `"unverified"` | `"low_confidence"`

### Extended `CrawlGraphState`

```python
class CrawlGraphState(TypedDict, total=False):
    source: SourceRecord
    run_id: int
    html: str
    page_analysis: PageAnalysis          # NEW
    classification: SourceClassification # NEW
    embedded_data: EmbeddedDataResult    # NEW
    extraction_plan: ExtractionPlan      # NEW
    extracted: list[ExtractedChapter]
    normalized: list[dict]
    review_items: list[ReviewItemCandidate]
    metrics: CrawlMetrics
    error: str
    final_status: str
    strategy_attempts: int               # NEW — bounds adaptive loops
```

### New Config Settings (add to `config.py`)

```python
crawler_llm_enabled: bool = Field(default=False, alias="CRAWLER_LLM_ENABLED")
crawler_llm_model: str = Field(default="gpt-4o-mini", alias="CRAWLER_LLM_MODEL")
crawler_llm_max_tokens: int = Field(default=2000, alias="CRAWLER_LLM_MAX_TOKENS")
crawler_llm_max_calls_per_run: int = Field(default=3, alias="CRAWLER_LLM_MAX_CALLS_PER_RUN")
openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
```

Add to `.env.example`:
```
CRAWLER_LLM_ENABLED=false
CRAWLER_LLM_MODEL=gpt-4o-mini
CRAWLER_LLM_MAX_TOKENS=2000
CRAWLER_LLM_MAX_CALLS_PER_RUN=3
OPENAI_API_KEY=your-openai-key-here
```

### LLM Extraction Contract

The LLM extractor must use OpenAI Structured Outputs (response_format=json_schema) and return exactly this shape:

```json
{
  "records": [
    {
      "chapter_name": "string",
      "school_name": "string | null",
      "city": "string | null",
      "state": "string | null",
      "address": "string | null",
      "website_url": "string | null",
      "instagram_url": "string | null",
      "email": "string | null",
      "source_confidence": 0.0
    }
  ],
  "page_level_confidence": 0.0,
  "extraction_notes": "string"
}
```

All LLM output must pass `jsonschema` validation before it enters the normalization step. Failed validation → review item, never a crash. LLM is never called if `crawler_llm_enabled=false` (default).

### Adapter Registry Redesign

The current registry maps `parser_key` (a DB string) → adapter. The new registry maps `strategy_family` → extractor callable. The `parser_key` DB column becomes a hint, not a hard requirement. Unknown `parser_key` no longer routes immediately to review; instead, page analysis decides.

```python
class AdapterRegistry:
    _adapters: dict[str, ParserAdapter] = {
        "repeated_block": DirectoryV1Adapter(),    # existing (renamed strategy family)
        "table": DirectoryV1Adapter(),             # same adapter handles both
        "script_json": ScriptJsonAdapter(),        # NEW
        "locator_api": LocatorApiAdapter(),        # NEW
    }
```

### Extraction Strategy Routing Logic (strategy_selector.py)

```
if embedded_data.found:
    → strategy = "script_json" or "locator_api"
elif classification.page_type == "static_directory":
    → strategy = "repeated_block" or "table"
elif classification.confidence >= 0.75:
    → strategy = classification.recommended_strategy
elif llm_enabled and llm_calls_remaining > 0:
    → strategy = "llm"
else:
    → strategy = "review"
```

### Embedded Data Detection Priority (embedded_data_detector.py)

Check in this order (deterministic, no LLM):
1. JSON-LD `<script type="application/ld+json">`
2. Inline script variables matching patterns like `window.chapters`, `window.locations`, `storepoint`, `wpsl_settings`
3. `data-*` attributes containing JSON arrays
4. Discoverable API endpoint hints: `fetch(` calls in scripts, XHR URLs, GraphQL endpoint patterns

If any is found, extract directly. Mark `EmbeddedDataResult.found = True` and skip HTML-layout parsing entirely.

### Confidence Thresholds

| Confidence | Interpretation | Action |
|---|---|---|
| >= 0.85 | High | Persist |
| 0.60 – 0.85 | Medium | Persist with `low_confidence` field state |
| < 0.60 | Low | Review item |
| Page-level < 0.50 | Insufficient | Full page → review item |

### Field State and Job Queueing (Smarter Enrichment)

After normalization, field job creation should be confidence-aware:

- Field is `found` and confidence >= 0.85 → no job
- Field is `found` and confidence < 0.85 → queue `verify_*` job
- Field is `missing` → queue `find_*` job as before
- `instagram_url` and `contact_email` are never assumed present → always queue `find_*` if missing *(this keeps current behavior but makes it explicit)*

New field job types to add (alongside existing three):
- `verify_website` — confirm the URL is live and matches the chapter
- `verify_school_match` — confirm school name maps to expected canonical institution

---

## P2-3) Staged Implementation Plan

> **Rules (same as Phase 1):** Do not start a stage until all prior acceptance gates pass. Update CHANGELOG after each completed stage. All new parser paths need fixture tests. All LLM paths need mock-based unit tests that never hit the live API.

---

### Stage P2-A — Intelligence Foundation (No LLM)
**Priority: Highest — unblocks all other adaptive work.**

**Goal:** Add page inspection, classification, embedded data detection, and strategy selection as graph nodes. Keep all existing behavior. Directory sources that worked before must still work identically.

**Scope**

New files to create:
- `analysis/page_analyzer.py` — deterministic DOM analysis returning `PageAnalysis`
- `analysis/source_classifier.py` — heuristic classification returning `SourceClassification` (no LLM yet; unclear pages get `page_type="unsupported_or_unclear"`, `confidence=0.0`)
- `analysis/embedded_data_detector.py` — JSON-LD/script/API endpoint detection returning `EmbeddedDataResult`
- `analysis/strategy_selector.py` — routes to an `ExtractionPlan` based on analysis results

Files to update:
- `models.py` — add `PageAnalysis`, `SourceClassification`, `EmbeddedDataResult`, `ExtractionPlan`; add `source_confidence` to `ExtractedChapter`; add `field_states` to `NormalizedChapter`
- `orchestration/state.py` — extend `CrawlGraphState` with new fields
- `orchestration/graph.py` — add `analyze_page_structure`, `classify_source_type`, `detect_embedded_data`, `choose_extraction_strategy` nodes; rename `parse_page` to `extract_records`; add `validate_records` node
- `adapters/registry.py` — remap to strategy families; keep `directory_v1` under both `repeated_block` and `table` keys
- `normalization/normalizer.py` — populate `field_states`; tighten field-job creation logic

New directory: `analysis/__init__.py`

**Acceptance Gates - Stage P2-A**
- Existing `directory_v1` parser tests all still pass without modification
- A new unit test with a `static_directory` fixture classifies correctly to `page_type="static_directory"`, `confidence >= 0.80`, `recommended_strategy="repeated_block"` or `"table"`
- A page with JSON-LD chapter data correctly triggers `EmbeddedDataResult.found=True`
- A fully unknown page produces `page_type="unsupported_or_unclear"`, strategy `"review"`, and a review item — without crashing
- `choose_extraction_strategy` never picks `"llm"` when `crawler_llm_enabled=false` (default)
- `CrawlGraphState` carries all new fields through the graph without error
- Full existing test suite still passes

---

### Stage P2-B — Script/JSON and Locator Adapter Families
**Goal:** Add two new deterministic adapter families so the system can extract from the most common "hard" website patterns without LLM.

**Scope**

New files:
- `adapters/script_json.py` — parses structured JSON/JSON-LD arrays from script tags; returns `list[ExtractedChapter]` with `source_confidence` set based on completeness
- `adapters/locator_api.py` — attempts to fetch a detected backing API URL; parses the JSON response into `list[ExtractedChapter]`; if request fails, returns empty list and lets review fallback handle it

Both adapters must be covered by fixture tests (no live network calls in tests).

DB migration:
- Add `source_type` enum value `"script_embedded"` and `"locator_api"` to complement existing `"html_directory"` and `"json_api"`

Seed update:
- Add at least one entry per new source type for integration testing

**Acceptance Gates - Stage P2-B**
- `script_json.py` extracts chapters from a fixture containing a `window.chapters = [...]` pattern
- `script_json.py` extracts from a JSON-LD `@type: EducationalOrganization` fixture
- `locator_api.py` uses a mocked HTTP response (not live) and extracts correctly
- Unknown JSON shape → review item, no crash
- All prior acceptance gates remain met

---

### Stage P2-C — LLM-Assisted Extraction
**Goal:** Add the bounded LLM extraction path. LLM is never called unless `CRAWLER_LLM_ENABLED=true` and all deterministic strategies failed or had low confidence. Hard token/call budget per run.

**Scope**

New files:
- `llm/__init__.py`
- `llm/client.py` — thin OpenAI wrapper; reads `OPENAI_API_KEY` and model settings from `Settings`; raises `LLMUnavailableError` if key is absent or `crawler_llm_enabled=false`
- `llm/extractor.py` — calls OpenAI with `response_format={"type": "json_schema", ...}` using the extraction contract schema; parses and validates response with `jsonschema`; raises `ExtractionValidationError` on schema mismatch
- `llm/classifier.py` — LLM fallback for classify_source_type when heuristic confidence < 0.5; same guard: only called when LLM is enabled

Settings to add (in `config.py`):
- `crawler_llm_enabled`, `crawler_llm_model`, `crawler_llm_max_tokens`, `crawler_llm_max_calls_per_run`, `openai_api_key`

`.env.example` update:
- Add all five new LLM settings with safe placeholders

Graph update (`graph.py`):
- `classify_source_type` node: call `llm/classifier.py` only when heuristic confidence < 0.5 AND LLM budget remaining; log decision reason
- `extract_records` node: if `extraction_plan.primary_strategy == "llm"` AND LLM budget remaining, call `llm/extractor.py`; decrement call counter; validate output before returning
- Track `strategy_attempts` and `llm_calls_used` in state; never exceed `crawler_llm_max_calls_per_run`

**LLM Safety Rules (enforced by code, not convention):**
1. `llm/client.py` raises immediately if `openai_api_key` is `None`
2. `llm/extractor.py` always validates response with `jsonschema.Draft202012Validator` before returning
3. LLM output records with `source_confidence < 0.60` become review items, not chapter writes
4. LLM output records with `source_confidence >= 0.60 and < 0.85` write to DB with `field_states` marked `"low_confidence"`
5. LLM is never called for pages that already have `EmbeddedDataResult.found=True`

**Acceptance Gates - Stage P2-C**
- Unit test: `llm/extractor.py` with mocked OpenAI response returns correct `list[ExtractedChapter]`
- Unit test: malformed LLM response (missing required field) raises `ExtractionValidationError`, not a crash
- Unit test: when `crawler_llm_enabled=false`, `choose_extraction_strategy` never produces `strategy="llm"`
- Unit test: `llm_calls_used` counter in state correctly limits calls across records
- Integration: a fixture page classified as `unsupported_or_unclear` with LLM disabled → review item
- Integration: a fixture page classified as `unsupported_or_unclear` with LLM enabled (mocked) → chapter write with `low_confidence` field states if confidence 0.6-0.85
- All prior acceptance gates remain met

---

### Stage P2-D — Smarter Enrichment Field Jobs
**Goal:** Make field job creation confidence-aware and add new job types for verification. Fix the current over-aggressive `find_instagram`/`find_email` queuing.

**Scope**

`models.py`:
- Add `FIELD_JOB_VERIFY_WEBSITE = "verify_website"` and `FIELD_JOB_VERIFY_SCHOOL = "verify_school_match"` constants
- Update `FIELD_JOB_TYPES` tuple

`normalization/normalizer.py`:
- Change field job generation to respect `field_states` and `source_confidence`
- Queue `find_*` only when field is clearly `"missing"`
- Queue `verify_*` when field is present but `"low_confidence"`
- Do NOT queue `find_instagram` if `instagram_url` is present and confidence >= 0.85

`field_jobs.py`:
- Add handling for `verify_website` (HTTP HEAD check, 200 = done, else requeue)
- Add handling for `verify_school_match` (normalize school name, compare against known list or regex)

DB migration:
- Add `field_states JSONB NOT NULL DEFAULT '{}'::jsonb` column to `chapters`
- Update `complete_field_job` in repository to also write `field_states` entry when updating a field

**Acceptance Gates - Stage P2-D**
- A chapter with `source_confidence=0.95` for website does not produce a `verify_website` job
- A chapter with `source_confidence=0.70` for website produces a `verify_website` job
- A chapter missing `instagram_url` still produces a `find_instagram` job
- A chapter with `instagram_url` present at high confidence produces no instagram job
- `verify_website` engine marks job done on 200, requeues on timeout/4xx, fails terminal on 5xx > max_attempts
- Full field job suite passes

---

### Stage P2-E — Dashboard Integration and Observability
**Goal:** Surface the new intelligence signals in the operator dashboard so extraction decisions are auditable.

**Scope**

DB migration:
- Add `page_analysis JSONB` and `classification JSONB` columns to `crawl_runs` for inspection
- Persist `extraction_plan.primary_strategy` and `page_level_confidence` from final state into `crawl_runs.metadata`

`db/repository.py`:
- Update `finish_crawl_run` to accept and store page-level intelligence metadata
- Update `upsert_chapter` to write `field_states`

Web API updates:
- `GET /api/runs` — include `strategy_used`, `page_level_confidence`, `llm_calls_used` from metadata
- `GET /api/chapters` — add `field_states` to response payload
- `GET /api/review-items` — include `extraction_notes` from review item payload

Dashboard updates (`apps/web`):
- Crawl runs page: show `strategy_used` badge per run
- Chapter detail: show `field_states` per field (found/missing/unverified/low_confidence)
- Review page: show `extraction_notes` from the LLM or adapter alongside the failure reason

`packages/contracts`:
- Update `canonical-chapter.schema.json` to include `fieldStates`
- Update `review-item-payload.schema.json` to include optional `extractionNotes`

**Acceptance Gates - Stage P2-E**
- Crawl run record persists `strategy_used` and `page_level_confidence` from a real test run
- Chapter record shows correct `field_states` after a crawl
- Dashboard crawl runs page renders a strategy badge without error
- Dashboard chapter view shows field state labels
- Review page shows extraction notes for LLM-sourced items
- Contracts tests pass with updated schemas

---

## P2-4) Architectural Risks and Known Complexity

### Risk 1: DOM Truncation for LLM Context
**Problem:** Many fraternity websites have 200–800KB HTML. Passing raw HTML to GPT-4o-mini at 2000 max tokens will truncate critical content or exceed window limits.
**Mitigation:** The `page_analyzer.py` summary is the LLM's input, not raw HTML. Send: title, heading hierarchy, up to 20 text snippets from repeated blocks, and visible text sample (max 3000 chars). Never send full raw HTML to the LLM.
**Owner:** `llm/extractor.py` and `analysis/page_analyzer.py` must be designed together.

### Risk 2: LLM Token Cost at Scale
**Problem:** At 66 fraternity chapters × multiple sources each, unbounded LLM calls become expensive.
**Mitigation:** `CRAWLER_LLM_MAX_CALLS_PER_RUN` (default 3) hard-caps calls per source per run. Pages that exceed the budget route to review for human inspection, just like unsupported sources do today. Cost per run is bounded and predictable.

### Risk 3: Non-Deterministic LLM Output
**Problem:** Even with Structured Outputs, the LLM may return valid schema but incorrect content (hallucinated school names, wrong state, invented URLs).
**Mitigation:** LLM output is always written with `source_confidence` from the model response (< 1.0). Fields marked `low_confidence` are visible in the dashboard and subject to verification field jobs. Trust level is constrained by provenance — LLM-sourced fields do not silently overwrite previously high-confidence deterministic data.

### Risk 4: `parser_key` DB Column Coupling
**Problem:** Current seeds hardcode `parser_key = "directory_v1"`. The new system routes by strategy family, not parser key. Changing DB values breaks idempotency of existing migration/seed runs.
**Mitigation:** Keep `parser_key` in the DB as a metadata hint/label but stop treating it as a required routing key. Strategy selection now happens in `analyze_page_structure` and `choose_extraction_strategy`. The adapter registry maps strategy families. Old values become informational.

### Risk 5: Graph State Explosion
**Problem:** 11-node graph with 5 new state fields and multiple conditional edges is harder to reason about and debug than the current 6-node graph.
**Mitigation:** Each node still receives an error boundary wrapper (existing `_with_error_boundary` pattern). The new state fields (`page_analysis`, `classification`, etc.) are all `TypedDict total=False` so missing keys are safe. Add a `debug_log_state` helper that emits key state fields at each node boundary to structured logs for traceability. Keep node implementations thin — business logic stays in the `analysis/` and `llm/` modules.

### Risk 6: Backward Compatibility of `ExtractedChapter`
**Problem:** Adding `source_confidence: float = 1.0` and future fields to `ExtractedChapter` could break existing adapter tests that construct it without the field.
**Mitigation:** Use Python dataclass defaults. `source_confidence=1.0` is the default, so existing test fixtures and `directory_v1` output remain correct without any changes to existing tests. The field is additive.

### Risk 7: Locator/Map Sites Requiring JavaScript Rendering
**Problem:** Some chapter locator pages are fully JS-rendered; the backing API endpoint is genuinely not discoverable from the static HTML.
**Mitigation:** Do not add Playwright in Phase 2. If `locator_api.py` cannot discover an endpoint from static HTML, it returns an empty result and the page routes to review. A `needs_browser_rendering` flag on the review item signals this for future Phase 3 work. Do not over-engineer for this case now.

### Risk 8: Source Seed Coverage
**Problem:** The existing seed only has two sources (`beta-theta-pi-main`, `sigma-chi-main`), both `html_directory`. Phase 2 work needs an `unsupported_or_unclear` fixture and a `script_embedded` fixture to test the new paths.
**Mitigation:** Add a `test_sources` seed fixture block with HTML fixture files under `services/crawler/fixtures/`. No live network calls in tests. These fixtures drive acceptance testing for stages P2-A through P2-C.

---

## P2-5) Implementation Sequencing Rules (Phase 2)

- Stage P2-A must be completed and gates met before P2-B begins.
- Stages P2-B and P2-C can be worked in parallel by different workstreams if needed, but P2-B gates must pass before P2-C integration tests run against the full graph.
- Stage P2-D depends on P2-C completing (needs `field_states` from normalizer).
- Stage P2-E can begin once P2-A is complete for the non-LLM dashboard signals; full completion requires P2-C and P2-D.
- All new nodes follow the existing `_with_error_boundary` pattern from `graph.py`.
- All LLM integration tests use mocked OpenAI responses (never hit live API in CI).
- `CRAWLER_LLM_ENABLED` defaults to `false`; no LLM code path activates unless explicitly opted in.
- Update `CHANGELOG.md` and `README.md` after each completed stage gate.

## P2-6) Definition of Done (Phase 2)
- Crawler classifies at least three distinct page families correctly (static directory, script-embedded, locator/API) using saved fixtures.
- LLM extraction path activates for genuinely unclassifiable pages when enabled, is bounded by call budget, and all output is schema-validated before persistence.
- Field job creation is confidence-aware; `find_instagram` is not queued for chapters that already have a high-confidence `instagram_url`.
- `verify_website` and `verify_school_match` job types work end-to-end.
- Page-level classification and strategy decisions are visible in crawl run metadata and the dashboard.
- All new nodes in the graph pass through the existing error boundary; no new crash paths exist.
- `CRAWLER_LLM_ENABLED=false` (default) produces identical behavior to Phase 1 for all known source types.
- Golden fixture test compares Phase 1 `directory_v1` output against Phase 2 output for the same HTML: results must be identical.
