# LangGraph Unified Crawler Architecture

## Objective
Rebuild the crawler into a LangGraph-first agent system that uses one orchestration model for:

- source discovery and source revalidation
- national-site crawling and adaptive navigation
- chapter extraction and normalization
- contact enrichment through direct navigation and public search
- follow-up jobs, review routing, benchmarking, and adaptive learning

The goal is not to replace working logic. The goal is to move existing parsing, navigation, search, persistence, and evaluation capabilities behind a single graph runtime so the project stops paying the complexity tax of parallel execution models.

## What The Repository Looks Like Today

### Existing strengths worth preserving
- Deterministic extraction tools already exist in `analysis`, `adapters`, `navigation`, `normalization`, and `field_jobs`.
- The project already stores adaptive frontier telemetry, page observations, reward events, template profiles, and policy snapshots.
- The field-job system already proves that LangGraph can support chunk supervision, per-node telemetry, checkpointing, and runtime comparison.
- The web app already exposes benchmark, adaptive, and graph-run surfaces that can be extended instead of rebuilt.

### Current runtime split
| Area | Current implementation | Why it is costly |
| --- | --- | --- |
| Main crawl | `CrawlService` switches between `CrawlOrchestrator` and `AdaptiveCrawlOrchestrator` | Two separate graphs for the same business outcome |
| Adaptive learning | Only the adaptive runtime writes frontier memory, reward events, and policy snapshots | Learning is disconnected from the default crawl path |
| Search-backed contact discovery | Mostly lives inside `FieldJobEngine` after the main crawl | Search is a post-process instead of a first-class agent action |
| Field jobs | `legacy` engine plus `FieldJobGraphRuntime` plus supervisor graph | Runtime parity must be maintained twice |
| Control plane | `pipeline.py` remains the real orchestrator | LangGraph is used as a component, not the operating model |

### Root architectural problems
1. The crawl path is mode-switched at the service layer instead of graph-composed at the runtime layer.
2. Navigation, extraction, and enrichment are split across separate loops, so the system cannot adapt in one pass.
3. Search is treated as fallback labor after crawl completion instead of as a selectable action in the graph.
4. Telemetry schemas are useful but fragmented: adaptive sessions are rich, field-job graph runs are rich, legacy crawl runs are comparatively thin.
5. Benchmarking compares multiple runtimes that do not share the same execution substrate, which makes performance analysis noisier than it needs to be.

## North Star
Use LangGraph as the single control plane for every crawler workflow. All runtime modes become graph policies, not separate engines.

That means:

- one supervisor graph per batch or campaign
- one reusable source-crawl graph per source
- one reusable chapter-resolution and contact-enrichment subgraph
- one reusable field-job subgraph
- one shared event, checkpoint, and decision model across all graph runs
- one shared policy/memory layer for navigation, extraction, and enrichment

## Visual References

The engineer-focused visual companion set for this plan lives in [`docs/Diagrams`](../Diagrams/README.md):

- [`V3 System Overview`](../Diagrams/V3_SYSTEM_OVERVIEW.md)
- [`V3 Source Worker Graph`](../Diagrams/V3_SOURCE_WORKER_GRAPH.md)
- [`V3 Chapter Resolution Graph`](../Diagrams/V3_CHAPTER_RESOLUTION_GRAPH.md)
- [`V3 Distributed Control Plane`](../Diagrams/V3_DISTRIBUTED_CONTROL_PLANE.md)
- [`V3 Learning, Observability, And Review`](../Diagrams/V3_LEARNING_OBSERVABILITY_AND_REVIEW.md)

This document remains the written source of truth. The diagrams are its visual companion.

## Target End-To-End Architecture

```text
Batch Supervisor Graph
  -> Source Intake / Revalidation Subgraph
  -> Source Crawl Supervisor Subgraph
       -> Source Exploration Worker Graph
            -> Fetch page
            -> Analyze page
            -> Score candidate actions
            -> Execute action
            -> Extract chapter stubs or records
            -> Resolve chapter detail pages
            -> Enrich contacts from site
            -> Enrich contacts from search when needed
            -> Normalize and validate
            -> Persist chapter/provenance
            -> Emit follow-up tasks or review items
            -> Update memory, policy, and checkpoints
  -> Field Job Supervisor Subgraph
  -> Benchmark / Evaluation Subgraph
  -> Final reporting and summary persistence
```

## Core Design Principles

### 1. One graph runtime, many policies
Replace `legacy` versus `adaptive_*` with a single graph whose behavior changes through policy and guardrail settings.

Examples:
- `deterministic_strict`: prefer adapter extraction, no search unless required
- `assisted_live`: deterministic first, then search/navigation actions as needed
- `exploratory_train`: higher exploration, reward attribution enabled, checkpoint-rich
- `shadow_compare`: execute the chosen action plus a shadow action scorer for offline comparison

### 2. Keep tools deterministic, keep graphs orchestration-focused
Do not move HTML parsing into graph nodes. Reuse the current modules as tools:

- `analysis/*` for page inspection and classification
- `adapters/*` for record and stub extraction
- `orchestration/navigation.py` for chapter detail following and on-site contact discovery
- `search/client.py` for provider-aware public search
- `normalization/*` for canonicalization
- `db/repository.py` for persistence and telemetry
- `field_jobs.py` for candidate evaluation and resolution logic

The graph should decide when to invoke tools, in what order, with what budget, and what to do with the result.

### 3. Search becomes a first-class action
The new system should not wait until after crawl completion to discover contact information.

Search should be available as an action family inside the graph:
- `search_official_website`
- `search_contact_email`
- `search_instagram`
- `verify_search_candidate`
- `defer_to_followup_job`

This makes the crawler adaptive at the point of need instead of relying on a second system to repair missing data later.

### 4. Chapters are entities, not just extracted rows
Each chapter should have a graph-owned entity state that aggregates evidence from:

- national directory pages
- chapter detail pages
- chapter websites
- search results
- prior provenance
- prior field-job outcomes

This lets the system decide based on accumulated confidence rather than single-page extraction.

### 5. Memory and feedback must be shared
Template profiles, provider health, navigation outcomes, and resolution quality should all update the same memory layer so every future run benefits.

## Proposed Graph Stack

## A. Batch Supervisor Graph
Purpose: coordinate many sources, manage concurrency, and create run-level summaries.

Main responsibilities:
- load requested sources
- create graph run records
- fan out source crawl subgraphs
- apply global budgets and preflight checks
- collect per-source summaries
- trigger benchmark/eval comparisons when requested

This replaces `pipeline.py` as the true control plane.

## B. Source Intake / Revalidation Subgraph
Purpose: discover or verify the national source before crawling.

Actions:
- load verified source candidate
- evaluate source freshness and confidence
- run discovery search when confidence is low
- fetch and classify candidate source pages
- score source trust
- persist or update `verified_sources`
- route weak candidates to review

Reuse:
- `discovery.py`
- existing verified source repository methods

## C. Source Exploration Worker Graph
Purpose: crawl a national source adaptively and emit chapter entities plus evidence.

### Required state
- source metadata
- run and graph IDs
- frontier queue
- visited URLs
- current page payload
- page analysis and classification
- candidate actions and selected action
- chapter entity cache
- provider health snapshot
- per-source budget state
- template and policy memory snapshot
- review queue
- checkpoint metadata

### Candidate action families
- `extract_table`
- `extract_repeated_block`
- `extract_script_json`
- `extract_locator_api`
- `extract_stubs_only`
- `follow_chapter_detail`
- `expand_same_section_links`
- `expand_internal_links`
- `expand_map_children`
- `search_official_website`
- `search_contact_email`
- `search_instagram`
- `verify_candidate`
- `queue_followup`
- `review_branch`
- `stop_branch`

### Recommended node flow
1. `load_or_resume_source_session`
2. `seed_frontier`
3. `claim_frontier_item`
4. `fetch_page`
5. `analyze_page`
6. `build_entity_context`
7. `propose_actions`
8. `score_actions`
9. `select_action`
10. `execute_action`
11. `merge_evidence`
12. `validate_and_normalize`
13. `persist_progress`
14. `update_memory_and_rewards`
15. `evaluate_stop_conditions`
16. loop or finalize

### Why this is better than the current split
- It absorbs the current adaptive graph instead of competing with it.
- It absorbs the current deterministic crawl graph as a policy mode.
- It can choose direct extraction, navigation, or search in one session.
- It emits richer telemetry for every crawl, not only adaptive runs.

## D. Chapter Resolution And Contact Enrichment Subgraph
Purpose: resolve one chapter entity to a usable canonical record with confidence-aware contact evidence.

Inputs:
- chapter stub or partial record
- current source context
- prior provenance
- candidate websites, emails, social links

Actions:
- follow chapter detail page
- extract on-site email / Instagram / website
- search for official website
- search for contact email after website confidence threshold
- search for Instagram handle
- verify candidate host and school match
- create review item if evidence conflicts

Outputs:
- chapter updates
- provenance bundle
- field-state updates
- review items
- remaining follow-up tasks if unresolved

This subgraph should eventually own most of the logic currently living in `FieldJobEngine`.

## E. Field Job Supervisor Graph
Purpose: keep the existing queue-based enrichment model, but run it on the same runtime patterns and telemetry contracts as source crawling.

Keep:
- chunk supervision
- node instrumentation
- decision logging
- checkpoint durability

Refactor:
- make field jobs use the same chapter-resolution/contact-enrichment subgraph as live crawling
- stop maintaining separate decision logic in a legacy engine and a graph runtime

## F. Benchmark / Evaluation Graph
Purpose: run shadow, replay, and training workflows against the same graph substrate.

Actions:
- launch matched control/treatment runs
- capture graph decisions and outcome diffs
- run replay over stored observations
- emit KPI summaries and alerts
- save policy snapshots only from the unified runtime

This keeps the current benchmark dashboards but gives them cleaner apples-to-apples comparisons.

## Shared Memory And Decision Layers

### 1. Frontier memory
Keep and expand:
- `crawl_sessions`
- `crawl_frontier_items`
- `crawl_page_observations`
- `crawl_reward_events`
- `crawl_template_profiles`
- `crawl_policy_snapshots`

Add or standardize:
- graph run ID linking supervisor and worker runs
- action family taxonomy shared across crawl and field-job graphs
- provider health snapshots per run
- chapter entity evidence summary per run

### 2. Provider health memory
Search preflight should no longer live only as a gate before field jobs. Persist provider health as reusable graph context:

- provider
- run timestamp
- success rate
- anomaly/challenge rate
- fallback chain used
- recommended worker cap

This allows the source crawl graph to decide whether to search now, search later, or queue a job.

### 3. Entity evidence memory
Create a chapter-evidence view or table that stores per-run evidence bundles:

- chapter slug or entity key
- evidence source URL
- evidence type
- candidate value
- confidence
- verification status
- conflict flags

That gives the graph a better basis for multi-step decisions than raw extracted records alone.

## Recommended Persistence Model Changes

## Keep as-is
- canonical chapter tables
- provenance
- review items
- field jobs
- adaptive telemetry tables
- field-job graph telemetry tables

## Add next
1. `graph_runs`
Purpose: generic run record for any LangGraph workflow, linked to crawl runs, field-job batches, campaigns, or benchmarks.

2. `graph_run_events`
Purpose: generic node-level event stream so crawl graphs and field-job graphs share one observability surface.

3. `graph_run_checkpoints`
Purpose: generic persisted state snapshots with durability mode.

4. `chapter_evidence`
Purpose: normalized evidence ledger for website, email, Instagram, school-match, and status signals.

5. `provider_health_snapshots`
Purpose: searchable history of search and fetch health used by graph policies.

The existing field-job graph tables can either be generalized into these tables or mirrored until cutover is complete.

## Recommended Module Refactor

```text
services/crawler/src/fratfinder_crawler/
  graph_runtime/
    policies/
    state/
    nodes/
    subgraphs/
    telemetry/
  tools/
    fetch.py
    analyze.py
    navigate.py
    extract.py
    search.py
    verify.py
    normalize.py
    persist.py
    reward.py
  memory/
    template_profiles.py
    provider_health.py
    entity_evidence.py
  orchestration/
    legacy/            # temporary compatibility layer during migration
```

### Refactor mapping
- Move reusable node logic out of `orchestration/graph.py` and `orchestration/adaptive_graph.py` into shared tools and nodes.
- Convert `FieldJobEngine` into a library of chapter-resolution tools plus a thin compatibility adapter.
- Keep `pipeline.py` only as CLI/service entry glue that launches graph runs.

## End-To-End Contact Discovery Strategy

The adaptive crawler should follow this decision ladder for each chapter entity:

1. Trust deterministic evidence from the source page if confidence is high.
2. If the source page has a likely chapter detail page, follow it before searching.
3. If the detail page yields a likely official website, validate and trust it.
4. Only search for email after website confidence is sufficient, unless repeated provider failures trigger a policy escape hatch.
5. Search for Instagram using school and fraternity-aware query templates.
6. Verify candidate host affinity, school affinity, and social handle fit before writing.
7. If confidence is medium, persist evidence and route to review instead of mutating canonical fields directly.
8. If provider health is degraded, queue or defer instead of spinning in retries.

This is the key behavior change that will make the system both efficient and accurate.

## Runtime Modes In The New World

Runtime modes should become policy packs instead of separate codepaths.

| Mode | Behavior |
| --- | --- |
| `deterministic_strict` | adapter-first, no exploratory search unless required |
| `assisted_live` | adapter-first, navigation/search allowed under confidence guardrails |
| `shadow_compare` | live path persists, shadow path only records alternative decisions |
| `exploratory_train` | broader action exploration, dense reward logging, snapshot saving |

The old labels can be supported temporarily as aliases:
- `legacy` -> `deterministic_strict`
- `adaptive_shadow` -> `shadow_compare`
- `adaptive_assisted` -> `assisted_live`
- `adaptive_primary` -> `exploratory_train` or future production variant

## Migration Plan

## Phase 1: Unify contracts and naming
- Introduce a generic graph run contract and action taxonomy.
- Rename current runtime modes into policy packs while keeping CLI aliases.
- Extract reusable node/tool logic from both crawl orchestrators.

Acceptance:
- no feature loss
- existing commands still work
- benchmark and dashboard routes still resolve

## Phase 2: Build unified source crawl graph in shadow mode
- Create the new source exploration worker graph.
- Run it alongside current `CrawlOrchestrator` for selected sources.
- Persist graph telemetry without using it as the primary writer yet.

Acceptance:
- same or better record yield on core fixtures
- no spike in review rates
- page observation telemetry available for all shadow runs

## Phase 3: Move adaptive logic into the unified graph
- Port frontier selection, reward scoring, template updates, and stop conditions.
- Remove `AdaptiveCrawlOrchestrator` as a separate runtime.

Acceptance:
- adaptive replay and policy snapshot flows run off the new graph
- benchmark comparisons no longer compare different orchestration implementations

## Phase 4: Convert chapter enrichment into a reusable subgraph
- Wrap current field-job resolution logic as graph actions.
- Allow live crawl sessions to invoke search and verification actions inline.
- Keep queue-backed field jobs for deferred work.

Acceptance:
- contact coverage improves without large retry-waste growth
- field-job decisions and live-crawl decisions use the same candidate evaluation rules

## Phase 5: Generalize graph observability
- Introduce generic graph run tables or a compatibility view layer.
- Expose source-crawl graph runs in the web app just like field-job graph runs.

Acceptance:
- operators can inspect crawl graph runs, decisions, checkpoints, and alerts from the UI

## Phase 6: Retire legacy runtime
- Remove `CrawlOrchestrator` as an independent implementation.
- Keep only deterministic policy packs inside the unified graph.
- Reduce `pipeline.py` to launch, supervise, and summarize graph runs.

Acceptance:
- one production crawl runtime remains
- legacy behavior is reproducible through configuration, not separate code

## Practical First Refactors

These are the highest-leverage implementation moves:

1. Create a shared action taxonomy and policy-pack config.
2. Extract common node tools from `graph.py` and `adaptive_graph.py`.
3. Add inline search/verification actions to the source crawl graph.
4. Reuse the field-job graph telemetry model for source-crawl graph runs.
5. Introduce chapter-evidence persistence before attempting full runtime cutover.

## Success Criteria

The rebuild is successful when the project has:

- one LangGraph runtime for crawl and enrichment
- one policy layer instead of separate legacy and adaptive engines
- one observability model for graph execution
- adaptive navigation and search as first-class graph actions
- higher contact coverage with lower retry waste
- cleaner benchmark comparisons because control and treatment share the same graph substrate

## Summary
The repository is already close to the right ingredients. The missing step is architectural consolidation.

The best target is not "replace the legacy crawler with the current adaptive crawler." The best target is:

- keep deterministic extraction
- keep adaptive frontier learning
- keep search-backed enrichment
- keep graph telemetry and checkpoints
- compose them into one LangGraph operating model

That design gives you an adaptive crawler that can navigate, search, verify, and learn inside one end-to-end system instead of coordinating multiple runtimes that solve different slices of the same problem.


