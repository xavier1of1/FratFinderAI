# FratFinderAI Portfolio Knowledge Base

Date: 2026-04-14

Purpose: This document is a comprehensive technical and product audit of the FratFinderAI codebase. It is written as a reusable knowledge base for resume bullets, interview stories, portfolio writeups, and LinkedIn content.

Scope notes:
- This report is based on the repository state and documentation available on 2026-04-14.
- It reflects the implemented system, not just the intended architecture.
- Where the code is clearer than the docs, implementation was treated as the source of truth.
- Where rationale is not explicitly documented, it is marked as an inference from the architecture and code.

## Executive Summary

FratFinderAI is best understood as a precision-first data acquisition, normalization, and operations platform for fraternity chapter intelligence. It is not "just a crawler." It combines source discovery, graph-based crawl orchestration, canonical data modeling, provenance preservation, search-backed contact enrichment, operator review workflows, and benchmark/evaluation infrastructure into a single system.

The core technical challenge is not fetching HTML. The hard part is producing trustworthy chapter-level records and chapter-specific contact data from noisy, inconsistent, partially broken public-web sources without silently writing false positives into the canonical database.

What makes the project strong:
- It models business truth explicitly instead of relying on ad hoc scraper output.
- It preserves provenance and routes ambiguity into review instead of over-automating.
- It uses durable queue semantics and worker leases in Postgres instead of fragile in-memory scheduling.
- It has graph-based orchestration for request execution, field-job execution, and adaptive crawling.
- It includes evaluation, campaign, and benchmarking infrastructure, which is unusual for projects at this scale.

What is still transitional:
- Some orchestration-adjacent logic still lives in the web app and shells out to Python subprocesses.
- CI is real but incomplete for production confidence.
- There is no obvious authentication or RBAC layer, so the product reads more like an internal operator tool than a hardened multi-tenant SaaS.
- The config surface is powerful but sprawling, which increases operational complexity.

## 1. Project Overview

### What the system does

FratFinderAI discovers fraternity chapter data from national fraternity websites and related public sources, normalizes that data into a canonical relational model, preserves evidence for every extracted field, enriches missing contact information, and exposes the result through an operator dashboard and API surface.

In practical terms, the system:
- discovers or validates the correct national source for a fraternity
- crawls chapter directories and detail pages
- extracts chapter entities and chapter-level signals
- normalizes those entities into canonical chapter records
- records provenance for every accepted field
- creates review items for ambiguous or low-confidence cases
- queues follow-up work for missing website, email, or Instagram data
- runs repair, benchmark, and campaign workflows on top of the same operational dataset

### Core problem it solves

The system solves the problem of building and maintaining a reliable chapter directory for NIC fraternities when public web data is:
- inconsistent across national organizations
- split across national sites, chapter sites, school directories, and social pages
- often stale, incomplete, or historically polluted
- vulnerable to false positives such as historical timelines, rankings, percentages, faculty departments, or generic national contact pages

### Target users and use cases

Primary users:
- internal operators managing fraternity/source intake
- data operations users monitoring crawl runs, field-job queues, repair lanes, and review items
- engineers evaluating crawl quality, throughput, and runtime experiments

Likely downstream users and use cases inferred from the product shape:
- CRM or outreach dataset generation
- national organization intelligence and coverage tracking
- chapter contact enrichment for internal operations
- benchmarking alternative runtime strategies and policy improvements

### Why this problem is non-trivial

This is a non-trivial systems problem because the project must solve both entity truth and contact truth.

Entity truth is hard because:
- the same fraternity may appear under multiple aliases
- one source may contain active chapters, inactive chapters, historical references, chapter house pages, or unrelated campus content
- chapter names are inconsistent and sometimes omit the university or use local nicknames
- sources vary between structured directories, semi-structured CMS pages, PDFs, outbound links, or generic landing pages

Contact truth is harder because:
- a valid contact field must be chapter-specific, not merely fraternity-specific
- national websites often provide generic headquarters contact info that is true but wrong for the chapter
- school directories may mention a chapter without providing direct contact details
- social and email signals are weak and often require school-affiliation validation
- search providers can degrade, rate-limit, or return high-noise results

## 2. Product Positioning

FratFinderAI behaves like a vertical data platform for one difficult entity domain.

It is productively closer to:
- a source-aware entity resolution system
- a workflow-driven ops platform
- a precision-focused enrichment engine

It is not primarily:
- a generic web scraper
- an LLM chat product
- a generic RAG system
- a commodity ETL pipeline

The product thesis appears to be:
- trust source-native and official data first
- write automatically only when confidence is high
- preserve evidence for everything
- treat ambiguity as an operational workflow, not a hidden failure

That thesis is a meaningful product strength because it aligns the technical architecture with downstream trust.

## 3. System Architecture

### High-level architecture

At a high level, the platform has five major layers:

1. Operator and API layer
- Next.js web app
- operator workflows for intake, runs, chapters, review, benchmarks, campaigns, and agent ops

2. Orchestration and worker layer
- request worker for crawl requests
- field-job supervisor and field-job graph runtime
- chapter repair lane
- evaluation worker for benchmark and campaign jobs

3. Crawl and enrichment execution layer
- deterministic crawl graph
- adaptive crawl graph with policy-driven action selection
- search-backed field-job enrichment engine

4. Storage and state layer
- Postgres/Supabase as system of record
- queue tables, graph run tables, event tables, checkpoint tables, evidence tables, and canonical business tables

5. Contracts and typing layer
- shared schemas and typed models across TypeScript and Python
- canonical data contracts, review payloads, and field-job payloads

### End-to-end architecture diagram

```text
Operator UI / API
    |
    v
fraternity_crawl_requests
    |
    v
Request Worker (lease-based)
    |
    +--> Source recovery / source quality checks
    |
    +--> Crawl runtime
    |      |
    |      +--> Legacy deterministic crawl graph
    |      |
    |      +--> Adaptive crawl graph with frontier, policy, rewards
    |
    +--> Canonical writes
    |      |
    |      +--> fraternities
    |      +--> sources / verified_sources / national_profiles
    |      +--> chapters
    |      +--> chapter_provenance
    |      +--> review_items
    |      +--> provisional_chapters
    |
    +--> Queue follow-up work
           |
           +--> field_jobs
           +--> chapter_repair_jobs
           +--> evaluation_jobs

Field-job worker lane
    |
    v
FieldJobSupervisorGraphRuntime
    |
    v
FieldJobGraphRuntime
    |
    v
chapter updates / evidence / field-job graph tables

Evaluation lane
    |
    v
benchmark_runs / campaign_runs / alerts / runtime comparison telemetry
```

### Detailed component interaction

#### Web layer

The web app provides:
- dashboard pages for chapters, benchmarks, campaigns, review items, runs, fraternity intake, nationals, and agent ops
- API routes for querying operational state and creating benchmark/campaign/request work
- repository modules that access Postgres directly using `pg`
- evaluation orchestration logic and job claiming for certain lanes

Important architectural nuance:
- The web app no longer appears to own durable scheduling in the older "GET route does work" style.
- Read-only route tests explicitly verify that GET handlers do not mutate runtime state.
- However, some orchestration-adjacent logic still exists in `apps/web/src/lib`, and some workflows still shell out to the Python CLI.

This means the architecture is materially improved, but not fully converged into a single clean worker boundary.

#### Python crawler service

The Python service is the execution-heavy layer responsible for:
- HTTP fetching and retries
- HTML parsing and extraction
- source discovery
- chapter normalization and validation
- crawl orchestration
- field-job enrichment
- adaptive policy runtime and reward accounting

This split is sensible because Python is a better fit for:
- parsing-heavy and heuristic-heavy logic
- flexible data modeling with Pydantic/dataclasses
- graph/runtime experimentation around crawling and rewards

#### Storage and queue layer

Postgres is doing double duty as:
- canonical system of record
- durable queue backend
- audit/event store
- worker coordination surface

This is an intentional and pragmatic architecture choice.

Instead of introducing Kafka, SQS, Redis queues, or Celery early, the system uses:
- typed queue tables
- leases and heartbeats
- worker registration
- `FOR UPDATE SKIP LOCKED` claiming
- persisted graph events and checkpoints

This keeps the architecture understandable and highly debuggable, at the cost of increased database responsibility.

#### Evaluation and experimentation layer

A particularly strong aspect of the project is the first-class evaluation infrastructure:
- benchmark runs
- campaign runs
- strict/shared isolation modes
- drift alerts
- runtime comparison analytics
- adaptive policy replay, training, diffing, and reporting

This elevates the project beyond a one-off scraper into an engineering system that can evaluate changes quantitatively.

## 4. Data Flow Through the System

### Intake and source acquisition

The operator enters or selects a fraternity.

The system then:
- attempts to discover a likely national source URL
- scores discovered sources
- can prefer verified sources when confidence is high
- stores source quality metadata

### Crawl request lifecycle

A crawl request is created and persisted in `fraternity_crawl_requests`.

The request worker:
- claims due requests via lease-based worker ownership
- loads request context and source quality state
- optionally performs source recovery if the current source is weak or blocked
- launches crawl execution
- tracks graph runs, checkpoints, and events
- enters enrichment cycles after crawl persistence
- evaluates provisional promotion paths
- finalizes business status

### Crawl execution lifecycle

The crawl runtime operates in one of two broad modes:
- deterministic legacy graph for structured extraction
- adaptive graph for frontier-based exploration and policy-guided action choice

The crawl logic:
- fetches pages
- analyzes page structure
- classifies source/page types
- extracts candidate chapter stubs or records
- validates and normalizes records
- persists canonical entities and evidence
- spawns follow-up field jobs for unresolved contact fields

### Enrichment lifecycle

Missing contact fields become `field_jobs`.

The field-job supervisor:
- claims queued jobs
- chunks them for bounded parallel processing
- runs a graph-native contact resolution path
- enforces dependency gates and degraded-mode behavior
- records processed, requeued, terminal failure, and fallback telemetry

The field-job graph:
- loads job context
- evaluates preconditions
- resolves the job using provenance, chapter website, or search
- decides the outcome
- persists contact updates and graph telemetry

### Repair and provisional lifecycle

If a chapter record is ambiguous, partial, or not trusted enough for direct canonical promotion:
- it can become a provisional chapter
- it can create a repair job
- it can create a review item

This creates a controlled middle ground between "auto-accept" and "discard."

### Evaluation lifecycle

Benchmarks and campaigns are modeled as durable jobs rather than ad hoc scripts.

The evaluation lane:
- claims `evaluation_jobs`
- runs benchmark or campaign execution under lease ownership
- persists run metadata and telemetry
- emits alerts and comparison output

That is a strong architectural choice because it makes experiments reproducible and inspectable.

## 5. Distributed and Asynchronous Processes

The system is asynchronous and multi-process, but not broker-driven in the usual cloud-native sense.

It uses a database-backed distributed workflow pattern:
- multiple worker lanes
- durable queues
- lease-based claims
- heartbeat updates
- graph-run persistence
- event and checkpoint storage

Current asynchronous processes include:
- request workers for crawl requests
- field-job workers for contact enrichment
- repair lane workers
- evaluation workers for benchmarks and campaigns
- optional adaptive training and replay commands

Within a worker, additional parallelism exists:
- field-job supervisor uses chunked execution with a thread pool
- search and enrichment behavior is bounded by worker caps and provider pacing

Tradeoff:
- This is easier to debug than a broker-heavy architecture.
- It can become database-write-heavy if event/checkpoint volumes grow too quickly.

## 6. Technical Stack and Justification

| Layer | Choice | Why it fits | Main tradeoff |
|---|---|---|---|
| Operator UI and API | Next.js 14 + React 18 + TypeScript | Fast internal-tool iteration, typed server routes, shared domain types, simple full-stack deployment surface | Some backend orchestration still lives in app-lib code, which muddies service boundaries |
| Crawler and enrichment runtime | Python 3.11 | Better ergonomics for parsing, heuristics, graph experimentation, CLI workflows, and data-heavy logic | Cross-language integration introduces subprocess seams and duplicated type concerns |
| Database | Postgres / Supabase | Strong transactional semantics, relational modeling, unique constraints, queue claims with `SKIP LOCKED`, auditability | DB becomes both source of truth and coordination bus, which can create write-pressure bottlenecks |
| Graph orchestration | LangGraph | Explicit state, node boundaries, checkpoints, durable transitions, better reasoning visibility than ad hoc loops | More tables, more persistence, and more complexity than a simple loop for trivial paths |
| Validation and settings | Pydantic / pydantic-settings / Zod / JSON Schema | Strong schema enforcement across Python and TypeScript | Requires discipline to keep models aligned across languages |
| HTML parsing | Requests + BeautifulSoup | Fast, inexpensive, readable, and enough for many directory sites | Weak on highly dynamic JS-only sites compared with browser automation |
| Testing | Pytest + Vitest | Good fit for language-specific unit tests and route behavior tests | Integration testing across services is still thin |
| Local infra | Docker Compose + Adminer | Easy reproducible local database setup and inspection | Not a production deployment story by itself |

### Why these choices make sense

The stack shows a pragmatic split:
- TypeScript for operator and API ergonomics
- Python for data extraction and orchestration complexity
- Postgres for both relational truth and operational durability

That is a strong choice for a portfolio project because it reflects product-driven architecture rather than tech-fashion choices.

### Tradeoffs versus obvious alternatives

Compared to a browser-first scraping stack:
- Current design is cheaper, faster, and more deterministic for directory-style content.
- It likely struggles more on JS-heavy sites or anti-bot-heavy flows.

Compared to Celery or message-broker queues:
- Current design is easier to inspect end-to-end from SQL.
- It places more coordination load on Postgres.

Compared to an ORM-heavy design:
- Current repository pattern keeps SQL explicit and domain-aware.
- It creates more hand-written query code to maintain.

Compared to a pure LLM agent approach:
- Current design is more controllable, auditable, and safer for canonical writes.
- It gives up some flexibility on edge cases unless explicit fallback logic is added.

## 7. Core Features and Functionality

### 7.1 Source discovery and source quality management

The system does not assume the source URL is already known or trustworthy.

It includes:
- verified source registry support
- search-based source discovery
- source scoring and optimization
- blocked-host and weak-host heuristics
- fraternity alias support
- source recovery paths when current source quality is weak

This matters because national fraternity data quality starts with selecting the correct root source, not merely scraping whatever URL was initially provided.

### 7.2 Crawler design and strategy

The crawler supports multiple execution styles:

Deterministic crawl path:
- designed for relatively structured directory extraction
- uses page analysis, extraction strategies, source classification, stub following, and normalization

Adaptive crawl path:
- uses a frontier of candidate pages
- scores or chooses actions over page observations
- persists crawl sessions, frontier items, observations, and reward events
- supports replay and training/eval loops

This dual-mode design is a real strength:
- deterministic mode gives safety and predictability
- adaptive mode provides a structured way to explore harder source layouts

### 7.3 Extraction and parsing logic

Extraction is not a naive text scrape.

The system includes logic for:
- page structure analysis
- directory layout profiling
- embedded-data detection
- source classification
- extraction-plan selection
- stub extraction and follow-up page traversal
- candidate sanitization

This is exactly what serious extraction systems require: explicit handling of page type before entity extraction.

### 7.4 Deduplication and normalization

The platform has a strong normalization layer with:
- canonical chapter records
- slug-based identity
- optional external IDs when present
- validation of chapter candidates
- invalid-entity filtering for junk rows
- partial unique constraints on external IDs
- provenance capture for accepted fields

Normalization is especially important in this domain because raw source pages often mix:
- active and inactive chapters
- chapter names and school names in different formats
- repeated links or alias forms
- non-entity rows such as rankings, timelines, percentages, or department entries

### 7.5 Enrichment pipeline

When canonical chapters are missing website, email, or Instagram fields, the system does not blindly search the web.

It performs a constrained enrichment workflow:
- check chapter evidence and provenance first
- prefer official chapter website or school-affiliated pages
- use search only when needed
- bound search depth, pages per job, and query count
- apply page-scope and contact-specificity logic
- reject wrong-organization and generic-national false positives
- defer rather than force progress when prerequisites are missing

This is a very mature enrichment philosophy.

### 7.6 Review and validation workflow

Uncertainty is a first-class workflow state.

The system supports:
- `review_items` for ambiguous decisions
- provisional chapters as pre-canonical entities
- chapter repair jobs
- evidence-driven operator inspection
- audit logs for review transitions

This is one of the strongest product design choices in the repository because it acknowledges that extraction truth has an operations component.

### 7.7 Logging and observability

Observability is deeper than ordinary scraper logs.

The platform tracks:
- crawl runs
- request graph runs, events, and checkpoints
- field-job graph runs, events, checkpoints, and decisions
- benchmark runs and shadow diffs
- campaign runs and provider-health telemetry
- ops alerts
- worker registration and leases
- queue state and repair state

This is not just for debugging. It enables product-level reasoning about quality, drift, throughput, and runtime safety.

### 7.8 Error handling and retries

The project contains multiple layers of defensive behavior:
- HTTP retries and browser-like headers
- search-provider fallback and circuit breakers
- provider-specific pacing
- negative-result cooldowns
- dependency backoff for jobs that should wait
- degraded-mode worker caps
- stale-run reconciliation
- runtime fallbacks and explicit business-status tracking

The system also distinguishes runtime completion from business success. That is a subtle but important design choice.

## 8. AI and Agentic Components

### What is genuinely agentic in this project

The project is agentic mainly through graph-based orchestration and adaptive policy behavior, not through a conversational LLM agent controlling everything.

Current agentic layers:
- request lifecycle orchestration via LangGraph
- field-job lifecycle orchestration via LangGraph
- adaptive crawl orchestration via frontier state, action scoring, and rewards

This is a better fit than a free-form agent because the domain needs:
- explicit state transitions
- deterministic checkpoints
- auditable decisions
- safe write boundaries

### Where AI is used today

AI usage appears intentionally constrained:
- optional LLM classifier and extractor modules exist
- LLM output is schema-bounded
- LLM usage is configuration-gated and not the primary extraction path

That is a good design choice for a system that writes into canonical records.

### Role of agents

The "agents" in this system are better described as graph-backed workers with domain tools than as autonomous chat agents.

Examples:
- request agent decides how to progress a crawl request through discovery, crawl, enrichment, provisional evaluation, and finalization
- field-job agent decides whether a contact field can be resolved, deferred, rejected, or escalated
- adaptive crawl agent chooses next crawl actions from a frontier using learned priors and reward signals

### RAG versus tool-calling

This project is not a RAG system.

There is no evidence of:
- embeddings
- vector search
- retrieval over an unstructured corpus for synthesis

The problem is not "answer a question from documents." The problem is "discover, validate, and write structured chapter records from public web evidence."

That makes tool- and workflow-centric orchestration the right design choice:
- HTTP fetch
- search provider query
- extraction
- validation
- persistence
- review routing

### Reinforcement learning and adaptive behavior

The adaptive runtime is the most advanced experimental part of the system.

It includes:
- action priors for navigation and extraction
- epsilon-greedy exploration
- reward weighting
- delayed credit assignment
- replay windows
- policy snapshots and diffs
- training/eval loops
- balanced KPI scoring

This is sophisticated infrastructure, but it should be described carefully in portfolio language:
- the system contains reinforcement-learning-inspired adaptive policy infrastructure
- the architecture is designed to measure and compare policy changes
- that is stronger and more honest than claiming "full RL production autonomy"

### Limitations and edge cases of the AI/agentic approach

- Adaptive behavior is only as good as the telemetry, reward design, and evaluation isolation.
- Graph persistence improves auditability but increases write amplification.
- Structured heuristics still do most of the heavy lifting for contact truth.
- Search-provider health can dominate outcomes more than policy quality.
- The strongest value today is safe orchestration and measurable experimentation, not magical autonomy.

## 9. Data Model and Storage Design

### Core business entities

The relational model includes:
- `fraternities`
- `sources`
- `verified_sources`
- `national_profiles`
- `chapters`
- `chapter_provenance`
- `review_items`
- `review_item_audit_logs`

This establishes a clean separation between:
- source entities
- canonical business entities
- evidence and review artifacts

### Operational entities

The platform also models runtime and queue state explicitly:
- `field_jobs`
- `fraternity_crawl_requests`
- `request_graph_runs`
- `request_graph_events`
- `request_graph_checkpoints`
- `field_job_graph_runs`
- `field_job_graph_events`
- `field_job_graph_checkpoints`
- `field_job_graph_decisions`
- `chapter_repair_jobs`
- `evaluation_jobs`
- `worker_processes`
- `ops_alerts`
- `benchmark_runs`
- `campaign_runs`

This is a strong sign of architecture maturity. Runtime state is not hidden in log files or ephemeral memory.

### Handling ambiguity and duplicates

The project uses multiple mechanisms to prevent dirty canonical writes:
- unique constraints on fraternity slugs, source slugs, and chapter slugs within a fraternity
- partial unique index on chapter external IDs when present
- provisional chapter workflow
- review items for ambiguity
- canonical versus provisional separation
- field-level provenance
- candidate sanitization and invalid-entity filtering

### Why Postgres is the right choice here

Postgres is a strong fit because the system needs:
- transactional upserts
- strong uniqueness guarantees
- relational joins across evidence, canonical rows, queue state, and runtime telemetry
- `SKIP LOCKED` work claiming
- easy operator inspection through SQL and admin tools

This is a better fit than a document store because the domain is highly relational and audit-heavy.

## 10. Scalability and Performance

### How the system handles larger-scale crawling

The system addresses scale through bounded concurrency and durable work partitioning rather than through massive distributed compute.

Key mechanisms:
- request queue batching
- worker leases and heartbeats
- field-job worker caps
- chunked field-job supervisor execution
- query-count caps per job
- max pages and hop limits in crawl graphs
- provider pacing and degraded-mode search profiles
- negative-result cooldowns and dependency waits

This is a sensible scaling strategy for a precision-first system that values correctness over brute-force throughput.

### Likely bottlenecks

The main bottlenecks are not CPU-bound parsing.

They are more likely to be:
- external search-provider health and rate limits
- network variability on public sites
- graph-event and checkpoint write amplification
- DB contention if queue and telemetry volumes grow simultaneously
- cross-language subprocess overhead between web/evaluation workflows and Python CLI execution
- source-specific layout irregularity that defeats reuse and forces extra page fetches

### Concurrency and rate limiting strategy

Concurrency is bounded deliberately:
- queue claims are lease-based
- workers use `FOR UPDATE SKIP LOCKED`
- field-job concurrency is capped by config
- degraded search mode reduces worker count and query breadth
- per-provider pacing and global min request intervals reduce challenge risk

This demonstrates mature thinking: the system scales by staying healthy, not by maximizing raw request fanout.

### Caching and optimization approaches

Observed optimization patterns include:
- verified source preference to avoid repeated source discovery
- search result caching
- negative-result cooldowns to prevent hot-looping
- frontier memory and template-aware adaptive behavior
- chapter search and validity analytics inside progress models
- typed queue states to avoid repeatedly reprocessing invalid or blocked work

### Performance assessment

From an engineering perspective, the system appears optimized for:
- explainable progress
- bounded retries
- queue hygiene
- operator observability

It is less optimized for:
- minimum possible database writes
- minimum cross-process orchestration overhead
- cloud-elastic horizontal scale

That tradeoff is rational for the product stage shown in the repository.

## 11. Reliability and Edge Cases

### Common failure scenarios the system explicitly handles

- source URL is weak, blocked, or incorrect
- source is structured differently from the expected pattern
- extracted row is historical or non-entity junk
- chapter looks real but cannot be safely promoted
- website/email/Instagram field exists but is generic or wrong-organization
- provider collapses or returns low-signal results mid-batch
- email should not run before confident website discovery
- repair work must finish before a field job can safely continue
- stale runs or stale queue claims must be reconciled
- public web returns incomplete or inconsistent contact signals

### How the system recovers

Recovery is handled by a combination of:
- deferral instead of forced failure
- queue-state typing
- dependency gating
- chapter repair jobs
- provisional chapter flow
- review routing
- preflight and degraded search mode
- stale-run reconciliation
- explicit alerting

This makes the system resilient in the important sense: it preserves queue health and data integrity when it cannot make safe progress.

### Data integrity guarantees

The system has several integrity-oriented design patterns:
- direct writes only for high-confidence outcomes
- provenance for accepted fields
- review workflow for ambiguous outcomes
- unique constraints to prevent duplicate canonical rows
- typed runtime and queue state instead of overloading one status field
- read-only route tests to prevent accidental control-plane mutations

This is exactly the kind of architecture that supports a credible claim of "production-style data correctness thinking."

## 12. Security and Compliance Considerations

### What the codebase handles well

- secrets are environment-based rather than hard-coded
- API keys for search providers and OpenAI are configuration-driven
- the crawler uses bounded, public-web-oriented retrieval rather than invasive authenticated scraping flows
- rate limiting, pacing, and degraded-mode logic reduce abusive traffic patterns

### What appears underdeveloped

There is no obvious first-class authentication or RBAC layer in the web app.

Inference:
- This likely operates today as an internal operator tool or local-first system rather than a public-facing multi-user product.

Other production hardening gaps:
- no obvious secrets manager integration
- no visible audit policy around operator identity
- no clear tenant isolation model
- no clear privacy retention policy for collected contact data

### Ethical considerations of scraping

The project is on stronger footing than many scraping systems because it is:
- source-aware
- provenance-preserving
- cautious about contact specificity
- explicit about uncertainty

That said, a production version should still formalize:
- robots and terms-of-service policy
- scrape rate governance
- retention rules for public contact data
- operator guidance for handling ambiguous or outdated chapter info

## 13. Development and DevOps

### Repository structure

The repository is cleanly separated at the top level:
- `apps/web` for operator UI and server routes
- `services/crawler` for Python runtime logic
- `packages/contracts` for shared schema contracts
- `infra` for Docker and SQL migrations
- `tests/integration` for planned cross-service tests
- `docs` for architecture, reports, and diagrams

This is a strong monorepo layout for a cross-language internal platform.

### Testing strategy

Strengths:
- substantial Python test coverage surface
- unit tests for search, discovery, field jobs, graph runtimes, adaptive runtime, and pipeline workers
- TypeScript tests for source-selection logic and API read-only behavior
- contract tests for shared schemas

Important current reality:
- crawler tests are much deeper than web tests
- integration tests are planned but not meaningfully implemented yet
- there is explicit test coverage protecting against read routes mutating system state

### Deployment approach

Current deployment posture is local/dev oriented:
- Docker Compose for Postgres, Adminer, web, and crawler
- environment-file configuration
- simple Dockerfiles

This is good for iteration and reproducibility, but not yet a mature cloud runtime story.

### CI/CD maturity

CI currently runs:
- pnpm install
- lint
- typecheck
- contract tests
- crawler pytest with coverage threshold

CI currently does not appear to run:
- web build verification
- broader web test coverage beyond what `pnpm test:web` would cover
- integration tests
- end-to-end runtime smoke tests across web, crawler, and Postgres

So the project has meaningful CI, but not production-grade release confidence yet.

## 14. Key Engineering Challenges and Solutions

### Challenge 1: Extracting canonical entities from inconsistent national sources

Why it is hard:
- sites differ wildly in structure
- some are clean directories, others are generic CMS pages
- the same page can mix real chapters with junk rows

How the project addresses it:
- page analysis and source classification
- extraction-strategy selection
- normalization and validity filtering
- source recovery and verified-source preference

### Challenge 2: Preventing false-positive contact writes

Why it is hard:
- contact signals are easy to find but easy to misattribute
- national-level contact details can look authoritative but be wrong for chapter-level records

How the project addresses it:
- page scope model
- contact specificity model
- school affiliation and official-domain checks
- evidence-first resolution and review routing

This is arguably the most important engineering challenge in the whole system.

### Challenge 3: Queue thrash and wasted work

Why it is hard:
- missing prerequisites can cause jobs to bounce repeatedly
- provider failure can turn active search into expensive churn
- stale or invalid historical rows can poison throughput

How the project addresses it:
- typed queue states
- dependency deferral
- degraded-mode search behavior
- invalid-entity filtering
- repair lanes
- queue reconciliation logic

### Challenge 4: Safely evolving runtime behavior

Why it is hard:
- changes to crawl or enrichment policy can improve one KPI while harming another
- production-like evaluation is difficult without durable telemetry and comparison tooling

How the project addresses it:
- benchmark runs
- campaign runs
- shadow/runtime comparison support
- adaptive replay, train/eval, and policy diff tooling
- drift alerts

This is a standout feature from a hiring-manager perspective because it shows engineering discipline around change management.

### Challenge 5: Converging architecture across web and crawler boundaries

Why it is hard:
- the system spans TypeScript UI/API code and Python execution code
- queue ownership and operational control can drift into the wrong layer

How the project addresses it:
- backend-owned evaluation jobs
- worker leases
- read-only route protections
- repository modules and shared contracts

What remains:
- some web-owned orchestration seams still exist
- Python subprocess bridging is still part of the architecture

## 15. Business Impact and Value

### Why this system matters

The system creates leverage in a domain where manual work is expensive and error-prone.

Instead of manually:
- finding national source pages
- checking chapter directories
- validating chapter identity
- researching missing websites, emails, and Instagram pages
- documenting evidence
- rechecking ambiguous or stale records

the platform turns that into:
- a repeatable ingestion pipeline
- a durable queue system
- an operator review surface
- an experimentation environment for improving quality and throughput

### How it creates leverage

- reduces manual source-discovery effort
- standardizes canonical chapter records across inconsistent sources
- preserves field-level provenance for trust and auditability
- keeps uncertainty visible instead of silently polluting data
- supports operational tuning through benchmarks and campaigns
- lays groundwork for measured adaptive improvements rather than one-off heuristic changes

### Potential real-world applications

- internal directory maintenance for fraternity-related services
- outreach/contact dataset creation
- chapter-status and coverage intelligence
- operational analytics around active/inactive chapter networks
- broader adaptation into other chaptered or branch-based organizations with similar data patterns

## 16. Future Improvements

### Highest-priority technical improvements

1. Harden service boundaries
- replace remaining web-to-Python subprocess orchestration with cleaner worker RPC or queue-driven boundaries
- reduce orchestration logic living inside `apps/web/src/lib`

2. Add authentication and authorization
- introduce operator identity, RBAC, and auditability
- make the dashboard safe for real multi-user deployment

3. Expand integration testing
- add end-to-end tests for web + crawler + Postgres
- verify idempotency, queue claims, and canonical write integrity across services

4. Reduce telemetry write amplification
- keep graph observability, but rationalize event/checkpoint frequency
- consider summarized projections or batched writes for high-volume paths

5. Rationalize configuration
- group the large env surface into clearer profiles
- reduce risk of invalid or conflicting runtime tuning

### Product and platform improvements

6. Add operator-grade access patterns
- role-based review queues
- user attribution on review/repair actions
- richer evidence drill-down and explainability summaries

7. Add browser fallback for hard sites
- reserve headless-browser execution for sources that defeat static HTTP retrieval
- keep it as an exception path rather than the default

8. Improve analytical reporting
- export benchmark and campaign telemetry into analysis-friendly views
- build longitudinal reporting over drift, provider health, and queue quality

9. Converge architecture docs
- align diagrams and docs with the current implemented queue architecture
- reduce confusion between target-state and runtime-state descriptions

10. Strengthen production deployment story
- production Docker images
- environment promotion strategy
- managed worker execution and restart policy
- database migration/release discipline

## 17. Hiring Manager Takeaways

If this project were being reviewed as a portfolio item, the strongest signals would be:

- The candidate did not build a toy scraper. They built a domain-aware data platform with canonical modeling, provenance, review workflows, and operational telemetry.
- The candidate understands that "AI" is not the same thing as throwing an LLM at a problem. The system uses deterministic workflows, graph state, and bounded adaptive behavior where reliability matters.
- The candidate thinks in terms of production failure modes: queue thrash, invalid history, provider collapse, stale claims, low-confidence writes, and isolation for evaluation.
- The candidate can work across frontend, backend, data modeling, orchestration, and DevOps concerns in one coherent system.
- The candidate understands how to preserve business correctness while still pushing toward automation and learning-based improvement.

Weaknesses a hiring manager would also notice:

- production hardening is not finished
- service boundary convergence is incomplete
- auth/RBAC is not yet apparent
- cross-service integration testing is still light

Those weaknesses do not negate the project. They actually make the portfolio story more credible if they are presented honestly as the next phase of maturation.

## 18. Resume and Interview Angle

The most defensible framing for resume/interview use is:

"Built a source-aware data acquisition and enrichment platform for fraternity chapter intelligence, combining graph-based crawl orchestration, canonical data modeling, evidence-preserving review workflows, and durable queue-based contact enrichment over a Postgres-backed worker architecture."

Supporting themes that are especially credible:
- graph-based orchestration with durable state and checkpoints
- precision-first contact resolution and provenance capture
- benchmark and campaign infrastructure for controlled runtime experimentation
- queue and worker reliability improvements using typed state, leases, and degraded-mode controls
- cross-language system design spanning Next.js/TypeScript and Python

Claims to avoid or phrase carefully:
- Avoid saying the system is "fully autonomous AI" or "production-scale RL" unless you can back that up with formal evaluation.
- Better phrasing is "adaptive policy infrastructure," "graph-native agent orchestration," or "reinforcement-learning-inspired crawl optimization."

## 19. Evidence Basis

Key files and areas used for this assessment:
- `README.md`
- `docs/NEW_DEVELOPER_GUIDE.md`
- `docs/Diagrams/CURRENT_IMPLEMENTED_QUEUE_ARCHITECTURE.md`
- `docs/Diagrams/V4_PLATFORM_ARCHITECTURE.md`
- `docs/SystemReport/ARCHITECTURE_AUDIT_2026-04-06.md`
- `docs/SystemReport/PROGRAM_CLOSEOUT_2026-04-06.md`
- `docs/SystemReport/ACCURACY_CONCEPTUAL_MODEL_2026-04-09.md`
- `docs/SystemReport/QUEUE_SYSTEM_VISUALS.md`
- `services/crawler/src/fratfinder_crawler/pipeline.py`
- `services/crawler/src/fratfinder_crawler/orchestration/request_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/field_job_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/field_job_supervisor_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/adaptive_graph.py`
- `services/crawler/src/fratfinder_crawler/orchestration/graph.py`
- `services/crawler/src/fratfinder_crawler/field_jobs.py`
- `services/crawler/src/fratfinder_crawler/discovery.py`
- `services/crawler/src/fratfinder_crawler/normalization/normalizer.py`
- `services/crawler/src/fratfinder_crawler/models.py`
- `services/crawler/src/fratfinder_crawler/config.py`
- `services/crawler/src/fratfinder_crawler/db/repository.py`
- `services/crawler/src/fratfinder_crawler/db/request_repository.py`
- `services/crawler/src/fratfinder_crawler/search/client.py`
- `apps/web/src/lib/db.ts`
- `apps/web/src/lib/fraternity-crawl-request-runner.ts`
- `apps/web/src/lib/benchmark-runner.ts`
- `apps/web/src/lib/campaign-runner.ts`
- `apps/web/src/lib/evaluation-worker.ts`
- `apps/web/src/lib/repositories/runtime-worker-repository.ts`
- `apps/web/src/lib/repositories/fraternity-crawl-request-repository.ts`
- `apps/web/src/lib/types.ts`
- `apps/web/src/app/api/read-only-routes.test.ts`
- `packages/contracts/src/schemas.ts`
- `infra/supabase/migrations`
- `.github/workflows/ci.yml`
- `infra/docker/docker-compose.yml`
- `services/crawler/pyproject.toml`
- `apps/web/package.json`
- `tests/integration/README.md`

## 20. Bottom Line

FratFinderAI is an unusually strong portfolio project because it demonstrates end-to-end ownership of a hard data problem:
- product understanding
- domain modeling
- extraction and enrichment logic
- queue-backed orchestration
- reliability engineering
- evaluation infrastructure
- honest handling of ambiguity

Its most impressive trait is not that it "uses AI." Its most impressive trait is that it treats correctness, evidence, and operational control as core product requirements while still building room for adaptive behavior and measurable system improvement.
