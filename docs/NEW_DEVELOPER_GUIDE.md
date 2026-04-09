# Frat Finder AI: New Developer Guide

## 1. What This Project Is Trying To Achieve

Frat Finder AI is a fraternity chapter discovery, enrichment, and operations platform.

The end goal is:

- accept the name of a fraternity from an operator or user
- identify the correct national source or directory for that fraternity
- crawl that source to discover chapter records across the United States
- normalize those records into a canonical database model
- preserve provenance for every discovered field
- enrich missing chapter contact data such as website, Instagram, and email
- expose everything through an operator-facing website so the system can be monitored, reviewed, corrected, and benchmarked

In practical terms, this project is building toward a system where a user can enter a fraternity name and the platform can reliably answer:

- what chapters exist
- where they are
- which national source they came from
- what contact fields are known
- what still needs review, repair, or enrichment

The system is fraternity-first today, but many of the patterns here are general directory-ingestion patterns that could later be adapted to similar domains.

## 2. Product Mindset And Why The Project Exists

This is not just a web crawler.

The product is trying to solve a hard operational problem:

- fraternity national sites are inconsistent
- chapter directories are often incomplete
- layouts vary from simple tables to maps, cards, scripts, or hybrid pages
- contact information is frequently missing or low quality
- broad web search is useful but noisy

Because of that, the project combines:

- deterministic source-native extraction
- adaptive orchestration
- deferred enrichment jobs
- human review for ambiguous cases
- benchmarking and campaign tooling for performance tuning

The product philosophy is:

- trust national or source-native data first
- write automatically only when confidence is high
- preserve evidence so bad writes can be explained and corrected
- use review instead of silently mutating uncertain data
- optimize not only for recall, but for throughput, reliability, and operator visibility

## 3. High-Level Architecture

At the highest level, the repository has four major areas:

- `apps/web`
  - Next.js operator dashboard and API routes
- `services/crawler`
  - Python crawler, enrichment, graph orchestration, and worker logic
- `packages/contracts`
  - shared schemas and contract definitions
- `infra`
  - migrations, Docker files, and local infrastructure helpers

The main architecture is split into:

- operator interfaces
- orchestration and workers
- deterministic extraction and search tools
- persistence and telemetry

### Operator Layer

The operator-facing website provides:

- fraternity intake
- chapter browsing and editing
- review queue handling
- run history
- benchmarks and campaigns
- agent and queue operations visibility

The website is not supposed to contain crawler business logic. It should present data, invoke APIs, and expose operator controls.

### Execution Layer

Execution is primarily owned by the Python service.

Important execution lanes include:

- crawl execution
- request execution
- field-job enrichment
- repair jobs
- benchmark and campaign evaluation

Today, the system is in a transitional state:

- crawl paths use LangGraph-based orchestration in some areas
- field-job processing has a graph-native path and legacy compatibility seams
- request execution is graph-supervised
- some operational scheduling logic is still split across subsystems

### Data Layer

The database holds both product data and runtime telemetry.

Core product tables include:

- `sources`
- `crawl_runs`
- `chapters`
- `chapter_provenance`
- `field_jobs`
- `review_items`
- `fraternity_crawl_requests`
- `campaign_runs`
- `benchmark_runs`

Telemetry and graph-runtime tables include:

- `request_graph_runs`
- `request_graph_events`
- `request_graph_checkpoints`
- `field_job_graph_runs`
- `field_job_graph_events`
- `crawl_sessions`
- `crawl_frontier_items`
- `crawl_page_observations`
- `crawl_reward_events`
- `chapter_evidence`
- `provider_health_snapshots`

This matters because the system is not just storing final results. It is also storing how the system made decisions.

## 4. How The System Works End To End

There are several major workflows.

### A. Fraternity Intake Workflow

This starts when an operator enters a fraternity name through the website or CLI.

The system then:

1. normalizes the fraternity identity
2. attempts source discovery or verified source lookup
3. evaluates source quality and trust
4. creates or updates a `fraternity_crawl_request`
5. routes that request into execution if it is safe enough

If the source is weak or blocked, the request can be held for confirmation rather than blindly executed.

### B. Crawl Workflow

Once a source is selected, the crawler runs against the source to discover chapters.

The crawler:

1. loads a source record
2. fetches source pages
3. analyzes structure
4. uses adapters to extract chapter-like entities
5. follows chapter detail pages or contact hints where allowed
6. normalizes records into canonical chapter rows
7. writes provenance for all extracted fields
8. creates review items or follow-up jobs when fields are ambiguous or missing

### C. Field-Job Enrichment Workflow

If a chapter is missing website, Instagram, or email, the system creates a field job.

Field jobs:

- can run in legacy or LangGraph-native runtime modes
- search for missing contact fields
- verify candidates before writing
- requeue when blocked or degraded
- fail terminally when attempts are exhausted or the outcome is clearly invalid

Field jobs are important because many national sources do not contain complete contact information.

### D. Review Workflow

When a candidate is useful but not safe enough to write automatically, it is routed into review.

Review allows an operator to:

- inspect the candidate
- inspect source evidence
- resolve ambiguity
- accept or ignore items
- keep the canonical data model clean

### E. Benchmark And Campaign Workflow

Benchmarks and campaigns are how the platform is tuned and validated.

They are used to:

- compare runtime modes
- measure queue throughput
- measure contact coverage
- test degraded provider behavior
- validate adaptive logic changes

This is a big part of how the project becomes production-ready rather than just “working on one source.”

## 5. What Makes This Project Agentic

This project is agentic because it does not simply execute one fixed parser or one fixed script.

Instead, it makes bounded runtime decisions about:

- whether a source is trustworthy enough to crawl
- how to route a request through recovery or crawl branches
- whether enrichment is needed
- which field jobs to process
- whether a candidate is safe to write, needs review, or should be retried later
- when to degrade behavior because providers are unhealthy

The system is agentic in orchestration, not in the vague sense of “it uses AI.”

Important agentic characteristics here are:

- durable state transitions
- explicit runtime decisions
- bounded fallback behavior
- recoverable workflows
- telemetry-rich execution
- evidence-based mutation

### LangGraph’s Role

LangGraph is used to make orchestration explicit.

That means:

- workflow nodes are named and durable
- execution can checkpoint
- retries and branches are visible
- graph events can be inspected later
- request and job execution are traceable instead of hidden inside one large loop

Even where the crawl core still uses a legacy mode for stability, the orchestration around it is increasingly graph-supervised.

### What This Project Does Not Mean By “Agentic”

It does not mean:

- unconstrained LLM browsing
- blind autonomous writes
- replacing deterministic extraction with a chatbot

LLMs, where used, are escalation tools for ambiguity or recovery, not the primary runtime.

## 6. Supported Features

The system currently supports a broad set of features across product, crawling, and operations.

### Product Features

- fraternity intake and request creation
- chapter browsing
- chapter editing
- chapter deletion
- rerun requests for selected chapters
- map and coverage views
- review queue management
- crawl run visibility
- benchmark and campaign visibility
- agent operations visibility

### Crawler Features

- crawl all active sources or one source by slug
- crawl in legacy mode
- crawl in adaptive runtime modes
- source discovery for new fraternities
- verified source bootstrap and revalidation
- source-aware chapter extraction
- chapter normalization and provenance capture
- chapter evidence persistence
- bounded navigation and same-domain traversal

### Enrichment Features

- website discovery
- email discovery
- Instagram discovery
- provider preflight checks
- degraded-mode throttling
- search-provider pacing
- dependency-aware requeueing
- website-first email gating
- Instagram-specific query handling

### Reliability And Operations Features

- backend-owned request workers
- graph run events and checkpoints
- stale job reconciliation
- queue visibility
- benchmark execution
- campaign execution
- ops alerts
- provider-health telemetry

## 7. Current Runtime Reality

A new developer should understand that the codebase contains both:

- target architecture documents
- the current transitional implementation

Those are not always the same thing.

As of the current codebase:

- request execution is graph-supervised and backend-owned
- field-job processing has graph-native runtime support
- adaptive crawl exists but is not always the safest default for all real-world sources
- some architecture documents describe the desired future state more cleanly than the current runtime

If you are debugging production-like behavior, favor:

- current implementation docs
- queue architecture docs
- repository code
- worker code
- runtime tables

If you are planning future improvements, also consult:

- V3 and V4 diagrams
- migration and implementation phase docs
- architecture audit reports

## 8. Important Runtime Concepts

### Source Quality

The platform tries to avoid crawling weak or obviously wrong sources.

This is important because many failures come from bad national-source selection, not from parser bugs.

### Provenance

Every useful field should be traceable back to where it came from.

That includes:

- source URL
- snippet
- confidence
- source slug

This is one of the most important integrity guarantees in the project.

### Canonical Data vs Evidence

The project distinguishes between:

- canonical chapter data
- candidate evidence

Canonical data is what the UI shows as the trusted chapter record.
Evidence is what the system found but did not necessarily trust enough to auto-write.

This separation is essential for safe automation.

### Queue State

Field jobs and request jobs are not just “pending” or “done.”

They can be:

- queued
- running
- blocked
- requeued
- terminally failed
- review-routed

Understanding queue state is critical for performance debugging.

## 9. How To Think About Performance

Performance in this system is not just request latency.

The main KPIs are more like:

- chapters discovered
- any-contact coverage
- website/email/Instagram coverage
- jobs per minute
- queue burn-down
- retry waste
- provider degradation rate
- failure mode distribution

A fast crawler that discovers low-quality data or floods the queue with useless retries is not considered successful.

## 10. Common Failure Modes

A new developer should expect several recurring failure classes.

### Source Failures

- wrong or weak national source selected
- no chapters discovered from selected source
- brittle source structure

### Search And Provider Failures

- provider unavailable
- search result quality too weak
- rate limiting or transport degradation
- queue buildup behind website confidence gating

### Data Quality Failures

- malformed state values
- low-confidence websites from generic sources
- wrong social candidates
- university mismatches

### Runtime Failures

- stale running jobs
- queue imbalance from very large fraternities
- benchmark/campaign reliability issues
- transitional architecture seams between graph and non-graph paths

## 11. Important Entry Points For Developers

If you are new, start here:

- `README.md`
- `docs/Diagrams/V4_PLATFORM_ARCHITECTURE.md`
- `docs/Diagrams/CURRENT_IMPLEMENTED_QUEUE_ARCHITECTURE.md`
- `docs/Diagrams/V3_SYSTEM_OVERVIEW.md`

Important code entry points:

- `services/crawler/src/fratfinder_crawler/cli.py`
- `services/crawler/src/fratfinder_crawler/pipeline.py`
- `services/crawler/src/fratfinder_crawler/db/repository.py`
- `services/crawler/src/fratfinder_crawler/orchestration/`
- `apps/web/src/app/`
- `apps/web/src/lib/repositories/`

## 12. Local Development Basics

Typical local setup is:

1. copy `.env.example` to `.env`
2. start the local DB stack
3. run migrations and seed data
4. install web dependencies
5. install crawler dependencies
6. run the website
7. run crawler CLI commands as needed

Common commands:

```bash
pnpm dev:web
python -m fratfinder_crawler.cli run
python -m fratfinder_crawler.cli process-field-jobs --limit 25
python -m fratfinder_crawler.cli search-preflight --probes 4
python -m fratfinder_crawler.cli discover-source --fraternity-name "Lambda Chi Alpha"
```

## 13. What A Good Change Looks Like In This Repo

A good change in this project usually does at least one of these things:

- improves source accuracy
- improves enrichment quality
- reduces queue waste
- increases operator visibility
- tightens write safety
- makes graph execution easier to inspect or recover

A risky change is one that:

- silently broadens auto-write behavior
- mixes UI and crawler business logic
- bypasses repository or contract layers
- improves one benchmark while degrading data integrity

## 14. Recommended Mental Model For New Developers

Think of the platform as three connected systems:

1. a product surface for operators
2. a set of durable runtimes that discover and enrich chapter data
3. a telemetry and evidence system that explains what happened

If you keep those three in mind, most of the codebase becomes easier to navigate.

## 15. Suggested Reading Order

For a new developer, the best reading order is:

1. this document
2. `README.md`
3. `docs/Diagrams/V4_PLATFORM_ARCHITECTURE.md`
4. `docs/Diagrams/CURRENT_IMPLEMENTED_QUEUE_ARCHITECTURE.md`
5. `docs/Diagrams/V3_SYSTEM_OVERVIEW.md`
6. `services/crawler/src/fratfinder_crawler/cli.py`
7. `services/crawler/src/fratfinder_crawler/pipeline.py`
8. one web API route and one repository module

That order gives you:

- product understanding
- architecture understanding
- runtime understanding
- code-entry understanding

## 16. Bottom Line

Frat Finder AI is a source-aware, graph-supervised fraternity chapter intelligence platform.

Its purpose is not merely to scrape pages.
Its purpose is to build trustworthy chapter records from inconsistent web sources, enrich them safely, expose them to operators, and continuously improve performance through telemetry, review, and benchmarking.

That combination of:

- source-aware crawling
- adaptive orchestration
- evidence-based writes
- deferred enrichment
- operator review
- benchmark-driven improvement

is what makes the project both technically interesting and operationally useful.
