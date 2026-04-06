# Deep Architecture Audit: Queue Processing And System Design

## Executive Summary

The FratFinderAI platform is no longer one queue architecture. It is several partially overlapping queue architectures sharing the same database:

- a Python request-supervisor graph
- an adaptive crawl runtime
- a transitional field-job graph plus imperative field-job engine
- web-owned benchmark and campaign schedulers
- database-driven reconciliation and stale-run cleanup

That split is the main architectural problem.

The product mission is clear:

- find real fraternity chapters
- validate that those entities are truly chapters
- recover trustworthy website, email, and Instagram data
- preserve evidence
- avoid junk chapter creation and noisy search churn

The conceptual strategy in the design docs mostly matches that mission. The implementation only partially does. The biggest mismatch is that the implemented system still revolves too much around queue mechanics and too little around durable product-semantic workflow stages such as chapter validity, repair, contact resolution, and provisional promotion.

In practical terms:

- queue health has improved
- junk suppression has improved
- the chapter-validity gate helped materially
- throughput-first field-job changes helped materially

But the system is still carrying architectural debt that will limit scale and clarity:

- GET routes mutate production state
- Next.js owns long-running schedulers with in-memory state
- graph orchestration and imperative orchestration both own queue behavior
- queue-critical state is still mostly JSON-driven
- repair is logically present but not operationally first-class
- provisional promotion and benchmark alert loops are incomplete

## 1. Product Purpose

This system is not a generic job queue. Its purpose is to answer a very specific product need:

1. find a fraternity's authoritative or best-available national source
2. discover real chapter entities from that source
3. determine which entities are truly chapters versus junk, navigation, awards, demographics, or academic artifacts
4. recover trustworthy contact information for valid chapter entities
5. preserve evidence and route ambiguity to review

That means the queue exists to support truth discovery. If the queue is technically busy but is processing invalid entities or burning provider budget on low-value work, the system is not succeeding at its actual mission.

## 2. Audit Method

The audit used four evidence classes:

- code review across crawler, repository, web, and migration layers
- design-doc comparison across V3 and V4 architecture diagrams
- operational evidence from logs and live web APIs
- direct PostgreSQL snapshots of queue state, run state, and review/evidence tables

The goal was not only to find code smells, but to compare:

- conceptual strategy vs implemented behavior
- control-plane ownership vs product workflow
- queue metrics vs mission metrics

## 3. Conceptual Strategy vs Current Implementation

### Intended conceptual strategy

The design docs describe a mission-aligned flow:

```text
source recovery
-> chapter discovery
-> chapter validity / repair
-> contact resolution
-> evidence / review / promotion
-> benchmark / learning
```

This is the right product model because it prioritizes chapter truth before contact search.

### Actual implementation shape

The implementation currently behaves more like:

```text
request graph
-> crawl runtime
-> create chapter rows + field jobs
-> field-job triage / repair inside pipeline
-> contact search loop
-> evidence / review
-> benchmark / campaign orchestration from web server
```

This is a meaningful difference. The current implementation still makes queue creation and queue processing central, while the conceptual strategy makes semantic validity central.

### Why this matters

This explains why some improvements helped but did not fully generalize:

- queue churn dropped
- invalid work suppression improved
- but some sources still produced non-chapter entities because semantic truth is not yet the only durable control plane

## 4. Current Queue Topology

Today the queue system is effectively four workload planes sharing one persistence layer:

1. request processing
2. crawl processing
3. contact field-job processing
4. benchmark and campaign orchestration

Those planes are not owned by one backend scheduler. They are split across:

- Python LangGraph runtimes
- imperative Python services
- Next.js API routes and schedulers
- Postgres-driven stale reconciliation

## 5. Live Operational Snapshot

The system snapshot taken during the audit showed:

| Metric | Value | Interpretation |
|---|---:|---|
| Queued field jobs | 10,823 | Field-job backlog is still the dominant live operational load |
| Deferred field jobs | 288 | Deferred state exists, but repair is still not its own durable lane |
| Terminal no-signal jobs | 128 | The bounded contact model is active, but this is still only one queue outcome |
| Review-required field jobs | 314 | Manual review burden remains meaningful |
| Updated field jobs | 1,920 | Contact work is generating writes, but queue pressure remains high |
| Provisional chapters open | 287 | Provisional workflow is producing data |
| Provisional promoted | 0 | Promotion loop is not operationally closing |
| Evidence total | 4,767 | Evidence persistence is active and meaningful |
| Request graph runs | 4 | Request-level graph footprint is small relative to field-job work |
| Field-job graph runs | 104 | Most operational traffic is flowing through field-job work |
| Benchmark runs | 54 total | Evaluation is active, but not stable enough |
| Failed benchmark runs | 16 | Evaluation subsystem is still brittle |
| Campaign runs | 11 total | Campaigning exists but is not yet robust |
| Failed campaign runs | 7 | Campaign execution is still unreliable |

### Current queue concentrations by source

| Source | Actionable queued | Deferred queued | Blocked invalid failed |
|---|---:|---:|---:|
| `pi-kappa-alpha-main` | 3,270 | 42 | 1,027 |
| `sigma-alpha-epsilon-main` | 2,921 | 246 | 614 |
| `alpha-delta-gamma-main` | 983 | 0 | 787 |
| `delta-kappa-epsilon-main` | 595 | 0 | 0 |

### Current top review reasons

| Review reason | Count |
|---|---:|
| low-confidence `contact_email` candidate | 166 |
| low-confidence `website_url` candidate | 160 |
| placeholder/navigation chapter record | 71 |
| overlong chapter name | 32 |
| identity semantically incomplete | 31 |
| overlong slug | 31 |

These numbers reinforce the same story:

- the queue problem is still mostly a contact-resolution problem
- upstream entity validity has improved, but not enough to eliminate noisy sources
- the system is preserving evidence, but still producing too much uncertain work

## 6. Architecture Findings

### 6.1 High-severity issues

#### A. Read paths mutate production state

Several web GET routes do operational mutation:

- `GET /api/campaign-runs` can reconcile stale campaigns, schedule due runs, and re-schedule running campaigns
- `GET /api/benchmarks` can fail stale benchmark runs
- `GET /api/runs` and `GET /api/agent-ops` reconcile stale crawl runs

This is a strong anti-pattern because observability and control are mixed. Dashboard reads should not own operational side effects.

#### B. Long-running schedulers live inside the web app

Benchmark and campaign execution are managed in the Next.js process with:

- in-memory `Set<string>` state
- `queueMicrotask()`
- spawned Python subprocesses

This creates durability and scale risks:

- restart risk
- horizontal scale ambiguity
- split ownership between UI and worker planes

#### C. Queue ownership is split across layers

The queue control plane is fragmented:

- request work is graph-owned in Python
- field jobs are partly graph-owned and partly imperative
- campaigns and benchmarks are web-owned
- stale reconciliation is triggered from both repositories and GET routes

This makes recovery semantics inconsistent and increases the chance of subtle race or state-drift bugs.

#### D. `pipeline.py`, `field_jobs.py`, and `repository.py` are all god-objects

The most important operational behavior is concentrated in overly broad modules:

- `pipeline.py` controls runtime selection, triage, repair, field-job entrypoints, and policy packs
- `field_jobs.py` controls admission, provider behavior, candidate scoring, verification, and retry logic
- `repository.py` handles creation, claim, triage reads, stale reconciliation, and graph persistence

That much responsibility in single modules increases coupling and makes refactors risky.

#### E. Hot workflow state is still mostly JSON-driven

Queue-critical state lives in nested payloads such as:

- `contactResolution.queueState`
- `queueTriage`
- `chapterRepair`
- `chapterValidity`

This made rapid iteration easier, but it is now a structural problem:

- hard to index
- hard to enforce
- hard to reason about transactionally
- easy for dashboards and processors to disagree

### 6.2 Medium-severity issues

#### F. Repair is not a first-class workload lane

Repair exists as logic, counters, and progress state, but not as a distinct durable queue analogous to `field_jobs`.

That creates three problems:

- repair cannot be scheduled independently
- repair cannot have dedicated fairness or concurrency rules
- repair is harder to observe and benchmark as its own subsystem

#### G. Success semantics are too infrastructure-centric

Field-job graph runs can succeed even when business progress is effectively zero. Operationally, that muddies observability:

- runtime success is not the same as mission success
- processed `0`, requeued `0`, and success is not very informative to operators

#### H. Benchmark and campaign KPIs are not fully mission-normalized

The benchmark framework measures:

- throughput
- requeues
- queue delta
- cycle latency

Those are useful, but they are not enough. The product really needs stronger primary KPIs around:

- valid chapter coverage
- trusted contact coverage
- false-entity suppression
- review burden per true chapter

#### I. The docs and runtime are meaningfully out of sync

The diagrams describe a graph-native control plane, but the implemented V4 path still includes:

- web-owned schedulers
- non-graph field-job execution
- embedded repair rather than a true repair queue

That doc drift creates planning and onboarding risk.

## 7. Mission Alignment Findings

### What is aligned

- The request lifecycle is increasingly aligned with the product mission.
- The chapter-validity gate is a real improvement.
- Contact resolution is more bounded and evidence-aware than before.
- Review and evidence storage are meaningful and active.

### What is still misaligned

- Contact resolution still partly acts as a semantic validator instead of being downstream of chapter truth.
- The queue still represents implementation work more than product-semantic stages.
- Provisional creation exists, but provisional promotion does not yet operate as a real workflow.
- Evaluation emphasizes queue KPIs more than product truth KPIs.

### Mission-correct interpretation

The largest architectural risk is not just scheduler sprawl or JSON state. It is that the conceptual system is built around truth discovery, but the implementation is still partly built around queue processing.

That is the core reason some fixes improve churn or speed without fully generalizing to chapter/contact accuracy.

## 8. Feature Gaps

The audit surfaced several meaningful gaps:

1. No durable, first-class repair queue
2. No closed provisional promotion loop in production data
3. No active benchmark alert loop despite alert schema existing
4. No strong benchmark isolation from live queue conditions
5. No durable backend scheduler for campaigns and benchmarks independent of the web app
6. No fully normalized workload-state model for queue-critical transitions
7. No complete mission-first KPI set at benchmark/campaign level
8. Limited automated test coverage around the riskiest web-owned schedulers

## 9. Scalability And Separation Recommendations

### Recommendation 1: move all long-running control-plane work out of Next.js

The web app should:

- create runs
- request work
- render dashboards

It should not:

- own long-running schedulers
- shell out to Python for production benchmark/campaign lifecycles
- mutate state from GET routes

### Recommendation 2: split workload lanes explicitly

The system should have durable first-class workload lanes for:

- request supervision
- chapter repair
- contact resolution
- benchmark/evaluation execution

Each lane should have:

- its own claim loop
- its own worker fairness limits
- its own health metrics
- its own SLA semantics

### Recommendation 3: promote queue-critical state into typed relational columns

Keep JSON for diagnostics, but move hot state into typed fields such as:

- `workload_lane`
- `queue_state`
- `validity_class`
- `repair_state`
- `terminal_outcome`
- `actionability`
- `blocked_reason`

This will materially improve queryability, fairness, analytics, and operational safety.

### Recommendation 4: break the broad repositories and services by bounded context

Refactor toward:

- `RequestQueueRepository`
- `FieldJobQueueRepository`
- `RepairQueueRepository`
- `CrawlRunRepository`
- `EvaluationRunRepository`
- `AdaptiveTelemetryRepository`

Do the same at the service level to reduce the current god-object files.

### Recommendation 5: make repair a durable first-class queue

Repair should have:

- persisted repair jobs
- source-aware repair policies
- repair attempt caps
- repair-specific outcomes
- repair-specific dashboards and benchmarks

### Recommendation 6: batch and decouple graph persistence

Per-node synchronous event/checkpoint writes are too expensive for scale. The graph runtime should reduce write amplification by:

- grouping writes
- buffering non-critical events
- separating hot operational state from deep audit telemetry

## 10. Recommended Refactor Sequence

1. Remove state mutation and scheduling from GET routes.
2. Move benchmark and campaign execution into a backend evaluation worker.
3. Create a first-class repair queue and worker lane.
4. Split `pipeline.py`, `field_jobs.py`, and `repository.py` by bounded context.
5. Promote queue-critical JSON state into typed DB columns.
6. Add mission-normalized KPIs to benchmark and campaign summaries.
7. Close the provisional promotion loop.
8. Update the docs so implemented architecture and target architecture are clearly separated.

## 11. Bottom Line

The platform has improved materially. It is much better at recognizing bad work and suppressing junk than it was before.

But the architecture is still transitional.

The most important remaining truth is:

- the product strategy is built around chapter truth and contact truth
- the implementation still spreads queue control across too many layers

The next major architectural win will come from making queue ownership singular, repair first-class, workload lanes explicit, and mission semantics stronger than queue mechanics.
