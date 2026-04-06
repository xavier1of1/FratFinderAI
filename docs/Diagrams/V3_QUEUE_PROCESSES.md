# V3 Queue Processes

This visual set explains how work moves through the current V3 queue model and how bottlenecks are avoided.

## Request Queue Lifecycle

```mermaid
sequenceDiagram
  autonumber
  participant U as Operator or API
  participant W as Web Intake Layer
  participant DB as Postgres
  participant RW as Request Worker
  participant RG as Request Supervisor Graph
  participant CR as Crawl Runtime
  participant FJ as Field Job Runtime
  participant AO as Agent Ops UI

  U->>W: Create or confirm crawl request
  W->>DB: Insert or update fraternity_crawl_requests (queued)
  RW->>DB: reconcile_stale_requests()
  RW->>DB: claim_next_due_request(worker_id)
  DB-->>RW: claimed request row
  RW->>RG: run(request_id)
  RG->>DB: start request_graph_runs row
  RG->>CR: run crawl for source
  CR-->>RG: crawl completed metrics
  RG->>FJ: run enrichment cycles if needed
  FJ-->>RG: processed or requeued or failed_terminal
  RG->>DB: update request progress and stage projection
  RG->>DB: finish request_graph_runs row
  AO->>DB: read request graph summary and queue counters
  DB-->>AO: queue health and run telemetry
```

## Internal Queue Control Loop

```mermaid
flowchart TD
  A["worker start"] --> B["reconcile stale requests"]
  B --> C["claim next due request"]
  C -->|none claimed| D["idle cycle"]
  D --> E{"once mode or batch limit reached?"}
  E -->|no| B
  E -->|yes| Z["worker exit"]

  C -->|claimed| F["run request supervisor graph"]
  F --> G["append graph events and checkpoint"]
  G --> H{"request terminal?"}
  H -->|yes| I["record succeeded or failed or paused"]
  H -->|no| J["continue graph transitions"]
  J --> G
  I --> K{"once mode or batch limit reached?"}
  K -->|no| B
  K -->|yes| Z
```

## Backpressure Signals Used In V3

The current architecture uses concrete queue and runtime signals to decide whether to keep processing or slow down.

- `requestQueueQueued` and `requestQueueRunning` from Agent Ops summary indicate request-level backlog.
- `queued` and `running` field-job totals indicate enrichment pressure.
- `lowProgressCycles` and `degradedCycleCount` in enrichment analytics indicate reduced yield.
- provider preflight and health snapshots indicate whether search-backed enrichment should degrade or defer.

These are not conceptual-only signals; they are persisted and surfaced for operators.

