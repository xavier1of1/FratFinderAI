# V3 Distributed Control Plane

This diagram captures the production-grade control plane that sits around the LangGraph runtime.

It fills in the operational pieces that are easy to miss in a pure execution graph:

- distributed work claiming
- checkpoint and heartbeat recovery
- provider degradation controls
- retries and dead-letter handling
- shared graph-run observability

## Distributed Supervisor And Worker Composition

```mermaid
flowchart TB
  subgraph ENTRY["Launch + Operator Surface"]
    API["CLI / API launch requests"]
    DASH["Operator dashboard<br/>runs, checkpoints, failures, alerts"]
  end

  subgraph CTRL["Control Plane"]
    COORD["Batch Coordinator<br/>creates graph runs and fanout plans"]
    SRCQ["Source Work Queue<br/>source sessions to be claimed"]
    JOBQ["Deferred Enrichment Queue<br/>field jobs and recoveries"]
    EVALQ["Evaluation Queue<br/>benchmark, replay, train jobs"]
    DEG["Provider Degradation Controller<br/>caps workers, disables actions, defers unsafe work"]
    DLQ["Dead Letter Queue<br/>poison pages, exhausted jobs, unrecoverable sessions"]
  end

  subgraph WORKERS["Distributed Workers"]
    SRCW1["Source Worker Pod A<br/>source worker graph"]
    SRCW2["Source Worker Pod B<br/>source worker graph"]
    JOBW["Enrichment Worker Pod<br/>enrichment job graph"]
    EVALW["Evaluation Worker Pod<br/>benchmark and replay graphs"]
  end

  subgraph OBS["Shared Graph Observability"]
    GR["graph_runs<br/>supervisor and worker lineage"]
    GE["graph_run_events<br/>node events, metrics, diagnostics"]
    GC["graph_run_checkpoints<br/>resume snapshots"]
    HB["worker heartbeats and leases<br/>claim liveness and ownership"]
  end

  subgraph MEM["State And Policy Stores"]
    FRONT["frontier and crawl session state"]
    EVID["chapter evidence and provenance"]
    POL["template profiles and policy snapshots"]
    PH["provider health snapshots and circuit states"]
  end

  DB[("Postgres / Supabase")]

  API --> COORD
  DASH --> COORD
  DASH --> GR
  DASH --> GE
  DASH --> GC
  DASH --> DLQ

  COORD --> SRCQ
  COORD --> JOBQ
  COORD --> EVALQ
  DEG --> SRCQ
  DEG --> JOBQ
  DEG --> EVALQ

  SRCQ --> SRCW1
  SRCQ --> SRCW2
  JOBQ --> JOBW
  EVALQ --> EVALW

  SRCW1 --> HB
  SRCW2 --> HB
  JOBW --> HB
  EVALW --> HB

  SRCW1 --> GR
  SRCW1 --> GE
  SRCW1 --> GC
  SRCW2 --> GR
  SRCW2 --> GE
  SRCW2 --> GC
  JOBW --> GR
  JOBW --> GE
  JOBW --> GC
  EVALW --> GR
  EVALW --> GE
  EVALW --> GC

  SRCW1 --> FRONT
  SRCW1 --> EVID
  SRCW1 --> POL
  SRCW1 --> PH
  SRCW2 --> FRONT
  SRCW2 --> EVID
  SRCW2 --> POL
  SRCW2 --> PH
  JOBW --> EVID
  JOBW --> PH
  EVALW --> POL
  EVALW --> GE

  HB --> COORD
  GC --> COORD
  COORD --> DLQ

  GR --> DB
  GE --> DB
  GC --> DB
  HB --> DB
  FRONT --> DB
  EVID --> DB
  POL --> DB
  PH --> DB
  DLQ --> DB
```

## Recovery Model

- Workers claim bounded units of work with lease ownership.
- Heartbeats keep leases alive while a worker graph is healthy.
- Missing heartbeats allow the coordinator to resume from the latest checkpoint.
- Poison pages and terminal failures route to dead-letter storage for operator inspection instead of hot-loop retries.
- Provider degradation policy can reduce worker concurrency, disable expensive action families, or force defer behavior without changing graph code.
