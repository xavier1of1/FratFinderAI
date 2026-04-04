# V3 Learning, Observability, And Review

This diagram shows the hybrid live-plus-offline improvement loop for V3.

It makes explicit how:

- production runs emit telemetry
- operators inspect failures and evidence
- offline replay and training evaluate candidate policy changes
- only approved policy packs return to production

## Feedback Loop

```mermaid
flowchart LR
  subgraph PROD["Production Runtime"]
    PRUN["Source and enrichment graph runs"]
    PEVT["Node events and decision traces"]
    POBS["Page observations and reward events"]
    PEVID["Chapter evidence and provenance"]
    PREV["Review items and operator actions"]
  end

  subgraph OPS["Operator + Review Loop"]
    DASH["Operator dashboard"]
    TRIAGE["Review triage and resolution"]
    LABEL["Conflict labeling and evidence validation"]
  end

  subgraph OFFLINE["Offline Evaluation And Learning"]
    REPLAY["Replay graph<br/>reconstruct policy outcomes from stored observations"]
    BENCH["Benchmark graph<br/>control, treatment, shadow comparisons"]
    TRAIN["Training graph<br/>policy updates and reward tuning"]
    GATE["Promotion gate<br/>coverage, reliability, throughput, safety"]
  end

  subgraph POLICY["Policy Management"]
    SNAP["Policy snapshots"]
    PACK["Policy packs<br/>deterministic_strict, assisted_live, shadow_compare, exploratory_train"]
    PROMO["Approved production policy"]
  end

  DB[("Postgres / Supabase")]

  PRUN --> PEVT
  PRUN --> POBS
  PRUN --> PEVID
  PRUN --> PREV

  PEVT --> DASH
  POBS --> DASH
  PEVID --> DASH
  PREV --> DASH

  DASH --> TRIAGE
  TRIAGE --> LABEL
  LABEL --> PEVID
  TRIAGE --> PREV

  PEVT --> REPLAY
  POBS --> REPLAY
  POBS --> BENCH
  PEVID --> BENCH
  PEVID --> TRAIN
  PREV --> TRAIN

  REPLAY --> GATE
  BENCH --> GATE
  TRAIN --> GATE

  TRAIN --> SNAP
  SNAP --> PACK
  GATE -->|approve| PROMO
  GATE -->|reject| TRAIN
  PROMO --> PACK
  PACK --> PRUN

  PEVT --> DB
  POBS --> DB
  PEVID --> DB
  PREV --> DB
  SNAP --> DB
  PACK --> DB
```

## Hybrid Learning Model

- Production may update live memory such as template profiles, provider health, and evidence confidence summaries.
- Production should not auto-promote broad policy changes without offline replay and benchmark gates.
- Operator review outcomes are training signals, not just audit data.
- Benchmark and replay graphs are first-class workflows because they validate whether a candidate policy is safe to promote.
