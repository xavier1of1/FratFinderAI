# V3 Interfaces And State

This visual explains how the crawler runtime interfaces with the database and frontend, and how request state is projected for operator workflows.

## Persistence Model

```mermaid
erDiagram
  FRATERNITY_CRAWL_REQUESTS ||--o{ FRATERNITY_CRAWL_REQUEST_EVENTS : emits
  FRATERNITY_CRAWL_REQUESTS ||--o{ REQUEST_GRAPH_RUNS : owns
  REQUEST_GRAPH_RUNS ||--o{ REQUEST_GRAPH_EVENTS : logs
  REQUEST_GRAPH_RUNS ||--o{ REQUEST_GRAPH_CHECKPOINTS : checkpoints
  FRATERNITY_CRAWL_REQUESTS ||--o{ CHAPTER_EVIDENCE : contributes
  FRATERNITY_CRAWL_REQUESTS ||--o{ PROVISIONAL_CHAPTERS : discovers
  FRATERNITY_CRAWL_REQUESTS ||--o{ PROVIDER_HEALTH_SNAPSHOTS : monitors

  FRATERNITY_CRAWL_REQUESTS {
    uuid id PK
    text fraternity_slug
    text source_slug
    text status
    text stage
    jsonb progress
    jsonb config
  }

  REQUEST_GRAPH_RUNS {
    bigint id PK
    uuid request_id FK
    text runtime_mode
    text status
    text active_node
    jsonb summary
    jsonb metadata
  }

  REQUEST_GRAPH_EVENTS {
    bigint id PK
    bigint run_id FK
    uuid request_id FK
    text node_name
    text phase
    text status
    int latency_ms
    jsonb diagnostics
  }

  REQUEST_GRAPH_CHECKPOINTS {
    bigint id PK
    bigint run_id FK
    uuid request_id FK
    text node_name
    jsonb state
  }

  CHAPTER_EVIDENCE {
    bigint id PK
    uuid request_id FK
    text chapter_slug
    text field_name
    text candidate_value
    numeric confidence
    text evidence_status
  }

  PROVISIONAL_CHAPTERS {
    bigint id PK
    uuid request_id FK
    text slug
    text status
    jsonb evidence_payload
  }

  PROVIDER_HEALTH_SNAPSHOTS {
    bigint id PK
    uuid request_id FK
    text provider
    bool healthy
    numeric success_rate
    jsonb payload
  }
```

## Interface Projection To Frontend

```mermaid
flowchart LR
  DB[(Postgres)]
  Repo["Web Repositories<br/>request, agent-ops, benchmark, review"]
  API["Next API Routes"]
  UI["Dashboard Pages<br/>Overview, Intake, Agent Ops, Benchmarks, Review"]

  DB --> Repo
  Repo --> API
  API --> UI
```

## Request Progress Projection Contract

The key state projection pattern is that graph internals remain in graph tables, while operator-facing status remains in the request `progress` payload and request stage/status fields.

```mermaid
flowchart TD
  A["graph state transition"] --> B["append request_graph_event"]
  B --> C["upsert request_graph_checkpoint"]
  C --> D["update request.progress.graph"]
  D --> E["update request.progress.analytics"]
  E --> F["project status and stage for UI compatibility"]
  F --> G["render in Intake and Agent Ops"]
```

This keeps backward compatibility for existing dashboards while still exposing graph-native internals to operators.

