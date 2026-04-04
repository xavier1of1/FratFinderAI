# V3 Architecture Diagrams

This folder contains the engineer-focused visual companion set for the V3 LangGraph architecture.

All diagrams are authored in Markdown with embedded Mermaid so they can live in the repository as editable source.

## How To View

### VS Code
- Open any file in this folder.
- Use `Ctrl+Shift+V` or `Open Preview to the Side` to render the Mermaid block.
- If your local Markdown preview does not render Mermaid, install a Mermaid-capable Markdown preview extension.

### GitHub
- GitHub renders Mermaid blocks inside Markdown automatically.

## Diagram Set

- [V3 System Overview](./V3_SYSTEM_OVERVIEW.md): layered architecture showing entrypoints, supervisor graphs, worker graphs, tool layers, memory, and storage.
- [V3 Source Worker Graph](./V3_SOURCE_WORKER_GRAPH.md): end-to-end source crawl worker execution graph with loops, actions, and persistence points.
- [V3 Chapter Resolution Graph](./V3_CHAPTER_RESOLUTION_GRAPH.md): chapter/entity resolution subgraph for website, email, Instagram, review, and defer decisions.
- [V3 Distributed Control Plane](./V3_DISTRIBUTED_CONTROL_PLANE.md): distributed supervisor/worker orchestration, claiming, recovery, degradation, and observability substrate.
- [V3 Learning, Observability, And Review](./V3_LEARNING_OBSERVABILITY_AND_REVIEW.md): hybrid live/offline learning loop, operator review feedback, telemetry, and policy promotion.

## Naming Conventions

- `Supervisor Graph`: batch- or queue-level orchestration responsible for coordination and aggregation.
- `Worker Graph`: a graph that performs bounded crawl or enrichment work on one source or work item.
- `Subgraph`: a reusable graph fragment invoked by a worker or supervisor graph.
- `Tool Layer`: deterministic execution modules called by graph nodes.
- `Escalation Layer`: LLM-only paths used for ambiguity, recovery, or conflict explanation.
- `Memory Layer`: persistent telemetry, evidence, policy, and checkpoint state used by future runs.

## Shared Legend

- `Supervisor / control nodes`: orchestration, routing, scheduling, and aggregation responsibilities.
- `Worker / runtime nodes`: active crawl or enrichment execution steps.
- `Deterministic tool nodes`: fetch, analyze, extract, navigate, search, verify, normalize, persist.
- `LLM escalation nodes`: ambiguity-only or recovery-only model calls.
- `Persistence / memory nodes`: graph runs, checkpoints, frontier state, evidence, rewards, provider health, policy snapshots.
- `Human / operator nodes`: dashboard, review actions, approval, benchmark inspection, and policy promotion decisions.

## Source Of Truth

These diagrams are the visual companion to the written V3 architecture plan:

- [LangGraph Unified Crawler Architecture](../plans/LANGGRAPH_UNIFIED_CRAWLER_ARCHITECTURE_2026-04-04.md)

The written plan remains the authoritative architecture specification if wording or detail ever diverges.
