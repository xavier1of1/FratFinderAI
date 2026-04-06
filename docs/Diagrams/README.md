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

- [Current Implemented Queue Architecture](./CURRENT_IMPLEMENTED_QUEUE_ARCHITECTURE.md): implementation-accurate view of the queue system that exists today, including backend-owned evaluation jobs, graph-native request/contact execution, repair lane, and remaining transitional seams.
- [V3 Latest Architecture Explained](./V3_LATEST_ARCHITECTURE_EXPLAINED.md): sentence-heavy, implementation-accurate walkthrough of how the current V3 crawler works end to end.
- [V3 Queue Processes](./V3_QUEUE_PROCESSES.md): request queue lifecycle, worker claim loop, and backpressure signals.
- [V3 Decision Trees](./V3_DECISION_TREES.md): source recovery, crawl fallback, evidence write routing, and enrichment continuation logic.
- [V3 Interfaces And State](./V3_INTERFACES_AND_STATE.md): DB state model, API projection, and frontend integration contract.
- [V4 Platform Architecture (High-Fidelity)](./V4_PLATFORM_ARCHITECTURE.md): transitional architecture view that shows the platform-wide layering while explicitly calling out the remaining non-graph or transitional ownership seams.
- [V4 LangGraph Logic Map (Current Runtime)](./V4_LANGGRAPH_LOGIC_MAP.md): exact runtime-selection path and both implemented LangGraph topologies, including the current non-graph field-job path.
- [V3 System Overview](./V3_SYSTEM_OVERVIEW.md): target-state graph-native architecture. Use alongside the implemented/current architecture docs, not as a claim that every lane is already fully migrated.
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

## Status Guide

- `Implemented`: describes the architecture that is actually running today.
- `Transitional`: describes the migration-period architecture that still contains compatibility seams or mixed ownership.
- `Target`: describes the intended end-state architecture after the migration program is complete.

When comparing diagrams, prefer:

1. `Current Implemented Queue Architecture` for operational debugging
2. `V4 Platform Architecture` for transitional migration context
3. `V3 System Overview` for end-state design intent
