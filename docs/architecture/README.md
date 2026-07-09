# Architecture

This folder contains the public architecture subset for FratFinderAI.

## Recommended Reading Order

1. [Platform Architecture](./platform-architecture.md) - high-level system layers and ownership boundaries.
2. [Queue Architecture](./queue-architecture.md) - current queue, worker, and backpressure model.
3. [LangGraph Runtime](./langgraph-runtime.md) - graph runtime flow and supported live modes.
4. [Status Verification Model](./status-verification-model.md) - school-recognition-first chapter status engine.
5. [Search Provider Reliability](./search-provider-reliability.md) - provider health, SearXNG hardening, and managed backup strategy.

## Design Principles

- School recognition is the highest authority for final chapter active/inactive status.
- Contact enrichment is status-first and provenance-gated.
- Search providers are dependency managers, not sources of truth.
- Queue states should make blocked work explicit instead of hiding failure behind generic retries.
- Ambiguous or conflicting evidence should route to review rather than produce unsafe canonical writes.
- The web app should remain an operator/client layer; crawler business logic belongs in the Python service.
