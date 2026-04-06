# System Report

This folder captures the April 6, 2026 deep system review of FratFinderAI's queue-driven crawl platform.

The report package focuses on the systems behind:

- request processing
- crawl execution
- chapter discovery and validity
- chapter repair
- contact-resolution field jobs
- benchmarks and campaigns
- review, evidence, and provisional workflows

The review was written with the product mission in mind:

- find real fraternity chapters
- recover trustworthy website, email, and Instagram contact info
- avoid creating junk chapter rows
- avoid wasting queue and provider budget on non-chapter work

## Contents

- [Architecture Audit 2026-04-06](./ARCHITECTURE_AUDIT_2026-04-06.md)
  - Deep written analysis of architecture, anti-patterns, mission alignment, scalability risks, feature gaps, and recommended target shape.
- [Queue System Visuals](./QUEUE_SYSTEM_VISUALS.md)
  - Diagrams, queue topology sketches, KPI tables, and summary visuals for the current vs target architecture.
- [Evidence Index](./EVIDENCE_INDEX.md)
  - Files, logs, APIs, and database snapshots used during the audit.

## Snapshot Date

- Audit date: `2026-04-06`
- Repository root: `d:\VSC Programs\FratFinderAI`

## Headline Takeaways

- The system is improving at filtering bad work, but queue ownership is still split across Python graphs, imperative worker code, and the Next.js app.
- Operational state is still too dependent on mutable JSON payloads rather than durable typed workflow columns.
- The conceptual product strategy is strong, but the implementation still centers too much behavior around queue mechanics rather than chapter truth and contact truth.
- The largest remaining structural gaps are:
  - web-owned scheduling and state mutation from read paths
  - lack of a first-class repair queue
  - incomplete provisional promotion loop
  - benchmark and campaign execution that is not fully isolated from live system conditions

## Intended Use

Use this folder as:

- a current-state architecture record
- a gap analysis between product strategy and implementation
- a planning input for the next major queue/control-plane refactor
