# V3 Decision Trees

This file shows the highest-impact decision logic in the current architecture: source recovery, crawl fallback, and evidence write behavior.

## Source Recovery Decision Tree

```mermaid
flowchart TD
  A["load request context"] --> B{"source missing or weak?"}
  B -->|no| C["continue to crawl"]
  B -->|yes| D{"recovery attempts < configured limit?"}
  D -->|no| E["set awaiting_confirmation and pause request"]
  D -->|yes| F["run source discovery and score candidates"]
  F --> G{"stronger trustworthy source found?"}
  G -->|no| E
  G -->|yes| H["upsert source, update request progress, continue to crawl"]
```

## Crawl Runtime Fallback Decision Tree

```mermaid
flowchart TD
  A["start crawl with configured runtime mode"] --> B["sync crawl progress"]
  B --> C{"recordsSeen > 0 ?"}
  C -->|yes| D["continue to enrichment or finalize"]
  C -->|no| E{"runtime mode is non-legacy and fallback unused?"}
  E -->|yes| F["emit runtime_fallback event and retry crawl in legacy mode"]
  F --> B
  E -->|no| G{"source recovery still possible?"}
  G -->|yes| H["set recovery_reason and return to recover_source node"]
  G -->|no| I["set awaiting_confirmation or fail terminally"]
```

## Field-Level Evidence Decision Tree

```mermaid
flowchart TD
  A["candidate contact value found"] --> B["verify identity and trust"]
  B --> C{"confidence and trust high?"}
  C -->|yes| D["allow canonical write"]
  C -->|no| E{"confidence medium or conflict present?"}
  E -->|yes| F["persist chapter_evidence and route to review"]
  E -->|no| G{"provider degraded or dependency blocked?"}
  G -->|yes| H["defer or requeue bounded follow-up"]
  G -->|no| I["mark as no-candidate or terminal failure"]
```

## Enrichment Continuation Decision Tree

```mermaid
flowchart TD
  A["run enrichment cycle"] --> B["sync enrichment progress"]
  B --> C{"remaining queue == 0?"}
  C -->|yes| D["mark request completed"]
  C -->|no| E{"cyclesCompleted >= maxEnrichmentCycles?"}
  E -->|yes| F["mark budget_exhausted and fail request"]
  E -->|no| G{"degraded cycle count > 0?"}
  G -->|yes| H["pause by policy and continue next cycle"]
  G -->|no| I["continue next cycle immediately"]
```

