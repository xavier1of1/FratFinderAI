# V3 Source Worker Graph

This diagram shows the end-to-end execution loop for one source crawl worker in the unified V3 runtime.

It combines:

- adaptive navigation
- inline search-backed enrichment
- chapter/entity evidence merge
- checkpoint-safe persistence
- stop conditions and deferred follow-up routing

## Execution Graph

```mermaid
flowchart TD
  A["1. load_or_resume_source_session<br/>restore source config, policy pack, checkpoints, budgets"] -->
  B["2. seed_frontier<br/>verified source URLs, known hints, discovery targets"]

  B --> C["3. claim_frontier_item<br/>pick highest-value queued URL"]
  C --> D["4. fetch_page<br/>HTML or API payload plus latency and transport diagnostics"]
  D --> E["5. analyze_page<br/>structure, source type, embedded data, chapter-index mode"]
  E --> F["6. build_entity_context<br/>prior provenance, evidence, field states, source trust"]
  F --> G["7. propose_actions<br/>extract, follow, expand, search, verify, defer, review, stop"]
  G --> H["8. score_actions<br/>heuristics plus template memory plus provider health plus policy pack"]
  H --> I["9. select_action<br/>live choice plus optional shadow comparison"]
  I --> J["10. execute_action<br/>dispatch selected tool family"]

  J --> K["11. extract_or_follow<br/>extract records or stubs, or follow detail pages"]
  K --> L["12. inline_search_enrichment<br/>website then email then Instagram search ladder"]
  L --> M["13. merge_evidence<br/>combine on-site plus search plus prior evidence"]
  M --> N["14. validate_and_normalize<br/>confidence gates, canonical fields, conflicts"]
  N --> O["15. persist_progress<br/>chapter writes, provenance, evidence, events, checkpoints"]
  O --> P["16. update_memory_and_rewards<br/>template profiles, provider health, rewards"]
  P --> Q["17. evaluate_stop_conditions<br/>budget, saturation, confidence completion"]
  Q -->|continue| C
  Q -->|stop| R["18. emit_followups_or_review<br/>queue deferred jobs, create review items"]
  R --> S["19. finalize_source_run<br/>summary, stop reason, metrics, policy snapshot link"]

  G --> G1["Action: extract_table"]
  G --> G2["Action: extract_repeated_block"]
  G --> G3["Action: extract_script_json"]
  G --> G4["Action: extract_locator_api"]
  G --> G5["Action: extract_stubs_only"]
  G --> G6["Action: follow_chapter_detail"]
  G --> G7["Action: expand_same_section_links"]
  G --> G8["Action: expand_internal_links"]
  G --> G9["Action: expand_map_children"]
  G --> G10["Action: search_official_website"]
  G --> G11["Action: search_contact_email"]
  G --> G12["Action: search_instagram"]
  G --> G13["Action: verify_candidate"]
  G --> G14["Action: queue_followup"]
  G --> G15["Action: review_branch"]
  G --> G16["Action: stop_branch"]
```

## Node Responsibilities

- `load_or_resume_source_session`: rehydrate source state, budgets, graph-run metadata, and checkpoint lineage.
- `build_entity_context`: attach existing chapter evidence and field state before deciding whether to extract, navigate, or search.
- `inline_search_enrichment`: keeps website, email, and Instagram recovery inside the main crawl loop instead of postponing all enrichment to field jobs.
- `persist_progress`: writes both canonical outputs and graph telemetry so the run can resume safely after failure.
- `emit_followups_or_review`: keeps deferred work bounded by policy instead of letting retries spin in-place.

## Stop Conditions

The worker should stop when one or more of the following becomes true:

- crawl budget exhausted
- low-yield or empty-page saturation reached
- source confidently completed
- provider degradation policy requires defer
- operator-imposed cancellation or supervisor stop signal received
