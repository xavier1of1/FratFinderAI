# V4 LangGraph Logic Map (Current Runtime)

This visual maps the **actual LangGraph logic currently implemented** in the crawler service:

- runtime selection in `CrawlService`
- legacy crawl graph topology
- adaptive crawl graph topology and loop
- explicit note for field-job path (currently non-LangGraph)

## LangGraph Runtime Map

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
    "fontSize": "22px",
    "fontFamily": "Segoe UI, Arial, sans-serif",
    "lineColor": "#1f2937",
    "primaryTextColor": "#111827",
    "clusterBkg": "#f8fafc",
    "clusterBorder": "#334155"
  },
  "flowchart": {
    "curve": "basis",
    "nodeSpacing": 65,
    "rankSpacing": 90
  }
}}%%
flowchart TB

  subgraph ENTRY["A) Runtime Selection Path"]
    CLI["CLI Commands<br/>run / run-legacy / run-adaptive"]
    SRV["CrawlService.run(...)"]
    SEL["_resolve_runtime_mode + _build_orchestrator"]
  end

  subgraph LEG["B) LangGraph: CrawlOrchestrator (Legacy)"]
    L1["fetch_page"]
    L2["analyze_page_structure"]
    L3["classify_source_type"]
    L4["detect_embedded_data"]
    L5["detect_chapter_index_mode"]
    L6["extract_chapter_stubs"]
    L7["follow_chapter_detail_or_outbound"]
    L8["extract_contacts_from_chapter_site"]
    L9["choose_extraction_strategy"]
    L10["extract_records"]
    L11["validate_records"]
    L12["normalize_records"]
    L13["persist_records"]
    L14["spawn_followup_jobs"]
    L15["finalize -> END"]
  end

  subgraph ADP["C) LangGraph: AdaptiveCrawlOrchestrator"]
    A1["initialize_session"]
    A2["load_session_checkpoint"]
    A3["seed_frontier"]
    A4["select_frontier_item"]
    A5["fetch_page_http"]
    A6["analyze_page"]
    A7["compute_template_signature"]
    A8["propose_actions"]
    A9["score_actions"]
    A10["execute_action"]
    A11["extract_records_or_stubs"]
    A12["expand_frontier"]
    A13["score_reward"]
    A14["update_template_memory"]
    A15["update_policy_state"]
    A16["persist_checkpoint"]
    A17["evaluate_stop_conditions"]
    A18["finalize -> END"]
  end

  subgraph NOTE["D) Current Non-Graph Path"]
    FJ["process_field_jobs<br/>ThreadPoolExecutor -> FieldJobEngine.process<br/>(imperative orchestration)"]
  end

  CLI --> SRV --> SEL
  SEL -->|runtime_mode = legacy| L1
  SEL -->|runtime_mode = adaptive_*| A1
  SEL -->|field-job command| FJ

  L1 --> L2 --> L3 --> L4 --> L5 --> L6 --> L7 --> L8 --> L9 --> L10 --> L11 --> L12 --> L13 --> L14 --> L15
  L1 -. any error state .-> L15
  L2 -. any error state .-> L15
  L3 -. any error state .-> L15
  L4 -. any error state .-> L15
  L5 -. any error state .-> L15
  L6 -. any error state .-> L15
  L7 -. any error state .-> L15
  L8 -. any error state .-> L15
  L9 -. any error state .-> L15
  L10 -. any error state .-> L15
  L11 -. any error state .-> L15
  L12 -. any error state .-> L15
  L13 -. any error state .-> L15

  A1 --> A2 --> A3
  A3 -->|done| A18
  A3 -->|continue| A4
  A4 -->|done| A18
  A4 -->|continue| A5
  A5 -->|error| A18
  A5 -->|ok| A6
  A6 -->|error| A18
  A6 -->|ok| A7 --> A8 --> A9 --> A10 --> A11 --> A12 --> A13 --> A14 --> A15 --> A16
  A16 -->|error| A18
  A16 -->|ok| A17
  A17 -->|done| A18
  A17 -->|continue loop| A4
```

## What This Diagram Clarifies

- There are currently **two LangGraph runtimes** in crawl execution.
- Adaptive runtime is truly loop-based with checkpoint and stop-condition control.
- Field-job orchestration is still outside LangGraph, which is the main architecture split.
