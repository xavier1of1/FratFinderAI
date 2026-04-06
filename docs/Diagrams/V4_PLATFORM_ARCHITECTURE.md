# V4 Platform Architecture (High-Fidelity)

This visual shows the entire platform as a layered runtime system, from operators and APIs down to execution engines and storage.

Goals of this version:

- larger text for readability
- stricter layer boundaries
- explicit runtime ownership (LangGraph vs non-LangGraph)
- clear data and telemetry flow

## Full Platform Visual

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
    "nodeSpacing": 70,
    "rankSpacing": 90
  }
}}%%
flowchart TB

  subgraph L0["1) Operator + External Layer"]
    OP["Operator UX<br/>Web Dashboard / CLI"]
    EXTWEB["External Web Targets<br/>Fraternity sites + school pages"]
    EXTSEARCH["External Search Providers<br/>SearXNG / Tavily / Serper / Bing / DDG / Brave"]
    EXTLLM["LLM Providers<br/>classification + extraction fallback"]
  end

  subgraph L1["2) Control + Scheduling Layer"]
    API["Next.js API Layer<br/>runs, benchmarks, campaigns, requests"]
    BMR["Benchmark Runner<br/>cycle orchestration + KPI capture"]
    CAM["Campaign Runner<br/>multi-request admission + tuning"]
    REQ["Fraternity Request Runner<br/>crawl + enrichment stage loop"]
    CSV["Crawler Service<br/>runtime selection + batch execution"]
  end

  subgraph L2["3) Orchestration Runtime Layer"]
    LGLEG["LangGraph Runtime A<br/>CrawlOrchestrator (legacy graph)"]
    LGADP["LangGraph Runtime B<br/>AdaptiveCrawlOrchestrator"]
    FJENG["FieldJobEngine Runtime<br/>imperative job loop (non-graph)"]
  end

  subgraph L3["4) Deterministic Execution Layer"]
    HTTP["HTTP Fetch Stack<br/>HttpClient + requests sessions + pacing"]
    ANALYSIS["Analysis Stack<br/>page analysis, source classification, index mode"]
    ADAPTERS["Adapter Stack<br/>table / repeated_block / script_json / locator_api"]
    NAV["Navigation Stack<br/>stub extraction + detail-follow + contact hints"]
    SEARCH["Search Stack<br/>provider chain + result/page fetch + filters"]
    NORMALIZE["Normalization + Sanitization<br/>canonical chapter + candidate sanitizer"]
    DISCOVERY["Discovery Stack<br/>source discovery + verified source workflows"]
  end

  subgraph L4["5) Data Contract + Repository Layer"]
    REPO["CrawlerRepository<br/>claim, persist, requeue, telemetry writes"]
    CONTRACTS["JSON-Schema Contracts<br/>chapter / provenance / review / field-job payloads"]
    TYPES["Shared Types + Models<br/>crawler models + web API response contracts"]
  end

  subgraph L5["6) Storage + Telemetry Layer (Postgres / Supabase)"]
    CORE["Core Crawl Data<br/>sources / crawl_runs / chapters / chapter_provenance"]
    QUEUE["Enrichment Queue Data<br/>field_jobs / review_items / review_item_audit_logs"]
    ADAPT["Adaptive Telemetry Data<br/>crawl_sessions / frontier / observations / rewards / policy snapshots / epoch metrics"]
    OPS["Operations Data<br/>benchmark_runs / campaign_runs / fraternity_crawl_requests"]
  end

  OP --> API
  OP --> CSV

  API --> BMR
  API --> CAM
  API --> REQ

  BMR --> CSV
  CAM --> REQ
  REQ --> CSV

  CSV --> LGLEG
  CSV --> LGADP
  CSV --> FJENG

  LGLEG --> HTTP
  LGLEG --> ANALYSIS
  LGLEG --> ADAPTERS
  LGLEG --> NAV
  LGLEG --> SEARCH
  LGLEG --> NORMALIZE

  LGADP --> HTTP
  LGADP --> ANALYSIS
  LGADP --> ADAPTERS
  LGADP --> NAV
  LGADP --> SEARCH
  LGADP --> NORMALIZE

  FJENG --> SEARCH
  FJENG --> HTTP
  FJENG --> NORMALIZE
  CSV --> DISCOVERY

  ANALYSIS -. fallback .-> EXTLLM
  ADAPTERS -. fallback .-> EXTLLM

  HTTP --> EXTWEB
  SEARCH --> EXTSEARCH

  LGLEG --> REPO
  LGADP --> REPO
  FJENG --> REPO
  DISCOVERY --> REPO
  REPO --> CONTRACTS
  REPO --> TYPES

  REPO --> CORE
  REPO --> QUEUE
  REPO --> ADAPT
  REPO --> OPS
```

## Quick Read

- LangGraph currently powers crawl runtimes (`legacy` + `adaptive`) but not field-job execution.
- Field jobs remain the largest imperative control-flow island.
- Benchmarks and campaign workflows execute crawler commands and feed back into the same queue and telemetry substrate.
