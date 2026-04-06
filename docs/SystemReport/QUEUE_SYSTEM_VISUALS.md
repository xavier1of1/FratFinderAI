# Queue System Visuals

## 1. Product Goal vs Implementation Reality

```mermaid
flowchart LR
    A[Source Recovery] --> B[Chapter Discovery]
    B --> C[Chapter Validity]
    C --> D[Chapter Repair]
    C --> E[Canonical Valid Chapter]
    E --> F[Contact Resolution]
    F --> G[Evidence / Review / Promotion]
    G --> H[Benchmarks / Learning]
```

The mission-correct architecture above is product-semantic.

Current implementation still behaves closer to this:

```mermaid
flowchart LR
    A[Request Graph] --> B[Crawl Runtime]
    B --> C[Create Chapter Rows]
    C --> D[Create Field Jobs]
    D --> E[Triage / Repair In Pipeline]
    E --> F[Contact Search Loop]
    F --> G[Evidence / Review]
    H[Next.js Benchmarks / Campaigns] --> I[Spawn Python]
    I --> F
```

## 2. Current Queue Ownership Map

```mermaid
flowchart TD
    UI[Next.js UI / API Routes]
    BR[Benchmark Runner]
    CR[Campaign Runner]
    RR[Request Runner]
    PY[Python CLI / CrawlService]
    RG[Request Supervisor Graph]
    AG[Adaptive / Legacy Crawl Graphs]
    FSG[Field Job Supervisor Graph]
    FJG[Field Job Graph / Engine]
    DB[(Postgres)]

    UI --> BR
    UI --> CR
    UI --> RR
    BR --> PY
    CR --> PY
    RR --> PY
    PY --> RG
    RG --> AG
    PY --> FSG
    FSG --> FJG
    AG --> DB
    RG --> DB
    FJG --> DB
    UI --> DB
```

Takeaway:

- web layer still owns long-running scheduling behavior
- Python owns some graphs and some imperative control
- database carries shared truth across all of it

## 3. Live Queue Concentrations

| Source | Actionable queued | Deferred queued | Blocked invalid failed |
|---|---:|---:|---:|
| `pi-kappa-alpha-main` | 3,270 | 42 | 1,027 |
| `sigma-alpha-epsilon-main` | 2,921 | 246 | 614 |
| `alpha-delta-gamma-main` | 983 | 0 | 787 |
| `delta-kappa-epsilon-main` | 595 | 0 | 0 |

```mermaid
xychart-beta
    title "Top Actionable Queue Sources"
    x-axis ["PKA","SAE","ADG","DKE"]
    y-axis "Queued jobs" 0 --> 3500
    bar [3270, 2921, 983, 595]
```

## 4. Field-Job Queue State Distribution

| Queue state / field | Count |
|---|---:|
| actionable `find_email` | 4,823 |
| actionable `find_instagram` | 3,937 |
| actionable `verify_website` | 927 |
| actionable `find_website` | 848 |
| deferred `find_website` | 288 |

```mermaid
pie showData
    title Field-Job Queue Mix
    "find_email actionable" : 4823
    "find_instagram actionable" : 3937
    "verify_website actionable" : 927
    "find_website actionable" : 848
    "find_website deferred" : 288
```

## 5. Runtime Footprint Comparison

| Runtime artifact | Count |
|---|---:|
| request graph runs | 4 |
| field-job graph runs | 104 |
| benchmark runs succeeded | 38 |
| benchmark runs failed | 16 |
| campaign runs succeeded | 3 |
| campaign runs failed | 7 |

Interpretation:

- field-job work dominates current runtime traffic
- evaluation and campaign control is still failure-prone
- request-graph footprint is small compared with downstream queue operations

## 6. Review-Reason Pressure

| Review reason | Count |
|---|---:|
| low-confidence `contact_email` candidate | 166 |
| low-confidence `website_url` candidate | 160 |
| placeholder/navigation chapter record | 71 |
| overlong chapter name | 32 |
| identity semantically incomplete | 31 |
| overlong slug | 31 |

```mermaid
xychart-beta
    title "Top Review Reasons"
    x-axis ["email low conf","website low conf","placeholder","overlong name","semantic incomplete","overlong slug"]
    y-axis "Count" 0 --> 180
    bar [166, 160, 71, 32, 31, 31]
```

## 7. Read-Path Anti-Pattern

```mermaid
flowchart LR
    A[Dashboard GET] --> B[API Route]
    B --> C[Read DB]
    B --> D[Fail stale runs]
    B --> E[Schedule due campaigns]
    B --> F[Reschedule running work]
```

Problem:

- read paths are mutating operational state
- observability and control are coupled

## 8. Missing First-Class Repair Lane

```mermaid
flowchart LR
    A[Chapter Validity Gate] --> B[Canonical Valid]
    A --> C[Repairable Candidate]
    A --> D[Invalid Candidate]

    B --> E[Contact Queue]
    C --> F[Inline Repair Logic In Pipeline]
    D --> G[Blocked / Review]
```

Current problem:

- repair exists as logic and counters
- repair does not yet exist as a durable queue lane with its own worker type

Target direction:

```mermaid
flowchart LR
    A[Chapter Validity Gate] --> B[Canonical Valid]
    A --> C[Repairable Candidate]
    A --> D[Invalid Candidate]

    B --> E[Contact Resolution Queue]
    C --> H[Repair Queue]
    H --> I[Promote / Downgrade / Confirm Invalid]
    D --> G[Blocked / Review]
```

## 9. Recommended Target Control Plane

```mermaid
flowchart TD
    UI[Web UI]
    API[Read/Write APIs]
    EVAL[Evaluation Worker]
    REQ[Request Worker]
    REP[Repair Worker]
    CON[Contact Worker]
    DB[(Postgres)]

    UI --> API
    API --> DB
    EVAL --> DB
    REQ --> DB
    REP --> DB
    CON --> DB
```

Target principles:

- the web app submits and observes
- backend workers own long-running scheduling
- each workload lane has its own concurrency and fairness
- queue-critical state is explicit and typed

## 10. Mission KPI Stack

Operational KPIs are necessary but not sufficient.

The KPI stack should look like this:

1. Product truth KPIs
   - true chapters found
   - false chapters suppressed
   - trusted website/email/Instagram coverage
   - review burden per true chapter
2. Workflow KPIs
   - repair yield
   - provisional promotion rate
   - evidence acceptance rate
   - actionable queue burn-down
3. Infrastructure KPIs
   - jobs/minute
   - requeue rate
   - cycle latency
   - worker saturation

If the system optimizes only layer 3, it can still be fast while doing the wrong work.
