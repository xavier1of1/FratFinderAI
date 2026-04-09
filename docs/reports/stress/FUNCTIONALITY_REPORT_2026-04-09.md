# Functionality Report: Six-Hour Stress Run

Date: `2026-04-09`

Run under analysis: `stress-20260409-full`

## Scope

This report summarizes the large stress run executed against every chapter in scope that was either missing all contact information (`website`, `email`, `instagram`) or already marked inactive. It combines the final live database state, the full stress JSONL timeline, the intermediate rerun artifacts, the current implemented architecture, and the patches landed during the run.

Primary evidence sources:

- `docs/reports/stress/stress-20260409-full.jsonl`
- `docs/reports/stress/stress-20260409-full.out`
- `docs/reports/stress/stress-20260409-post-hardstop.out`
- `docs/reports/stress/stress-20260409-post-dependency-deferral.out`
- `docs/reports/stress/stress-20260409-post-preserve-deferred.out`
- `docs/reports/stress/stress-20260409-post-email-prereq-fix.out`
- `docs/reports/stress/stress-20260409-final-clean-pass.out`
- `docs/Diagrams/CURRENT_IMPLEMENTED_QUEUE_ARCHITECTURE.md`
- `CHANGELOG.md`

## Executive Summary

The observed run window lasted `5:35:17` from `2026-04-09T03:52:30+00:00` to `2026-04-09T09:27:47+00:00`. Over that period the system moved from a hot queue with `2271` actionable jobs to a final state with `0` actionable and `0` running jobs. The remaining `1,879` queued jobs are intentionally deferred behind provider health, chapter-repair, or website-prerequisite gates rather than being left in a wasteful retry loop.

The run proved three things:

- The new architecture can survive large-load contact backlog processing without leaving a hot actionable tail.
- Provider degradation, not graph-runtime instability, was the dominant external constraint during the whole run.
- The most important gains came from queue correctness and prerequisite gating, not from increasing raw search volume.

## Architecture Under Test

The current implemented architecture is backend-owned and lane-based: Request Worker, Contact Worker, Repair Lane, and Evaluation Worker all claim durable work from Postgres-backed queues. For this stress run the active lanes were primarily:

- `field_jobs` via `FieldJobSupervisorGraphRuntime + FieldJobGraphRuntime`
- `chapter_repair_jobs` via the repair processing loop
- the new school policy / school activity caches used during validation

The web/operator layer was not the bottleneck in this run; the dominant behaviors were queue triage, provider preflight, contact graph execution, and repair admission.

## KPI Table

| KPI | Value | Meaning |
| --- | --- | --- |
| Observed run window | 2026-04-09T03:52:30+00:00 to 2026-04-09T09:27:47+00:00 | 5:35:17 wall clock from first run_started to final run_finished |
| Recorded batch count | 21 | 21 discrete stress batches captured in JSONL |
| Peak throughput | 104.40 jobs/min | Reached during dependency/repair-aware drain after queue hardening |
| Lowest throughput | 2.34 jobs/min | Observed when provider degradation dominated and nearly all work requeued |
| Average throughput | 60.37 jobs/min | Average across all 21 recorded batches |
| Total processed across batches | 454 | Batch-logged processed count, not just final done rows |
| Total requeued across batches | 4105 | Shows how much work was preserved instead of hard-failed |
| Actionable queue reduction | 2271 -> 0 | 100% reduction to zero actionable jobs |
| Deferred queue growth | 875 -> 1879 | Work moved from hot queue into intentional cooldown lanes |
| Done jobs growth | 314 -> 580 | Final stress-scope done rows increased by 266 from first recorded snapshot |
| Provider preflight health | 0 healthy / 21 degraded | All 21 recorded batches ran under unhealthy preflight conditions |
| Final deferred reasons | queued_for_entity_repair=492, provider_degraded=471, identity_semantically_incomplete=421, dependency_wait=281, transient_network=99, website_required=96, provider_low_signal=19 | Deferred work is dominated by repair, provider, and dependency gating rather than uncontrolled retries |
| Final failed reasons | ranking_or_report_row=3427, year_or_percentage_as_identity=2570, school_division_or_department=170, history_or_timeline_row=137, award_or_honor_row=3 | Failed rows are almost entirely blocked-invalid historical junk |
| School policy registry coverage | 134 schools | 35 allowed, 99 unknown, 0 banned persisted in this snapshot |
| Activity cache coverage | 43 fraternity/school pairs | 37 confirmed inactive, 6 confirmed active |
| Repair lane footprint | 708 repair jobs | 474 queued, 220 exhausted, 14 promoted to canonical |

## Subsystem Analysis

| Subsystem | Observed Behavior | Impact | Readout |
| --- | --- | --- | --- |
| Queue triage + typed queue state | Queue triage canceled 6,307 invalid rows and ended with 0 actionable jobs. | Triage turned the stress run into a bounded queue problem instead of a hot-loop retry storm. | Typed `queue_state` plus invalid/dependency/repair deferral is now the main reliability control surface. |
| Search preflight + provider routing | All 21 recorded batches had unhealthy preflight; deferred reasons include provider_degraded=471 and transient_network=99. | Provider collapse was the dominant external constraint throughout the run. | Preflight and hard-stop logic prevented the system from converting degraded search into runaway query fanout. |
| Contact resolution engine | Final done counts were website=342, instagram=157, email=81 within the stress cohort. | The engine still produces useful work under load, but email remains the most dependent lane. | Website-first gating and no-signal exits improved accuracy more than raw throughput. |
| Chapter repair lane | 708 repair jobs exist in the lane; 14 promoted_to_canonical_valid, 220 repair_exhausted, 474 still queued. | Repair absorbed ambiguous identities instead of forcing them through contact enrichment. | The repair lane is doing the right architectural job, but it is now the largest remaining deferred bucket. |
| School policy + chapter activity validation | Registry now holds 134 school policy rows and 43 fraternity/school activity decisions. | Validation is being persisted and reused instead of rediscovered ad hoc on every job. | Coverage is still mostly unknown/partial, but the subsystem is active and reducing duplicate validation work. |
| Graph runtime + supervisor | field_job_graph_runs: 386 succeeded langgraph_primary runs, average businessProgressCount 16.35. | The graph runtime stayed up and kept producing progress even while providers were degraded. | Runtime stability is no longer the bottleneck; search health and prerequisite ordering are. |
| Observability + reinforcement loop | 21 batch_progress records plus multiple targeted rerun artifacts documented each queue state transition. | The system produced enough telemetry to support rapid fix -> rerun -> verify loops during the same night. | The logged metrics were good enough to drive architecture improvements in real time. |

## Reinforcement / Adaptation Loop

This run was not a single static benchmark. It was a closed-loop stress test in which failures were observed, generalized fixes were applied, and the same cohort was rerun until the hot queue drained cleanly.

| When | Observation | Patch | Measured Effect |
| --- | --- | --- | --- |
| 03:56 UTC | Historical junk dominated the hot queue (5,306 invalid rows canceled in first recorded batch). | Added invalid-entity gate and sibling cancellation in field-job triage. | Invalid rows stopped consuming worker slots; failed rows became intentionally blocked_invalid instead of noisy retries. |
| 04:01-04:30 UTC | Provider layer stayed unhealthy, but jobs still risked fanout waste. | Added degraded-mode search skipping and provider hard-stop behavior. | Deferred jobs began landing with 0 chapter queries and explicit provider_degraded/transient_network reasons. |
| 07:31 UTC | Email jobs were still crowding the actionable queue while websites were unresolved. | Added proactive dependency deferral for email until a confident website is available. | Actionable queue dropped from 870 to 270 in one batch. |
| 08:20 UTC | Deferred canonical jobs were being resurrected as actionable during later reconciliation. | Preserved deferred canonical state instead of resetting it to actionable. | Actionable queue fell from the polluted 589 back to 110 after the next rerun. |
| 09:24 UTC | Email jobs without a website sibling could still sit actionable after website exhaustion. | Added website_required deferral when no confident website exists, even without a pending website job. | Actionable queue fell from 81 to 1; 96 email jobs were explicitly cooled down. |
| 09:27 UTC | Repair promotion could reactivate one last email outlier. | Applied the same website prerequisite inside the chapter-repair promotion path. | Final actionable queue reached 0. |

## Batch Timeline and Queue Dynamics

| Timestamp | Processed | Requeued | Jobs/min | Actionable | Deferred | Invalid Canceled | Dependency Deferred | Repair Queued | Top Outcomes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-04-09T03:56:25+00:00 | 217 | 23 | 61.32 | 2271 | 875 | 5306 | 0 | 856 | Canceled invalid historical field job: ranking_or_report_row=2861; Canceled invalid historical field job: year_or_percentage_as_identity=2179; actionable=1230 |
| 2026-04-09T04:01:31+00:00 | 2 | 238 | 66.18 | 930 | 1213 | 1900 | 0 | 645 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=972 |
| 2026-04-09T04:06:43+00:00 | 3 | 237 | 70.08 | 890 | 1246 | 0 | 0 | 1 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=973 |
| 2026-04-09T04:14:07+00:00 | 0 | 240 | 92.46 | 665 | 1471 | 0 | 0 | 791 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=973 |
| 2026-04-09T04:17:07+00:00 | 3 | 237 | 87.06 | 449 | 1684 | 0 | 0 | 638 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=970 |
| 2026-04-09T04:22:01+00:00 | 18 | 222 | 48.96 | 418 | 1696 | 0 | 0 | 0 | =413; No candidate instagram URL found in provenance, chapter website, or search results; search provider or network unavailable=79; Waiting for confident website discovery before email enrichment=71 |
| 2026-04-09T04:27:28+00:00 | 0 | 240 | 97.92 | 516 | 1598 | 0 | 0 | 1 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=970 |
| 2026-04-09T04:30:28+00:00 | 18 | 222 | 104.40 | 539 | 1557 | 0 | 0 | 723 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=967 |
| 2026-04-09T06:12:28+00:00 | 0 | 240 | 2.34 | 421 | 1675 | 0 | 0 | 625 | =421; Waiting for confident website discovery before email enrichment=127; Deferred until chapter repair queue finishes=94 |
| 2026-04-09T06:25:32+00:00 | 0 | 240 | 19.20 | 715 | 1375 | 0 | 0 | 1317 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=961 |
| 2026-04-09T07:19:50+00:00 | 1 | 238 | 34.08 | 909 | 1173 | 0 | 0 | 1293 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=952 |
| 2026-04-09T07:25:51+00:00 | 1 | 239 | 85.86 | 870 | 1211 | 0 | 0 | 0 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=949 |
| 2026-04-09T07:31:05+00:00 | 56 | 184 | 90.84 | 270 | 1755 | 0 | 526 | 1245 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=947 |
| 2026-04-09T07:33:54+00:00 | 41 | 199 | 99.12 | 50 | 1934 | 0 | 0 | 0 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=947 |
| 2026-04-09T08:16:15+00:00 | 25 | 215 | 5.82 | 64 | 1891 | 0 | 482 | 0 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=947 |
| 2026-04-09T08:20:23+00:00 | 0 | 80 | 38.52 | 589 | 1366 | 0 | 335 | 647 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=944 |
| 2026-04-09T08:25:59+00:00 | 0 | 240 | 87.00 | 351 | 1604 | 0 | 129 | 529 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=941 |
| 2026-04-09T08:30:21+00:00 | 16 | 224 | 97.86 | 110 | 1829 | 0 | 336 | 0 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=941 |
| 2026-04-09T09:17:50+00:00 | 48 | 192 | 5.10 | 81 | 1803 | 0 | 129 | 0 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=876 |
| 2026-04-09T09:24:04+00:00 | 5 | 115 | 53.76 | 1 | 1878 | 0 | 891 | 531 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=913 |
| 2026-04-09T09:27:47+00:00 | 0 | 40 | 19.86 | 0 | 1879 | 0 | 131 | 0 | Canceled invalid historical field job: ranking_or_report_row=3427; Canceled invalid historical field job: year_or_percentage_as_identity=2570; Deferred until chapter repair queue finishes=913 |

## Failure Mode Table A: Invalid Historical Field Jobs (15 examples)

| Request / Source | Chapter slug | Field | Query path / queries attempted | Result | Last error | Cause -> Effect |
| --- | --- | --- | --- | --- | --- | --- |
| stress-20260409-full / pi-kappa-alpha-main | applied-health-sciences-1895 | find_instagram | N/A - blocked in queue triage before search | year_or_percentage_as_identity | Canceled invalid historical field job: year_or_percentage_as_identity | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / pi-kappa-alpha-main | truman-school-of-public-affairs-2001-52 | find_instagram | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / alpha-gamma-rho-main | wirkkala-kenneth-1964 | find_website | N/A - blocked in queue triage before search | year_or_percentage_as_identity | Canceled invalid historical field job: year_or_percentage_as_identity | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / pi-kappa-alpha-main | physics-21 | find_email | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / pi-kappa-alpha-main | law-14-152 | find_email | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / sigma-alpha-epsilon-main | molecular-biology-genetics-135 | find_website | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / pi-kappa-alpha-main | low-income-b-12 | find_instagram | N/A - blocked in queue triage before search | year_or_percentage_as_identity | Canceled invalid historical field job: year_or_percentage_as_identity | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / pi-kappa-alpha-main | economics-business-5 | find_website | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / pi-kappa-alpha-main | clinical-medicine-100 | find_instagram | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / alpha-delta-gamma-main | 1975-fil-fuentes-memorial-charitable-activities-award | find_instagram | N/A - blocked in queue triage before search | year_or_percentage_as_identity | Canceled invalid historical field job: year_or_percentage_as_identity | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / sigma-alpha-epsilon-main | supply-chain-management-logistics-4 | find_instagram | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / sigma-alpha-epsilon-main | physics-71 | find_website | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / sigma-alpha-epsilon-main | production-operation-management-12 | find_instagram | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / pi-kappa-alpha-main | average-age-20 | find_instagram | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |
| stress-20260409-full / sigma-alpha-epsilon-main | law-43-tie | find_website | N/A - blocked in queue triage before search | ranking_or_report_row | Canceled invalid historical field job: ranking_or_report_row | Historical junk would have consumed a worker slot -> triage converted it into blocked_invalid and removed it from the hot queue. |

## Failure Mode Table B: Provider Degradation and Network Failure Deferrals (15 examples)

Shared probe bundle observed repeatedly in stress artifacts:

- `"sigma chi" University of Virginia instagram`
- `"delta chi" Mississippi State chapter website`
- `"lambda chi alpha" Purdue contact email`
- `"phi gamma delta" chapter directory`

| Request / Source | Chapter slug | Field | Query path / queries attempted | Provider failure evidence | Result | Cause -> Effect |
| --- | --- | --- | --- | --- | --- | --- |
| stress-20260409-full / sigma-chi-main | theta-eta-missouri-university-of-science-technology | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=7, request_error=1, circuit_open=6 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / delta-kappa-epsilon-main | beta-delta-university-of-georgia | find_website | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate website URL available; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / theta-xi-main | gamma-mu-montclair-state-university | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / sigma-chi-main | sigma-chi-upsilon-belmont-university | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / phi-gamma-delta-main | kappa-chi-chapter-william-woods-university | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / alpha-tau-omega-main | lambda-epsilon-kennesaw-state | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / delta-kappa-epsilon-main | psi-the-university-of-alabama | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / sigma-alpha-epsilon-main | virginia-omicron-university-of-virginia | find_website | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=4, request_error=4, circuit_open=3 | No candidate website URL available; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / delta-kappa-epsilon-main | iota-centre-college | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / phi-gamma-delta-main | lambda-tau-provisional-chapter-texas-tech-university | find_website | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate website URL available; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / delta-kappa-epsilon-main | miami-university-kappa-miami-university-kappa | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / phi-gamma-delta-main | sigma-tau-chapter-university-of-washington | find_instagram | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / delta-kappa-epsilon-main | beta-phi-university-of-rochester | find_website | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate website URL available; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / theta-xi-main | alpha-psi-missouri-university-of-science-technology | find_website | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate website URL available; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |
| stress-20260409-full / delta-kappa-epsilon-main | university-of-tennessee-lambda-tau-university-of-tennessee-lambda-tau | find_website | 0 chapter queries; shared preflight probes only: "sigma chi" University of Virginia instagram; "delta chi" Mississippi State chapter website; "lambda chi alpha" Purdue contact email; "phi gamma delta" chapter directory | attempts=8, unavailable=8, request_error=0, circuit_open=8 | No candidate website URL available; search preflight degraded | Provider health was already collapsed -> the job skipped low-yield fanout and cooled down safely instead of burning more search budget. |

## Failure Mode Table C: Website-Prerequisite / Dependency Deferrals (15 examples)

| Request / Source | Chapter slug | Field | Query path / queries attempted | Reason code | Current website state | Cause -> Effect |
| --- | --- | --- | --- | --- | --- | --- |
| stress-20260409-full / chi-psi-main | delta-delta | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / chi-psi-main | xi | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / chi-psi-main | lambda | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / chi-psi-main | iota-delta-i | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / sigma-chi-main | kappa-phi-embry-riddle-aeronautical-university-prescott | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / chi-psi-main | kappa-k | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / theta-chi-main | beta-chi-allegheny-college | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / sigma-alpha-epsilon-main | mississippi-theta-mississippi-state-university | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / delta-kappa-epsilon-main | gannon-university-gamma-iota-gannon-university-gamma-iota | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / sigma-chi-main | nu-cumberland-university | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / delta-kappa-epsilon-main | miami-university-kappa-miami-university-kappa | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / theta-chi-main | beta-upsilon-california-state-university-fresno | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / delta-kappa-epsilon-main | beta-gamma-new-york-university | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / theta-xi-main | omega-washington-state-university | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |
| stress-20260409-full / theta-chi-main | beta-gamma-university-of-north-dakota | find_email | No email search started; website prerequisite unresolved | dependency_wait | website_url=null, website_state=missing | Email would have been low-signal without a confident website -> queue triage kept it deferred instead of falsely actionable. |

## Failure Mode Table D: Chapter Repair and Identity-Incomplete Deferrals (15 examples)

| Request / Source | Chapter slug | Field | Query path / queries attempted | Reason code | Repair state | Cause -> Effect |
| --- | --- | --- | --- | --- | --- | --- |
| stress-20260409-full / theta-chi-main | kappa-nu-colorado | find_email | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | eta-psi-west-florida | find_instagram | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | eta-psi-west-florida | find_website | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | eta-psi-west-florida | find_email | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | tarleton-state | find_website | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | tarleton-state | find_instagram | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | tarleton-state | find_email | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | epsilon-delta-north-dakota-state | find_website | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | epsilon-delta-north-dakota-state | find_instagram | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | epsilon-delta-north-dakota-state | find_email | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / pi-kappa-alpha-main | public-health-56-hershey | find_email | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | iota-nu-uc-santa-barbara | find_website | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | iota-nu-uc-santa-barbara | find_instagram | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / alpha-tau-omega-main | iota-nu-uc-santa-barbara | find_email | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |
| stress-20260409-full / theta-chi-main | theta-delta-santa-clara | find_website | No contact search; chapter routed through repair lane | identity_semantically_incomplete | repair_exhausted | Chapter identity remained incomplete after repair work -> contact jobs stayed deferred so ambiguous entities did not pollute chapter/contact data. |

## Failure Mode Table E: Terminal No-Signal / Not-Enough-Identity Outcomes (15 examples)

| Request / Source | Chapter slug | Field | Query path / queries attempted | Reason code | Result | Cause -> Effect |
| --- | --- | --- | --- | --- | --- | --- |
| stress-20260409-full / alpha-tau-omega-main | eta-omega-montevallo | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | cached_no_signal | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / delta-kappa-epsilon-main | auburn-university-delta-chi-auburn-university-delta-chi | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | cached_no_signal | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / theta-xi-main | kappa-rose-hulman-institute-of-technology | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | cached_no_signal | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / delta-kappa-epsilon-main | depaul-university-alpha-beta-depaul-university-alpha-beta | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | cached_no_signal | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / theta-xi-main | delta-alpha-arizona-state-university | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | cached_no_signal | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / sigma-alpha-epsilon-main | new-york-beta-liu-post | find_website | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / theta-chi-main | kappa-alpha-spokane-wa | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / theta-chi-main | kappa-alpha-spokane-wa | find_website | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / chi-psi-main | zeta-z | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / alpha-gamma-rho-main | undergradute-brothers-attending | find_website | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / alpha-gamma-rho-main | undergradute-brothers-attending | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / chi-psi-main | nu-delta-n | find_website | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / chi-psi-main | nu-delta-n | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / theta-chi-main | alpha-gamma-colony-michigan | find_instagram | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |
| stress-20260409-full / theta-chi-main | alpha-gamma-colony-michigan | find_website | attempted=0, succeeded=0, failed=0; path=job_started -> load_context -> admission_gate | not_enough_identity | terminal_no_signal | The job ended with terminal no-signal because identity or context was too weak to justify more search work. |

## Provider and Search Findings

The search-provider subsystem was degraded for all 21 recorded batches. The most important operational consequence is that later-stage improvements were not about finding more data; they were about preventing the system from wasting queue budget when provider health was already known to be bad.

Key provider findings:

- Every recorded `search_preflight_completed` snapshot was unhealthy.
- Deferred reasons attributable to provider health totaled `570` rows (`provider_degraded=471`, `transient_network=99`).
- After the hard-stop patch, many jobs ended with `0` attempted chapter queries because the system relied on preflight evidence instead of performing doomed fanout.
- The most common provider signatures were `HTTPError`, `ConnectTimeout`, `challenge_or_anomaly`, and `circuit_open` after fallback escalation.

## School Policy and Chapter Activity Findings

The new school-policy subsystem is active but still early in its coverage. The live database snapshot at report time shows:

| Metric | Value |
| --- | --- |
| `school_greek_life_registry` total rows | 134 |
| `school_greek_life_registry.allowed` | 35 |
| `school_greek_life_registry.unknown` | 99 |
| `fraternity_school_activity_cache` total rows | 43 |
| `fraternity_school_activity_cache.confirmed_inactive` | 37 |
| `fraternity_school_activity_cache.confirmed_active` | 6 |

This subsystem prevented further drift from false inactive/active assumptions, but the registry still has much more `unknown` than `allowed`. That means it is functioning, but still growing into the backlog.

## Graph Runtime Findings

| Runtime mode | Status | Count | Avg business progress | Avg processed | Avg requeued |
| --- | --- | --- | --- | --- | --- |
| langgraph_primary | failed | 12 | 0.00 | 0.00 | 0.00 |
| langgraph_primary | partial | 11 | 11.00 | 4.91 | 5.64 |
| langgraph_primary | succeeded | 364 | 17.05 | 3.16 | 14.39 |
| langgraph_shadow | failed | 6 | 0.00 | 0.00 | 0.00 |
| langgraph_shadow | succeeded | 22 | 0.00 | 0.82 | 3.50 |

Interpretation:

- `langgraph_primary` was the meaningful production path during the run.
- The graph runtime mostly succeeded; the main source of requeues was external provider health and prerequisite gating, not runtime crashes.
- `langgraph_shadow` had near-zero business progress and was not the path driving the queue cleanup.

## Overall Judgment

The six-hour stress run did not show a perfect contact-enrichment engine. It showed a resilient queue system that became materially more correct over the course of the run. The biggest improvements were not ?more searches?; they were:

- removing invalid historical work from the queue
- stopping search fanout under degraded provider conditions
- enforcing website prerequisites before email
- preventing deferred jobs from oscillating back to actionable
- keeping repaired-but-still-weak chapters out of the hot email path

By the end of the run, the system had reached the right terminal operational state for this cohort: `0` actionable jobs, `0` running jobs, and all remaining work explicitly deferred for a known reason.
