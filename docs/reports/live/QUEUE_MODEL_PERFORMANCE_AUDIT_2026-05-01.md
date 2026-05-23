# Queue And Model Performance Audit - 2026-05-01

## Executive Summary

The queue is operationally alive and much healthier than earlier failure modes, but model-level productivity is currently constrained by evidence and identity gates rather than worker throughput.

Current live queue snapshot:

| Metric | Value |
| --- | ---: |
| queued_jobs | 6,403 |
| actionable_jobs | 911 |
| deferred_jobs | 0 |
| blocked_provider_jobs | 98 |
| blocked_dependency_jobs | 3,263 |
| blocked_repairable_jobs | 2,131 |
| running_jobs | 0 |
| active field workers | 1 |
| field worker alert open | false |

The important interpretation is that `running_jobs = 0` is not itself a failure right now. The field worker is alive, heartbeating, and cycling through phases such as `preflight` and `triage`. Jobs complete quickly enough that the instantaneous running count often returns to zero between batches.

## Provider Health

SearXNG is reachable but not useful at this moment:

| Endpoint | Reachable | JSON parseable | Result-bearing | Health reason |
| --- | --- | --- | --- | --- |
| `http://localhost:8888` | yes | yes | no | `challenge_or_anomaly`: `startpage` suspended by CAPTCHA |
| `http://localhost:8889` | yes | yes | no | `challenge_or_anomaly`: `startpage` suspended by CAPTCHA |

Latest `search-preflight --probes 4`:

| Metric | Value |
| --- | ---: |
| healthy | false |
| success_rate | 0.0 |
| provider_window_success_rate | 0.0 |
| viable_providers | 0 |
| dominant failure | `challenge_or_anomaly` then `provider_unavailable` cooldown |

This is not a local container-down problem. Both endpoints answer. The upstream SearXNG engine is suspended/CAPTCHA-blocked, so the crawler is correctly entering degraded mode.

## Recent Throughput

The latest live worker batch processed successfully:

| Metric | Value |
| --- | ---: |
| processed | 120 |
| requeued | 0 |
| failed_terminal | 0 |
| workers_executed | 4 |
| productive_yield | 0.0 |
| verify_school_provider_search_attempted | 0 |
| nationals_only_contact_rows | 0 |

The four-cycle stress test at `docs/reports/live/stress-20260430-210800/` produced:

| Metric | Value |
| --- | ---: |
| cycles | 4 |
| processed_total | 832 |
| requeued_total | 128 |
| failed_total | 0 |
| new_complete_total | 0 |
| new_inactive_total | 0 |
| average_productive_yield | 0.0 |

This proves the queue can drain work without crashing or requeue-storming, but it also proves the current cohort is mostly non-productive verification/triage work.

## Queue Blocker Mix

Top blocker reasons:

| Reason | Count | Lane |
| --- | ---: | --- |
| `status_unknown` | 1,913 | dependency blocked |
| `identity_semantically_incomplete` | 1,656 | repair backlog |
| `school_evidence_missing` | 547 | dependency blocked |
| `queued_for_entity_repair` | 475 | repair backlog |
| `status_review_required` | 375 | dependency blocked |
| `dependency_wait` | 327 | dependency blocked |
| `transient_network` | 95 | provider dependent |

Provider blocking is now small relative to the total queue: 98 blocked-provider jobs, about 1.5% of queued work. The major bottleneck is status/identity evidence.

## Model Decision Quality Signals

Recent `verify_school_match` decisions are mostly safe but low-yield:

| Decision status | Count in last 6h | Write allowed |
| --- | ---: | --- |
| `school_identity_verified_activity_unknown` | 8,695 | false |
| `verified` | 185 | true |

This is safer than the old behavior because the resolver is not pretending generic Greek-life evidence proves chapter activity. However, it means a huge amount of processing is confirming only school identity, not chapter activity. That does not unlock most downstream contact work.

Latest status-decision creation is very low volume:

| Status decision | Count in last 24h |
| --- | ---: |
| `unknown / no_conclusive_school_status_evidence` | 5 |
| `review / national_all_status_directory_inactive_school_unknown` | 3 |
| `active / recognized` | 2 |
| `inactive / unrecognized` | 1 |

This shows the missing piece: the system is running many school-verification jobs, but only a tiny number of durable `chapter_status_decisions` are being created.

## Canonical Data Snapshot

| Metric | Value |
| --- | ---: |
| total_chapters | 3,410 |
| active_chapters | 3,083 |
| inactive_chapters | 327 |
| website_rows | 918 |
| instagram_rows | 1,715 |
| email_rows | 309 |
| complete_contact_rows | 158 |
| inactive_validated_rows | 191 |
| nationals_only_contact_rows | 0 |

Safety remains good: `nationals_only_contact_rows = 0`. Coverage is still the weak point, especially email and fully complete contact rows.

## Hard Examples From The Backlog

Representative `status_unknown` blockers:

| Field | Chapter | School | Source |
| --- | --- | --- | --- |
| `find_email` | Alpha Alpha | Hobart College | unknown |
| `find_website` | Alpha Nu | University of Texas-Austin | `sigma-chi-main` |
| `find_website` | Alpha Omega | Stanford University | unknown |
| `find_email` | Alpha Theta | Massachusetts Institute of Technology | unknown |
| `find_email` | Alpha Upsilon | University of Southern California | `sigma-chi-main` |
| `find_website` | Beta Delta | University of Montana | `sigma-chi-main` |
| `find_email` | Beta Lambda | Duke University | `sigma-chi-main` |
| `find_email` | Delta Iota | University of Denver | unknown |
| `find_email` | Epsilon Nu | Texas Tech University | unknown |
| `find_email` | Eta Omega | Baylor University | unknown |

These are not random noise rows. Many look like valid active chapters, but the status-first gate blocks contact enrichment until official active status evidence exists.

## Main Bottlenecks

1. Status-decision creation is too slow relative to contact-job demand.
   - `status_unknown` blocks 1,913 jobs.
   - Only 11 chapter status decisions were created in the last 24h.

2. Identity repair remains a large backlog.
   - `identity_semantically_incomplete` blocks 1,656 jobs.
   - This is mostly correct isolation, but it means about a third of queued work cannot progress through normal field execution.

3. SearXNG is operationally reachable but upstream-blocked.
   - The configured `startpage` engine is CAPTCHA-suspended.
   - The current SearXNG-only chain is safer than noisy HTML fallback, but it means search-heavy jobs cannot progress until the engine recovers or an alternative high-quality provider is available.

4. Verify-school is safe but low-yield.
   - It avoids provider search, which is good.
   - It is producing mostly `school_identity_verified_activity_unknown`, which does not unlock contact jobs.

5. Provenance hygiene still has legacy debt.
   - Baseline reported 1,289 accepted rows missing reason codes.
   - `accepted_rows_matching_national_profile = 0`, so this is not a current nationals-leak safety issue, but it does limit explainability.

## Health Verdict

| Area | Verdict |
| --- | --- |
| Worker liveness | Healthy |
| Queue execution | Healthy |
| Requeue control | Improved |
| Provider health | Degraded |
| Contact safety | Healthy |
| Model productivity | Weak |
| Status evidence creation | Main bottleneck |
| Repair backlog | Large but isolated |

## Recommended Next Actions

1. Run a bounded `refresh-school-evidence` cohort for `status_unknown` and `school_evidence_missing`, then measure how many active status decisions are created and how many contact jobs unblock.
2. Add a specific KPI for `status_decisions_created_per_100_verify_school_jobs`; today this ratio is far too low.
3. Keep SearXNG throttled while `startpage` is suspended; do not promote low-quality engines just to make preflight green.
4. Focus the next implementation pass on converting official school roster evidence into durable `chapter_status_decisions`.
5. Backfill missing provenance reason codes separately; do not mix that with live throughput fixes.

## Artifacts

- Structured audit snapshot: `docs/reports/live/queue-model-performance-audit-20260501.json`
- Stress test artifacts: `docs/reports/live/stress-20260430-210800/`
- Latest field worker log: `docs/reports/live/field-job-worker-20260501-121716.err.log`
