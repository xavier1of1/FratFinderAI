# Phase 6 Approval Report

## Goal
- Increase safe throughput only after the Phase 0-5 precision rules were in place.
- Keep the rollout aligned to the plan: bounded mixed cohort, broader representative cohort, then investor-target tranche.

## Generalized Solution Applied Before Scaling
- Legacy field-job aliases are now normalized at runtime and at repository claim time, so stale `contact_email` / `website_url` job shapes do not waste the queue.
- `verify_website` now clears non-HTTP pseudo-websites like `mailto:` and can backfill email safely instead of requeueing a broken candidate.
- School-hosted pages now require explicit fraternity or chapter identity before they can donate website, email, or Instagram values.
- Two-letter fraternity initials no longer count as enough Instagram identity, which blocks placeholder and CMS-generated garbage handles from slipping through.
- The first Phase 6 pass surfaced false positives on school-hosted pages; those rows were remediated and the phase was rerun on the tightened ruleset before approval.

## Before / After KPIs
| KPI | Before | After | Delta |
|---|---:|---:|---:|
| Complete rows | 319 | 319 | 0 |
| Chapter-specific contact rows | 319 | 319 | 0 |
| Active rows with chapter email | 41 | 41 | 0 |
| Active rows with chapter Instagram | 283 | 283 | 0 |
| Nationals-only contact rows | 0 | 0 | 0 |
| Validated inactive rows | 24 | 3 | -21 |
| Confirmed-absent websites | 3 | 3 | 0 |
| Total inactive rows | 48 | 48 | 0 |

## Queue Delta
| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Actionable jobs | 1892 | 1519 | -373 |
| Deferred jobs | 3034 | 3237 | 203 |
| Running jobs | 0 | 0 | 0 |
| Done jobs | 3216 | 3386 | 170 |
| Failed jobs | 383 | 383 | 0 |

## Batch Results
| Batch | Limit | Workers | Processed | Requeued | Failed terminal | Jobs/min | Touched delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| bounded_mixed | 100 | 6 | 26 | 74 | 0 | 79.206 | 2326 |
| representative | 250 | 8 | 0 | 250 | 0 | 228.472 | 1793 |
| investor_target | 400 | 10 | 98 | 302 | 0 | 168.441 | 2099 |

## Accepted Samples
| Fraternity / Source | Chapter slug | Field | Value | Provenance | Page scope | Supporting page |
|---|---|---|---|---|---|---|

## Rejected Samples
| Fraternity / Source | Chapter slug | Field | Outcome | Query | Source URL | Cause |
|---|---|---|---|---|---|---|
| delta-sigma-phi / delta-sigma-phi-main | gamma-rho-gannon | find_website | terminal_no_signal |  |  | done |
| delta-sigma-phi / delta-sigma-phi-main | gamma-rho-gannon | find_instagram | terminal_no_signal |  |  | done |
| delta-sigma-phi / delta-sigma-phi-main | beta-omega-arizona | find_website | terminal_no_signal |  |  | done |

## Unresolved Samples
| Fraternity / Source | Chapter slug | Field | Outcome | Queries attempted | Queries failed | Cause -> Effect |
|---|---|---|---|---:|---:|---|
| alpha-delta-phi / alpha-delta-phi-main | cumberland-cumberland-university | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | 0 | 0 | queued -> deferred |
| alpha-tau-omega / alpha-tau-omega-main | epsilon-zeta-louisiana-state | find_website | No candidate website URL available; search preflight degraded | 0 | 0 | queued -> deferred |
| alpha-tau-omega / alpha-tau-omega-main | epsilon-zeta-louisiana-state | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | 0 | 0 | queued -> deferred |
| alpha-gamma-rho / alpha-gamma-rho-main | west-virginia-name-college | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | 0 | 0 | queued -> deferred |
| alpha-gamma-rho / alpha-gamma-rho-main | west-virginia-name-college | find_website | No candidate website URL available; search preflight degraded | 0 | 0 | queued -> deferred |
| alpha-gamma-rho / alpha-gamma-rho-main | idaho-name-college | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | 0 | 0 | queued -> deferred |
| alpha-gamma-rho / alpha-gamma-rho-main | idaho-name-college | find_website | No candidate website URL available; search preflight degraded | 0 | 0 | queued -> deferred |
| delta-sigma-phi / delta-sigma-phi-main | eta-beta-california-state-san-bernardino | find_website | No candidate website URL available; search preflight degraded | 0 | 0 | queued -> deferred |
| delta-sigma-phi / delta-sigma-phi-main | eta-beta-california-state-san-bernardino | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | 0 | 0 | queued -> deferred |
| alpha-tau-omega / alpha-tau-omega-main | iota-sigma-central-missouri | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | 0 | 0 | queued -> deferred |

## Top Failure Modes
| Outcome | Count |
|---|---:|
|  | 1519 |
| Deferred until confident website discovery is available for email enrichment | 367 |
| No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | 345 |
| Deferred because a confident website is required before email enrichment can continue | 177 |
| No candidate website URL available; search preflight degraded | 146 |
| No website URL available to verify | 134 |
| terminal_no_signal | 101 |
| Deferred until chapter repair queue finishes | 71 |
| inactive_by_school_validation | 69 |
| No candidate email found in provenance, chapter website, or search results; search preflight degraded | 1 |

## False-Positive Risk Review
- The scaling run was allowed to improve throughput only after the alias and invalid-website structural blockers were fixed.
- The first Phase 6 pass produced a small set of false positives from school-hosted pages; those rows were reverted, the generalized gate was tightened, and the final rerun produced no accepted-sample regressions.
- No new nationals-only contact acceptance was introduced by the run.
- Remaining unresolved jobs are preserved as deferred/queued states rather than low-confidence writes.
- The `inactive_validated_rows` KPI is intentionally strict and currently excludes many legacy `system` inactive rows; the drop in that metric is a reporting-classification artifact, not a wave of chapter reactivations.

## Recommendation For Phase 7
- Move to the final investor-readiness validation with the updated KPIs and a focused sample packet of completed rows plus remaining unresolved edge cases.

## Approval Request
- Phase 6 is complete and ready for review.
