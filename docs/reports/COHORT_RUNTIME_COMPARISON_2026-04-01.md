# Cohort Runtime Comparison (2026-04-01)

## Scope
- Modes: `legacy` vs `adaptive_shadow`
- Sources: `sigma-chi-main`, `chi-psi-main`, `kappa-delta-rho-main`, `delta-kappa-epsilon-main`, `alpha-tau-omega-main`, `delta-sigma-phi-main`
- Command log: `docs/reports/cohort_runtime_commands_2026-04-01.log`

## Aggregate
- Legacy pages processed: 96
- Adaptive pages processed: 29 (-69.79%)
- Legacy records seen: 466
- Adaptive records seen: 466 (0%)
- Legacy records/page: 4.85
- Adaptive records/page: 16.07 (231.34%)

## Per-Source
| Source | Legacy pages | Adaptive pages | Legacy records | Adaptive records | Legacy records/page | Adaptive records/page | Page delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| sigma-chi-main | 1 | 4 | 0 | 0 | 0 | 0 | 3 |
| chi-psi-main | 30 | 5 | 51 | 51 | 1.7 | 10.2 | -25 |
| kappa-delta-rho-main | 61 | 5 | 87 | 87 | 1.43 | 17.4 | -56 |
| delta-kappa-epsilon-main | 1 | 5 | 108 | 108 | 108 | 21.6 | 4 |
| alpha-tau-omega-main | 2 | 5 | 126 | 126 | 63 | 25.2 | 3 |
| delta-sigma-phi-main | 1 | 5 | 94 | 94 | 94 | 18.8 | 4 |

## Notes
- Adaptive shadow preserved records seen/upserted on all six sources in this run.
- Adaptive shadow required significantly fewer pages overall, with one exception (`sigma-chi-main`) where both modes produced zero records and adaptive explored more pages before saturation.
- Event logs include intermittent SSL/network warnings on some chapter detail pages; these did not prevent successful completion for the five productive sources.
