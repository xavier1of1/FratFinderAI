# Post-Run KPI Report (V2.1 Runtime Proof)

Generated: `2026-04-01T23:22:21.410094+00:00`
Run scope: `crawl_runs.id BETWEEN 120 AND 173`
Cohort label: `target-cohort-v21`
Eval sources: `delta-kappa-epsilon-main, alpha-tau-omega-main, delta-sigma-phi-main`

## Execution Summary
- Applied missing migration columns before run (`0014`-`0016`).
- Fixed runtime blocker in `pipeline.py` (`import time`) that prevented `adaptive_train_loop` from starting.
- Ran real external train/eval benchmark:
  - `python -m fratfinder_crawler.cli adaptive-train-loop --rounds 2 --epochs-per-round 3 --runtime-mode adaptive_assisted --cohort-label target-cohort-v21 --train-sources "sigma-chi-main,chi-psi-main,kappa-delta-rho-main" --eval-sources "delta-kappa-epsilon-main,alpha-tau-omega-main,delta-sigma-phi-main" --report-dir docs/reports`

## KPI Summary (Eval Runs Only)
| Runtime | Runs | Records Seen | Records Upserted | Pages | Records/Page | Pages/Record | Jobs/Min | Review Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| legacy | 18 | 1968 | 1968 | 24 | 82.0000 | 0.0122 | 262.0750 | 0.0000 |
| adaptive_assisted | 18 | 1968 | 1968 | 90 | 21.8667 | 0.0457 | 449.7332 | 0.0000 |

- Throughput delta (`jobs/min`): `71.60%` (adaptive vs legacy)
- Queue-efficiency regression (`pages/record`): `275.00%` (adaptive vs legacy)

## Contact Coverage (From chapter_provenance for Eval Runs)
| Runtime | Chapters | Any Contact | Website | Email | Instagram | All Three | Any Contact Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| legacy | 1968 | 0 | 0 | 0 | 0 | 0 | 0.00% |
| adaptive_assisted | 1968 | 0 | 0 | 0 | 0 | 0 | 0.00% |

## Round Slope Snapshot
| Round | Balanced Score Slope | Jobs/Min Delta Slope |
| --- | ---: | ---: |
| Round 1 | 0.222550 | 11.127750 |
| Round 2 | -0.235150 | -11.759050 |

## Promotion Gate Evaluation
| Gate | Target | Result | Status |
| --- | --- | ---: | --- |
| Any-contact coverage | `>= 60%` | `0.00%` | FAIL |
| Balanced-score slope (latest round) | `>= 0` | `-0.235150` | FAIL |
| Queue regression (pages/record) | `<= 10%` | `275.00%` | FAIL |
| Two consecutive passing rounds | `required` | `no` | FAIL |

### Overall Promotion Decision: **FAIL**

## Interpretation
- Adaptive currently wins strongly on throughput (`jobs/min`) for this cohort window.
- Adaptive currently fails core promotion gates due to:
  - zero measured contact coverage in eval outputs,
  - significant queue-efficiency regression (more pages per record),
  - negative balanced-score slope in the latest round.
- Recommendation: keep adaptive in `adaptive_assisted` training mode and do not cut over to `adaptive_primary` yet.

## Artifacts
- `docs/reports/ADAPTIVE_TRAIN_LOOP_ROUND_01.md`
- `docs/reports/ADAPTIVE_TRAIN_LOOP_ROUND_02.md`
- `docs/reports/POST_RUN_KPI_REPORT_2026-04-01.md`
- `docs/reports/POST_RUN_KPI_REPORT_2026-04-01.json`
