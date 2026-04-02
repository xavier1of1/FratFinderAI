# Stop-Point Phase 2 Summary (2026-04-01)

Generated: `2026-04-02T01:06Z`

## Scope
This phase continued from the 45-minute benchmark stopping point and focused on hardening eval-time behavior when search/network providers are degraded.

## Validation Runs
- `adaptive-train-eval` (phase2 pre-fix): report `docs/reports/ADAPTIVE_EPOCH_REPORT_STOPPOINT_PHASE2_2026-04-01.md`
  - Run IDs: train `183`, eval legacy `184`, eval adaptive `185`
- `adaptive-train-eval` (phase2b post-fix): report `docs/reports/ADAPTIVE_EPOCH_REPORT_STOPPOINT_PHASE2B_2026-04-01.md`
  - Run IDs: train `186`, eval legacy `187`, eval adaptive `188`

## Root Cause Observed During Stop-Point Validation
- Source fetches and search fanout repeatedly failed with `WinError 10013` (socket access forbidden).
- Search preflight for the run window reported `healthy=false`, `success_rate=0.0`, and `successes=0/4` probes.
- This state can trigger wasted enrichment churn (high requeue with zero processed) if not explicitly gated.

## Implemented Changes In This Phase
1. Added adaptive eval enrichment health controls (config + env):
   - `Agent:ADAPTIVE_EVAL_ENRICHMENT_RUN_PREFLIGHT`
   - `Agent:ADAPTIVE_EVAL_ENRICHMENT_REQUIRE_HEALTHY_SEARCH`
2. Added deterministic per-epoch enrichment gate in `adaptive_train_eval`:
   - one shared preflight decision is computed and reused for both legacy and adaptive eval passes.
3. Added enrichment skip telemetry:
   - `skipped_provider_degraded` in eval enrichment summary.
   - event `eval_enrichment_skipped_provider_degraded` with attached preflight snapshot.
4. Added regression tests for this behavior:
   - `services/crawler/src/fratfinder_crawler/tests/test_pipeline_eval_enrichment.py`.
5. Updated docs/config artifacts:
   - `README.md` (new Agentic env vars)
   - `.env.example` (new Agentic env vars)
   - `CHANGELOG.md` (fixed outage churn note)

## Before/After (Targeted)
Using single-source stop-point validation (`delta-sigma-phi-main` eval):

- Pre-fix (phase2):
  - `evalLegacy.enrichment`: `processed=0`, `requeued=0`, `skipped_provider_degraded=1`
  - `evalAdaptive.enrichment`: `processed=0`, `requeued=50`, `skipped_provider_degraded=0`

- Post-fix (phase2b):
  - `evalLegacy.enrichment`: `processed=0`, `requeued=0`, `skipped_provider_degraded=1`
  - `evalAdaptive.enrichment`: `processed=0`, `requeued=0`, `skipped_provider_degraded=1`

Result: asymmetric requeue churn during provider outage was removed for both eval paths.

## Current KPI Status
- Coverage and throughput KPIs remain `0` in this validation window because upstream crawl and search egress were blocked (`WinError 10013`).
- The phase objective here was reliability under degraded provider conditions, and that objective passed.

## Next Phase (Execution)
1. Restore crawl/search network egress in the runtime environment (or run on a host where outbound sockets are permitted).
2. Re-run train/eval loop on target cohort in `adaptive_assisted`:
   - at least `2` rounds x `3` epochs.
3. Regenerate post-run KPI report and reassess promotion gates (`>=60% any-contact`, slope non-negative, queue regression <=10%).
