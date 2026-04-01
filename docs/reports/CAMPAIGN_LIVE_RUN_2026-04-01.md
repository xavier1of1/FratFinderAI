# Campaign Live Run 2026-04-01

## Scope
- Campaign: `NIC Coverage Campaign 2026-04-01`
- Campaign id: `1dce2a22-dae0-492a-b479-140f0c8eab52`
- Cohort shape: `20 fraternities`
- Mix: `11 new` + `9 control/backfill`

This report captures the real long-run campaign after the campaign product surface was launched on the website. It also records the fixes applied while the run was active.

## Website/Product State
- `/campaigns` is live and usable for launch, monitoring, resume, cancel, and export.
- Campaign detail now includes:
  - provider-health history
  - control-vs-new comparison
  - exportable JSON/CSV report links
  - runtime-attachment warnings for detached runners
- `GET /api/health` now exposes active campaign runtime state for diagnostics.

## Live Improvements Applied During The Run
1. Campaign runner auto-reattach
- Problem:
  - campaign row remained `running` while the in-memory runner detached after dev/runtime reloads
- Fix:
  - active campaign APIs now reattach detached `running` campaigns during normal polling
- Result:
  - campaign resumed without requiring repeated manual recovery

2. Enrichment timeout hardening
- Problem:
  - long `process-field-jobs` cycles could fail the full fraternity request at the timeout boundary
- Fix:
  - enrichment-cycle timeouts now preserve progress, emit degraded-cycle events, and continue when useful work remains
- Result:
  - long-running fraternities are less likely to be mislabeled as hard failures

3. Crawl-run timeout hardening
- Problem:
  - `python -m fratfinder_crawler.cli run --source-slug ...` could fail after a fixed timeout even when a crawl had already produced chapters/field jobs
- Fix:
  - crawl-run timeouts now inspect produced work and continue into enrichment when the ingest already generated usable records
- Result:
  - productive crawls are no longer discarded purely because the source ingest was slow

4. Provider-order optimization
- Problem:
  - live provider telemetry showed:
    - `searxng_json`: unavailable
    - `tavily_api`: repeated request errors
    - `serper_api`: stable successful fallback
- Fix:
  - reordered free-provider chain to prefer `serper_api` before `tavily_api`
  - aligned both code defaults and live `.env`
- Result:
  - fallback path now reaches a healthy provider faster and avoids wasting attempts on the currently failing Tavily path

## Current Live Metrics Snapshot
Snapshot source:
- `/api/campaign-runs/1dce2a22-dae0-492a-b479-140f0c8eab52/export?format=json`

Observed after the above fixes:
- `completedCount`: `5`
- `failedCount`: `7`
- `activeCount`: `5`
- `queueDepthDelta`: `+103` burn-down from campaign checkpoint baseline
- `totalProcessed`: `233`
- `totalRequeued`: `218`
- `totalFailedTerminal`: `6`
- `jobsPerMinute`: `10.3`
- `anyContactSuccessRate`: `10.6%`
- `allThreeSuccessRate`: `11.4%`

## Cohort Comparison Snapshot
- New cohort:
  - `anyContactSuccessRate`: `49.0%`
  - `allThreeSuccessRate`: `8.8%`
- Control cohort:
  - `anyContactSuccessRate`: `0.5%`
  - `allThreeSuccessRate`: `12.1%`

Interpretation:
- New fraternities are currently yielding more “any contact” gains than controls.
- Controls remain harder because the scorecard is delta-based against existing baseline coverage, not raw totals.

## Dominant Failure Modes So Far
1. `No chapters discovered from the selected national source. Confirm source URL or parser strategy.`
- This remains the primary miss pattern.
- Affected examples:
  - Sigma Phi Society
  - Kappa Delta Rho
  - Delta Kappa Epsilon
  - Chi Psi
  - Alpha Tau Omega
  - Sigma Chi

2. Long-run timeout pressure on large/complex sources
- Examples observed during the live run:
  - Delta Chi enrichment timeout
  - Phi Gamma Delta crawl-run timeout
- These directly motivated the timeout hardening work above.

## Source Quality Observations
Active control sources now show the preferred-source logic is helping:
- Alpha Gamma Rho:
  - `https://www.alphagammarho.org/chapters`
- Alpha Delta Phi:
  - `https://www.alphadeltaphi.org/chapter-roll`
- Lambda Chi Alpha:
  - `https://www.lambdachi.org/chapters/`

Remaining biggest quality gap:
- some verified/control sources still resolve to low-yield or wrong-entry pages
- source-specific extraction hints remain the next highest-leverage improvement

## Operational Notes
- The campaign is still active.
- The benchmark surface is now good enough for continued live monitoring from the website.
- The current system is measurably more robust than it was at launch because:
  - detached campaigns now recover
  - long productive jobs do not fail as aggressively
  - provider fallback now reaches the healthy API sooner

## Recommended Next Steps
1. Add source-specific extraction hints for the recurring zero-chapter nationals.
2. Replay timeout-affected controls after the new timeout logic has had time to run through them.
3. Keep monitoring control-vs-new deltas and provider-health history from the website until the full campaign reaches terminal completion.
