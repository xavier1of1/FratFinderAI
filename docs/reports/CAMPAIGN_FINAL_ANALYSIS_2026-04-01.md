# Campaign Final Analysis 2026-04-01

## Campaign
- Name: `NIC Coverage Campaign 2026-04-01`
- Id: `1dce2a22-dae0-492a-b479-140f0c8eab52`
- Window:
  - Started: `2026-04-01 09:23:11 UTC`
  - Finished: `2026-04-01 10:27:42 UTC`
- Total duration: `3,870,239 ms` (`64.5 minutes`)

## Plan Alignment
This analysis follows the campaign benchmark goals defined in the production-readiness plan:
- multi-fraternity long-run benchmark
- website-visible progress and exportable reporting
- provider-health history
- control-vs-new comparison
- throughput tracking
- failure-mode histogram
- “successful habits” scorecards
- tuning action visibility

## Final Summary
- Target fraternities: `20`
- Completed items: `6`
- Failed items: `14`
- Skipped items: `0`
- Active items at finish: `0`

Coverage summary:
- `anyContactSuccessRate`: `27.50%`
- `allThreeSuccessRate`: `6.56%`
- `websiteCoverageRate`: `7.60%`
- `emailCoverageRate`: `7.45%`
- `instagramCoverageRate`: `27.20%`

Operational summary:
- `jobsPerMinute`: `9.66`
- `totalProcessed`: `623`
- `totalRequeued`: `1699`
- `totalFailedTerminal`: `46`
- `checkpointCount`: `29`

Queue summary:
- `queueDepthStart`: `1686`
- `queueDepthEnd`: `3563`
- `queueDepthDelta`: `-1877`

Interpretation:
- the campaign processed meaningful work and stayed active for over an hour
- however, it produced more queue growth/requeue churn than queue burn-down
- coverage improved, but not enough to meet the intended target outcomes

## Cohort Comparison
### New cohort
- Item count: `11`
- Completed: `5`
- Failed: `6`
- `anyContactSuccessRate`: `55.88%`
- `allThreeSuccessRate`: `9.80%`
- `websiteCoverageRate`: `36.27%`
- `emailCoverageRate`: `19.61%`
- `instagramCoverageRate`: `34.31%`
- `avgJobsPerItem`: `18.82`

### Control cohort
- Item count: `9`
- Completed: `1`
- Failed: `8`
- `anyContactSuccessRate`: `25.16%`
- `allThreeSuccessRate`: `6.29%`
- `websiteCoverageRate`: `5.24%`
- `emailCoverageRate`: `6.45%`
- `instagramCoverageRate`: `26.61%`
- `avgJobsPerItem`: `240.11`

Interpretation:
- new fraternities materially outperformed controls on `any contact`, `website`, and `email`
- controls were far more expensive per item
- this supports the plan’s decision to track control-vs-new separately
- it also shows that baseline-heavy controls are a stress test for throughput and retry policy

## Provider Health Timeline
### Healthy opening
- early checkpoints were healthy
- good states observed:
  - `searxng_json` initially healthy
  - later `serper_api` carried fallback successfully when SearXNG dropped out

### Degraded ending
Later tuning windows show search collapse:
- `searxng_json`: unavailable
- `serper_api`: request errors
- `tavily_api`: request errors
- `duckduckgo_html`: timeouts
- `bing_html`: anomaly/challenge
- `brave_html`: HTTP errors / 429 pressure

The campaign auto-tuned:
- concurrency `4 -> 3`
- concurrency `3 -> 2`
- concurrency `2 -> 1`

Interpretation:
- provider degradation became a major late-stage limiter
- the auto-tuning worked as designed in that it reduced concurrency under degraded conditions
- but the degraded provider state still caused large retry pressure and queue growth

## Top Failure Modes
1. `No chapters discovered from the selected national source. Confirm source URL or parser strategy.`
- Count: `12`
- This was the dominant miss pattern.

2. `Enrichment cycle budget exhausted before queue drained`
- Count: `8`
- This shows the cycle budget is still too small for some larger/high-workload fraternities.

3. `Command timed out after 600000ms: python -m fratfinder_crawler.cli process-field-jobs ...`
- Seen on:
  - Delta Sigma Phi
  - Delta Chi
  - Alpha Gamma Rho

4. `Command timed out after 1800000ms: python -m fratfinder_crawler.cli run --source-slug phi-gamma-delta-main`
- Phi Gamma Delta remained a large-source crawl pressure case even after the timeout hardening pass.

## Item-Level Highlights
Strongest performers:
- Alpha Delta Gamma
  - `802` chapters discovered
  - `303` chapters with any contact
  - overwhelmingly instagram-heavy yield
- Delta Sigma Phi
  - `94` chapters discovered
  - `55` chapters with any contact
  - strong balanced website/email/instagram gains
- Alpha Delta Phi
  - `42` chapters discovered
  - `5` chapters with any contact

Notable low-yield failures:
- Sigma Phi Society
- Kappa Delta Rho
- Delta Kappa Epsilon
- Chi Psi
- Alpha Tau Omega
- Sigma Chi

These map directly to the source-native zero-chapter failure mode.

## Successful Habits
Campaign-reported averages:
- `avg source-native yield`: `0.524`
- `avg search efficiency`: `1.115`
- `avg confidence quality`: `0.120`
- `avg queue efficiency`: `0.192`

Interpretation:
- source-native discovery is still the best predictor of useful progress
- search can be effective when it works, but its reliability is unstable over long windows
- queue efficiency is still low enough that too much work is being spent on retries, requeues, and degraded windows

## Acceptance Criteria Assessment
### 1. Queue remains progressing
Partial pass.
- The campaign continued moving and completed all items.
- But queue depth worsened overall:
  - start `1686`
  - end `3563`

### 2. Campaign is resumable
Pass.
- We observed and fixed runner detachment during the live run.
- The campaign resumed and completed.

### 3. Provider degradation is detected and logged
Pass.
- provider-health history was captured
- degraded windows were visible
- tuning actions were logged

### 4. Tuning actions are applied safely and visibly
Pass.
- concurrency reductions were applied and recorded in campaign events/runtime notes

### 5. Final report includes throughput, field coverage, control deltas, failure modes, successful habits
Pass.
- the campaign export and dashboard now contain those views

### 6. Performance expectations from the benchmark prompt
#### Significant throughput improvements
Partial pass.
- absolute throughput reached `9.66 jobs/min`
- however queue growth and requeue pressure indicate the system is not yet efficient enough at scale

#### Effective use of API calls
Partial pass.
- reordering to `serper_api` improved provider efficiency in the healthy window
- but degraded windows still caused large failed-attempt volume later

#### Minimal rate-limiting issues
Fail.
- late-stage provider history clearly shows widespread provider degradation and HTTP/rate-limit style failures

#### Resumable progress
Pass.
- this was achieved and validated live

#### 75% success rate for contact info
Fail.
- headline `anyContactSuccessRate` finished at `27.50%`
- stretch target was not reached

## Core Conclusions
### What worked
- website-visible campaign operations
- exportable campaign reporting
- control-vs-new comparison
- provider-health history
- campaign resume behavior
- auto-tuning under degraded provider conditions
- strong results on some source-native fraternities, especially in the new cohort

### What did not work well enough
- source-native parsing for a set of nationals still failed outright
- search reliability collapsed over longer runtime
- enrichment budgets remained too small for some high-volume sources
- overall queue efficiency and retry behavior are still not good enough for the target contact-coverage vision

## Highest-Priority Next Steps
1. Add source-specific extraction hints for the zero-chapter nationals.
- This is the clearest highest-impact fix.

2. Rework enrichment budgeting for high-volume sources.
- Current `maxEnrichmentCycles` and per-cycle limits leave too many partially successful fraternities classified as failed.

3. Add stronger provider degradation policy.
- When all providers degrade, campaign behavior should shift earlier into protected mode instead of accumulating requeue pressure.

4. Split large-control fraternities into staged sub-batches.
- Current controls are too expensive and dominate `avgJobsPerItem`.

5. Add campaign replay action for failed items only.
- The system is now mature enough that replaying only failed fraternities is the right next operational workflow.
