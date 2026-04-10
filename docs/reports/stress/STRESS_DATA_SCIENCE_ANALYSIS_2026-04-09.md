# Stress Run Data Science Analysis

Date: `2026-04-09`

Run under analysis: `stress-20260409-full`

## Scope

This document is a deeper quantitative analysis of the stress run beyond the existing functionality report. The goal is to answer four questions:

1. Was reinforcement learning materially responsible for the observed improvement?
2. How did the agentic system actually behave under stress?
3. What were the dominant failure modes, and what caused them?
4. Which metrics were real signs of business progress versus queue-health or triage progress?

## Evidence And Method

Primary evidence:

- `docs/reports/stress/stress-20260409-full.jsonl`
- `docs/reports/stress/FUNCTIONALITY_REPORT_2026-04-09.md`
- `docs/reports/stress/stress-20260409-post-hardstop.out`
- `docs/reports/stress/stress-20260409-post-dependency-deferral.out`
- `docs/reports/stress/stress-20260409-post-preserve-deferred.out`
- `docs/reports/stress/stress-20260409-post-email-prereq-fix.out`
- `docs/reports/stress/stress-20260409-final-clean-pass.out`
- `CHANGELOG.md`

Method:

- Used the 21 recorded `batch_progress` rows from the JSONL file as the canonical time series.
- Treated the stress artifacts as the source of truth instead of the current live database because the live DB has drifted since the report was generated.
- Used Spearman correlation rather than Pearson for most relationships because the sample is small (`n=21`) and several metrics move non-linearly in patch-driven steps.
- Created one derived metric, `progress_yield = processed / (processed + requeued)`, to separate productive work from queue motion.
- Grouped outcome labels into cause families:
  - `historical_invalid`
  - `identity_repair`
  - `dependency_ordering`
  - `provider_health`
  - `inactive_validation`
  - `terminal_low_signal`
  - `authoritative_resolution`
  - `queue_state_artifact`

Important caveat:

- Outcome labels in the batch snapshots are causal labels, not a perfect 1:1 mirror of final job statuses. They are excellent for understanding failure pressure and cause prevalence, but they should not be treated as exact substitutes for `done`, `failed`, and `deferred` status counts.

## Headline Findings

1. This was not an online reinforcement-learning win.
   - All 21 batches used `runtime_mode_used=langgraph_primary`.
   - All 21 batches used `policy_pack=default`.
   - Total runtime fallback count was `0`.
   - Conclusion: the observed improvement came from deterministic queue-policy and orchestration fixes, not from live policy exploration or adaptive RL behavior.

2. The dominant failure pressure was bad inventory, not bad search.
   - The two largest outcome classes were:
     - `Canceled invalid historical field job: ranking_or_report_row = 3427`
     - `Canceled invalid historical field job: year_or_percentage_as_identity = 2570`
   - Together they represent `5997` jobs, or about `68.4%` of the entire `8766`-job cohort.

3. Queue health improved mainly because the system got better at refusing bad work.
   - Average throughput was `60.37 jobs/min`, but average `progress_yield` was only `9.11%`.
   - In other words, the run mostly optimized queue correctness, cooling, and safe deferral, not high business-yield contact discovery.

4. Provider collapse was real, but the patched system handled it correctly.
   - The hard-stop artifact shows `healthy=false`, `success_rate=0.0`, and `0/4` successful global probes with provider-wide request errors, timeouts, and challenge/anomaly failures.
   - After the provider gating fixes, affected jobs were deferred with zero chapter-specific external queries instead of burning budget on low-yield fanout.

5. The remaining backlog is mostly valid deferred work.
   - Final state:
     - `580` done
     - `6307` failed
     - `1879` deferred
     - `0` actionable
     - `0` running
   - Operationally, that is a clean finish.
   - Analytically, it means the unresolved tail has been converted from hot thrash into typed waiting states.

## KPI Summary

| Metric | Value | Interpretation |
| --- | --- | --- |
| Chapters in cohort | `2922` | Full cohort size under stress |
| Field jobs in cohort | `8766` | Three lanes per chapter |
| Recorded batches | `21` | Enough for time-series correlation, but still a small sample |
| Average jobs/min | `60.37` | Operational drain speed, not business yield |
| Peak jobs/min | `104.40` | Highest queue-motion speed after queue-policy fixes |
| Average progress yield | `9.11%` | Most batch motion was requeue/triage, not productive resolution |
| Runtime modes seen | `langgraph_primary` only | No legacy fallback observed |
| Policy packs seen | `default` only | No policy switching during the run |
| Runtime fallbacks | `0` | Runtime was stable |
| Final actionable queue | `0` | No hot queue leakage remained |
| Final deferred queue | `1879` | Remaining work is blocked behind explicit prerequisites or provider conditions |

## Reinforcement Learning Assessment

Short version: there is no evidence that reinforcement learning materially contributed to the improvement in this run.

Why:

- There was no policy-pack variation during the run.
- There was no runtime fallback or runtime-mode switching.
- The largest changes in queue behavior line up with explicit code fixes documented in the changelog and rerun artifacts:
  - invalid historical cleanup
  - provider hard-stop and degraded-mode skipping
  - website prerequisite enforcement for email
  - preserving deferred canonical state
  - preserving that same prerequisite after repair promotion

What this run actually measures:

- Orchestration stability
- Queue-policy correctness
- Failure containment
- Graceful degradation under provider collapse

What it does not measure:

- Online policy learning
- Exploration-versus-exploitation quality
- Reward shaping quality
- Comparative uplift of one policy pack versus another

Bottom line:

- Calling this an RL success would overstate what the data proves.
- Calling it an agentic orchestration and queue-policy success is accurate.

## Agentic Behavior Assessment

The agentic system behaved more like a conservative operations controller than a discovery-maximizing search agent. Under stress, that was the correct behavior.

Observed behavior:

1. It preferred typed deferral over speculative search.
2. It treated repair as a first-class containment lane instead of forcing weak identities through enrichment.
3. It used dependency gates to stop email jobs from outrunning website discovery.
4. It converted some open-ended search cases into explicit terminal outcomes like `terminal_no_signal`.
5. It kept the LangGraph runtime stable while the external search surface was unhealthy.

This is important because the run disproves a common failure pattern in agentic systems: "when uncertain, keep searching." Here, the system got better by doing the opposite.

## Throughput Versus Real Progress

The most important metric correction from this analysis is that `jobs/min` is a weak proxy for real progress in this run.

Spearman correlations with `jobs_per_minute`:

| Metric | Spearman rho with jobs/min | Reading |
| --- | --- | --- |
| `processed` | `0.1410` | Very weak positive relationship |
| `requeued` | `0.1341` | Very weak positive relationship |
| `actionable_jobs` | `0.0545` | Almost no relationship |
| `deferred_jobs` | `0.2156` | Weak positive relationship |
| `done_jobs` | `-0.0800` | Essentially no useful relationship |
| `failed_jobs` | `0.3255` | Moderate positive because invalid cancellation can inflate queue-motion throughput |
| `progress_yield` | `0.1364` | Very weak positive relationship |
| `dependencyDeferred` | `-0.1799` | Slight throughput drag when gating is applied |
| `repairQueued` | `0.0218` | No meaningful relationship |
| `invalidCancelled` | `-0.0280` | No meaningful relationship |

Interpretation:

- High `jobs/min` often meant "the queue is getting better at moving work to the correct lane."
- It did not reliably mean "the system is discovering more useful contacts."
- This is why the average `progress_yield` of `9.11%` matters so much: only about one in eleven units of batch motion was productive processing.

## Quasi-Experimental Patch Analysis

The run contains several natural before/after windows where a fix landed and the next batch shows the effect.

| Patch window | Before | After | Effect |
| --- | --- | --- | --- |
| Invalid cleanup consolidation | `03:56:25` actionable `2271` | `04:01:31` actionable `930` | `-59.0%` actionable, `+1001` failed-invalid, modest throughput gain from `61.32` to `66.18` jobs/min |
| Dependency deferral fix | `07:25:51` actionable `870` | `07:31:05` actionable `270` | `-69.0%` actionable, `+544` deferred, `+56` done, throughput rose from `85.86` to `90.84` jobs/min |
| Preserve deferred canonical jobs | `08:20:23` actionable `589` | `08:30:21` actionable `110` | `-81.3%` actionable, `+463` deferred, throughput rose from `38.52` to `97.86` jobs/min |
| Email prerequisite tail fix | `09:17:50` actionable `81` | `09:24:04` actionable `1` | `-98.8%` actionable, `+75` deferred, `+5` done, throughput rose from `5.10` to `53.76` jobs/min |
| Final clean pass | `09:24:04` actionable `1` | `09:27:47` actionable `0` | Tail fully cooled without reactivation |

This pattern is consistent across the run:

- Most improvements came from better admission control and safer deferral.
- Those fixes reduced hot-queue churn faster than they increased true chapter-resolution yield.

## Root-Cause Family Analysis

Top root-cause families by peak observed prevalence:

| Cause family | Peak concurrent count | Final concurrent count | Spearman rho with actionable queue | Spearman rho with progress yield | Interpretation |
| --- | --- | --- | --- | --- | --- |
| `historical_invalid` | `6600` | `6600` | `-0.1323` | `-0.1469` | Massive data-quality debt; removing it improves correctness more than yield |
| `identity_repair` | `973` | `913` | `0.4772` | `-0.2548` | Unresolved identity remains a real bottleneck; it keeps work alive without generating much progress |
| `provider_health` | `622` | `530` | `-0.7593` | `0.2986` | Strong cooling effect; provider gating prevents wasteful searches and improves queue hygiene |
| `dependency_ordering` | `377` | `377` | `-0.8481` | `0.0650` | Strongest actionable-queue cooling effect; dependency gating was one of the most valuable fixes |
| `terminal_low_signal` | `239` | `239` | `-0.6821` | `0.1309` | Clean terminal closure reduces future thrash |
| `inactive_validation` | `15` | `0` | `0.1475` | `0.3459` | Small count but high-value precision wins when available |
| `authoritative_resolution` | `13` | `0` | `0.1172` | `0.0312` | Useful but too small to drive run-level behavior |

Key reading:

- `identity_repair` is the only large family that correlates positively with actionable queue size and negatively with progress yield. That makes it the clearest structural bottleneck left after the queue fixes.
- `provider_health` and `dependency_ordering` both strongly anti-correlate with actionable queue size. That means the gates did their job: they cooled the queue instead of allowing degraded work to stay hot.
- `historical_invalid` dominates the cohort so strongly that it distorts almost every top-line metric.

## Top 20 Observed Failure Modes

Ranked by peak concurrent prevalence across the 21 recorded batches.

| Rank | Outcome | Cause family | Peak count | Peak share of cohort | rho(actionable) | rho(jobs/min) | What it means |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `Canceled invalid historical field job: ranking_or_report_row` | `historical_invalid` | `3427` | `39.09%` | `-0.1323` | `0.3255` | The queue was overloaded with report/ranking artifacts posing as chapters |
| 2 | `Canceled invalid historical field job: year_or_percentage_as_identity` | `historical_invalid` | `2570` | `29.32%` | `-0.1323` | `0.3255` | Large amount of historical numeric junk had reached the contact lane |
| 3 | `Deferred until chapter repair queue finishes` | `identity_repair` | `973` | `11.10%` | `0.4772` | `0.4733` | Weak identities were correctly contained, but the repair backlog became the biggest live bottleneck |
| 4 | `Deferred until confident website discovery is available for email enrichment` | `dependency_ordering` | `362` | `4.13%` | `-0.7206` | `0.0511` | Strong evidence that website-first gating was necessary and effective |
| 5 | `No candidate instagram URL found ...; search preflight degraded` | `provider_health` | `315` | `3.59%` | `-0.8417` | `0.2682` | Instagram fanout was correctly stopped when providers were unhealthy |
| 6 | `No candidate website URL available; search preflight degraded` | `provider_health` | `307` | `3.50%` | `-0.7920` | `0.3845` | Website search was also safely suppressed under degraded provider conditions |
| 7 | `invalid_entity_filtered` | `historical_invalid` | `296` | `3.38%` | `0.0804` | `0.3483` | Additional invalid-entity filtering was still active after the major cleanup sweep |
| 8 | `Waiting for confident website discovery before email enrichment` | `dependency_ordering` | `245` | `2.79%` | `0.3663` | `0.1864` | Early dependency-state wording shows the same issue before the stricter final defer contract |
| 9 | `terminal_no_signal` | `terminal_low_signal` | `239` | `2.73%` | `-0.6821` | `-0.1529` | The system increasingly chose explicit closure over infinite retries |
| 10 | `Canceled invalid historical field job: school_division_or_department` | `historical_invalid` | `170` | `1.94%` | `-0.1323` | `0.3255` | School units and departments were still entering as fake chapters |
| 11 | `Canceled invalid historical field job: history_or_timeline_row` | `historical_invalid` | `137` | `1.56%` | `-0.1323` | `0.3255` | Timeline/history rows were also contaminating the queue |
| 12 | `No candidate website URL available; search provider or network unavailable` | `provider_health` | `110` | `1.25%` | `0.1020` | `-0.3474` | True provider/network unavailability dragged throughput and left jobs waiting |
| 13 | `No candidate instagram URL found ...; search provider or network unavailable` | `provider_health` | `104` | `1.19%` | `0.3525` | `-0.2335` | Similar provider failure pattern in the Instagram lane |
| 14 | `Deferred because a confident website is required before email enrichment can continue` | `dependency_ordering` | `96` | `1.10%` | `-0.5090` | `-0.2143` | The final, stricter email prerequisite rule was actively preventing low-signal work |
| 15 | `No candidate instagram URL found in provenance, chapter website, or search results` | `other` | `20` | `0.23%` | `0.3572` | `-0.1834` | True chapter-level no-signal case without explicit provider degradation |
| 16 | `No candidate website URL available` | `other` | `17` | `0.19%` | `0.3796` | `-0.2058` | Similar no-signal website case |
| 17 | `inactive_by_school_validation` | `inactive_validation` | `15` | `0.17%` | `0.1475` | `0.5793` | Small count, but high-value authoritative pruning when school evidence existed |
| 18 | `resolved_from_authoritative_source` | `authoritative_resolution` | `13` | `0.15%` | `0.1172` | `-0.0204` | Good behavior, but not prevalent enough to shift cohort outcomes |
| 19 | `No candidate email found ...; search preflight degraded` | `provider_health` | `3` | `0.03%` | `-0.0369` | `-0.3693` | Email search was also suppressed under degraded preflight, though less often than website/Instagram |
| 20 | `Candidate for website_url failed official-domain verification` | `other` | `1` | `0.01%` | `-0.0739` | `-0.1108` | Verification guardrails were catching isolated low-quality website candidates |

## Field-Level Interpretation

Final field-state distribution:

| Field | Done | Failed | Deferred | Done share | Failed share | Deferred share | Reading |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `find_website` | `342` | `1997` | `583` | `11.7%` | `68.3%` | `20.0%` | Best closure lane and the main prerequisite lane |
| `find_instagram` | `157` | `2161` | `604` | `5.4%` | `74.0%` | `20.7%` | More sensitive to provider health and low signal |
| `find_email` | `81` | `2149` | `692` | `2.8%` | `73.5%` | `23.7%` | Most constrained by website dependency and identity completeness |

Important caution:

- `done` here means operationally closed, not necessarily "contact found."
- A `done` job can be:
  - `terminal_no_signal`
  - `inactive_by_school_validation`
  - `confirmed_absent`
  - `updated`
- So these lane-level `done` shares should be read as closure rates, not business-success rates.

## Failure Chains

The run is best understood as a set of recurring failure chains.

### Failure Chain 1: Historical junk entered the contact lane

Cause:

- Ranking rows
- Timeline/history rows
- Percentages and year tokens
- School departments or divisions

Effect:

- Worker slots were consumed by impossible work
- Throughput looked active even though the system was not learning anything useful
- Queue health improved only after invalid rows were aggressively blocked or canceled

### Failure Chain 2: Providers collapsed before chapter-specific search could be justified

Cause:

- Global preflight probes showed request errors, unavailability, timeouts, and challenge/anomaly failures across the provider stack

Effect:

- Without gating, the system would have burned query budget on low expected value
- With the patch, jobs were deferred with zero chapter-specific fanout

### Failure Chain 3: Email outran website confidence

Cause:

- Email jobs were still claimable or reactivate-able without a confident website

Effect:

- The queue held low-identity email work hot for no good reason
- After the prerequisite fix, actionable queue pressure dropped sharply

### Failure Chain 4: Deferred canonical jobs were reactivated

Cause:

- Reconciliation logic polluted the actionable queue by undoing valid deferred state

Effect:

- Queue oscillation
- Hot-tail leakage
- Artificial actionability spikes, especially around the `08:20` batch

### Failure Chain 5: Weak identities were not yet repaired

Cause:

- Some chapter records were semantically incomplete and needed repair or promotion before contact work made sense

Effect:

- These jobs stayed alive without producing much yield
- The repair lane became the largest remaining structural backlog

## What The Data Says About The Current Architecture

The current LangGraph-centered field-job runtime looks operationally sound in this run.

Evidence:

- No runtime fallback
- No runtime-mode churn
- Tail drained to zero actionable jobs
- Provider collapse did not cause uncontrolled fanout
- Repair, dependency, and terminal-no-signal lanes all behaved like intended containment mechanisms

What is still not strong enough:

1. Upstream entity quality before jobs are enqueued
2. Repair-lane throughput
3. Business-yield analytics distinct from operational completion analytics
4. Provider resilience when the whole search surface is degraded

## Final Judgment

### Reinforcement learning

Not proven by this run. The run did not meaningfully exercise online RL behavior.

### Agentic orchestration

Strongly supported. The system behaved like a disciplined controller:

- it avoided bad work
- contained weak work
- respected prerequisites
- stopped low-yield fanout under provider failure
- terminated some hopeless cases cleanly

### Main bottlenecks still visible

1. Invalid historical inventory contamination
2. Repair-lane backlog
3. Provider degradation
4. Email's dependence on website confidence

## Recommendations

1. Treat upstream invalid-entity suppression as the highest-ROI improvement.
   - The biggest apparent "failure" in this run was not search quality. It was allowing non-chapter artifacts to become field jobs in the first place.

2. Promote repair-lane analytics to a first-class performance target.
   - `identity_repair` is the clearest remaining structural blocker after the queue fixes.
   - Track repair backlog age, promotion rate, and percent of deferred work attributable to repair.

3. Split operational completion metrics from business-success metrics.
   - `done` is too coarse.
   - Add a report layer that separates:
     - contact found
     - authoritative inactive
     - confirmed absent
     - terminal no signal

4. If you want to prove RL value, instrument a real experiment.
   - Run matched cohorts with different `policy_pack` values.
   - Persist per-job policy decisions.
   - Define a reward that balances contact yield, wrong-contact pollution, query cost, and queue cooling.
   - Compare business outcomes, not just `jobs/min`.

5. Keep provider-health gating, but rank deferred work by expected future value.
   - Once providers recover, the system should resume the most promising deferred jobs first instead of treating the deferred queue as flat.

## Bottom Line

The stress run shows that the system is becoming much better at operational self-control, but not because of live reinforcement learning. The biggest win was architectural: LangGraph-driven workers plus typed queue lanes and stricter gating turned a thrashing queue into a controlled one. The biggest remaining issue is not that the agents are too weak. It is that too much invalid or semantically incomplete work still reaches the contact pipeline, and the measurement stack still over-rewards queue motion relative to true business yield.
