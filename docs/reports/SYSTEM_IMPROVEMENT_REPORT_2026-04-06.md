# System Improvement Report (2026-04-06)

## Scope

This report summarizes the concrete system improvements made after the April 4-5, 2026 V4 benchmark failures and the follow-up live diagnosis of V3 performance.

The work was driven by two major findings:

1. The V3 chapter-search core was collapsing into low-value outbound fanout on locator/map sources and certain same-host expansion paths.
2. The contact-enrichment / field-job subsystem was generating excessive queue churn, weak-contact retries, and completion drag.

This report covers:

- what failed
- what was changed
- why those changes matter
- how the changes were validated
- what remains incomplete

---

## 1. Failure Context

### 1.1 V4 training benchmark failure

The overnight V4 RL improvement program did not complete successfully on April 4-5, 2026.

The key operational failure was:

- round 1 training never completed
- no campaign items were admitted into the final live validation phase
- no policy snapshot was promoted

The root cause analysis showed that `adaptive-train-eval` was still vulnerable to one pathological source in the evaluation batch:

- `phi-gamma-delta-main`

The specific failure mode on that source was:

- chapter stubs were extracted quickly from a locator/map payload
- the crawler then treated many external chapter websites as chapter-discovery targets
- several stale `http://` chapter-owned domains were extremely slow or dead
- one dead host could consume minutes because of retry + timeout behavior
- the batch was executed serially, so one bad source blocked the whole round

This was not primarily an RL failure. It was a crawl-core and orchestration failure:

- chapter discovery policy was too permissive
- outbound target classification was too weak
- the training subprocess had no per-source isolation

### 1.2 Queue / contact-resolution failure

The next major bottleneck was the field-job backlog.

At the time of analysis:

- queued field jobs were roughly in the `12k+` range
- oldest queued jobs were stale
- top weak outcomes were dominated by low-confidence `contact_email` and `website_url`
- request completion still depended heavily on queue drain
- the existing field-job logic requeued weak work too aggressively

The contact subsystem was still behaving like a separate imperative retry engine instead of a precision-first V3 subsystem.

---

## 2. Improvement Strategy

The work was intentionally split into two high-impact tracks, in this order:

1. Rebuild the V3 chapter-search core.
2. Rebuild the V3 contact-resolution core.

This ordering was chosen because chapter discovery had to stop generating pathological fanout and junk candidates before downstream contact work could become stable.

---

## 3. Chapter Search Improvements

### 3.1 Goal

Rebuild chapter search so V3 could produce a trustworthy chapter set without treating outbound chapter websites as primary chapter-existence proof.

The core design shift was:

- national -> institutional -> broader web
- chapter-owned websites are downstream assets, not chapter-existence authorities
- broader-web discovery creates provisional chapters/evidence, not canonical chapters

### 3.2 Runtime changes

The chapter-search rebuild was implemented primarily in:

- [adaptive_graph.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/adaptive_graph.py)
- [navigation.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/navigation.py)
- [state.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/state.py)
- [models.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/models.py)
- [request_graph.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/request_graph.py)
- [request_repository.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/db/request_repository.py)

The main changes were:

- introduced typed chapter-search contracts and structured chapter-search state
- added source-class-aware chapter target policy
- added hard chapter-intent gating before normalization
- prevented locator/map sources from bulk-following chapter-owned external sites during chapter discovery
- treated locator/KML/API payloads as identity feeds instead of follow-every-link feeds
- clamped broader-web expansion to provisional-only outcomes during chapter search
- exposed chapter-search metrics in request progress and Agent Ops

### 3.3 Additional hardening

After the first chapter-search rollout, additional fixes were made to reduce residual fanout:

- blocked irrelevant same-host expansion such as `/careers`, `/about-us`, and other low-yield paths
- filtered weak `anchor_list` stubs before they could become records or frontier items
- skipped institutional follow when chapter identity was already complete
- skipped `.edu` personal-homepage-style and timeout-risk targets during chapter search
- reconciled stale historical `running` crawl rows so old stuck runs no longer polluted the UI

These later fixes were implemented in:

- [navigation.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/navigation.py)
- [crawl-run-repository.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/lib/repositories/crawl-run-repository.ts)
- [api/runs/route.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/app/api/runs/route.ts)
- [api/agent-ops/route.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/app/api/agent-ops/route.ts)

### 3.4 Product visibility

Chapter-search telemetry was surfaced in:

- [agent-ops/page.tsx](/d:/VSC%20Programs/FratFinderAI/apps/web/src/app/agent-ops/page.tsx)
- [agent-ops-repository.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/lib/repositories/agent-ops-repository.ts)
- [benchmarks-dashboard.tsx](/d:/VSC%20Programs/FratFinderAI/apps/web/src/components/benchmarks-dashboard.tsx)
- [campaigns-dashboard.tsx](/d:/VSC%20Programs/FratFinderAI/apps/web/src/components/campaigns-dashboard.tsx)
- [runtime-comparison.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/lib/runtime-comparison.ts)
- [types.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/lib/types.ts)

The product now shows chapter-search-specific behavior such as:

- source class
- coverage state
- canonical vs provisional creation
- national vs institutional follow counts
- skipped external chapter sites
- chapter-search wall time
- chapter-search gate checks

### 3.5 Observed impact

The clearest live proof was `phi-gamma-delta-main`, which had been the pathological source during the V4 failure analysis.

Before hardening:

- locator-map extraction was followed by dead outbound chapter-site fanout
- the source could stall a training batch

After the chapter-search rebuild:

- `phi-gamma-delta-main` adaptive runs completed successfully
- chapter-owned external sites were skipped instead of followed as discovery targets
- wider-web results became provisional-only
- institutional follow was reduced and then eliminated when identity was already complete

Measured live progression:

- run `257`: about `19s`, `institutionalTargetsFollowed=2`, `chapterOwnedTargetsSkipped=40`
- run `258`: about `6s`, `institutionalTargetsFollowed=0`, `chapterOwnedTargetsSkipped=40`

This was the core chapter-search success condition:

- chapter search no longer collapsed into dead outbound fanout

---

## 4. Contact Resolution Improvements

### 4.1 Goal

Rebuild contact enrichment so V3 treats contact resolution as a precision-first subsystem instead of a generic retry loop.

The core design goal was:

- better queue admission
- bounded no-signal handling
- less requeue churn
- request completion based on actionable queue state, not raw queue volume

### 4.2 Runtime changes

The contact-resolution rebuild was implemented primarily in:

- [normalizer.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/normalization/normalizer.py)
- [field_jobs.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/field_jobs.py)
- [field_job_graph.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/field_job_graph.py)
- [repository.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/db/repository.py)
- [request_repository.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/db/request_repository.py)
- [request_graph.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/request_graph.py)

The main behavior changes were:

- missing contact fields are no longer auto-queued on weak identity alone
- queue admission now depends on whether a record has enough identity support
- recurring weak/no-candidate jobs can terminate as `terminal_no_signal`
- provider-degraded or low-signal jobs can be marked deferred instead of churning immediately
- deferred queued work is deprioritized behind actionable queued work
- request completion logic now uses actionable queue remaining, not raw queued count
- field-job graph decisions now distinguish `updated`, `review_required`, `terminal_no_signal`, and other explicit outcomes

### 4.3 State and telemetry model

The rebuild deliberately reused the existing tables and JSON payload model instead of adding an unnecessary migration.

New runtime state now lives in the existing payload surfaces:

- `payload.contactResolution.queueState`
- `payload.contactResolution.reasonCode`
- `payload.contactResolution.nextBackoffSeconds`
- `completed_payload.status`

Request progress now includes a first-class `contactResolution` section with:

- `queuedActionable`
- `queuedDeferred`
- `processed`
- `requeued`
- `reviewRequired`
- `terminalNoSignal`
- `providerDegraded`
- `autoWritten`
- `writesByField`
- `rejectionReasonCounts`

### 4.4 Product visibility

Contact-resolution health was surfaced in:

- [types.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/lib/types.ts)
- [agent-ops-repository.ts](/d:/VSC%20Programs/FratFinderAI/apps/web/src/lib/repositories/agent-ops-repository.ts)
- [agent-ops/page.tsx](/d:/VSC%20Programs/FratFinderAI/apps/web/src/app/agent-ops/page.tsx)
- [benchmarks-dashboard.tsx](/d:/VSC%20Programs/FratFinderAI/apps/web/src/components/benchmarks-dashboard.tsx)
- [campaigns-dashboard.tsx](/d:/VSC%20Programs/FratFinderAI/apps/web/src/components/campaigns-dashboard.tsx)

The UI now exposes:

- deferred field jobs
- terminal-no-signal outcomes
- review-required outcomes
- auto-written outcomes
- queue-efficiency slices in Benchmarks and Campaigns
- low-confidence drift indicators in Campaigns

### 4.5 Observed impact

The live validation run for contact resolution used:

- `python -m fratfinder_crawler.cli process-field-jobs --runtime-mode langgraph_primary --limit 2 --workers 1`

Result:

- `processed=2`
- `requeued=0`
- `failed_terminal=0`
- `runtime_fallback_count=0`

DB verification of the latest completed jobs showed:

- one job completed as `review_required`
- one job completed as `terminal_no_signal`

This was the desired behavior:

- weak or unresolved work did not get requeued forever
- the system made bounded, explicit decisions

The live Agent Ops API also reflected the new counters after this batch:

- `fieldJobsTerminalNoSignal=1`
- `fieldJobsReviewRequired=309`
- `fieldJobsUpdated=1913`

---

## 5. Validation Summary

### 5.1 Automated crawler validation

The following focused crawler suites passed after the contact-resolution rebuild:

- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_normalizer.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_field_job_supervisor_graph.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_navigation_upgrade.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_adaptive_learning_v21.py -q`

### 5.2 Web validation

The web app validated successfully after the dashboard/reporting additions:

- `pnpm --filter @fratfinder/web build`
- `pnpm --filter @fratfinder/web typecheck`

Live production-build smoke passed on `http://localhost:3200`:

- `GET /api/health/readiness`
- `GET /api/agent-ops`
- `GET /benchmarks`
- `GET /campaigns`

### 5.3 Live runtime validation

Live chapter-search validation proved the original Phi Gam failure mode was fixed.

Live contact-resolution validation proved:

- the LangGraph field-job runtime stayed primary
- runtime fallback did not trigger
- weak work produced bounded terminal/review outcomes

---

## 6. What Improved Systemically

These changes improved the system in four important ways:

### 6.1 Chapter discovery is now more reliable

The crawler is much less likely to waste its budget following dead or low-value external chapter sites while trying to prove chapter existence.

### 6.2 Queue behavior is more disciplined

The field-job system now admits less weak work, gives low-signal work bounded exits, and stops treating every missing field as equally actionable.

### 6.3 Request completion is more honest

Requests no longer have to wait on every deferred or low-signal job forever. Completion now reflects whether meaningful work remains, not whether any queued row exists at all.

### 6.4 Product observability improved

Operator surfaces now expose both chapter-search and contact-resolution health instead of forcing all diagnosis into logs or DB queries.

---

## 7. Remaining Gaps

The system is materially better, but not finished.

### 7.1 V3 crawl-core parity is not universally proven

Although chapter search is dramatically healthier, broad adaptive superiority over legacy on the full difficult source cohort has not yet been re-established.

### 7.2 Contact-resolution reporting is still partially shallow

Benchmarks and Campaigns now expose queue-efficiency slices, but the deepest per-field rejection breakdown still lives more fully in request progress and Agent Ops than in a dedicated benchmark-side backend report.

### 7.3 Queue backlog is still large

The backlog behavior is better governed, but the total queued field-job volume is still high. This work reduced churn and improved decision quality; it did not instantly eliminate the inherited backlog.

### 7.4 RL is still secondary

RL plumbing has improved, but this work intentionally did not make RL the authority. The system first needed better crawl-core and contact-resolution signals.

---

## 8. Recommended Next Steps

Based on the current system state, the next best steps are:

1. Run a wider benchmark focused specifically on the rebuilt contact-resolution behavior for the worst backlog sources:
   - `pi-kappa-alpha-main`
   - `sigma-alpha-epsilon-main`
   - `alpha-delta-gamma-main`

2. Build a dedicated contact-resolution benchmark/report backend so per-field rejection reasons, terminal-no-signal rates, and review causes are first-class benchmark artifacts.

3. Re-run the broader adaptive-vs-legacy benchmark cohort now that:
   - chapter search no longer collapses on Phi Gam
   - queue behavior is less requeue-heavy

4. Only after those signals are stable, expand RL optimization within the bounded contact-resolution decision model.

---

## 9. Bottom Line

The system improvements after the failure report were substantial and targeted.

The work did not try to paper over the observed problems. Instead, it addressed the two highest-impact broken components directly:

- chapter search
- contact resolution

Chapter search now behaves like a controlled identity-discovery system instead of an outbound fanout trap.

Contact resolution now behaves more like a bounded precision-first subsystem and less like an endless retry queue.

The result is a V3 runtime that is more stable, more observable, and more aligned with the actual product goal: trustworthy chapter and contact discovery with controlled queue behavior.
