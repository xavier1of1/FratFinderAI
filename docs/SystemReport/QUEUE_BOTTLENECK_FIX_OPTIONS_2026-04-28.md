# Queue Bottleneck Fix Options - 2026-04-28

This document evaluates 2-3 implementation concepts for each major bottleneck found in `QUEUE_BOTTLENECK_DEEP_DIVE_2026-04-28.md`.

The goal is not to pick the most exciting fix. The goal is to pick the fix that improves throughput while preserving FratFinderAI's safety rules:

- no unsafe contact writes
- no generic national contact writes
- no generic school-office contact writes
- no false active status from weak evidence
- no provider-heavy hot loops

## Decision Summary

| Bottleneck | Recommended approach | Why |
|---|---|---|
| `identity_semantically_incomplete` | Add school-alias normalization plus school-roster rescue before repair exhaustion | Highest leverage, directly attacks bad identity strings without weakening gates. |
| `school_evidence_missing` | Build bounded official-evidence refresh for top school/fraternity cohorts | Verify-school is now correctly cache-first; evidence creation is the missing upstream path. |
| `status_dependency_unmet` | Split reasons and run status refresh on `unknown` or no-decision cohorts | Data shows active decisions are not stuck; active decisions are missing. |
| SearXNG engine degradation | Configure stable engine allowlist plus rescue endpoint | Reduces engine-unresponsive noise without replacing the provider architecture. |
| Worker phase observability | Add worker phase heartbeat to `worker_processes.metadata` | Cheap, low-risk, and prevents misreading `running_jobs = 0` as dead workers. |

## 1. Reduce `identity_semantically_incomplete`

### Current Evidence

Observed patterns:

- School aliases like `Illinois State`, `Kennesaw State`, `Cleveland State`.
- Bad prefixes like `At Radford University`, `At The College`, `At The University`.
- School-as-chapter rows like `Georgia Southern University / Georgia Southern University`.
- Status leakage in chapter name like `Alpha Psi (active)`.
- Repair exhausted rows still marked `canonical_valid` in some cases.

Current scale:

- `verify_school_match`: 651 blocked with `identity_semantically_incomplete`.
- `find_email`: 320 repairable exhausted plus 81 canonical-valid exhausted.
- `find_website`: 273 repairable exhausted plus 64 canonical-valid exhausted.
- `find_instagram`: 250 repairable exhausted plus 37 canonical-valid exhausted.

### Concept A: Relax The Identity Gate

Description:

Allow more low-confidence rows through field jobs if the chapter has some useful fields such as Instagram, website, or a plausible school string.

Optimistic view:

- Fastest way to reduce blocked queue count.
- Would immediately make more jobs claimable.
- Could produce quick wins for chapters that are "obviously probably correct" to a human.

Pessimistic view:

- This reintroduces the exact false-positive class the newer safety model was built to prevent.
- Short school aliases and bad prefixes can cause cross-campus contamination.
- Contact jobs might write data for the wrong chapter before identity is truly repaired.

Data-driven view:

- Many examples are not merely low-confidence but structurally wrong, such as school-as-chapter rows and `At The University`.
- Relaxing the gate would reduce blocked count but likely increase unsafe candidate review or false writes.

Recommendation:

Do not use this as the primary fix. It is tempting, but it weakens the core safety boundary.

### Concept B: Add School-Alias Normalization Before Repair Exhaustion

Description:

Create a school alias resolver that normalizes common incomplete but recoverable school names before rows enter repair exhaustion.

Examples:

- `Illinois State` -> `Illinois State University`
- `Kennesaw State` -> `Kennesaw State University`
- `Cleveland State` -> `Cleveland State University`
- `U.S. Military Academy` -> `United States Military Academy`
- `At Radford University` -> `Radford University`
- `At Christopher Newport University` -> `Christopher Newport University`

Optimistic view:

- Directly targets the most common recoverable identity defects.
- Does not weaken contact/status gates.
- Should reduce both `identity_semantically_incomplete` and downstream `status_dependency_unmet`.

Pessimistic view:

- Alias normalization can be wrong for ambiguous names.
- Requires a trusted school alias index or careful fallback scoring.
- Could create false confidence if aliases are inferred too aggressively.

Data-driven view:

- The examples show many recoverable school-name defects rather than impossible identities.
- This is safer than relaxing field-job gates because the normalized value can be required to match a known school/campus table or roster source.

Implementation shape:

- Add a `school_alias_resolver` module.
- Prefer existing school/campus rows if present.
- Only auto-normalize when alias maps to exactly one known school.
- Store before/after identity repair evidence.
- Route ambiguous aliases to repair review, not direct promotion.

Recommendation:

Use this as the first identity fix.

### Concept C: School-Roster Rescue Before Field-Job Creation

Description:

If a request is school-scoped or a chapter has a plausible school, build/reuse the campus roster/status index to bind chapter identity before contact/status jobs are created.

Optimistic view:

- Fixes the upstream extraction/promotion boundary rather than cleaning up after it.
- Strong for VT-style failures and school-published rosters.
- Can rescue provisional, colony, and alias-heavy rows.

Pessimistic view:

- More expensive than alias normalization.
- Requires reliable official-school evidence discovery.
- Might be slow under poor SearXNG health unless cached school evidence exists.

Data-driven view:

- Many identity failures are school-context failures.
- The queue already has many status and school-evidence dependencies, so campus-level reuse is strategically aligned.

Recommendation:

Use this as the second identity fix, after alias normalization. Alias normalization is cheaper; roster rescue is broader.

### Final Recommendation For Identity

Implement in this order:

1. Deterministic school alias normalization against a trusted known-school index.
2. Prefix cleanup for `At <School>` and status suffix cleanup like `(active)`.
3. School-roster rescue for remaining high-volume unresolved cohorts.

Avoid relaxing the identity gate.

## 2. Create Or Refresh Official School Evidence For `school_evidence_missing`

### Current Evidence

Observed shape:

- About 480 `verify_school_match` jobs blocked as `school_evidence_missing`.
- Many rows have `chapter_status_decisions`, but latest status is `unknown`.
- Many rows have school policy records, but not decisive chapter activity records.
- Official activity cache is mostly absent for these cohorts.

Examples:

- Lambda Chi Alpha at Kettering University.
- Lambda Chi Alpha at University of Kansas.
- Alpha Tau Omega at University of Alabama.
- Delta Kappa Epsilon at Southern Methodist University.
- Delta Sigma Phi at Transylvania University.

### Concept A: Let Verify-School Search Again On Cache Miss

Description:

When verify-school cannot find cached evidence, allow it to run provider-backed official-school search.

Optimistic view:

- Simple mental model: the job needs evidence, so go find it.
- Could resolve many blocked jobs in-place.
- Reduces need for a separate evidence-refresh command.

Pessimistic view:

- This reverses the cache-first fix that just stabilized verify-school.
- Provider outages would again turn verify-school into transient-network churn.
- It mixes deterministic verification with crawling/search, making failures harder to reason about.

Data-driven view:

- Earlier failures showed verify-school was too search-sensitive.
- The new live validation showed `verify_school_provider_search_attempted = 0`, `0 requeues`, and fast completion. That is worth preserving.

Recommendation:

Do not put search back into hot verify-school.

### Concept B: Bounded Evidence-Refresh Job For Top Cohorts

Description:

Create a separate evidence-refresh path that targets the highest-volume `school_evidence_missing` cohorts, discovers official school rosters/status pages, and writes:

- `chapter_status_decisions`
- `fraternity_school_activity_cache`
- `school_greek_life_registry`

Optimistic view:

- Keeps verify-school deterministic and fast.
- Allows rate-limited, observable crawling/search.
- Can be run on bounded cohorts and paused safely.

Pessimistic view:

- More moving parts than putting search in verify-school.
- Needs careful idempotency and dedupe.
- Search provider health still matters.

Data-driven view:

- The blocker is missing official evidence, not verify-school logic.
- Fraternity-level clusters make cohort targeting efficient: Lambda Chi Alpha, Sigma Pi, Delta Kappa Epsilon, Sigma Nu, etc.

Implementation shape:

- Add `refresh-school-evidence` CLI.
- Input: `--limit`, `--fraternity-slug`, `--school`, `--reason school_evidence_missing`.
- Search/fetch only official school domains where possible.
- Persist evidence and trigger queue reconciliation.
- Report accepted evidence count, unknown count, review count, unsafe count.

Recommendation:

Use this as the primary fix.

### Concept C: Bulk Seed School Policy From Known School Roster Pages

Description:

For schools already known to allow Greek life, bulk seed `school_greek_life_registry` and maybe roster source URLs.

Optimistic view:

- Fastest way to reduce pure school-policy unknowns.
- Useful for known campuses with stable FSL pages.
- Low search cost if data already exists in provenance.

Pessimistic view:

- `allowed` school policy does not prove target chapter activity.
- Could create a false sense of progress while verify-school remains blocked.
- If used incorrectly, it may weaken chapter activity semantics.

Data-driven view:

- The new resolver intentionally does not treat `allowed` as chapter verification.
- This is useful only as supporting evidence, not final chapter evidence.

Recommendation:

Use only as a supporting sub-step of Concept B, not as a standalone fix.

### Final Recommendation For School Evidence

Implement a separate bounded official evidence-refresh path. Do not add search back into verify-school. Use school policy seeding only as supporting evidence, while activity cache/status decisions remain the real unlock.

## 3. Unblock `status_dependency_unmet`

### Current Evidence

The initial hypothesis was that active status decisions might not be unblocking contact jobs. Live data did not support that.

Observed latest status distribution for `status_dependency_unmet`:

- `find_email`: 764 unknown, 210 review, 33 no decision.
- `find_website`: 427 unknown, 109 review, 77 no decision.
- Active status decisions stuck behind `status_dependency_unmet`: 0 observed.

Examples:

- Sigma Chi at Florida Atlantic University: unknown status.
- Sigma Pi at Johnson & Wales University: unknown status.
- Sigma Pi at Keene State College: unknown status.
- Sigma Pi at Emporia State University: unknown status.
- Sigma Chi at Indiana University: unknown status.
- Theta Chi at Clarkson University: unknown status.
- Lambda Chi Alpha at UNC Charlotte: unknown status.
- Lambda Chi Alpha at Arkansas State University: unknown status.

### Concept A: Reactivate Contact Jobs From Canonical `chapter_status = active`

Description:

If `chapters.chapter_status = active`, allow contact jobs to proceed even without a new chapter status decision.

Optimistic view:

- Would instantly unblock many contact jobs.
- Uses existing canonical row data.
- Could increase throughput quickly.

Pessimistic view:

- Historical `chapter_status = active` may not be evidence-backed.
- This bypasses the new status engine's explicit decision/evidence requirements.
- Could reopen false active and unsafe contact write risk.

Data-driven view:

- The deep-dive found many `chapter_status = active` rows with `field_states.university_name = low_confidence`.
- Canonical active alone is not trustworthy enough for new contact writes.

Recommendation:

Do not use this as a broad unblocker.

### Concept B: Split Reason Codes By Missing Prerequisite

Description:

Replace the overloaded `status_dependency_unmet` reason with more specific reasons:

- `status_no_decision`
- `status_unknown`
- `status_review_required`
- `status_identity_repair_required`
- `status_evidence_refresh_required`

Optimistic view:

- Makes the queue explain itself.
- Lets unlockers target the actual missing prerequisite.
- Lowers operator confusion and prevents wrong fixes.

Pessimistic view:

- Does not directly reduce the queue count by itself.
- Requires migration/backfill and dashboard updates.

Data-driven view:

- Current `status_dependency_unmet` is hiding materially different states: no decision, unknown decision, review decision.
- The wrong fix would be to reactivate all of them.

Recommendation:

Implement this as the first status-dependency fix.

### Concept C: Status Refresh For Unknown/No-Decision Cohorts

Description:

Run a bounded refresh that targets jobs blocked by `status_no_decision` or `status_unknown`, prioritizing strong school identities and high-volume sources.

Optimistic view:

- Directly converts blocked contact jobs into active/inactive/review decisions.
- Keeps contact safety intact.
- Works well with the official evidence-refresh path.

Pessimistic view:

- Depends on SearXNG/evidence quality.
- Some cases will remain review/unknown.
- Needs careful queue reconciliation after writing decisions.

Data-driven view:

- Since no active decisions were stuck, the correct target is status creation, not unblock logic.

Recommendation:

Use this after reason-code split, because the split gives the refresh path a precise target list.

### Final Recommendation For Status Dependencies

Do not loosen contact gates. First split `status_dependency_unmet` into precise reasons. Then run status/evidence refresh on `status_no_decision` and `status_unknown` cohorts. Active decisions already appear to unblock correctly.

## 4. Tighten SearXNG Engine Config

### Current Evidence

SearXNG is reachable and still the best strategic provider, but its engine pool is degraded.

Observed SearXNG and fallback issues:

- SearXNG `engine_unresponsive`: 6,575 attempts in 24h.
- SearXNG `low_signal_fallback`: 13,400 attempts in 24h.
- Bing HTML `challenge_or_anomaly`: 8,550 attempts in 24h.
- DuckDuckGo HTML `timeout`: 8,203 attempts in 24h.
- Local `.env` only configures `localhost:8888`; rescue endpoint and stable engines are empty.

Unresponsive or bad SearXNG engines observed:

- `brave`: suspended too many requests.
- `duckduckgo`: CAPTCHA or timeout.
- `startpage`: CAPTCHA.
- `karmasearch`: access denied.
- `wikipedia`: too many requests.

### Concept A: Increase SearXNG Throughput

Description:

Increase `CRAWLER_SEARCH_SEARXNG_MAX_IN_FLIGHT`, lower request intervals, or add more worker concurrency.

Optimistic view:

- Could increase throughput if the bottleneck is crawler-side pacing.
- Simple config-only change.

Pessimistic view:

- Evidence points to upstream engines being suspended or CAPTCHAed, not local underuse.
- More concurrency can make upstream suspension worse.
- Could increase low-signal and provider-unavailable churn.

Data-driven view:

- Current config already uses conservative pacing: max in flight 1 and 750ms interval.
- Failures include CAPTCHA, suspended, timeout, and access denied. More pressure is likely harmful.

Recommendation:

Do not increase throughput until engine selection is stable.

### Concept B: Stable Engine Allowlist And Rescue Endpoint

Description:

Configure SearXNG to use only engines that pass smoke tests, and enable a secondary local endpoint on port 8889.

Optimistic view:

- Directly avoids known-bad engines.
- Rescue endpoint gives failover without changing architecture.
- Keeps SearXNG primary while reducing volatility.

Pessimistic view:

- If all free engines degrade, allowlisting only helps so much.
- Requires maintaining an engine smoke routine.
- Some query classes may need different engines.

Data-driven view:

- The current problem is not endpoint down; it is engine-specific degradation.
- The repo already supports `CRAWLER_SEARCH_SEARXNG_STABLE_ENGINES`, `CRAWLER_SEARCH_SEARXNG_RESCUE_ENGINES`, and `CRAWLER_SEARCH_SEARXNG_BASE_URLS`.

Recommendation:

Use this as the primary SearXNG fix.

### Concept C: Promote A Managed Provider Backup

Description:

Use Serper, Tavily, DataForSEO, or another API as the main backup when SearXNG is degraded.

Optimistic view:

- Cleaner than HTML scraping.
- Could stabilize official evidence refresh.
- Reduces reliance on fragile free HTML providers.

Pessimistic view:

- Current Serper/Tavily attempts show quota exceeded.
- Adds cost and auth/quota operational burden.
- Must be evaluated by accepted evidence rate, not raw SERP success.

Data-driven view:

- Existing managed integrations are not currently viable in this environment.
- This should remain a smoke-tested backup, not the immediate fix.

Recommendation:

Keep as phase two after SearXNG allowlist/rescue endpoint.

### Final Recommendation For SearXNG

Do not increase load. First run engine smoke tests, configure a stable engine allowlist, enable the 8889 rescue endpoint, and keep Bing/DuckDuckGo as last-resort only. Consider a managed backup only after isolated smoke tests pass.

## 5. Improve Worker Phase Observability

### Current Evidence

The worker is alive, but `running_jobs = 0` often misleads operators.

Examples:

- 22:34:17 batch finished: 120 processed, 0 requeued.
- 22:35:42 batch finished: 109 processed, 11 requeued.
- 22:36:33 batch finished: 119 processed, 1 requeued.
- 22:37:08 batch finished: 120 processed, 0 requeued.
- 22:38:53 batch finished: 111 processed, 9 requeued.
- `verify_school_match` p95 running duration is about 0.1002s.

### Concept A: Increase Polling Frequency Or Keep Jobs Running Longer

Description:

Make jobs easier to observe by polling faster or keeping rows in `running` longer.

Optimistic view:

- Dashboard would show nonzero running more often.
- Minimal schema changes.

Pessimistic view:

- Polling faster increases DB load.
- Artificially keeping jobs running is misleading and could interfere with recovery.
- It treats the symptom, not the observability gap.

Data-driven view:

- Jobs are genuinely fast. Making them look slower is the wrong metric fix.

Recommendation:

Do not use this.

### Concept B: Add Worker Phase To `worker_processes.metadata`

Description:

Persist a worker phase during the main loop:

- `polling`
- `preflight`
- `triage`
- `claiming`
- `executing`
- `aggregating`
- `sleeping`

Optimistic view:

- Low-risk additive change.
- Explains why `running_jobs` is zero while workers are alive.
- Useful for dashboards, CLI doctor, and alerts.

Pessimistic view:

- Requires touching worker loop and supervisor code.
- Phases need to be updated reliably or they become stale/misleading.

Data-driven view:

- Batch logs already imply these phases; they just are not persisted.
- Worker lease exists, but phase does not.

Recommendation:

Use this as the primary observability fix.

### Concept C: Add Batch Timeline Table

Description:

Persist each batch lifecycle event to a durable table:

- preflight started/finished
- triage started/finished
- chunks prepared
- execution started/finished
- aggregation finished

Optimistic view:

- Best historical analysis.
- Makes benchmarks and queue health reports stronger.
- Enables p50/p95 phase duration.

Pessimistic view:

- More schema and storage.
- More complex than needed for immediate operator clarity.

Data-driven view:

- Useful, but phase heartbeat solves the immediate confusion at lower cost.

Recommendation:

Add later if phase heartbeat is not enough.

### Final Recommendation For Worker Observability

Add worker phase heartbeat first. Do not inflate `running_jobs` or slow jobs down. If more analysis is needed later, add a durable batch timeline table.

## Combined Recommended Implementation Plan

### Phase 1: Cheap Safety-Preserving Clarity

1. Add worker phase heartbeat to `worker_processes.metadata`.
2. Add dashboard/API display of worker phase.
3. Split `status_dependency_unmet` into precise reason codes.

Expected result:

- Operators stop interpreting `running_jobs = 0` as dead workers.
- Queue blockers become more actionable.

### Phase 2: Provider Stability

1. Run `searxng-engine-smoke`.
2. Configure `CRAWLER_SEARCH_SEARXNG_STABLE_ENGINES`.
3. Start/configure rescue endpoint on `localhost:8889`.
4. Set `CRAWLER_SEARCH_SEARXNG_BASE_URLS=http://localhost:8888,http://localhost:8889`.
5. Keep managed providers quarantined unless smoke tests pass.

Expected result:

- Lower `engine_unresponsive`.
- Lower fallback churn into Bing/DuckDuckGo.

### Phase 3: Identity Repair Throughput

1. Add deterministic school alias resolver.
2. Normalize `At <School>` prefixes.
3. Strip status text from chapter names.
4. Run bounded repair replay on high-volume identity blockers.

Expected result:

- Lower `identity_semantically_incomplete`.
- More jobs become eligible for official status evidence refresh.

### Phase 4: Official Evidence Refresh

1. Add bounded `refresh-school-evidence` path.
2. Target top `school_evidence_missing` cohorts.
3. Persist official activity/status evidence.
4. Reconcile blocked verify-school and contact jobs.

Expected result:

- Lower `school_evidence_missing`.
- Lower `status_no_decision` and `status_unknown`.
- More safe contact jobs become actionable.

## Final Opinion

The best path is not to loosen gates or make search more aggressive. The safest high-throughput path is:

```text
better visibility
then better provider hygiene
then better identity normalization
then bounded official evidence creation
```

That sequence preserves the accuracy work already done while attacking the actual root causes shown by the data.

