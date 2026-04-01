# Validation Report - 2026-04-01 Precision Hardening

## Scope
This pass focused on website-enrichment precision hardening after live batch tests showed unsafe auto-writes from:
- nationals-directory overmatching
- partial Greek-token organization matching
- generic chapter slug tokens acting like identity evidence

The goal of this pass was not to maximize coverage blindly. It was to stop provably wrong website assignments while preserving throughput and keeping uncertain candidates in review or requeue paths.

## Code Changes
Implemented in `services/crawler/src/fratfinder_crawler/field_jobs.py` and covered by `services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py`:
- Nationals entries now require real school/chapter evidence before they can be used for the target chapter.
- Fraternity matching for multi-token Greek-letter organizations is stricter; `Gamma Rho` no longer matches `Alpha Gamma Rho`.
- Short fraternity initialisms no longer match arbitrary URL substrings.
- Generic slug tokens such as `chapter`, `colony`, `active`, and `provisional` no longer count as chapter identity evidence.
- Added regressions for:
  - Delta Chi Canada misassignment
  - Alpha Gamma Rho vs Sigma Gamma Rho confusion
  - safer nationals-directory website behavior

## Test Status
Passed:
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_search_client.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests -q`
- `pnpm --filter @fratfinder/web typecheck`

## Batch Inputs Reviewed
### Earlier bad random sample
Before this pass, the following rows were polluted by clearly wrong website values:
- `west-texas-a-m-colony-active-west-texas-a-m-university -> https://sgrho1922.org/`
- `chi-suspended-california-polytechnic-state-university -> https://www.sjsu.edu/getinvolved/fraternity-and-sorority-life/chapters-and-councils/usfc.php`
- 10 Delta Chi chapters -> `http://www.deltachi.ca/`

Those 12 rows were cleared and requeued at high priority for validation.

## Live Validation Results
### Random/arbitrary sample rerun
Sources:
- `logs/20260401_013200_agr_precision_rerun.log`
- `logs/20260401_013200_dchi_precision_rerun.log`

#### Alpha Gamma Rho sample
Before:
- 2/2 targeted rows were unsafe auto-writes

After:
- `west-texas-a-m-colony-active-west-texas-a-m-university`
  - website remains blank
  - result routed to review from school org page
  - no unsafe write
- `chi-suspended-california-polytechnic-state-university`
  - website remains blank
  - job requeued with `No candidate website URL available`
  - no unsafe write

Outcome:
- Unsafe auto-write rate dropped from 100% to 0% for this AGR sample.

#### Delta Chi sample
Before:
- 10/10 targeted Delta Chi rows were unsafe auto-writes to `http://www.deltachi.ca/`

After:
- Review-required, no write:
  - `colorado-state-chapter-colorado-state`
  - `denver-chapter-denver`
  - `louisiana-tech-chapter-louisiana-tech`
  - `michigan-state-university-provisional-chapter-michigan-state-university-provisional`
  - `rutgers-chapter-rutgers`
- Requeued, no write:
  - `illinois-state-chapter-illinois-state`
  - `indiana-university-provisional-chapter-indiana-university-provisional`
  - `jacksonville-state-chapter-jacksonville-state`
  - `purdue-provisional-chapter-purdue-provisional`
  - `southern-arkansas-chapter-southern-arkansas`

Outcome:
- Unsafe auto-write rate dropped from 100% to 0% for the targeted Delta Chi sample.
- 5 rows surfaced reviewable leads instead of poisoning the DB.
- 5 rows stayed unresolved and need better source-native extraction or stronger school disambiguation.

### Control regression rerun
Source:
- `logs/20260401_013900_adp_regression_rerun.log`

Control rows:
- `northwestern-northwestern-university`
- `amherst-amherst-college`

Outcome:
- `northwestern-northwestern-university` completed with `updates: {}` and did not reintroduce a bad website.
- `amherst-amherst-college` requeued with `No candidate website URL available` and did not reintroduce a bad website.

This confirms the new precision rules did not regress those earlier stabilized ADP cases.

### Ambiguous-school validation rerun
Source:
- `logs/ambiguous_school_validation_2026-04-01_targeted.log`

Target rows:
- `colorado-state-chapter-colorado-state`
- `denver-chapter-denver`
- `rutgers-chapter-rutgers`

Outcome:
- `colorado-state-chapter-colorado-state`
  - remained a review candidate at `https://fsl.colostate.edu/chapters/delta-chi/`
  - this is the desired outcome because the path contains explicit fraternity identity and campus context
- `rutgers-chapter-rutgers`
  - remained a review candidate at `https://www.rutgerspdc.org/about-1`
  - still ambiguous enough to keep out of auto-write, but preserved as a useful operator lead
- `denver-chapter-denver`
  - did **not** complete into review from the generic `msudenver.edu` organization directory
  - requeued instead, with rejection telemetry showing `website:ambiguous_school_tier1_generic`
  - this is the intended safety behavior for one-token ambiguous schools

This validation confirms the new ambiguous-school guard is active in real queue processing, not just unit tests.

## Throughput Notes
- Search preflight remained healthy with SearXNG-first and Tavily fallback.
- The precision pass improved safety more than throughput.
- These reruns still issue large query families per unresolved job, so unresolved cases remain expensive.
- The engine now prefers no write/review over wrong write, which is the correct production tradeoff at this stage.

## What Improved Materially
- Wrong nationals-directory website assignments were eliminated in the validated sample.
- Cross-fraternity confusion between similar Greek names was eliminated in the validated sample.
- Generic slug tokens no longer inflate chapter matching.
- Ambiguous one-token school pages now fail safely instead of advancing generic campus-directory candidates.
- Review queue quality improved because ambiguous cases are now surfaced instead of auto-written.
- Review operators now have richer context available in the dashboard queue:
  - candidate value
  - confidence
  - source link
  - triggering query
  - rejection summary histogram

## Remaining Gaps
1. One-token or ambiguous school names still cost a full search cycle even when they now fail safely.
   - Example: `denver-chapter-denver` no longer surfaced `msudenver.edu` as a live review outcome, but it still consumed 15 successful search queries before requeueing.
2. Source-native extraction still leaves coverage on the table for some nationals sites.
   - Delta Chi sample showed that we prevented the wrong website, but 5/10 rows still requeued.
3. Search query families are still expensive for unresolved rows.
   - We should continue reducing dependence on broad web search when a national directory has usable structure.

## Recommended Next Iteration
1. Add source-specific extraction hints for known nationals sites.
   - Delta Chi remains a strong candidate for better state-page parsing instead of falling back to broad search.
2. Add search-budget pruning for ambiguous-school branches.
   - Once the engine detects a one-token ambiguous school with only generic `.edu` leads, it should short-circuit earlier instead of exhausting the full website query family.
3. Add candidate provenance and rejection reason filtering in the dashboard.
   - The dashboard now shows this data per row; the next step is to make it filterable and comparable across runs.

## Bottom Line
This pass materially improved production safety.

For the validated sample set:
- Before: 12 clearly wrong website auto-writes across AGR + Delta Chi sample rows
- After: 0 wrong website auto-writes

Coverage did not jump in the unresolved cases, but the system is now significantly more trustworthy and robust, which is the right foundation for the next recall-focused pass.
