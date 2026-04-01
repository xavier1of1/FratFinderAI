# Benchmark Response Iteration 2026-04-01

## Context
This iteration responds directly to the failures documented in [CAMPAIGN_FINAL_ANALYSIS_2026-04-01.md](d:/VSC Programs/FratFinderAI/docs/reports/CAMPAIGN_FINAL_ANALYSIS_2026-04-01.md):

- low coverage:
  - any contact `27.50%`
  - website `7.60%`
  - email `7.45%`
  - instagram `27.20%`
- poor queue efficiency:
  - `623` processed
  - `1699` requeued
  - `46` terminal failures
- dominant failure modes:
  - no chapters discovered from selected national source
  - enrichment budget exhausted before queue drained
  - long-running timeout failures on large fraternities

## Goals For This Iteration
1. Stop weak national-source selection from poisoning crawl requests.
2. Give large fraternities more adaptive enrichment budgets instead of one-size-fits-all cycle limits.
3. Surface the new diagnostics in the website so operators can see why a request is struggling.
4. Improve request execution so degraded-provider protections are used in the same path that real campaign requests follow.

## Changes Made

### 1. Source-quality scoring in the web app
Files:
- `apps/web/src/lib/source-selection.ts`
- `apps/web/src/lib/source-selection.test.ts`
- `apps/web/src/lib/fraternity-discovery.ts`
- `apps/web/src/lib/campaign-runner.ts`

What changed:
- Added deterministic source-quality scoring for discovered source URLs.
- Weak source patterns now include member/alumni/login/portal style URLs.
- Discovery results can now be upgraded toward better chapter-directory candidates before intake requests are created.
- Campaign item creation re-runs discovery when the preferred source quality is weak and upgrades to a stronger source when found.

Why it matters:
- This directly targets the dominant benchmark failure `No chapters discovered from the selected national source`.

### 2. Discovery hardening in the crawler service
Files:
- `services/crawler/src/fratfinder_crawler/discovery.py`
- `services/crawler/src/fratfinder_crawler/tests/test_discovery.py`

What changed:
- Added curated source hints for known problematic fraternities such as Sigma Chi.
- Verified registry URLs now reject member/alumni/memberhub-style paths.
- Existing configured sources now use the same weak-source rejection logic instead of bypassing it.
- Host-hint matching is now domain-aware rather than naive substring matching.
- Search selection can now prefer curated chapter-directory hints over:
  - noisy alumni chapter search hits
  - same-host informational pages like `/history/` when the hint is a chapter-directory path

Why it matters:
- This closes the leak discovered during the live sanity check where Sigma Chi still escaped through the `existing_source` path even after verified-source validation improved.

### 3. Adaptive enrichment budgets for real crawl requests
Files:
- `apps/web/src/lib/fraternity-crawl-request-runner.ts`
- `apps/web/src/lib/types.ts`
- `apps/web/src/components/fraternity-intake-dashboard.tsx`

What changed:
- Added adaptive enrichment configuration based on:
  - discovered chapter count
  - queue pressure
  - low-progress cycles
  - degraded cycles
- Increased campaign request defaults so larger fraternities are not forced through tiny cycle budgets.
- Added one-time source recovery when a crawl run discovers zero chapters.
- Added enrichment analytics into request progress so the website exposes the current budget strategy.
- Real request execution now includes `--run-preflight` during field-job processing.

Why it matters:
- This directly targets the benchmark failure `Enrichment cycle budget exhausted before queue drained`.
- It also makes provider-degradation protections active in the same request path used by campaigns.

## Live Validation

### Automated checks
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_discovery.py -q`
- `pnpm --filter @fratfinder/web test -- source-selection`
- `pnpm --filter @fratfinder/web typecheck`

All passed.

### Live Sigma Chi sanity check
Command:
- `python -m fratfinder_crawler.cli discover-source --fraternity-name "Sigma Chi"`

Observed result after the fixes:
- `selected_url`: `https://sigmachi.org/chapters/`
- weak verified source rejected: `https://members.sigmachi.org/alumnigroups`
- weak existing source rejected: `https://members.sigmachi.org/alumnigroups`
- final selection uses the curated chapter-directory hint instead of:
  - alumni chapter search results
  - same-host informational pages like `/history/`

Why this matters:
- Sigma Chi was one of the benchmark's notable low-yield failures.
- This is a concrete live example of the new iteration correcting the exact source-selection pathology that previously starved crawl coverage.

## Remaining Work
This iteration materially improves the benchmark response, but it does not yet prove the final target metrics on its own. The next validating step should be a targeted rerun or mini-campaign focused on:

1. Sigma Chi
2. Chi Psi
3. Kappa Delta Rho
4. Delta Kappa Epsilon
5. Alpha Tau Omega
6. One previously strong performer such as Delta Sigma Phi as a control

Metrics to compare against the last campaign:
- chapters discovered per fraternity
- any-contact success rate
- website coverage
- requeue rate
- cycles exhausted
- jobs per minute
- source recovery frequency

## Expected Impact
Most likely gains from this iteration:
- fewer zero-chapter failures from bad national source selection
- more stable campaign startup quality
- less wasted retry churn on large fraternities that simply needed bigger budgets
- better operator visibility into weak-source and budget-exhaustion problems

These changes are specifically aimed at the three biggest benchmark bottlenecks and should improve both coverage and operational efficiency in the next campaign pass.

## Additional Iteration: Source-Hint Execution and Parser Breadth

After the initial benchmark-response fixes, the next pass focused on turning benchmark learnings into executable parser behavior instead of leaving them as operator knowledge.

### Additional changes made

#### 4. Metadata-driven extraction hints in the crawler
Files:
- `services/crawler/src/fratfinder_crawler/analysis/strategy_selector.py`
- `services/crawler/src/fratfinder_crawler/orchestration/navigation.py`
- `services/crawler/src/fratfinder_crawler/orchestration/graph.py`
- `services/crawler/src/fratfinder_crawler/models.py`
- `services/crawler/src/fratfinder_crawler/adapters/base.py`
- `services/crawler/src/fratfinder_crawler/adapters/locator_api.py`
- `services/crawler/src/fratfinder_crawler/adapters/script_json.py`

What changed:
- `sources.metadata.extractionHints` can now influence:
  - `chapterIndexMode`
  - `primaryStrategy`
  - `fallbackStrategies`
  - `stubStrategies`
- The graph and navigation layers now pass source metadata all the way into adapter execution.
- Source-specific parser hints are now part of runtime extraction instead of passive metadata.

Why it matters:
- This creates the mechanism needed to address recurring low-yield nationals without hardcoding fraternity-specific logic deep in the parser core.

#### 5. Broader directory parsing for card archives
Files:
- `services/crawler/src/fratfinder_crawler/adapters/directory_v1.py`
- `services/crawler/src/fratfinder_crawler/tests/test_directory_adapter.py`

What changed:
- Added support for Bootstrap-style chapter cards such as:
  - `.grid-item .card`
  - `.card.h-100`
  - `.card-title a`
- Added title splitting for combined patterns like:
  - `Beta - Cornell University`

Why it matters:
- This directly improves source-native recall on card-based nationals such as Kappa Delta Rho.

#### 6. Source backfill migration
Files:
- `infra/supabase/migrations/0010_benchmark_source_hint_backfills.sql`

What changed:
- Added a conservative migration to:
  - move Sigma Chi toward `https://sigmachi.org/chapters/`
  - attach KDR extraction hints for card selectors and direct-list mode

Why it matters:
- This reduces dependence on rediscovery for known benchmark-problem sources and makes the benchmark response durable in database state.

### Additional automated validation
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_directory_adapter.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_analysis_upgrade.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_adaptive_adapters.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_navigation_upgrade.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_discovery.py -q`

All passed after the parser-hint plumbing and card-archive support landed.

### Updated expected impact
The iteration now targets all three dominant benchmark bottlenecks more directly:
- weak source-native parsing:
  - improved by metadata-driven strategy hints and broader card parsing
- too-small enrichment budgets:
  - improved by adaptive request-level enrichment budgets
- provider degradation pressure:
  - improved by ensuring preflight/degraded-mode logic is used during real request execution

## Additional Iteration: Targeted Source Recovery Validation

After the source-quality, adaptive-budget, and extraction-hint passes, the next validation loop focused on the fraternities that most clearly exposed the campaign's bottlenecks. This was a live recovery tranche, not just another unit-test sweep.

### Additional changes made

#### 7. Navigation hardening for Sigma Chi style chapter-roll noise
Files:
- `services/crawler/src/fratfinder_crawler/orchestration/navigation.py`
- `services/crawler/src/fratfinder_crawler/tests/test_navigation_upgrade.py`

What changed:
- Added blocking heuristics for chapter-roll stub extraction when the text block is actually navigation or site chrome.
- Rejected overlong "chapter" and "school" strings that were really menu text, quick links, or page fragments.

Why it matters:
- Sigma Chi's official chapter page was previously producing false chapter stubs from navigation content.
- After this fix the source became a safe `0-record` outcome instead of creating poisoned pseudo-chapters.

#### 8. HTTP posture hardening for hostile official sites
Files:
- `services/crawler/src/fratfinder_crawler/http/client.py`
- `services/crawler/src/fratfinder_crawler/config.py`
- `.env.example`
- `services/crawler/src/fratfinder_crawler/tests/test_http_client.py`

What changed:
- The crawler now uses a browser-like default request posture when no stronger user-agent is configured.
- Added browser-style request headers and origin referer behavior so official sites that reject bot-like defaults are less likely to fail with transport-level 403s.

Why it matters:
- This directly recovered Delta Kappa Epsilon from a hard `403` failure into a successful source-native crawl.

#### 9. Chi Psi source backfill and header-aware table parsing
Files:
- `infra/supabase/migrations/0011_targeted_source_backfills_and_http_hardening.sql`
- `services/crawler/src/fratfinder_crawler/adapters/directory_v1.py`
- `services/crawler/src/fratfinder_crawler/tests/test_directory_adapter.py`

What changed:
- Backfilled Chi Psi from the generic site root to `https://chipsi.org/where-we-are/`.
- Added header-aware table parsing so the crawler can read live tables that use labels like `ALPHA`, `SYMBOL`, and `COLLEGE` instead of assuming fixed column positions.
- Added value cleanup for soft-hyphen and mixed-text cell artifacts.

Why it matters:
- Chi Psi moved from a zero-chapter failure to a valid official-source parse with clean chapter and university fields.

#### 10. DKE KML splitting for combined chapter-school titles
Files:
- `services/crawler/src/fratfinder_crawler/adapters/locator_api.py`
- `services/crawler/src/fratfinder_crawler/tests/test_adaptive_adapters.py`

What changed:
- Added splitting for KML placemark names like `Vanderbilt University - Gamma` so the school and chapter are stored separately instead of as one fused field.

Why it matters:
- Delta Kappa Epsilon now produces cleaner source-native chapter records after the HTTP hardening recovered the feed itself.

#### 11. ATO official mapdata support and source recovery
Files:
- `services/crawler/src/fratfinder_crawler/adapters/script_json.py`
- `services/crawler/src/fratfinder_crawler/discovery.py`
- `services/crawler/src/fratfinder_crawler/tests/test_adaptive_adapters.py`
- `services/crawler/src/fratfinder_crawler/tests/test_discovery.py`
- `infra/supabase/migrations/0012_alpha_tau_omega_map_backfill.sql`

What changed:
- Added support for inline Mapplic `data-mapdata` payloads embedded in official chapter map pages.
- Discovery now prefers a curated deeper official directory path over a generic same-host root when that root is not itself the chapter directory.
- Backfilled Alpha Tau Omega to the official map page `https://ato.org/home-2/ato-map/` and pinned extraction to `script_json`.

Why it matters:
- Alpha Tau Omega moved from `unsupported_or_unclear_source` into a successful crawl with real chapter output.

### Additional automated validation
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_adaptive_adapters.py services/crawler/src/fratfinder_crawler/tests/test_discovery.py -q`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_directory_adapter.py services/crawler/src/fratfinder_crawler/tests/test_analysis_upgrade.py services/crawler/src/fratfinder_crawler/tests/test_adaptive_adapters.py services/crawler/src/fratfinder_crawler/tests/test_navigation_upgrade.py services/crawler/src/fratfinder_crawler/tests/test_discovery.py services/crawler/src/fratfinder_crawler/tests/test_http_client.py -q`

All passed.

### Live targeted validation results

#### Sigma Chi
- Discovery remains corrected to `https://sigmachi.org/chapters/`.
- After navigation-noise hardening the official page now yields a safe no-data outcome instead of bogus chapter-roll records.
- Status: safer, but still a recall gap that needs a source-specific parser.

#### Kappa Delta Rho
- Live run: `pages_processed=61`, `records_seen=87`, `records_upserted=87`, `field_jobs_created=261`
- Status: recovered from zero-chapter benchmark failure to strong source-native extraction.

#### Delta Kappa Epsilon
- Before HTTP hardening: hard `403` transport failure.
- After HTTP hardening and KML splitting:
  - live run `98`
  - `records_seen=108`
  - `records_upserted=108`
  - `field_jobs_created=315`
- Status: recovered and producing cleaner chapter-school separation.

#### Chi Psi
- Before backfill: generic root source with `0` useful chapters.
- After source backfill and header-aware table parsing:
  - live run `97`
  - `records_seen=51`
  - `records_upserted=51`
  - `field_jobs_created=153`
- Status: recovered, though historical bad rows from earlier parses still remain in the DB and should be handled as a separate cleanup decision.

#### Alpha Tau Omega
- Discovery now resolves to `https://ato.org/home-2/ato-map/` with `high` confidence.
- Live run `99`:
  - `pages_processed=2`
  - `records_seen=126`
  - `records_upserted=126`
  - `field_jobs_created=378`
- Status: recovered from `unsupported_or_unclear_source` to a strong official-source crawl.

#### Delta Sigma Phi control
- Control remained strong and continues to serve as a healthy comparison point.

### Updated targeted snapshot
Current chapter-level snapshot for the main benchmark-problem set:
- `alpha-tau-omega`: `126` chapters
- `chi-psi`: `103` chapters
- `delta-kappa-epsilon`: `212` chapters
- `delta-sigma-phi`: `94` chapters, `35` websites, `20` emails, `33` instagrams
- `kappa-delta-rho`: `87` chapters, `87` websites
- `sigma-chi`: `243` chapters, `169` websites, `17` emails, `73` instagrams

### Updated interpretation
This follow-up materially shrinks the benchmark failure set:
- `No chapters discovered from selected national source`
  - significantly improved for KDR, DKE, Chi Psi, and ATO
- `weak source-native parsing`
  - improved through header-aware tables, KML splitting, Mapplic parsing, and navigation-noise rejection
- `queue efficiency`
  - should improve in the next campaign pass because several fraternities that previously churned or failed early can now enter enrichment with real chapter inventories

### Remaining benchmark-response gaps
The next campaign should still watch these areas closely:
1. Sigma Chi still needs source-specific recall work on the official chapters page.
2. Some recovered fraternities now have chapter inventories but not yet strong contact-field coverage.
3. Historical bad rows from older failed parses remain in the database and should be cleaned only with an explicit operator-safe plan.
