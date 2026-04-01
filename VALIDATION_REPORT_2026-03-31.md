# Validation Report - 2026-03-31

## Scope
- Validate frontend/API behavior for fraternity intake after registry-first + navigation upgrade.
- Verify staged crawl request flow for fraternities not previously in `fraternities`.
- Benchmark crawl/enrichment capability and throughput on a new-fraternity cohort.

## What Was Validated

### 1) Frontend/API integration
- `pnpm --filter @fratfinder/web typecheck` passed.
- Intake API request payload now carries discovery trace metadata:
  - `sourceProvenance`
  - `fallbackReason`
  - `resolutionTrace`
- Verified via live API response payload for a newly requested fraternity (`Chi Psi`) showing:
  - `sourceProvenance = verified_registry`
  - non-empty `resolutionTrace`

### 2) Crawler/backend safety and correctness
- `python -m pytest services/crawler/src/fratfinder_crawler/tests -q` passed.
- Registry bootstrap + revalidation command paths are operational:
  - `bootstrap-nic-sources --input research_nav_21.json --dry-run`
  - `bootstrap-nic-sources --input research_nav_21.json`
- `verified_sources` populated successfully (`21` rows).

### 3) Intake staged workflow behavior
- Staged request events were persisted and correctly sequenced:
  - `request_created`
  - `request_queued`
  - `stage_started`
  - terminal `stage_failed` for zero-chapter cases (safety gate confirmed)
- Example (`chi-psi`) shows zero-chapter halt behavior working as intended.

## Benchmark Cohort (Not In DB At Selection Time)
- `alpha-delta-gamma`
- `alpha-delta-phi`
- `alpha-gamma-rho`
- `beta-upsilon-chi`
- `chi-phi`

## Benchmark Configuration
- Discovery: registry-first (`verified_sources` loaded)
- Crawl run per source: `python -m fratfinder_crawler.cli run --source-slug <slug>`
- Enrichment cycles per field:
  - fields: `find_website`, `find_email`, `find_instagram`
  - workers: configured max (`10` in current env)
  - limit per field batch: `40`
- Provider profile observed during run:
  - `CRAWLER_SEARCH_ENABLED=true`
  - `CRAWLER_SEARCH_PROVIDER=bing_html`

## Results Summary

### Discovery quality
- 4/5 selected directly from `verified_registry` with high confidence and fast latency (`~20-26ms`).
- 1/5 (`chi-phi`) fell back safely due unhealthy seed (`410 Gone` source), with low confidence and explicit fallback reason.

### Crawl/extraction outcomes
- `alpha-gamma-rho`: strong success
  - `records_seen=74`
  - `records_upserted=74`
  - `field_jobs_created=222`
- `alpha-delta-gamma`, `alpha-delta-phi`, `beta-upsilon-chi`: partial runs with `empty_extraction` review items.
- `chi-phi`: failed cleanly with recorded `crawl_failure` (`410 Gone`) and no unsafe writes.

### Contact coverage snapshot after benchmark
- `alpha-gamma-rho`: 74 chapters total
  - website: 61
  - instagram: 19
  - email: 13
- Other four benchmark fraternities: 0 chapters ingested in this run.

### Queue/throughput behavior
- `alpha-gamma-rho` field jobs after one bounded pass:
  - `done=32`, `queued=190`
- No stuck/running leak observed in sampled statuses; remaining workload is queued (expected).

## Success Criteria Check

### A) Request crawl for any specific fraternity
- Pass (API/backend): can create request by fraternity name even when absent in `fraternities`.
- Pass (discovery visibility): provenance/trace now present and persisted.

### B) Robustness/safety
- Pass:
  - unhealthy source handled as explicit failure/review path (`chi-phi` 410).
  - zero-chapter crawl gate blocks false success and prevents enrichment churn.
  - no unsafe auto-writes observed in failed/partial cases.

### C) Capability/performance improvement
- Improved:
  - discovery speed and determinism (registry-first high-confidence in milliseconds).
  - operator observability (traceable decisions in progress payload).
  - navigation path can ingest real chapter sets on compatible sites (`alpha-gamma-rho`).
- Not yet sufficient for all targets:
  - multiple sources still produce `empty_extraction` due site-specific structure gaps.

## Primary Remaining Bottlenecks
- Non-uniform chapter directory layouts still bypass current adapter/stub extraction on some national sites.
- Some “chapter directory” URLs are marketing or client-side views that need source-specific locator/script handling.
- Current single-pass benchmark showed queue depth remains high after initial enrichment pass for large ingests.

## Recommended Next Iteration
1. Add source-specific extraction hints for known problematic nationals in `verified_sources.metadata` (mode + selectors/API hints).
2. Promote review-item-driven auto-learning:
   - when `empty_extraction` repeats for a source, capture DOM signatures and queue targeted parser rule generation.
3. Add intake-side “preflight source probe” card:
   - page role, mode detection, expected extractor family, and chapter-stub count before running full crawl.
4. Run multi-cycle throughput benchmark on `alpha-gamma-rho` until queue drains to measure true end-to-end contact fill rate.

## Continuation Update (2026-03-31 late pass)

### What was changed in this pass
- Hardened navigation fallback precision:
  - constrained Wix fallback extraction to Wix-like pages only in [navigation.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/navigation.py)
  - added URL fetch de-duplication cache hit accounting in navigation follow stage
- Added normalization safety gate for placeholder/navigation records in [normalizer.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/normalization/normalizer.py):
  - blocks `find-a-chapter`, `our-chapters`, `chapter-roll`, `the-byx-at-your-university`
  - blocks known noisy prefixes (`visit-page-*`, `society-chapters-*`)
  - routes these to `ambiguous_record` review instead of persisting bad chapters
- Added regression tests in [test_normalizer.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/tests/test_normalizer.py) for placeholder rejection.

### Validation status
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_navigation_upgrade.py -q` passed.
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_normalizer.py -q` passed.
- `python -m pytest services/crawler/src/fratfinder_crawler/tests -q` passed.

### Re-benchmark focus (previously weak sources)
Recrawled sources:
- `alpha-delta-gamma-main`
- `alpha-delta-phi-main`
- `beta-upsilon-chi-main`

Latest crawl-run results:
- `alpha-delta-gamma-main` (`run_id=39`): `records_seen=36`, `records_upserted=31`, `review_items_created=5`
- `alpha-delta-phi-main` (`run_id=40`): `records_seen=42`, `records_upserted=40`, `review_items_created=2`
- `beta-upsilon-chi-main` (`run_id=41`): `records_seen=40`, `records_upserted=36`, `review_items_created=4`

### Coverage delta for new-fraternity cohort
Previous pass for weak trio had effectively no retained clean coverage. Current retained coverage:
- `alpha-delta-gamma`: `chapters=31`, `website=31`, `instagram=31`, `email=31`
- `alpha-delta-phi`: `chapters=40`, `website=0`, `instagram=1`, `email=0`
- `beta-upsilon-chi`: `chapters=36`, `website=36`, `instagram=0`, `email=35`
- Combined (three fraternities): `chapters=107`, `website=67`, `instagram=32`, `email=66`

### Quality checks
- Noisy persisted slugs from prior pass (`find-a-chapter`, `our-chapters`, `chapter-roll`, `visit-page-*`, `the-byx-at-your-university`) were removed from DB and now reclassify as review items.
- Verification query confirms zero remaining rows matching those noisy slug patterns.

### Remaining bottlenecks surfaced
- `alpha-delta-phi` currently has low contact yield (mostly no website/email/instagram), despite good chapter extraction.
- `beta-upsilon-chi` has strong website/email but zero Instagram in this profile, indicating search-query mismatch and/or chapter social naming variance.
- Large queues remain (`find_instagram`/`find_email`/`verify_website`) for these sources and need additional processing cycles or better per-field strategy.

### Next high-impact iteration
1. Improve ADP/BYX per-field strategy before expanding breadth:
   - ADP: chapter detail page traversal + email/instagram extraction from linked chapter sites.
   - BYX: Instagram query alias expansion for school + `byx`/`beta upsilon chi` handle forms.
2. Add low-signal dedupe guard to avoid repeatedly writing national-level instagram to many chapters when no chapter-specific anchor is present.
3. Run 3-cycle bounded enrichment benchmark on these three sources and compare per-cycle lift.

## Continuation Update (2026-03-31 final pass)

### Stability and safety fixes implemented
- Added browser-header HTTP wrappers for field-job page fetches and website verification in [field_jobs.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/field_jobs.py).
- Added `HEAD -> GET` fallback in website verification so sites that block `HEAD` can still be verified safely.
- Hardened related-website backfill guard:
  - generic source directory/list URLs are now blocked from being backfilled as chapter `website_url` during email/instagram enrichment.
  - added regression test `test_candidate_result_does_not_backfill_generic_source_directory_url_as_website` in [test_field_jobs_engine.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py).
- Adjusted `verify_website` missing-website path to `dependency_wait` requeue with preserved attempts (no terminal fail churn).

### Validation run findings
- Test suites passed:
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py -q`
  - `python -m pytest services/crawler/src/fratfinder_crawler/tests -q`
- Live BYX Instagram batches after patch no longer write generic national directory websites into chapter rows.
- Confirmed cleanup: no rows currently set to the previously bad values (`https://byx.org/join-a-chapter`, `http://byx.org/contact-us`, generic `betaupsilonchi` instagram backfill).

### Current hard blocker for enrichment lift
- Search provider health remains degraded in this environment:
  - Bing HTML returns anti-bot challenge pages.
  - Resulting field jobs requeue with `search provider or network unavailable` and cannot progress on search-dependent chapters.
- This is the dominant reason latest benchmark cycles still show high requeue and low processed counts for search-heavy sources.

### Immediate next action to unlock measurable lift
1. Restore reliable search connectivity (preferred: `CRAWLER_SEARCH_PROVIDER=auto` with valid Brave API key, or approved alternate provider path).
2. Re-run the 5-source validation benchmark set after provider recovery to measure true post-fix lift.
3. Keep current safety guards enabled to prevent generic source-url contamination during high-volume runs.

## Continuation Update (2026-03-31 late-night pass)

### Instagram fallback iteration and safety rollback
- Implemented a direct Instagram-handle probe fallback path in [field_jobs.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/field_jobs.py), then validated it against live traffic.
- During live validation, observed a critical reliability issue: Instagram logged-out HTML pages do not provide deterministic profile-existence signals (non-existent handles can still return generic `200` pages).
- Result: direct probing can produce synthetic false positives if used as an auto-write source.
- Final mitigation shipped:
  - added `CRAWLER_SEARCH_INSTAGRAM_DIRECT_PROBE_ENABLED` in [config.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/config.py) (default `false`)
  - wired through [pipeline.py](/d:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/pipeline.py)
  - documented in [.env.example](/d:/VSC%20Programs/FratFinderAI/.env.example) and [README.md](/d:/VSC%20Programs/FratFinderAI/README.md)
  - probe tests remain, but now explicitly opt-in.

### Additional Instagram precision hardening
- Tightened Instagram gating to reject weak handles missing fraternity anchor:
  - `_instagram_looks_relevant_to_job` now requires fraternity-handle anchor for mid-score chapter-signal cases.
  - `_instagram_search_candidate_passes_gate` now rejects search-origin handles with no fraternity token unless explicit chapter designation evidence exists.
- This directly blocked observed false-positive class (for example, unrelated Greek handles returned in partially degraded Bing result sets).

### Data hygiene action
- Cleared and re-queued chapter Instagram values where the latest write came from `instagram_probe:*` query provenance:
  - `probe_latest_chapters=188`
  - `instagram_cleared=188`
  - `requeued_jobs=188`
- This preserves strict write safety over raw coverage when source verification is ambiguous.

### Benchmarks written to dashboard (new runs)
- `63ca2487-fad5-4f00-84d5-4b976bdcc9a8` (`alpha-delta-phi-main`): processed `0`, requeued `79`
- `e188155a-a76d-4bc8-b55b-8215ff784785` (`beta-upsilon-chi-main`): processed `18`, requeued `21`
- `eeaf74dd-4f85-40b7-af1c-fe4c5aa6de81` (`sigma-chi-main`): processed `134`, requeued `346`
- `59ad50b2-0513-4566-b5aa-1db65f6fa0c2` (`alpha-gamma-rho-main`): processed `31`, requeued `68`
- `3b79c7a3-6f85-4b48-a5ed-fd67f72fea3b` (`delta-chi-main`): processed `1`, requeued `35`

### Current bottleneck recap (ADP/BYX focus)
- `alpha-delta-phi-main` queue remains blocked primarily by search-provider instability:
  - large `find_website`/`find_instagram` requeues with `search provider or network unavailable`
  - email queue mostly waiting on confident website dependency
- `beta-upsilon-chi-main` shows similar transient-network-driven Instagram requeues.
- With probe fallback disabled by default (safety), sustainable lift now depends on restoring reliable search connectivity.

## Continuation Update (2026-03-31 post-safety benchmark)

### Fresh benchmark runs written to dashboard
- `0e65b737-8a2a-42e9-b117-a24f2c704c97` (`alpha-delta-phi-main`): processed `0`, requeued `101`, queue delta `0`
- `bdd971e5-63a2-4d54-b443-cad609210804` (`beta-upsilon-chi-main`): processed `0`, requeued `21`, queue delta `0`
- `24a6e7af-de75-4019-82bd-26d63ec7403e` (`sigma-chi-main`): processed `0`, requeued `480`, queue delta `0`
- `fe78b45f-ee51-48f8-9e2f-53c706d63ced` (`alpha-gamma-rho-main`): processed `0`, requeued `68`, queue delta `0`
- `8c03a4ad-06fe-4865-bf97-ee294635888d` (`delta-chi-main`): processed `0`, requeued `35`, queue delta `0`

### Interpretation
- System behavior is now safety-conservative and deterministic under provider stress:
  - no unsafe probe-driven writes,
  - no queue deadlocks,
  - but near-zero throughput when search providers are challenge-blocked.
- Primary blocker is external search availability, not queue orchestration or state transitions.

### Coverage snapshot after safety cleanup
- `alpha-delta-phi`: `chapters=40`, `website=4`, `instagram=2`, `email=1`
- `beta-upsilon-chi`: `chapters=36`, `website=34`, `instagram=0`, `email=35`
- `sigma-chi`: `chapters=243`, `website=3`, `instagram=7`, `email=5`
- `alpha-gamma-rho`: `chapters=74`, `website=61`, `instagram=36`, `email=28`
- `delta-chi`: `chapters=49`, `website=41`, `instagram=45`, `email=16`
