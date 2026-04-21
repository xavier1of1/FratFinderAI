# Search Provider Reliability Audit

Date: `2026-04-20`

Primary question: should FratFinderAI preserve and fix the current search-provider stack, or replace it with a new solution?

## Executive Summary

The short answer is:

- Preserve the **search architecture**.
- Preserve and harden **`searxng_json`**.
- Preserve **`bing_html`** only as an opportunistic fallback.
- Demote **`duckduckgo_html`** to last-resort fallback.
- Remove **`brave_html`** from the automatic live chain.
- Quarantine **`serper_api`** and **`tavily_api`** until they pass isolated smoke validation.
- If a new solution is added, add it as a **single high-confidence backup** to the current architecture, not as a full replacement.

The main finding is not that "all providers are bad." The main finding is that **provider performance is unstable across windows**:

- On `2026-04-09`, `searxng_json` was the only materially useful provider in the long stress run.
- On `2026-04-15`, `bing_html` was the only provider with clear preflight wins in the throughput proof window.
- On `2026-04-20`, all three currently configured free providers were degraded in the live preflight window.

That means the platform should not bet on one provider alone, but it also should not throw away the existing search subsystem. The current subsystem already has the right primitives:

- provider chains
- provider ranking
- circuit breakers
- preflight probes
- degraded-mode execution
- authoritative-only fallback behavior

The problem is **provider operations and provider selection**, not the existence of the subsystem itself.

## Scope And Evidence

This audit uses four evidence classes:

### 1. Implementation evidence

- [search/client.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/search/client.py)
- [pipeline.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/pipeline.py)
- [request_graph.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/request_graph.py)
- [config.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/config.py)

### 2. Historical database evidence

From `provider_health_snapshots` in the local Postgres database:

- snapshot range: `2026-04-07 01:30:22+00:00` to `2026-04-10 12:40:25+00:00`
- total snapshots: `540`

### 3. Historical run artifacts

- [DEEP_ANALYSIS_2026-04-09.md](D:/VSC%20Programs/FratFinderAI/docs/reports/stress/DEEP_ANALYSIS_2026-04-09.md)
- [FUNCTIONALITY_REPORT_2026-04-09.md](D:/VSC%20Programs/FratFinderAI/docs/reports/stress/FUNCTIONALITY_REPORT_2026-04-09.md)
- [QUEUE_THROUGHPUT_PROOF_2026-04-15.jsonl](D:/VSC%20Programs/FratFinderAI/docs/SystemReport/QUEUE_THROUGHPUT_PROOF_2026-04-15.jsonl)
- [stress-20260414-rl-train-wave1.jsonl](D:/VSC%20Programs/FratFinderAI/docs/reports/stress/stress-20260414-rl-train-wave1.jsonl)

### 4. Latest live observation

Live commands run during this audit:

- `python -m fratfinder_crawler.cli doctor`
- `python -m fratfinder_crawler.cli search-preflight --probes 4`

Latest observed free-provider order from `doctor`:

1. `searxng_json`
2. `duckduckgo_html`
3. `bing_html`

## Important Measurement Caveat

There is one important observability limitation in the current implementation.

`provider_health_snapshots.healthy` is **not provider-specific health**. In [request_graph.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/orchestration/request_graph.py), each provider snapshot row is inserted with:

- `healthy = bool(preflight_snapshot.get("healthy", False))`

That means the `healthy` boolean reflects the **overall preflight verdict for the batch**, not the true health of the individual provider row.

Because of that:

- `healthy snapshot rate` is **not safe** to use as a provider ranking metric.
- provider comparisons in this report rely primarily on:
  - `payload.attempts`
  - `payload.successes`
  - `payload.request_error`
  - `payload.unavailable`
  - run-artifact probe attempt logs

There is a second observability caveat:

- `search_preflight()` in [pipeline.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/pipeline.py) aggregates only:
  - `attempts`
  - `successes`
  - `unavailable`
  - `request_error`
  - `skipped`
- but `_provider_window_state_from_preflight()` later tries to infer `challenge_or_anomaly_count`

That means some anti-bot failures show up correctly in `probe_outcomes[*].provider_attempts[*].failure_type`, but get flattened in the higher-level provider window summary.

This does not invalidate the audit, but it does mean the attempt-level logs are more trustworthy than the aggregate challenge counters.

## Current Provider Stack Behavior

The existing search subsystem has several strong design properties that should be preserved:

- The client supports multiple providers through one interface.
- Provider chains fail over in-process.
- Circuit breakers stop repeated waste after failure streaks.
- Providers are dynamically ranked by observed success.
- Preflight probes decide whether search-heavy work should run at all.
- Degraded mode preserves authoritative-only progress when search is unhealthy.

These are good production patterns. Replacing the entire subsystem would throw away useful engineering that already works.

## Historical Performance By Window

## Window A: Database Snapshot History (`2026-04-07` to `2026-04-10`)

This is the cleanest persisted provider history available in the local database.

### Provider totals from `provider_health_snapshots.payload`

| Provider | Attempts | Successes | Success % | Request Errors | Unavailable |
| --- | ---: | ---: | ---: | ---: | ---: |
| `bing_html` | 405 | 67 | 16.5% | 0 | 287 |
| `searxng_json` | 385 | 30 | 7.8% | 68 | 282 |
| `duckduckgo_html` | 355 | 1 | 0.3% | 315 | 39 |
| `brave_html` | 338 | 0 | 0.0% | 338 | 0 |
| `serper_api` | 355 | 0 | 0.0% | 355 | 0 |
| `tavily_api` | 355 | 0 | 0.0% | 355 | 0 |

### Interpretation

- `bing_html` was the best performer in this stored window.
- `searxng_json` was second-best and had the highest upside, with a recorded max snapshot success rate of `1.0`.
- `duckduckgo_html` was effectively non-viable.
- `brave_html`, `serper_api`, and `tavily_api` were dead in this window.

## Window B: Long Stress Run (`2026-04-09`)

From [DEEP_ANALYSIS_2026-04-09.md](D:/VSC%20Programs/FratFinderAI/docs/reports/stress/DEEP_ANALYSIS_2026-04-09.md):

| Provider | Total Attempts | Successes | Success % |
| --- | ---: | ---: | ---: |
| `searxng_json` | 1084 | 229 | 21.1% |
| `serper_api` | 857 | 5 | 0.6% |
| `tavily_api` | 861 | 1 | 0.1% |
| `duckduckgo_html` | 861 | 0 | 0.0% |
| `bing_html` | 1683 | 0 | 0.0% |
| `brave_html` | 1707 | 0 | 0.0% |

### Interpretation

- In this longer and more stressful window, `searxng_json` was the **only provider with useful output**.
- `bing_html` completely collapsed here despite being best in the database snapshot window.
- `brave_html` stayed non-viable.
- `serper_api` and `tavily_api` had only token successes.

This is the strongest evidence that the system should not abandon `searxng_json`, because it was the only provider that materially survived a large stress window.

## Window C: Queue Throughput Proof (`2026-04-15`)

From [QUEUE_THROUGHPUT_PROOF_2026-04-15.jsonl](D:/VSC%20Programs/FratFinderAI/docs/SystemReport/QUEUE_THROUGHPUT_PROOF_2026-04-15.jsonl), aggregated across the recorded provider windows:

| Provider | Attempts | Successes | Success % | Request Errors | Unavailable |
| --- | ---: | ---: | ---: | ---: | ---: |
| `bing_html` | 32 | 16 | 50.0% | 0 | 0 |
| `searxng_json` | 16 | 0 | 0.0% | 16 | 0 |
| `serper_api` | 16 | 0 | 0.0% | 16 | 0 |
| `tavily_api` | 16 | 0 | 0.0% | 16 | 0 |
| `duckduckgo_html` | 16 | 0 | 0.0% | 3 | 13 |
| `brave_html` | 16 | 0 | 0.0% | 16 | 0 |

### Interpretation

- In this window, `bing_html` was clearly the best fallback.
- `searxng_json` was fully down.
- `duckduckgo_html` failed mostly through `unavailable` outcomes, consistent with anti-bot or anomaly responses.
- API providers were still effectively dead.

This is the strongest evidence that `bing_html` should remain in the system, even if it should not be trusted as the sole foundation.

## Window D: Latest Live Preflight (`2026-04-20`)

Latest live preflight during this audit:

| Provider | Attempts | Successes | Success % | Observed dominant failure |
| --- | ---: | ---: | ---: | --- |
| `searxng_json` | 4 | 0 | 0.0% | `ConnectionError` / local instance unreachable |
| `duckduckgo_html` | 4 | 0 | 0.0% | challenge / anomaly response |
| `bing_html` | 4 | 0 | 0.0% | challenge / anomaly response |

### Interpretation

- `searxng_json` is currently failing because the local instance is down, not because the integration path is fundamentally broken.
- `duckduckgo_html` and `bing_html` are currently losing to anti-bot behavior.
- The present outage does **not** prove the architecture is wrong. It proves the current live provider conditions are bad.

## Provider-By-Provider Findings

## `searxng_json`

### What the history says

- Best provider in the `2026-04-09` stress run
- Second-best provider in the DB snapshot window
- Highest upside of any provider: max stored snapshot success rate `1.0`
- Currently down because the local instance is unreachable

### Common failure modes

From [search/client.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/search/client.py):

- local service not running -> `ConnectionError`
- base URL missing -> `SearchUnavailableError`
- upstream engines unresponsive -> "unresponsive engines" `SearchUnavailableError`
- no parseable results -> unavailable

### Real interpretation

`searxng_json` is not a disposable fallback. It is the only provider in the evidence base that has shown **both**:

- useful success at scale
- operator control

That makes it the most strategically valuable provider to preserve.

### Decision

Preserve and fix.

### Required fixes

1. Treat the SearXNG process as an operational dependency, not a casual local service.
2. Add startup/runtime health checks that distinguish:
   - app down
   - app up but engines dead
   - app up but empty results
3. Add a second SearXNG instance or secondary engine set if high availability matters.
4. Add an auto-start or supervisor process if this stays laptop-hosted.

## `bing_html`

### What the history says

- Best provider in the DB snapshot window
- Best provider in the `2026-04-15` throughput proof window
- Completely dead in the `2026-04-09` stress window
- Currently degraded by anti-bot challenge behavior

### Common failure modes

From [search/client.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/search/client.py):

- challenge pages
- anomaly pages
- empty anti-bot pages with no parseable `li.b_algo`
- low-signal results that trigger a fallback even after a nominal success

### Real interpretation

`bing_html` is valuable, but fragile.

It is not stable enough to be the single foundation of the platform, but it has repeatedly shown that it can become the best fallback in certain windows. It should be kept as a tactical provider, not a strategic dependency.

### Decision

Preserve as fallback only.

### Required fixes

1. Keep the low-signal fallback behavior.
2. Keep circuit breakers.
3. Track anti-bot events explicitly in snapshot metrics.
4. Do not rely on Bing HTML as the first or only provider during investor-facing long runs.

## `duckduckgo_html`

### What the history says

- Near-zero success in the DB snapshot window (`1 / 355`)
- Zero success in the `2026-04-15` throughput proof window
- Current live preflight shows challenge/anomaly behavior

### Common failure modes

From [search/client.py](D:/VSC%20Programs/FratFinderAI/services/crawler/src/fratfinder_crawler/search/client.py):

- blocked or anomaly responses (`202`, `403`, `429`)
- no parseable lite/html results
- apparent request errors during some stored windows

### Real interpretation

DuckDuckGo HTML is not totally useless, but there is no evidence in this repository that it is a strong production-grade fallback for this workload. It is better treated as a low-confidence last fallback rather than a serious provider tier.

### Decision

Preserve, but demote.

### Required fixes

1. Keep it at the end of the free fallback chain.
2. Do not interpret current DDG failure as a reason to rewrite the stack.
3. Continue using it only when stronger providers are unavailable.

## `brave_html`

### What the history says

- `0 / 338` in the DB snapshot window
- `0 / 1707` in the `2026-04-09` stress analysis
- `0 / 16` in the `2026-04-15` proof window

### Common failure modes

- request errors
- HTTP errors
- no parseable results
- challenge/anomaly behavior is possible, but the historical evidence here is dominated by hard request failure

### Real interpretation

The repository evidence does not justify continuing to use Brave HTML in the automatic live chain. This aligns with operator intuition that Brave is not working reliably in practice.

### Decision

Remove from automatic live chain.

### Required action

- Keep the code only if you want it as an optional manual experiment.
- Do not leave it in live free-provider defaults.

## `serper_api`

### What the history says

- `0 / 355` in the DB snapshot window
- `5 / 857` in the `2026-04-09` long stress run
- `0 / 16` in the `2026-04-15` proof window

### Common failure modes

From the probe attempts in the run artifacts:

- `HTTPError`
- no parseable results

### Real interpretation

There is not enough positive evidence here to treat Serper as a proven production backup. The tiny number of successes in the long stress run is not enough to justify automatic reliance, especially since later windows show zero value.

This does **not** mean the integration should be deleted. It means it should be isolated and revalidated.

### Decision

Quarantine until isolated validation passes.

### Required action

1. Run a dedicated 50-100 query smoke test against Serper alone.
2. Confirm quota, response parsing, and error semantics.
3. Only then reintroduce it as a live backup.

## `tavily_api`

### What the history says

- `0 / 355` in the DB snapshot window
- `1 / 861` in the `2026-04-09` long stress run
- `0 / 16` in the `2026-04-15` proof window

### Common failure modes

- `HTTPError`
- no parseable results

### Real interpretation

The current evidence does not support Tavily as an effective chapter-search provider for this workload. Like Serper, it may still be salvageable, but the present repo data says it should not be in the automatic live chain until it proves itself in isolated validation.

### Decision

Quarantine until isolated validation passes.

## `brave_api`

### What the history says

- No meaningful production history in the local evidence set
- Current `doctor` output showed no configured API key

### Real interpretation

There is not enough evidence to include Brave API in the preserve-vs-replace decision. It is not a real participant in the current production-like stack.

### Decision

No decision yet. Not enough data.

## Cross-Provider Failure Mode Summary

## 1. Local dependency failure

Mainly affects `searxng_json`.

Symptoms:

- `ConnectionError`
- connection refused
- unresponsive engines

This is not a search-quality problem. It is an operational availability problem.

## 2. Anti-bot / challenge pages

Mainly affects `bing_html` and `duckduckgo_html`.

Symptoms:

- anomaly pages
- challenge pages
- captcha-like flows
- empty pages with no parseable organic results

This is the defining weakness of HTML scraping providers.

## 3. API request failure

Mainly affects `serper_api`, `tavily_api`, and much of `brave_html` history.

Symptoms:

- `HTTPError`
- request failures without usable results

This usually means one or more of:

- quota problems
- auth/config mismatch
- endpoint response shape mismatch
- provider instability

## 4. Low-signal result sets

Most visible in `bing_html`.

Symptoms:

- search technically succeeds
- results are too generic or low-value
- client correctly falls back

This is not the same as provider outage. It is a quality-control behavior that should stay.

## What Should Be Preserved

The following should be preserved:

### Search architecture

- multi-provider chain
- preflight
- circuit breakers
- degraded-mode execution
- provider ranking
- low-signal fallback

### Providers to preserve

- `searxng_json`
- `bing_html`
- `duckduckgo_html` only as last fallback

## What Should Be Changed

### Remove from default live chain

- `brave_html`

### Quarantine until proven

- `serper_api`
- `tavily_api`

### Operational hardening

- SearXNG should not depend on "maybe my laptop service is up."
- Add active checks for:
  - process up/down
  - engine responsiveness
  - empty-result health
- Add one more operator-visible surface for provider failure modes, not just provider status.

## Recommended Target Architecture

The best next-step architecture is:

### Tier 1

`searxng_json`

But hardened:

- supervised process
- health check
- restart path
- optional secondary instance

### Tier 2

One **verified** backup provider.

Based on current evidence, `bing_html` is the only currently justified live backup, but it should remain explicitly fallback-tier because anti-bot behavior can wipe it out suddenly.

### Tier 3

`duckduckgo_html` as last-resort emergency fallback.

### Tier 4

Optional paid backup, but only after isolated proof.

The best preserve-vs-replace answer here is:

- do **not** replace the subsystem
- do **replace** the unreliable parts of the provider mix
- do **add** one better backup solution if reliability needs to rise further

## If You Want A New Solution

If you decide to add one new solution, the strongest recommendation is:

### Add a second SearXNG deployment before adding more HTML providers

Reason:

- It fits the current architecture cleanly.
- It keeps operator control.
- It addresses the most common SearXNG failure mode: local instance unavailability.
- It avoids overfitting the platform to HTML scraping engines that anti-bot systems can kill arbitrarily.

If you want one managed provider in addition to that:

- choose **one**
- run a dedicated smoke cohort
- only then promote it into live fallback order

Current repo evidence does not justify blindly trusting `serper_api` or `tavily_api`.

## Final Decision

### Recommended decision

Preserve and fix the existing system.

### Specifically

- Preserve the architecture.
- Preserve `searxng_json` and harden it.
- Preserve `bing_html` as fallback only.
- Keep `duckduckgo_html` only as a final fallback.
- Remove `brave_html` from the automatic chain.
- Do not rely on `serper_api` or `tavily_api` until isolated tests prove they work.

### Why

Because the evidence says the architecture is sound, the providers are unstable, and the biggest upside comes from **better provider operations and provider selection**, not from rewriting the crawler's search subsystem.

## Follow-Up Work

If we want to turn this report into action, the highest-value next sequence is:

1. Add a machine-readable provider audit artifact per preflight run.
2. Record `challenge_or_anomaly` explicitly in `provider_health`.
3. Add SearXNG process supervision or a second instance.
4. Remove Brave HTML from live defaults.
5. Run a single-provider validation cohort for:
   - `serper_api`
   - `tavily_api`
6. Recompute provider rankings after those smoke tests.
