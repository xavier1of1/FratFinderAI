# Provider Degradation Conclusion Validation

Date: `2026-04-21`

Primary question: is the provider degradation conclusion accurate against the **current codebase**, and are its recommendations sound enough to implement?

## Implementation Status Update

This validation report has now been materially acted on. The codebase includes:

- a canonical provider catalog and automatic-chain normalization
- SearXNG multi-endpoint failover via `CRAWLER_SEARCH_SEARXNG_BASE_URLS`
- provider-attempt persistence through `search_provider_attempts`
- richer `doctor` endpoint diagnostics for SearXNG reachability and provider classes
- a `search-provider-smoke` CLI with stored smoke outputs for `serper_api`, `tavily_api`, and `dataforseo_api`

The remaining open work is operational rather than architectural: SearXNG availability, managed-provider credential/config correctness, and promotion of any backup only after passing smoke gates.

## Verdict

The conclusion is **mostly sound** and is safe to use as the basis for implementation, with three important corrections:

1. The report's telemetry caveat is now **outdated**.
   - It was true when written.
   - It is no longer fully true in the current code because provider-specific health payloads are now persisted.
2. Some recommendations are already partially true in code by default.
   - `brave_html` is already absent from the default automatic provider order.
   - `serper_api` and `tavily_api` are already absent from the default free automatic order.
3. Some recommendations are strategically sound but **not directly implementable yet**.
   - The current code supports only **one** `CRAWLER_SEARCH_SEARXNG_BASE_URL`.
   - A primary/secondary SearXNG failover design would require new config and routing support.

Overall judgment:

- The report is directionally correct on architecture.
- The report is directionally correct on provider ordering strategy.
- The report is directionally correct on the need to harden SearXNG.
- The report is directionally correct on treating this as a search reliability and observability problem.
- The report should be updated before implementation so it reflects the current telemetry fix and the current defaults accurately.

## What Was Validated Against Current Code

Code inspected:

- [search/client.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\search\client.py)
- [config.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\config.py)
- [pipeline.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\pipeline.py)
- [request_graph.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\orchestration\request_graph.py)
- [test_pipeline_workers.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\tests\test_pipeline_workers.py)
- [test_request_graph_runtime.py](D:\VSC Programs\FratFinderAI\services\crawler\src\fratfinder_crawler\tests\test_request_graph_runtime.py)

Live commands run:

- `python -m fratfinder_crawler.cli doctor`
- `python -m fratfinder_crawler.cli search-preflight --probes 4`
- targeted provider tests in the crawler test suite

## Claim-By-Claim Validation

### 1. "This is a search reliability and observability problem, not proof the architecture should be replaced."

Status: `Validated`

Why:

- The code still uses a sound provider-chain architecture.
- `search/client.py` still provides:
  - provider chains
  - in-process fallback
  - circuit breakers
  - request pacing
  - provider ranking
- `pipeline.py` still provides:
  - preflight probes
  - degraded-mode decisions
  - provider-window state
  - blocked-provider queue behavior
- `field_jobs.py` still provides authoritative-path behavior during search degradation.

Conclusion:

- The report is correct that the right fix is not "throw away the search subsystem."

### 2. "Preserve the current provider-chain architecture."

Status: `Validated`

Why:

- The current architecture already separates:
  - provider selection
  - preflight health
  - degraded-mode execution
  - authoritative-only fallback behavior
- Replacing the whole subsystem would discard working control-plane logic that is already integrated with queues and field jobs.

Conclusion:

- This recommendation is sound.

### 3. "Harden SearXNG and treat it as the main controllable provider."

Status: `Validated, with implementation caveat`

Why:

- `searxng_json` is still first in the effective live free-provider order.
- `doctor` on `2026-04-21` showed the system still expects SearXNG first:
  - `searxng_json`
  - `duckduckgo_html`
  - `bing_html`
- `search-preflight --probes 4` on `2026-04-21` showed SearXNG currently fails due to `ConnectionError` because `localhost:8888` is unreachable.
- That is an operational dependency failure, not evidence that the integration path is conceptually wrong.

Implementation caveat:

- The current code supports only one SearXNG base URL:
  - `CRAWLER_SEARCH_SEARXNG_BASE_URL`
- There is no built-in multi-endpoint SearXNG failover today.

Conclusion:

- "Preserve and harden SearXNG" is correct.
- "Use primary + secondary SearXNG" is sound, but it is a new feature, not current behavior.

### 4. "Keep Bing HTML only as opportunistic fallback."

Status: `Validated`

Why:

- `bing_html` remains one of the configured free providers.
- It is still not suitable as the sole foundation because its reliability is highly window-dependent.
- The current code already treats it as a fallback under `auto` and `auto_free`, not as a dedicated system-wide primary.

Conclusion:

- The recommendation is sound.

### 5. "Demote DuckDuckGo HTML."

Status: `Sound recommendation, not yet reflected in the current default order`

Why:

- Current effective provider order from `doctor` on `2026-04-21` is:
  1. `searxng_json`
  2. `duckduckgo_html`
  3. `bing_html`
- That means DuckDuckGo is still ranked ahead of Bing in the current default order.
- Live preflight on `2026-04-21` showed:
  - `duckduckgo_html`: `1/4` successes, but `3/4` challenge/anomaly failures, below threshold
  - `bing_html`: `0/3` successes, `3/3` challenge/anomaly failures
- Current code does not yet justify promoting DuckDuckGo above Bing based on downstream yield.

Conclusion:

- Demoting DuckDuckGo remains a sound recommendation.
- It is **not yet implemented** in the current default config.

### 6. "Remove Brave HTML from automatic live use."

Status: `Sound, and already mostly true by default`

Why:

- `brave_html` is still implemented and can still be selected explicitly.
- But the default free provider order in `config.py` is:
  - `searxng_json,bing_html,duckduckgo_html`
- `brave_html` is **not** in the default automatic free chain.

Conclusion:

- The recommendation is sound.
- For the current default path, this is already mostly true.
- If you want stronger enforcement, remove or reject `brave_html` from user-configurable provider-order chains as well.

### 7. "Quarantine Serper and Tavily until isolated smoke tests pass."

Status: `Sound, and already partly true by default`

Why:

- `serper_api` and `tavily_api` are still supported providers in `search/client.py`.
- They are not in the default free-provider order.
- `doctor` showed both are configured in the local environment, but they are not used automatically by the default free chain.

Conclusion:

- The recommendation is sound.
- In practice, they are already "quarantined from the default free live path," but they are still available for explicit selection or custom ordering.

### 8. "Current telemetry flattens several different failure modes into search degraded."

Status: `Partially outdated`

Why:

- This was materially true at the time of the prior audit.
- It is no longer fully true in the current code.

Current code now records:

- `low_signal`
- `challenge_or_anomaly`
- `failure_types`
- provider-level `healthy`
- provider-level `health_reason`

Evidence:

- `pipeline.py` now preserves provider-specific failure detail during preflight.
- `request_graph.py` now writes provider-specific health payloads into `provider_health_snapshots`.
- targeted tests still pass:
  - preflight/provider-window tests
  - request-graph provider-specific health tests

What is still true:

- There is still a top-level batch `healthy` verdict.
- The system still ultimately degrades search-heavy work into a smaller number of execution decisions.
- There is still no normalized provider-attempt database table with one row per attempt.

Conclusion:

- The report should be corrected here.
- The observability problem still exists at the data-model level, but it is no longer as severe as the report states.

### 9. "Add provider-attempt storage or richer telemetry."

Status: `Validated as a good recommendation`

Why:

- The current code preserves richer provider failure detail in preflight payloads, but not as a first-class attempt table.
- That means historical analytics still require parsing nested payload structures.

Conclusion:

- This is still a strong recommendation.

### 10. "Rank providers by downstream accepted evidence, not just raw search success."

Status: `Validated as strategically correct; not implemented today`

Why:

- Current `SearchClient._rank_providers(...)` ranks providers using:
  - attempts
  - successes
  - circuit-open state
- It does **not** rank by:
  - official page hit rate
  - accepted evidence rate
  - safe contact write rate
  - downstream yield

Conclusion:

- This is one of the strongest recommendations in the report.

## External Claim Validation

Time-sensitive external claims were checked against current public sources on `2026-04-21`.

### Bing Search API retirement

Status: `Validated`

Microsoft Learn says Bing Search APIs were retired on `August 11, 2025`, with customers directed toward Grounding with Bing Search in Azure AI Agents:

- https://learn.microsoft.com/en-us/lifecycle/announcements/bing-search-api-retirement

Conclusion:

- The report is correct that Bing official search APIs are not a good new raw SERP dependency for this architecture.

### Google Custom Search JSON API as a new default

Status: `Mostly validated`

Google for Developers confirms:

- JSON API exists
- 100 free queries/day
- additional queries cost `$5 per 1000`

Source:

- https://developers.google.com/custom-search/v1/overview

What was **not** validated from the current official page during this review:

- the specific "existing customers only until January 1, 2027" wording was not clearly present in the page content returned here

Conclusion:

- The cost/throughput argument against Google CSE as a new default is still strong.
- The "existing customers only until 2027" part should be treated cautiously unless re-verified directly from a current official page or account notice.

### SearXNG JSON format and anti-bot/limiter behavior

Status: `Validated`

SearXNG docs confirm:

- `/search?...&format=json` requires enabled formats
- unset formats can return `403 Forbidden`
- limiter behavior exists because upstream search engines can CAPTCHA/block SearXNG traffic

Sources:

- https://docs.searxng.org/dev/search_api.html
- https://docs.searxng.org/admin/searx.limiter
- https://docs.searxng.org/admin/settings/index.html

Conclusion:

- The report's SearXNG operational recommendations are sound.

### Brave Search API pricing

Status: `Partially validated with correction`

Current Brave pricing pages show:

- free tier exists
- base pricing can be lower than `$5 / 1000`
- pro tier can be `$5 / 1000`
- rate-limiting docs confirm a 1-second sliding window and 429 behavior

Sources:

- https://api-dashboard.search.brave.com/documentation/pricing
- https://api-dashboard.search.brave.com/documentation/guides/rate-limiting

Conclusion:

- The report is directionally correct that Brave API is an official API worth considering.
- The specific pricing summary should be updated because Brave currently exposes multiple tiers, not just a flat `$5 / 1000`.

### Serper pricing and positioning

Status: `Validated`

Serper's public pricing page currently advertises:

- 2,500 free queries
- `$50` starter tier for `50,000` credits
- `50` QPS on that starter tier

Source:

- https://serper.dev/

Conclusion:

- The report's Serper positioning as a plausible managed backup candidate is sound.

### DataForSEO pricing

Status: `Validated`

DataForSEO's pricing page currently states:

- pay-as-you-go
- example of 1,000 SERPs for `$0.60`
- minimum deposit `$50`

Source:

- https://dataforseo.com/apis/serp-api/pricing

Conclusion:

- The report's DataForSEO cost framing is sound.

### Tavily pricing and rate limits

Status: `Validated`

Tavily docs currently show:

- 1,000 free API credits per month
- pay-as-you-go pricing
- documented RPM limits for development and production

Sources:

- https://docs.tavily.com/guides/api-credits
- https://docs.tavily.com/documentation/rate-limits

Conclusion:

- The report's Tavily cost/rate-limit summary is sound.
- The judgment that Tavily is not yet proven as the right raw source-discovery backup is still a strategic recommendation, not a code fact.

### SerpApi legal risk

Status: `Validated, but better sourced differently`

There is current public evidence that Google filed suit against SerpApi in late 2025.

A stronger source than Reuters now exists:

- Google's own public statement:
  - https://blog.google/innovation-and-ai/technology/safety-security/serpapi-lawsuit/

Conclusion:

- The report is reasonable to flag legal/contract risk around SerpApi.
- If the report is revised, it should cite Google's complaint/blog rather than relying on Reuters alone.

## Current Live Snapshot (`2026-04-21`)

### Doctor

Observed effective free-provider order:

1. `searxng_json`
2. `duckduckgo_html`
3. `bing_html`

Observed reachability:

- `searxng_json`: configured, unreachable
- `duckduckgo_html`: fallback-chain public provider
- `bing_html`: fallback-chain public provider
- `serper_api`: API key configured
- `tavily_api`: API key configured
- `brave_api`: not configured

### Preflight

Observed current outcome:

- overall `healthy = false`
- `success_rate = 0.25`
- threshold `= 0.34`
- no viable providers by current threshold logic

Provider details:

- `searxng_json`
  - `4/4` request errors
  - health reason `request_error_only`
- `duckduckgo_html`
  - `1/4` successes
  - `3/4` challenge/anomaly unavailable outcomes
  - health reason `below_success_threshold`
- `bing_html`
  - `0/3` successes
  - `3/3` challenge/anomaly unavailable outcomes
  - health reason `challenge_or_anomaly`

Conclusion:

- Today's runtime still supports the report's operational diagnosis:
  - SearXNG is the main local dependency and is currently down.
  - HTML providers remain unstable and anti-bot-sensitive.

## What Should Be Corrected Before Implementation

### Correction 1

Replace:

- "provider health snapshots are not provider-specific"

With:

- "provider-specific health is now persisted in payloads, but the system still lacks a normalized provider-attempt event model"

### Correction 2

Replace:

- "remove Brave HTML from the automatic live chain"

With:

- "Brave HTML is already absent from the default automatic free chain; decide whether to fully deprecate explicit selection as well"

### Correction 3

Replace:

- "quarantine Serper and Tavily"

With:

- "Serper and Tavily are already outside the default free live chain, but should remain out of promoted/default paths until isolated smoke cohorts prove usefulness"

### Correction 4

Add explicit implementation caveat:

- "SearXNG primary/secondary failover is not available in the current code and requires new config and client logic"

### Correction 5

Update Brave API pricing language:

- current Brave pricing is tiered, not just one flat `$5 / 1000`

## Final Recommendation

Proceed using the report as a planning input, but only after these corrections are applied.

Best implementation-ready summary:

- Preserve the provider-chain architecture.
- Keep SearXNG as the strategic controllable provider.
- Harden SearXNG operations first.
- Move toward Bing-over-DDG fallback ordering unless new cohort data disproves it.
- Keep `brave_html` out of default automatic execution.
- Keep `serper_api` and `tavily_api` out of promoted/default execution until smoke-tested.
- Add one managed backup only after isolated cohort proof.
- Improve ranking based on downstream accepted evidence.
- Treat provider observability as improved, but not finished.

## Validation Evidence

Targeted tests run during this validation:

- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py -k "search_preflight or provider_window_logic"`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_request_graph_runtime.py -k "provider_specific_health"`

Results:

- `3 passed`
- `1 passed`

These tests confirm that:

- provider-specific preflight behavior is implemented
- provider-specific persisted health payloads are implemented
- provider-window logic is still active
