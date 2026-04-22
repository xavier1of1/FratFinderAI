# Provider Iteration Update 2026-04-21

## What Changed

This iteration addressed the concrete issues observed during the provider stress benchmark:

1. Restarted the local `searxng` container, which had been down for 11 days.
2. Fixed `doctor` so its SearXNG probe timeout matches live preflight behavior more closely.
3. Removed duplicate SearXNG attempt recording from provider telemetry.
4. Improved managed-provider HTTP error detail so response bodies are surfaced in smoke output.
5. Reclassified managed-provider plan/credit failures as `quota_exceeded` instead of generic `request_error_only`.
6. Updated local config/docs examples to use the canonical automatic free order:
   - `searxng_json,bing_html,duckduckgo_html`

## Measured Improvement

### Before iteration

From the benchmark:

- `doctor` reported SearXNG unreachable
- repeated preflight health: `0 / 5` healthy cycles
- preflight success rate: `0.0`
- provider-window success rate: `0.0`
- `searxng_json` smoke success rate: `0.0`

### After iteration

Immediate rerun results:

- `doctor` now reports SearXNG reachable with:
  - `reachable = true`
  - `jsonParseable = true`
  - `resultBearing = true`
  - `healthReason = healthy`
- `search-preflight --probes 4` recovered to:
  - `healthy = true`
  - `success_rate = 1.0` on the first rerun
  - `provider_window_success_rate = 1.0` on the first rerun
- follow-up `system-baseline --probes 4` stayed healthy overall:
  - `healthy = true`
  - `success_rate = 0.75`
  - `provider_window_success_rate = 0.5`
  - viable provider: `searxng_json`
- `search-provider-smoke --provider searxng_json --max-queries 5` now passes promotion gates:
  - `raw_success_rate = 1.0`
  - `accepted_evidence_rate = 1.0`
  - `official_school_result_rate = 1.0`
  - `national_directory_result_rate = 0.4`
  - `median_latency_ms = 843.0`
  - `p95_latency_ms = 3531.0`
  - `promotionGates.passed = true`

## Managed Provider Diagnosis

The improved error handling turned two vague failures into actionable findings:

- `serper_api` now clearly reports:
  - `Not enough credits`
  - classified as `quota_exceeded`
- `tavily_api` now clearly reports:
  - `This request exceeds your plan's set usage limit`
  - classified as `quota_exceeded`

This means both managed providers are currently blocked by account state, not by payload-shape ambiguity.

## Log Observations

Observed from `docker logs searxng` after restart:

- SearXNG itself is running and serving results again.
- Some upstream engines inside SearXNG remain noisy:
  - DuckDuckGo engine timeouts
  - Brave `403` / too-many-requests / access-denied behavior
- Despite those upstream engine issues, SearXNG still returned useful results and recovered the benchmark.

Interpretation:

- the highest-value fix was restoring the SearXNG service itself
- the next SearXNG improvement should be engine tuning, not replacing the integration

## Remaining Bottlenecks

Provider recovery improved search health, but it did not fix everything:

- field-job worker liveness is still open
  - actionable jobs remain while active workers are `0`
- queue backlog remains large in:
  - `blocked_provider`
  - `blocked_dependency`
  - `blocked_repairable`
- HTML fallbacks are still weak and challenge-prone

## Net Result

This iteration materially improved the project.

The system moved from:

- no viable provider path

to:

- one healthy, benchmark-passing provider path through `searxng_json`

and it did so with better telemetry quality than before.
