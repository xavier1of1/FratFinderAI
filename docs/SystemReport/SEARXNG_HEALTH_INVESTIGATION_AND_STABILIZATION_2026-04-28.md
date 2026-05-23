# SearXNG Health Investigation And Stabilization

## Summary

SearXNG is not currently failing as a local container process. The container is reachable, JSON is enabled, and the API responds with HTTP 200. The dominant failure is upstream engine distress inside SearXNG:

- `brave`: suspended for too many requests
- `duckduckgo`: timeout / suspended timeout
- `karmasearch`: access denied
- `startpage`: CAPTCHA suspended
- `aol`: HTTP protocol errors / upstream 500s

This means restarting the container can temporarily clear symptoms, but it does not fix the underlying issue. The correct fix is to pace SearXNG, avoid repeatedly hitting suspended engines, cache repeated result-bearing queries, and surface engine-level diagnostics clearly.

## Raw Evidence

Docker state during investigation:

- container: `searxng`
- image: `searxng/searxng:latest`
- status: running for about an hour
- port: `localhost:8888 -> 8080`
- CPU/memory snapshot: CPU varied around 25%, memory about 422 MiB of 13.58 GiB

Raw SearXNG API probes:

- `GET /search?q=delta+chi+chapter+directory&format=json`
- HTTP status: `200`
- JSON parseable: yes
- result-bearing status varied by window
- unresponsive engines repeatedly included `brave`, `duckduckgo`, `karmasearch`, `startpage`, and `aol`

`searxng-engine-smoke` result:

- `bing` had the highest raw result-bearing rate in the sample, but returned obvious low-signal results for several fraternity queries. The final smoke scorer marked its `lowSignalTopUrlRate` as `1.0`, so it was not promoted.
- `startpage` was fully CAPTCHA-suspended during the smoke.
- `duckduckgo`, `brave`, `qwant`, `aol`, and `karmasearch` were not viable in the smoke window.
- No engine allowlist was automatically promoted because raw result-bearing did not prove downstream safety.

`search-preflight --probes 4` after stabilization code:

- search health: unhealthy
- `searxng_json`: first attempt hit `engine_unresponsive`, then endpoint-level cooldown prevented repeated SearXNG hammering
- `bing_html`: challenge/anomaly
- `duckduckgo_html`: timeout
- authoritative fetch lane remained healthy

## Implemented Stabilization Features

### Raw diagnostics

Added:

- `searxng-health`
- `searxng-engine-smoke`

These commands inspect SearXNG directly instead of relying only on FratFinderAI search logs.

### Endpoint classification

SearXNG endpoint health now distinguishes:

- `endpoint_down`
- `connection_refused`
- `dns_error`
- `json_disabled`
- `engine_unresponsive`
- `empty_results`
- `healthy`
- `healthy_degraded`

### Admission control

Added SearXNG-specific crawler controls:

- `CRAWLER_SEARCH_SEARXNG_MAX_IN_FLIGHT`
- `CRAWLER_SEARCH_SEARXNG_MIN_INTERVAL_MS`
- `CRAWLER_SEARCH_SEARXNG_BACKOFF_SECONDS`
- `CRAWLER_SEARCH_SEARXNG_ADMISSION_WAIT_SECONDS`

Defaults keep one in-flight SearXNG request per worker process and add request spacing/backoff so suspended upstream engines are not hammered repeatedly.

### Search caching

Added in-process positive-result caching:

- `CRAWLER_SEARCH_RESULT_CACHE_TTL_SECONDS`

This reduces duplicate SearXNG traffic for repeated query windows.

### Preflight caching

Added in-process preflight cache support:

- `CRAWLER_SEARCH_PREFLIGHT_CACHE_TTL_SECONDS`

This keeps worker loops from repeatedly probing an already-known degraded search window.

### Deployment support

Added Docker Compose `search` profile services:

- `searxng-primary`
- `searxng-rescue`

The rescue endpoint is intentionally opt-in through `CRAWLER_SEARCH_SEARXNG_BASE_URLS`; the local `.env` does not assume a second endpoint is running.

## Current Recommendation

Do not pin a SearXNG engine allowlist yet. The current smoke sample did not identify a clearly safe, high-quality engine set.

Recommended next step:

1. Keep SearXNG first in FratFinderAI provider order.
2. Keep SearXNG paced and backoff-protected.
3. Run `searxng-engine-smoke` across multiple windows.
4. Promote a stable engine allowlist only after it proves both result-bearing reliability and downstream relevance.

Candidate command:

```powershell
python -m fratfinder_crawler.cli searxng-engine-smoke --engines startpage,mojeek,bing,google,duckduckgo,brave,qwant,aol,karmasearch --max-queries 4 --delay-ms 500
```

If engine health remains poor, the managed-provider smoke path should be used before increasing SearXNG load.
