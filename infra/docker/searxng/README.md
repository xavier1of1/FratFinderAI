# FratFinderAI SearXNG Operations

FratFinderAI treats SearXNG as a local/self-hosted search dependency, not a generic public instance. The Docker Compose `search` profile starts two isolated SearXNG endpoints:

- `fratfinder-searxng-primary` on `localhost:8888`
- `fratfinder-searxng-rescue` on `localhost:8889`

Start them with:

```powershell
docker compose -f infra/docker/docker-compose.yml --profile search up -d searxng-primary searxng-rescue
```

Recommended crawler config:

```dotenv
CRAWLER_SEARCH_PROVIDER_ORDER_FREE=searxng_json,bing_html,duckduckgo_html
CRAWLER_SEARCH_SEARXNG_BASE_URLS=http://localhost:8888,http://localhost:8889
CRAWLER_SEARCH_SEARXNG_MAX_IN_FLIGHT=1
CRAWLER_SEARCH_SEARXNG_MIN_INTERVAL_MS=750
CRAWLER_SEARCH_SEARXNG_BACKOFF_SECONDS=90
CRAWLER_SEARCH_RESULT_CACHE_TTL_SECONDS=1800
```

Use raw diagnostics before blaming crawler code:

```powershell
python -m fratfinder_crawler.cli searxng-health --include-docker-logs
python -m fratfinder_crawler.cli searxng-engine-smoke --engines startpage,mojeek,bing,google,duckduckgo,brave --max-queries 4 --delay-ms 500
```

If the endpoint is reachable but reports `engine_unresponsive`, the problem is upstream engine health or SearXNG engine mix. If the endpoint reports `endpoint_down`, `connection_refused`, or `json_disabled`, fix the SearXNG deployment/config before running field jobs.

When engine smoke identifies a stable local allowlist, prefer setting `CRAWLER_SEARCH_SEARXNG_STABLE_ENGINES` and `CRAWLER_SEARCH_SEARXNG_ENGINE_PROFILE=stable` instead of letting the default SearXNG engine set repeatedly call suspended engines.
