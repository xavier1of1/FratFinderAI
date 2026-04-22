# Provider Stress Benchmark 2026-04-21

## Scope

This benchmark measures the **live provider subsystem** after the provider reliability hardening pass. The goal was to quantify:

- provider reliability under repeated preflight pressure
- per-provider smoke-cohort performance
- failure-mode distribution
- downstream usefulness
- queue and worker context that determines whether provider health can convert into actual throughput

This was intentionally scoped at the provider/search layer rather than re-enqueuing a full field-job stress cohort, so we could evaluate the new provider controls without introducing a fresh queue-wide perturbation.

## Method

The benchmark used four measurement surfaces:

1. `doctor`
2. `system-baseline --probes 4`
3. `5` repeated preflight cycles with `4` probes each
4. `12`-query smoke cohorts for:
   - `searxng_json`
   - `bing_html`
   - `duckduckgo_html`
   - `serper_api`
   - `tavily_api`
   - `dataforseo_api`

Artifacts written:

- [PROVIDER_BENCHMARK_RAW_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_BENCHMARK_RAW_2026-04-21.json)
- [PROVIDER_BENCHMARK_BASELINE_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_BENCHMARK_BASELINE_2026-04-21.json)
- [PROVIDER_SMOKE_searxng_json_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_SMOKE_searxng_json_2026-04-21.json)
- [PROVIDER_SMOKE_bing_html_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_SMOKE_bing_html_2026-04-21.json)
- [PROVIDER_SMOKE_duckduckgo_html_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_SMOKE_duckduckgo_html_2026-04-21.json)
- [PROVIDER_SMOKE_serper_api_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_SMOKE_serper_api_2026-04-21.json)
- [PROVIDER_SMOKE_tavily_api_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_SMOKE_tavily_api_2026-04-21.json)
- [PROVIDER_SMOKE_dataforseo_api_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_SMOKE_dataforseo_api_2026-04-21.json)

## Executive Summary

The provider hardening implementation is working technically, but the live provider environment is still operationally unhealthy.

Measured results:

- `0 / 5` repeated preflight cycles were healthy
- preflight success rate was `0.0` in every cycle
- provider-window success rate was `0.0` in every cycle
- `0 / 6` smoke-tested providers passed promotion gates
- `0 / 6` providers produced accepted evidence in the benchmark cohort
- `0` providers produced a single successful search result in the benchmark cohort

The dominant causes were:

- `searxng_json`: endpoint down / connection refused, then circuit open
- `bing_html`: challenge/anomaly, then circuit open
- `duckduckgo_html`: challenge/anomaly, then circuit open
- `serper_api`: repeatable request errors (`400`), then circuit open
- `tavily_api`: repeatable request errors (`432`), then circuit open
- `dataforseo_api`: missing credentials, then circuit open

This means the current state is **architecturally improved but operationally blocked**. The code now measures provider health correctly, persists attempt history, and classifies failures far more precisely, but there is still no viable live provider path in this environment.

## Queue And Worker Baseline

Baseline snapshot from [PROVIDER_BENCHMARK_BASELINE_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_BENCHMARK_BASELINE_2026-04-21.json):

| KPI | Value |
| --- | --- |
| `queued_jobs` | `6,817` |
| `actionable_jobs` | `594` |
| `blocked_provider_jobs` | `2,633` |
| `blocked_dependency_jobs` | `1,947` |
| `blocked_repairable_jobs` | `1,643` |
| `running_jobs` | `0` |
| `provider_degraded_ratio` | `0.3862` |
| `dependency_blocked_ratio` | `0.2856` |
| `repair_backlog_ratio` | `0.2410` |
| `worker_liveness_ratio` | `0.0` |
| `worker_liveness_alert.open` | `true` |

Interpretation:

- provider health is a major bottleneck, but not the only one
- even perfect provider recovery would still leave a significant dependency and repair backlog
- worker liveness is currently a separate operational problem because `594` actionable jobs exist while `0` field-job workers are active

## Repeated Preflight Stability

Repeated preflight results from [PROVIDER_BENCHMARK_RAW_2026-04-21.json](D:\VSC Programs\FratFinderAI\docs\SystemReport\PROVIDER_BENCHMARK_RAW_2026-04-21.json):

| Metric | Value |
| --- | --- |
| cycles | `5` |
| healthy cycles | `0` |
| mean success rate | `0.0` |
| mean provider-window success rate | `0.0` |
| min success rate | `0.0` |
| max success rate | `0.0` |

Provider-level preflight failure totals across the `5` cycles:

| Provider | Attempts | Successes | Dominant failure modes |
| --- | --- | --- | --- |
| `searxng_json` | `30` | `0` | `connection_refused=10`, `SearchUnavailableError=10`, `circuit_open=10` |
| `bing_html` | `20` | `0` | `challenge_or_anomaly=20` |
| `duckduckgo_html` | `20` | `0` | `challenge_or_anomaly=20` |

Interpretation:

- `searxng_json` is failing as an infrastructure dependency, not as a low-signal provider
- the two HTML fallbacks are not timing out or crashing first; they are being actively challenged or served anomalous pages
- there was no evidence of intermittent recovery in the preflight window

## Smoke Cohort Results

All smoke-cohort metrics are from the benchmark artifacts linked above. Each provider was tested on `12` live queries spanning:

- national-source discovery
- official school status discovery
- chapter website discovery
- Instagram discovery
- email/supporting-page recovery
- low-signal/negative traps

| Provider | Raw success | Request error | Unavailable | Challenge/anomaly | Median latency ms | Accepted evidence | Official-school result | National-directory result | Cost / 1000 | Promotion passed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `searxng_json` | `0.0000` | `0.1429` | `0.8571` | `0.0000` | `0.0` | `0.0000` | `0.0000` | `0.0000` | `0.0` | `false` |
| `bing_html` | `0.0000` | `0.0526` | `0.9474` | `0.2105` | `0.0` | `0.0000` | `0.0000` | `0.0000` | `0.0` | `false` |
| `duckduckgo_html` | `0.0000` | `0.0526` | `0.9474` | `0.2105` | `0.0` | `0.0000` | `0.0000` | `0.0000` | `0.0` | `false` |
| `serper_api` | `0.0000` | `0.3333` | `0.6667` | `0.0000` | `0.0` | `0.0000` | `0.0000` | `0.0000` | `1.0` | `false` |
| `tavily_api` | `0.0000` | `0.3333` | `0.6667` | `0.0000` | `0.0` | `0.0000` | `0.0000` | `0.0000` | `8.0` | `false` |
| `dataforseo_api` | `0.0000` | `0.0000` | `1.0000` | `0.0000` | `0.0` | `0.0000` | `0.0000` | `0.0000` | `0.6` | `false` |

Interpretation:

- no provider returned a useful result in this benchmark window
- no provider generated accepted evidence
- the managed candidates are currently worse operationally than the HTML fallbacks because they are failing at request/auth/config level before usefulness can even be evaluated

## Search Attempt Telemetry

DB attempt history from the last 24 hours, based on `search_provider_attempts`:

| Provider | Attempts | Successes | Request errors | Unavailable | Avg latency ms | Max latency ms |
| --- | --- | --- | --- | --- | --- | --- |
| `searxng_json` | `82` | `0` | `20` | `62` | `2071.27` | `4282` |
| `bing_html` | `50` | `0` | `0` | `50` | `262.58` | `860` |
| `duckduckgo_html` | `50` | `0` | `0` | `50` | `262.42` | `1000` |
| `serper_api` | `14` | `0` | `6` | `8` | `99.29` | `531` |
| `dataforseo_api` | `12` | `0` | `0` | `12` | `0.00` | `0` |
| `tavily_api` | `12` | `0` | `4` | `8` | `68.92` | `594` |

Failure-type breakdown:

| Provider | Failure type | Count |
| --- | --- | --- |
| `searxng_json` | `circuit_open` | `42` |
| `searxng_json` | `connection_refused` | `20` |
| `searxng_json` | `SearchUnavailableError` | `20` |
| `bing_html` | `challenge_or_anomaly` | `34` |
| `bing_html` | `circuit_open` | `16` |
| `duckduckgo_html` | `challenge_or_anomaly` | `34` |
| `duckduckgo_html` | `circuit_open` | `16` |
| `serper_api` | `request_error_only` | `6` |
| `serper_api` | `circuit_open` | `8` |
| `tavily_api` | `request_error_only` | `4` |
| `tavily_api` | `circuit_open` | `8` |
| `dataforseo_api` | `SearchUnavailableError` | `4` |
| `dataforseo_api` | `circuit_open` | `8` |

This is the cleanest evidence that the provider hardening work is paying off diagnostically. The system can now tell us:

- `searxng_json` is down
- `bing_html` and `duckduckgo_html` are being challenged
- `serper_api` and `tavily_api` are erroring at request level
- `dataforseo_api` is unavailable due to configuration state

Before the telemetry fix, these would have been flattened into a much less useful "search degraded" signal.

## Reliability Conclusions

### 1. Provider reliability is currently `0%` for production usefulness

By the benchmark's strict business metric, every provider currently has:

- `0` successful cohorts
- `0` accepted evidence
- `0` official-school hits
- `0` national-directory hits

That means the **observed provider usefulness rate is 0%** in this benchmark window.

### 2. The primary blocker is still SearXNG availability

`searxng_json` remains the strategic primary provider, but it is hard-down in this environment because `http://localhost:8888` is refusing connections.

This is the biggest single operational blocker because:

- it is the intended primary provider
- all free-chain fallback behavior is downstream of its failure
- its failure also increases fallback churn and circuit-open behavior

### 3. HTML fallbacks are available in code but not viable in practice

`bing_html` and `duckduckgo_html` are fast enough to respond, but they are not viable for the current workload because they are returning anomaly/challenge behavior rather than useful results.

### 4. Managed backups are not yet candidates

Current benchmark outcome:

- `serper_api`: integrated but currently failing with repeatable `400` request errors
- `tavily_api`: integrated but currently failing with repeatable `432` request errors
- `dataforseo_api`: integrated, but not testable until credentials are supplied

So the benchmark does **not** justify promoting any managed backup yet.

## Performance Conclusions

### Strong points

- provider attempt capture works
- endpoint-specific telemetry works
- smoke harness works
- preflight stability testing works
- failure classification is now operationally meaningful

### Weak points

- search throughput is effectively zero because no provider path produced useful results
- queue throughput is constrained by both provider blockage and worker liveness
- the free-chain order can be canonical in code, but it does not matter until one provider is actually healthy

## Success Evaluation

Against the intended success standard for this phase:

| Criterion | Result |
| --- | --- |
| provider ordering is canonical in code | `pass` |
| SearXNG endpoint telemetry is explicit | `pass` |
| provider attempt history is persisted | `pass` |
| managed-provider smoke harness exists | `pass` |
| a viable live provider path exists | `fail` |
| any provider passed promotion gates | `fail` |
| provider layer produced accepted evidence in benchmark | `fail` |
| live queue is positioned to exploit provider recovery immediately | `fail` because worker liveness is also open |

## Recommended Next Steps

1. Restore SearXNG service health first.
2. Add or configure a secondary SearXNG endpoint before further provider benchmarking.
3. Fix `serper_api` request-shape/auth behavior before evaluating it again.
4. Fix `tavily_api` request-shape/auth behavior before evaluating it again.
5. Supply `dataforseo_api` credentials if it is the chosen next managed candidate.
6. Re-run this exact benchmark after SearXNG is healthy.
7. Fix field-job worker liveness in parallel, because provider recovery alone will not drain the `594` actionable jobs while active workers remain `0`.

## Bottom Line

The implementation pass succeeded technically and failed operationally.

That is still valuable progress.

We now have a benchmarkable provider subsystem with trustworthy telemetry, repeatable smoke cohorts, endpoint-aware diagnostics, and persisted attempt history. What the benchmark proved is not that the hardening failed, but that the current environment has **no live provider path capable of producing useful search output right now**.
