# Queue Bottleneck Deep Dive - 2026-04-28

Investigation target:

1. Reduce `identity_semantically_incomplete`.
2. Create or refresh official school evidence for `school_evidence_missing`.
3. Unblock `status_dependency_unmet` jobs after active status decisions.
4. Tighten SearXNG engine config so it avoids suspended or CAPTCHA-prone engines.
5. Improve observability with a worker phase metric: `polling`, `preflight`, `triage`, `claiming`, `executing`, `sleeping`.

## Executive Findings

The queue is not dead. The field-job worker is alive, and recent logs show repeated batches processing 97-120 jobs per batch. The misleading number is `running_jobs = 0`, which is a moment-in-time row-status count. Recent `verify_school_match` jobs average about `0.0801s` in `running`, with p95 about `0.1002s`, so ordinary dashboard polling will almost always miss them.

The real bottlenecks are:

| Rank | Bottleneck | Current evidence | Main interpretation |
|---:|---|---|---|
| 1 | `identity_semantically_incomplete` | 1,600+ repair-blocked rows across field jobs | Extraction/normalization is producing ambiguous school or chapter identities. |
| 2 | `status_dependency_unmet` | Mostly `unknown`, `review`, or no status decision, not active decisions stuck blocked | The unlocker is working for active statuses; the missing piece is status evidence creation/resolution. |
| 3 | `school_evidence_missing` | 480 verify-school jobs blocked by absent decisive official evidence | Verify-school is now correctly cheap, but the evidence-refresh path must populate official activity/policy caches. |
| 4 | SearXNG engine degradation | SearXNG is reachable but often degraded; fallback HTML providers are noisy or blocked | SearXNG needs an engine allowlist/rescue endpoint, not more broad fallback attempts. |
| 5 | Worker observability | Worker lease is active and batches finish, but no explicit phase is persisted | Operators need phase visibility beyond `running_jobs`. |

## Bottleneck 1: `identity_semantically_incomplete`

### Quantified Shape

Current grouped counts observed during this investigation:

| Field | Validity class | Repair state | Count |
|---|---|---|---:|
| `verify_school_match` | `canonical_valid` | none | 651 |
| `find_email` | `repairable_candidate` | `repair_exhausted` | 320 |
| `find_website` | `repairable_candidate` | `repair_exhausted` | 273 |
| `find_instagram` | `repairable_candidate` | `repair_exhausted` | 250 |
| `find_email` | `canonical_valid` | `repair_exhausted` | 81 |
| `find_website` | `canonical_valid` | `repair_exhausted` | 64 |
| `verify_website` | `repairable_candidate` | `repair_exhausted` | 59 |
| `find_instagram` | `canonical_valid` | `repair_exhausted` | 37 |

### Relevant Logic

The main guard is `school_identity_requires_repair_before_match(...)` in `services/crawler/src/fratfinder_crawler/field_job_support.py`.

It routes to repair when:

| Trigger | Effect |
|---|---|
| Empty school slug | Repair required |
| Generic school name like `At The University`, `College`, `State University` | Repair required |
| `field_states.university_name` is `missing`, `invalid_entity`, `confirmed_absent`, or `inactive` | Repair required |
| `field_states.university_name = low_confidence` after another claim attempt and no matching `candidateSchoolName` | Repair required |

This is the right safety posture, but the backlog shows upstream extraction and repair are not producing enough resolved identities.

### Hard Examples

| Example | Fraternity | Field | Observed identity | Why it is hard |
|---|---|---|---|---|
| Illinois State Chapter / Illinois State | Delta Chi | `find_website`, `find_email` | School is `Illinois State`, not full `Illinois State University`; no `candidateSchoolName` | School may be a short alias, but the system lacks a confident identity binding. |
| Iota Mu / Kennesaw State | Delta Sigma Phi | `find_website`, `find_instagram`, `find_email` | School is `Kennesaw State`; field states are low confidence | School likely means Kennesaw State University, but current state is not trusted enough for contact work. |
| Kappa Iota / U.S. Military Academy | Theta Chi | `find_website`, `find_email` | School and name are low confidence; Instagram exists | Social evidence exists, but school identity confidence still blocks safe contact enrichment. |
| Epsilon Omega / Illinois State | Delta Sigma Phi | `find_website`, `find_email`, `find_instagram` | School is short alias and core fields low confidence | Needs school-name normalization before status/contact work. |
| Delta Omega / Cleveland State | Delta Sigma Phi | `find_website`, `find_instagram` | School is short alias and core fields low confidence | Similar alias problem; likely Cleveland State University. |
| Georgia Southern University / Georgia Southern University | Beta Upsilon Chi | `find_website` | Chapter name equals school name | Parser promoted a school label as the chapter identity. |
| East Tennessee State University / East Tennessee State University | Beta Upsilon Chi | `find_website` | Chapter name equals school name | Same school-as-chapter extraction failure. |
| Texas Christian University / Texas Christian University | Beta Upsilon Chi | `find_website` | Chapter name equals school name | Same school-as-chapter extraction failure. |
| Johns Hopkins / Johns Hopkins University | Alpha Delta Phi | `find_email` | Short chapter/school alias mismatch | Repair exhausted despite likely resolvable school identity. |
| Alpha Psi (active) / University of Wisconsin-River Falls | Alpha Gamma Rho | `verify_school_match` | Status text leaked into chapter name | Needs chapter designation cleanup before verification. |

### Diagnosis

This is not one bug. It is a boundary problem between extraction, identity binding, and repair exhaustion.

The system is seeing enough text to create rows, but too often the row identity is only partially normalized. Once that reaches field jobs, the safety checks correctly block contact/status execution. The highest-value fix is not to relax the guard. It is to improve school-name normalization, chapter-designation cleanup, and school-roster rescue before field jobs are created.

## Bottleneck 2: `school_evidence_missing`

### Quantified Shape

Current `verify_school_match` cache-miss backlog includes about 480 jobs. The largest fraternity groups:

| Fraternity | Count | Attempted | Has latest status row | Has official activity cache | Has school policy row |
|---|---:|---:|---:|---:|---:|
| Lambda Chi Alpha | 100 | 79 | 100 | 0 | 100 |
| Sigma Pi | 73 | 0 | 73 | 0 | 73 |
| Delta Kappa Epsilon | 72 | 47 | 70 | 0 | 59 |
| Sigma Nu | 40 | 0 | 40 | 0 | 40 |
| Phi Gamma Delta | 28 | 19 | 28 | 0 | 28 |
| Pi Kappa Alpha | 27 | 0 | 27 | 0 | 27 |
| Sigma Alpha Epsilon | 24 | 0 | 24 | 0 | 24 |
| Sigma Chi | 23 | 0 | 23 | 0 | 23 |

Important nuance: many rows do have a status/policy row, but the latest status is `unknown`, school recognition is `unknown`, or policy is not decisive. The new resolver correctly refuses to treat generic `allowed` or `unknown` school policy as chapter activity verification.

### Relevant Logic

The resolver is in `services/crawler/src/fratfinder_crawler/school_verification.py`.

Claimable evidence is:

| Evidence | Outcome |
|---|---|
| Exact `candidateSchoolName` equals stored school | Verified |
| `candidateSchoolName` conflicts | Review |
| Latest official chapter status decision is active | Verified |
| Latest official chapter status decision is inactive | Inactive |
| Official activity cache is `confirmed_active` | Verified |
| Official activity cache is `confirmed_inactive` | Inactive |
| Official school policy is `banned` | Inactive |
| Official school policy is `allowed` only | `school_identity_only`, not activity verification |
| None of the above | `school_evidence_missing` |

### Hard Examples

| Example | Fraternity | Stored school | Cached status | Activity cache | Why blocked |
|---|---|---|---|---|---|
| Delta Nu | Alpha Tau Omega | `Chapter Bylaws and the provisions of North Dakota State University` | `unknown` | none | Stored school text is contaminated by policy prose. |
| Lambda-Epsilon A | Lambda Chi Alpha | Kettering University | `unknown` | none | No official activity evidence for chapter-school pair. |
| Delta Chi | Delta Kappa Epsilon | Auburn University | `unknown` | none | Chapter/fraternity identity looks suspicious; no decisive official evidence. |
| Beta Mu | Delta Sigma Phi | Transylvania University | `unknown` | none | No chapter activity cache. |
| Lambda Eta | Alpha Tau Omega | University of Texas at Tyler | `unknown` | none | No official activity cache. |
| Zeta-Iota | Lambda Chi Alpha | University of Kansas | `unknown` | none | School policy exists but does not prove target chapter activity. |
| Zeta-Tau | Lambda Chi Alpha | Stetson University | `unknown` | none | Same allowed/unknown policy issue. |
| Beta Delta | Alpha Tau Omega | University of Alabama | `unknown` | none | Strong school name, missing official chapter activity evidence. |
| Lambda Delta | Delta Kappa Epsilon | Southern Methodist University | `unknown` | none | Missing target chapter evidence. |
| Gamma-Gamma | Lambda Chi Alpha | University of Cincinnati | `unknown` | none | Missing decisive official chapter status. |

### Diagnosis

The new verify-school resolver is doing what we wanted: it blocks instead of searching. The bottleneck has moved to the evidence-refresh/unlocker path.

The next system improvement should be a bounded official-evidence refresh for the schools/fraternities with the largest `school_evidence_missing` counts. It should populate either `chapter_status_decisions` or `fraternity_school_activity_cache`, then queue reconciliation can reactivate the jobs.

## Bottleneck 3: `status_dependency_unmet`

### Quantified Shape

The original hypothesis was "jobs may be stuck even after active status decisions." The live data does not support that as the dominant problem.

| Field | Latest final status | School recognition | Count |
|---|---|---|---:|
| `find_email` | `unknown` | `unknown` | 764 |
| `find_website` | `unknown` | `unknown` | 427 |
| `find_email` | `review` | `unknown` | 210 |
| `find_website` | `review` | `unknown` | 109 |
| `find_website` | no decision | no decision | 77 |
| `find_email` | no decision | no decision | 33 |
| any | `active` | any | 0 |

There were no observed `status_dependency_unmet` jobs with latest `final_status = active`.

### Relevant Logic

`pipeline.py` only reactivates non-Instagram `status_dependency_unmet` jobs when `_job_has_active_status_decision(...)` returns true. Instagram gets an evidence-first bypass if it has strong reusable support.

That means the unblocker is strict by design:

| State | Behavior |
|---|---|
| Active status decision exists | Reactivate |
| Canonical active chapter row but no status decision | Do not reactivate for email/website |
| Review/unknown status decision | Stay blocked |
| No decision | Stay blocked |

### Hard Examples

| Example | Fraternity | Field | Latest status | Why blocked |
|---|---|---|---|---|
| Lambda Tau / Florida Atlantic University | Sigma Chi | `find_email` | `unknown`, `no_conclusive_school_status_evidence` | Status not active; school recognition unknown. |
| Eta-Omega / Johnson & Wales University | Sigma Pi | `find_email` | `unknown` | Missing conclusive school evidence. |
| Iota-Rho / Keene State College | Sigma Pi | `find_email` | `unknown` | Missing conclusive school evidence. |
| Epsilon-Epsilon / Emporia State University | Sigma Pi | `find_email` | `unknown` | Missing conclusive school evidence. |
| Lambda / Indiana University | Sigma Chi | `find_email` | `unknown` | Status unresolved despite likely recognizable school. |
| Delta Sigma / Clarkson University | Theta Chi | `find_email` | `unknown` | Low-confidence identity and missing official status evidence. |
| Eta Nu / At The University | Phi Sigma Pi | `find_email` | `unknown` | School identity is semantically incomplete. |
| Zeta Alpha / Arizona State University | Alpha Tau Omega | `find_website` | `unknown` | Stronger school identity, but still no active status decision. |
| Beta-Upsilon / UNC Charlotte | Lambda Chi Alpha | `find_email` | `unknown` | Field states found, but status remains unknown. |
| Iota-Theta / Arkansas State University | Lambda Chi Alpha | `find_email` | `unknown` | Field states found, but status remains unknown. |

### Diagnosis

This bottleneck is not mainly "active decisions are not unblocking jobs." It is "active decisions are not being produced often enough." The strict gate is protecting contact writes correctly.

The improvement target should be:

| Needed change | Reason |
|---|---|
| Increase official status evidence refresh coverage | Converts `unknown`/no decision into active/inactive. |
| Split `status_dependency_unmet` into clearer reason families | Distinguish no decision, unknown decision, review decision, and identity repair. |
| Use school-roster rescue for strong school identities | Cases like Arizona State, UNC Charlotte, Arkansas State should not remain unknown indefinitely. |

## Bottleneck 4: SearXNG Engine Config

### Quantified Shape

Last 24h provider-attempt history:

| Provider/status | Attempts | Result-bearing | Main issue |
|---|---:|---:|---|
| SearXNG `cache_hit` | 14,620 | 14,620 | Good, but cache masks live engine instability. |
| SearXNG `low_signal_fallback` | 13,400 | 13,400 | Results returned but judged weak/noisy. |
| SearXNG live success | 9,354 | 9,354 | Strategic primary is useful when engines respond. |
| SearXNG `engine_unresponsive` | 6,575 | 0 | Upstream engines are failing/suspended. |
| Bing `challenge_or_anomaly` | 8,550 | 0 | HTML fallback is often blocked/challenged. |
| DuckDuckGo timeout | 8,203 | 0 | HTML fallback often times out. |
| Bing circuit open | 12,095 | 0 | Circuit breaker correctly avoids repeated failing HTML calls. |
| DuckDuckGo circuit open | 12,275 | 0 | Same. |
| Serper/Tavily quota exceeded | 447 each | 0 | Managed providers are not viable unless quota/auth is fixed. |

Current SearXNG health:

| Probe | Result | Notes |
|---|---|---|
| `delta chi chapter directory` | result-bearing, degraded | Returned irrelevant Delta Waterfowl results; engines suspended. |
| `site:vt.edu "Delta Chi" "Fraternity and Sorority Life"` | no results | Engine unresponsive. |
| `sigma chi official chapter directory` | result-bearing, degraded | Returned Sigma-Aldrich-like results; low relevance. |

Observed unresponsive engines include `brave`, `duckduckgo`, `startpage`, `karmasearch`, `karmasearch videos`, and `wikipedia`.

### Config Finding

The repo now supports two SearXNG endpoints, but local `.env` is only using one endpoint:

| Env | Current value |
|---|---|
| `CRAWLER_SEARCH_SEARXNG_BASE_URL` | `http://localhost:8888` |
| `CRAWLER_SEARCH_SEARXNG_BASE_URLS` | empty |
| `CRAWLER_SEARCH_SEARXNG_ENGINES` | empty |
| `CRAWLER_SEARCH_SEARXNG_ENGINE_PROFILE` | `auto` |
| `CRAWLER_SEARCH_SEARXNG_STABLE_ENGINES` | empty |
| `CRAWLER_SEARCH_SEARXNG_RESCUE_ENGINES` | empty |

`infra/docker/searxng/README.md` recommends:

| Recommended capability | Current state |
|---|---|
| Primary endpoint on `localhost:8888` | Present |
| Rescue endpoint on `localhost:8889` | Supported by compose, not configured in `.env` |
| Stable engine allowlist | Supported, empty |
| Rescue engine allowlist | Supported, empty |
| `max_in_flight = 1`, interval 750ms | Present |

### Hard Examples

| Query | Provider outcome | Failure mode |
|---|---|---|
| `"delta chi" Mississippi State chapter website` | SearXNG low signal, Bing challenge, DuckDuckGo timeout | Search chain falls through to failing HTML providers. |
| `"At North Carolina State University" fraternity sorority life site:.edu` | SearXNG low signal, Bing challenge, DuckDuckGo timeout | Bad school phrase from identity layer poisons search quality. |
| `"University of Tennessee-Chattanooga" fraternity sorority life site:.edu` | SearXNG low signal, Bing challenge, DuckDuckGo timeout | School alias/query formatting likely weak. |
| `"At North Carolina State University" greek life site:.edu` | SearXNG low signal, Bing challenge, DuckDuckGo timeout | Same identity phrasing issue. |
| `"South Dakota School of Mines & Technology" fraternity sorority life site:.edu` | SearXNG engine unresponsive, Bing challenge, DuckDuckGo timeout | Endpoint reachable but selected engines fail. |
| `"University of Alabama" fraternity sorority life site:.edu` | SearXNG low signal, Bing challenge, DuckDuckGo timeout | SearXNG returns results but relevance gate rejects. |
| `"South Dakota School of Mines & Technology" greek life site:.edu` | SearXNG engine unresponsive, Bing challenge, DuckDuckGo timeout | No healthy fallback. |
| `"delta chi" Mississippi State chapter website` | Repeated low signal | Query repeatedly hits weak relevance despite cache. |

### Diagnosis

SearXNG should stay primary, but it needs a stable engine profile and a rescue endpoint. The crawler is already paced conservatively. The next reliability gain is not more throughput. It is avoiding known-bad SearXNG engines and avoiding HTML fallbacks that are already circuit-opening.

## Bottleneck 5: Worker Phase Observability

### What Operators See

Operators can see:

| Metric | Current value pattern |
|---|---|
| `running_jobs` | Often 0 |
| Worker process lease | Active |
| Batch logs | Processing continues |
| Recent terminal updates | Moving |

The missing piece is the worker phase. A worker can be active but not have a job row in `running` while it is polling, preflighting, triaging, preparing chunks, aggregating, or sleeping.

### Hard Examples

| Time UTC | Event | Processed | Requeued | Interpretation |
|---|---|---:|---:|---|
| 22:34:17 | `field_job_batch_finished` | 120 | 0 | Worker batch completed cleanly. |
| 22:34:42 to 22:34:53 | prepared -> aggregated | 97 | 23 | Worker was active for 11s, but only claim windows count as `running`. |
| 22:35:20 to 22:35:42 | prepared -> aggregated | 109 | 11 | Same active phase with mixed requeues. |
| 22:36:13 to 22:36:33 | prepared -> aggregated | 119 | 1 | Mostly successful batch. |
| 22:37:04 to 22:37:08 | prepared -> aggregated | 120 | 0 | Full batch finished in about 4s. |
| 22:37:33 to 22:37:56 | prepared -> aggregated | 97 | 23 | Batch active for 23s, then finished. |
| 22:38:27 to 22:38:53 | prepared -> aggregated | 111 | 9 | Batch active, no worker alert. |
| Last 2h DB latency | `verify_school_match` | 1,876 completed | avg `0.0801s` running | Dashboard polling cannot reliably observe these as running. |

### Diagnosis

The worker is not idle in the way `running_jobs = 0` implies. The system lacks a phase model.

Add a worker phase heartbeat in `worker_processes.metadata`:

| Phase | Meaning |
|---|---|
| `polling` | Worker is checking queue counts. |
| `preflight` | Worker is running search/provider preflight. |
| `triage` | Worker is reconciling queue states. |
| `claiming` | Worker is claiming jobs. |
| `executing` | At least one chunk is executing claimed jobs. |
| `aggregating` | Supervisor is merging chunk results. |
| `sleeping` | Worker found no runnable work or is between polls. |

Recommended dashboard metric:

```text
worker_liveness =
  active lease exists
  AND last heartbeat age < lease threshold
  AND phase in {polling, preflight, triage, claiming, executing, aggregating, sleeping}
```

Do not use `running_jobs > 0` alone as worker liveness.

## Cross-Bottleneck Relationships

| Relationship | Evidence |
---|---|
| Bad school identities degrade search queries | Queries like `"At North Carolina State University" ...` come directly from weak identity strings. |
| `school_evidence_missing` and `status_dependency_unmet` overlap conceptually | Missing decisive school evidence prevents active status decisions, which keeps contact jobs blocked. |
| Verify-school is now fast and safe, but it exposes upstream data gaps | It processes quickly when evidence exists and blocks when evidence is missing. |
| SearXNG degradation magnifies official evidence gaps | Evidence-refresh paths need search; if SearXNG engines are degraded, blocked evidence remains blocked. |
| Lack of phase metrics makes normal fast execution look like zero work | Batch logs prove throughput while `running_jobs` often reads zero. |

## Recommended Next Implementation Order

| Priority | Action | Expected effect |
|---:|---|---|
| 1 | Add worker phase heartbeat and dashboard/API exposure | Stops misdiagnosing healthy polling/batch execution as dead workers. |
| 2 | Configure SearXNG rescue endpoint and stable engine allowlist | Reduces search preflight volatility and avoids known bad engines. |
| 3 | Add school-name normalization repair for short aliases and `At ...` prefixes | Directly reduces identity and status blocker load. |
| 4 | Build a bounded official evidence refresh for top `school_evidence_missing` schools/fraternities | Converts cache misses into active/inactive decisions. |
| 5 | Split `status_dependency_unmet` into `status_no_decision`, `status_unknown`, and `status_review_required` | Makes the queue tell operators what prerequisite is actually missing. |
| 6 | Promote school-roster rescue for strong school identities | Converts unknown decisions to active/inactive without broad search. |

## Acceptance Metrics For The Next Pass

| Metric | Target |
|---|---:|
| `identity_semantically_incomplete` queued count | Down 25 percent after school-name repair pass |
| `school_evidence_missing` verify-school count | Down 25 percent after official evidence refresh |
| `status_dependency_unmet` with no decision/unknown | Down 20 percent after status refresh |
| SearXNG `engine_unresponsive` rate | Down 50 percent after engine allowlist/rescue endpoint |
| Worker phase coverage | 100 percent of active worker heartbeats include phase |
| Unsafe contact writes | Remains 0 nationals-only contact rows |

