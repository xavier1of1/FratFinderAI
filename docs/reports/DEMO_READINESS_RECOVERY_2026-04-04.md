# Demo Readiness Recovery Report - 2026-04-04

## Scope
Implemented and validated the Demo-Readiness Recovery Plan (LangGraph-forward, legacy-backed) across discovery, web intake, crawl runner safety, and field-job runtime fallback observability.

## Completed Changes

1. Discovery and source selection hardening
- Added blocked-host-safe selection policy and final source-quality gate.
- Added explicit `source_quality` and `selected_candidate_rationale` to discovery payloads.
- Added resilient provider-chain behavior: per-query errors no longer abort discovery.
- Added richer reject traces: `blocked_host`, `weak_path`, `no_safe_candidate`, `search_query_error`.
- Added robust alias support for:
  - Pi Kappa Alpha (`pike`, `pka`, `\u03a0\u039a\u0391`)
  - Tau Kappa Epsilon (`tke`, `tekes`, `\u03a4\u039a\u0395`)
  - plus SAE/SigEp/ATO/Kappa Sigma alias canonicals.

2. Web intake safety
- `POST /api/fraternity-crawl-requests` now applies source-quality gating and only auto-queues when source is medium/high confidence and non-weak.
- Unsafe/weak sources are created as `draft` + `awaiting_confirmation` with actionable error reasons.

3. Web runner auto-recovery and stop-state safety
- Added deterministic pre-run source check in request runner.
- If weak source is detected pre-run:
  - one safe rediscovery attempt is performed,
  - source is auto-upgraded when a stronger safe candidate is found,
  - otherwise request is moved to `awaiting_confirmation` (not silently failed).
- Zero-chapter crawl outcomes now move requests to `awaiting_confirmation` with `source_rejected` telemetry instead of terminal dead-end failure.

4. LangGraph-forward with explicit legacy backup
- Added runtime fallback in enrichment cycle execution:
  - on LangGraph cycle error (non-timeout), runner retries cycle with `legacy` runtime,
  - emits `runtime_fallback` event.
- Added runtime metadata to `process-field-jobs` output:
  - `runtime_mode_used`
  - `runtime_fallback_count`
- Added guardrail fallback in pipeline chunk processing:
  - fallback on missing graph tables,
  - fallback on runtime graph exception.

5. Analytics/counters
- Enrichment analytics now track:
  - `runtimeFallbackCount`
  - `queueBurnRate`
- Source analytics now track recovery/rejection/zero-chapter prevention counters.

## Validation Results

## Automated tests
| Suite | Result |
|---|---|
| `python -m py_compile` (discovery/pipeline/supervisor) | Pass |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_discovery.py -q` | Pass (18/18) |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_workers.py -q` | Pass |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_pipeline_eval_enrichment.py -q` | Pass |
| `pnpm --filter @fratfinder/web typecheck` | Pass |

## Demo-oriented CLI checks
| Input | Result | Selected URL | Notes |
|---|---|---|---|
| `Pi Kappa Alpha` | Pass | `https://pikes.org/chapters/` | Blocked Wikipedia existing source safely rejected, curated safe hint selected |
| `\u03a0\u039a\u0391` | Pass | `https://pikes.org/chapters/` | Greek letters normalized to canonical fraternity identity |
| `Tau Kappa Epsilon` | Pass | `https://tke.org/join-tke/find-a-chapter/` | Verified registry selection with high confidence |
| `\u03a4\u039a\u0395` | Pass | `https://tke.org/join-tke/find-a-chapter/` | Greek letters normalized to canonical fraternity identity |

## Runtime metadata check
| Command | Result |
|---|---|
| `python -m fratfinder_crawler.cli process-field-jobs --limit 0 --source-slug sigma-chi-main --runtime-mode langgraph_primary --graph-durability async` | Pass, returns `runtime_mode_used` + `runtime_fallback_count` |

## Environment Note
Search provider calls in this execution environment showed network/socket restrictions for external providers. Despite that, discovery behavior remained deterministic and safe because:
- per-query failures are traced and non-fatal,
- blocked/weak candidate sources are not auto-selected,
- curated safe hints and verified registry records are still used.

## Operator Playbook (Demo)
1. Create request with full name or Greek-letter variant.
2. If request lands in `awaiting_confirmation`, inspect `source_rejected` reason and choose one of listed alternatives.
3. Re-queue request after confirmation.
4. Monitor events:
   - `source_recovered`
   - `runtime_fallback`
   - `enrichment_cycle`
5. Validate queue burn-down from enrichment analytics (`queueBurnRate`, `queueRemaining`).
