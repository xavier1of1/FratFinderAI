# V3 MVP Validation (2026-04-04)

## Scope

This report captures the first live validation pass for the V3 request-worker runtime:

- dedicated Python request worker owns queued request execution
- LangGraph request supervisor persists graph runs, events, and checkpoints
- request progress stays compatible with the current web product
- Agent Ops surfaces expose request graph runs, provisional chapters, and evidence

## Validation Summary

### Automated checks

- `pnpm --filter @fratfinder/web typecheck`
- `python -m pytest services/crawler/src/fratfinder_crawler/tests -q`

Both passed after the V3 worker/runtime changes.

### Live request-level validation

Baseline V2 request:

- Request ID: `e97c8aee-ddd0-4904-b86e-8f4be8457a75`
- Fraternity: `Tau Kappa Epsilon`
- Status: `succeeded`
- Records seen: `2`
- Records upserted: `1`
- End-to-end elapsed from request row timestamps: `53.808s`

Live V3 request:

- Request ID: `99fe6b24-0632-4869-a9a3-017dfa0aad10`
- Runtime: `v3_request_supervisor`
- Crawl runtime used: `legacy`
- Status: `succeeded`
- Records seen: `2`
- Records upserted: `1`
- CLI elapsed: `7.819s`
- Request row elapsed: `7.425s`

Result:

- V3 matched the useful output of the recent V2 baseline
- V3 completed about `7.2x` faster on this validated request path

### Live worker claim-loop validation

Worker-run request:

- Request ID: `e7c5d314-4b03-4e34-a40e-3dc590f36647`
- Command: `python -m fratfinder_crawler.cli run-request-worker --once --limit 1`
- Worker: `local-request-worker`
- Status: `succeeded`
- Stage: `completed`
- Crawl runtime used: `legacy`
- Records seen: `2`

Result:

- queued request was claimed by the worker
- request completed through the production worker path
- graph lineage was written back into request progress

### Live queue-drain validation

Multi-request worker run:

- Inserted request IDs:
  - `c3ed3b75-d16c-48b3-ae83-3d95d4c99a67`
  - `fc35702c-bcdc-4690-8020-fbe1901c76d5`
- Command: `python -m fratfinder_crawler.cli run-request-worker --once --limit 2`
- Worker elapsed: `10.419s`
- Requests processed: `2`
- Requests succeeded: `2`
- Pending request count after run: `0`

Result:

- the V3 request worker drained a fresh queue batch without leaving stuck `queued` or `running` rows
- both requests completed with `queueRemaining = 0`
- both request graph runs finalized as `succeeded`

### Website validation

Validated operator surfaces:

- `pnpm --filter @fratfinder/web build`
- `pnpm --filter @fratfinder/web typecheck`
- `pnpm --filter @fratfinder/web test`

Result:

- the Next.js app compiled successfully with the V3 Agent Ops additions
- the new Agent Ops API/page rendered as part of the production build
- Fraternity Intake now exposes request graph metadata and provisional promotion state
- Overview and layout surfaces now identify the product as `V3.0.0`

### Live website smoke

Built server smoke on `http://localhost:3100`:

- `GET /` -> `200`
- `GET /agent-ops` -> `200`
- `GET /api/agent-ops?limit=5` -> `200`
- API payload:
  - `success = true`
  - `graphRunsReturned = 4`
  - `queuedRequests = 0`
  - `runningRequests = 0`
  - `provisionalOpen = 0`

Result:

- the V3 operator dashboard is serving correctly from a production build
- the Agent Ops page no longer throws during server rendering
- the request queue was clear during the final smoke pass

## Important Finding

Adaptive crawl is not yet at parity on several real sources. During live validation:

- `tau-kappa-epsilon-main`: legacy produced `2` records, adaptive produced `0`
- `pi-kappa-alpha-main`: legacy produced `1492` records, adaptive produced `0`
- `sigma-alpha-epsilon-main`: legacy produced `1287` records, adaptive produced `0`

The root cause on major sources was not just extraction quality. Some source records were seeded from `wikipedia.org`, and adaptive frontier expansion followed low-value same-host links before useful extraction completed.

## MVP Runtime Decision

Because the goal for MVP is usable product functionality plus better-than-V2 performance, the V3 default crawl runtime is set to `legacy` for now:

- `CRAWLER_V3_CRAWL_RUNTIME_MODE=legacy`

V3 still improves the system materially because it now adds:

- request-level LangGraph supervision
- worker-owned execution
- graph runs / checkpoints / event persistence
- Agent Ops visibility
- evidence ledger support
- provider-health snapshots
- field-job LangGraph continuation

Adaptive crawl remains available behind runtime overrides and fallback logic until it reaches source-parity.
