# Operator Access Control And Audit Logging

## Purpose

FratFinderAI exposes an operator console that can create crawl requests, run benchmarks, mutate chapter records, dispatch CRM campaigns, and perform runtime maintenance. This control adds app-level role checks and audit evidence for privileged actions.

## Threat/Risk Addressed

Without app-level RBAC, any user or script that can reach the operator API can trigger destructive or side-effecting workflows. Audit logging makes privileged actions attributable and reviewable.

## Implementation Summary

The web app uses `apps/web/src/lib/security/operator-access.ts` for:

- Token authentication.
- HttpOnly operator sessions.
- Bearer-token script access.
- Role enforcement.
- Privileged-route audit logging.

## Roles

| Role | Capabilities |
| --- | --- |
| `admin` | Full operator access, including chapter deletion, CRM dispatch, and runtime maintenance. |
| `operator` | Operational workflows such as crawl requests, benchmark runs, campaign creation, review updates, and chapter reruns. |
| `analyst` | Read-only dashboard/API access. |

## Permission Matrix

| Route/action family | Required role |
| --- | --- |
| Health liveness/readiness | Public |
| Read-only dashboard/API GETs | `analyst`, `operator`, `admin` |
| Review item updates | `operator`, `admin` |
| Crawl request create/confirm/cancel/reschedule/expedite | `operator`, `admin` |
| Chapter reruns | `operator`, `admin` |
| Benchmark creation/execution | `operator`, `admin` |
| CRM campaign create/update/status changes | `operator`, `admin` |
| CRM dispatch/send | `admin` |
| Chapter deletion | `admin` |
| Runtime maintenance | `admin` |

## Configuration

Set these environment variables:

```text
WEB_ADMIN_TOKEN=<strong admin token>
WEB_OPERATOR_TOKEN=<strong operator token>
WEB_ANALYST_TOKEN=<strong analyst token>
WEB_OPERATOR_SESSION_SECRET=<strong signing secret>
```

`WEB_AUTH_DISABLED=true` is allowed only in local development and tests. Production fails closed when auth is disabled or required secrets are weak/missing.

## Session Behavior

`POST /api/auth/operator-login` accepts an operator token and sets an HttpOnly session cookie. Cookies use SameSite=Lax and are Secure outside local development.

`POST /api/auth/operator-logout` clears the session cookie.

`GET /api/auth/operator-session` reports whether the current request has a valid operator session without exposing secrets.

## Bearer Token Behavior

Scripts and tests may send:

```text
Authorization: Bearer <operator-token>
```

Bearer tokens are compared using constant-time comparison.

## Audit Event Schema

The `operator_audit_events` table records:

- `actor`
- `role`
- `action`
- `route`
- `method`
- `target_type`
- `target_id`
- `request_id`
- `result`
- `metadata`
- `error_code`
- `created_at`

`result` is constrained to `allowed`, `denied`, or `error`.

## Examples

- An anonymous `POST /api/fraternity-crawl-requests` is denied and logged as `denied`.
- An operator can confirm or cancel crawl requests.
- An operator cannot delete chapters or dispatch CRM sends.
- An admin can run runtime maintenance.
- Health readiness/liveness routes remain public.

## Validation Commands

```powershell
pnpm lint
pnpm typecheck
pnpm test:web
pnpm test:contracts
python -m pytest tests/integration -m integration
```

## Expected Logs

Privileged allowed, denied, and error outcomes should create rows in `operator_audit_events`. Denied responses do not reveal which token or role check failed.

## Known Limitations

This sprint uses environment-token authentication rather than enterprise identity. Tokens must be rotated manually and protected as secrets.

## Future Hardening Steps

- Replace environment tokens with OAuth, SAML, or an identity-aware proxy.
- Add per-operator named identities.
- Add audit log retention policies and export.
- Add UI-visible audit history for privileged workflows.
