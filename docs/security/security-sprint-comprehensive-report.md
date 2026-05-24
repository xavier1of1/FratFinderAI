# FratFinderAI Security Sprint Comprehensive Report

Date: 2026-05-24

Project version: 3.0.4

Workspace: `D:\VSC Programs\FratFinderAI`

## Executive Summary

The three-phase security sprint added professional, automation-backed security controls to FratFinderAI without weakening crawler precision, provenance rules, queue safety, local SearXNG/provider behavior, or contact-write safeguards.

The sprint delivered:

- Phase 1: SBOM and SCA evidence pipeline.
- Phase 2: SSRF-safe outbound crawler fetch policy.
- Phase 3: Operator RBAC and privileged audit logging.

Local validation is complete for all three phases. The remaining release-polish item is to capture one successful GitHub Actions run proving the same SBOM/SCA artifacts and validation commands pass outside the local workstation.

## Overall Validation Status

| Area | Status | Evidence |
| --- | --- | --- |
| Phase 1 SBOM/SCA | Locally complete, CI configured | Local Syft/Grype artifacts generated; policy passed with two temporary critical ignores. |
| Phase 2 SSRF-safe fetch | Locally accepted | Focused SSRF tests, call-site coverage, crawler suite, integration, lint, and typecheck passed. |
| Phase 3 Operator RBAC/audit | Locally complete | Web tests, route coverage, lint, typecheck, contracts, and integration passed. |
| Final CI proof | Pending release polish | `security-sbom-sca` workflow is configured; one successful hosted run and artifact proof should be attached before final release acceptance. |

## Validation Command Summary

| Command | Result |
| --- | --- |
| `pnpm.cmd lint` | PASS |
| `pnpm.cmd typecheck` | PASS |
| `pnpm.cmd test:contracts` | PASS, 1 file and 5 tests |
| `pnpm.cmd test:web` | PASS, 7 files and 47 tests |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_security_sca_policy.py services/crawler/src/fratfinder_crawler/tests/test_url_safety.py services/crawler/src/fratfinder_crawler/tests/test_ssrf_callsite_coverage.py` | PASS, 36 security-focused tests |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests -k "url_safety or ssrf"` | PASS, 28 tests |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70` | PASS, 589 tests, 70.84% coverage |
| `python -m pytest tests/integration -m integration` | PASS, 2 tests |
| `python scripts/security/security_sca.py validate-ignores --ignore-file .security/vulnerability-ignores.yml` | PASS |
| `powershell -ExecutionPolicy Bypass -File scripts\security\generate_sbom_sca.ps1` | PASS |
| `python scripts/security/security_sca.py evaluate --ignore-file .security/vulnerability-ignores.yml --scan security/vulnerability-scan.json` | PASS |

## Phase 1: SBOM And SCA Evidence Pipeline

### Objective

Phase 1 created a repeatable software supply-chain evidence workflow. The goal was not just to generate an SBOM file, but to produce component inventories, vulnerability scan output, policy decisions, documented exceptions, and CI artifacts.

### Implemented Controls

- Added a `security-sbom-sca` GitHub Actions job in `.github/workflows/ci.yml`.
- Added security scripts under `scripts/security/`.
- Added `.security/vulnerability-ignores.yml`.
- Added documentation in `docs/security/sbom.md`.
- Generated CycloneDX JSON SBOMs with Syft.
- Scanned generated SBOMs and dependency outputs with Grype.
- Produced Markdown evidence in `security/sbom-evidence.md`.
- Enforced vulnerability policy:
  - malformed ignores fail
  - expired ignores fail
  - unignored critical vulnerabilities fail
  - high vulnerabilities warn by default
  - highs can be promoted to failure with `SECURITY_FAIL_ON_HIGH=true`

### Required Artifacts

The workflow and local generator produce:

```text
sbom/apps-web.cdx.json
sbom/crawler-python.cdx.json
sbom/docker-web.cdx.json
sbom/docker-crawler.cdx.json
security/vulnerability-scan.json
security/sbom-evidence.md
```

### Local Artifact Proof

Syft and Grype versions:

```text
Syft:  Application: syft, Version: 1.44.0, Platform: windows/amd64
Grype: Application: grype, Version: 0.112.0, Platform: windows/amd64
```

Generated artifact sizes:

```text
sbom/apps-web.cdx.json             174,223 bytes
sbom/crawler-python.cdx.json         4,207 bytes
sbom/docker-web.cdx.json           941,073 bytes
sbom/docker-crawler.cdx.json       466,742 bytes
security/vulnerability-scan.json   566,255 bytes
security/sbom-evidence.md            1,688 bytes
```

CycloneDX component proof:

```text
sbom/apps-web.cdx.json:        bomFormat=CycloneDX, components=145
sbom/crawler-python.cdx.json:  bomFormat=CycloneDX, components=1
sbom/docker-web.cdx.json:      bomFormat=CycloneDX, components=1164
sbom/docker-crawler.cdx.json:  bomFormat=CycloneDX, components=788
```

### Vulnerability Policy Result

```text
Critical vulnerability policy result: pass
Ignored vulnerability count: 2
Expired ignore count: 0
Final result: pass
Critical: 2
High: 30
Medium: 53
Low: 9
Negligible: 0
Unknown: 0
```

### Remediation Completed During Phase 1

- Upgraded `next` from `14.2.15` to `14.2.25`.
- Added a pnpm override to move `esbuild` from `0.21.5` to `0.27.7`.
- Changed the web image base from `node:20-bookworm-slim` to `node:22-alpine`.
- Changed the crawler image base from `python:3.11-slim` to `python:3.13-alpine`.
- Fixed SBOM/SCA generation gaps around Docker ignore coverage, PowerShell native-command exit handling, scan wildcard expansion, UTF-8 BOM JSON parsing, and app SBOM component discovery.

### Temporary Exceptions

Two critical Python findings remain temporarily ignored:

```text
CVE-2026-6100 in python
CVE-2026-7210 in python
```

Both exceptions:

- are owned by `security-team`
- expire on `2026-06-30`
- include a non-placeholder justification
- are accepted only because tested Python base image candidates still reported no fixed version from Grype

### Phase 1 Assessment

Phase 1 is locally complete and CI-configured. Final release acceptance should include one successful hosted `security-sbom-sca` workflow run with artifact proof.

## Phase 2: SSRF-Safe Outbound Fetch Policy

### Objective

Phase 2 prevents crawler-discovered URLs from reaching local services, private networks, cloud metadata endpoints, or unsafe schemes while preserving trusted configured provider behavior such as local SearXNG.

### Implemented Controls

- Added central module `services/crawler/src/fratfinder_crawler/security/url_safety.py`.
- Added `safe_untrusted_fetch`, `safe_untrusted_get`, and `safe_untrusted_head`.
- Refactored untrusted crawler fetch paths to use the safe wrapper.
- Added static call-site coverage in `test_ssrf_callsite_coverage.py`.
- Added unit coverage in `test_url_safety.py`.
- Added documentation in `docs/security/ssrf-url-safety.md`.
- Added detailed validation report in `docs/security/phase-2-ssrf-validation-report.md`.

### URL Safety Rules

The safe wrapper:

- allows only `http` and `https`
- rejects malformed URLs and missing host/netloc
- rejects embedded credentials
- resolves DNS before request
- validates all resolved IPv4 and IPv6 addresses
- blocks loopback, private, link-local, multicast, reserved, unspecified, and metadata ranges
- specifically blocks `169.254.169.254`
- disables automatic redirects
- manually follows redirects up to the configured limit
- revalidates every redirect target
- streams response bodies and enforces a byte cap
- enforces allowed content types
- sets request timeouts
- strips sensitive outbound headers
- disables ambient process auth/proxy inheritance for crawler-discovered URLs
- logs denied URLs with stable reason codes

### Deny Reason Codes

```text
invalid_url
unsupported_scheme
embedded_credentials_blocked
hostname_resolution_failed
blocked_ip_range
redirect_blocked
too_many_redirects
content_type_not_allowed
response_too_large
request_timeout
request_error
```

### Required Blocked Targets Covered

Tests prove these are blocked:

```text
localhost
127.0.0.1
0.0.0.0
[::1]
10.0.0.1
172.16.0.1
192.168.1.1
169.254.169.254
169.254.0.1
224.0.0.1
255.255.255.255
fc00::1
fe80::1
domain resolving to private IP
public URL redirecting to private IP
file:///etc/passwd
oversized response
disallowed content type
local SearXNG through untrusted fetch
```

Tests also prove valid public HTTPS HTML fixtures are allowed and local SearXNG works through the trusted provider path.

### Call-Site Coverage

Untrusted crawler fetch paths now use `safe_untrusted_*` in:

```text
discovery.py
field_jobs.py
http/client.py
pipeline.py
social/bulk_backfill_instagram.py
status/page_fetcher.py
```

Direct raw request usage is restricted to approved exceptions:

- the safe wrapper itself
- trusted search provider client
- trusted SearXNG health diagnostics
- tests and fixtures

### Phase 2 Validation

```text
python -m pytest services/crawler/src/fratfinder_crawler/tests -k "url_safety or ssrf"
28 passed

python -m pytest services/crawler/src/fratfinder_crawler/tests/test_security_sca_policy.py services/crawler/src/fratfinder_crawler/tests/test_url_safety.py services/crawler/src/fratfinder_crawler/tests/test_ssrf_callsite_coverage.py
36 passed

python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70
589 passed, 70.84% coverage

python -m pytest tests/integration -m integration
2 passed

pnpm.cmd lint
PASS

pnpm.cmd typecheck
PASS
```

### Phase 2 Assessment

Phase 2 is locally accepted. It is centralized, tested, documented, enforced at untrusted crawler call sites, preserves trusted provider behavior, and includes regression coverage for the major SSRF bypass classes.

## Phase 3: Operator RBAC And Audit Logging

### Objective

Phase 3 turns the operator dashboard/API from a trusted local console into an app-level protected operator surface with role-gated actions and auditable privileged operations.

### Implemented Controls

- Added shared helper `apps/web/src/lib/security/operator-access.ts`.
- Added exactly three roles:
  - `admin`
  - `operator`
  - `analyst`
- Added environment-token auth:
  - `WEB_ADMIN_TOKEN`
  - `WEB_OPERATOR_TOKEN`
  - `WEB_ANALYST_TOKEN`
  - `WEB_OPERATOR_SESSION_SECRET`
- Added constant-time token comparison.
- Added session auth endpoints:
  - `POST /api/auth/operator-login`
  - `POST /api/auth/operator-logout`
  - `GET /api/auth/operator-session`
- Added HttpOnly, SameSite=Lax, production-Secure session cookies.
- Added bearer-token support for scripts/tests.
- Added fail-closed production behavior for disabled auth or invalid/missing secrets.
- Added `requireRole`.
- Added `withOperatorAccess`.
- Added `withReadOnlyOperatorAccess`.
- Added migration `infra/supabase/migrations/0037_operator_audit_events.sql`.
- Added documentation in `docs/security/operator-access-control.md`.
- Added detailed validation report in `docs/security/phase-3-operator-rbac-validation-report.md`.

### Permission Model

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

### Audit Event Schema

The `operator_audit_events` table includes:

```text
id
actor
role
action
route
method
target_type
target_id
request_id
result
metadata
ip_address
user_agent
error_code
created_at
```

The `result` field is constrained to:

```text
allowed
denied
error
```

### Route Coverage

The route coverage test scans all API route files under `apps/web/src/app/api/**/route.ts`.

Rules:

- all mutating `POST`, `PUT`, `PATCH`, and `DELETE` handlers must be exported through `withOperatorAccess`
- all non-public `GET` handlers must be exported through `withReadOnlyOperatorAccess`
- explicit public exceptions are documented for auth and health routes

The route coverage test was strengthened so a route cannot pass merely because wrapper text appears somewhere in the file; the exported handler assignment itself must be wrapped.

### Security Behavior Tested

Tests cover:

- anonymous mutation denied
- invalid bearer token denied
- analyst read allowed
- analyst mutation denied through route matrix
- operator operational actions allowed
- operator admin-only actions denied
- admin admin-only actions allowed
- `WEB_AUTH_DISABLED=true` denied in production
- missing production session secret denied
- successful protected handler creates allowed audit event
- denied requests create denied audit event
- protected handler exception creates error audit event
- production protected-route errors hide raw exception text and include request ID
- login sets HttpOnly SameSite=Lax cookie
- logout clears session cookie
- session endpoint does not expose secrets
- liveness/readiness remain public

### Phase 3 Validation

```text
pnpm.cmd test:web
7 files passed, 47 tests passed

pnpm.cmd lint
PASS

pnpm.cmd typecheck
PASS

pnpm.cmd test:contracts
1 file passed, 5 tests passed

python -m pytest tests/integration -m integration
2 passed
```

Expected stderr from fail-closed tests:

```text
Operator auth configuration failed closed: WEB_AUTH_DISABLED cannot be used in production.
Operator auth configuration failed closed: WEB_OPERATOR_SESSION_SECRET is required when operator auth is enabled.
```

These messages are expected because tests intentionally exercise production fail-closed behavior.

### Phase 3 Assessment

Phase 3 is locally complete. Privileged routes are role-gated, route coverage protects against unwrapped API handlers, audit logging records allowed/denied/error outcomes, and production error behavior avoids raw internal error leakage.

## Security Controls Preserved

The sprint did not weaken the project’s existing accuracy/safety model:

- crawler source confidence thresholds preserved
- provenance recording preserved
- review queue behavior preserved
- zero-chapter safety gates preserved
- contact write rules preserved
- field-job backoff behavior preserved
- trusted local SearXNG/provider behavior preserved
- health/liveness/readiness routes preserved

## Documentation Produced

```text
docs/security/sbom.md
docs/security/ssrf-url-safety.md
docs/security/operator-access-control.md
docs/security/security-sprint-validation.md
docs/security/phase-2-ssrf-validation-report.md
docs/security/phase-3-operator-rbac-validation-report.md
docs/security/security-sprint-comprehensive-report.md
```

## Known Limitations And Follow-Up Work

### Phase 1

- Needs one successful hosted GitHub Actions run proving artifact upload outside the local workstation.
- Two Python critical vulnerabilities remain temporarily ignored until `2026-06-30`.
- Signed attestations and SPDX export are future hardening.

### Phase 2

- Application-layer SSRF controls do not replace firewall, container, or cloud egress controls.
- DNS pinning/custom transport support remains future hardening.
- Denied-URL telemetry could be surfaced in the operator console later.

### Phase 3

- Uses environment tokens and signed cookies, not OAuth/SAML/IAP.
- Actors are role-derived token/session identities, not named human accounts.
- Audit log UI/export/retention policy is future work.

## Final Acceptance Recommendation

The sprint is locally complete and ready for CI-backed final acceptance.

Before calling the security sprint fully release-complete, attach:

1. A successful GitHub Actions run for `security-sbom-sca`.
2. The uploaded SBOM/security artifact list from that run.
3. The hosted CI result for lint, typecheck, web tests, crawler tests, and integration tests.

Once those hosted CI artifacts are captured, the three-phase security sprint will have both local and CI evidence suitable for a professional security/compliance review.
