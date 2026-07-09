# Security Sprint Validation

## Scope

Validated the three security phases implemented for FratFinderAI:

- SBOM and SCA evidence pipeline
- SSRF-safe outbound crawler fetching
- Operator RBAC and audit logging

Validation was run locally on Windows from `<repo root>` on 2026-05-23.

## Results

| Command | Result | Evidence |
| --- | --- | --- |
| `pnpm lint` | PASS | Contracts and web TypeScript lint/type checks completed. |
| `pnpm typecheck` | PASS | Contracts and web typecheck completed. |
| `pnpm test:contracts` | PASS | 1 file, 5 tests passed. |
| `pnpm test:web` | PASS | 7 files, 47 tests passed. |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_security_sca_policy.py services/crawler/src/fratfinder_crawler/tests/test_url_safety.py services/crawler/src/fratfinder_crawler/tests/test_ssrf_callsite_coverage.py` | PASS | 36 security-focused tests passed. |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests -k "url_safety or ssrf"` | PASS | 28 tests passed, 561 deselected. |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70` | PASS | 589 tests passed, coverage 70.84%. |
| `python -m pytest tests/integration -m integration` | PASS | 2 integration tests passed. |
| `python scripts/security/security_sca.py validate-ignores --ignore-file .security/vulnerability-ignores.yml` | PASS | Ignore file schema accepted. |
| `powershell -ExecutionPolicy Bypass -File scripts\security\generate_sbom_sca.ps1` | PASS | Local Syft/Grype generation completed, including Docker image builds, Docker SBOMs, vulnerability scan, evidence report, and policy gate. |
| `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_security_sca_policy.py` | PASS | 8 SCA policy tests passed. |
| `python scripts/security/security_sca.py evaluate --ignore-file .security/vulnerability-ignores.yml --scan security/vulnerability-scan.json` | PASS | Policy accepted generated scan output with 2 valid, non-expired temporary Python ignores. |

## SBOM Artifact Validation

The GitHub Actions job `security-sbom-sca` is configured to generate and upload:

- `sbom/apps-web.cdx.json`
- `sbom/crawler-python.cdx.json`
- `sbom/docker-web.cdx.json`
- `sbom/docker-crawler.cdx.json`
- `security/vulnerability-scan.json`
- `security/sbom-evidence.md`

The CI job validates CycloneDX format, component counts, scan JSON validity, and evidence report presence with `jq` and shell checks.

Local artifact generation was completed on 2026-05-23 after installing Syft and Grype with WinGet:

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

CycloneDX and component-count checks passed:

```text
sbom/apps-web.cdx.json:        bomFormat=CycloneDX, components=145
sbom/crawler-python.cdx.json:  bomFormat=CycloneDX, components=1
sbom/docker-web.cdx.json:      bomFormat=CycloneDX, components=1164
sbom/docker-crawler.cdx.json:  bomFormat=CycloneDX, components=788
```

Policy evidence result:

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

The two remaining critical findings are temporarily ignored in `.security/vulnerability-ignores.yml`:

- `CVE-2026-6100` in `python`
- `CVE-2026-7210` in `python`

Both ignores are owned by `security-team`, expire on `2026-06-30`, and are justified because all tested Python base image candidates still reported these Python binary findings with no fixed version available from Grype. The tested candidate set included Debian slim and Alpine variants across Python 3.11, 3.12, and 3.13.

Remediation already completed before applying those two temporary ignores:

- Upgraded `next` from `14.2.15` to `14.2.25`, clearing the critical Next.js finding.
- Added a pnpm override to move `esbuild` from vulnerable `0.21.5` to `0.27.7`, clearing Go stdlib findings from the web image.
- Changed the web image base from `node:20-bookworm-slim` to `node:22-alpine`, removing the Debian OS criticals from the web image.
- Changed the crawler image base from `python:3.11-slim` to `python:3.13-alpine`, reducing crawler image criticals to the two no-fix Python findings above.

## Documented Exceptions

- Local SBOM/SCA artifact generation requires external scanner binaries. This was intentionally kept outside production dependencies.
- Phase 1 has local artifact proof and a passing policy gate. The two remaining critical findings are accepted only through short-lived, owner-assigned ignores that must be revisited before `2026-06-30`.
- Health and auth session routes remain public by design.
- `GET /api/ops/runtime-maintenance` remains public only as a `405 Method Not Allowed` response; the mutating runtime-maintenance `POST` is admin-only.

## Security Controls Verified

- Vulnerability ignores reject missing fields, expired dates, weak reasons, and unsupported vulnerability IDs.
- Unignored critical vulnerabilities fail policy.
- High vulnerabilities warn by default and can be promoted to failure with `--fail-on-high` or `SECURITY_FAIL_ON_HIGH=true` in CI.
- SSRF guard blocks local/private/link-local/multicast/reserved targets, metadata IPs, unsupported schemes, embedded credentials, private DNS, redirect-to-private, oversized bodies, and disallowed content types.
- SSRF guard strips sensitive outbound headers from untrusted fetches and disables ambient process auth/proxy inheritance for crawler-discovered URLs.
- Local SearXNG remains allowed through the trusted provider path and blocked through untrusted crawler fetching.
- Static crawler call-site coverage blocks new raw `requests.get`, `requests.head`, or `requests.Session` usage outside approved exceptions.
- Mutating API route coverage requires `withOperatorAccess`.
- Read-only dashboard/API GET route coverage requires `withReadOnlyOperatorAccess`.
- Anonymous, denied, allowed, and error privileged route outcomes are auditable through `operator_audit_events`.

## Phase 2 Detailed Evidence

See `docs/security/phase-2-ssrf-validation-report.md` for the SSRF-safe outbound fetch requirement matrix, call-site coverage proof, blocked-target coverage, and real validation log excerpts.

## Phase 3 Detailed Evidence

See `docs/security/phase-3-operator-rbac-validation-report.md` for the operator RBAC and audit logging requirement matrix, route coverage proof, permission-matrix evidence, and real validation log excerpts.

## Comprehensive Report

See `docs/security/security-sprint-comprehensive-report.md` for the consolidated Phase 1, Phase 2, and Phase 3 security sprint report, including objectives, implementation details, validation summaries, known limitations, and final CI acceptance guidance.
