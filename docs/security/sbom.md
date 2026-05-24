# SBOM And SCA Evidence

## Purpose

FratFinderAI generates software bills of materials (SBOMs) and vulnerability scan evidence in CI so supply-chain risk is visible, repeatable, and reviewable.

## Threat/Risk Addressed

The platform contains a Next.js operator console, Python crawler, and Docker images. A vulnerable dependency or base image can affect crawl automation, operator workflows, or data integrity. SBOMs provide a component inventory, and SCA scanning connects that inventory to vulnerability policy.

## Implementation Summary

The `security-sbom-sca` GitHub Actions job generates CycloneDX JSON SBOMs with Syft for:

- `sbom/apps-web.cdx.json`
- `sbom/crawler-python.cdx.json`
- `sbom/docker-web.cdx.json`
- `sbom/docker-crawler.cdx.json`

The job scans those SBOMs with Grype and writes:

- `security/vulnerability-scan.json`
- `security/sbom-evidence.md`

The policy helper in `scripts/security/security_sca.py` validates ignores, evaluates critical/high vulnerability policy, and renders the Markdown evidence summary.

## Configuration

Vulnerability exceptions live in `.security/vulnerability-ignores.yml`.

Each ignore must include:

- `package`
- `vulnerability_id`
- `reason`
- `owner`
- `expires`

Reasons must be specific, and `expires` must be a future ISO date.

Set `SECURITY_FAIL_ON_HIGH=true` in CI to promote unignored high-severity findings from warnings to failures. By default, only unignored critical findings fail the policy gate.

## Validation Commands

```powershell
pnpm lint
pnpm typecheck
pnpm test:contracts
pnpm test:web
python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70
python scripts/security/security_sca.py validate-ignores --ignore-file .security/vulnerability-ignores.yml
.\scripts\security\generate_sbom_sca.ps1
```

The local SBOM script requires `syft`, `grype`, and Docker if Docker SBOMs are not skipped. CI installs Syft and Grype automatically.

CI also validates generated artifacts with JSON checks equivalent to:

```bash
jq -e '.bomFormat == "CycloneDX"' sbom/apps-web.cdx.json
jq -e '.bomFormat == "CycloneDX"' sbom/crawler-python.cdx.json
jq -e '(.components | length) > 0' sbom/apps-web.cdx.json
jq -e '(.components | length) > 0' sbom/crawler-python.cdx.json
jq -e '.' security/vulnerability-scan.json
test -s security/sbom-evidence.md
```

## Expected CI Artifacts

The `security-sbom-sca` job uploads a `security-sbom-sca` artifact containing all SBOM JSON files, the combined vulnerability scan JSON, and the Markdown evidence report.

## Policy

- Unignored critical vulnerabilities fail CI.
- High vulnerabilities are reported as warnings.
- Malformed, expired, or unjustified ignores fail CI.
- No secrets, tokens, database URLs, or full environment dumps are printed by the security job.

## Known Limitations

The scanner results depend on public vulnerability databases available to the CI runner. Docker SBOMs are generated after image builds and are not produced if Docker itself fails before the SBOM step.

## Future Hardening Steps

- Add signed artifact attestations.
- Add SPDX export if required by downstream consumers.
- Promote selected high vulnerabilities to failure once the baseline is stable.
- Add dependency-review comments on pull requests.
