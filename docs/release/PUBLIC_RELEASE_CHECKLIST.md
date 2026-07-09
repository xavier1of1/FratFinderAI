# Public Release Checklist

Use this checklist before posting FratFinderAI publicly on LinkedIn or opening the repository for outside contributors.

## Already Completed In This Cleanup Pass

- Public docs were reduced to focused onboarding, architecture, reports, security, and release-readiness material.
- Stale internal markdown worklogs and generated benchmark/stress artifacts were removed from the public docs tree.
- Local interview-prep material was added to `.gitignore`.
- Runtime logs, coverage files, generated SBOM/security output, TypeScript build info, Python egg-info, and temporary scripts were added to `.gitignore`.

## Must Do Before Publishing

- Add an explicit open-source license.
- Decide whether this remains a portfolio source drop or accepts external contributions immediately.
- Add `CONTRIBUTING.md` with setup, branch, testing, and review expectations.
- Add `CODE_OF_CONDUCT.md` if accepting contributions.
- Add a `SECURITY.md` vulnerability disclosure policy.
- Run a final secret scan on a clean clone, not just the current worktree.
- Run CI in GitHub and confirm SBOM/SCA artifacts upload successfully.
- Confirm `.env.example` contains no real credentials or private endpoints.
- Confirm Docker images build from a clean clone.
- Confirm the README quick start works on a clean machine.
- Review screenshots/PDFs for private names, paths, tokens, or sensitive operational details.
- Replace or remove any remaining personal interview/demo documents from tracked files.
- Decide whether generated PDF/HTML overview artifacts should stay tracked or be linked from releases.

## Recommended LinkedIn Launch Prep

- Prepare a short demo clip showing intake, agent ops, review queue, and security evidence.
- Link directly to `README.md`, `docs/reports/PROJECT_REPORT.md`, and `docs/security/security-sprint-comprehensive-report.md`.
- Be clear that FratFinderAI is a portfolio/open-source automation platform and not an official fraternity or university data source.
- Mention the core engineering themes: evidence orchestration, safe automation, queue reliability, search-provider observability, and DevSecOps controls.

## Known Caveats To Disclose

- Local development can disable operator auth, but production mode is designed to fail closed.
- Search-provider behavior depends on local/network conditions; SearXNG should be treated as an operational dependency.
- Some demo seed data may be synthetic or stale and should not be treated as authoritative public chapter data.
- No open-source license is present yet, so external use/contribution rights are undefined until one is added.
