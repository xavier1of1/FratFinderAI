# FratFinderAI Documentation

This folder contains the public documentation set for FratFinderAI. It is intentionally small: new users should be able to understand what the project does, how to run it locally, how the major systems fit together, and what security controls are in place.

## Start Here

- [Getting Started](./GETTING_STARTED.md) - local setup, services, workers, and validation commands.
- [Architecture](./architecture/README.md) - system diagrams and core runtime models.
- [Project Report](./reports/PROJECT_REPORT.md) - concise portfolio-style overview of the full platform.
- [Security Sprint Report](./security/security-sprint-comprehensive-report.md) - SBOM/SCA, SSRF-safe fetching, RBAC, and audit logging.
- [Public Release Checklist](./release/PUBLIC_RELEASE_CHECKLIST.md) - remaining steps before LinkedIn/public open-source release.

## Directory Map

```text
docs/
  architecture/  Current architecture and reliability models.
  reports/       Public project overview and concise PDF/HTML summary.
  security/      Security controls, validation evidence, and operator access docs.
  release/       Public-release and contribution-readiness checklists.
```

## What Was Removed

Older internal worklogs, stress-run dumps, one-off benchmark artifacts, and dated implementation planning notes were removed from the public documentation tree. The repository now keeps public docs focused on stable onboarding, architecture, security, and release-readiness material.
