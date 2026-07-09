# FratFinderAI - Comprehensive Project Report

> Generated: 2026-06-09 | Current version: 3.0.4

## 1. Executive Summary

FratFinderAI is a production-style data automation platform for discovering, verifying, and enriching fraternity chapter records. It combines a Next.js operator console, a Python crawler/orchestration service, PostgreSQL-backed queues, strict provenance, search-provider reliability controls, and security evidence automation.

The core problem is harder than "scrape a directory." National fraternity directories, school recognition pages, chapter websites, social profiles, conduct pages, suspended-chapter lists, and CRM-ready contact records all disagree in subtle ways. FratFinderAI treats that as an evidence-ranking and workflow problem:

- discover likely national and school sources
- extract raw chapter candidates
- normalize fraternity, school, chapter, and contact identity
- verify chapter activity/status from official evidence
- enrich website, Instagram, and email only when safety gates pass
- preserve provenance and route ambiguity to review instead of silently writing bad data

The system is built for an operator persona: someone who needs to launch crawls, monitor queues, inspect evidence, resolve ambiguous records, and trust that unsafe writes are blocked by design.

## 2. Current Project Snapshot

| Area | Current State |
| --- | --- |
| Version | `3.0.4` |
| Web app | Next.js 14 App Router dashboard and API routes |
| Crawler | Python service with LangGraph request/field-job orchestration |
| Database | PostgreSQL with 37 versioned migrations through `0037_operator_audit_events.sql` |
| Queue model | PostgreSQL `SKIP LOCKED` field-job queue with typed queue states and worker phase metadata |
| Search | SearXNG-first provider chain with endpoint failover, health snapshots, attempt history, and managed-provider smoke harness |
| Status model | Campus recognition/status engine with school-first authority, conclusive absence logic, conflict flags, and evidence bundles |
| Security | SBOM/SCA evidence pipeline, SSRF-safe outbound fetch policy, operator RBAC, and audit logging |
| Validation | Latest local security gate: 589 crawler tests, 47 web tests, 5 contract tests, 2 integration tests |

## 3. Repository Structure

```text
apps/web/
  Next.js operator dashboard, API routes, RBAC wrappers, repositories

services/crawler/
  Python crawler, request graphs, field-job graph, status engine, search client,
  SSRF-safe HTTP wrapper, queue workers, tests

packages/contracts/
  Shared TypeScript/schema contracts

infra/
  Docker Compose, Postgres migrations, seed/smoke scripts

docs/
  System reports, validation reports, security evidence, benchmark analysis

scripts/security/
  SBOM/SCA generation, vulnerability policy, evidence rendering
```

## 4. What The System Does

### 4.1 National And School Source Discovery

FratFinderAI starts from a fraternity name or known source and resolves candidate national directories, school pages, chapter pages, and supporting evidence. Source discovery is not treated as a simple search result ranking problem. The platform separates:

- search relevance
- source authority
- page role
- national-directory capability
- school officialness
- currentness
- parse completeness
- contact-specificity safety

This prevents high-ranking but unsafe pages from becoming canonical crawl roots or contact evidence.

### 4.2 Chapter Extraction

The crawler handles several source shapes:

- table and card directories
- script/JSON embedded records
- locator APIs and map-style directories
- chapter detail pages
- national directory outbound links
- school FSL/RSO pages
- supporting pages used only as evidence

Adapters extract raw chapter candidates, then downstream promotion logic decides whether a candidate is strong enough to become a chapter row, a provisional review item, or a rejected artifact.

### 4.3 Campus Recognition Status

The project now uses a campus-recognition-first status model. Official school evidence outranks national pages for final active/inactive status because the product definition of "active" requires current school recognition.

The status subsystem includes:

- `CampusStatusIndex` style source/zones/evidence modeling
- official-school source classification
- status-zone parsing to prevent page-wide keyword leakage
- no-Greek-life policy handling
- conclusive absence rules for complete official rosters
- national-directory capability profiles
- conflict flags and explainable decision traces
- review routing for ambiguous or contradictory evidence

Important distinction: probation/probationary recognition does not automatically mean inactive. Interim suspension, suspended, closed, dismissed, expelled, unrecognized, and school fraternity bans are negative signals.

### 4.4 Contact Enrichment

Contact enrichment is status-first and provenance-first. Website, Instagram, and email fields are not written just because a search result exists.

Safety gates include:

- active/review-approved status dependency before contact writes
- school/chapter identity verification
- chapter-local evidence requirement for school pages
- generic national/HQ contact rejection
- generic school-office contact rejection
- wrong-school and wrong-fraternity rejection
- reused-host safeguards across multiple schools
- evidence-first Instagram extraction before residual search

If evidence is incomplete or unsafe, jobs are blocked, deferred, or routed to review instead of producing false positives.

## 5. Architecture Flow

```text
Operator / API intake
  -> Request supervisor
  -> Source discovery and source-quality gates
  -> National/source crawl graph
  -> Raw chapter candidate extraction
  -> School-conditioned promotion
  -> Campus status verification
  -> Queue-backed contact enrichment
  -> Provenance-backed canonical writes or review items
  -> Dashboard, audits, reports, and metrics
```

The platform deliberately keeps parsing, orchestration, database writes, and React UI concerns separated:

- React components do not own crawler/parsing/DB logic.
- LangGraph nodes orchestrate; adapters parse.
- Repository modules own database boundaries.
- Security and provider controls are centralized.
- Evidence is stored with decision traces rather than hidden in logs.

## 6. Web Operator Console

The web app is a Next.js dashboard backed by repository modules and consistent API envelopes.

Key pages:

| Route | Purpose |
| --- | --- |
| `/` | System overview and KPIs |
| `/chapters` | Filterable chapter table and coverage view |
| `/fraternity-intake` | New fraternity intake and source confirmation workflow |
| `/review-items` | Human triage for ambiguous or unsafe records |
| `/runs` | Crawl run history |
| `/benchmarks` | Benchmark and evaluation runs |
| `/agent-ops` | Queue/runtime operational visibility |
| `/nationals` | National profile/source registry |
| `/crm` | Outreach/campaign workflow |

There are 44 API route files under `apps/web/src/app/api`. Mutating routes are protected with operator RBAC, and read-only dashboard APIs are protected with read-only operator access unless explicitly public.

## 7. Python Crawler And Orchestration

The crawler service is organized into focused subsystems:

| Module | Role |
| --- | --- |
| `orchestration/` | Request graphs, field-job graph, supervisor graph, state transitions |
| `adapters/` | Source-format-specific extraction |
| `status/` | Campus status engine and decision models |
| `search/` | Provider catalog, SearXNG health, search client, provider attempts |
| `social/` | Instagram extraction, identity scoring, candidate bank, sweeps |
| `security/` | SSRF-safe URL fetching and SCA policy helpers |
| `db/` | Repository and connection boundaries |
| `normalization/` | Name/school/state normalization |

LangGraph is used as an orchestration backbone for request and field-job flows, while parsing remains in adapters and helper modules. This keeps graph state transitions inspectable without burying source-specific scraping logic inside graph nodes.

## 8. Queue And Worker Model

FratFinderAI uses PostgreSQL-backed queues with explicit typed states rather than an external queue broker.

Current queue states include:

```text
actionable
deferred
blocked_provider
blocked_dependency
blocked_repairable
blocked_invalid
```

Operational improvements added after live benchmarking:

- worker phase metadata: polling, preflight, triage, claiming, executing, aggregating, sleeping
- priority ordering that respects verification lanes before contact jobs
- dependency blockers that do not churn as hot actionable work
- status dependency reason families instead of one generic blocker
- evidence refresh path for official school evidence
- safer retry/recovery of failed jobs

The result is a queue model optimized for "make blocked work understandable" instead of "keep retrying until something writes."

## 9. Search Provider Reliability

Search is treated as a dependency manager, not the product. The current strategy preserves provider-chain orchestration while improving reliability and observability.

Current search policy:

- SearXNG is the primary controllable provider.
- SearXNG supports ordered endpoint failover.
- Bing HTML and DuckDuckGo HTML are fallback HTML providers.
- Serper and Tavily are opt-in until smoke cohorts prove value.
- Provider attempts are recorded with context, endpoint, latency, result counts, and failure classes.
- Degraded mode continues authoritative-only work and avoids false no-candidate conclusions during provider outages.

Important provider failure classes include:

```text
connection_refused
dns_error
timeout
json_disabled
engine_unresponsive
rate_limited_429
challenge_or_anomaly
parse_empty
low_signal_fallback
provider_unavailable
```

## 10. Security Controls

The security sprint added three professional controls.

### 10.1 SBOM And SCA Evidence

The `security-sbom-sca` workflow and local scripts generate CycloneDX SBOMs for:

- web dependencies
- crawler dependencies
- web Docker image
- crawler Docker image

The scan policy fails on unignored critical vulnerabilities, warns on highs by default, rejects malformed/expired ignores, and produces Markdown evidence.

### 10.2 SSRF-Safe Outbound Fetching

Crawler-discovered URLs pass through `fratfinder_crawler.security.url_safety`.

The wrapper:

- allows only HTTP/HTTPS
- resolves DNS before requests
- blocks local/private/link-local/multicast/reserved/metadata targets
- revalidates redirect targets
- caps body size and content type
- strips sensitive auth/cookie/API-key headers
- disables ambient process auth/proxy inheritance

Trusted configured providers, including local SearXNG, remain separate from untrusted crawler URL fetching.

### 10.3 Operator RBAC And Audit Logging

The operator API now has:

- roles: admin, operator, analyst
- environment-token login and bearer-token script access
- HttpOnly SameSite session cookies
- fail-closed production behavior
- route coverage tests for mutating/read-only API routes
- `operator_audit_events` table for allowed, denied, and error outcomes
- production-safe protected-route error responses with request IDs

## 11. Database And Evidence Model

The database has 37 migrations, from initial crawl tables through status engines, provider attempts, CRM, search health cache, promotion recovery, and operator audit events.

Key persistent concepts:

- fraternities
- sources and verified/national profiles
- crawl requests and request events
- crawl runs
- chapters
- chapter evidence and provenance
- field jobs and graph runs
- review items
- status sources, zones, evidence, and decisions
- search provider attempts and health cache
- campaign/CRM tables
- operator audit events

The core design principle is that decisions should be explainable from durable evidence, not only from transient logs.

## 12. Validation Evidence

Latest security sprint validation:

```text
pnpm.cmd lint                                             PASS
pnpm.cmd typecheck                                        PASS
pnpm.cmd test:contracts                                   PASS, 5 tests
pnpm.cmd test:web                                         PASS, 47 tests
python -m pytest services/crawler/... --cov-fail-under=70 PASS, 589 tests, 70.84% coverage
python -m pytest tests/integration -m integration          PASS, 2 tests
```

Security artifact generation:

```text
Syft 1.44.0
Grype 0.112.0
apps-web CycloneDX components: 145
crawler-python CycloneDX components: 1
docker-web CycloneDX components: 1164
docker-crawler CycloneDX components: 788
```

Two temporary Python critical vulnerability exceptions remain documented and expire on `2026-06-30`.

## 13. Engineering Decisions Worth Highlighting

1. School recognition is final status authority.
2. Search is residual evidence gathering, not a source of truth.
3. Contact writes require status/provenance/specificity gates.
4. Queue states are explicit, inspectable, and repairable.
5. Provider failures are classified by failure class and endpoint.
6. Security controls are automated, tested, and documented.
7. The web app is an operator surface, not a crawler runtime.
8. Ambiguity routes to review instead of canonical data pollution.

## 14. Demo Talking Points

For a technical recruiter or engineering interviewer, the strongest framing is:

- This is an end-to-end data automation platform, not a toy scraper.
- It handles messy real-world source disagreement through evidence ranking and conservative write gates.
- It uses queue-backed orchestration and durable provenance to make automation auditable.
- It has real DevSecOps controls: SBOM/SCA, SSRF protection, RBAC, and audit logs.
- It was improved through benchmark-driven debugging, not only feature prompts.

## 15. Known Limitations And Next Work

- Hosted CI artifact proof is still needed for final SBOM/SCA release acceptance.
- Some queue cohorts remain blocked until official status or school evidence is refreshed.
- SearXNG primary/rescue endpoints need stable engine allowlists in long-running production environments.
- Operator auth currently uses environment tokens, not OAuth/SAML/IAP.
- Audit log UI/export/retention policy is future work.
- More gold-set status cases are needed before making any public 99% accuracy claim.

## 16. Best Supporting Documents

| Document | Purpose |
| --- | --- |
| `docs/security/security-sprint-comprehensive-report.md` | Security and DevSecOps evidence |
| `docs/security/phase-2-ssrf-validation-report.md` | SSRF implementation proof |
| `docs/security/phase-3-operator-rbac-validation-report.md` | RBAC/audit implementation proof |
| `docs/architecture/status-verification-model.md` | Status verification and accuracy model |
| `docs/architecture/search-provider-reliability.md` | Search-provider reliability research |
| `CHANGELOG.md` | Full implementation timeline |

## 17. One-Sentence Summary

FratFinderAI is a security-aware, evidence-first automation platform that crawls messy fraternity and school web ecosystems, verifies chapter status from authoritative sources, enriches contact data through safe queue-backed workflows, and preserves the provenance needed to trust every write.
