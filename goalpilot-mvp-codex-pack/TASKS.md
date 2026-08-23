# GoalPilot MVP Execution Plan

**Audience:** Codex and GoalPilot contributors  
**Status:** Authoritative sequential implementation plan  
**Version:** 1.0  
**Baseline date:** 2026-08-23  

This file converts `PRD.md`, `STACK.md`, and `CONVENTIONS.md` into small, verifiable implementation milestones. Codex must execute milestones in order unless a milestone explicitly identifies safe parallel work. A milestone is complete only when its exit gate and evidence requirements are satisfied.

## 0. How to use this file

### Required reading before every run

1. `PRD.md`
2. `STACK.md`
3. `CONVENTIONS.md`
4. The current milestone in this file
5. The current Git diff and test status

### Status values

Use one of these markers in the milestone index:

- `[ ]` Not started
- `[-]` In progress
- `[x]` Verified complete
- `[!]` Blocked by a documented external dependency

Only a human integrator or an explicitly assigned integration agent may change a milestone to `[x]` after reviewing the evidence.

### Execution rules

- Complete one milestone before beginning a dependent milestone.
- Do not broaden the MVP scope.
- Do not add real financial connectivity.
- Do not change financial policy silently.
- Add tests with the implementation, not later.
- Keep `main` releasable.
- Use a separate branch or worktree per milestone or tightly related submilestone.
- Record exact commands and results in the pull request.
- Use synthetic data only.
- When blocked, document the smallest required decision in the milestone notes and continue only with genuinely independent work.

### Required repository scripts

By the end of Milestone 2, the root package must expose:

```text
pnpm dev
pnpm build
pnpm format
pnpm format:check
pnpm lint
pnpm typecheck
pnpm test
pnpm test:unit
pnpm test:integration
pnpm test:e2e
pnpm test:a11y
pnpm test:coverage
pnpm db:generate
pnpm db:migrate
pnpm db:reset:test
pnpm infra:test
pnpm infra:synth
pnpm security:scan
pnpm sbom
pnpm verify
```

`pnpm verify` must eventually run all release-blocking local checks that do not require a deployed environment.

## 1. Milestone index

| Status | Milestone | Product increment | Depends on |
|---|---:|---|---|
| [ ] | M00 | Repository contract and compatibility proof | None |
| [ ] | M01 | Monorepo and local developer environment | M00 |
| [ ] | M02 | Quality, CI, and security baseline | M01 |
| [ ] | M03 | PostgreSQL schema and persistence foundation | M02 |
| [ ] | M04 | GoalPilot visual system and application shell | M02 |
| [ ] | M05 | Financial primitives and contribution schedules | M02 |
| [ ] | M06 | Vehicle catalog, eligibility policy, and projections | M05 |
| [ ] | M07 | Fastify API foundation and stateless preview | M03, M06 |
| [ ] | M08 | Cognito authentication and secure sessions | M07 |
| [ ] | M09 | Authenticated goal lifecycle and ownership | M08 |
| [ ] | M10 | Goal builder and vehicle-comparison experience | M04, M07, M09 |
| [ ] | M11 | Simulated account, immutable plan versions, and ledger | M09, M10 |
| [ ] | M12 | Dashboard, progress, and activity experience | M11 |
| [ ] | M13 | Demo autopilot and scheduled interest processing | M11 |
| [ ] | M14 | Goal completion, history, export, and deletion | M12, M13 |
| [ ] | M15 | Security hardening and threat-model verification | M08-M14 |
| [ ] | M16 | AWS CDK and nonproduction cloud deployment | M15 |
| [ ] | M17 | Full end-to-end, accessibility, and recovery validation | M16 |
| [ ] | M18 | Final release audit and portfolio demonstration | M17 |

---

# M00. Repository contract and compatibility proof

## Objective

Establish the repository as the source of truth and prove that the pinned stack can be installed together before application development begins.

## Tasks

- [ ] **M00-T01:** Create or select the dedicated GoalPilot repository.
- [ ] **M00-T02:** Copy `PRD.md`, `STACK.md`, `CONVENTIONS.md`, `TASKS.md`, and `MVP_PROJECT_REPORT.md` into the repository root.
- [ ] **M00-T03:** Add a root `AGENTS.md` that instructs Codex to follow these files in authority order.
- [ ] **M00-T04:** Create `docs/adr/`, `docs/security/`, `docs/evidence/`, and `docs/operations/`.
- [ ] **M00-T05:** Create `docs/adr/0001-simulation-first-mvp.md`.
- [ ] **M00-T06:** Create `docs/adr/0002-lambda-fastify-neon.md`.
- [ ] **M00-T07:** Create a temporary compatibility workspace using every direct runtime and development dependency pinned in `STACK.md`.
- [ ] **M00-T08:** Install with exact versions and a committed lockfile.
- [ ] **M00-T09:** Compile one minimal React entry point, one minimal Fastify entry point, one Drizzle schema, and one CDK stack.
- [ ] **M00-T10:** Record any required version correction in `STACK.md` with the exact failing command and reason.
- [ ] **M00-T11:** Confirm licenses of all direct dependencies are acceptable for the project.

## Deliverable

A dedicated repository containing the approved planning files, a lockfile, and a passing compatibility proof. No product feature is required.

## Required verification

```bash
node --version
pnpm --version
pnpm install --frozen-lockfile
pnpm typecheck
pnpm build
pnpm infra:synth
```

## Exit gate

- All direct dependencies install together.
- React, Fastify, Drizzle, and CDK compile.
- The lockfile and `STACK.md` agree.
- No package substitution occurred without documentation.
- Repository authority order is documented.

## Evidence

- Compatibility CI run
- Lockfile commit
- ADRs 0001 and 0002
- Direct-dependency license inventory

---

# M01. Monorepo and local developer environment

## Objective

Create a one-command local environment for web, API, PostgreSQL, and tests.

## Tasks

- [ ] **M01-T01:** Create the pnpm workspace and package boundaries defined in `STACK.md`.
- [ ] **M01-T02:** Add root TypeScript, ESLint, Prettier, and EditorConfig configuration.
- [ ] **M01-T03:** Create `apps/web` using React, Vite, and TypeScript.
- [ ] **M01-T04:** Create `apps/api` with a reusable Fastify factory, local Node entry point, and Lambda entry point.
- [ ] **M01-T05:** Create empty package public APIs for contracts, domain, data access, auth, observability, provider ports, provider simulators, test support, and UI.
- [ ] **M01-T06:** Add Docker Compose with PostgreSQL 17 for local development and tests.
- [ ] **M01-T07:** Add the Dev Container configuration with Node, pnpm, PostgreSQL client, AWS CLI, CDK, Git, and required browser-test dependencies.
- [ ] **M01-T08:** Add typed environment parsing and `.env.example` with nonsecret placeholders.
- [ ] **M01-T09:** Implement `/health` without dependencies and `/ready` with a database readiness placeholder.
- [ ] **M01-T10:** Add a `scripts/doctor.ts` command that checks Node, pnpm, Docker, environment, ports, and database connectivity.
- [ ] **M01-T11:** Write local setup instructions in `README.md`.

## Deliverable

A clean checkout can start the React shell, Fastify API, and local PostgreSQL instance.

## Required verification

```bash
pnpm install --frozen-lockfile
docker compose up -d postgres
pnpm db:migrate
pnpm dev
pnpm doctor
curl http://localhost:3000/health
curl http://localhost:3000/ready
```

## Exit gate

- A second developer can follow only `README.md` and start the environment.
- Web and API hot reload work.
- No secret is required for local startup.
- The API factory is reusable by local and Lambda entry points.
- Every package passes an empty strict type check.

## Evidence

- Setup transcript from a clean environment
- Screenshot of web shell
- Health and readiness responses

---

# M02. Quality, CI, and security baseline

## Objective

Make quality and security failures visible before financial-domain code is written.

## Tasks

- [ ] **M02-T01:** Implement all required root scripts listed at the top of this file.
- [ ] **M02-T02:** Configure Vitest workspaces and coverage reporting.
- [ ] **M02-T03:** Configure Testing Library and jsdom for React tests.
- [ ] **M02-T04:** Configure Playwright with local web and API startup.
- [ ] **M02-T05:** Configure PostgreSQL integration-test lifecycle.
- [ ] **M02-T06:** Add GitHub Actions for format, lint, typecheck, unit tests, integration tests, builds, and artifact retention.
- [ ] **M02-T07:** Add CodeQL, Dependabot, dependency review, Gitleaks, production dependency audit, and CycloneDX SBOM generation.
- [ ] **M02-T08:** Add branch-protection setup instructions and `CODEOWNERS`.
- [ ] **M02-T09:** Create `docs/security/data-inventory.md` and classify all currently planned data.
- [ ] **M02-T10:** Create the initial threat model covering authentication, IDOR, injection, XSS, CSRF, secret leakage, log leakage, rate abuse, supply chain, and misleading financial output.
- [ ] **M02-T11:** Add a seeded fake-secret fixture that proves secret scanning fails without exposing a real secret.
- [ ] **M02-T12:** Add a CI artifact containing the SBOM and test summaries.

## Deliverable

Every pull request receives automatic correctness, security, dependency, and build feedback.

## Required verification

```bash
pnpm format:check
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm security:scan
pnpm sbom
```

## Exit gate

- CI passes on the baseline branch.
- Lint produces zero warnings.
- The fake-secret test proves the scanner blocks a commit or CI run.
- SBOM is generated as CycloneDX.
- Threat model and data inventory are reviewed by both developers.

## Evidence

- Successful CI URL
- SBOM artifact
- Threat-model review record
- Branch-protection screenshot or configuration record

---

# M03. PostgreSQL schema and persistence foundation

## Objective

Create the normalized relational foundation and migration discipline before feature APIs.

## Tasks

- [ ] **M03-T01:** Implement initial tables: users, goals, vehicle assumption versions, vehicle assumptions, plan versions, simulated accounts, ledger entries, schedule occurrences, idempotency records, audit events, and data requests.
- [ ] **M03-T02:** Add foreign keys, unique constraints, ownership fields, checks, timestamps, and indexes.
- [ ] **M03-T03:** Store all money as `bigint` cents.
- [ ] **M03-T04:** Create the initial reviewed SQL migration.
- [ ] **M03-T05:** Create deterministic illustrative assumption seed data for the four MVP vehicles.
- [ ] **M03-T06:** Implement transaction and database-client factories for local Postgres and Neon.
- [ ] **M03-T07:** Add repository interfaces and Drizzle adapters without HTTP dependencies.
- [ ] **M03-T08:** Add migration tests against an empty database.
- [ ] **M03-T09:** Add constraint tests for duplicate idempotency keys, ledger occurrence IDs, invalid money, and broken ownership references.
- [ ] **M03-T10:** Create `docs/architecture/ERD.md` with a Mermaid ERD and record-level invariants.

## Deliverable

A migrated database with deterministic assumptions and tested persistence primitives.

## Required verification

```bash
pnpm db:reset:test
pnpm db:migrate
pnpm test:integration --filter data-access
```

## Exit gate

- Empty-database migration passes.
- Reapplying migrations is safe.
- Constraint tests fail for invalid records.
- No network call occurs inside a database transaction.
- The ERD matches the migration.

## Evidence

- Migration SQL
- Schema test output
- ERD review

---

# M04. GoalPilot visual system and application shell

## Objective

Establish a polished, accessible interface before feature pages diverge visually.

## Safe parallelization

M04 may proceed in parallel with M05 after M02 is complete. It must use static fixtures until API contracts are ready.

## Tasks

- [ ] **M04-T01:** Define design tokens for British racing green, ivory, gold, charcoal, semantic status colors, spacing, radii, shadow, typography, and motion.
- [ ] **M04-T02:** Verify AA contrast for every token pair used in critical flows.
- [ ] **M04-T03:** Install only the approved shadcn/ui components.
- [ ] **M04-T04:** Build accessible Button, FormField, MoneyInput, DateInput, Select, Card, Alert, Dialog, Progress, DataTable, Skeleton, EmptyState, and ErrorState wrappers.
- [ ] **M04-T05:** Create responsive public and authenticated shells.
- [ ] **M04-T06:** Create navigation, footer, skip link, error boundary, not-found route, loading route, and toast region.
- [ ] **M04-T07:** Build the landing page from static content.
- [ ] **M04-T08:** Add theme and component Storybook alternative only if needed; prefer a lightweight internal `/design-system` development route rather than another dependency.
- [ ] **M04-T09:** Add keyboard, reduced-motion, responsive, and axe tests for the shell and primitives.
- [ ] **M04-T10:** Capture mobile and desktop visual evidence.

## Deliverable

A branded, responsive, accessible GoalPilot shell and reusable visual primitives.

## Required verification

```bash
pnpm --filter web test
pnpm test:a11y --filter shell
pnpm test:e2e --grep "application shell"
pnpm --filter web build
```

## Exit gate

- Critical primitives have accessible names and focus behavior.
- No serious or critical axe finding exists.
- Layout works at 360, 768, 1024, and 1440 pixels.
- Design uses no second component system.
- Financial charts have accessible fallback patterns defined.

## Evidence

- Mobile and desktop screenshots
- Contrast report
- Accessibility test output

---

# M05. Financial primitives and contribution schedules

## Objective

Implement pure, exact, deterministic building blocks for all later calculations.

## Tasks

- [ ] **M05-T01:** Implement branded `MoneyCents`, `BasisPoints`, `CalendarDate`, `GoalId`, and version value types.
- [ ] **M05-T02:** Implement safe constructors and Zod boundary schemas.
- [ ] **M05-T03:** Configure Decimal precision and rounding policy in one module.
- [ ] **M05-T04:** Implement weekly, biweekly, and monthly contribution schedule generation.
- [ ] **M05-T05:** Define month-end behavior explicitly.
- [ ] **M05-T06:** Implement zero-interest required-contribution calculation.
- [ ] **M05-T07:** Implement contribution principal totals and calendar-period counts.
- [ ] **M05-T08:** Implement daily deposit-interest accrual and monthly posting rules using an explicit processing date.
- [ ] **M05-T09:** Add golden tests for simple hand-calculated cases.
- [ ] **M05-T10:** Add boundary tests for leap years, short months, same-day deadlines, past dates, and zero values.
- [ ] **M05-T11:** Add property tests for schedule ordering, no duplicate dates, and monotonic principal.

## Deliverable

A pure domain package that can produce an exact recurring schedule, zero-interest plan, and deterministic deposit-interest ledger.

## Required verification

```bash
pnpm test:unit --filter domain
pnpm test:coverage --filter domain
pnpm typecheck
```

## Exit gate

- Domain has no framework, database, cloud, environment, or clock dependency.
- Hand-calculated golden cases pass.
- Every schedule cadence and boundary is tested.
- Domain line and branch coverage meet the threshold in `CONVENTIONS.md`.

## Evidence

- Golden fixture file
- Coverage report
- Domain dependency audit

---

# M06. Vehicle catalog, eligibility policy, and projections

## Objective

Deliver the complete deterministic product comparison engine for cash, HYSA, CD ladders, and Treasury-bill ladders.

## Tasks

- [ ] **M06-T01:** Define versioned vehicle-assumption contracts.
- [ ] **M06-T02:** Implement `StaticRateProvider` behind the `RateProvider` port.
- [ ] **M06-T03:** Implement plain-cash projection.
- [ ] **M06-T04:** Implement variable-rate HYSA projection using the versioned illustrative APY.
- [ ] **M06-T05:** Implement CD ladder cash-flow and lock/penalty policy.
- [ ] **M06-T06:** Implement Treasury-bill ladder maturity-alignment policy.
- [ ] **M06-T07:** Implement liquidity and capital-preservation eligibility gates.
- [ ] **M06-T08:** Implement required recurring contribution search to the nearest cent.
- [ ] **M06-T09:** Implement deterministic ranking from `PRD.md`.
- [ ] **M06-T10:** Return a full result trace including zero-interest baseline, principal, interest, ending balance, completion date, eligibility, rejection code, and assumption version.
- [ ] **M06-T11:** Add golden fixtures for each vehicle and rejection path.
- [ ] **M06-T12:** Add invariant tests: equal input equality, interest separated from principal, ineligible products unranked, stale assumptions rejected, and higher contributions never reduce deterministic ending balance.
- [ ] **M06-T13:** Create `docs/domain/CALCULATION_SPEC.md` and `docs/domain/VEHICLE_POLICY.md`.

## Deliverable

A pure comparison engine that produces complete, explainable results for all MVP vehicles.

## Required verification

```bash
pnpm test:unit --filter domain
pnpm test:coverage --filter domain
pnpm typecheck
```

## Exit gate

- Every vehicle has approved golden cases.
- Every policy gate has positive and negative tests.
- No result uses a live or unlabeled rate.
- Ranking matches the PRD exactly.
- The calculation specification matches implementation.

## Evidence

- Golden results
- Policy test matrix
- Calculation specification review

---

# M07. Fastify API foundation and stateless preview

## Objective

Expose the calculation engine through a secure, versioned, documented API without authentication dependency for public previews.

## Tasks

- [ ] **M07-T01:** Create shared input and output Zod schemas.
- [ ] **M07-T02:** Implement centralized application-error mapping.
- [ ] **M07-T03:** Implement request IDs and Pino redaction.
- [ ] **M07-T04:** Configure Helmet, CORS allowlist, request-size limits, and supplemental rate limits.
- [ ] **M07-T05:** Implement `GET /health`, `GET /ready`, and `GET /api/v1/vehicle-catalog`.
- [ ] **M07-T06:** Implement `POST /api/v1/previews` using the pure engine.
- [ ] **M07-T07:** Generate OpenAPI from route schemas.
- [ ] **M07-T08:** Add invalid-body, unknown-field, oversized-body, invalid-origin, and rate-limit tests.
- [ ] **M07-T09:** Add API-to-engine golden consistency tests.
- [ ] **M07-T10:** Confirm the same Fastify app works through local server and Lambda adapter.

## Deliverable

A stateless preview API that returns an auditable comparison and never persists anonymous financial inputs.

## Required verification

```bash
pnpm test:unit --filter api
pnpm test:integration --filter previews
pnpm --filter api build
pnpm typecheck
```

## Exit gate

- Preview returns all required fields.
- Invalid requests return stable 400 responses.
- Unexpected errors expose no internal details.
- Logs contain no request body, financial amount, token, or secret.
- Local and Lambda adapters pass the same contract tests.

## Evidence

- OpenAPI artifact
- Preview response fixture
- Redaction test output

---

# M08. Cognito authentication and secure sessions

## Objective

Implement production-structured identity without exposing OAuth tokens to browser JavaScript.

## Tasks

- [ ] **M08-T01:** Define Cognito CDK constructs for user pool, app client, callback URLs, password policy, email verification, and optional TOTP MFA.
- [ ] **M08-T02:** Implement `/auth/login` with state, nonce, and PKCE.
- [ ] **M08-T03:** Persist short-lived, single-use OAuth transactions.
- [ ] **M08-T04:** Implement `/auth/callback` and server-side code exchange.
- [ ] **M08-T05:** Verify token signature, issuer, client ID, token use, expiration, state, and nonce.
- [ ] **M08-T06:** Create opaque hashed server-side sessions with idle and absolute expiration.
- [ ] **M08-T07:** Set secure session and CSRF cookies.
- [ ] **M08-T08:** Implement CSRF and Origin validation for mutations.
- [ ] **M08-T09:** Implement `/auth/logout`, session revocation, and cookie expiration.
- [ ] **M08-T10:** Implement `GET /api/v1/me`.
- [ ] **M08-T11:** Create an isolated local test-auth adapter that cannot compile or deploy in nonlocal environments.
- [ ] **M08-T12:** Add tests for reused state, wrong nonce, expired transaction, session fixation, expired session, CSRF failure, origin failure, and logout.

## Deliverable

A secure authenticated browser session and user profile boundary.

## Required verification

```bash
pnpm test:unit --filter auth
pnpm test:integration --filter auth
pnpm test:e2e --grep authentication
pnpm typecheck
```

## Exit gate

- Access and refresh tokens are absent from localStorage, sessionStorage, URLs after callback, and logs.
- OAuth transaction reuse fails.
- Session rotates after authentication.
- State-changing requests without valid CSRF fail.
- Development auth is rejected outside local/test.

## Evidence

- Auth sequence diagram
- Cookie inspection screenshot with values redacted
- Negative test results

---

# M09. Authenticated goal lifecycle and ownership

## Objective

Persist user-owned goals with server-enforced isolation, idempotency, and optimistic concurrency.

## Tasks

- [ ] **M09-T01:** Implement internal user provisioning on first authenticated request.
- [ ] **M09-T02:** Implement authenticated goal repositories with ownership in every SQL predicate.
- [ ] **M09-T03:** Implement `GET`, `POST`, `PATCH`, and `DELETE` goal endpoints.
- [ ] **M09-T04:** Enforce one active simulated goal per user.
- [ ] **M09-T05:** Implement `Idempotency-Key` behavior for create.
- [ ] **M09-T06:** Implement goal version or ETag checks for updates.
- [ ] **M09-T07:** Append audit events for create, update, pause, resume, complete, archive, and delete.
- [ ] **M09-T08:** Add same-user positive tests and cross-user 404 tests for every goal operation.
- [ ] **M09-T09:** Test idempotent replay and conflicting key reuse.
- [ ] **M09-T10:** Add pagination for archived goal history.

## Deliverable

Authenticated users can manage their own goal without learning whether another user's goal exists.

## Required verification

```bash
pnpm test:integration --filter goals
pnpm test:security --filter authorization
pnpm typecheck
```

## Exit gate

- All protected repository operations require internal user ID.
- Cross-user tests return 404.
- Duplicate create requests replay the original response.
- Stale writes return 409.
- Audit events contain safe metadata only.

## Evidence

- Authorization matrix
- Integration test report
- Example redacted audit event

---

# M10. Goal builder and vehicle-comparison experience

## Objective

Connect the polished frontend to the preview and goal APIs so users can understand feasibility and choose a simulated plan.

## Tasks

- [ ] **M10-T01:** Implement the typed API client and React Query key factories.
- [ ] **M10-T02:** Build the goal form using React Hook Form and shared Zod contracts.
- [ ] **M10-T03:** Add immediate local field validation without reproducing financial calculations.
- [ ] **M10-T04:** Submit to preview API and render zero-interest feasibility.
- [ ] **M10-T05:** Render eligible vehicle cards with required contribution, principal, modeled interest, ending balance, access, and assumption version.
- [ ] **M10-T06:** Render rejected options with stable reasons.
- [ ] **M10-T07:** Distinguish recommended simulated fit from other eligible options.
- [ ] **M10-T08:** Add the simulation disclosure adjacent to results.
- [ ] **M10-T09:** Allow authenticated users to save the goal.
- [ ] **M10-T10:** Implement loading, validation, network, empty, and stale-assumption states.
- [ ] **M10-T11:** Add text summaries for charts and comparisons.
- [ ] **M10-T12:** Add mobile and desktop browser tests.

## Deliverable

A user can build a goal, understand the zero-interest baseline, compare four modeled vehicles, and save the plan.

## Required verification

```bash
pnpm --filter web test
pnpm test:e2e --grep "goal builder"
pnpm test:a11y --filter goal-builder
pnpm --filter web build
```

## Exit gate

- The browser displays exactly the API numbers.
- Rejected products remain visible.
- Disclosure is visible without opening a hidden legal page.
- Keyboard and mobile flows pass.
- No client-side recommendation math exists.

## Evidence

- Desktop and mobile screenshots
- API-to-UI consistency test
- Accessibility output

---

# M11. Simulated account, immutable plan versions, and ledger

## Objective

Turn a saved goal into an auditable simulated set-and-forget plan.

## Tasks

- [ ] **M11-T01:** Implement `SimulationGoalAccountProvider` and `SimulationFundingProvider`.
- [ ] **M11-T02:** Implement goal activation using an eligible vehicle.
- [ ] **M11-T03:** Create an immutable plan version containing normalized goal input, selected vehicle, calculation output, and assumption version.
- [ ] **M11-T04:** Create one simulated account for the active goal.
- [ ] **M11-T05:** Implement append-only ledger entries.
- [ ] **M11-T06:** Implement manual simulated contribution with idempotency.
- [ ] **M11-T07:** Implement interest accrual and monthly posting through a domain service.
- [ ] **M11-T08:** Derive and verify balance from ledger entries.
- [ ] **M11-T09:** Implement reversals without editing original entries.
- [ ] **M11-T10:** Implement plan change by creating a new version, not mutating history.
- [ ] **M11-T11:** Add transaction, duplicate, reversal, and reconciliation tests.
- [ ] **M11-T12:** Add activate, pause, resume, plan-history, ledger, and contribution endpoints.

## Deliverable

An authenticated user can activate a plan, add simulated contributions, earn modeled interest, and inspect an immutable ledger.

## Required verification

```bash
pnpm test:unit --filter provider-simulators
pnpm test:integration --filter simulated-account
pnpm test:security --filter simulated-account
pnpm typecheck
```

## Exit gate

- Duplicate contribution submissions create one ledger entry.
- Balance equals ledger sum in every test.
- Plan changes preserve prior versions.
- All ledger corrections are reversals.
- No provider simulator is imported by domain code.

## Evidence

- Ledger state transition diagram
- Reconciliation test output
- Example immutable plan version

---

# M12. Dashboard, progress, and activity experience

## Objective

Deliver the core recurring-use experience that makes progress immediately understandable.

## Tasks

- [ ] **M12-T01:** Build authenticated dashboard route and empty state.
- [ ] **M12-T02:** Build active-goal summary with saved amount, target, progress, principal, modeled interest, next contribution, and projected completion.
- [ ] **M12-T03:** Build accessible progress and principal-versus-interest charts.
- [ ] **M12-T04:** Build contribution schedule and activity ledger tables.
- [ ] **M12-T05:** Add manual contribution flow.
- [ ] **M12-T06:** Add pause, resume, edit, archive, and complete controls with confirmation where appropriate.
- [ ] **M12-T07:** Build assumption snapshot panel.
- [ ] **M12-T08:** Show stale data, calculation failure, and unavailable schedule states.
- [ ] **M12-T09:** Add mobile dashboard layout.
- [ ] **M12-T10:** Add component, browser, and accessibility tests.

## Deliverable

A user can return to GoalPilot and understand plan status in less than one minute.

## Required verification

```bash
pnpm --filter web test
pnpm test:e2e --grep dashboard
pnpm test:a11y --filter dashboard
```

## Exit gate

- Principal and interest are visually and textually distinct.
- Charts have accessible summaries.
- Every action has an error and loading state.
- Dashboard works at all required breakpoints.

## Evidence

- Dashboard screenshots
- Usability checklist
- Accessibility report

---

# M13. Demo autopilot and scheduled interest processing

## Objective

Demonstrate a real cloud-controlled set-and-forget loop using simulated money.

## Tasks

- [ ] **M13-T01:** Implement deterministic schedule occurrence IDs.
- [ ] **M13-T02:** Implement `processDueSimulationEvents(processingDate)` as an application use case.
- [ ] **M13-T03:** Find due active plans without processing paused, completed, archived, or purchase-ready goals.
- [ ] **M13-T04:** Create one simulated contribution for each due occurrence.
- [ ] **M13-T05:** Accrue and post interest through the processing date.
- [ ] **M13-T06:** Transition fully funded goals to `purchase_ready` and stop future occurrences.
- [ ] **M13-T07:** Implement bounded batching and per-goal error isolation.
- [ ] **M13-T08:** Add EventBridge Scheduler and Lambda handler in CDK.
- [ ] **M13-T09:** Add retry, duplicate invocation, partial failure, catch-up, and month-boundary tests.
- [ ] **M13-T10:** Add an authenticated demo-autopilot setting and clear simulation label.
- [ ] **M13-T11:** Add metrics for processed, skipped, duplicate, failed, and purchase-ready transitions.

## Deliverable

A scheduled cloud job can advance simulated plans exactly once per due installment and post modeled interest safely.

## Required verification

```bash
pnpm test:unit --filter scheduler
pnpm test:integration --filter autopilot
pnpm infra:test
pnpm infra:synth
```

## Exit gate

- Repeated scheduler invocations are idempotent.
- One goal failure does not corrupt another goal.
- Purchase-ready goals receive no later scheduled contribution.
- All scheduled operations use an explicit processing date.

## Evidence

- Duplicate-invocation test
- CDK schedule assertion
- Sample structured metrics

---

# M14. Goal completion, history, export, and deletion

## Objective

Complete the product lifecycle and give users control over their data.

## Tasks

- [ ] **M14-T01:** Implement purchase-ready notification state in the UI.
- [ ] **M14-T02:** Implement mark-completed and adjust-target flows.
- [ ] **M14-T03:** Implement archived and completed goal history.
- [ ] **M14-T04:** Implement machine-readable data export with goal, plan, assumption, and ledger history.
- [ ] **M14-T05:** Implement deletion request state and idempotent deletion workflow.
- [ ] **M14-T06:** Define which audit records are retained or pseudonymized.
- [ ] **M14-T07:** Revoke sessions during deletion.
- [ ] **M14-T08:** Add tests for export ownership, complete export contents, repeated deletion, and inaccessible deleted data.
- [ ] **M14-T09:** Create `docs/security/privacy-lifecycle.md`.

## Deliverable

A user can finish a simulated goal, retrieve their information, and request account deletion.

## Required verification

```bash
pnpm test:integration --filter privacy
pnpm test:e2e --grep "goal completion|data export|account deletion"
pnpm test:security --filter privacy
```

## Exit gate

- Exports contain only the authenticated user's data.
- Deletion is idempotent.
- Sessions become invalid after deletion.
- Retention behavior is documented and implemented.

## Evidence

- Synthetic export sample
- Deletion test trace
- Privacy lifecycle review

---

# M15. Security hardening and threat-model verification

## Objective

Verify that the complete MVP satisfies the intended security boundaries before cloud release.

## Tasks

- [ ] **M15-T01:** Update the threat model with every implemented route, scheduled job, session flow, and data store.
- [ ] **M15-T02:** Create an authorization matrix for every protected operation.
- [ ] **M15-T03:** Add cross-user tests for every protected resource and subresource.
- [ ] **M15-T04:** Verify CSRF, CORS, Origin, cookie, token, redirect, and session controls.
- [ ] **M15-T05:** Verify parameterized SQL and unknown-field rejection.
- [ ] **M15-T06:** Test log redaction using seeded sensitive values.
- [ ] **M15-T07:** Test rate and payload-size abuse behavior.
- [ ] **M15-T08:** Review dependency licenses, vulnerabilities, and SBOM.
- [ ] **M15-T09:** Verify no real-money integration, credential, or misleading label exists.
- [ ] **M15-T10:** Add a security headers test for web and API responses.
- [ ] **M15-T11:** Conduct an independent read-only Codex security review and human review.
- [ ] **M15-T12:** Fix all actionable high and critical findings.

## Deliverable

A reviewed security baseline with evidence that tenant isolation and simulation boundaries hold.

## Required verification

```bash
pnpm test:security
pnpm security:scan
pnpm sbom
pnpm audit --prod
pnpm verify
```

## Exit gate

- Authorization matrix is fully tested.
- No high or critical unresolved production vulnerability exists.
- Logs contain none of the prohibited test values.
- No route can imply or initiate real money movement.
- Independent review has no unresolved release blocker.

## Evidence

- Updated threat model
- Authorization matrix
- Security review report
- SBOM and scan artifacts

---

# M16. AWS CDK and nonproduction cloud deployment

## Objective

Deploy the complete MVP to a minimal-cost AWS environment with repeatable infrastructure and no long-lived credentials.

## Tasks

- [ ] **M16-T01:** Implement Edge, Identity, and API CDK stacks from `STACK.md`.
- [ ] **M16-T02:** Make the S3 web bucket private and use CloudFront Origin Access Control.
- [ ] **M16-T03:** Configure SPA fallback and security response headers.
- [ ] **M16-T04:** Configure HTTP API routes, throttles, Lambda permissions, log retention, alarms, and reserved concurrency safeguards.
- [ ] **M16-T05:** Configure Cognito callback and logout URLs.
- [ ] **M16-T06:** Configure SSM parameters and least-privilege access.
- [ ] **M16-T07:** Configure EventBridge Scheduler for simulation processing.
- [ ] **M16-T08:** Add AWS Budgets and cost-notification setup documentation.
- [ ] **M16-T09:** Add GitHub OIDC deployment roles restricted by repository, branch, and environment.
- [ ] **M16-T10:** Add CDK assertions for encryption, public-access blocking, log retention, IAM scope, and development-auth rejection.
- [ ] **M16-T11:** Create staging deployment workflow and postdeployment smoke tests.
- [ ] **M16-T12:** Record actual deployment resources and initial cost observations.

## Deliverable

A real nonproduction URL serving the full React/Fastify/Cognito application through AWS-managed services.

## Required verification

```bash
pnpm infra:test
pnpm infra:synth
pnpm build
pnpm deploy:staging
pnpm smoke:staging
```

## Exit gate

- Deployment uses GitHub OIDC, not static keys.
- S3 is not public.
- Development auth is absent.
- Lambda and API Gateway limits are configured.
- Budget alerts are active before sustained use.
- Smoke tests pass through CloudFront.

## Evidence

- CDK synth artifact
- Deployed resource inventory
- Smoke-test report
- Initial cost dashboard screenshot

---

# M17. Full end-to-end, accessibility, and recovery validation

## Objective

Prove the product works as one deployed system and can be restored or redeployed from source.

## Tasks

- [ ] **M17-T01:** Run the complete anonymous preview journey.
- [ ] **M17-T02:** Run sign-up, sign-in, goal creation, activation, contribution, pause, resume, purchase-ready, completion, export, and deletion journeys.
- [ ] **M17-T03:** Run all journeys at mobile and desktop sizes.
- [ ] **M17-T04:** Run automated axe checks and a manual keyboard audit.
- [ ] **M17-T05:** Run a bounded 50-user-equivalent load test against nonproduction with cost limits.
- [ ] **M17-T06:** Exercise Lambda cold starts and database-sleep behavior.
- [ ] **M17-T07:** Export the PostgreSQL schema and synthetic data, restore to a clean database, and run smoke tests.
- [ ] **M17-T08:** Deploy the prior application artifact, deploy the candidate, and demonstrate application rollback.
- [ ] **M17-T09:** Review CloudWatch logs and alarms under injected failures.
- [ ] **M17-T10:** Document recovery, rollback, and incident-response procedures.

## Deliverable

A tested release candidate with usability, accessibility, performance, rollback, and recovery evidence.

## Required verification

```bash
pnpm verify
pnpm test:e2e:staging
pnpm test:a11y:staging
pnpm test:load:staging
pnpm restore:rehearsal
pnpm rollback:rehearsal
```

## Exit gate

- All critical journeys pass in the deployed environment.
- No serious or critical accessibility finding remains.
- Load behavior remains within the approved cost and error bounds.
- Database restore succeeds.
- Application rollback succeeds.
- Runbooks are executable by the second developer.

## Evidence

- Full test report
- Accessibility report
- Load report
- Restore transcript
- Rollback transcript

---

# M18. Final release audit and portfolio demonstration

## Objective

Produce a truthful, polished portfolio release that proves product judgment, engineering quality, security, cloud competency, and future extensibility.

## Tasks

- [ ] **M18-T01:** Freeze feature development.
- [ ] **M18-T02:** Run a clean-clone release verification.
- [ ] **M18-T03:** Perform an independent review of the complete diff against all four planning files.
- [ ] **M18-T04:** Verify every acceptance criterion in `PRD.md` has evidence.
- [ ] **M18-T05:** Verify `STACK.md` matches the lockfile and deployed topology.
- [ ] **M18-T06:** Verify `CONVENTIONS.md` through static checks and review.
- [ ] **M18-T07:** Mark only evidence-backed milestones complete in this file.
- [ ] **M18-T08:** Create release notes, architecture diagram, ERD, API reference, threat model, runbook, and cost summary.
- [ ] **M18-T09:** Record known limitations and the exact future provider-integration boundary.
- [ ] **M18-T10:** Prepare a five-minute demonstration and a deeper technical walkthrough.
- [ ] **M18-T11:** Tag the release only after both developers approve it.

## Deliverable

A portfolio-quality GoalPilot MVP release with deployed evidence and no false claim of real financial functionality.

## Required verification

```bash
git clean -xfd
pnpm install --frozen-lockfile
pnpm verify
pnpm infra:synth
pnpm test:e2e:staging
pnpm smoke:staging
```

## Exit gate

- Clean-clone verification passes.
- Every PRD acceptance criterion maps to a test, document, screenshot, or deployment artifact.
- No unresolved high or critical finding exists.
- The demonstration works without manual database edits.
- All screens state the simulation boundary accurately.
- Both developers approve the release.

## Evidence

- Signed release checklist
- Release tag and commit
- Test and security artifacts
- Architecture and cost summary
- Demo recording or presentation notes

---

# 2. Parallel contribution map

The plan is sequential at integration boundaries, but two developers can safely work in parallel after contracts are stable.

| Window | Developer A or backend lane | Developer B or frontend lane |
|---|---|---|
| After M02 | M03 database | M04 design system |
| After M04 and M05 | M06 engine | Refine fixture-based builder components |
| After M07 | M08 authentication | Goal-builder UI against preview API |
| After M09 | M11 simulated account backend | M10 and M12 frontend |
| After M11 | M13 scheduler | M12 dashboard and M14 completion UI |
| Hardening | M15 backend/security | Accessibility and browser audit |
| Cloud | M16 infrastructure | Deployed UX and smoke tests |

Rules:

- Contract changes merge before dependent parallel work begins.
- Only one active migration owner exists at a time.
- Only one task may change a given public API contract at a time.
- The nonauthor reviews every pull request.

# 3. Deferred backlog after MVP

The following tasks must not be pulled into M00 through M18:

- live rate ingestion;
- Plaid or account aggregation;
- embedded-finance account opening;
- KYC or identity documents;
- recurring ACH;
- customer custody;
- partner webhooks;
- reconciliation against real balances;
- Treasury purchase execution;
- managed securities;
- personalized advice;
- crypto or staking;
- merchant affiliate checkout;
- subscriptions or payments;
- shared or household goals;
- mobile native applications.

Each future capability begins with an ADR, provider due diligence, threat-model update, cost model, legal/compliance review, and sandbox implementation.

# 4. Codex continuation prompt

Use this prompt at the beginning of a fresh Codex milestone session:

```text
Execute the next unverified milestone in TASKS.md.

Read PRD.md, STACK.md, CONVENTIONS.md, TASKS.md, the current Git status, and all files relevant to the milestone. Confirm its prerequisites and run the relevant baseline checks before editing.

Implement only the active milestone. Preserve the approved simulation-only scope and module boundaries. Add meaningful tests, update owned documentation, run every required verification command, and record exact evidence. Do not weaken a quality gate or claim success without a passing command.

When the implementation is complete, perform a self-review against the milestone exit gate, then request an independent review. Do not begin the next milestone automatically unless the current one has been verified and merged.
```
