# GoalPilot MVP Product Requirements Document

**Document owner:** Xavier Kubancik  
**Status:** Approved implementation baseline for Codex  
**Version:** 1.0  
**Baseline date:** 2026-08-23  
**Target release:** Public portfolio-quality MVP with a small invited test group  

## 1. Purpose

This document defines the first buildable version of GoalPilot. It replaces the earlier production-heavy architecture with a deliberately inexpensive MVP that still demonstrates the core product thesis, full-stack engineering, cloud deployment, cybersecurity discipline, and an extensible path to real interest-bearing partner accounts.

GoalPilot helps a user answer a concrete question:

> How much should I save, where could the money earn interest while I wait, and am I on pace to afford the purchase by my deadline?

The MVP does **not** hold, transfer, invest, or stake real customer funds. It creates a secure, authenticated planning and simulation experience that models recurring contributions and interest-bearing financial vehicles. The internal architecture must make later provider integration possible without rewriting the domain model or user experience.

## 2. Source basis and superseded assumptions

This PRD preserves the strongest requirements from the prior GoalPilot product and system design:

- feasibility before yield;
- policy eligibility before optimization;
- deterministic, reproducible financial calculations;
- every rejected option remains visible with a reason;
- no guaranteed outcomes;
- AI never owns a financial number;
- user-owned data access is enforced server-side;
- security, auditability, and versioned assumptions are first-class concerns.

It also preserves the security roadmap's emphasis on low-cost controls before expensive external audits: MFA, protected branches, secret scanning, data classification, threat modeling, least privilege, structured logs, dependency review, and an SBOM.

The following earlier assumptions are superseded for this MVP:

- The MVP will **not** deploy the fixed-cost ECS, ALB, private-subnet, NAT Gateway, WAF, and RDS topology.
- The MVP will **not** implement live money movement, account opening, Plaid, custody, brokerage execution, or real recurring ACH.
- The MVP will **not** use live financial rates unless a trustworthy source and approval workflow are added later.
- The MVP will **not** claim that an implementation already exists. Completion is established only by repository code and passing evidence.

## 3. Product vision

### 3.1 Long-term vision

```text
Choose something to save for
→ choose the amount and deadline
→ authorize recurring installments
→ GoalPilot places each installment into an interest-earning account
→ GoalPilot monitors progress automatically
→ the user withdraws the money when the goal is reached
```

### 3.2 MVP expression of that vision

```text
Choose something to save for
→ choose the amount and deadline
→ compare eligible interest-bearing vehicle models
→ activate a simulated GoalPilot plan
→ record or simulate recurring contributions
→ apply versioned illustrative interest assumptions
→ monitor progress automatically
→ mark the goal purchase-ready
```

The distinction must be conspicuous throughout the product: the MVP simulates the experience and does not open or control a financial account.

## 4. Goals

The MVP must:

1. Give users a clear, polished goal-planning experience.
2. Calculate whether contributions alone can reach the goal.
3. Compare a narrow set of interest-bearing financial vehicles appropriate for dated purchases.
4. Estimate the recurring contribution required to reach the target under each eligible vehicle.
5. Show the contribution principal, modeled interest, ending balance, and projected completion date.
6. Allow an authenticated user to save a goal and activate a simulated plan.
7. Support manual simulated contributions and optional scheduled demo contributions.
8. Recalculate progress using an immutable vehicle-assumption version.
9. Demonstrate secure full-stack design and low-cost AWS deployment.
10. Preserve clean provider interfaces for later bank, funding, and rate integrations.

## 5. Non-goals

The MVP must not:

- accept, custody, pool, transfer, or withdraw real money;
- open a bank, brokerage, crypto, or custodial account;
- connect to a real bank through Plaid or another aggregator;
- collect routing numbers, account numbers, SSNs, identity documents, or bank credentials;
- execute ACH, cards, wires, securities trades, Treasury purchases, crypto trades, or staking;
- present personalized investment advice;
- scrape current APYs or claim a rate is currently available;
- guarantee interest, returns, principal protection, or goal completion;
- calculate individualized tax liability;
- include equities, options, leveraged products, or crypto in the MVP recommendation catalog;
- use an LLM to calculate, select, rank, or modify a financial result;
- implement social feeds, shared goals, merchant checkout, lending, or subscriptions.

## 6. Target users

### Primary user

A financially motivated young professional saving for a discretionary purchase of approximately $500 to $25,000 within 3 to 36 months, such as:

- travel;
- a vehicle down payment;
- a computer or electronics purchase;
- furniture or appliances;
- a wedding or event;
- hobby equipment;
- a home-improvement purchase.

### User needs

The user needs to know:

- whether the goal is feasible with no interest;
- the installment required at a chosen cadence;
- how much modeled interest could help;
- which products conflict with the deadline or liquidity need;
- how far ahead or behind the plan is;
- what controllable action will restore the plan when it falls behind.

## 7. Product principles

| Principle | Required behavior |
|---|---|
| Feasibility before yield | Always calculate the zero-interest contribution baseline first. |
| Interest is a buffer | Never understate the installment solely because a variable rate may continue. |
| Eligibility before ranking | A product that violates horizon or liquidity rules cannot be recommended. |
| Transparent assumptions | Every result displays rate, source type, effective date, version, and illustrative status. |
| Controllable levers first | When behind, suggest contribution, deadline, or target changes before more risk. |
| Deterministic core | Equal input plus equal assumption version produces equal output. |
| Every option visible | Rejected vehicles remain visible with stable reason codes. |
| Simulation is explicit | No screen may imply that GoalPilot currently holds or invests money. |
| Security by default | Ownership, validation, logging redaction, and least privilege are not optional. |
| Provider-ready, not provider-dependent | Domain interfaces support future real providers while the MVP uses simulations. |

## 8. MVP scope

### 8.1 Vehicle catalog

The MVP supports four modeled vehicles:

1. **Plain cash**
   - 0% modeled APY.
   - Immediate liquidity.
   - Baseline comparator.

2. **High-yield savings model**
   - Variable illustrative APY.
   - Immediate or next-business-day modeled liquidity.
   - Principal-preservation-oriented deposit model.

3. **Certificate-of-deposit ladder model**
   - Fixed illustrative yield for defined terms.
   - Early-withdrawal or lock constraint.
   - Eligible only when the modeled ladder fits the deadline and liquidity requirement.

4. **Treasury-bill ladder model**
   - Illustrative yield and maturity schedule.
   - Modeled to maturity rather than relying on secondary-market sale.
   - Eligible only when maturities complete before the purchase date.

The catalog uses static, reviewed demo assumptions. The UI must display **Illustrative rate — not a live offer**.

### 8.2 Goal fields

Each goal contains:

- name;
- optional category;
- target amount in USD cents;
- current saved amount in USD cents;
- target date;
- recurring contribution amount in USD cents;
- contribution cadence: weekly, biweekly, or monthly;
- liquidity need: anytime, within 30 days, or only at goal date;
- capital-preservation preference: required or flexible;
- selected confidence setting for scenario display;
- optional notes;
- status: draft, active, paused, purchase_ready, completed, archived.

### 8.3 User-visible screens

1. **Public landing page**
   - Product promise.
   - Three-step explanation.
   - Security and simulation disclosure.
   - Call to action.

2. **Authentication**
   - Sign up, sign in, verify email, reset password, and sign out through Cognito.

3. **Dashboard**
   - Active goal card.
   - Amount saved, target, progress percentage, modeled interest, next contribution, and projected completion.
   - Empty, loading, error, and stale-assumption states.

4. **Goal builder**
   - Accessible multi-section form.
   - Immediate zero-interest feasibility feedback.
   - Clear explanation of every input.

5. **Vehicle comparison**
   - Eligible and rejected products.
   - Required contribution, modeled ending balance, modeled interest, access, and reason codes.
   - Recommended simulated fit based on deterministic policy.

6. **Plan detail**
   - Principal contributed versus modeled interest.
   - Progress chart.
   - Contribution schedule.
   - Assumption snapshot.
   - Pause, resume, edit, archive, complete, and add simulated contribution actions.

7. **Activity ledger**
   - Simulated contribution, interest-credit, adjustment, and reversal records.

8. **Settings**
   - Account profile.
   - Data export request.
   - Account deletion request.

### 8.4 Explicit product language

Every projection or plan screen must include language equivalent to:

> Educational simulation using illustrative assumptions. GoalPilot does not hold, transfer, or invest money in this MVP. Rates and outcomes are not guaranteed.

## 9. Core user flows

### Flow A: Anonymous preview

1. User opens the landing page.
2. User enters goal amount, deadline, current savings, contribution, and cadence.
3. Client validates the shared schema.
4. API calculates a stateless preview.
5. User sees the no-interest baseline, vehicle comparison, and recommendation trace.
6. User is invited to create an account to save the plan.

### Flow B: Create and activate a simulated plan

1. Authenticated user creates a goal.
2. API verifies identity and validates input.
3. API stores the goal with the user's Cognito subject.
4. User selects an eligible simulated vehicle.
5. API creates an immutable assumption snapshot and active simulated account.
6. User sees the plan dashboard and upcoming simulated schedule.

### Flow C: Add a simulated contribution

1. User selects **Add contribution**.
2. User enters amount and effective date.
3. API validates ownership and amount.
4. API writes a ledger entry idempotently.
5. API recalculates the balance and projection.
6. UI displays the updated progress and audit-safe activity.

### Flow D: Demo autopilot

1. User enables demo autopilot and confirms cadence.
2. A scheduled cloud job finds due simulated contributions.
3. The job creates one idempotent contribution per due schedule.
4. It accrues modeled interest through the processing date.
5. It updates the projection and records an audit event.
6. User sees the next contribution and updated balance.

### Flow E: Goal becomes purchase-ready

1. Available simulated balance reaches or exceeds target.
2. System stops future simulated contributions.
3. Goal state becomes `purchase_ready`.
4. User marks the goal completed or adjusts the target.
5. No real withdrawal is offered.

## 10. Functional requirements

### 10.1 Authentication and users

- **AUTH-001:** Use Amazon Cognito for email-based sign-up and sign-in.
- **AUTH-002:** Verify JWT signature, issuer, client ID, token use, and expiration server-side.
- **AUTH-003:** Never trust a user ID supplied in request body, query, or URL as ownership proof.
- **AUTH-004:** Hide whether another user's goal exists; inaccessible resources return 404.
- **AUTH-005:** Support TOTP MFA configuration in infrastructure, optional for MVP testers and mandatory before any real financial integration.

### 10.2 Goal management

- **GOAL-001:** Create, retrieve, update, pause, resume, complete, archive, and delete a user-owned goal.
- **GOAL-002:** Use optimistic concurrency through a version number or ETag.
- **GOAL-003:** Use idempotency keys for goal creation and simulated contribution creation.
- **GOAL-004:** Store money as integer cents and dates as ISO calendar dates.
- **GOAL-005:** Limit the MVP to one active simulated goal per user while allowing archived history.

### 10.3 Calculation engine

- **CALC-001:** Produce a zero-interest contribution baseline.
- **CALC-002:** Generate weekly, biweekly, and monthly contribution dates.
- **CALC-003:** Model deposit interest using deterministic daily accrual and monthly posting.
- **CALC-004:** Model CDs and Treasury bills using maturity-aligned cash flows and explicit lock constraints.
- **CALC-005:** Calculate total principal, modeled interest, ending balance, shortfall or surplus, and completion date.
- **CALC-006:** Search for the required recurring contribution to the nearest cent.
- **CALC-007:** Reject stale, missing, malformed, or disabled assumptions.
- **CALC-008:** Return stable eligibility and rejection reason codes.
- **CALC-009:** Avoid floating-point storage for money; calculations must use integer cents and exact decimal or basis-point rules.

### 10.4 Vehicle policy

- **VEH-001:** Version the vehicle catalog.
- **VEH-002:** Plain cash is always available as a baseline.
- **VEH-003:** CD models are rejected when the liquidity requirement conflicts with lock or penalty assumptions.
- **VEH-004:** Treasury models are rejected when maturity falls after the goal date.
- **VEH-005:** Capital-preservation-required goals exclude any future market-value vehicle automatically.
- **VEH-006:** The recommended option is the eligible product that reaches the goal with the lowest required contribution, then highest modeled ending balance, then highest liquidity.
- **VEH-007:** Compensation or affiliate status is not part of the ranking function.

### 10.5 Simulated account and ledger

- **SIM-001:** An active goal may have one simulated account tied to one assumption snapshot.
- **SIM-002:** Ledger entry types are contribution, interest_credit, adjustment, and reversal.
- **SIM-003:** Ledger entries are append-only; corrections use reversals.
- **SIM-004:** A scheduled contribution has a unique schedule occurrence ID.
- **SIM-005:** Re-running a scheduled job cannot duplicate a contribution or interest posting.
- **SIM-006:** Balance is derived from the ledger and may be cached only with a reconciliation test.
- **SIM-007:** Changing the selected vehicle creates a new plan version; it never rewrites prior history.

### 10.6 Assumptions

- **RATE-001:** Demo assumptions are stored in a versioned database record or reviewed seed file.
- **RATE-002:** Each assumption includes vehicle code, APY or yield, effective date, retrieval/review date, liquidity, lock, minimum, source label, and `illustrative=true`.
- **RATE-003:** The UI displays the exact version used by each saved plan.
- **RATE-004:** An administrator interface is not required; changing assumptions is a reviewed migration or seed update.
- **RATE-005:** A future `RateProvider` interface must allow a live provider without changing calculation consumers.

### 10.7 Privacy and audit

- **PRIV-001:** Provide a machine-readable export of user, goal, plan, and ledger data.
- **PRIV-002:** Provide an account-deletion workflow with a documented retention boundary.
- **AUDIT-001:** Append audit events for authentication-sensitive and goal-state-changing actions.
- **AUDIT-002:** Audit metadata must not contain secrets, JWTs, cookies, full request bodies, or unnecessary financial values.

## 11. Data model

Minimum relational records:

| Record | Purpose |
|---|---|
| `users` | Internal profile keyed by Cognito subject. |
| `goals` | User-owned goal, contribution plan, status, and version. |
| `vehicle_assumption_versions` | Immutable catalog version metadata. |
| `vehicle_assumptions` | Vehicle parameters belonging to a catalog version. |
| `plan_versions` | Immutable selected vehicle, normalized input, output, and assumption version. |
| `simulated_accounts` | Active simulated account and plan reference. |
| `ledger_entries` | Append-only principal, interest, adjustment, and reversal events. |
| `schedule_occurrences` | Idempotent recurring simulation work. |
| `idempotency_records` | User-scoped request replay protection. |
| `audit_events` | Security-safe event history. |
| `data_requests` | Export or deletion request state. |

Future provider records must be addable without changing the goal or plan identity:

- `financial_connections`;
- `partner_customers`;
- `partner_accounts`;
- `funding_sources`;
- `provider_transfers`;
- `provider_webhook_events`;
- `reconciliation_runs`.

## 12. API surface

Public:

```text
GET  /health
GET  /ready
GET  /api/v1/vehicle-catalog
POST /api/v1/previews
```

Authenticated:

```text
GET    /api/v1/goals
POST   /api/v1/goals
GET    /api/v1/goals/:goalId
PATCH  /api/v1/goals/:goalId
DELETE /api/v1/goals/:goalId
POST   /api/v1/goals/:goalId/activate
POST   /api/v1/goals/:goalId/pause
POST   /api/v1/goals/:goalId/resume
POST   /api/v1/goals/:goalId/complete
GET    /api/v1/goals/:goalId/plan-versions
GET    /api/v1/goals/:goalId/ledger
POST   /api/v1/goals/:goalId/contributions
POST   /api/v1/data-exports
POST   /api/v1/account-deletion
GET    /api/v1/me
```

Internal scheduled endpoint or Lambda handler:

```text
processDueSimulationEvents(processingDate)
```

## 13. Frontend design requirements

The visual system uses:

- British racing green as the primary brand color;
- warm ivory backgrounds;
- restrained muted-gold accents;
- high-contrast charcoal typography;
- generous whitespace;
- rounded but not cartoonish cards;
- one primary action per screen;
- clear financial-number hierarchy;
- subtle motion that respects `prefers-reduced-motion`.

The frontend must use free, maintainable resources:

- Tailwind CSS for tokens and layout;
- shadcn/ui source-owned components;
- Radix UI primitives through shadcn/ui;
- Lucide icons;
- Recharts for charts;
- Motion for restrained transitions;
- Figma Community references for inspiration only, never copied blindly.

Requirements:

- WCAG 2.2 AA target for critical flows;
- complete keyboard navigation;
- visible focus states;
- semantic headings and landmarks;
- labels and instructions for every form control;
- error summaries plus field-level errors;
- color is never the sole status indicator;
- responsive support from 360px mobile width through desktop;
- no horizontal scrolling in ordinary flows;
- charts have text summaries and accessible labels.

## 14. Security requirements

- Validate every external input with shared runtime schemas.
- Parameterize all SQL through Drizzle or explicit prepared statements.
- Apply the authenticated subject in every protected database predicate.
- Use generic error responses for unexpected failures.
- Redact authorization, cookies, tokens, emails, and financial amounts from routine logs.
- Use a strict CORS allowlist and security headers.
- Add API Gateway throttling and application-level per-subject rate limits.
- Store secrets only in environment injection from SSM Parameter Store or deployment secrets.
- Never commit `.env`, cloud credentials, database credentials, or JWTs.
- Use GitHub OIDC for AWS deployment; no long-lived AWS keys in GitHub.
- Generate a CycloneDX SBOM in CI.
- Run CodeQL, Dependabot, dependency review, secret scanning, and production dependency audit.
- Keep CloudWatch log retention short in development and explicitly configured in production.
- Maintain a lightweight threat model and data inventory before adding any financial provider.

## 15. Reliability and performance

MVP objectives:

| Objective | Target |
|---|---|
| Stateless preview p95 | Under 1 second at supported inputs, excluding cold start outliers |
| Authenticated API p95 | Under 1 second for ordinary CRUD at low traffic |
| Determinism | Equal normalized input and version produce equal output |
| Duplicate simulated contribution | Zero |
| Cross-user data disclosure | Zero |
| Availability target | Best-effort MVP; architecture designed for 99.9% later |
| Recovery | Database provider backups plus exported migrations; documented restore rehearsal before public beta |

The API must be compatible with AWS Lambda but remain ordinary Fastify code so it can move to ECS Fargate later without rewriting domain or route logic.

## 16. Analytics and success metrics

Collect privacy-minimized product events:

- landing CTA selected;
- preview started and completed;
- goal created;
- plan activated;
- simulated contribution created;
- autopilot enabled or disabled;
- plan edited;
- goal became purchase-ready;
- goal completed;
- validation or calculation failure category.

Do not send goal names, balances, contribution amounts, or notes to third-party analytics in the MVP.

Success indicators:

- at least 70% of testers who start the builder complete a preview;
- at least 50% of authenticated testers activate a simulated plan;
- at least 50% of active-plan testers record or simulate three contributions;
- zero policy-gate bypasses;
- zero cross-user authorization failures;
- zero explanation or displayed-number mismatches;
- qualitative feedback indicates users understand principal versus modeled interest.

## 17. Provider-ready interfaces

The application must define ports, even though only simulated adapters ship:

```ts
interface RateProvider {
  getCatalog(asOf: Date): Promise<VehicleCatalog>;
}

interface GoalAccountProvider {
  open(input: OpenGoalAccountInput): Promise<GoalAccountReference>;
  getBalance(accountId: string): Promise<ProviderBalance>;
}

interface FundingProvider {
  createRecurringAuthorization(input: AuthorizationInput): Promise<AuthorizationReference>;
  requestContribution(input: ContributionRequest): Promise<ContributionReference>;
}
```

MVP implementations:

- `StaticRateProvider`;
- `SimulationGoalAccountProvider`;
- `SimulationFundingProvider`.

Future implementations may include an embedded-finance or sponsor-bank adapter after legal, security, commercial, and provider approval.

## 18. Release acceptance criteria

The MVP is releasable only when:

1. A clean clone installs from the committed lockfile.
2. Local bootstrap and database migration are documented and repeatable.
3. Public preview works end to end.
4. Cognito-authenticated goal CRUD works.
5. Cross-user authorization tests pass for every protected resource.
6. All four vehicle models have golden tests.
7. Contribution schedules pass date-boundary tests.
8. Simulated interest and ledger reconciliation tests pass.
9. Duplicate scheduled runs do not create duplicate entries.
10. Mobile and desktop critical flows pass Playwright tests.
11. Automated accessibility checks report no serious or critical finding in critical flows.
12. Strict TypeScript, formatting, and linting pass with zero warnings.
13. Production web and API builds pass.
14. CDK synthesis passes.
15. CodeQL, dependency audit, secret scan, and SBOM generation pass.
16. Deployment to a nonproduction AWS environment succeeds.
17. Smoke tests pass through the deployed CloudFront URL.
18. The UI consistently labels all money and rates as simulated or illustrative.
19. No real-money provider credential or integration exists in the release.
20. `TASKS.md` contains evidence links for completed milestones.

## 19. Future scale path

The MVP architecture must permit these later steps without redesigning the product core:

1. Replace static assumptions with an approved rate-ingestion adapter.
2. Add provider sandbox account opening behind `GoalAccountProvider`.
3. Add verified funding sources and recurring ACH behind `FundingProvider`.
4. Replace simulated ledger events with verified provider events while preserving the GoalPilot operational ledger.
5. Add webhook inbox, transactional outbox, and reconciliation records.
6. Introduce a paid database tier or AWS RDS when workload, recovery, or contract requirements justify it.
7. Add WAF before a broad public or financially connected beta.
8. Move Fastify from Lambda to ECS Fargate only if sustained load, connection behavior, or background processing justifies it.
9. Add Treasury or investment execution only under an approved regulated operating model.

The MVP is successful when it proves that users understand and value the set-and-forget goal experience, not when it prematurely reproduces a bank.