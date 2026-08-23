# GoalPilot MVP Project Report

**Version:** 1.0  
**Date:** 2026-08-23  
**Purpose:** Define the product shape, technical strategy, security posture, cost model, and future scale path for the first buildable GoalPilot release.  

## Executive recommendation

GoalPilot should return to development as a **simulation-first, provider-ready web application** rather than attempting to launch a regulated financial account in its first release.

The long-term promise remains unchanged:

```text
Choose something to save for
→ choose the amount and deadline
→ authorize recurring installments
→ place each installment into an interest-earning account
→ monitor progress automatically
→ withdraw the money when the goal is reached
```

The MVP will reproduce that experience using clearly labeled simulated funds and versioned illustrative rates. Users will create a purchase goal, compare appropriate interest-bearing vehicle models, activate a simulated plan, add or automatically simulate recurring contributions, observe modeled interest, and track progress until the goal becomes purchase-ready.

This is not a superficial prototype. It will be a secure, authenticated, deployed full-stack application with deterministic financial calculations, PostgreSQL persistence, scheduled cloud processing, infrastructure as code, automated tests, and a polished React interface. It deliberately excludes real money movement so the team can validate the user experience and engineering architecture before accepting provider cost, legal obligations, fraud exposure, or custody risk.

## What the MVP will look like

### Customer experience

A user lands on a focused page explaining that GoalPilot helps make an expensive purchase responsibly. The user enters:

- what they want to buy;
- the target amount;
- current savings;
- purchase deadline;
- recurring contribution;
- weekly, biweekly, or monthly cadence;
- liquidity requirement;
- capital-preservation preference.

GoalPilot first calculates the zero-interest baseline. It then compares four modeled vehicles:

1. plain cash;
2. a high-yield savings account model;
3. a certificate-of-deposit ladder model;
4. a Treasury-bill ladder model.

Every result separates contributed principal from modeled interest. The system shows the required installment, modeled ending balance, projected completion date, liquidity, assumption version, and reason a vehicle is eligible or rejected. It never recommends the highest yield merely because it is highest.

After signing in, the user can save one active goal and activate a simulated plan. The dashboard displays:

- progress toward the target;
- principal contributed;
- modeled interest earned;
- next scheduled contribution;
- projected completion date;
- contribution history;
- the exact illustrative assumption used.

The user can manually add a simulated contribution or enable **Demo Autopilot**. A scheduled AWS process then creates due simulated contributions, posts modeled interest, updates the projection, and stops automatically when the goal is funded. The user can pause, resume, edit, complete, archive, export, or delete the plan.

Every planning screen will state that the MVP is an educational simulation and does not hold, transfer, or invest money.

### Visual design

The web application will use a restrained financial-product visual system rather than a generic dashboard template:

- British racing green as the primary brand color;
- warm ivory surfaces;
- muted gold accents;
- charcoal typography;
- generous spacing and a clear numerical hierarchy;
- accessible cards, forms, progress indicators, tables, and charts;
- subtle motion that respects reduced-motion settings.

The design system will use free, maintainable resources: Tailwind CSS, shadcn/ui source components, Radix primitives, Lucide icons, Recharts, Motion, and selected Figma Community references for inspiration. GoalPilot will own and adapt its component source instead of depending on a paid template or multiple competing UI libraries.

The critical flows will target WCAG 2.2 AA, keyboard operation, visible focus, mobile support beginning at 360 pixels, text alternatives for charts, and complete loading, empty, error, stale, and success states.

## Product boundary

The MVP proves the planning and automation experience without pretending to be a bank.

| Included now | Deliberately deferred |
|---|---|
| Goal creation and authenticated persistence | Real bank linking or Plaid |
| Deterministic installment calculations | Account opening and KYC |
| Illustrative interest-bearing vehicle comparison | ACH, cards, wires, or withdrawals |
| Simulated contribution and interest ledger | Custody of customer funds |
| Scheduled Demo Autopilot | Brokerage or Treasury execution |
| Dashboard, history, export, and deletion | Personalized advice, lending, crypto, or staking |
| Secure AWS deployment and CI/CD | Live or scraped APYs |

This boundary protects users and keeps the product legally honest. It also prevents provider negotiations from blocking software development.

## Technical architecture

The MVP will be a TypeScript monorepo with clear module boundaries:

```text
React + TypeScript browser
        ↓
Amazon CloudFront
        ├── private S3 static origin
        └── API Gateway HTTP API
                ↓
        AWS Lambda running Fastify
                ↓
        Neon PostgreSQL through Drizzle

EventBridge Scheduler
        ↓
Simulation Lambda handler
        ↓
Append-only simulated ledger
```

Amazon Cognito will provide registration, email verification, password reset, and optional TOTP MFA. The application will use authorization code flow with PKCE and a server-side opaque session so OAuth refresh tokens are not exposed to browser JavaScript.

The financial domain will be a pure TypeScript package. It will not depend on React, Fastify, PostgreSQL, AWS, environment variables, system time, or provider SDKs. Money will be represented as integer cents. Rates will use basis points or exact decimal arithmetic. Every calculation will receive an explicit processing date and assumption version, making output deterministic and testable.

PostgreSQL will store users, goals, assumption versions, immutable plan versions, simulated accounts, append-only ledger entries, scheduled occurrences, idempotency records, audit events, and privacy requests. The schema will remain standard PostgreSQL so the database can move from Neon to RDS or Aurora without changing the product model.

Provider-neutral interfaces will exist from the beginning:

```text
RateProvider
GoalAccountProvider
FundingProvider
```

The MVP will supply static and simulation adapters. A future sponsor-bank or embedded-finance adapter can replace them without rewriting the goal, plan, calculation, or user-interface domains.

## Cost and cloud strategy

The original production design demonstrated substantial AWS knowledge but carried fixed costs from ECS Fargate, an Application Load Balancer, private networking, NAT Gateway, WAF, and RDS. Those services are justified after real financial connectivity or sustained traffic, not before product validation.

The revised MVP targets approximately **$0 to $6 per month plus an optional domain** at very small usage:

| Component | Cost posture |
|---|---|
| S3 and CloudFront | Usually negligible at portfolio traffic |
| API Gateway and Lambda | Scales to zero; usually within small-usage allowances |
| Cognito | Targeting free small-user usage |
| EventBridge Scheduler | Negligible for one low-frequency schedule |
| Neon PostgreSQL | Free-plan target, subject to provider limits |
| CloudWatch | Controlled through structured low-volume logs and short retention |
| SSM Parameter Store | Standard parameters, avoiding per-secret fixed cost |
| GitHub Actions | Targeting included account allowances |

This figure is a design target, not a promise. AWS free-tier eligibility, database-plan limits, logs, traffic, and pricing can change. AWS Budgets and alarms will be configured before deployment.

The architecture still demonstrates cloud competency through S3, CloudFront, API Gateway, Lambda, Cognito, EventBridge, CloudWatch, IAM, STS, CDK, CloudFormation, SSM, and GitHub OIDC. It demonstrates judgment by choosing the least expensive services that satisfy the MVP rather than deploying infrastructure for its own sake.

## Security posture

Cybersecurity is part of the product definition, not a final checklist. The MVP will include:

- server-verified Cognito identity;
- subject-scoped SQL predicates for every protected record;
- opaque secure browser sessions;
- CSRF and Origin validation;
- strict runtime schemas and unknown-field rejection;
- parameterized database access;
- idempotency for state-changing operations;
- append-only simulated financial ledger entries;
- structured logs with tested redaction;
- least-privilege IAM;
- GitHub OIDC instead of static AWS deployment keys;
- branch protection, CodeQL, Dependabot, dependency review, Gitleaks, production audit, and a CycloneDX SBOM;
- a maintained data inventory and threat model;
- export and deletion workflows;
- automated authorization, accessibility, integration, browser, and infrastructure tests.

The MVP collects no bank credentials, routing numbers, account numbers, SSNs, identity documents, or real transaction data. This sharply reduces the initial breach impact while allowing the team to prove the controls required for later provider integrations.

## What this MVP proves

A completed release will demonstrate that the team can:

1. Turn a financial-product concept into a coherent customer journey.
2. Build a polished React and TypeScript interface.
3. Design a deterministic, explainable financial-domain engine.
4. Operate a Fastify API and PostgreSQL data model securely.
5. Enforce tenant isolation and auditable state changes.
6. Deploy and monitor a serverless AWS system through CDK and CI/CD.
7. Use scheduled cloud processing to create a credible set-and-forget experience.
8. Apply cybersecurity controls and produce evidence rather than relying on a production-ready label.
9. Keep the architecture inexpensive today while preserving a direct path to real interest-bearing partner accounts.

## Future path to real funds

After the MVP demonstrates that users create goals, understand principal versus interest, and return to monitor progress, GoalPilot can begin a provider-sandbox phase. The static rate source can be replaced by an approved rate adapter. The simulated account and funding providers can be replaced by embedded-finance adapters. Real integrations will add verified webhooks, durable provider commands, reconciliation, provider-authoritative balances, and stronger operational and compliance controls.

At that point the team can justify paid infrastructure, WAF, independent security validation, legal review, and partner-program expenses with actual product evidence.

## Recommendation

Build the simulation-first MVP exactly to this boundary. It retains the core product insight, produces an impressive and useful full-stack application, costs almost nothing to operate at early usage, and avoids the mistake of making bank-partner negotiations a prerequisite for learning whether people actually want GoalPilot.
