# GoalPilot MVP Project Report

**Version:** 1.1  
**Date:** 2026-08-23  
**Format target:** No more than three pages when rendered at 11-point type with standard report margins.  

## Executive recommendation

GoalPilot should resume development as a **simulation-first, provider-ready web application**. The long-term promise remains:

```text
Choose something to save for
→ choose the amount and deadline
→ authorize recurring installments
→ place each installment into an interest-earning account
→ monitor progress automatically
→ withdraw the money when the goal is reached
```

The MVP will reproduce the planning and automation experience with simulated funds and versioned illustrative rates. It will not open accounts, connect banks, transfer money, or present live offers. This is the right first release because it validates whether users understand and value the product while avoiding provider contracts, custody risk, fraud exposure, and premature fixed infrastructure costs.

The release will still be a substantive full-stack product: a polished React interface, deterministic financial-domain logic, a Fastify API, PostgreSQL persistence, secure authentication, scheduled cloud processing, AWS infrastructure as code, CI/CD, observability, and cybersecurity evidence.

## Product experience

A user enters:

- purchase goal and optional category;
- target amount and current savings;
- purchase deadline;
- recurring contribution and weekly, biweekly, or monthly cadence;
- liquidity requirement;
- capital-preservation preference.

GoalPilot first calculates the zero-interest baseline. It then compares four modeled vehicles:

1. plain cash;
2. high-yield savings;
3. a certificate-of-deposit ladder;
4. a Treasury-bill ladder.

Every comparison separates contributed principal from modeled interest and shows the required installment, ending balance, projected completion date, access constraints, assumption version, and eligibility decision. A product that conflicts with the user's deadline or liquidity need remains visible but cannot be recommended.

After authentication, the user can save one active goal and activate a simulated plan. The dashboard shows progress, principal, modeled interest, next contribution, projected completion, contribution history, and the assumption snapshot. Users can add simulated contributions manually or enable **Demo Autopilot**. A scheduled AWS process then creates due simulated contributions, posts modeled interest, recalculates progress, and stops once the goal is purchase-ready.

Every plan and projection screen will state:

> Educational simulation using illustrative assumptions. GoalPilot does not hold, transfer, or invest money in this MVP. Rates and outcomes are not guaranteed.

## Scope boundary

| Included in the MVP | Deferred until after validation |
|---|---|
| Public goal preview | Plaid or real bank aggregation |
| Authenticated goal CRUD | KYC and account opening |
| Deterministic installment engine | ACH, cards, wires, or withdrawals |
| Cash, HYSA, CD, and T-bill models | Custody or investment execution |
| Simulated contribution and interest ledger | Personalized advice, loans, crypto, or staking |
| Scheduled Demo Autopilot | Live or scraped APYs |
| Dashboard, history, export, and deletion | Merchant checkout and subscriptions |
| Deployed AWS application and CI/CD | Native mobile applications |

This is not a disposable prototype. The goal, plan, ledger, and provider boundaries are designed so a future regulated partner can replace simulation adapters without rewriting the user experience or financial-domain model.

## Frontend and design

The web application will use React and TypeScript with a GoalPilot-specific visual system:

- British racing green;
- warm ivory surfaces;
- muted gold accents;
- high-contrast charcoal typography;
- generous spacing and clear financial-number hierarchy;
- responsive cards, forms, progress indicators, tables, and charts;
- restrained motion with reduced-motion support.

Free resources will include Tailwind CSS, shadcn/ui source components, Radix primitives, Lucide icons, Recharts, Motion, and Figma Community references for inspiration. GoalPilot will own and adapt its component source rather than depending on a paid template or several competing UI systems.

Critical flows will target WCAG 2.2 AA, keyboard operation, visible focus, mobile support beginning at 360 pixels, text alternatives for charts, and complete loading, empty, error, stale, and success states.

## Technical architecture

```text
React + TypeScript browser
        ↓
Amazon CloudFront
        ├── private S3 web origin
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

Amazon Cognito will provide registration, verification, password reset, and optional TOTP MFA. GoalPilot will use authorization code flow with PKCE and a server-side opaque session so refresh tokens do not enter browser-accessible storage.

The financial engine will be a pure TypeScript package with no React, Fastify, database, AWS, provider, environment, or implicit-clock dependency. Money will use integer cents. Rates will use basis points or exact decimal arithmetic. Equal inputs, assumption version, and processing date will produce equal results.

PostgreSQL will store users, goals, versioned assumptions, immutable plan versions, simulated accounts, append-only ledger entries, scheduled occurrences, idempotency records, audit events, and privacy requests. The schema will remain ordinary PostgreSQL so it can later move from Neon to RDS or Aurora.

Provider-neutral interfaces will exist from the beginning:

```text
RateProvider
GoalAccountProvider
FundingProvider
```

The MVP will implement static and simulation adapters. Future embedded-finance adapters can replace them behind the same contracts.

## Cost and cloud strategy

The earlier architecture demonstrated mature AWS design but imposed fixed costs through ECS Fargate, an Application Load Balancer, private networking, NAT Gateway, WAF, and RDS. The revised architecture targets approximately **$0 to $6 per month plus an optional domain** at very small usage:

| Component | MVP cost posture |
|---|---|
| S3 and CloudFront | Usually negligible at portfolio traffic |
| API Gateway and Lambda | Scale to zero and remain usage-based |
| Cognito | Small-test-user target within included usage |
| EventBridge Scheduler | Negligible for one low-frequency schedule |
| Neon PostgreSQL | Free-plan target, subject to provider limits |
| CloudWatch and SSM | Controlled logs and standard parameters |

This is a target, not a guarantee. Free-tier eligibility, database limits, logs, traffic, and vendor pricing can change. AWS Budgets and alarms will be configured before deployment.

The project still demonstrates practical cloud competency through S3, CloudFront, API Gateway, Lambda, Cognito, EventBridge, CloudWatch, IAM, STS, CDK, CloudFormation, SSM, and GitHub OIDC. It demonstrates judgment by selecting services appropriate to the workload rather than maximizing service count.

## Security and evidence

The MVP will include:

- server-verified Cognito identity and subject-scoped SQL;
- opaque secure sessions, CSRF protection, and Origin validation;
- strict shared schemas and parameterized database access;
- idempotency for state-changing operations;
- append-only ledger records and immutable plan versions;
- structured logging with tested redaction;
- least-privilege IAM and GitHub OIDC rather than static AWS keys;
- branch protection, CodeQL, Dependabot, dependency review, Gitleaks, production audit, and a CycloneDX SBOM;
- a data inventory, threat model, audit events, export, and deletion;
- unit, golden, integration, authorization, accessibility, browser, infrastructure, deployed-smoke, rollback, and restore tests.

The MVP collects no bank credentials, routing or account numbers, SSNs, identity documents, or real transaction data. This keeps initial breach impact low while proving the engineering controls needed before provider integration.

## Outcome and future path

A successful MVP proves that users can create a goal, understand principal versus modeled interest, activate a set-and-forget plan, and return to monitor progress. It also demonstrates full-stack engineering, explainable financial calculations, secure tenant isolation, AWS deployment, scheduled automation, CI/CD, and operational evidence.

After product validation, GoalPilot can replace the static rate source and simulation providers with approved sandbox adapters. Real connectivity will then add verified webhooks, durable provider commands, reconciliation, provider-authoritative balances, WAF, paid recovery guarantees, external security validation, and legal and commercial approval.

**Recommendation:** build to this simulation-only boundary. It preserves the core product insight, produces a credible and beautiful portfolio application, costs almost nothing at early usage, and prevents bank-partner negotiations from becoming a prerequisite for learning whether users want GoalPilot.
