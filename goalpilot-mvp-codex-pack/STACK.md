# GoalPilot MVP Technology Stack

**Status:** Approved stack baseline for Codex  
**Version:** 1.0  
**Pinned on:** 2026-08-23  
**Decision goal:** Lowest practical MVP cost while preserving security, full-stack depth, AWS experience, PostgreSQL portability, and a clean path to real financial-provider integrations.

## 1. Architecture decision

GoalPilot will use a TypeScript monorepo containing a React single-page application, a Fastify modular API, a pure calculation domain, PostgreSQL persistence, AWS serverless hosting, and provider ports with simulation adapters.

```text
Browser
  → CloudFront
     → private S3 origin for React assets
     → /api and /auth to API Gateway HTTP API
        → Lambda running Fastify
           → Neon PostgreSQL
           → Cognito OAuth endpoints
           → scheduled simulation handler
```

Scheduled demo activity:

```text
EventBridge Scheduler
  → simulation Lambda handler
     → PostgreSQL ledger and schedule records
```

This replaces the earlier fixed-cost beta topology of ECS Fargate, an Application Load Balancer, NAT Gateway, WAF, and RDS. Fastify remains transport-portable so the API can move to ECS later without rewriting routes or domain code.

## 2. Cost posture

### Target at MVP volume

| Item | MVP target | Notes |
|---|---:|---|
| React hosting through S3 and CloudFront | $0–$2/month | Usually within low-traffic/free allowances; usage dependent. |
| API Gateway HTTP API and Lambda | $0–$2/month | Scales to zero; usage and free-tier status vary by account. |
| Cognito | $0 at small test volume | Confirm current MAU pricing before public release. |
| EventBridge Scheduler | Approximately $0 | One low-frequency schedule. |
| CloudWatch | $0–$2/month | Structured, low-volume logs with short retention. |
| SSM Parameter Store standard parameters | $0 target | Avoid Secrets Manager's per-secret fixed charge for MVP. |
| Neon PostgreSQL | $0 target | Free plan; storage, compute, sleep, backup, and connection limits must be monitored. |
| GitHub Actions | $0 target | Private-repository allowances depend on the account plan. |
| Domain | Optional, roughly annual registration cost | CloudFront URL works before a domain is purchased. |
| **Expected tiny-MVP infrastructure** | **Approximately $0–$6/month plus domain** | A target, not a guarantee. Set AWS budgets before deployment. |

Cost can increase because of traffic, logs, database usage, free-tier expiration, custom domains, build minutes, data transfer, or provider-plan changes. Codex must not describe the MVP as permanently free.

## 3. Runtime and package management

| Tool | Exact baseline | Decision |
|---|---:|---|
| Node.js | `24.19.0` LTS | Local, CI, Lambda runtime family and Dev Container. |
| pnpm | `11.22.0` | Workspace management and deterministic lockfile. |
| TypeScript | `6.0.3` | Chosen instead of the newer TypeScript 7 line for broader tooling compatibility. |
| Module format | ESM | `"type": "module"` in all packages. |
| Package policy | Exact direct versions | Do not use `^`, `~`, `latest`, or wildcard versions. |
| Lockfile | `pnpm-lock.yaml` committed | CI uses `pnpm install --frozen-lockfile`. |

Before writing application code, Codex must execute a compatibility spike that installs this exact set, runs type checking, builds the web/API/infra packages, and records any necessary correction in `STACK.md`. It may not silently substitute a package.

## 4. Repository structure

```text
goalpilot/
├── PRD.md
├── STACK.md
├── CONVENTIONS.md
├── TASKS.md
├── MVP_PROJECT_REPORT.md
├── package.json
├── pnpm-lock.yaml
├── pnpm-workspace.yaml
├── tsconfig.base.json
├── apps/
│   ├── web/
│   └── api/
├── packages/
│   ├── contracts/
│   ├── domain/
│   ├── data-access/
│   ├── auth/
│   ├── observability/
│   ├── provider-ports/
│   ├── provider-simulators/
│   ├── test-support/
│   └── ui/
├── infra/
│   └── cdk/
├── tests/
│   ├── e2e/
│   ├── security/
│   └── fixtures/
├── scripts/
└── .github/
    └── workflows/
```

## 5. Frontend stack

### 5.1 Core

| Package | Exact version | Purpose |
|---|---:|---|
| `react` | `19.2.8` | UI runtime. |
| `react-dom` | `19.2.8` | DOM renderer. |
| `vite` | `8.2.1` | Development and production build. |
| `@vitejs/plugin-react` | `6.1.0` | React Fast Refresh and JSX integration. |
| `react-router` | `8.3.0` | Client routing. |
| `@tanstack/react-query` | `5.101.4` | Server-state cache and mutations. |
| `react-hook-form` | `7.85.0` | Accessible, performant form state. |
| `@hookform/resolvers` | `5.9.1` | Zod form integration. |
| `zod` | `4.4.3` | Shared runtime schemas. |

### 5.2 Design system and visualization

| Package/resource | Exact version | Use |
|---|---:|---|
| `tailwindcss` | `4.3.3` | Design tokens and utility styling. |
| `@tailwindcss/vite` | `4.3.3` | Tailwind Vite integration. |
| `shadcn` CLI | `4.19.0` | One-time component source generation; generated code is owned by the repository. |
| `class-variance-authority` | `0.7.1` | Component variants. |
| `clsx` | `2.1.1` | Conditional class names. |
| `tailwind-merge` | `3.6.0` | Resolve Tailwind class conflicts. |
| `lucide-react` | `1.33.0` | Open-source icons. |
| `recharts` | `3.10.1` | Progress and projection charts. |
| `motion` | `13.1.1` | Restrained transitions and micro-interactions. |
| `date-fns` | `4.1.0` | Calendar calculations and presentation; domain date rules remain separately tested. |

Use only the shadcn/ui components needed for the MVP: button, card, input, label, select, radio group, checkbox, dialog, alert dialog, tooltip, progress, tabs, table, skeleton, toast, dropdown menu, sheet, and form helpers. Do not install a second component system.

### 5.3 Free design resources

These are permitted references or dependencies:

- shadcn/ui component source;
- Radix primitives installed by selected shadcn components;
- Lucide icons;
- Tailwind CSS;
- Recharts;
- Motion's free MIT-licensed APIs;
- Figma Community wireframes and finance-dashboard examples for inspiration only;
- Google Fonts or system fonts only when licensing and performance are documented.

Do not paste an entire community template into the product. Build a GoalPilot-specific system with:

- British racing green primary;
- warm ivory surfaces;
- muted gold accent;
- charcoal body text;
- AA contrast;
- responsive components;
- reduced-motion support.

## 6. Backend stack

| Package | Exact version | Purpose |
|---|---:|---|
| `fastify` | `5.11.3` | HTTP application framework. |
| `@fastify/aws-lambda` | `6.4.1` | API Gateway/Lambda adapter while preserving local Fastify execution. |
| `@fastify/cors` | `11.3.0` | Explicit origin allowlist. |
| `@fastify/helmet` | `13.1.1` | Security response headers. |
| `@fastify/rate-limit` | `11.2.0` | Supplemental per-instance rate limits; API Gateway remains the distributed throttle. |
| `@fastify/swagger` | `9.8.1` | OpenAPI generation from route schemas. |
| `@fastify/swagger-ui` | `6.1.1` | Development-only API reference. Disabled in production unless authenticated. |
| `@fastify/sensible` | `6.0.5` | Narrow HTTP utility set. |
| `zod` | `4.4.3` | Domain and request validation. |
| `aws-jwt-verify` | `5.2.1` | Cognito JWT verification. |
| `drizzle-orm` | `0.45.2` | Typed SQL and repository implementation. |
| `@neondatabase/serverless` | `1.1.0` | Serverless PostgreSQL driver. |
| `decimal.js` | `10.6.0` | Exact rate arithmetic where integer rational math is impractical. Money remains integer cents. |
| `ulid` | `3.0.2` | Sortable opaque application identifiers. |
| `pino` | `10.3.1` | Structured JSON logging with redaction. |

The API has two entry points:

- ordinary local Node server;
- AWS Lambda handler wrapping the same Fastify instance.

Business logic must not import Lambda event types.

## 7. Database

| Choice | Baseline |
|---|---|
| Engine | PostgreSQL major version 17 |
| Development | Docker image `postgres:17-alpine`; minor patches float within major for security updates |
| Hosted MVP | Neon serverless PostgreSQL free plan |
| ORM | Drizzle ORM `0.45.2` |
| Migration tool | Drizzle Kit `0.31.10` |
| Migration format | Reviewed SQL committed to `packages/data-access/migrations` |
| Identifier | ULID text with check constraints or PostgreSQL UUID where a provider requires UUID |
| Money | `bigint` cents, never PostgreSQL floating point |
| Rates | integer basis points where possible; exact numeric/decimal for fractional calculations |
| Time | `timestamptz` in UTC |
| Calendar dates | PostgreSQL `date` |

Why Neon:

- zero-idle-cost target for the MVP;
- PostgreSQL-compatible relational model;
- serverless driver suitable for Lambda;
- avoids an always-on RDS instance before product validation.

Tradeoffs:

- free plans can sleep, throttle, cap storage, or change;
- cold database compute can add latency;
- cross-provider network latency exists;
- production financial integrations may require a paid plan, stronger retention, private networking, or migration to RDS/Aurora.

Portability requirements:

- no Neon-specific SQL in domain repositories unless isolated;
- all schema changes are normal PostgreSQL migrations;
- backups and data export are documented;
- migration to RDS must not require a domain rewrite.

## 8. Authentication and session design

Use Amazon Cognito User Pools with the hosted authorization endpoint, authorization-code flow, and PKCE.

Recommended web flow:

```text
Browser requests /auth/login
→ API creates state, nonce and PKCE transaction
→ browser redirects to Cognito
→ Cognito redirects to /auth/callback
→ API exchanges code server-side
→ API validates tokens
→ API stores encrypted refresh-token/session material in PostgreSQL
→ browser receives opaque HttpOnly Secure SameSite=Lax session cookie
```

Requirements:

- refresh tokens never enter JavaScript-accessible storage;
- session IDs are random and stored as hashes;
- state-changing routes require CSRF protection and Origin validation;
- sessions have absolute and idle expiration;
- sign-out revokes or invalidates session state;
- TOTP MFA is supported in infrastructure;
- local development uses a clearly isolated test-auth adapter only when `AUTH_MODE=development` and deployment synthesis rejects that mode.

## 9. Calculation implementation

`packages/domain` is pure TypeScript and may depend only on:

- `decimal.js`;
- selected `date-fns` pure helpers, wrapped behind local date utilities;
- `zod` for boundary schemas.

It may not import:

- Fastify;
- React;
- Drizzle;
- AWS SDKs;
- environment variables;
- databases;
- provider SDKs;
- AI libraries.

Money is represented as branded integer cents. Rate calculations use explicit conventions and rounding modes. Every calculation takes an `asOfDate` and assumption version; it never reads the system clock internally.

## 10. AWS services

### Required for MVP deployment

| Service | Purpose |
|---|---|
| S3 | Private React build origin. |
| CloudFront | HTTPS delivery, caching, SPA fallback, and single public hostname. |
| ACM | Certificate when a custom domain is introduced. |
| API Gateway HTTP API | Low-cost `/api` and `/auth` ingress, CORS and throttling. |
| Lambda | Fastify API and scheduled simulation processing. |
| Cognito User Pools | Registration, authentication, password reset and optional MFA. |
| EventBridge Scheduler | Low-frequency simulated contribution processing. |
| CloudWatch Logs and alarms | Structured logs, errors, invocation metrics and alarms. |
| SSM Parameter Store | Standard SecureString configuration for database URL and OAuth client secret. |
| AWS Budgets | Cost notifications before deployment usage grows. |
| CDK and CloudFormation | Repeatable infrastructure. |
| IAM and STS | Least-privilege roles and GitHub OIDC deployment. |

### Explicitly omitted from MVP

- ECS Fargate;
- Application Load Balancer;
- NAT Gateway;
- RDS or Aurora;
- ElastiCache or Redis;
- OpenSearch;
- DynamoDB;
- RDS Proxy;
- Step Functions;
- paid VPC interface endpoints;
- Bedrock;
- WAF;
- multi-region deployment.

WAF becomes mandatory before broad public access or any real financial-provider integration. It is deferred only to avoid a fixed cost during the simulation MVP.

## 11. Infrastructure as code

| Package | Exact version |
|---|---:|
| `aws-cdk-lib` | `2.266.0` |
| `constructs` | `10.8.0` |
| CDK CLI | `2.1135.1` |
| `esbuild` | `0.28.2` |

CDK stacks:

1. `GoalPilotEdgeStack`
   - S3, CloudFront, optional ACM integration and security headers.
2. `GoalPilotIdentityStack`
   - Cognito pool, clients and hosted-domain configuration.
3. `GoalPilotApiStack`
   - HTTP API, Lambda functions, schedules, log groups, alarms, parameters and permissions.
4. `GoalPilotDeploymentStack` or deployment construct
   - optional GitHub OIDC role and artifact permissions.

Stateful resources receive explicit retention and removal policies. Development and production configurations are separate. Production deletion protection is enabled where supported.

## 12. Testing stack

| Package | Exact version | Purpose |
|---|---:|---|
| `vitest` | `4.1.10` | Unit and integration test runner. |
| `@playwright/test` | `1.62.1` | Browser end-to-end testing. |
| `@testing-library/react` | `16.3.0` | React behavior tests. |
| `@testing-library/jest-dom` | `6.9.1` | Accessible DOM assertions. |
| `jsdom` | `27.0.1` | Component-test browser environment. |
| `@axe-core/playwright` | `4.10.2` | Automated accessibility checks. |

Test environments:

- pure unit tests without services;
- PostgreSQL integration tests using Docker;
- API tests through Fastify `inject`;
- browser tests against local web/API;
- CDK assertion and synthesis tests;
- deployed smoke tests against a nonproduction URL.

Do not mock the calculation engine in API integration tests. Do not mock the API in the critical end-to-end journey.

## 13. Developer tooling

Baseline direct development dependencies:

| Package | Exact version |
|---|---:|
| `eslint` | `10.8.1` |
| `typescript-eslint` | `8.67.0` |
| `prettier` | `3.6.2` |
| `tsx` | `4.23.12` |
| `drizzle-kit` | `0.31.10` |
| `shadcn` | `4.19.0` |

Repository tooling:

- EditorConfig;
- ESLint flat configuration;
- Prettier;
- TypeScript project references;
- Husky is intentionally omitted; CI is authoritative and optional local Git hooks can be documented without becoming a required dependency;
- Changesets is omitted until packages are published;
- Turborepo or Nx is omitted; pnpm workspaces and TypeScript references are enough at this scale.

## 14. TypeScript configuration

Base requirements:

```json
{
  "compilerOptions": {
    "target": "ES2023",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "useUnknownInCatchVariables": true,
    "noImplicitOverride": true,
    "noFallthroughCasesInSwitch": true,
    "noPropertyAccessFromIndexSignature": true,
    "verbatimModuleSyntax": true,
    "isolatedModules": true,
    "forceConsistentCasingInFileNames": true,
    "skipLibCheck": false
  }
}
```

Web configuration may use `moduleResolution: "Bundler"` as required by Vite, but shared/backend packages use NodeNext. Every package has its own `tsconfig.json` extending the base.

## 15. CI and security tooling

GitHub Actions must run:

1. frozen dependency install;
2. formatting check;
3. zero-warning lint;
4. strict type checking;
5. unit and integration tests;
6. coverage;
7. production builds;
8. PostgreSQL migrations against an empty test database;
9. Playwright critical flows;
10. CDK tests and synthesis;
11. CodeQL;
12. GitHub dependency review;
13. `pnpm audit --prod`;
14. Gitleaks secret scan;
15. CycloneDX SBOM generation;
16. artifact upload for test results and SBOM.

Deployment uses GitHub OIDC to assume an AWS role restricted by repository, branch and environment. No AWS access key is stored in GitHub.

## 16. Scaling path

### Stage A: simulation MVP

- Lambda/API Gateway;
- Neon free PostgreSQL;
- one scheduled job;
- static assumptions;
- no provider credentials.

### Stage B: public portfolio release

- paid Neon tier if required;
- WAF;
- stronger alarms and backups;
- custom domain;
- penetration testing appropriate to public exposure.

### Stage C: provider sandbox

- provider adapters;
- webhook inbox;
- transactional outbox;
- provider operation records;
- reconciliation;
- SQS FIFO for financial commands;
- legal and vendor review.

### Stage D: live financial beta

- selected sponsor-bank or custodian program;
- production KYC and funding;
- paid database with verified recovery objectives;
- WAF and threat monitoring;
- independent security validation;
- human operational review;
- compliance-approved language and procedures.

### Stage E: sustained scale

Move API or workers to ECS only when measured evidence supports it. Migrate PostgreSQL to RDS/Aurora only when contracts, networking, workload, recovery, or cost justify it. Domain, contracts and repositories remain unchanged.

## 17. Version update policy

- Exact direct versions are committed.
- Dependabot may open patch and minor updates weekly.
- No automatic merge for production dependencies.
- Every update runs the complete compatibility suite.
- Major upgrades require an ADR and migration notes.
- Security fixes take priority over version freeze.
- `STACK.md` and the lockfile must agree.
- Codex must never replace a library because it prefers a different tool without explicit approval.

## 18. Final stack rationale

This stack demonstrates:

- a polished React and TypeScript application;
- a production-structured Node/Fastify backend;
- PostgreSQL schema design and migrations;
- deterministic financial-domain engineering;
- authentication and tenant isolation;
- serverless AWS architecture;
- infrastructure as code;
- scheduled cloud processing;
- observability, CI/CD and security controls;
- a provider-adapter path to the long-term product.

It avoids paying for mature-platform infrastructure before GoalPilot proves that users value the simulated set-and-forget goal experience.