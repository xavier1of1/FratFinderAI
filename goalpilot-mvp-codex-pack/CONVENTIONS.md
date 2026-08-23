# GoalPilot Coding Conventions

**Status:** Mandatory for humans and Codex  
**Version:** 1.0  
**Baseline date:** 2026-08-23  

These rules are part of the implementation contract. A pull request is incomplete when it violates this document, even if the code appears to work.

## 1. Operating rules for Codex

Before editing code, Codex must:

1. Read `PRD.md`, `STACK.md`, `CONVENTIONS.md`, and the active section of `TASKS.md`.
2. Inspect the repository, current branch, existing tests, and current diff.
3. Confirm the task's prerequisites are complete.
4. Identify the smallest vertical slice that satisfies the active task.
5. Run the current relevant baseline checks before making changes.

During implementation, Codex must:

- work on one task group at a time;
- remain inside the paths assigned by the task;
- make ordinary engineering decisions without asking for permission when these documents resolve them;
- stop and record a decision when a proposed change conflicts with product policy, security boundaries, real-money restrictions, or the approved stack;
- add or update tests with every behavior change;
- update documentation in the same change when contracts or operations change;
- preserve a runnable repository;
- fix root causes rather than suppressing errors;
- never reduce a quality threshold to get a passing result;
- never claim that a command passed unless it was executed successfully.

Codex must not:

- add a real financial provider;
- add Plaid, bank credentials, money movement, custody, trading, lending, or staking;
- add another framework or database;
- change exact package versions without updating `STACK.md` and recording the reason;
- place secrets in source, tests, screenshots, examples, logs, or documentation;
- mark a task complete before its stated evidence exists;
- combine unrelated refactors with a feature task;
- silently change calculation or eligibility policy;
- create a workaround when a direct permanent fix is available.

## 2. Language and module rules

- All application and infrastructure code is TypeScript.
- JavaScript is allowed only in tool-generated configuration when TypeScript is unsupported.
- Use ESM imports and exports.
- Use named exports by default.
- Default exports are allowed only when a framework convention requires them.
- Use `import type` for type-only imports.
- Do not use CommonJS `require` in application code.
- Do not use barrel files across domain boundaries. A package may have one intentional public `index.ts` API.
- Internal files may not be imported through deep paths from another package.
- Circular dependencies are prohibited.

## 3. Repository boundaries

### 3.1 Allowed dependency direction

```text
apps/web
  → packages/ui
  → packages/contracts

apps/api
  → packages/auth
  → packages/data-access
  → packages/provider-ports
  → packages/contracts
  → packages/domain
  → packages/observability

packages/provider-simulators
  → packages/provider-ports
  → packages/contracts
  → packages/domain

packages/data-access
  → packages/contracts
  → packages/domain

infra/cdk
  → no application package except build artifact entry points
```

### 3.2 Prohibited dependencies

`packages/domain` must not import:

- React;
- Fastify;
- Drizzle;
- AWS SDKs;
- Cognito;
- environment variables;
- databases;
- file systems;
- HTTP clients;
- provider adapters;
- AI libraries.

`packages/contracts` must not import application packages.

`apps/web` must not import database, AWS, provider, or backend implementation packages.

`packages/provider-ports` contains interfaces and provider-neutral value types only. Provider-specific SDK types may not cross this boundary.

## 4. Naming conventions

### Files and folders

| Item | Convention | Example |
|---|---|---|
| folders | kebab-case | `recommendation-engine` |
| ordinary TypeScript files | kebab-case | `calculate-required-contribution.ts` |
| React components | PascalCase file | `GoalProgressCard.tsx` |
| tests | subject plus `.test.ts` or `.test.tsx` | `goal-policy.test.ts` |
| integration tests | `.integration.test.ts` | `goal-repository.integration.test.ts` |
| Playwright tests | `.spec.ts` | `goal-builder.spec.ts` |
| migrations | timestamp plus description | `202608230001_create_goals.sql` |

### TypeScript identifiers

| Item | Convention | Example |
|---|---|---|
| variables and functions | camelCase | `calculateEndingBalance` |
| classes and types | PascalCase | `GoalProjection` |
| interfaces | PascalCase, no `I` prefix | `RateProvider` |
| constants | camelCase unless truly global | `maxGoalAmountCents` |
| environment keys | SCREAMING_SNAKE_CASE | `DATABASE_URL` |
| stable error codes | SCREAMING_SNAKE_CASE | `GOAL_NOT_FOUND` |
| database tables and columns | snake_case | `vehicle_assumption_versions` |
| route parameters | camelCase | `goalId` |
| event names | past tense dot notation | `goal.plan_activated` |

Avoid vague names such as `data`, `item`, `thing`, `manager`, `helper`, `utils`, `common`, or `misc` unless the scope makes the meaning unambiguous.

## 5. TypeScript standards

The compiler runs in strict mode with the options in `STACK.md`.

Mandatory rules:

- `any` is prohibited. Use `unknown` and narrow it.
- Type assertions are exceptional and must be justified by a nearby comment or boundary validator.
- Non-null assertions are prohibited outside tests unless a preceding invariant check makes the value logically certain.
- Use discriminated unions for finite states.
- Use exhaustive `switch` statements with an `assertNever` function.
- Prefer immutable values and `readonly` fields.
- Avoid mutable module-level state.
- Prefer pure functions for domain calculations.
- Return a new object instead of mutating an input.
- Public functions require explicit parameter and return types.
- Do not use numeric enums. Use string-literal unions or `as const` maps.
- Do not use `Date` as an implicit global clock inside domain logic. Pass `asOfDate` explicitly.
- Validate all values crossing process, HTTP, database, provider, or configuration boundaries.

Example state type:

```ts
type GoalStatus =
  | 'draft'
  | 'active'
  | 'paused'
  | 'purchase_ready'
  | 'completed'
  | 'archived';
```

## 6. Money, rates, dates, and financial calculations

### Money

- Store and transport USD money as integer cents.
- Database type is `bigint`.
- API JSON uses a string for values that may exceed safe JavaScript integer range, otherwise a contract-approved integer. Use one representation consistently.
- Domain code uses a branded `MoneyCents` type.
- Never store money as `float`, `double precision`, or a JavaScript decimal number with fractional cents.
- Formatting belongs at the presentation boundary.

### Rates

- Store simple rates in integer basis points when possible.
- Use `decimal.js` only for exact division, compounding, or fractional-rate calculations.
- Configure one explicit precision and rounding mode in the domain package.
- Convert to cents only at documented posting boundaries.
- Never use an advertised APY to infer a guaranteed outcome.

### Dates

- Calendar target dates use `YYYY-MM-DD` and have no timezone.
- Events and audit times use UTC `timestamptz`.
- The domain receives an explicit processing date.
- Weekly and biweekly schedules preserve the selected weekday.
- Monthly schedules define month-end behavior in tests.
- Leap years, daylight-saving transitions, short months, and target-date boundaries require tests.

### Calculation behavior

- Equal normalized input, assumptions, and processing date must yield equal output.
- Calculation functions return both result and trace metadata.
- Every policy rejection uses a stable code and human-readable message mapping.
- The zero-interest baseline is calculated before any vehicle result.
- Interest is separated from contributed principal in every result.
- When a goal is behind, the engine recommends contribution, deadline, or target changes before any future increase in risk.

## 7. Validation and contracts

- Zod schemas are the runtime source of truth for HTTP and shared contracts.
- Input and output schemas are separate when server-generated fields differ.
- Do not call `.passthrough()` on financial or authentication schemas.
- Reject unknown fields on write endpoints.
- Normalize strings, dates, money, and enums once at the boundary.
- Domain functions receive validated normalized types, not raw HTTP values.
- OpenAPI must be generated from or tested against the same route schemas.
- Schema changes require API tests and, where applicable, migration notes.

## 8. Backend layering

Each backend feature follows:

```text
route
→ application service/use case
→ domain policy or calculation
→ repository/provider port
→ adapter
```

### Route handlers

Route handlers may:

- read authenticated context;
- validate route parameters, query, headers, and body;
- invoke one application use case;
- map success to an HTTP response;
- allow centralized error mapping to handle failures.

Route handlers may not:

- contain financial calculations;
- compose SQL;
- call Drizzle directly;
- trust ownership fields from the request;
- catch and discard errors;
- return database records directly.

### Application services

Application services coordinate a use case and transaction. They enforce authorization before a protected mutation and call domain functions for business decisions.

### Repositories

- Every protected repository method requires the authenticated internal user ID.
- Ownership belongs in the SQL predicate, not only in application memory.
- Return `null` when an owned record does not exist.
- Cross-user and missing records map to the same `404` behavior.
- Repositories return domain or persistence DTOs, never HTTP responses.
- Queries must select only required columns.

## 9. Database conventions

- All schema changes use committed SQL migrations.
- Never use schema push in shared or deployed environments.
- Migrations are forward-safe and reviewed.
- Destructive migrations require an expand, migrate, contract sequence.
- Foreign keys and check constraints enforce important invariants.
- Use unique constraints for idempotency and schedule occurrence IDs.
- Use transactions for multi-record state changes.
- Keep transaction callbacks short and free of network calls.
- Never call AWS, OAuth, or another provider inside a database transaction.
- Use indexes based on actual query patterns.
- `created_at` and `updated_at` are UTC timestamps.
- Append-only tables do not expose update methods.
- Seed data is deterministic and versioned.

## 10. Authentication, authorization, and sessions

- Cognito authenticates identity. GoalPilot authorizes every operation.
- The API derives the Cognito subject from a verified server-side session.
- Never accept `userId`, `ownerId`, or Cognito subject from a write body.
- Session cookies are `HttpOnly`, `Secure`, `SameSite=Lax`, and use the narrowest possible path and domain.
- Session identifiers are cryptographically random and stored only as hashes.
- OAuth state, nonce, and PKCE transactions are single-use and expire quickly.
- Refresh-token material is encrypted at rest and never logged.
- State-changing browser requests require CSRF validation and Origin checking.
- Authorization failure for a resource identifier returns 404 unless the endpoint is explicitly administrative.
- Administrative authorization is denied by default.

## 11. Error handling

Define one error hierarchy:

```ts
abstract class AppError extends Error {
  abstract readonly code: ErrorCode;
  abstract readonly httpStatus: number;
  readonly safeDetails?: Readonly<Record<string, unknown>>;
}
```

Approved categories:

- `ValidationError` - 400;
- `AuthenticationRequiredError` - 401;
- `ForbiddenOperationError` - 403 only for non-resource policy denials;
- `ResourceNotFoundError` - 404;
- `ConflictError` - 409;
- `RateLimitError` - 429;
- `ExternalDependencyError` - 502 or 503;
- `InvariantViolationError` - 500, never exposes details;
- `ConfigurationError` - startup failure.

Rules:

- Do not throw raw strings.
- Do not leak stack traces, SQL, tokens, internal IDs, configuration, or dependency messages to clients.
- Unexpected errors receive a correlation ID and generic `INTERNAL_ERROR` response.
- User-correctable errors provide safe field-level guidance.
- Catch an error only when adding context, translating it, retrying under a bounded policy, or performing required cleanup.
- Never write `catch {}`.

API error shape:

```json
{
  "error": {
    "code": "GOAL_NOT_FOUND",
    "message": "The requested goal was not found.",
    "requestId": "01K...",
    "fieldErrors": null
  }
}
```

## 12. Idempotency

State-changing operations identified in `PRD.md` require an `Idempotency-Key` header.

Rules:

- Scope the key to authenticated user plus operation.
- Hash the normalized request.
- Replay the stored successful response for the same key and request hash.
- Return 409 when the same key is reused with a different request.
- Use a unique database constraint, not an in-memory check.
- Scheduled simulation events use deterministic occurrence IDs.
- Never delete idempotency records before their documented expiration.

## 13. Logging and observability

Use structured Pino logs.

Every request log may include:

- timestamp;
- level;
- service;
- environment;
- request ID;
- route template;
- method;
- response status;
- duration;
- anonymous internal user reference only when required;
- stable error code.

Never log:

- authorization headers;
- cookies;
- OAuth codes;
- access, ID, or refresh tokens;
- PKCE verifiers;
- CSRF tokens;
- passwords or MFA codes;
- email addresses in routine logs;
- goal names or notes;
- complete request or response bodies;
- database URLs;
- secrets;
- dollar amounts unless aggregated and expressly approved.

All redaction is tested. `console.log` is prohibited in application code.

## 14. API conventions

- Base path is `/api/v1`.
- Resource paths use plural nouns.
- Actions use explicit subresources only when normal CRUD is insufficient.
- JSON uses camelCase.
- HTTP status codes are intentional and tested.
- Create returns 201 plus canonical representation.
- Delete or archive returns 204 when no body is needed.
- Async work returns 202 with a status URL.
- Pagination uses cursor-based pagination.
- Dates are ISO strings.
- Responses are DTOs, not persistence objects.
- Health and readiness endpoints expose no secrets or dependency credentials.
- Swagger UI is development-only unless protected.

## 15. React conventions

### Components

- Prefer small feature components with clear inputs.
- A component does not fetch data and render a large page unless it is a route-level container.
- Shared primitive components live in `packages/ui`.
- Feature components remain with the feature.
- Do not edit generated shadcn source blindly. Adapt it to GoalPilot conventions and test it.
- Use semantic HTML before ARIA.
- Do not create generic wrappers that only rename an HTML element.

### State

- Server state uses TanStack Query.
- Form state uses React Hook Form plus Zod.
- URL state uses React Router search parameters where shareable.
- Local UI state uses React state.
- Do not add Redux, Zustand, MobX, or another global store without an ADR.
- Derived values are computed, not duplicated in state.
- Financial calculations are never reproduced in the browser. Preview numbers come from the API.

### Data fetching

- All requests pass through one typed API client.
- Query keys are feature-owned factories.
- Mutations invalidate or update the narrowest correct cache.
- Abort requests on navigation where appropriate.
- Display loading, error, empty, stale, and success states.
- Never hide an API error only in the browser console.

### Forms

- Every field has a visible label.
- Helper text precedes errors in reading order.
- On submission failure, focus the error summary or first invalid field.
- Buttons state the action, such as `Create goal`, not `Submit`.
- Destructive actions require an explicit confirmation dialog.
- Currency input is parsed to cents at the boundary; do not store formatted strings as domain values.

## 16. Accessibility and design

- Target WCAG 2.2 AA for critical flows.
- All functionality is keyboard operable.
- Focus indicators are always visible.
- Modal focus is trapped and restored.
- Text contrast meets AA.
- Color is never the sole meaning carrier.
- Respect `prefers-reduced-motion`.
- Charts have a table or textual summary.
- Icons used as controls have accessible names.
- Decorative icons are hidden from assistive technology.
- Touch targets are at least 44 by 44 CSS pixels where practical.
- Test at 360, 768, 1024, and 1440 pixel widths.
- Do not use animation to delay access to financial results.

## 17. CSS and visual system

- Use design tokens rather than arbitrary repeated colors.
- Primary brand starts from British racing green, but contrast determines actual foreground pairings.
- Do not place low-contrast green text on dark green.
- Use muted gold only as an accent, never the sole indicator of warning or success.
- Avoid gradients unless they add hierarchy and remain subtle.
- Avoid excessive glassmorphism, neon glow, and dashboard clutter.
- Use one chart accent palette defined in tokens.
- Tailwind arbitrary values are allowed only when a token is genuinely inappropriate.
- Use the `cn` helper for class composition.
- No inline style objects except calculated SVG/chart geometry or unavoidable third-party integration.

## 18. Testing conventions

### Test pyramid

1. Pure unit tests for domain functions.
2. Property and golden tests for calculations.
3. PostgreSQL integration tests for repositories and migrations.
4. Fastify injection tests for API behavior.
5. React behavior tests for components.
6. Playwright tests for critical journeys.
7. CDK assertions and synthesis tests.
8. Deployed smoke tests.

### Rules

- Test behavior, not private implementation.
- Every defect receives a regression test.
- A test name states the expected behavior.
- Tests use deterministic clocks, assumptions, and identifiers.
- No real network calls in unit or integration tests.
- Fixtures contain synthetic data only.
- Do not use snapshots for financial results or complex business behavior.
- Avoid brittle selectors; Playwright uses roles, labels, and visible names.
- Do not use arbitrary sleeps. Wait for observable conditions.
- Flaky tests are defects and may not be retried indefinitely.

### Minimum quality expectations

- Domain package: at least 90 percent meaningful line and branch coverage, plus every documented invariant.
- API and data packages: at least 85 percent meaningful coverage.
- Every protected resource: positive and cross-user negative test.
- Every state transition: valid and invalid transition test.
- Every critical web journey: Playwright coverage.
- Every financial number displayed in a critical flow: API-to-UI consistency assertion.

## 19. Security conventions

- Threat model all new trust boundaries.
- Apply least privilege to IAM.
- Use GitHub OIDC, never static AWS keys.
- Use SSM parameters or deployment secret stores, never committed configuration.
- Validate environment variables at startup and fail closed.
- Use a fixed CORS allowlist.
- Add CSP and other security headers.
- API Gateway throttling is authoritative; in-process rate limiting is supplemental.
- Do not trust headers from the internet unless API Gateway or CloudFront overwrites them.
- Protect against IDOR, injection, XSS, CSRF, open redirects, SSRF, unsafe deserialization, and resource exhaustion.
- There are no arbitrary URL-fetch features in the MVP.
- No user-uploaded files in the MVP.
- Dependency additions require license and security review.
- Critical or high production dependency vulnerabilities block release unless a written exception is approved.

## 20. Configuration conventions

- Parse environment configuration once at startup.
- Export a typed immutable configuration object.
- No direct `process.env` reads outside the configuration module.
- Separate local, test, staging, and production configuration.
- Never default to insecure production behavior.
- `AUTH_MODE=development` is rejected when `ENVIRONMENT` is not local or test.
- Every external resource name includes project and environment.
- Feature flags default off.

## 21. Git and pull request conventions

Branch names:

```text
feat/gp-###-short-description
fix/gp-###-short-description
chore/gp-###-short-description
```

Commit subjects use imperative Conventional Commits:

```text
feat(goals): add simulated contribution ledger
fix(auth): reject reused OAuth state
chore(ci): generate CycloneDX SBOM
```

Pull requests must include:

- task IDs;
- product outcome;
- files and contracts changed;
- exact tests executed;
- screenshots for visual changes;
- accessibility impact;
- security and privacy impact;
- database migration impact;
- infrastructure and cost impact;
- rollback behavior;
- known limitations.

No direct pushes to `main`. One non-author review and all required checks are mandatory.

## 22. Documentation conventions

- Documentation describes current behavior, not aspiration, unless labeled future.
- Every command must be copyable.
- Do not claim a deployment or test exists without evidence.
- Update OpenAPI with route changes.
- Update the ERD with schema changes.
- Update `TASKS.md` only after exit checks pass.
- Use plain factual language.
- Avoid marketing claims in implementation documentation.
- Record an ADR for changes to architecture, financial policy, authentication, persistence, or cloud topology.

## 23. Definition of done for any task

A task is complete only when:

1. Acceptance criteria are satisfied.
2. Code follows module boundaries.
3. Relevant tests were added and pass.
4. Strict type checking passes.
5. Lint and formatting pass with no warnings.
6. Documentation and contracts are current.
7. Security and privacy implications were reviewed.
8. No unrelated diff remains.
9. Evidence is recorded in the pull request or task section.
10. The non-author review is complete.

An implementation that merely works on the author's machine is not done.