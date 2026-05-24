import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "fs";
import path from "path";

const PUBLIC_MUTATING_ROUTES = new Set([
  "auth/operator-login/route.ts",
  "auth/operator-logout/route.ts"
]);

const PUBLIC_GET_ROUTES = new Set([
  "auth/operator-session/route.ts",
  "health/route.ts",
  "health/liveness/route.ts",
  "health/readiness/route.ts",
  "ops/runtime-maintenance/route.ts"
]);

function apiRoutes(dir: string): string[] {
  const entries: string[] = [];
  for (const item of readdirSync(dir)) {
    const full = path.join(dir, item);
    if (statSync(full).isDirectory()) {
      entries.push(...apiRoutes(full));
    } else if (item === "route.ts") {
      entries.push(full);
    }
  }
  return entries;
}

describe("operator access route coverage", () => {
  it("protects every mutating API route or documents it as public", () => {
    const apiRoot = path.resolve(__dirname);
    const violations: string[] = [];
    for (const file of apiRoutes(apiRoot)) {
      const relative = path.relative(apiRoot, file).replaceAll("\\", "/");
      const source = readFileSync(file, "utf-8");
      const mutatingMethods = [...source.matchAll(/export\s+(?:async\s+function|const)\s+(POST|PUT|PATCH|DELETE)\b/g)].map((match) => match[1]);
      for (const method of mutatingMethods) {
        const wrappedExport = new RegExp(`export\\s+const\\s+${method}\\s*=\\s*withOperatorAccess\\s*\\(`).test(source);
        if (!PUBLIC_MUTATING_ROUTES.has(relative) && !wrappedExport) {
          violations.push(`${relative}: ${method} handler missing exported withOperatorAccess wrapper`);
        }
      }

      const hasGet = /export\s+(?:async\s+function|const)\s+GET\b/.test(source);
      if (hasGet && !PUBLIC_GET_ROUTES.has(relative)) {
        const wrappedGet = /export\s+const\s+GET\s*=\s*withReadOnlyOperatorAccess\s*\(/.test(source);
        if (!wrappedGet) {
          violations.push(`${relative}: GET handler missing exported withReadOnlyOperatorAccess wrapper`);
        }
      }
    }
    expect(violations).toEqual([]);
  });
});
