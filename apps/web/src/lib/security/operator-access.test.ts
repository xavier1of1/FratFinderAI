import { beforeEach, describe, expect, it, vi } from "vitest";

const query = vi.fn(async () => ({ rowCount: 1 }));

vi.mock("@/lib/db", () => ({
  getDbPool: () => ({ query })
}));

describe("operator access", () => {
  beforeEach(() => {
    vi.resetModules();
    query.mockClear();
    vi.stubEnv("NODE_ENV", "test");
    vi.stubEnv("WEB_AUTH_DISABLED", "false");
    vi.stubEnv("WEB_ADMIN_TOKEN", "admin-token-value");
    vi.stubEnv("WEB_OPERATOR_TOKEN", "operator-token-value");
    vi.stubEnv("WEB_ANALYST_TOKEN", "analyst-token-value");
    vi.stubEnv("WEB_OPERATOR_SESSION_SECRET", "test-session-secret-long-enough");
  });

  it("denies anonymous mutating requests and records a denied audit event", async () => {
    const { requireRole } = await import("./operator-access");
    const result = await requireRole(new Request("http://localhost/api/chapters/actions", { method: "POST" }), ["admin"], "chapter_delete");

    expect(result.ok).toBe(false);
    expect(query).toHaveBeenCalled();
    const calls = query.mock.calls as unknown as unknown[][];
    expect(calls[0]?.[1]).toContain("denied");
  });

  it("allows analysts to read when analyst role is accepted", async () => {
    const { requireRole } = await import("./operator-access");
    const request = new Request("http://localhost/api/chapters", {
      headers: { Authorization: "Bearer analyst-token-value" }
    });
    const result = await requireRole(request, ["analyst", "operator", "admin"], "dashboard_read");

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.operator.role).toBe("analyst");
    }
  });

  it("denies operators from admin-only actions", async () => {
    const { requireRole } = await import("./operator-access");
    const request = new Request("http://localhost/api/chapters/actions", {
      method: "POST",
      headers: { Authorization: "Bearer operator-token-value" }
    });
    const result = await requireRole(request, ["admin"], "chapter_delete");

    expect(result.ok).toBe(false);
    const calls = query.mock.calls as unknown as unknown[][];
    expect(calls.at(-1)?.[1]).toContain("operator_role_denied");
  });

  it("denies invalid bearer tokens and records a denied audit event", async () => {
    const { requireRole } = await import("./operator-access");
    const request = new Request("http://localhost/api/fraternity-crawl-requests", {
      method: "POST",
      headers: { Authorization: "Bearer invalid-token-value" }
    });
    const result = await requireRole(request, ["operator", "admin"], "crawl_request_create");

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.response.status).toBe(401);
    }
    const calls = query.mock.calls as unknown as unknown[][];
    expect(calls.at(-1)?.[1]).toContain("operator_auth_required");
  });

  it("allows admins to perform admin-only actions", async () => {
    const { requireRole } = await import("./operator-access");
    const request = new Request("http://localhost/api/chapters/actions", {
      method: "POST",
      headers: { Authorization: "Bearer admin-token-value" }
    });
    const result = await requireRole(request, ["admin"], "chapter_delete");

    expect(result.ok).toBe(true);
  });

  it("writes allowed audit events for successful protected handlers", async () => {
    const { withOperatorAccess } = await import("./operator-access");
    const handler = withOperatorAccess(["operator", "admin"], "crawl_request_create", { targetType: "fraternity_crawl_request" }, async () => {
      return Response.json({ ok: true }, { status: 202 });
    });
    const response = await handler(
      new Request("http://localhost/api/fraternity-crawl-requests", {
        method: "POST",
        headers: { Authorization: "Bearer operator-token-value" }
      })
    );

    expect(response.status).toBe(202);
    const calls = query.mock.calls as unknown as unknown[][];
    expect(calls.at(-1)?.[1]).toContain("allowed");
    expect(calls.at(-1)?.[1]).toContain("crawl_request_create");
  });

  it("writes error audit events and hides raw production error messages", async () => {
    vi.stubEnv("NODE_ENV", "production");
    const { withOperatorAccess } = await import("./operator-access");
    const handler = withOperatorAccess(["admin"], "runtime_maintenance", { targetType: "runtime_maintenance" }, async () => {
      throw new Error("database password leaked in raw error");
    });
    const response = await handler(
      new Request("http://localhost/api/ops/runtime-maintenance", {
        method: "POST",
        headers: { Authorization: "Bearer admin-token-value", "x-request-id": "request-123" }
      })
    );
    const body = await response.text();

    expect(response.status).toBe(500);
    expect(body).toContain("request-123");
    expect(body).toContain("Unexpected server error.");
    expect(body).not.toContain("database password");
    const calls = query.mock.calls as unknown as unknown[][];
    expect(calls.at(-1)?.[1]).toContain("error");
    expect(calls.at(-1)?.[1]).toContain("runtime_maintenance");
  });

  it("fails closed when auth is disabled in production", async () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("WEB_AUTH_DISABLED", "true");
    const { requireRole } = await import("./operator-access");
    const request = new Request("http://localhost/api/ops/runtime-maintenance", { method: "POST" });
    const result = await requireRole(request, ["admin"], "runtime_maintenance");

    expect(result.ok).toBe(false);
  });

  it("fails closed in production when the session secret is missing", async () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("WEB_AUTH_DISABLED", "false");
    vi.stubEnv("WEB_OPERATOR_SESSION_SECRET", "");
    const { requireRole } = await import("./operator-access");
    const request = new Request("http://localhost/api/fraternity-crawl-requests", {
      method: "POST",
      headers: { Authorization: "Bearer operator-token-value" }
    });
    const result = await requireRole(request, ["operator", "admin"], "crawl_request_create");

    expect(result.ok).toBe(false);
  });
});
