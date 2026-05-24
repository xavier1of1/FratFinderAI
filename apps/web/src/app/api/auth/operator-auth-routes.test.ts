import { beforeEach, describe, expect, it, vi } from "vitest";

const query = vi.fn(async () => ({ rowCount: 1 }));

vi.mock("@/lib/db", () => ({
  getDbPool: () => ({ query })
}));

describe("operator auth routes", () => {
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

  it("logs in with a valid operator token and sets an HttpOnly SameSite session cookie", async () => {
    const route = await import("./operator-login/route");
    const response = await route.POST(
      new Request("http://localhost/api/auth/operator-login", {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-request-id": "login-request" },
        body: JSON.stringify({ token: "operator-token-value" })
      })
    );
    const setCookie = response.headers.get("set-cookie") ?? "";
    const body = await response.json();

    expect(response.status).toBe(200);
    expect(body.data.role).toBe("operator");
    expect(setCookie).toContain("fratfinder_operator_session=");
    expect(setCookie.toLowerCase()).toContain("httponly");
    expect(setCookie.toLowerCase()).toContain("samesite=lax");
    expect(query).toHaveBeenCalled();
    expect((query.mock.calls.at(-1) as unknown[])[1]).toContain("allowed");
  });

  it("denies invalid login tokens without exposing token details", async () => {
    const route = await import("./operator-login/route");
    const response = await route.POST(
      new Request("http://localhost/api/auth/operator-login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token: "wrong-token" })
      })
    );
    const body = await response.text();

    expect(response.status).toBe(401);
    expect(body).toContain("Operator authentication failed.");
    expect(body).not.toContain("wrong-token");
    expect((query.mock.calls.at(-1) as unknown[])[1]).toContain("denied");
  });

  it("reports bearer-token sessions without exposing secrets", async () => {
    const route = await import("./operator-session/route");
    const response = await route.GET(
      new Request("http://localhost/api/auth/operator-session", {
        headers: { Authorization: "Bearer analyst-token-value" }
      })
    );
    const body = await response.json();

    expect(response.status).toBe(200);
    expect(body.data.authenticated).toBe(true);
    expect(body.data.role).toBe("analyst");
    expect(JSON.stringify(body)).not.toContain("analyst-token-value");
  });

  it("clears the operator session cookie on logout", async () => {
    const route = await import("./operator-logout/route");
    const response = await route.POST();
    const setCookie = response.headers.get("set-cookie") ?? "";

    expect(response.status).toBe(200);
    expect(setCookie).toContain("fratfinder_operator_session=");
    expect(setCookie.toLowerCase()).toContain("httponly");
    expect(setCookie.toLowerCase()).toContain("max-age=0");
  });
});
