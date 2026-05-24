import { createHmac, randomUUID, timingSafeEqual } from "crypto";
import { NextResponse } from "next/server";

import { apiError } from "@/lib/api-envelope";
import { insertOperatorAuditEvent } from "@/lib/repositories/operator-audit-repository";

export type OperatorRole = "admin" | "operator" | "analyst";
export type AuditResult = "allowed" | "denied" | "error";

export interface OperatorContext {
  actor: string;
  role: OperatorRole;
  authMode: "bearer" | "session" | "disabled";
}

export interface AccessTarget {
  targetType?: string;
  targetId?: string | null;
  metadata?: Record<string, unknown>;
}

export interface OperatorAuditEvent extends AccessTarget {
  actor: string;
  role: OperatorRole | "anonymous" | "unknown";
  action: string;
  route: string;
  method: string;
  requestId: string;
  result: AuditResult;
  errorCode?: string;
}

const SESSION_COOKIE = "fratfinder_operator_session";
const TOKEN_MIN_LENGTH = 12;
const PLACEHOLDER_TOKENS = new Set(["admin", "operator", "analyst", "password", "changeme", "change-me", "change-me-local"]);

function isProduction(): boolean {
  return process.env.NODE_ENV === "production";
}

function isAuthDisabled(): boolean {
  return process.env.WEB_AUTH_DISABLED === "true" && !isProduction();
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = createHmac("sha256", "fratfinder-operator-token-compare").update(left).digest();
  const rightBuffer = createHmac("sha256", "fratfinder-operator-token-compare").update(right).digest();
  return timingSafeEqual(leftBuffer, rightBuffer);
}

function validateConfiguredToken(role: OperatorRole, token: string | undefined): string | null {
  const value = (token ?? "").trim();
  if (!value) {
    return null;
  }
  if (isProduction() && (value.length < TOKEN_MIN_LENGTH || PLACEHOLDER_TOKENS.has(value.toLowerCase()))) {
    throw new Error(`Invalid production token configured for ${role}.`);
  }
  return value;
}

function configuredTokens(): Array<{ role: OperatorRole; token: string }> {
  const entries = [
    { role: "admin" as const, token: validateConfiguredToken("admin", process.env.WEB_ADMIN_TOKEN) },
    { role: "operator" as const, token: validateConfiguredToken("operator", process.env.WEB_OPERATOR_TOKEN) },
    { role: "analyst" as const, token: validateConfiguredToken("analyst", process.env.WEB_ANALYST_TOKEN) }
  ];
  return entries.filter((entry): entry is { role: OperatorRole; token: string } => Boolean(entry.token));
}

function assertOperatorAuthConfiguration(): void {
  if (process.env.WEB_AUTH_DISABLED === "true" && isProduction()) {
    throw new Error("WEB_AUTH_DISABLED cannot be used in production.");
  }
  if (!isAuthDisabled()) {
    sessionSecret();
  }
  configuredTokens();
}

function sessionSecret(): string {
  const secret = (process.env.WEB_OPERATOR_SESSION_SECRET ?? "").trim();
  if (!secret) {
    if (isAuthDisabled()) {
      return "auth-disabled-local-only";
    }
    throw new Error("WEB_OPERATOR_SESSION_SECRET is required when operator auth is enabled.");
  }
  if (isProduction() && (secret.length < 24 || PLACEHOLDER_TOKENS.has(secret.toLowerCase()))) {
    throw new Error("WEB_OPERATOR_SESSION_SECRET is too weak for production.");
  }
  return secret;
}

function signPayload(payload: string): string {
  return createHmac("sha256", sessionSecret()).update(payload).digest("base64url");
}

function encodeSession(context: OperatorContext): string {
  const payload = Buffer.from(
    JSON.stringify({
      actor: context.actor,
      role: context.role,
      exp: Date.now() + 12 * 60 * 60 * 1000
    })
  ).toString("base64url");
  return `${payload}.${signPayload(payload)}`;
}

function decodeSession(value: string | undefined): OperatorContext | null {
  if (!value || !value.includes(".")) {
    return null;
  }
  const [payload, signature] = value.split(".", 2);
  if (!payload || !signature || !safeEqual(signature, signPayload(payload))) {
    return null;
  }
  try {
    const parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf-8")) as { actor?: string; role?: string; exp?: number };
    if (!parsed.exp || parsed.exp < Date.now()) {
      return null;
    }
    if (!["admin", "operator", "analyst"].includes(parsed.role ?? "")) {
      return null;
    }
    return {
      actor: parsed.actor || `${parsed.role}-session`,
      role: parsed.role as OperatorRole,
      authMode: "session"
    };
  } catch {
    return null;
  }
}

function bearerToken(request: Request): string | null {
  const header = request.headers.get("authorization") ?? "";
  const match = /^Bearer\s+(.+)$/i.exec(header);
  return match?.[1]?.trim() || null;
}

function cookieValue(request: Request, name: string): string | undefined {
  const cookie = request.headers.get("cookie") ?? "";
  for (const part of cookie.split(";")) {
    const [rawKey, ...rest] = part.trim().split("=");
    if (rawKey === name) {
      return rest.join("=");
    }
  }
  return undefined;
}

export function roleAllowed(role: OperatorRole, allowedRoles: OperatorRole[]): boolean {
  return allowedRoles.includes(role);
}

export function authenticateOperator(request: Request): OperatorContext | null {
  try {
    assertOperatorAuthConfiguration();
  } catch (error) {
    console.error("Operator auth configuration failed closed:", error instanceof Error ? error.message : "unknown_error");
    return null;
  }
  if (isAuthDisabled()) {
    console.warn("WEB_AUTH_DISABLED=true: operator auth is disabled for local/test only.");
    return { actor: "auth-disabled-local", role: "admin", authMode: "disabled" };
  }

  const token = bearerToken(request);
  if (token) {
    for (const entry of configuredTokens()) {
      if (safeEqual(token, entry.token)) {
        return { actor: `${entry.role}-token`, role: entry.role, authMode: "bearer" };
      }
    }
    return null;
  }

  return decodeSession(cookieValue(request, SESSION_COOKIE));
}

export async function writeOperatorAuditEvent(event: OperatorAuditEvent): Promise<void> {
  try {
    await insertOperatorAuditEvent(event);
  } catch (error) {
    console.error("Failed to write operator audit event", error);
  }
}

export async function requireRole(
  request: Request,
  allowedRoles: OperatorRole[],
  action: string,
  target: AccessTarget = {}
): Promise<{ ok: true; operator: OperatorContext; requestId: string } | { ok: false; response: NextResponse; requestId: string }> {
  const requestId = request.headers.get("x-request-id") || randomUUID();
  const operator = authenticateOperator(request);
  const route = new URL(request.url).pathname;
  if (!operator) {
    await writeOperatorAuditEvent({
      actor: "anonymous",
      role: "anonymous",
      action,
      route,
      method: request.method,
      requestId,
      result: "denied",
      errorCode: "operator_auth_required",
      ...target
    });
    return {
      ok: false,
      requestId,
      response: apiError({ status: 401, code: "operator_auth_required", message: "Operator authentication is required.", requestId })
    };
  }

  if (!roleAllowed(operator.role, allowedRoles)) {
    await writeOperatorAuditEvent({
      actor: operator.actor,
      role: operator.role,
      action,
      route,
      method: request.method,
      requestId,
      result: "denied",
      errorCode: "operator_role_denied",
      ...target
    });
    return {
      ok: false,
      requestId,
      response: apiError({ status: 403, code: "operator_role_denied", message: "Operator role is not permitted for this action.", requestId })
    };
  }

  return { ok: true, operator, requestId };
}

export function withOperatorAccess<TRequest extends Request, TContext = unknown>(
  allowedRoles: OperatorRole[],
  action: string,
  target: AccessTarget | ((request: TRequest, context: TContext) => AccessTarget),
  handler: (request: TRequest, context: TContext, operator: OperatorContext) => Promise<Response>
) {
  return async (request: TRequest, context?: TContext): Promise<Response> => {
    const routeContext = context as TContext;
    const targetPayload = typeof target === "function" ? target(request, routeContext) : target;
    const access = await requireRole(request, allowedRoles, action, targetPayload);
    if (!access.ok) {
      return access.response;
    }
    const route = new URL(request.url).pathname;
    try {
      const response = await handler(request, routeContext, access.operator);
      await writeOperatorAuditEvent({
        actor: access.operator.actor,
        role: access.operator.role,
        action,
        route,
        method: request.method,
        requestId: access.requestId,
        result: response.status >= 500 ? "error" : "allowed",
        ...targetPayload
      });
      return response;
    } catch (error) {
      await writeOperatorAuditEvent({
        actor: access.operator.actor,
        role: access.operator.role,
        action,
        route,
        method: request.method,
        requestId: access.requestId,
        result: "error",
        errorCode: error instanceof Error ? error.name : "unknown_error",
        ...targetPayload
      });
      return apiError({
        status: 500,
        code: "internal_error",
        message: isProduction() ? "Unexpected server error." : error instanceof Error ? error.message : "Unexpected server error.",
        requestId: access.requestId
      });
    }
  };
}

export function withReadOnlyOperatorAccess<TRequest extends Request, TContext = unknown>(
  action: string,
  target: AccessTarget | ((request: TRequest, context: TContext) => AccessTarget),
  handler: (request: TRequest, context: TContext, operator: OperatorContext) => Promise<Response>
) {
  return withOperatorAccess(["analyst", "operator", "admin"], action, target, handler);
}

export function loginResponse(context: OperatorContext): NextResponse {
  const response = NextResponse.json({ success: true, data: { actor: context.actor, role: context.role } });
  response.cookies.set({
    name: SESSION_COOKIE,
    value: encodeSession(context),
    httpOnly: true,
    secure: isProduction(),
    sameSite: "lax",
    path: "/",
    maxAge: 12 * 60 * 60
  });
  return response;
}

export function logoutResponse(): NextResponse {
  const response = NextResponse.json({ success: true, data: { loggedOut: true } });
  response.cookies.set({
    name: SESSION_COOKIE,
    value: "",
    httpOnly: true,
    secure: isProduction(),
    sameSite: "lax",
    path: "/",
    maxAge: 0
  });
  return response;
}

export function authenticateLoginToken(token: string): OperatorContext | null {
  try {
    assertOperatorAuthConfiguration();
  } catch (error) {
    console.error("Operator auth configuration failed closed:", error instanceof Error ? error.message : "unknown_error");
    return null;
  }
  const candidate = token.trim();
  for (const entry of configuredTokens()) {
    if (safeEqual(candidate, entry.token)) {
      return { actor: `${entry.role}-session`, role: entry.role, authMode: "session" };
    }
  }
  return null;
}
