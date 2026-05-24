import { randomUUID } from "crypto";
import { z } from "zod";

import { apiError } from "@/lib/api-envelope";
import { authenticateLoginToken, loginResponse, writeOperatorAuditEvent } from "@/lib/security/operator-access";

const payloadSchema = z.object({
  token: z.string().min(1)
});

export async function POST(request: Request) {
  const requestId = request.headers.get("x-request-id") || randomUUID();
  const route = new URL(request.url).pathname;
  try {
    const payload = payloadSchema.parse(await request.json());
    const context = authenticateLoginToken(payload.token);
    if (!context) {
      await writeOperatorAuditEvent({
        actor: "anonymous",
        role: "anonymous",
        action: "operator_login",
        route,
        method: "POST",
        requestId,
        result: "denied",
        errorCode: "invalid_operator_token"
      });
      return apiError({ status: 401, code: "invalid_operator_token", message: "Operator authentication failed.", requestId });
    }
    await writeOperatorAuditEvent({
      actor: context.actor,
      role: context.role,
      action: "operator_login",
      route,
      method: "POST",
      requestId,
      result: "allowed"
    });
    return loginResponse(context);
  } catch {
    return apiError({ status: 400, code: "invalid_request", message: "The request payload is invalid.", requestId });
  }
}
