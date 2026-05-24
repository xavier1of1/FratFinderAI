import { getDbPool } from "@/lib/db";

export type OperatorAuditResult = "allowed" | "denied" | "error";

export interface InsertOperatorAuditEventInput {
  actor: string;
  role: string;
  action: string;
  route: string;
  method: string;
  targetType?: string | null;
  targetId?: string | null;
  requestId: string;
  result: OperatorAuditResult;
  metadata?: Record<string, unknown>;
  errorCode?: string | null;
}

export async function insertOperatorAuditEvent(event: InsertOperatorAuditEventInput): Promise<void> {
  await getDbPool().query(
    `INSERT INTO operator_audit_events (
      actor, role, action, route, method, target_type, target_id, request_id, result, metadata, error_code
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11)`,
    [
      event.actor,
      event.role,
      event.action,
      event.route,
      event.method,
      event.targetType ?? null,
      event.targetId ?? null,
      event.requestId,
      event.result,
      JSON.stringify(event.metadata ?? {}),
      event.errorCode ?? null,
    ]
  );
}
