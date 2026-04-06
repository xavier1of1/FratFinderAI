import { getDbPool } from "../db";

export async function upsertOpenOpsAlert(params: {
  alertScope: "benchmark" | "campaign" | "queue" | "repair" | "provider" | "system";
  alertType: string;
  severity: "info" | "warning" | "critical";
  message: string;
  fingerprint?: string | null;
  benchmarkRunId?: string | null;
  campaignRunId?: string | null;
  requestId?: string | null;
  sourceSlug?: string | null;
  payload?: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  const fingerprint = params.fingerprint?.trim() || null;
  if (fingerprint) {
    await dbPool.query(
      `
        INSERT INTO ops_alerts (
          alert_scope,
          alert_type,
          severity,
          status,
          benchmark_run_id,
          campaign_run_id,
          request_id,
          source_slug,
          message,
          fingerprint,
          payload
        )
        VALUES ($1, $2, $3, 'open', $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (fingerprint) WHERE status = 'open' AND fingerprint IS NOT NULL
        DO UPDATE SET
          severity = EXCLUDED.severity,
          message = EXCLUDED.message,
          benchmark_run_id = EXCLUDED.benchmark_run_id,
          campaign_run_id = EXCLUDED.campaign_run_id,
          request_id = EXCLUDED.request_id,
          source_slug = EXCLUDED.source_slug,
          payload = EXCLUDED.payload,
          resolved_at = NULL,
          status = 'open',
          updated_at = NOW()
      `,
      [
        params.alertScope,
        params.alertType,
        params.severity,
        params.benchmarkRunId ?? null,
        params.campaignRunId ?? null,
        params.requestId ?? null,
        params.sourceSlug ?? null,
        params.message,
        fingerprint,
        JSON.stringify(params.payload ?? {}),
      ]
    );
    return;
  }

  await dbPool.query(
    `
      INSERT INTO ops_alerts (
        alert_scope,
        alert_type,
        severity,
        status,
        benchmark_run_id,
        campaign_run_id,
        request_id,
        source_slug,
        message,
        fingerprint,
        payload
      )
      VALUES ($1, $2, $3, 'open', $4, $5, $6, $7, $8, NULL, $9)
    `,
    [
      params.alertScope,
      params.alertType,
      params.severity,
      params.benchmarkRunId ?? null,
      params.campaignRunId ?? null,
      params.requestId ?? null,
      params.sourceSlug ?? null,
      params.message,
      JSON.stringify(params.payload ?? {}),
    ]
  );
}

export async function resolveOpsAlertsByFingerprintPrefix(params: {
  prefix: string;
  resolvedReason?: string;
  metadata?: Record<string, unknown>;
}): Promise<number> {
  const dbPool = getDbPool();
  const trimmed = params.prefix.trim();
  if (!trimmed) {
    return 0;
  }
  const { rowCount } = await dbPool.query(
    `
      UPDATE ops_alerts
      SET
        status = 'resolved',
        resolved_at = NOW(),
        payload = COALESCE(payload, '{}'::jsonb) || jsonb_build_object(
          'resolvedReason', $2::text,
          'resolvedMetadata', $3::jsonb,
          'resolvedAt', NOW()
        ),
        updated_at = NOW()
      WHERE status = 'open'
        AND fingerprint LIKE ($1 || '%')
    `,
    [trimmed, params.resolvedReason?.trim() || "resolved", JSON.stringify(params.metadata ?? {})]
  );
  return Number(rowCount ?? 0);
}

export async function getOpsAlertSummary(): Promise<{
  openTotal: number;
  openCritical: number;
  openWarning: number;
  resolvedLast24h: number;
  oldestOpenMinutes: number;
}> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    openTotal: number;
    openCritical: number;
    openWarning: number;
    resolvedLast24h: number;
    oldestOpenMinutes: number | null;
  }>(
    `
      SELECT
        COUNT(*) FILTER (WHERE status = 'open')::int AS "openTotal",
        COUNT(*) FILTER (WHERE status = 'open' AND severity = 'critical')::int AS "openCritical",
        COUNT(*) FILTER (WHERE status = 'open' AND severity = 'warning')::int AS "openWarning",
        COUNT(*) FILTER (WHERE status = 'resolved' AND resolved_at >= NOW() - INTERVAL '24 hours')::int AS "resolvedLast24h",
        COALESCE(
          FLOOR(EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 'open'))) / 60),
          0
        )::int AS "oldestOpenMinutes"
      FROM ops_alerts
    `
  );
  const row = rows[0];
  return {
    openTotal: Number(row?.openTotal ?? 0),
    openCritical: Number(row?.openCritical ?? 0),
    openWarning: Number(row?.openWarning ?? 0),
    resolvedLast24h: Number(row?.resolvedLast24h ?? 0),
    oldestOpenMinutes: Number(row?.oldestOpenMinutes ?? 0),
  };
}

export async function listOpsAlerts(limit = 50): Promise<Array<{
  id: string;
  alertScope: "benchmark" | "campaign" | "queue" | "repair" | "provider" | "system";
  alertType: string;
  severity: "info" | "warning" | "critical";
  status: "open" | "resolved";
  benchmarkRunId: string | null;
  campaignRunId: string | null;
  requestId: string | null;
  sourceSlug: string | null;
  message: string;
  fingerprint: string | null;
  payload: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  resolvedAt: string | null;
}>> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    id: string;
    alertScope: "benchmark" | "campaign" | "queue" | "repair" | "provider" | "system";
    alertType: string;
    severity: "info" | "warning" | "critical";
    status: "open" | "resolved";
    benchmarkRunId: string | null;
    campaignRunId: string | null;
    requestId: string | null;
    sourceSlug: string | null;
    message: string;
    fingerprint: string | null;
    payload: Record<string, unknown> | null;
    createdAt: string;
    updatedAt: string;
    resolvedAt: string | null;
  }>(
    `
      SELECT
        id::text AS id,
        alert_scope AS "alertScope",
        alert_type AS "alertType",
        severity,
        status,
        benchmark_run_id AS "benchmarkRunId",
        campaign_run_id AS "campaignRunId",
        request_id AS "requestId",
        source_slug AS "sourceSlug",
        message,
        fingerprint,
        payload,
        created_at AS "createdAt",
        updated_at AS "updatedAt",
        resolved_at AS "resolvedAt"
      FROM ops_alerts
      ORDER BY
        CASE WHEN status = 'open' THEN 0 ELSE 1 END,
        CASE severity
          WHEN 'critical' THEN 0
          WHEN 'warning' THEN 1
          ELSE 2
        END,
        created_at DESC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );

  return rows.map((row) => ({
    ...row,
    payload: row.payload ?? {},
  }));
}
