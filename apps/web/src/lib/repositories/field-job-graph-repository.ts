import { getDbPool } from "../db";
import type {
  FieldJobGraphDecisionItem,
  FieldJobGraphEventItem,
  FieldJobGraphRunDetail,
  FieldJobGraphRunListItem,
} from "../types";

interface FieldJobGraphRunRow {
  id: number;
  workerId: string;
  runtimeMode: string;
  sourceSlug: string | null;
  fieldName: string | null;
  requestedLimit: number;
  status: string;
  summary: Record<string, unknown>;
  errorMessage: string | null;
  createdAt: string;
  updatedAt: string;
  finishedAt: string | null;
  eventCount: number;
  decisionCount: number;
}

interface FieldJobGraphEventRow {
  id: number;
  runId: number;
  jobId: string | null;
  attempt: number | null;
  nodeName: string;
  phase: string;
  status: string;
  latencyMs: number;
  metricsDelta: Record<string, unknown>;
  diagnostics: Record<string, unknown>;
  createdAt: string;
}

interface FieldJobGraphDecisionRow {
  id: number;
  runId: number;
  jobId: string;
  attempt: number;
  fieldName: string;
  decisionStatus: string;
  confidence: number | null;
  candidateKind: string | null;
  candidateValue: string | null;
  reasonCodes: string[];
  writeAllowed: boolean;
  requiresReview: boolean;
  metadata: Record<string, unknown>;
  createdAt: string;
}

function mapRun(row: FieldJobGraphRunRow): FieldJobGraphRunListItem {
  return {
    id: Number(row.id),
    workerId: row.workerId,
    runtimeMode: row.runtimeMode,
    sourceSlug: row.sourceSlug,
    fieldName: row.fieldName,
    requestedLimit: Number(row.requestedLimit ?? 0),
    status: row.status,
    summary: row.summary ?? {},
    errorMessage: row.errorMessage,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    finishedAt: row.finishedAt,
    eventCount: Number(row.eventCount ?? 0),
    decisionCount: Number(row.decisionCount ?? 0),
  };
}

function mapEvent(row: FieldJobGraphEventRow): FieldJobGraphEventItem {
  return {
    id: Number(row.id),
    runId: Number(row.runId),
    jobId: row.jobId,
    attempt: row.attempt === null ? null : Number(row.attempt),
    nodeName: row.nodeName,
    phase: row.phase,
    status: row.status,
    latencyMs: Number(row.latencyMs ?? 0),
    metricsDelta: row.metricsDelta ?? {},
    diagnostics: row.diagnostics ?? {},
    createdAt: row.createdAt,
  };
}

function mapDecision(row: FieldJobGraphDecisionRow): FieldJobGraphDecisionItem {
  return {
    id: Number(row.id),
    runId: Number(row.runId),
    jobId: row.jobId,
    attempt: Number(row.attempt),
    fieldName: row.fieldName,
    decisionStatus: row.decisionStatus,
    confidence: row.confidence === null ? null : Number(row.confidence),
    candidateKind: row.candidateKind,
    candidateValue: row.candidateValue,
    reasonCodes: Array.isArray(row.reasonCodes) ? row.reasonCodes : [],
    writeAllowed: Boolean(row.writeAllowed),
    requiresReview: Boolean(row.requiresReview),
    metadata: row.metadata ?? {},
    createdAt: row.createdAt,
  };
}

export async function listFieldJobGraphRuns(params?: {
  limit?: number;
  sourceSlug?: string | null;
  fieldName?: string | null;
  runtimeMode?: string | null;
}): Promise<FieldJobGraphRunListItem[]> {
  const dbPool = getDbPool();
  const limit = Math.max(1, Math.min(500, Number(params?.limit ?? 50)));

  const { rows } = await dbPool.query<FieldJobGraphRunRow>(
    `
      SELECT
        r.id,
        r.worker_id AS "workerId",
        r.runtime_mode AS "runtimeMode",
        r.source_slug AS "sourceSlug",
        r.field_name AS "fieldName",
        r.requested_limit AS "requestedLimit",
        r.status,
        r.summary,
        r.error_message AS "errorMessage",
        r.created_at AS "createdAt",
        r.updated_at AS "updatedAt",
        r.finished_at AS "finishedAt",
        COALESCE(ec.event_count, 0)::int AS "eventCount",
        COALESCE(dc.decision_count, 0)::int AS "decisionCount"
      FROM field_job_graph_runs r
      LEFT JOIN (
        SELECT run_id, COUNT(*) AS event_count
        FROM field_job_graph_events
        GROUP BY run_id
      ) ec ON ec.run_id = r.id
      LEFT JOIN (
        SELECT run_id, COUNT(*) AS decision_count
        FROM field_job_graph_decisions
        GROUP BY run_id
      ) dc ON dc.run_id = r.id
      WHERE ($1::text IS NULL OR r.source_slug = $1)
        AND ($2::text IS NULL OR r.field_name = $2)
        AND ($3::text IS NULL OR r.runtime_mode = $3)
      ORDER BY r.created_at DESC
      LIMIT $4
    `,
    [params?.sourceSlug ?? null, params?.fieldName ?? null, params?.runtimeMode ?? null, limit]
  );

  return rows.map(mapRun);
}

export async function getFieldJobGraphRunDetail(
  runId: number,
  params?: { eventLimit?: number; decisionLimit?: number }
): Promise<FieldJobGraphRunDetail | null> {
  const dbPool = getDbPool();
  const eventLimit = Math.max(1, Math.min(1000, Number(params?.eventLimit ?? 200)));
  const decisionLimit = Math.max(1, Math.min(1000, Number(params?.decisionLimit ?? 200)));

  const runRows = await dbPool.query<FieldJobGraphRunRow>(
    `
      SELECT
        r.id,
        r.worker_id AS "workerId",
        r.runtime_mode AS "runtimeMode",
        r.source_slug AS "sourceSlug",
        r.field_name AS "fieldName",
        r.requested_limit AS "requestedLimit",
        r.status,
        r.summary,
        r.error_message AS "errorMessage",
        r.created_at AS "createdAt",
        r.updated_at AS "updatedAt",
        r.finished_at AS "finishedAt",
        COALESCE(ec.event_count, 0)::int AS "eventCount",
        COALESCE(dc.decision_count, 0)::int AS "decisionCount"
      FROM field_job_graph_runs r
      LEFT JOIN (
        SELECT run_id, COUNT(*) AS event_count
        FROM field_job_graph_events
        GROUP BY run_id
      ) ec ON ec.run_id = r.id
      LEFT JOIN (
        SELECT run_id, COUNT(*) AS decision_count
        FROM field_job_graph_decisions
        GROUP BY run_id
      ) dc ON dc.run_id = r.id
      WHERE r.id = $1
      LIMIT 1
    `,
    [runId]
  );

  const runRow = runRows.rows[0];
  if (!runRow) {
    return null;
  }

  const eventRows = await dbPool.query<FieldJobGraphEventRow>(
    `
      SELECT
        id,
        run_id AS "runId",
        job_id AS "jobId",
        attempt,
        node_name AS "nodeName",
        phase,
        status,
        latency_ms AS "latencyMs",
        metrics_delta AS "metricsDelta",
        diagnostics,
        created_at AS "createdAt"
      FROM field_job_graph_events
      WHERE run_id = $1
      ORDER BY id DESC
      LIMIT $2
    `,
    [runId, eventLimit]
  );

  const decisionRows = await dbPool.query<FieldJobGraphDecisionRow>(
    `
      SELECT
        id,
        run_id AS "runId",
        job_id AS "jobId",
        attempt,
        field_name AS "fieldName",
        decision_status AS "decisionStatus",
        confidence,
        candidate_kind AS "candidateKind",
        candidate_value AS "candidateValue",
        reason_codes AS "reasonCodes",
        write_allowed AS "writeAllowed",
        requires_review AS "requiresReview",
        metadata,
        created_at AS "createdAt"
      FROM field_job_graph_decisions
      WHERE run_id = $1
      ORDER BY id DESC
      LIMIT $2
    `,
    [runId, decisionLimit]
  );

  return {
    run: mapRun(runRow),
    events: eventRows.rows.map(mapEvent),
    decisions: decisionRows.rows.map(mapDecision),
  };
}
