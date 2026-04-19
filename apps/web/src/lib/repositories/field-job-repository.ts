import { getDbPool } from "../db";
import type { FieldJobListItem, FieldJobLogFeed, FieldJobLogItem } from "../types";

export async function listFieldJobs(limit = 100): Promise<FieldJobListItem[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<FieldJobListItem>(
    `
      SELECT
        fj.id,
        c.slug AS "chapterSlug",
        fj.field_name AS "fieldName",
        fj.status,
        COALESCE(fj.queue_state, 'actionable') AS "queueState",
        NULLIF(BTRIM(fj.blocked_reason), '') AS "blockedReason",
        fj.terminal_failure AS "terminalFailure",
        fj.claimed_by AS "claimedBy",
        fj.attempts,
        fj.max_attempts AS "maxAttempts",
        fj.scheduled_at AS "scheduledAt",
        fj.started_at AS "startedAt",
        fj.finished_at AS "finishedAt",
        fj.last_error AS "lastError"
      FROM field_jobs fj
      JOIN chapters c ON c.id = fj.chapter_id
      ORDER BY fj.scheduled_at ASC
      LIMIT $1
    `,
    [limit]
  );

  return rows;
}

type FieldJobLogRow = {
  id: number;
  jobId: string;
  attempt: number | null;
  createdAt: string;
  kind: "event" | "decision";
  nodeName?: string | null;
  phase?: string | null;
  status?: string | null;
  latencyMs?: number | null;
  diagnostics?: Record<string, unknown> | null;
  decisionStatus?: string | null;
  candidateKind?: string | null;
  candidateValue?: string | null;
  reasonCodes?: unknown;
};

function asShortText(value: unknown, maxLength = 96): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().replace(/\s+/g, " ");
  if (!normalized) {
    return null;
  }
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength - 1)}…`;
}

function buildEventMessage(row: FieldJobLogRow): { message: string; signature: string } {
  const diagnostics = row.diagnostics ?? {};
  const query = asShortText(diagnostics.query);
  const pathCandidate =
    asShortText(diagnostics.supportingPageUrl) ??
    asShortText(diagnostics.sourceUrl) ??
    asShortText(diagnostics.url) ??
    asShortText(diagnostics.path);
  const parts = [`${row.nodeName ?? "node"} ${row.phase ?? "phase"} ${row.status ?? "status"}`];
  if (query) {
    parts.push(`query: ${query}`);
  }
  if (pathCandidate) {
    parts.push(`path: ${pathCandidate}`);
  }
  if (row.latencyMs && row.latencyMs > 0) {
    parts.push(`${Math.round(row.latencyMs)}ms`);
  }
  const message = parts.join(" | ");
  return {
    message,
    signature: `event:${row.attempt ?? "na"}:${row.nodeName ?? ""}:${row.phase ?? ""}:${row.status ?? ""}:${query ?? ""}:${pathCandidate ?? ""}`
  };
}

function buildDecisionMessage(row: FieldJobLogRow): { message: string; signature: string } {
  const candidateValue = asShortText(row.candidateValue);
  const reasonCodes = Array.isArray(row.reasonCodes)
    ? row.reasonCodes.map((value) => String(value)).filter(Boolean)
    : [];
  const parts = [`decision ${row.decisionStatus ?? "unknown"}`];
  if (row.candidateKind) {
    parts.push(row.candidateKind);
  }
  if (candidateValue) {
    parts.push(candidateValue);
  }
  if (reasonCodes.length > 0) {
    parts.push(reasonCodes.join(", "));
  }
  const message = parts.join(" | ");
  return {
    message,
    signature: `decision:${row.attempt ?? "na"}:${row.decisionStatus ?? ""}:${row.candidateKind ?? ""}:${candidateValue ?? ""}:${reasonCodes.join(",")}`
  };
}

export async function getFieldJobLogFeed(jobId: string, limit = 80): Promise<FieldJobLogFeed> {
  const dbPool = getDbPool();
  const safeLimit = Math.max(10, Math.min(limit, 250));

  const { rows: jobRows } = await dbPool.query<{
    id: string;
    chapterSlug: string;
    fieldName: string;
    status: string;
    queueState: string;
    blockedReason: string | null;
    attempts: number;
    maxAttempts: number;
    claimedBy: string | null;
    scheduledAt: string;
    startedAt: string | null;
    finishedAt: string | null;
    lastError: string | null;
  }>(
    `
      SELECT
        fj.id,
        c.slug AS "chapterSlug",
        fj.field_name AS "fieldName",
        fj.status,
        COALESCE(fj.queue_state, 'actionable') AS "queueState",
        NULLIF(BTRIM(fj.blocked_reason), '') AS "blockedReason",
        fj.attempts,
        fj.max_attempts AS "maxAttempts",
        fj.claimed_by AS "claimedBy",
        fj.scheduled_at AS "scheduledAt",
        fj.started_at AS "startedAt",
        fj.finished_at AS "finishedAt",
        fj.last_error AS "lastError"
      FROM field_jobs fj
      JOIN chapters c ON c.id = fj.chapter_id
      WHERE fj.id = $1::uuid
      LIMIT 1
    `,
    [jobId]
  );

  const job = jobRows[0];
  if (!job) {
    return {
      jobId,
      lines: [],
      dedupedCount: 0,
      generatedAt: new Date().toISOString()
    };
  }

  let graphRows: FieldJobLogRow[] = [];
  try {
    const result = await dbPool.query<FieldJobLogRow>(
      `
        SELECT
          e.id,
          e.job_id AS "jobId",
          e.attempt,
          e.created_at AS "createdAt",
          'event'::text AS kind,
          e.node_name AS "nodeName",
          e.phase,
          e.status,
          e.latency_ms AS "latencyMs",
          e.diagnostics,
          NULL::text AS "decisionStatus",
          NULL::text AS "candidateKind",
          NULL::text AS "candidateValue",
          NULL::jsonb AS "reasonCodes"
        FROM field_job_graph_events e
        WHERE e.job_id = $1::uuid
        UNION ALL
        SELECT
          d.id,
          d.job_id AS "jobId",
          d.attempt,
          d.created_at AS "createdAt",
          'decision'::text AS kind,
          NULL::text AS "nodeName",
          NULL::text AS phase,
          NULL::text AS status,
          NULL::integer AS "latencyMs",
          d.metadata AS diagnostics,
          d.decision_status AS "decisionStatus",
          d.candidate_kind AS "candidateKind",
          d.candidate_value AS "candidateValue",
          to_jsonb(d.reason_codes) AS "reasonCodes"
        FROM field_job_graph_decisions d
        WHERE d.job_id = $1::uuid
        ORDER BY "createdAt" ASC, id ASC
      `,
      [jobId]
    );
    graphRows = result.rows;
  } catch (error) {
    const code = typeof error === "object" && error && "code" in error ? String((error as { code?: string }).code ?? "") : "";
    if (code !== "42P01") {
      throw error;
    }
  }

  const lines: FieldJobLogItem[] = [];
  let previousSignature = "";
  let dedupedCount = 0;

  const jobSummary = [
    `${job.fieldName} ${job.status}`,
    `queue: ${job.queueState}`,
    `attempts: ${job.attempts}/${job.maxAttempts}`,
    job.claimedBy ? `worker: ${job.claimedBy}` : "worker: unclaimed",
    job.blockedReason ? `reason: ${job.blockedReason}` : null,
    job.lastError ? `last error: ${asShortText(job.lastError, 140)}` : null
  ].filter(Boolean).join(" | ");

  lines.push({
    id: `${jobId}:status`,
    jobId,
    kind: "status",
    attempt: job.attempts,
    createdAt: job.finishedAt ?? job.startedAt ?? job.scheduledAt,
    message: `${job.chapterSlug} | ${jobSummary}`,
    signature: `status:${job.status}:${job.queueState}:${job.blockedReason ?? ""}:${job.attempts}:${job.claimedBy ?? ""}`
  });
  previousSignature = `status:${job.status}:${job.queueState}:${job.blockedReason ?? ""}:${job.attempts}:${job.claimedBy ?? ""}`;

  for (const row of graphRows) {
    const built = row.kind === "decision" ? buildDecisionMessage(row) : buildEventMessage(row);
    if (built.signature === previousSignature) {
      dedupedCount += 1;
      continue;
    }
    previousSignature = built.signature;
    lines.push({
      id: `${row.kind}:${row.id}`,
      jobId,
      kind: row.kind,
      attempt: row.attempt ?? null,
      createdAt: row.createdAt,
      message: built.message,
      signature: built.signature
    });
  }

  return {
    jobId,
    lines: lines.slice(-safeLimit),
    dedupedCount,
    generatedAt: new Date().toISOString()
  };
}
