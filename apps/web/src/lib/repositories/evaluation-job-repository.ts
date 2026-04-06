import { getDbPool } from "../db";

export type EvaluationJobKind = "benchmark_run" | "campaign_run";
export type EvaluationJobStatus = "queued" | "running" | "succeeded" | "failed" | "canceled";
export type EvaluationIsolationMode = "shared_live_observed" | "strict_live_isolated";

export interface EvaluationJobRecord {
  id: string;
  jobKind: EvaluationJobKind;
  status: EvaluationJobStatus;
  benchmarkRunId: string | null;
  campaignRunId: string | null;
  sourceSlug: string | null;
  evaluationPhase: string | null;
  isolationMode: EvaluationIsolationMode;
  payload: Record<string, unknown>;
  preconditions: Record<string, unknown>;
  result: Record<string, unknown>;
  attempts: number;
  priority: number;
  scheduledAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  lastError: string | null;
  runtimeWorkerId: string | null;
  runtimeLeaseExpiresAt: string | null;
  runtimeLastHeartbeatAt: string | null;
  createdAt: string;
  updatedAt: string;
}

interface EvaluationJobRow {
  id: string;
  jobKind: EvaluationJobKind;
  status: EvaluationJobStatus;
  benchmarkRunId: string | null;
  campaignRunId: string | null;
  sourceSlug: string | null;
  evaluationPhase: string | null;
  isolationMode: EvaluationIsolationMode;
  payload: Record<string, unknown>;
  preconditions: Record<string, unknown>;
  result: Record<string, unknown>;
  attempts: number;
  priority: number;
  scheduledAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  lastError: string | null;
  runtimeWorkerId: string | null;
  runtimeLeaseExpiresAt: string | null;
  runtimeLastHeartbeatAt: string | null;
  createdAt: string;
  updatedAt: string;
}

function mapEvaluationJob(row: EvaluationJobRow): EvaluationJobRecord {
  return {
    id: row.id,
    jobKind: row.jobKind,
    status: row.status,
    benchmarkRunId: row.benchmarkRunId,
    campaignRunId: row.campaignRunId,
    sourceSlug: row.sourceSlug,
    evaluationPhase: row.evaluationPhase,
    isolationMode: row.isolationMode,
    payload: row.payload ?? {},
    preconditions: row.preconditions ?? {},
    result: row.result ?? {},
    attempts: Number(row.attempts ?? 0),
    priority: Number(row.priority ?? 100),
    scheduledAt: row.scheduledAt,
    startedAt: row.startedAt,
    finishedAt: row.finishedAt,
    lastError: row.lastError,
    runtimeWorkerId: row.runtimeWorkerId,
    runtimeLeaseExpiresAt: row.runtimeLeaseExpiresAt,
    runtimeLastHeartbeatAt: row.runtimeLastHeartbeatAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

export async function createEvaluationJob(params: {
  jobKind: EvaluationJobKind;
  benchmarkRunId?: string | null;
  campaignRunId?: string | null;
  sourceSlug?: string | null;
  evaluationPhase?: string | null;
  isolationMode?: EvaluationIsolationMode;
  priority?: number;
  scheduledAt?: string;
  payload?: Record<string, unknown>;
}): Promise<EvaluationJobRecord> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<EvaluationJobRow>(
    `
      INSERT INTO evaluation_jobs (
        job_kind,
        status,
        benchmark_run_id,
        campaign_run_id,
        source_slug,
        evaluation_phase,
        isolation_mode,
        priority,
        scheduled_at,
        payload
      )
      VALUES ($1, 'queued', $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT DO NOTHING
      RETURNING
        id,
        job_kind AS "jobKind",
        status,
        benchmark_run_id AS "benchmarkRunId",
        campaign_run_id AS "campaignRunId",
        source_slug AS "sourceSlug",
        evaluation_phase AS "evaluationPhase",
        isolation_mode AS "isolationMode",
        payload,
        preconditions,
        result,
        attempts,
        priority,
        scheduled_at AS "scheduledAt",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        last_error AS "lastError",
        runtime_worker_id AS "runtimeWorkerId",
        runtime_lease_expires_at AS "runtimeLeaseExpiresAt",
        runtime_last_heartbeat_at AS "runtimeLastHeartbeatAt",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
    `,
    [
      params.jobKind,
      params.benchmarkRunId ?? null,
      params.campaignRunId ?? null,
      params.sourceSlug ?? null,
      params.evaluationPhase ?? null,
      params.isolationMode ?? "shared_live_observed",
      Math.max(1, Number(params.priority ?? 100)),
      params.scheduledAt ?? new Date().toISOString(),
      JSON.stringify(params.payload ?? {}),
    ]
  );

  const created = rows[0];
  if (created) {
    return mapEvaluationJob(created);
  }

  const existing = await getEvaluationJobByRun({
    benchmarkRunId: params.benchmarkRunId ?? null,
    campaignRunId: params.campaignRunId ?? null,
  });
  if (!existing) {
    throw new Error("Failed to create evaluation job");
  }
  return existing;
}

export async function getEvaluationJobByRun(params: {
  benchmarkRunId?: string | null;
  campaignRunId?: string | null;
}): Promise<EvaluationJobRecord | null> {
  const dbPool = getDbPool();
  const runId = params.benchmarkRunId ?? params.campaignRunId ?? null;
  const runColumn = params.benchmarkRunId ? "benchmark_run_id" : "campaign_run_id";
  if (!runId) {
    return null;
  }
  const { rows } = await dbPool.query<EvaluationJobRow>(
    `
      SELECT
        id,
        job_kind AS "jobKind",
        status,
        benchmark_run_id AS "benchmarkRunId",
        campaign_run_id AS "campaignRunId",
        source_slug AS "sourceSlug",
        evaluation_phase AS "evaluationPhase",
        isolation_mode AS "isolationMode",
        payload,
        preconditions,
        result,
        attempts,
        priority,
        scheduled_at AS "scheduledAt",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        last_error AS "lastError",
        runtime_worker_id AS "runtimeWorkerId",
        runtime_lease_expires_at AS "runtimeLeaseExpiresAt",
        runtime_last_heartbeat_at AS "runtimeLastHeartbeatAt",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM evaluation_jobs
      WHERE ${runColumn} = $1
      ORDER BY created_at DESC
      LIMIT 1
    `,
    [runId]
  );
  return rows[0] ? mapEvaluationJob(rows[0]) : null;
}

export async function captureEvaluationWorkerSnapshot(): Promise<Record<string, unknown>> {
  const dbPool = getDbPool();
  const [workerCounts, benchmarkCounts, campaignCounts] = await Promise.all([
    dbPool.query<{ workloadLane: string; count: number }>(
      `
        SELECT workload_lane AS "workloadLane", COUNT(*)::int AS count
        FROM worker_processes
        WHERE status = 'active'
          AND (lease_expires_at IS NULL OR lease_expires_at >= NOW())
        GROUP BY workload_lane
      `
    ),
    dbPool.query<{ status: string; count: number }>(
      `
        SELECT status, COUNT(*)::int AS count
        FROM benchmark_runs
        GROUP BY status
      `
    ),
    dbPool.query<{ status: string; count: number }>(
      `
        SELECT status, COUNT(*)::int AS count
        FROM campaign_runs
        GROUP BY status
      `
    ),
  ]);

  return {
    capturedAt: new Date().toISOString(),
    activeWorkersByLane: Object.fromEntries(
      workerCounts.rows.map((row) => [row.workloadLane, Number(row.count ?? 0)])
    ),
    benchmarkRunsByStatus: Object.fromEntries(
      benchmarkCounts.rows.map((row) => [row.status, Number(row.count ?? 0)])
    ),
    campaignRunsByStatus: Object.fromEntries(
      campaignCounts.rows.map((row) => [row.status, Number(row.count ?? 0)])
    ),
  };
}

export async function claimNextEvaluationJob(params: {
  workerId: string;
  leaseToken: string;
  leaseSeconds: number;
}): Promise<EvaluationJobRecord | null> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<EvaluationJobRow>(
    `
      WITH candidate AS (
        SELECT id
        FROM evaluation_jobs
        WHERE status = 'queued'
          AND scheduled_at <= NOW()
          AND (
            runtime_worker_id IS NULL
            OR runtime_lease_expires_at IS NULL
            OR runtime_lease_expires_at < NOW()
          )
        ORDER BY priority ASC, scheduled_at ASC, created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE evaluation_jobs ej
      SET
        status = 'running',
        attempts = ej.attempts + 1,
        started_at = COALESCE(ej.started_at, NOW()),
        runtime_worker_id = $1,
        runtime_lease_token = $2,
        runtime_lease_expires_at = NOW() + ($3::int * INTERVAL '1 second'),
        runtime_last_heartbeat_at = NOW()
      FROM candidate
      WHERE ej.id = candidate.id
      RETURNING
        ej.id,
        ej.job_kind AS "jobKind",
        ej.status,
        ej.benchmark_run_id AS "benchmarkRunId",
        ej.campaign_run_id AS "campaignRunId",
        ej.source_slug AS "sourceSlug",
        ej.evaluation_phase AS "evaluationPhase",
        ej.isolation_mode AS "isolationMode",
        ej.payload,
        ej.preconditions,
        ej.result,
        ej.attempts,
        ej.priority,
        ej.scheduled_at AS "scheduledAt",
        ej.started_at AS "startedAt",
        ej.finished_at AS "finishedAt",
        ej.last_error AS "lastError",
        ej.runtime_worker_id AS "runtimeWorkerId",
        ej.runtime_lease_expires_at AS "runtimeLeaseExpiresAt",
        ej.runtime_last_heartbeat_at AS "runtimeLastHeartbeatAt",
        ej.created_at AS "createdAt",
        ej.updated_at AS "updatedAt"
    `,
    [params.workerId, params.leaseToken, Math.max(15, params.leaseSeconds)]
  );
  return rows[0] ? mapEvaluationJob(rows[0]) : null;
}

export async function heartbeatEvaluationJobLease(params: {
  jobId: string;
  workerId: string;
  leaseToken: string;
  leaseSeconds: number;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE evaluation_jobs
      SET
        runtime_lease_expires_at = NOW() + ($4::int * INTERVAL '1 second'),
        runtime_last_heartbeat_at = NOW()
      WHERE id = $1
        AND runtime_worker_id = $2
        AND runtime_lease_token = $3
    `,
    [params.jobId, params.workerId, params.leaseToken, Math.max(15, params.leaseSeconds)]
  );
}

export async function updateEvaluationJobPreconditions(params: {
  jobId: string;
  preconditions: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE evaluation_jobs
      SET preconditions = $2
      WHERE id = $1
    `,
    [params.jobId, JSON.stringify(params.preconditions ?? {})]
  );
}

export async function completeEvaluationJob(params: {
  jobId: string;
  result?: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE evaluation_jobs
      SET
        status = 'succeeded',
        result = $2,
        finished_at = NOW(),
        last_error = NULL,
        runtime_worker_id = NULL,
        runtime_lease_token = NULL,
        runtime_lease_expires_at = NULL
      WHERE id = $1
    `,
    [params.jobId, JSON.stringify(params.result ?? {})]
  );
}

export async function failEvaluationJob(params: {
  jobId: string;
  error: string;
  result?: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE evaluation_jobs
      SET
        status = 'failed',
        result = $3,
        finished_at = NOW(),
        last_error = $2,
        runtime_worker_id = NULL,
        runtime_lease_token = NULL,
        runtime_lease_expires_at = NULL
      WHERE id = $1
    `,
    [params.jobId, params.error, JSON.stringify(params.result ?? {})]
  );
}

export async function cancelEvaluationJob(jobId: string): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE evaluation_jobs
      SET
        status = 'canceled',
        finished_at = NOW(),
        runtime_worker_id = NULL,
        runtime_lease_token = NULL,
        runtime_lease_expires_at = NULL
      WHERE id = $1
        AND status IN ('queued', 'running')
    `,
    [jobId]
  );
}

export async function releaseEvaluationJobLease(params: {
  jobId: string;
  workerId: string;
  leaseToken: string;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE evaluation_jobs
      SET
        runtime_worker_id = NULL,
        runtime_lease_token = NULL,
        runtime_lease_expires_at = NULL
      WHERE id = $1
        AND runtime_worker_id = $2
        AND runtime_lease_token = $3
    `,
    [params.jobId, params.workerId, params.leaseToken]
  );
}
