import os from "os";

import { getDbPool } from "../db";

export type RuntimeWorkerLane =
  | "request"
  | "campaign"
  | "benchmark"
  | "evaluation"
  | "contact_resolution"
  | "chapter_repair";

export async function upsertRuntimeWorker(params: {
  workerId: string;
  workloadLane: RuntimeWorkerLane;
  runtimeOwner: string;
  status?: "active" | "idle" | "stopped" | "failed";
  leaseSeconds?: number | null;
  metadata?: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      INSERT INTO worker_processes (
        worker_id,
        workload_lane,
        runtime_owner,
        hostname,
        process_id,
        status,
        lease_expires_at,
        last_heartbeat_at,
        metadata
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        CASE
          WHEN $7::int IS NULL THEN NULL
          ELSE NOW() + ($7::int * INTERVAL '1 second')
        END,
        NOW(),
        $8::jsonb
      )
      ON CONFLICT (worker_id)
      DO UPDATE SET
        workload_lane = EXCLUDED.workload_lane,
        runtime_owner = EXCLUDED.runtime_owner,
        hostname = EXCLUDED.hostname,
        process_id = EXCLUDED.process_id,
        status = EXCLUDED.status,
        lease_expires_at = EXCLUDED.lease_expires_at,
        last_heartbeat_at = NOW(),
        metadata = EXCLUDED.metadata
    `,
    [
      params.workerId,
      params.workloadLane,
      params.runtimeOwner,
      os.hostname(),
      process.pid,
      params.status ?? "active",
      params.leaseSeconds ?? null,
      JSON.stringify(params.metadata ?? {}),
    ]
  );
}

export async function heartbeatRuntimeWorker(workerId: string, leaseSeconds?: number | null): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE worker_processes
      SET
        last_heartbeat_at = NOW(),
        lease_expires_at = CASE
          WHEN $2::int IS NULL THEN lease_expires_at
          ELSE NOW() + ($2::int * INTERVAL '1 second')
        END,
        status = 'active'
      WHERE worker_id = $1
    `,
    [workerId, leaseSeconds ?? null]
  );
}

export async function stopRuntimeWorker(workerId: string, status: "stopped" | "failed" = "stopped"): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE worker_processes
      SET
        status = $2,
        lease_expires_at = NULL,
        last_heartbeat_at = NOW()
      WHERE worker_id = $1
    `,
    [workerId, status]
  );
}

export async function countActiveRuntimeWorkersByLane(
  lanes: RuntimeWorkerLane[]
): Promise<Record<string, number>> {
  if (lanes.length === 0) {
    return {};
  }
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ workloadLane: string; count: number }>(
    `
      SELECT workload_lane AS "workloadLane", COUNT(*)::int AS count
      FROM worker_processes
      WHERE workload_lane = ANY($1::text[])
        AND status = 'active'
        AND (lease_expires_at IS NULL OR lease_expires_at >= NOW())
      GROUP BY workload_lane
    `,
    [lanes]
  );
  return Object.fromEntries(rows.map((row) => [row.workloadLane, Number(row.count ?? 0)]));
}

export async function claimBenchmarkRunLease(params: {
  runId: string;
  workerId: string;
  leaseToken: string;
  leaseSeconds: number;
}): Promise<boolean> {
  const dbPool = getDbPool();
  const { rowCount } = await dbPool.query(
    `
      UPDATE benchmark_runs
      SET
        runtime_worker_id = $2,
        runtime_lease_token = $3,
        runtime_lease_expires_at = NOW() + ($4::int * INTERVAL '1 second'),
        runtime_last_heartbeat_at = NOW()
      WHERE id = $1
        AND status IN ('queued', 'running')
        AND (
          runtime_worker_id IS NULL
          OR runtime_lease_expires_at IS NULL
          OR runtime_lease_expires_at < NOW()
          OR (runtime_worker_id = $2 AND runtime_lease_token = $3)
        )
    `,
    [params.runId, params.workerId, params.leaseToken, Math.max(15, params.leaseSeconds)]
  );
  return Number(rowCount ?? 0) > 0;
}

export async function heartbeatBenchmarkRunLease(params: {
  runId: string;
  workerId: string;
  leaseToken: string;
  leaseSeconds: number;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE benchmark_runs
      SET
        runtime_lease_expires_at = NOW() + ($4::int * INTERVAL '1 second'),
        runtime_last_heartbeat_at = NOW()
      WHERE id = $1
        AND runtime_worker_id = $2
        AND runtime_lease_token = $3
    `,
    [params.runId, params.workerId, params.leaseToken, Math.max(15, params.leaseSeconds)]
  );
}

export async function releaseBenchmarkRunLease(params: {
  runId: string;
  workerId: string;
  leaseToken: string;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE benchmark_runs
      SET
        runtime_worker_id = NULL,
        runtime_lease_token = NULL,
        runtime_lease_expires_at = NULL
      WHERE id = $1
        AND runtime_worker_id = $2
        AND runtime_lease_token = $3
    `,
    [params.runId, params.workerId, params.leaseToken]
  );
}

export async function claimCampaignRunLease(params: {
  runId: string;
  workerId: string;
  leaseToken: string;
  leaseSeconds: number;
}): Promise<boolean> {
  const dbPool = getDbPool();
  const { rowCount } = await dbPool.query(
    `
      UPDATE campaign_runs
      SET
        runtime_worker_id = $2,
        runtime_lease_token = $3,
        runtime_lease_expires_at = NOW() + ($4::int * INTERVAL '1 second'),
        runtime_last_heartbeat_at = NOW()
      WHERE id = $1
        AND status IN ('queued', 'running')
        AND (
          runtime_worker_id IS NULL
          OR runtime_lease_expires_at IS NULL
          OR runtime_lease_expires_at < NOW()
          OR (runtime_worker_id = $2 AND runtime_lease_token = $3)
        )
    `,
    [params.runId, params.workerId, params.leaseToken, Math.max(15, params.leaseSeconds)]
  );
  return Number(rowCount ?? 0) > 0;
}

export async function heartbeatCampaignRunLease(params: {
  runId: string;
  workerId: string;
  leaseToken: string;
  leaseSeconds: number;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE campaign_runs
      SET
        runtime_lease_expires_at = NOW() + ($4::int * INTERVAL '1 second'),
        runtime_last_heartbeat_at = NOW()
      WHERE id = $1
        AND runtime_worker_id = $2
        AND runtime_lease_token = $3
    `,
    [params.runId, params.workerId, params.leaseToken, Math.max(15, params.leaseSeconds)]
  );
}

export async function releaseCampaignRunLease(params: {
  runId: string;
  workerId: string;
  leaseToken: string;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE campaign_runs
      SET
        runtime_worker_id = NULL,
        runtime_lease_token = NULL,
        runtime_lease_expires_at = NULL
      WHERE id = $1
        AND runtime_worker_id = $2
        AND runtime_lease_token = $3
    `,
    [params.runId, params.workerId, params.leaseToken]
  );
}
