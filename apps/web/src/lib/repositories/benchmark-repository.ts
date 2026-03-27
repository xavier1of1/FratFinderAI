import { getDbPool } from "../db";
import type {
  BenchmarkCycleSample,
  BenchmarkFieldName,
  BenchmarkQueueSnapshot,
  BenchmarkRunConfig,
  BenchmarkRunListItem,
  BenchmarkRunSummary,
  BenchmarkStatus
} from "../types";

interface BenchmarkRunRow {
  id: string;
  name: string;
  status: BenchmarkStatus;
  fieldName: BenchmarkFieldName;
  sourceSlug: string | null;
  config: BenchmarkRunConfig;
  summary: BenchmarkRunSummary | null;
  samples: BenchmarkCycleSample[];
  startedAt: string | null;
  finishedAt: string | null;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

function normalizeBenchmarkConfig(fieldName: BenchmarkFieldName, sourceSlug: string | null, config: unknown): BenchmarkRunConfig {
  const raw = typeof config === "object" && config !== null ? (config as Partial<BenchmarkRunConfig>) : {};

  return {
    fieldName: raw.fieldName ?? fieldName,
    sourceSlug: raw.sourceSlug ?? sourceSlug,
    workers: Number.isFinite(raw.workers) ? Number(raw.workers) : 8,
    limitPerCycle: Number.isFinite(raw.limitPerCycle) ? Number(raw.limitPerCycle) : 25,
    cycles: Number.isFinite(raw.cycles) ? Number(raw.cycles) : 5,
    pauseMs: Number.isFinite(raw.pauseMs) ? Number(raw.pauseMs) : 500
  };
}

function normalizeSamples(samples: unknown): BenchmarkCycleSample[] {
  if (!Array.isArray(samples)) {
    return [];
  }

  return samples
    .map((item) => {
      if (typeof item !== "object" || item === null) {
        return null;
      }

      const value = item as Partial<BenchmarkCycleSample>;
      return {
        cycle: Number(value.cycle ?? 0),
        startedAt: typeof value.startedAt === "string" ? value.startedAt : new Date().toISOString(),
        durationMs: Number(value.durationMs ?? 0),
        processed: Number(value.processed ?? 0),
        requeued: Number(value.requeued ?? 0),
        failedTerminal: Number(value.failedTerminal ?? 0),
        queued: Number(value.queued ?? 0),
        running: Number(value.running ?? 0),
        done: Number(value.done ?? 0),
        failed: Number(value.failed ?? 0)
      };
    })
    .filter((item): item is BenchmarkCycleSample => item !== null);
}

function normalizeSummary(summary: unknown): BenchmarkRunSummary | null {
  if (typeof summary !== "object" || summary === null) {
    return null;
  }

  const value = summary as Partial<BenchmarkRunSummary>;
  return {
    elapsedMs: Number(value.elapsedMs ?? 0),
    cyclesCompleted: Number(value.cyclesCompleted ?? 0),
    totalProcessed: Number(value.totalProcessed ?? 0),
    totalRequeued: Number(value.totalRequeued ?? 0),
    totalFailedTerminal: Number(value.totalFailedTerminal ?? 0),
    jobsPerMinute: Number(value.jobsPerMinute ?? 0),
    avgCycleMs: Number(value.avgCycleMs ?? 0),
    queueDepthStart: Number(value.queueDepthStart ?? 0),
    queueDepthEnd: Number(value.queueDepthEnd ?? 0),
    queueDepthDelta: Number(value.queueDepthDelta ?? 0)
  };
}

function mapBenchmarkRow(row: BenchmarkRunRow): BenchmarkRunListItem {
  return {
    id: row.id,
    name: row.name,
    status: row.status,
    fieldName: row.fieldName,
    sourceSlug: row.sourceSlug,
    config: normalizeBenchmarkConfig(row.fieldName, row.sourceSlug, row.config),
    summary: normalizeSummary(row.summary),
    samples: normalizeSamples(row.samples),
    startedAt: row.startedAt,
    finishedAt: row.finishedAt,
    lastError: row.lastError,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt
  };
}

export async function listBenchmarkRuns(limit = 100): Promise<BenchmarkRunListItem[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<BenchmarkRunRow>(
    `
      SELECT
        id,
        name,
        status,
        target_field_name AS "fieldName",
        source_slug AS "sourceSlug",
        config,
        summary,
        samples,
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM benchmark_runs
      ORDER BY created_at DESC
      LIMIT $1
    `,
    [limit]
  );

  return rows.map(mapBenchmarkRow);
}

export async function getBenchmarkRun(id: string): Promise<BenchmarkRunListItem | null> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<BenchmarkRunRow>(
    `
      SELECT
        id,
        name,
        status,
        target_field_name AS "fieldName",
        source_slug AS "sourceSlug",
        config,
        summary,
        samples,
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM benchmark_runs
      WHERE id = $1
      LIMIT 1
    `,
    [id]
  );

  const row = rows[0];
  return row ? mapBenchmarkRow(row) : null;
}

export async function createBenchmarkRun(params: {
  name: string;
  fieldName: BenchmarkFieldName;
  sourceSlug: string | null;
  config: BenchmarkRunConfig;
}): Promise<BenchmarkRunListItem> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<BenchmarkRunRow>(
    `
      INSERT INTO benchmark_runs (
        name,
        status,
        target_field_name,
        source_slug,
        config,
        summary,
        samples
      )
      VALUES ($1, 'queued', $2, $3, $4, NULL, '[]'::jsonb)
      RETURNING
        id,
        name,
        status,
        target_field_name AS "fieldName",
        source_slug AS "sourceSlug",
        config,
        summary,
        samples,
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
    `,
    [params.name, params.fieldName, params.sourceSlug, params.config]
  );

  const row = rows[0];
  if (!row) {
    throw new Error("Failed to create benchmark run");
  }

  return mapBenchmarkRow(row);
}

export async function markBenchmarkRunStarted(id: string): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE benchmark_runs
      SET
        status = 'running',
        started_at = COALESCE(started_at, NOW()),
        finished_at = NULL,
        last_error = NULL
      WHERE id = $1
    `,
    [id]
  );
}

export async function completeBenchmarkRun(params: {
  id: string;
  summary: BenchmarkRunSummary;
  samples: BenchmarkCycleSample[];
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE benchmark_runs
      SET
        status = 'succeeded',
        summary = $2,
        samples = $3,
        finished_at = NOW(),
        last_error = NULL
      WHERE id = $1
    `,
    [params.id, params.summary, params.samples]
  );
}

export async function failBenchmarkRun(params: {
  id: string;
  error: string;
  summary: BenchmarkRunSummary | null;
  samples: BenchmarkCycleSample[];
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE benchmark_runs
      SET
        status = 'failed',
        summary = COALESCE($3, summary),
        samples = $4,
        finished_at = NOW(),
        last_error = $2
      WHERE id = $1
    `,
    [params.id, params.error, params.summary, params.samples]
  );
}

export async function getFieldJobStatusSnapshot(params: {
  fieldName: BenchmarkFieldName;
  sourceSlug: string | null;
}): Promise<BenchmarkQueueSnapshot> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ status: string; count: number }>(
    `
      SELECT
        fj.status,
        COUNT(*)::int AS count
      FROM field_jobs fj
      LEFT JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
      LEFT JOIN sources s ON s.id = cr.source_id
      WHERE ($1 = 'all' OR fj.field_name = $1)
        AND ($2::text IS NULL OR s.slug = $2)
      GROUP BY fj.status
    `,
    [params.fieldName, params.sourceSlug]
  );

  const snapshot: BenchmarkQueueSnapshot = {
    queued: 0,
    running: 0,
    done: 0,
    failed: 0,
    total: 0
  };

  for (const row of rows) {
    const count = Number(row.count ?? 0);
    if (row.status === "queued") snapshot.queued = count;
    if (row.status === "running") snapshot.running = count;
    if (row.status === "done") snapshot.done = count;
    if (row.status === "failed") snapshot.failed = count;
    snapshot.total += count;
  }

  return snapshot;
}