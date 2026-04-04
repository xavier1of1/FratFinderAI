import { getDbPool } from "../db";
import type {
  BenchmarkAlert,
  BenchmarkCycleSample,
  BenchmarkFieldName,
  BenchmarkQueueSnapshot,
  BenchmarkRunConfig,
  BenchmarkRunListItem,
  BenchmarkRunSummary,
  BenchmarkShadowDiff,
  BenchmarkStatus
} from "../types";


function envRuntimeModeDefault(): "legacy" | "adaptive_shadow" | "adaptive_assisted" | "adaptive_primary" {
  const value = String(process.env.BENCHMARK_CRAWL_RUNTIME_MODE ?? "adaptive_assisted").trim();
  if (value === "legacy" || value === "adaptive_shadow" || value === "adaptive_assisted" || value === "adaptive_primary") {
    return value;
  }
  return "adaptive_assisted";
}

function envFieldJobRuntimeModeDefault(): "legacy" | "langgraph_shadow" | "langgraph_primary" {
  const value = String(process.env.BENCHMARK_FIELD_JOB_RUNTIME_MODE ?? "legacy").trim();
  if (value === "legacy" || value === "langgraph_shadow" || value === "langgraph_primary") {
    return value;
  }
  return "legacy";
}

function envFieldJobGraphDurabilityDefault(): "exit" | "async" | "sync" {
  const value = String(process.env.BENCHMARK_FIELD_JOB_GRAPH_DURABILITY ?? "sync").trim();
  if (value === "exit" || value === "async" || value === "sync") {
    return value;
  }
  return "sync";
}
function envWarmupDefault(): boolean {
  const value = String(process.env.BENCHMARK_RUN_ADAPTIVE_WARMUP ?? "true").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

interface BenchmarkShadowDiffRow {
  id: number;
  benchmarkRunId: string;
  cycle: number;
  runtimeMode: string;
  observedJobs: number;
  decisionMismatchCount: number;
  statusMismatchCount: number;
  mismatchRate: number;
  details: Record<string, unknown>;
  createdAt: string;
}

interface BenchmarkAlertRow {
  id: number;
  benchmarkRunId: string | null;
  alertType: string;
  severity: "info" | "warning" | "critical";
  status: "open" | "resolved";
  message: string;
  fingerprint: string | null;
  payload: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  resolvedAt: string | null;
}

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

  const runtimeMode =
    raw.crawlRuntimeMode === "legacy" ||
    raw.crawlRuntimeMode === "adaptive_shadow" ||
    raw.crawlRuntimeMode === "adaptive_assisted" ||
    raw.crawlRuntimeMode === "adaptive_primary"
      ? raw.crawlRuntimeMode
      : envRuntimeModeDefault();

  const fieldJobRuntimeMode = raw.fieldJobRuntimeMode === "legacy" || raw.fieldJobRuntimeMode === "langgraph_shadow" || raw.fieldJobRuntimeMode === "langgraph_primary" ? raw.fieldJobRuntimeMode : envFieldJobRuntimeModeDefault();
  const fieldJobGraphDurability = raw.fieldJobGraphDurability === "exit" || raw.fieldJobGraphDurability === "async" || raw.fieldJobGraphDurability === "sync" ? raw.fieldJobGraphDurability : envFieldJobGraphDurabilityDefault();

  return {
    fieldName: raw.fieldName ?? fieldName,
    sourceSlug: raw.sourceSlug ?? sourceSlug,
    workers: Number.isFinite(raw.workers) ? Number(raw.workers) : 8,
    limitPerCycle: Number.isFinite(raw.limitPerCycle) ? Number(raw.limitPerCycle) : 25,
    cycles: Number.isFinite(raw.cycles) ? Number(raw.cycles) : 5,
    pauseMs: Number.isFinite(raw.pauseMs) ? Number(raw.pauseMs) : 500,
    crawlRuntimeMode: runtimeMode,
    fieldJobRuntimeMode,
    fieldJobGraphDurability,
    runAdaptiveCrawlBeforeCycles: typeof raw.runAdaptiveCrawlBeforeCycles === "boolean" ? raw.runAdaptiveCrawlBeforeCycles : envWarmupDefault()
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

function normalizeShadowDiffs(rows: unknown): BenchmarkShadowDiff[] {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows
    .map((item) => {
      if (typeof item !== "object" || item === null) {
        return null;
      }
      const value = item as Partial<BenchmarkShadowDiff>;
      return {
        id: Number(value.id ?? 0),
        benchmarkRunId: String(value.benchmarkRunId ?? ""),
        cycle: Number(value.cycle ?? 0),
        runtimeMode: String(value.runtimeMode ?? "legacy"),
        observedJobs: Number(value.observedJobs ?? 0),
        decisionMismatchCount: Number(value.decisionMismatchCount ?? 0),
        statusMismatchCount: Number(value.statusMismatchCount ?? 0),
        mismatchRate: Number(value.mismatchRate ?? 0),
        details: typeof value.details === "object" && value.details !== null ? (value.details as Record<string, unknown>) : {},
        createdAt: typeof value.createdAt === "string" ? value.createdAt : new Date().toISOString(),
      };
    })
    .filter((item): item is BenchmarkShadowDiff => item !== null);
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
    shadowDiffs: [],
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

export async function failStaleBenchmarkRuns(maxAgeMinutes = 10): Promise<number> {
  const dbPool = getDbPool();
  const { rowCount } = await dbPool.query(
    `
      UPDATE benchmark_runs
      SET
        status = 'failed',
        finished_at = NOW(),
        last_error = COALESCE(last_error, 'Benchmark run stalled before completion')
      WHERE status IN ('queued', 'running')
        AND updated_at < NOW() - ($1::int * INTERVAL '1 minute')
    `,
    [Math.max(1, maxAgeMinutes)]
  );
  return Number(rowCount ?? 0);
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
  if (!row) {
    return null;
  }

  const run = mapBenchmarkRow(row);
  run.shadowDiffs = await listBenchmarkShadowDiffs(id);
  return run;
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
    [params.name, params.fieldName, params.sourceSlug, JSON.stringify(params.config)]
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
    [params.id, JSON.stringify(params.summary), JSON.stringify(params.samples)]
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
    [
      params.id,
      params.error,
      params.summary ? JSON.stringify(params.summary) : null,
      JSON.stringify(params.samples),
    ]
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


export async function upsertBenchmarkShadowDiff(params: {
  benchmarkRunId: string;
  cycle: number;
  runtimeMode: string;
  observedJobs: number;
  decisionMismatchCount: number;
  statusMismatchCount: number;
  mismatchRate: number;
  details: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      INSERT INTO benchmark_shadow_diffs (
        benchmark_run_id,
        cycle,
        runtime_mode,
        observed_jobs,
        decision_mismatch_count,
        status_mismatch_count,
        mismatch_rate,
        details
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (benchmark_run_id, cycle)
      DO UPDATE SET
        runtime_mode = EXCLUDED.runtime_mode,
        observed_jobs = EXCLUDED.observed_jobs,
        decision_mismatch_count = EXCLUDED.decision_mismatch_count,
        status_mismatch_count = EXCLUDED.status_mismatch_count,
        mismatch_rate = EXCLUDED.mismatch_rate,
        details = EXCLUDED.details,
        created_at = NOW()
    `,
    [
      params.benchmarkRunId,
      Math.max(1, params.cycle),
      params.runtimeMode,
      Math.max(0, params.observedJobs),
      Math.max(0, params.decisionMismatchCount),
      Math.max(0, params.statusMismatchCount),
      Math.max(0, params.mismatchRate),
      JSON.stringify(params.details ?? {}),
    ]
  );
}

export async function listBenchmarkShadowDiffs(benchmarkRunId: string, limit = 200): Promise<BenchmarkShadowDiff[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<BenchmarkShadowDiffRow>(
    `
      SELECT
        id,
        benchmark_run_id AS "benchmarkRunId",
        cycle,
        runtime_mode AS "runtimeMode",
        observed_jobs AS "observedJobs",
        decision_mismatch_count AS "decisionMismatchCount",
        status_mismatch_count AS "statusMismatchCount",
        mismatch_rate AS "mismatchRate",
        details,
        created_at AS "createdAt"
      FROM benchmark_shadow_diffs
      WHERE benchmark_run_id = $1
      ORDER BY cycle ASC
      LIMIT $2
    `,
    [benchmarkRunId, Math.max(1, limit)]
  );

  return normalizeShadowDiffs(rows);
}


export async function computeBenchmarkShadowDiffWindow(params: {
  sourceSlug: string | null;
  fieldName: BenchmarkFieldName;
  runtimeMode: string;
  fromIso: string;
  toIso: string;
}): Promise<{
  observedJobs: number;
  decisionMismatchCount: number;
  statusMismatchCount: number;
  mismatchRate: number;
  details: Record<string, unknown>;
}> {
  const dbPool = getDbPool();
  const fieldNameFilter = params.fieldName === "all" ? null : params.fieldName;

  const { rows } = await dbPool.query<{
    jobId: string;
    attempt: number;
    fieldName: string;
    decisionStatus: string;
    writeAllowed: boolean;
    requiresReview: boolean;
    reasonCodes: string[];
    metadata: Record<string, unknown>;
    jobStatus: string | null;
    createdAt: string;
  }>(
    `
      SELECT
        d.job_id AS "jobId",
        d.attempt,
        d.field_name AS "fieldName",
        d.decision_status AS "decisionStatus",
        d.write_allowed AS "writeAllowed",
        d.requires_review AS "requiresReview",
        d.reason_codes AS "reasonCodes",
        d.metadata,
        fj.status AS "jobStatus",
        d.created_at AS "createdAt"
      FROM field_job_graph_decisions d
      INNER JOIN field_job_graph_runs r ON r.id = d.run_id
      LEFT JOIN field_jobs fj ON fj.id = d.job_id
      WHERE d.created_at >= $1::timestamptz
        AND d.created_at <= $2::timestamptz
        AND ($3::text IS NULL OR r.source_slug = $3)
        AND ($4::text IS NULL OR d.field_name = $4)
        AND ($5::text IS NULL OR r.runtime_mode = $5)
      ORDER BY d.created_at ASC
    `,
    [params.fromIso, params.toIso, params.sourceSlug, fieldNameFilter, params.runtimeMode]
  );

  let decisionMismatchCount = 0;
  let statusMismatchCount = 0;
  const mismatches: Array<Record<string, unknown>> = [];
  const byDecision: Record<string, number> = {};

  const expectedStatus = (decisionStatus: string): string | null => {
    if (decisionStatus === "complete") return "done";
    if (decisionStatus === "fail_terminal") return "failed";
    if (decisionStatus === "requeue") return "queued";
    return null;
  };

  for (const row of rows) {
    byDecision[row.decisionStatus] = (byDecision[row.decisionStatus] ?? 0) + 1;

    const invalidDecision =
      (row.decisionStatus === "complete" && !row.writeAllowed) ||
      ((row.decisionStatus === "requeue" || row.decisionStatus === "fail_terminal") && row.writeAllowed);
    if (invalidDecision) {
      decisionMismatchCount += 1;
    }

    const expected = expectedStatus(row.decisionStatus);
    const actual = row.jobStatus;
    let statusMismatch = false;
    if (expected === "queued") {
      statusMismatch = actual !== "queued" && actual !== "running";
    } else if (expected) {
      statusMismatch = actual !== expected;
    }

    if (statusMismatch) {
      statusMismatchCount += 1;
    }

    if ((invalidDecision || statusMismatch) && mismatches.length < 20) {
      mismatches.push({
        jobId: row.jobId,
        attempt: row.attempt,
        fieldName: row.fieldName,
        decisionStatus: row.decisionStatus,
        writeAllowed: row.writeAllowed,
        requiresReview: row.requiresReview,
        reasonCodes: row.reasonCodes,
        expectedStatus: expected,
        actualStatus: actual,
        createdAt: row.createdAt,
      });
    }
  }

  const observedJobs = rows.length;
  const mismatchRate = observedJobs > 0 ? (decisionMismatchCount + statusMismatchCount) / observedJobs : 0;

  return {
    observedJobs,
    decisionMismatchCount,
    statusMismatchCount,
    mismatchRate,
    details: {
      window: { fromIso: params.fromIso, toIso: params.toIso },
      decisionHistogram: byDecision,
      samples: mismatches,
    },
  };
}


function mapBenchmarkAlertRow(row: BenchmarkAlertRow): BenchmarkAlert {
  return {
    id: Number(row.id),
    benchmarkRunId: row.benchmarkRunId,
    alertType: row.alertType,
    severity: row.severity,
    status: row.status,
    message: row.message,
    fingerprint: row.fingerprint,
    payload: row.payload ?? {},
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    resolvedAt: row.resolvedAt,
  };
}

export async function listBenchmarkAlerts(params?: {
  limit?: number;
  status?: "open" | "resolved" | "all";
  benchmarkRunId?: string | null;
  severity?: "info" | "warning" | "critical" | "all";
}): Promise<BenchmarkAlert[]> {
  const dbPool = getDbPool();
  const limit = Math.max(1, Math.min(500, Number(params?.limit ?? 100)));
  const status = params?.status ?? "open";
  const severity = params?.severity ?? "all";

  const { rows } = await dbPool.query<BenchmarkAlertRow>(
    `
      SELECT
        id,
        benchmark_run_id AS "benchmarkRunId",
        alert_type AS "alertType",
        severity,
        status,
        message,
        fingerprint,
        payload,
        created_at AS "createdAt",
        updated_at AS "updatedAt",
        resolved_at AS "resolvedAt"
      FROM benchmark_alerts
      WHERE ($1::text = 'all' OR status = $1)
        AND ($2::uuid IS NULL OR benchmark_run_id = $2)
        AND ($3::text = 'all' OR severity = $3)
      ORDER BY created_at DESC
      LIMIT $4
    `,
    [status, params?.benchmarkRunId ?? null, severity, limit]
  );

  return rows.map(mapBenchmarkAlertRow);
}

export async function getBenchmarkAlertSummary(): Promise<{
  openTotal: number;
  resolvedTotal: number;
  openInfo: number;
  openWarning: number;
  openCritical: number;
  resolvedLast24h: number;
  lastUpdatedAt: string;
}> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    openTotal: number;
    resolvedTotal: number;
    openInfo: number;
    openWarning: number;
    openCritical: number;
    resolvedLast24h: number;
    lastUpdatedAt: string;
  }>(
    `
      SELECT
        COUNT(*) FILTER (WHERE status = 'open')::int AS "openTotal",
        COUNT(*) FILTER (WHERE status = 'resolved')::int AS "resolvedTotal",
        COUNT(*) FILTER (WHERE status = 'open' AND severity = 'info')::int AS "openInfo",
        COUNT(*) FILTER (WHERE status = 'open' AND severity = 'warning')::int AS "openWarning",
        COUNT(*) FILTER (WHERE status = 'open' AND severity = 'critical')::int AS "openCritical",
        COUNT(*) FILTER (WHERE status = 'resolved' AND resolved_at >= NOW() - INTERVAL '24 hours')::int AS "resolvedLast24h",
        COALESCE(MAX(updated_at), NOW())::timestamptz::text AS "lastUpdatedAt"
      FROM benchmark_alerts
    `
  );

  const row = rows[0];
  return {
    openTotal: Number(row?.openTotal ?? 0),
    resolvedTotal: Number(row?.resolvedTotal ?? 0),
    openInfo: Number(row?.openInfo ?? 0),
    openWarning: Number(row?.openWarning ?? 0),
    openCritical: Number(row?.openCritical ?? 0),
    resolvedLast24h: Number(row?.resolvedLast24h ?? 0),
    lastUpdatedAt: row?.lastUpdatedAt ?? new Date().toISOString(),
  };
}
export async function upsertOpenBenchmarkAlert(params: {
  benchmarkRunId: string | null;
  alertType: string;
  severity: "info" | "warning" | "critical";
  message: string;
  fingerprint?: string | null;
  payload?: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  const fingerprint = params.fingerprint?.trim() || null;

  if (fingerprint) {
    await dbPool.query(
      `
        INSERT INTO benchmark_alerts (
          benchmark_run_id,
          alert_type,
          severity,
          status,
          message,
          fingerprint,
          payload
        )
        VALUES ($1, $2, $3, 'open', $4, $5, $6)
        ON CONFLICT (fingerprint) WHERE status = 'open' AND fingerprint IS NOT NULL
        DO UPDATE SET
          benchmark_run_id = EXCLUDED.benchmark_run_id,
          alert_type = EXCLUDED.alert_type,
          severity = EXCLUDED.severity,
          message = EXCLUDED.message,
          payload = EXCLUDED.payload,
          updated_at = NOW(),
          resolved_at = NULL,
          status = 'open'
      `,
      [
        params.benchmarkRunId,
        params.alertType,
        params.severity,
        params.message,
        fingerprint,
        JSON.stringify(params.payload ?? {}),
      ]
    );
    return;
  }

  await dbPool.query(
    `
      INSERT INTO benchmark_alerts (
        benchmark_run_id,
        alert_type,
        severity,
        status,
        message,
        fingerprint,
        payload
      )
      VALUES ($1, $2, $3, 'open', $4, NULL, $5)
    `,
    [
      params.benchmarkRunId,
      params.alertType,
      params.severity,
      params.message,
      JSON.stringify(params.payload ?? {}),
    ]
  );
}

export async function resolveBenchmarkAlertsByFingerprintPrefix(params: {
  prefix: string;
  resolvedReason?: string;
  metadata?: Record<string, unknown>;
}): Promise<number> {
  const dbPool = getDbPool();
  const trimmed = params.prefix.trim();
  if (!trimmed) {
    return 0;
  }
  const reason = params.resolvedReason?.trim() || "drift_scan_refresh";
  const metadata = params.metadata ?? {};
  const { rowCount } = await dbPool.query(
    `
      UPDATE benchmark_alerts
      SET
        status = 'resolved',
        payload = COALESCE(payload, '{}'::jsonb) || jsonb_build_object(
          'resolvedBy', 'drift_scan',
          'resolvedReason', $2,
          'resolvedByScanAt', NOW(),
          'resolvedMetadata', $3::jsonb
        ),
        resolved_at = NOW(),
        updated_at = NOW()
      WHERE status = 'open'
        AND fingerprint LIKE ($1 || '%')
    `,
    [trimmed, reason, JSON.stringify(metadata)]
  );
  return Number(rowCount ?? 0);
}




