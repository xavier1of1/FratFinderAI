import { getDbPool } from "../db";
import type {
  CrawlRunListItem,
  FraternityCrawlProgress,
  FraternityCrawlRequest,
  FraternityCrawlRequestConfig,
  FraternityCrawlRequestEvent,
  FraternityCrawlRequestStage,
  FraternityCrawlRequestStatus
} from "../types";

interface FraternityCrawlRequestRow {
  id: string;
  fraternityName: string;
  fraternitySlug: string;
  sourceSlug: string | null;
  sourceUrl: string | null;
  sourceConfidence: number | string | null;
  status: FraternityCrawlRequestStatus;
  stage: FraternityCrawlRequestStage;
  runtimeWorkerId: string | null;
  runtimeLeaseExpiresAt: string | null;
  runtimeLastHeartbeatAt: string | null;
  scheduledFor: string;
  startedAt: string | null;
  finishedAt: string | null;
  priority: number;
  config: FraternityCrawlRequestConfig;
  progress: FraternityCrawlProgress;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

interface FraternityCrawlRequestEventRow {
  id: number;
  requestId: string;
  eventType: string;
  message: string;
  payload: Record<string, unknown>;
  createdAt: string;
}

const DEFAULT_CONFIG: FraternityCrawlRequestConfig = {
  fieldJobWorkers: 8,
  fieldJobLimitPerCycle: 50,
  maxEnrichmentCycles: 24,
  pauseMs: 500,
  crawlPolicyVersion: null
};

function normalizeConfig(config: unknown): FraternityCrawlRequestConfig {
  if (!config || typeof config !== "object") {
    return { ...DEFAULT_CONFIG };
  }

  const value = config as Partial<FraternityCrawlRequestConfig>;
  return {
    fieldJobWorkers: Number.isFinite(value.fieldJobWorkers) ? Math.max(1, Number(value.fieldJobWorkers)) : DEFAULT_CONFIG.fieldJobWorkers,
    fieldJobLimitPerCycle: Number.isFinite(value.fieldJobLimitPerCycle)
      ? Math.max(1, Number(value.fieldJobLimitPerCycle))
      : DEFAULT_CONFIG.fieldJobLimitPerCycle,
    maxEnrichmentCycles: Number.isFinite(value.maxEnrichmentCycles)
      ? Math.max(1, Number(value.maxEnrichmentCycles))
      : DEFAULT_CONFIG.maxEnrichmentCycles,
    pauseMs: Number.isFinite(value.pauseMs) ? Math.max(0, Number(value.pauseMs)) : DEFAULT_CONFIG.pauseMs,
    crawlPolicyVersion: typeof value.crawlPolicyVersion === "string" && value.crawlPolicyVersion.trim()
      ? value.crawlPolicyVersion.trim()
      : null
  };
}

function normalizeProgress(progress: unknown): FraternityCrawlProgress {
  if (!progress || typeof progress !== "object") {
    return {};
  }
  return progress as FraternityCrawlProgress;
}

function mapRequestRow(row: FraternityCrawlRequestRow, events: FraternityCrawlRequestEvent[]): FraternityCrawlRequest {
  const sourceConfidence =
    row.sourceConfidence === null ? null : Number.isFinite(Number(row.sourceConfidence)) ? Number(row.sourceConfidence) : null;

  return {
    id: row.id,
    fraternityName: row.fraternityName,
    fraternitySlug: row.fraternitySlug,
    sourceSlug: row.sourceSlug,
    sourceUrl: row.sourceUrl,
    sourceConfidence,
    status: row.status,
    stage: row.stage,
    runtimeWorkerId: row.runtimeWorkerId,
    runtimeLeaseExpiresAt: row.runtimeLeaseExpiresAt,
    runtimeLastHeartbeatAt: row.runtimeLastHeartbeatAt,
    scheduledFor: row.scheduledFor,
    startedAt: row.startedAt,
    finishedAt: row.finishedAt,
    priority: row.priority,
    config: normalizeConfig(row.config),
    progress: normalizeProgress(row.progress),
    lastError: row.lastError,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    events
  };
}

async function listEventsForRequests(requestIds: string[]): Promise<Map<string, FraternityCrawlRequestEvent[]>> {
  const grouped = new Map<string, FraternityCrawlRequestEvent[]>();
  if (requestIds.length === 0) {
    return grouped;
  }

  const dbPool = getDbPool();
  const { rows } = await dbPool.query<FraternityCrawlRequestEventRow>(
    `
      SELECT
        id,
        request_id AS "requestId",
        event_type AS "eventType",
        message,
        payload,
        created_at AS "createdAt"
      FROM fraternity_crawl_request_events
      WHERE request_id = ANY($1::uuid[])
      ORDER BY created_at DESC
    `,
    [requestIds]
  );

  for (const row of rows) {
    const current = grouped.get(row.requestId) ?? [];
    current.push({
      id: row.id,
      requestId: row.requestId,
      eventType: row.eventType,
      message: row.message,
      payload: row.payload ?? {},
      createdAt: row.createdAt
    });
    grouped.set(row.requestId, current);
  }

  return grouped;
}

export async function listFraternityCrawlRequests(limit = 100): Promise<FraternityCrawlRequest[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<FraternityCrawlRequestRow>(
    `
      SELECT
        id,
        fraternity_name AS "fraternityName",
        fraternity_slug AS "fraternitySlug",
        source_slug AS "sourceSlug",
        source_url AS "sourceUrl",
        source_confidence AS "sourceConfidence",
        status,
        stage,
        runtime_worker_id AS "runtimeWorkerId",
        runtime_lease_expires_at AS "runtimeLeaseExpiresAt",
        runtime_last_heartbeat_at AS "runtimeLastHeartbeatAt",
        scheduled_for AS "scheduledFor",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        priority,
        config,
        progress,
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM fraternity_crawl_requests
      ORDER BY created_at DESC
      LIMIT $1
    `,
    [limit]
  );

  const eventsByRequestId = await listEventsForRequests(rows.map((row) => row.id));
  return rows.map((row) => mapRequestRow(row, eventsByRequestId.get(row.id) ?? []));
}

export async function getFraternityCrawlRequest(id: string): Promise<FraternityCrawlRequest | null> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<FraternityCrawlRequestRow>(
    `
      SELECT
        id,
        fraternity_name AS "fraternityName",
        fraternity_slug AS "fraternitySlug",
        source_slug AS "sourceSlug",
        source_url AS "sourceUrl",
        source_confidence AS "sourceConfidence",
        status,
        stage,
        runtime_worker_id AS "runtimeWorkerId",
        runtime_lease_expires_at AS "runtimeLeaseExpiresAt",
        runtime_last_heartbeat_at AS "runtimeLastHeartbeatAt",
        scheduled_for AS "scheduledFor",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        priority,
        config,
        progress,
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM fraternity_crawl_requests
      WHERE id = $1
      LIMIT 1
    `,
    [id]
  );

  const row = rows[0];
  if (!row) {
    return null;
  }

  const eventsByRequestId = await listEventsForRequests([id]);
  return mapRequestRow(row, eventsByRequestId.get(id) ?? []);
}

export async function createFraternityCrawlRequest(params: {
  fraternityName: string;
  fraternitySlug: string;
  sourceSlug: string | null;
  sourceUrl: string | null;
  sourceConfidence: number | null;
  status: FraternityCrawlRequestStatus;
  stage: FraternityCrawlRequestStage;
  scheduledFor: string;
  priority?: number;
  config?: Partial<FraternityCrawlRequestConfig>;
  progress?: FraternityCrawlProgress;
  lastError?: string | null;
}): Promise<FraternityCrawlRequest> {
  const dbPool = getDbPool();
  const config = normalizeConfig(params.config);
  const { rows } = await dbPool.query<FraternityCrawlRequestRow>(
    `
      INSERT INTO fraternity_crawl_requests (
        fraternity_name,
        fraternity_slug,
        source_slug,
        source_url,
        source_confidence,
        status,
        stage,
        scheduled_for,
        priority,
        config,
        progress,
        last_error
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      RETURNING
        id,
        fraternity_name AS "fraternityName",
        fraternity_slug AS "fraternitySlug",
        source_slug AS "sourceSlug",
        source_url AS "sourceUrl",
        source_confidence AS "sourceConfidence",
        status,
        stage,
        runtime_worker_id AS "runtimeWorkerId",
        runtime_lease_expires_at AS "runtimeLeaseExpiresAt",
        runtime_last_heartbeat_at AS "runtimeLastHeartbeatAt",
        scheduled_for AS "scheduledFor",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        priority,
        config,
        progress,
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
    `,
    [
      params.fraternityName,
      params.fraternitySlug,
      params.sourceSlug,
      params.sourceUrl,
      params.sourceConfidence,
      params.status,
      params.stage,
      params.scheduledFor,
      params.priority ?? 0,
      config,
      params.progress ?? {},
      params.lastError ?? null
    ]
  );

  const row = rows[0];
  if (!row) {
    throw new Error("Failed to create fraternity crawl request");
  }
  return mapRequestRow(row, []);
}

export async function appendFraternityCrawlRequestEvent(params: {
  requestId: string;
  eventType: string;
  message: string;
  payload?: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      INSERT INTO fraternity_crawl_request_events (request_id, event_type, message, payload)
      VALUES ($1, $2, $3, $4)
    `,
    [params.requestId, params.eventType, params.message, params.payload ?? {}]
  );
}

export async function updateFraternityCrawlRequest(params: {
  id: string;
  sourceSlug?: string | null;
  sourceUrl?: string | null;
  sourceConfidence?: number | null;
  status?: FraternityCrawlRequestStatus;
  stage?: FraternityCrawlRequestStage;
  scheduledFor?: string;
  startedAtNow?: boolean;
  finishedAtNow?: boolean;
  clearFinishedAt?: boolean;
  priority?: number;
  config?: FraternityCrawlRequestConfig;
  progress?: FraternityCrawlProgress;
  lastError?: string | null;
}): Promise<void> {
  const updates: string[] = [];
  const values: unknown[] = [];

  const push = (fragment: string, value: unknown) => {
    values.push(value);
    updates.push(`${fragment} = $${values.length}`);
  };

  if (params.sourceSlug !== undefined) push("source_slug", params.sourceSlug);
  if (params.sourceUrl !== undefined) push("source_url", params.sourceUrl);
  if (params.sourceConfidence !== undefined) push("source_confidence", params.sourceConfidence);
  if (params.status !== undefined) push("status", params.status);
  if (params.stage !== undefined) push("stage", params.stage);
  if (params.scheduledFor !== undefined) push("scheduled_for", params.scheduledFor);
  if (params.priority !== undefined) push("priority", params.priority);
  if (params.config !== undefined) push("config", normalizeConfig(params.config));
  if (params.progress !== undefined) push("progress", params.progress);
  if (params.lastError !== undefined) push("last_error", params.lastError);
  if (params.startedAtNow) updates.push("started_at = NOW()");
  if (params.finishedAtNow) updates.push("finished_at = NOW()");
  if (params.clearFinishedAt) updates.push("finished_at = NULL");

  if (updates.length === 0) {
    return;
  }

  values.push(params.id);
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE fraternity_crawl_requests
      SET ${updates.join(", ")}
      WHERE id = $${values.length}
    `,
    values
  );
}

export async function reconcileStaleFraternityCrawlRequests(maxAgeMinutes = 30): Promise<number> {
  const dbPool = getDbPool();
  const { rowCount } = await dbPool.query(
    `
      UPDATE fraternity_crawl_requests
      SET
        status = 'failed',
        stage = 'failed',
        finished_at = NOW(),
        last_error = COALESCE(last_error, 'Fraternity crawl request stalled before completion'),
        runtime_worker_id = NULL,
        runtime_lease_token = NULL,
        runtime_lease_expires_at = NULL
      WHERE status = 'running'
        AND (
          (runtime_lease_expires_at IS NOT NULL AND runtime_lease_expires_at < NOW())
          OR (runtime_lease_expires_at IS NULL AND updated_at < NOW() - ($1::int * INTERVAL '1 minute'))
        )
    `,
    [Math.max(1, maxAgeMinutes)]
  );
  return Number(rowCount ?? 0);
}

export async function listDueQueuedFraternityCrawlRequestIds(limit = 20): Promise<string[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ id: string }>(
    `
      SELECT id
      FROM fraternity_crawl_requests
      WHERE status = 'queued'
        AND scheduled_for <= NOW()
        AND (
          runtime_worker_id IS NULL
          OR runtime_lease_expires_at IS NULL
          OR runtime_lease_expires_at < NOW()
        )
      ORDER BY priority DESC, scheduled_for ASC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );
  return rows.map((row) => row.id);
}

export async function bumpQueuedFieldJobsForSource(sourceSlug: string, priority = 100): Promise<number> {
  const dbPool = getDbPool();
  const { rowCount } = await dbPool.query(
    `
      UPDATE field_jobs fj
      SET
        priority = GREATEST(priority, $2),
        scheduled_at = NOW()
      FROM crawl_runs cr
      JOIN sources s ON s.id = cr.source_id
      WHERE fj.crawl_run_id = cr.id
        AND s.slug = $1
        AND fj.status = 'queued'
    `,
    [sourceSlug, Math.max(1, priority)]
  );
  return Number(rowCount ?? 0);
}

export interface SourceFieldJobSnapshot {
  field: "find_website" | "find_email" | "find_instagram";
  queued: number;
  running: number;
  done: number;
  failed: number;
}

type SourceFieldName = SourceFieldJobSnapshot["field"];
type SourceFieldJobStatus = "queued" | "running" | "done" | "failed";
type SourceFieldCounts = Record<SourceFieldJobStatus, number>;

export async function getSourceFieldJobSnapshot(sourceSlug: string): Promise<SourceFieldJobSnapshot[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    fieldName: "find_website" | "find_email" | "find_instagram";
    status: "queued" | "running" | "done" | "failed";
    count: number;
  }>(
    `
      SELECT
        fj.field_name AS "fieldName",
        fj.status,
        COUNT(*)::int AS count
      FROM field_jobs fj
      JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
      JOIN sources s ON s.id = cr.source_id
      WHERE s.slug = $1
        AND fj.field_name IN ('find_website', 'find_email', 'find_instagram')
      GROUP BY fj.field_name, fj.status
    `,
    [sourceSlug]
  );

  const empty = (): SourceFieldCounts => ({ queued: 0, running: 0, done: 0, failed: 0 });
  const grouped: Record<SourceFieldName, SourceFieldCounts> = {
    find_website: empty(),
    find_email: empty(),
    find_instagram: empty()
  };

  for (const row of rows) {
    const bucket = grouped[row.fieldName];
    bucket[row.status] = Number(row.count ?? 0);
  }

  const fields: SourceFieldName[] = ["find_website", "find_email", "find_instagram"];
  return fields.map((field) => ({ field, ...grouped[field] }));
}

export async function getLatestCrawlRunForSource(
  sourceSlug: string,
  options?: {
    startedAfter?: string;
    excludeRunId?: number | null;
  }
): Promise<CrawlRunListItem | null> {
  const dbPool = getDbPool();
  const whereClauses = ["s.slug = $1"];
  const values: Array<string | number> = [sourceSlug];

  if (options?.startedAfter) {
    values.push(options.startedAfter);
    whereClauses.push(`cr.started_at >= $${values.length}::timestamptz`);
  }

  if (options?.excludeRunId !== undefined && options.excludeRunId !== null) {
    values.push(options.excludeRunId);
    whereClauses.push(`cr.id <> $${values.length}`);
  }

  const { rows } = await dbPool.query<CrawlRunListItem>(
    `
      SELECT
        cr.id,
        s.slug AS "sourceSlug",
        cr.status,
        cr.started_at AS "startedAt",
        cr.finished_at AS "finishedAt",
        cr.pages_processed AS "pagesProcessed",
        cr.records_seen AS "recordsSeen",
        cr.records_upserted AS "recordsUpserted",
        cr.review_items_created AS "reviewItemsCreated",
        cr.field_jobs_created AS "fieldJobsCreated",
        cr.last_error AS "lastError",
        cr.extraction_metadata ->> 'strategy_used' AS "strategyUsed",
        NULLIF(cr.extraction_metadata ->> 'page_level_confidence', '')::double precision AS "pageLevelConfidence",
        COALESCE(NULLIF(cr.extraction_metadata ->> 'llm_calls_used', '')::integer, 0) AS "llmCallsUsed"
      FROM crawl_runs cr
      JOIN sources s ON s.id = cr.source_id
      WHERE ${whereClauses.join(" AND ")}
      ORDER BY cr.started_at DESC
      LIMIT 1
    `,
    values
  );

  return rows[0] ?? null;
}


export async function upsertFraternityRecord(params: {
  slug: string;
  name: string;
  nicAffiliated?: boolean;
}): Promise<{ id: string; slug: string }> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ id: string; slug: string }>(
    `
      INSERT INTO fraternities (slug, name, nic_affiliated)
      VALUES ($1, $2, $3)
      ON CONFLICT (slug)
      DO UPDATE SET
        name = EXCLUDED.name,
        nic_affiliated = EXCLUDED.nic_affiliated,
        updated_at = NOW()
      RETURNING id, slug
    `,
    [params.slug, params.name, params.nicAffiliated ?? true]
  );

  const row = rows[0];
  if (!row) {
    throw new Error("Failed to upsert fraternity record");
  }
  return row;
}

export async function upsertSourceRecord(params: {
  fraternityId: string;
  slug: string;
  baseUrl: string;
  listPath?: string | null;
  sourceType?: "unsupported" | "html_directory" | "json_api" | "script_embedded" | "locator_api";
  parserKey?: string;
  active?: boolean;
  metadata?: Record<string, unknown>;
}): Promise<{ id: string; slug: string }> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ id: string; slug: string }>(
    `
      INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, active, metadata)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (slug)
      DO UPDATE SET
        fraternity_id = EXCLUDED.fraternity_id,
        source_type = EXCLUDED.source_type,
        parser_key = EXCLUDED.parser_key,
        base_url = EXCLUDED.base_url,
        list_path = EXCLUDED.list_path,
        active = EXCLUDED.active,
        metadata = EXCLUDED.metadata,
        updated_at = NOW()
      RETURNING id, slug
    `,
    [
      params.fraternityId,
      params.slug,
      params.sourceType ?? "unsupported",
      params.parserKey ?? "unsupported",
      params.baseUrl,
      params.listPath ?? null,
      params.active ?? true,
      params.metadata ?? {}
    ]
  );

  const row = rows[0];
  if (!row) {
    throw new Error("Failed to upsert source record");
  }
  return row;
}

