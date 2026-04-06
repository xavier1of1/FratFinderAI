import { getDbPool } from "../db";
import { evaluateSourceUrl } from "../source-selection";
import type {
  AdaptivePolicySnapshot,
  CampaignProviderHealthHistoryPoint,
  CampaignProviderHealthSnapshot,
  CampaignRun,
  CampaignRunConfig,
  CampaignRunEvent,
  CampaignRunItem,
  CampaignRunStatus,
  CampaignRunSummary,
  CampaignRunTelemetry,
  CampaignScorecard
} from "../types";

interface CampaignRunRow {
  id: string;
  name: string;
  status: CampaignRunStatus;
  scheduledFor: string;
  startedAt: string | null;
  finishedAt: string | null;
  config: CampaignRunConfig;
  summary: CampaignRunSummary;
  telemetry: CampaignRunTelemetry;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

interface CampaignRunItemRow {
  id: string;
  campaignRunId: string;
  fraternityName: string;
  fraternitySlug: string;
  requestId: string | null;
  cohort: "new" | "control";
  status: CampaignRunItem["status"];
  selectionReason: string | null;
  scorecard: CampaignScorecard;
  notes: string | null;
  createdAt: string;
  updatedAt: string;
}

interface CampaignRunEventRow {
  id: number;
  campaignRunId: string;
  eventType: string;
  message: string;
  payload: Record<string, unknown>;
  createdAt: string;
}

export interface SelectedCampaignFraternity {
  fraternityName: string;
  fraternitySlug: string;
  nationalUrl: string;
  confidence: number;
  cohort: "new" | "control";
  selectionReason: string;
  sourceSlug: string;
}

export interface PreferredCampaignSource {
  fraternityName: string;
  fraternitySlug: string;
  sourceSlug: string;
  sourceUrl: string;
  confidence: number;
  sourceProvenance: "verified_registry" | "existing_source" | "search";
  selectionReason: string;
}

const DEFAULT_CONFIG: CampaignRunConfig = {
  targetCount: 20,
  controlCount: 2,
  activeConcurrency: 4,
  maxDurationMinutes: 120,
  checkpointIntervalMs: 5 * 60_000,
  tuningIntervalMs: 15 * 60_000,
  itemPollIntervalMs: 15_000,
  preflightRequired: true,
  autoTuningEnabled: true,
  controlFraternitySlugs: [],
  programMode: "standard",
  runtimeMode: "adaptive_primary",
  fieldJobRuntimeMode: "langgraph_primary",
  frozenSourceSlugs: [],
  trainingRounds: 3,
  epochsPerRound: 1,
  trainingSourceBatchSize: 4,
  evalSourceBatchSize: 3,
  trainingCommandTimeoutMinutes: 30,
  checkpointPromotionEnabled: true,
  queueStallThresholdMinutes: 15,
  reviewWindowDays: 14
};

const EMPTY_SUMMARY: CampaignRunSummary = {
  targetCount: 20,
  itemCount: 0,
  completedCount: 0,
  failedCount: 0,
  skippedCount: 0,
  activeCount: 0,
  anyContactSuccessRate: 0,
  allThreeSuccessRate: 0,
  websiteCoverageRate: 0,
  emailCoverageRate: 0,
  instagramCoverageRate: 0,
  jobsPerMinute: 0,
  queueDepthStart: 0,
  queueDepthEnd: 0,
  queueDepthDelta: 0,
  totalProcessed: 0,
  totalRequeued: 0,
  totalFailedTerminal: 0,
  durationMs: 0,
  checkpointCount: 0
};

function normalizeConfig(config: unknown): CampaignRunConfig {
  if (!config || typeof config !== "object") {
    return { ...DEFAULT_CONFIG };
  }

  const value = config as Partial<CampaignRunConfig>;
  return {
    targetCount: Number.isFinite(value.targetCount) ? Math.max(1, Number(value.targetCount)) : DEFAULT_CONFIG.targetCount,
    controlCount: Number.isFinite(value.controlCount) ? Math.max(0, Number(value.controlCount)) : DEFAULT_CONFIG.controlCount,
    activeConcurrency: Number.isFinite(value.activeConcurrency)
      ? Math.max(1, Number(value.activeConcurrency))
      : DEFAULT_CONFIG.activeConcurrency,
    maxDurationMinutes: Number.isFinite(value.maxDurationMinutes)
      ? Math.max(5, Number(value.maxDurationMinutes))
      : DEFAULT_CONFIG.maxDurationMinutes,
    checkpointIntervalMs: Number.isFinite(value.checkpointIntervalMs)
      ? Math.max(10_000, Number(value.checkpointIntervalMs))
      : DEFAULT_CONFIG.checkpointIntervalMs,
    tuningIntervalMs: Number.isFinite(value.tuningIntervalMs)
      ? Math.max(30_000, Number(value.tuningIntervalMs))
      : DEFAULT_CONFIG.tuningIntervalMs,
    itemPollIntervalMs: Number.isFinite(value.itemPollIntervalMs)
      ? Math.max(5_000, Number(value.itemPollIntervalMs))
      : DEFAULT_CONFIG.itemPollIntervalMs,
    preflightRequired: value.preflightRequired ?? DEFAULT_CONFIG.preflightRequired,
    autoTuningEnabled: value.autoTuningEnabled ?? DEFAULT_CONFIG.autoTuningEnabled,
    controlFraternitySlugs: Array.isArray(value.controlFraternitySlugs)
      ? value.controlFraternitySlugs.map((item) => String(item)).filter(Boolean)
      : DEFAULT_CONFIG.controlFraternitySlugs,
    programMode: value.programMode === "v4_rl_improvement" ? "v4_rl_improvement" : DEFAULT_CONFIG.programMode,
    runtimeMode:
      value.runtimeMode === "legacy" ||
      value.runtimeMode === "adaptive_shadow" ||
      value.runtimeMode === "adaptive_assisted" ||
      value.runtimeMode === "adaptive_primary"
        ? value.runtimeMode
        : DEFAULT_CONFIG.runtimeMode,
    fieldJobRuntimeMode:
      value.fieldJobRuntimeMode === "legacy" ||
      value.fieldJobRuntimeMode === "langgraph_shadow" ||
      value.fieldJobRuntimeMode === "langgraph_primary"
        ? value.fieldJobRuntimeMode
        : DEFAULT_CONFIG.fieldJobRuntimeMode,
    frozenSourceSlugs: Array.isArray(value.frozenSourceSlugs)
      ? value.frozenSourceSlugs.map((item) => String(item).trim()).filter(Boolean)
      : DEFAULT_CONFIG.frozenSourceSlugs,
    trainingRounds: Number.isFinite(value.trainingRounds)
      ? Math.max(1, Math.min(6, Number(value.trainingRounds)))
      : DEFAULT_CONFIG.trainingRounds,
    epochsPerRound: Number.isFinite(value.epochsPerRound)
      ? Math.max(1, Math.min(8, Number(value.epochsPerRound)))
      : DEFAULT_CONFIG.epochsPerRound,
    trainingSourceBatchSize: Number.isFinite(value.trainingSourceBatchSize)
      ? Math.max(1, Math.min(20, Number(value.trainingSourceBatchSize)))
      : DEFAULT_CONFIG.trainingSourceBatchSize,
    evalSourceBatchSize: Number.isFinite(value.evalSourceBatchSize)
      ? Math.max(1, Math.min(20, Number(value.evalSourceBatchSize)))
      : DEFAULT_CONFIG.evalSourceBatchSize,
    trainingCommandTimeoutMinutes: Number.isFinite(value.trainingCommandTimeoutMinutes)
      ? Math.max(5, Math.min(180, Number(value.trainingCommandTimeoutMinutes)))
      : DEFAULT_CONFIG.trainingCommandTimeoutMinutes,
    checkpointPromotionEnabled:
      value.checkpointPromotionEnabled ?? DEFAULT_CONFIG.checkpointPromotionEnabled,
    queueStallThresholdMinutes: Number.isFinite(value.queueStallThresholdMinutes)
      ? Math.max(5, Math.min(120, Number(value.queueStallThresholdMinutes)))
      : DEFAULT_CONFIG.queueStallThresholdMinutes,
    reviewWindowDays: Number.isFinite(value.reviewWindowDays)
      ? Math.max(1, Math.min(90, Number(value.reviewWindowDays)))
      : DEFAULT_CONFIG.reviewWindowDays
  };
}

function normalizeSummary(summary: unknown, targetCount: number): CampaignRunSummary {
  if (!summary || typeof summary !== "object") {
    return { ...EMPTY_SUMMARY, targetCount };
  }
  const value = summary as Partial<CampaignRunSummary>;
  return {
    targetCount,
    itemCount: Number(value.itemCount ?? 0),
    completedCount: Number(value.completedCount ?? 0),
    failedCount: Number(value.failedCount ?? 0),
    skippedCount: Number(value.skippedCount ?? 0),
    activeCount: Number(value.activeCount ?? 0),
    anyContactSuccessRate: Number(value.anyContactSuccessRate ?? 0),
    allThreeSuccessRate: Number(value.allThreeSuccessRate ?? 0),
    websiteCoverageRate: Number(value.websiteCoverageRate ?? 0),
    emailCoverageRate: Number(value.emailCoverageRate ?? 0),
    instagramCoverageRate: Number(value.instagramCoverageRate ?? 0),
    jobsPerMinute: Number(value.jobsPerMinute ?? 0),
    queueDepthStart: Number(value.queueDepthStart ?? 0),
    queueDepthEnd: Number(value.queueDepthEnd ?? 0),
    queueDepthDelta: Number(value.queueDepthDelta ?? 0),
    totalProcessed: Number(value.totalProcessed ?? 0),
    totalRequeued: Number(value.totalRequeued ?? 0),
    totalFailedTerminal: Number(value.totalFailedTerminal ?? 0),
    durationMs: Number(value.durationMs ?? 0),
    checkpointCount: Number(value.checkpointCount ?? 0)
  };
}

function normalizeTelemetry(telemetry: unknown): CampaignRunTelemetry {
  if (!telemetry || typeof telemetry !== "object") {
    return {};
  }
  const value = telemetry as CampaignRunTelemetry;
  const providerHealthHistory = Array.isArray(value.providerHealthHistory)
    ? value.providerHealthHistory.map((item) => {
        const historyPoint = item as Partial<CampaignProviderHealthHistoryPoint>;
        return {
          timestamp: String(historyPoint.timestamp ?? new Date().toISOString()),
          healthy: Boolean(historyPoint.healthy),
          successRate: Number(historyPoint.successRate ?? 0),
          probes: Number(historyPoint.probes ?? 0),
          successes: Number(historyPoint.successes ?? 0),
          minSuccessRate: Number(historyPoint.minSuccessRate ?? 0),
          providerHealth:
            historyPoint.providerHealth && typeof historyPoint.providerHealth === "object"
              ? historyPoint.providerHealth
              : {},
          activeConcurrency: Number(historyPoint.activeConcurrency ?? 0),
          queueDepth: Number(historyPoint.queueDepth ?? 0)
        };
      })
    : [];
  return {
    providerHealth: value.providerHealth ?? null,
    providerHealthHistory,
    activeConcurrency: value.activeConcurrency ?? undefined,
    lastCheckpointAt: value.lastCheckpointAt ?? null,
    lastTuneAt: value.lastTuneAt ?? null,
    runtimeNotes: Array.isArray(value.runtimeNotes) ? value.runtimeNotes.map((item) => String(item)) : [],
    cohortManifest: Array.isArray(value.cohortManifest) ? value.cohortManifest.map((item) => String(item)) : [],
    activePolicyVersion: typeof value.activePolicyVersion === "string" ? value.activePolicyVersion : null,
    activePolicySnapshotId: Number.isFinite(Number(value.activePolicySnapshotId)) ? Number(value.activePolicySnapshotId) : null,
    promotionDecisions: Array.isArray(value.promotionDecisions) ? value.promotionDecisions : [],
    queueStallAlert:
      value.queueStallAlert && typeof value.queueStallAlert === "object"
        ? value.queueStallAlert
        : null,
    delayedRewardHealth:
      value.delayedRewardHealth && typeof value.delayedRewardHealth === "object"
        ? value.delayedRewardHealth
        : null,
    reviewReasonDrift: Array.isArray(value.reviewReasonDrift) ? value.reviewReasonDrift : [],
    acceptanceGate:
      value.acceptanceGate && typeof value.acceptanceGate === "object"
        ? value.acceptanceGate
        : null,
    baselineSnapshot:
      value.baselineSnapshot && typeof value.baselineSnapshot === "object"
        ? value.baselineSnapshot
        : null,
    finalSnapshot:
      value.finalSnapshot && typeof value.finalSnapshot === "object"
        ? value.finalSnapshot
        : null,
    programPhase:
      value.programPhase === "baseline" ||
      value.programPhase === "training" ||
      value.programPhase === "live_campaign" ||
      value.programPhase === "completed"
        ? value.programPhase
        : undefined,
    programStartedAt: typeof value.programStartedAt === "string" ? value.programStartedAt : null
  };
}

function emptyScorecard(): CampaignScorecard {
  return {
    baselineTotalChapters: 0,
    baselineWebsitesFound: 0,
    baselineEmailsFound: 0,
    baselineInstagramsFound: 0,
    baselineChaptersWithAnyContact: 0,
    baselineChaptersWithAllThree: 0,
    chaptersDiscovered: 0,
    fieldJobsCreated: 0,
    processedJobs: 0,
    requeuedJobs: 0,
    failedTerminalJobs: 0,
    reviewItemsCreated: 0,
    websitesFound: 0,
    emailsFound: 0,
    instagramsFound: 0,
    chaptersWithAnyContact: 0,
    chaptersWithAllThree: 0,
    sourceNativeYield: 0,
    searchEfficiency: 0,
    retryEfficiency: 0,
    confidenceQuality: 0,
    providerResilience: 0,
    queueEfficiency: 0,
    providerAttempts: {},
    failureHistogram: []
  };
}

function normalizeScorecard(scorecard: unknown): CampaignScorecard {
  if (!scorecard || typeof scorecard !== "object") {
    return emptyScorecard();
  }
  const value = scorecard as Partial<CampaignScorecard>;
  return {
    baselineTotalChapters: Number(value.baselineTotalChapters ?? 0),
    baselineWebsitesFound: Number(value.baselineWebsitesFound ?? 0),
    baselineEmailsFound: Number(value.baselineEmailsFound ?? 0),
    baselineInstagramsFound: Number(value.baselineInstagramsFound ?? 0),
    baselineChaptersWithAnyContact: Number(value.baselineChaptersWithAnyContact ?? 0),
    baselineChaptersWithAllThree: Number(value.baselineChaptersWithAllThree ?? 0),
    chaptersDiscovered: Number(value.chaptersDiscovered ?? 0),
    fieldJobsCreated: Number(value.fieldJobsCreated ?? 0),
    processedJobs: Number(value.processedJobs ?? 0),
    requeuedJobs: Number(value.requeuedJobs ?? 0),
    failedTerminalJobs: Number(value.failedTerminalJobs ?? 0),
    reviewItemsCreated: Number(value.reviewItemsCreated ?? 0),
    websitesFound: Number(value.websitesFound ?? 0),
    emailsFound: Number(value.emailsFound ?? 0),
    instagramsFound: Number(value.instagramsFound ?? 0),
    chaptersWithAnyContact: Number(value.chaptersWithAnyContact ?? 0),
    chaptersWithAllThree: Number(value.chaptersWithAllThree ?? 0),
    sourceNativeYield: Number(value.sourceNativeYield ?? 0),
    searchEfficiency: Number(value.searchEfficiency ?? 0),
    retryEfficiency: Number(value.retryEfficiency ?? 0),
    confidenceQuality: Number(value.confidenceQuality ?? 0),
    providerResilience: Number(value.providerResilience ?? 0),
    queueEfficiency: Number(value.queueEfficiency ?? 0),
    providerAttempts:
      value.providerAttempts && typeof value.providerAttempts === "object"
        ? Object.fromEntries(Object.entries(value.providerAttempts).map(([key, count]) => [key, Number(count ?? 0)]))
        : {},
    failureHistogram: Array.isArray(value.failureHistogram)
      ? value.failureHistogram.map((entry) => ({
          reason: String((entry as { reason?: unknown }).reason ?? "unknown"),
          count: Number((entry as { count?: unknown }).count ?? 0)
        }))
      : []
  };
}

function mapCampaignRunRow(row: CampaignRunRow, items: CampaignRunItem[], events: CampaignRunEvent[]): CampaignRun {
  const config = normalizeConfig(row.config);
  return {
    id: row.id,
    name: row.name,
    status: row.status,
    scheduledFor: row.scheduledFor,
    startedAt: row.startedAt,
    finishedAt: row.finishedAt,
    config,
    summary: normalizeSummary(row.summary, config.targetCount),
    telemetry: normalizeTelemetry(row.telemetry),
    lastError: row.lastError,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    items,
    events
  };
}

async function listItemsForCampaignRuns(runIds: string[]): Promise<Map<string, CampaignRunItem[]>> {
  const grouped = new Map<string, CampaignRunItem[]>();
  if (runIds.length === 0) {
    return grouped;
  }
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<CampaignRunItemRow>(
    `
      SELECT
        id,
        campaign_run_id AS "campaignRunId",
        fraternity_name AS "fraternityName",
        fraternity_slug AS "fraternitySlug",
        request_id AS "requestId",
        cohort,
        status,
        selection_reason AS "selectionReason",
        scorecard,
        notes,
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM campaign_run_items
      WHERE campaign_run_id = ANY($1::uuid[])
      ORDER BY created_at ASC
    `,
    [runIds]
  );

  for (const row of rows) {
    const item: CampaignRunItem = {
      id: row.id,
      campaignRunId: row.campaignRunId,
      fraternityName: row.fraternityName,
      fraternitySlug: row.fraternitySlug,
      requestId: row.requestId,
      cohort: row.cohort,
      status: row.status,
      selectionReason: row.selectionReason,
      scorecard: normalizeScorecard(row.scorecard),
      notes: row.notes,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt
    };

    const current = grouped.get(row.campaignRunId) ?? [];
    current.push(item);
    grouped.set(row.campaignRunId, current);
  }

  return grouped;
}

async function listEventsForCampaignRuns(runIds: string[]): Promise<Map<string, CampaignRunEvent[]>> {
  const grouped = new Map<string, CampaignRunEvent[]>();
  if (runIds.length === 0) {
    return grouped;
  }
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<CampaignRunEventRow>(
    `
      SELECT
        id,
        campaign_run_id AS "campaignRunId",
        event_type AS "eventType",
        message,
        payload,
        created_at AS "createdAt"
      FROM campaign_run_events
      WHERE campaign_run_id = ANY($1::uuid[])
      ORDER BY created_at DESC
    `,
    [runIds]
  );

  for (const row of rows) {
    const event: CampaignRunEvent = {
      id: row.id,
      campaignRunId: row.campaignRunId,
      eventType: row.eventType,
      message: row.message,
      payload: row.payload ?? {},
      createdAt: row.createdAt
    };

    const current = grouped.get(row.campaignRunId) ?? [];
    current.push(event);
    grouped.set(row.campaignRunId, current);
  }

  return grouped;
}

export async function listCampaignRuns(limit = 50): Promise<CampaignRun[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<CampaignRunRow>(
    `
      SELECT
        id,
        name,
        status,
        scheduled_for AS "scheduledFor",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        config,
        summary,
        telemetry,
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM campaign_runs
      ORDER BY created_at DESC
      LIMIT $1
    `,
    [limit]
  );

  const ids = rows.map((row) => row.id);
  const [itemsByRunId, eventsByRunId] = await Promise.all([listItemsForCampaignRuns(ids), listEventsForCampaignRuns(ids)]);
  return rows.map((row) => mapCampaignRunRow(row, itemsByRunId.get(row.id) ?? [], eventsByRunId.get(row.id) ?? []));
}

export async function getCampaignRun(id: string): Promise<CampaignRun | null> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<CampaignRunRow>(
    `
      SELECT
        id,
        name,
        status,
        scheduled_for AS "scheduledFor",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        config,
        summary,
        telemetry,
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM campaign_runs
      WHERE id = $1
      LIMIT 1
    `,
    [id]
  );
  const row = rows[0];
  if (!row) return null;

  const [itemsByRunId, eventsByRunId] = await Promise.all([listItemsForCampaignRuns([id]), listEventsForCampaignRuns([id])]);
  return mapCampaignRunRow(row, itemsByRunId.get(id) ?? [], eventsByRunId.get(id) ?? []);
}

export async function createCampaignRun(params: {
  name: string;
  config?: Partial<CampaignRunConfig>;
  scheduledFor?: string;
}): Promise<CampaignRun> {
  const dbPool = getDbPool();
  const config = normalizeConfig(params.config);
  const summary = normalizeSummary({}, config.targetCount);
  const telemetry = normalizeTelemetry({
    activeConcurrency: config.activeConcurrency,
    runtimeNotes: []
  });

  const { rows } = await dbPool.query<CampaignRunRow>(
    `
      INSERT INTO campaign_runs (
        name,
        status,
        scheduled_for,
        config,
        summary,
        telemetry
      )
      VALUES ($1, 'queued', $2, $3, $4, $5)
      RETURNING
        id,
        name,
        status,
        scheduled_for AS "scheduledFor",
        started_at AS "startedAt",
        finished_at AS "finishedAt",
        config,
        summary,
        telemetry,
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
    `,
    [params.name, params.scheduledFor ?? new Date().toISOString(), config, summary, telemetry]
  );

  const row = rows[0];
  if (!row) {
    throw new Error("Failed to create campaign run");
  }
  return mapCampaignRunRow(row, [], []);
}

export async function insertCampaignItems(
  campaignRunId: string,
  items: SelectedCampaignFraternity[]
): Promise<void> {
  if (items.length === 0) {
    return;
  }
  const dbPool = getDbPool();
  for (const item of items) {
    await dbPool.query(
      `
        INSERT INTO campaign_run_items (
          campaign_run_id,
          fraternity_name,
          fraternity_slug,
          cohort,
          status,
          selection_reason,
          scorecard
        )
        VALUES ($1, $2, $3, $4, 'planned', $5, $6)
        ON CONFLICT (campaign_run_id, fraternity_slug)
        DO NOTHING
      `,
      [campaignRunId, item.fraternityName, item.fraternitySlug, item.cohort, item.selectionReason, emptyScorecard()]
    );
  }
}

export async function updateCampaignRun(params: {
  id: string;
  status?: CampaignRunStatus;
  scheduledFor?: string;
  startedAtNow?: boolean;
  finishedAtNow?: boolean;
  clearFinishedAt?: boolean;
  config?: CampaignRunConfig;
  summary?: CampaignRunSummary;
  telemetry?: CampaignRunTelemetry;
  lastError?: string | null;
}): Promise<void> {
  const values: unknown[] = [];
  const updates: string[] = [];

  const push = (fragment: string, value: unknown) => {
    values.push(value);
    updates.push(`${fragment} = $${values.length}`);
  };

  if (params.status !== undefined) push("status", params.status);
  if (params.scheduledFor !== undefined) push("scheduled_for", params.scheduledFor);
  if (params.config !== undefined) push("config", normalizeConfig(params.config));
  if (params.summary !== undefined) push("summary", params.summary);
  if (params.telemetry !== undefined) push("telemetry", normalizeTelemetry(params.telemetry));
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
      UPDATE campaign_runs
      SET ${updates.join(", ")}
      WHERE id = $${values.length}
    `,
    values
  );
}

export async function appendCampaignRunEvent(params: {
  campaignRunId: string;
  eventType: string;
  message: string;
  payload?: Record<string, unknown>;
}): Promise<void> {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      INSERT INTO campaign_run_events (campaign_run_id, event_type, message, payload)
      VALUES ($1, $2, $3, $4)
    `,
    [params.campaignRunId, params.eventType, params.message, params.payload ?? {}]
  );
}

export async function updateCampaignRunItem(params: {
  id: string;
  requestId?: string | null;
  status?: CampaignRunItem["status"];
  scorecard?: CampaignScorecard;
  notes?: string | null;
}): Promise<void> {
  const values: unknown[] = [];
  const updates: string[] = [];
  const push = (fragment: string, value: unknown) => {
    values.push(value);
    updates.push(`${fragment} = $${values.length}`);
  };
  if (params.requestId !== undefined) push("request_id", params.requestId);
  if (params.status !== undefined) push("status", params.status);
  if (params.scorecard !== undefined) push("scorecard", normalizeScorecard(params.scorecard));
  if (params.notes !== undefined) push("notes", params.notes);
  if (updates.length === 0) return;

  values.push(params.id);
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE campaign_run_items
      SET ${updates.join(", ")}
      WHERE id = $${values.length}
    `,
    values
  );
}

export async function listQueuedCampaignRunIds(limit = 10): Promise<string[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ id: string }>(
    `
      SELECT id
      FROM campaign_runs
      WHERE status = 'queued'
        AND scheduled_for <= NOW()
      ORDER BY scheduled_for ASC, created_at ASC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );
  return rows.map((row) => row.id);
}

export async function reconcileStaleCampaignRuns(maxAgeMinutes = 45): Promise<number> {
  const dbPool = getDbPool();
  const { rowCount } = await dbPool.query(
    `
      UPDATE campaign_runs
      SET
        status = 'failed',
        finished_at = NOW(),
        last_error = COALESCE(last_error, 'Campaign run stalled before completion')
      WHERE status = 'running'
        AND updated_at < NOW() - ($1::int * INTERVAL '1 minute')
    `,
    [Math.max(5, maxAgeMinutes)]
  );
  return Number(rowCount ?? 0);
}

export async function selectCampaignFraternities(config: CampaignRunConfig): Promise<SelectedCampaignFraternity[]> {
  if (config.frozenSourceSlugs && config.frozenSourceSlugs.length > 0) {
    const selected: SelectedCampaignFraternity[] = [];
    for (const sourceSlug of config.frozenSourceSlugs) {
      const fraternitySlug = sourceSlug.endsWith("-main") ? sourceSlug.slice(0, -5) : sourceSlug;
      const preferredSource = await getPreferredCampaignSourceForFraternity(fraternitySlug);
      const verifiedSource = preferredSource ? null : await getVerifiedSourceForFraternity(fraternitySlug);
      const source = preferredSource
        ? {
            fraternityName: preferredSource.fraternityName,
            fraternitySlug: preferredSource.fraternitySlug,
            nationalUrl: preferredSource.sourceUrl,
            confidence: preferredSource.confidence,
            sourceSlug: preferredSource.sourceSlug,
          }
        : verifiedSource
          ? {
              fraternityName: verifiedSource.fraternityName,
              fraternitySlug: verifiedSource.fraternitySlug,
              nationalUrl: verifiedSource.nationalUrl,
              confidence: verifiedSource.confidence,
              sourceSlug: sourceSlug,
            }
          : null;
      if (!source) {
        continue;
      }
      selected.push({
        fraternityName: source.fraternityName,
        fraternitySlug: source.fraternitySlug,
        nationalUrl: source.nationalUrl,
        confidence: source.confidence,
        cohort: "new",
        selectionReason: `frozen_v4_manifest:${sourceSlug}`,
        sourceSlug,
      });
    }
    return selected.slice(0, config.targetCount);
  }

  const dbPool = getDbPool();
  const controlCount = Math.min(config.controlCount, Math.max(0, config.targetCount - 1));
  const newCount = Math.max(1, config.targetCount - controlCount);

  const controlsSql = config.controlFraternitySlugs.length
    ? `
      WITH explicit_controls AS (
        SELECT vs.fraternity_name, vs.fraternity_slug, vs.national_url, vs.confidence
        FROM verified_sources vs
        WHERE vs.is_active = TRUE
          AND vs.fraternity_slug = ANY($1::text[])
      )
      SELECT
        fraternity_name AS "fraternityName",
        fraternity_slug AS "fraternitySlug",
        national_url AS "nationalUrl",
        confidence
      FROM explicit_controls
      ORDER BY confidence DESC, "fraternitySlug" ASC
      LIMIT $2
    `
    : `
      SELECT
        vs.fraternity_name AS "fraternityName",
        vs.fraternity_slug AS "fraternitySlug",
        vs.national_url AS "nationalUrl",
        vs.confidence
      FROM verified_sources vs
      JOIN fraternities f ON f.slug = vs.fraternity_slug
      JOIN chapters c ON c.fraternity_id = f.id
      WHERE vs.is_active = TRUE
      GROUP BY vs.fraternity_name, vs.fraternity_slug, vs.national_url, vs.confidence
      ORDER BY COUNT(c.id) DESC, vs.confidence DESC, vs.fraternity_slug ASC
      LIMIT $1
    `;
  const controlCandidateLimit = Math.max(controlCount, config.targetCount);
  const controlsParams = config.controlFraternitySlugs.length
    ? [config.controlFraternitySlugs, controlCandidateLimit]
    : [controlCandidateLimit];

  const newSql = `
    SELECT
      vs.fraternity_name AS "fraternityName",
      vs.fraternity_slug AS "fraternitySlug",
      vs.national_url AS "nationalUrl",
      vs.confidence
    FROM verified_sources vs
    WHERE vs.is_active = TRUE
      AND NOT (vs.fraternity_slug = ANY($1::text[]))
      AND NOT EXISTS (
        SELECT 1
        FROM fraternities f
        JOIN chapters c ON c.fraternity_id = f.id
        WHERE f.slug = vs.fraternity_slug
      )
    ORDER BY vs.checked_at DESC, vs.confidence DESC, vs.fraternity_slug ASC
    LIMIT $2
  `;

  const controlsResult = await dbPool.query<{
    fraternityName: string;
    fraternitySlug: string;
    nationalUrl: string;
    confidence: number;
  }>(controlsSql, controlsParams);
  const allControlCandidates = controlsResult.rows;
  const controlSlugs = allControlCandidates.map((row) => row.fraternitySlug);
  const newResult = await dbPool.query<{
    fraternityName: string;
    fraternitySlug: string;
    nationalUrl: string;
    confidence: number;
  }>(newSql, [controlSlugs, newCount]);

  const selectedNew: SelectedCampaignFraternity[] = newResult.rows.map((row) => ({
    fraternityName: row.fraternityName,
    fraternitySlug: row.fraternitySlug,
    nationalUrl: row.nationalUrl,
    confidence: Number(row.confidence ?? 0),
    cohort: "new" as const,
    selectionReason: "verified_source_unseen",
    sourceSlug: `${row.fraternitySlug}-main`
  }));

  const primaryControls = allControlCandidates.slice(0, controlCount).map((row) => ({
    fraternityName: row.fraternityName,
    fraternitySlug: row.fraternitySlug,
    nationalUrl: row.nationalUrl,
    confidence: Number(row.confidence ?? 0),
    cohort: "control" as const,
    selectionReason: config.controlFraternitySlugs.length ? "explicit_control" : "existing_baseline_control",
    sourceSlug: `${row.fraternitySlug}-main`
  }));

  const selected: SelectedCampaignFraternity[] = [
    ...selectedNew,
    ...primaryControls
  ];

  const deficit = Math.max(0, config.targetCount - selected.length);
  if (deficit > 0) {
    const backfillControls = allControlCandidates
      .slice(controlCount)
      .filter((row) => !selected.some((item) => item.fraternitySlug === row.fraternitySlug))
      .slice(0, deficit)
      .map((row) => ({
        fraternityName: row.fraternityName,
        fraternitySlug: row.fraternitySlug,
        nationalUrl: row.nationalUrl,
        confidence: Number(row.confidence ?? 0),
        cohort: "control" as const,
        selectionReason: "control_backfill_for_target_count",
        sourceSlug: `${row.fraternitySlug}-main`
      }));

    selected.push(...backfillControls);
  }

  return selected.slice(0, config.targetCount);
}

export async function getFieldJobQueueDepth(): Promise<number> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ count: number }>(
    `SELECT COUNT(*)::int AS count FROM field_jobs WHERE status = 'queued'`
  );
  return Number(rows[0]?.count ?? 0);
}

export interface FieldJobQueueDiagnostics {
  queuedTotal: number;
  oldestQueuedAt: string | null;
  oldestQueuedAgeMinutes: number | null;
  perSource: Array<{ sourceSlug: string; queued: number }>;
}

export async function getFieldJobQueueDiagnostics(sourceSlugs?: string[]): Promise<FieldJobQueueDiagnostics> {
  const dbPool = getDbPool();
  const filterClause =
    sourceSlugs && sourceSlugs.length > 0
      ? "AND s.slug = ANY($1::text[])"
      : "";
  const params = sourceSlugs && sourceSlugs.length > 0 ? [sourceSlugs] : [];

  const queueQuery = `
      SELECT
        COUNT(*)::int AS queued_total,
        MIN(fj.scheduled_at) AS oldest_queued_at
      FROM field_jobs fj
      JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
      JOIN sources s ON s.id = cr.source_id
      WHERE fj.status = 'queued'
      ${filterClause}
    `;
  const perSourceQuery = `
      SELECT
        s.slug AS source_slug,
        COUNT(*)::int AS queued
      FROM field_jobs fj
      JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
      JOIN sources s ON s.id = cr.source_id
      WHERE fj.status = 'queued'
      ${filterClause}
      GROUP BY s.slug
      ORDER BY queued DESC, s.slug ASC
      LIMIT 20
    `;

  const [queueRows, perSourceRows] = await Promise.all([
    dbPool.query<{ queued_total: number; oldest_queued_at: string | null }>(queueQuery, params),
    dbPool.query<{ source_slug: string; queued: number }>(perSourceQuery, params),
  ]);

  const oldestQueuedAt = queueRows.rows[0]?.oldest_queued_at ?? null;
  const oldestQueuedAgeMinutes = oldestQueuedAt
    ? Math.max(0, Math.round((Date.now() - new Date(oldestQueuedAt).getTime()) / 60_000))
    : null;

  return {
    queuedTotal: Number(queueRows.rows[0]?.queued_total ?? 0),
    oldestQueuedAt,
    oldestQueuedAgeMinutes,
    perSource: perSourceRows.rows.map((row) => ({
      sourceSlug: row.source_slug,
      queued: Number(row.queued ?? 0),
    })),
  };
}

export async function getReviewReasonBreakdown(params?: {
  sourceSlugs?: string[];
  windowDays?: number;
  createdAfter?: string | null;
  limit?: number;
}): Promise<Array<{ reason: string; count: number }>> {
  const dbPool = getDbPool();
  const conditions = ["TRUE"];
  const values: Array<string | number | string[]> = [];
  if (params?.sourceSlugs && params.sourceSlugs.length > 0) {
    values.push(params.sourceSlugs);
    conditions.push(`s.slug = ANY($${values.length}::text[])`);
  }
  if (params?.windowDays && params.windowDays > 0) {
    values.push(Math.max(1, Math.floor(params.windowDays)));
    conditions.push(`ri.created_at >= NOW() - ($${values.length}::int * INTERVAL '1 day')`);
  }
  if (params?.createdAfter) {
    values.push(params.createdAfter);
    conditions.push(`ri.created_at >= $${values.length}::timestamptz`);
  }
  values.push(Math.min(Math.max(params?.limit ?? 20, 1), 100));

  const { rows } = await dbPool.query<{ reason: string; count: number }>(
    `
      SELECT
        ri.reason,
        COUNT(*)::int AS count
      FROM review_items ri
      LEFT JOIN sources s ON s.id = ri.source_id
      WHERE ${conditions.join(" AND ")}
      GROUP BY ri.reason
      ORDER BY count DESC, ri.reason ASC
      LIMIT $${values.length}
    `,
    values
  );

  return rows.map((row) => ({
    reason: row.reason,
    count: Number(row.count ?? 0),
  }));
}

export async function listAdaptivePolicySnapshots(params?: {
  policyVersion?: string | null;
  runtimeMode?: string | null;
  limit?: number;
}): Promise<AdaptivePolicySnapshot[]> {
  const dbPool = getDbPool();
  const conditions: string[] = [];
  const values: Array<string | number> = [];
  if (params?.policyVersion) {
    values.push(params.policyVersion);
    conditions.push(`policy_version = $${values.length}`);
  }
  if (params?.runtimeMode) {
    values.push(params.runtimeMode);
    conditions.push(`runtime_mode = $${values.length}`);
  }
  values.push(Math.min(Math.max(params?.limit ?? 20, 1), 100));
  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const { rows } = await dbPool.query<{
    id: number;
    policy_version: string;
    runtime_mode: string;
    feature_schema_version: string;
    metrics: Record<string, unknown>;
    created_at: string;
  }>(
    `
      SELECT
        id,
        policy_version,
        runtime_mode,
        feature_schema_version,
        metrics,
        created_at
      FROM crawl_policy_snapshots
      ${whereClause}
      ORDER BY created_at DESC, id DESC
      LIMIT $${values.length}
    `,
    values
  );

  return rows.map((row) => ({
    id: Number(row.id),
    policyVersion: row.policy_version,
    runtimeMode: row.runtime_mode,
    featureSchemaVersion: row.feature_schema_version,
    metrics: row.metrics ?? {},
    createdAt: row.created_at,
  }));
}

export interface SourceCoverageSnapshot {
  totalChapters: number;
  websitesFound: number;
  emailsFound: number;
  instagramsFound: number;
  chaptersWithAnyContact: number;
  chaptersWithAllThree: number;
}

export async function getSourceCoverageSnapshot(sourceSlug: string): Promise<SourceCoverageSnapshot> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    totalChapters: number;
    websitesFound: number;
    emailsFound: number;
    instagramsFound: number;
    chaptersWithAnyContact: number;
    chaptersWithAllThree: number;
  }>(
    `
      SELECT
        COUNT(*)::int AS "totalChapters",
        COUNT(*) FILTER (WHERE c.website_url IS NOT NULL AND c.website_url <> '')::int AS "websitesFound",
        COUNT(*) FILTER (WHERE c.contact_email IS NOT NULL AND c.contact_email <> '')::int AS "emailsFound",
        COUNT(*) FILTER (WHERE c.instagram_url IS NOT NULL AND c.instagram_url <> '')::int AS "instagramsFound",
        COUNT(*) FILTER (
          WHERE (c.website_url IS NOT NULL AND c.website_url <> '')
             OR (c.contact_email IS NOT NULL AND c.contact_email <> '')
             OR (c.instagram_url IS NOT NULL AND c.instagram_url <> '')
        )::int AS "chaptersWithAnyContact",
        COUNT(*) FILTER (
          WHERE (c.website_url IS NOT NULL AND c.website_url <> '')
            AND (c.contact_email IS NOT NULL AND c.contact_email <> '')
            AND (c.instagram_url IS NOT NULL AND c.instagram_url <> '')
        )::int AS "chaptersWithAllThree"
      FROM sources s
      JOIN fraternities f ON f.id = s.fraternity_id
      JOIN chapters c ON c.fraternity_id = f.id
      WHERE s.slug = $1
    `,
    [sourceSlug]
  );

  return {
    totalChapters: Number(rows[0]?.totalChapters ?? 0),
    websitesFound: Number(rows[0]?.websitesFound ?? 0),
    emailsFound: Number(rows[0]?.emailsFound ?? 0),
    instagramsFound: Number(rows[0]?.instagramsFound ?? 0),
    chaptersWithAnyContact: Number(rows[0]?.chaptersWithAnyContact ?? 0),
    chaptersWithAllThree: Number(rows[0]?.chaptersWithAllThree ?? 0)
  };
}

export async function countCampaignRunItemsByStatus(campaignRunId: string): Promise<Record<string, number>> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ status: string; count: number }>(
    `
      SELECT status, COUNT(*)::int AS count
      FROM campaign_run_items
      WHERE campaign_run_id = $1
      GROUP BY status
    `,
    [campaignRunId]
  );
  const counts: Record<string, number> = {};
  for (const row of rows) {
    counts[row.status] = Number(row.count ?? 0);
  }
  return counts;
}

export async function appendRuntimeNote(campaignRunId: string, note: string): Promise<void> {
  const current = await getCampaignRun(campaignRunId);
  if (!current) return;
  const nextTelemetry = normalizeTelemetry(current.telemetry);
  nextTelemetry.runtimeNotes = [...(nextTelemetry.runtimeNotes ?? []), note].slice(-20);
  await updateCampaignRun({
    id: campaignRunId,
    telemetry: nextTelemetry
  });
}

export function emptyCampaignProviderHealthSnapshot(): CampaignProviderHealthSnapshot {
  return {
    healthy: false,
    successRate: 0,
    probes: 0,
    successes: 0,
    minSuccessRate: 0,
    providerHealth: {}
  };
}

export async function getVerifiedSourceForFraternity(fraternitySlug: string): Promise<SelectedCampaignFraternity | null> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    fraternityName: string;
    fraternitySlug: string;
    nationalUrl: string;
    confidence: number;
  }>(
    `
      SELECT
        fraternity_name AS "fraternityName",
        fraternity_slug AS "fraternitySlug",
        national_url AS "nationalUrl",
        confidence
      FROM verified_sources
      WHERE fraternity_slug = $1
        AND is_active = TRUE
      ORDER BY checked_at DESC NULLS LAST, confidence DESC
      LIMIT 1
    `,
    [fraternitySlug]
  );

  const row = rows[0];
  if (!row) {
    return null;
  }

  return {
    fraternityName: row.fraternityName,
    fraternitySlug: row.fraternitySlug,
    nationalUrl: row.nationalUrl,
    confidence: Number(row.confidence ?? 0),
    cohort: "new",
    selectionReason: "verified_source_lookup",
    sourceSlug: `${row.fraternitySlug}-main`
  };
}

function scoreSourceUrl(url: string): number {
  const evaluation = evaluateSourceUrl(url);
  return evaluation.score + (evaluation.isWeak ? -1.5 : 0.5);
}

export async function getPreferredCampaignSourceForFraternity(fraternitySlug: string): Promise<PreferredCampaignSource | null> {
  const dbPool = getDbPool();
  const verified = await getVerifiedSourceForFraternity(fraternitySlug);
  const { rows } = await dbPool.query<{
    fraternityName: string;
    fraternitySlug: string;
    sourceSlug: string;
    sourceUrl: string;
  }>(
    `
      SELECT
        f.name AS "fraternityName",
        f.slug AS "fraternitySlug",
        s.slug AS "sourceSlug",
        COALESCE(s.list_path, s.base_url) AS "sourceUrl"
      FROM sources s
      JOIN fraternities f ON f.id = s.fraternity_id
      WHERE f.slug = $1
        AND s.active = TRUE
      ORDER BY s.updated_at DESC, s.created_at DESC
      LIMIT 1
    `,
    [fraternitySlug]
  );

  const existing = rows[0];
  if (!verified && !existing) {
    return null;
  }
  if (!verified && existing) {
    return {
      fraternityName: existing.fraternityName,
      fraternitySlug: existing.fraternitySlug,
      sourceSlug: existing.sourceSlug,
      sourceUrl: existing.sourceUrl,
      confidence: 0.7,
      sourceProvenance: "existing_source",
      selectionReason: "existing_source_only"
    };
  }
  if (verified && !existing) {
    return {
      fraternityName: verified.fraternityName,
      fraternitySlug: verified.fraternitySlug,
      sourceSlug: verified.sourceSlug,
      sourceUrl: verified.nationalUrl,
      confidence: verified.confidence,
      sourceProvenance: "verified_registry",
      selectionReason: "verified_source_only"
    };
  }

  const verifiedScore = scoreSourceUrl(verified!.nationalUrl) + verified!.confidence;
  const existingScore = scoreSourceUrl(existing!.sourceUrl) + 0.8;

  if (existingScore > verifiedScore + 0.75) {
    return {
      fraternityName: existing!.fraternityName,
      fraternitySlug: existing!.fraternitySlug,
      sourceSlug: existing!.sourceSlug,
      sourceUrl: existing!.sourceUrl,
      confidence: 0.8,
      sourceProvenance: "existing_source",
      selectionReason: "existing_source_preferred_by_url_quality"
    };
  }

  return {
    fraternityName: verified!.fraternityName,
    fraternitySlug: verified!.fraternitySlug,
    sourceSlug: verified!.sourceSlug,
    sourceUrl: verified!.nationalUrl,
    confidence: verified!.confidence,
    sourceProvenance: "verified_registry",
    selectionReason: "verified_registry_preferred"
  };
}
