import { getDbPool } from "../db";

import type { AdaptiveEpochMetric, AdaptiveInsights } from "../types";

function buildAdaptiveFilters(params: {
  sourceSlugs?: string[];
  runtimeMode?: string | null;
  windowDays?: number | null;
}) {
  const conditions: string[] = [];
  const values: Array<string | number | string[]> = [];

  if (params.sourceSlugs && params.sourceSlugs.length > 0) {
    values.push(params.sourceSlugs);
    conditions.push(`s.slug = ANY($${values.length}::text[])`);
  }

  if (params.runtimeMode) {
    values.push(params.runtimeMode);
    conditions.push(`cs.runtime_mode = $${values.length}`);
  }

  if (params.windowDays && params.windowDays > 0) {
    values.push(Math.max(1, Math.floor(params.windowDays)));
    conditions.push(`cpo.created_at >= NOW() - ($${values.length}::int * INTERVAL '1 day')`);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  return { whereClause, values };
}

export async function listAdaptiveEpochMetrics(params?: {
  limit?: number;
  runtimeMode?: string | null;
  cohortLabel?: string | null;
  policyVersion?: string | null;
}): Promise<AdaptiveEpochMetric[]> {
  const limit = Math.min(Math.max(params?.limit ?? 60, 1), 300);
  const values: Array<string | number> = [];
  const conditions: string[] = [];

  if (params?.runtimeMode) {
    values.push(params.runtimeMode);
    conditions.push(`runtime_mode = $${values.length}`);
  }
  if (params?.cohortLabel) {
    values.push(params.cohortLabel);
    conditions.push(`cohort_label = $${values.length}`);
  }
  if (params?.policyVersion) {
    values.push(params.policyVersion);
    conditions.push(`policy_version = $${values.length}`);
  }

  values.push(limit);
  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    id: number;
    epoch: number;
    policy_version: string;
    runtime_mode: string;
    train_sources: string[];
    eval_sources: string[];
    kpis: Record<string, number>;
    deltas: Record<string, number>;
    slopes: Record<string, number>;
    cohort_label: string;
    metadata: Record<string, unknown>;
    created_at: string;
  }>(
    `
      SELECT
        id,
        epoch,
        policy_version,
        runtime_mode,
        train_sources,
        eval_sources,
        kpis,
        deltas,
        slopes,
        cohort_label,
        metadata,
        created_at
      FROM crawl_epoch_metrics
      ${whereClause}
      ORDER BY created_at DESC, epoch DESC
      LIMIT $${values.length}
    `,
    values
  );

  return rows.map((row) => ({
    id: Number(row.id),
    epoch: Number(row.epoch),
    policyVersion: row.policy_version,
    runtimeMode: row.runtime_mode,
    trainSources: row.train_sources ?? [],
    evalSources: row.eval_sources ?? [],
    kpis: row.kpis ?? {},
    deltas: row.deltas ?? {},
    slopes: row.slopes ?? {},
    cohortLabel: row.cohort_label,
    metadata: row.metadata ?? {},
    createdAt: row.created_at,
  }));
}

export async function getAdaptiveInsights(params?: {
  sourceSlugs?: string[];
  runtimeMode?: string | null;
  windowDays?: number | null;
  limit?: number;
}): Promise<AdaptiveInsights> {
  const limit = Math.min(Math.max(params?.limit ?? 25, 1), 200);
  const dbPool = getDbPool();

  const filters = buildAdaptiveFilters({
    sourceSlugs: params?.sourceSlugs,
    runtimeMode: params?.runtimeMode ?? null,
    windowDays: params?.windowDays ?? null,
  });

  const actionQuery = `
    SELECT
      COALESCE(cpo.selected_action, 'unknown') AS action_type,
      COUNT(*)::int AS event_count,
      COALESCE(AVG(cpo.selected_action_score), 0) AS avg_score,
      COALESCE(AVG(cpo.risk_score), 0) AS avg_risk,
      COALESCE(SUM(CASE WHEN (cpo.outcome->>'recordsExtracted') ~ '^[0-9]+$' THEN (cpo.outcome->>'recordsExtracted')::int ELSE 0 END), 0)::int AS records_extracted
    FROM crawl_page_observations cpo
    JOIN crawl_sessions cs ON cs.id = cpo.crawl_session_id
    JOIN sources s ON s.id = cs.source_id
    ${filters.whereClause}
    GROUP BY COALESCE(cpo.selected_action, 'unknown')
    ORDER BY records_extracted DESC, event_count DESC
    LIMIT $${filters.values.length + 1}
  `;

  const delayedWhere = filters.whereClause
    ? `${filters.whereClause} AND cre.reward_stage = 'delayed'`
    : "WHERE cre.reward_stage = 'delayed'";
  const delayedQuery = `
    SELECT
      cre.action_type,
      COUNT(*)::int AS event_count,
      COALESCE(AVG(cre.reward_value), 0) AS avg_reward,
      COALESCE(SUM(cre.reward_value), 0) AS total_reward
    FROM crawl_reward_events cre
    JOIN crawl_sessions cs ON cs.id = cre.crawl_session_id
    JOIN sources s ON s.id = cs.source_id
    ${delayedWhere}
    GROUP BY cre.action_type
    ORDER BY total_reward DESC, event_count DESC
    LIMIT $${filters.values.length + 1}
  `;

  const guardrailQuery = `
    SELECT
      COUNT(*)::int AS total_pages,
      COALESCE(SUM(CASE WHEN jsonb_array_length(COALESCE(cpo.guardrail_flags, '[]'::jsonb)) > 0 THEN 1 ELSE 0 END), 0)::int AS guardrail_pages,
      COALESCE(SUM(CASE WHEN (cpo.outcome->>'validMissingCount') ~ '^[0-9]+$' THEN (cpo.outcome->>'validMissingCount')::int ELSE 0 END), 0)::int AS valid_missing_count,
      COALESCE(SUM(CASE WHEN (cpo.outcome->>'verifiedWebsiteCount') ~ '^[0-9]+$' THEN (cpo.outcome->>'verifiedWebsiteCount')::int ELSE 0 END), 0)::int AS verified_website_count
    FROM crawl_page_observations cpo
    JOIN crawl_sessions cs ON cs.id = cpo.crawl_session_id
    JOIN sources s ON s.id = cs.source_id
    ${filters.whereClause}
  `;

  const actionPromise = dbPool.query(actionQuery, [...filters.values, limit]);
  const delayedPromise = dbPool.query(delayedQuery, [...filters.values, limit]);
  const guardrailPromise = dbPool.query(guardrailQuery, filters.values);

  const [actionRows, delayedRows, guardrailRows] = await Promise.all([actionPromise, delayedPromise, guardrailPromise]);

  const actionLeaderboard = actionRows.rows.map((row) => ({
    actionType: String(row.action_type),
    count: Number(row.event_count ?? 0),
    avgScore: Number(row.avg_score ?? 0),
    avgRisk: Number(row.avg_risk ?? 0),
    recordsExtracted: Number(row.records_extracted ?? 0),
  }));

  const delayedAttribution = delayedRows.rows.map((row) => ({
    actionType: String(row.action_type),
    count: Number(row.event_count ?? 0),
    avgReward: Number(row.avg_reward ?? 0),
    totalReward: Number(row.total_reward ?? 0),
  }));

  const guardrail = guardrailRows.rows[0] ?? {
    total_pages: 0,
    guardrail_pages: 0,
    valid_missing_count: 0,
    verified_website_count: 0,
  };

  const totalPages = Number(guardrail.total_pages ?? 0);
  const guardrailPages = Number(guardrail.guardrail_pages ?? 0);

  return {
    actionLeaderboard,
    delayedAttribution,
    guardrailHitRate: totalPages > 0 ? guardrailPages / totalPages : 0,
    totalPages,
    guardrailPages,
    validMissingCount: Number(guardrail.valid_missing_count ?? 0),
    verifiedWebsiteCount: Number(guardrail.verified_website_count ?? 0),
  };
}
