import { getDbPool } from "../db";
import { normalizeInstagramUrl } from "../social";
import type { AccuracyRecoveryMetrics, AgentOpsSummary, ChapterEvidence, ChapterSearchRun, DecisionEvidence, OpsAlert, ProvisionalChapter, RequestGraphRun } from "../types";
import { getOpsAlertSummary, listOpsAlerts } from "./ops-alert-repository";

export async function getAccuracyRecoveryMetrics(): Promise<AccuracyRecoveryMetrics> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    total_chapters: string | number;
    complete_rows: string | number;
    chapter_specific_contact_rows: string | number;
    nationals_only_contact_rows: string | number;
    inactive_validated_rows: string | number;
    confirmed_absent_website_rows: string | number;
    active_rows_with_chapter_specific_email: string | number;
    active_rows_with_chapter_specific_instagram: string | number;
    active_rows_with_any_contact: string | number;
  }>(
    `
      WITH latest_evidence AS (
        SELECT DISTINCT ON (ce.chapter_id, ce.field_name)
          ce.chapter_id::text AS chapter_id,
          ce.field_name,
          ce.metadata,
          ce.created_at
        FROM chapter_evidence ce
        WHERE ce.field_name IN ('contact_email', 'instagram_url', 'website_url', 'chapter_status')
        ORDER BY ce.chapter_id, ce.field_name, ce.created_at DESC
      ),
      enriched AS (
        SELECT
          c.id::text AS chapter_id,
          c.chapter_status,
          c.field_states,
          c.contact_provenance,
          c.website_url,
          c.contact_email,
          c.instagram_url,
          COALESCE(
            c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType',
            le_email.metadata ->> 'contactSpecificity'
          ) AS email_specificity,
          COALESCE(
            c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType',
            le_instagram.metadata ->> 'contactSpecificity'
          ) AS instagram_specificity,
          COALESCE(
            c.contact_provenance -> 'chapter_status' ->> 'sourceType',
            le_status.metadata ->> 'evidenceSourceType'
          ) AS chapter_status_source_type
        FROM chapters c
        LEFT JOIN latest_evidence le_email
          ON le_email.chapter_id = c.id::text
         AND le_email.field_name = 'contact_email'
        LEFT JOIN latest_evidence le_instagram
          ON le_instagram.chapter_id = c.id::text
         AND le_instagram.field_name = 'instagram_url'
        LEFT JOIN latest_evidence le_status
          ON le_status.chapter_id = c.id::text
         AND le_status.field_name = 'chapter_status'
      )
      SELECT
        COUNT(*)::int AS total_chapters,
        COUNT(*) FILTER (
          WHERE chapter_status = 'active'
            AND (
              (contact_email IS NOT NULL AND email_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
              OR
              (instagram_url IS NOT NULL AND instagram_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
            )
        )::int AS complete_rows,
        COUNT(*) FILTER (
          WHERE chapter_status = 'active'
            AND (
              (contact_email IS NOT NULL AND email_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
              OR
              (instagram_url IS NOT NULL AND instagram_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
            )
        )::int AS chapter_specific_contact_rows,
        COUNT(*) FILTER (
          WHERE (contact_email IS NOT NULL OR instagram_url IS NOT NULL)
            AND (contact_email IS NULL OR email_specificity = 'national_generic')
            AND (instagram_url IS NULL OR instagram_specificity = 'national_generic')
        )::int AS nationals_only_contact_rows,
        COUNT(*) FILTER (
          WHERE chapter_status = 'inactive'
            AND chapter_status_source_type IN ('official_school', 'school_activity_validation', 'school_policy_validation')
        )::int AS inactive_validated_rows,
        COUNT(*) FILTER (
          WHERE COALESCE(field_states ->> 'website_url', '') = 'confirmed_absent'
        )::int AS confirmed_absent_website_rows,
        COUNT(*) FILTER (
          WHERE chapter_status = 'active'
            AND contact_email IS NOT NULL
            AND email_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter')
        )::int AS active_rows_with_chapter_specific_email,
        COUNT(*) FILTER (
          WHERE chapter_status = 'active'
            AND instagram_url IS NOT NULL
            AND instagram_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter')
        )::int AS active_rows_with_chapter_specific_instagram,
        COUNT(*) FILTER (
          WHERE chapter_status = 'active'
            AND (website_url IS NOT NULL OR contact_email IS NOT NULL OR instagram_url IS NOT NULL)
        )::int AS active_rows_with_any_contact
      FROM enriched
    `
  );
  const row = rows[0];
  return {
    totalChapters: Number(row?.total_chapters ?? 0),
    completeRows: Number(row?.complete_rows ?? 0),
    chapterSpecificContactRows: Number(row?.chapter_specific_contact_rows ?? 0),
    nationalsOnlyContactRows: Number(row?.nationals_only_contact_rows ?? 0),
    inactiveValidatedRows: Number(row?.inactive_validated_rows ?? 0),
    confirmedAbsentWebsiteRows: Number(row?.confirmed_absent_website_rows ?? 0),
    activeRowsWithChapterSpecificEmail: Number(row?.active_rows_with_chapter_specific_email ?? 0),
    activeRowsWithChapterSpecificInstagram: Number(row?.active_rows_with_chapter_specific_instagram ?? 0),
    activeRowsWithAnyContact: Number(row?.active_rows_with_any_contact ?? 0)
  };
}

export async function listRequestGraphRuns(limit = 100): Promise<RequestGraphRun[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<RequestGraphRun>(
    `
      SELECT
        rgr.id,
        rgr.request_id AS "requestId",
        rgr.worker_id AS "workerId",
        rgr.runtime_mode AS "runtimeMode",
        rgr.status,
        rgr.active_node AS "activeNode",
        rgr.summary,
        rgr.metadata,
        rgr.error_message AS "errorMessage",
        rgr.created_at AS "createdAt",
        rgr.updated_at AS "updatedAt",
        rgr.finished_at AS "finishedAt",
        fcr.fraternity_name AS "fraternityName",
        fcr.fraternity_slug AS "fraternitySlug",
        fcr.source_slug AS "sourceSlug",
        fcr.stage AS "requestStage",
        fcr.status AS "requestStatus"
      FROM request_graph_runs rgr
      LEFT JOIN fraternity_crawl_requests fcr ON fcr.id = rgr.request_id
      ORDER BY rgr.created_at DESC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );
  return rows.map((row) => ({
    ...row,
    summary: row.summary ?? {},
    metadata: row.metadata ?? {}
  }));
}

export async function listProvisionalChapters(limit = 100): Promise<ProvisionalChapter[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ProvisionalChapter>(
    `
      SELECT
        pc.id::text AS id,
        pc.fraternity_id AS "fraternityId",
        f.slug AS "fraternitySlug",
        s.slug AS "sourceSlug",
        pc.request_id AS "requestId",
        pc.promoted_chapter_id AS "promotedChapterId",
        pc.slug,
        pc.name,
        pc.university_name AS "universityName",
        pc.city,
        pc.state,
        pc.country,
        pc.website_url AS "websiteUrl",
        pc.instagram_url AS "instagramUrl",
        pc.contact_email AS "contactEmail",
        pc.status,
        pc.promotion_reason AS "promotionReason",
        pc.evidence_payload AS "evidencePayload",
        pc.created_at AS "createdAt",
        pc.updated_at AS "updatedAt"
      FROM provisional_chapters pc
      LEFT JOIN fraternities f ON f.id = pc.fraternity_id
      LEFT JOIN sources s ON s.id = pc.source_id
      ORDER BY pc.created_at DESC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );
  return rows.map((row) => ({
    ...row,
    instagramUrl: normalizeInstagramUrl(row.instagramUrl),
    evidencePayload: row.evidencePayload ?? {}
  }));
}

export async function listChapterEvidence(limit = 150): Promise<ChapterEvidence[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ChapterEvidence>(
    `
      SELECT
        ce.id::text AS id,
        ce.chapter_id AS "chapterId",
        ce.chapter_slug AS "chapterSlug",
        ce.fraternity_slug AS "fraternitySlug",
        ce.source_slug AS "sourceSlug",
        ce.request_id AS "requestId",
        ce.crawl_run_id AS "crawlRunId",
        ce.field_name AS "fieldName",
        ce.candidate_value AS "candidateValue",
        ce.confidence,
        ce.trust_tier AS "trustTier",
        ce.evidence_status AS "evidenceStatus",
        ce.source_url AS "sourceUrl",
        ce.source_snippet AS "sourceSnippet",
        ce.provider,
        ce.query,
        ce.related_website_url AS "relatedWebsiteUrl",
        ce.metadata,
        ce.created_at AS "createdAt"
      FROM chapter_evidence ce
      ORDER BY ce.created_at DESC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );
  return rows.map((row) => ({
    ...row,
    confidence: row.confidence === null ? null : Number(row.confidence),
    metadata: row.metadata ?? {},
    decisionEvidence: row.metadata
      ? {
          decisionStage: typeof row.metadata.decisionStage === "string" ? row.metadata.decisionStage : "",
          evidenceUrl: typeof row.metadata.supportingPageUrl === "string" ? row.metadata.supportingPageUrl : null,
          sourceType: typeof row.metadata.evidenceSourceType === "string" ? row.metadata.evidenceSourceType : null,
          pageScope: typeof row.metadata.pageScope === "string" ? row.metadata.pageScope as DecisionEvidence["pageScope"] : null,
          contactSpecificity: typeof row.metadata.contactSpecificity === "string" ? row.metadata.contactSpecificity as DecisionEvidence["contactSpecificity"] : null,
          confidence: typeof row.metadata.supportingConfidence === "number" ? row.metadata.supportingConfidence : null,
          reasonCode: typeof row.metadata.reasonCode === "string" ? row.metadata.reasonCode : null,
          metadata: row.metadata
        }
      : null
  }));
}

export async function listChapterSearchRuns(limit = 50): Promise<ChapterSearchRun[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ChapterSearchRun & { rejectionReasonCounts: Record<string, number> | null }>(
    `
      SELECT
        cr.id,
        s.slug AS "sourceSlug",
        cr.status,
        cr.started_at AS "startedAt",
        cr.finished_at AS "finishedAt",
        cr.extraction_metadata ->> 'runtime_mode' AS "runtimeMode",
        cr.extraction_metadata ->> 'strategy_used' AS "strategyUsed",
        cr.extraction_metadata ->> 'stop_reason' AS "stopReason",
        cr.pages_processed AS "pagesProcessed",
        cr.records_seen AS "recordsSeen",
        cr.records_upserted AS "recordsUpserted",
        cr.review_items_created AS "reviewItemsCreated",
        cr.field_jobs_created AS "fieldJobsCreated",
        cr.extraction_metadata -> 'chapter_search' ->> 'sourceClass' AS "sourceClass",
        cr.extraction_metadata -> 'chapter_search' ->> 'coverageState' AS "coverageState",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'candidatesExtracted', '')::integer, 0) AS "candidatesExtracted",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'candidatesRejected', '')::integer, 0) AS "candidatesRejected",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'canonicalChaptersCreated', '')::integer, 0) AS "canonicalChaptersCreated",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'provisionalChaptersCreated', '')::integer, 0) AS "provisionalChaptersCreated",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'nationalTargetsFollowed', '')::integer, 0) AS "nationalTargetsFollowed",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'institutionalTargetsFollowed', '')::integer, 0) AS "institutionalTargetsFollowed",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'chapterOwnedTargetsSkipped', '')::integer, 0) AS "chapterOwnedTargetsSkipped",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'broaderWebTargetsFollowed', '')::integer, 0) AS "broaderWebTargetsFollowed",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'chapterSearchWallTimeMs', '')::integer, 0) AS "chapterSearchWallTimeMs",
        cr.extraction_metadata -> 'chapter_search' -> 'rejectionReasonCounts' AS "rejectionReasonCounts",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_validity' ->> 'invalidCount', '')::integer, 0) AS "invalidCount",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_validity' ->> 'repairableCount', '')::integer, 0) AS "repairableCount",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_validity' ->> 'canonicalValidCount', '')::integer, 0) AS "canonicalValidCount",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_validity' ->> 'provisionalCount', '')::integer, 0) AS "provisionalCount",
        COALESCE(NULLIF(cr.extraction_metadata -> 'chapter_validity' ->> 'sourceInvaliditySaturated', '')::boolean, false) AS "sourceInvaliditySaturated",
        cr.extraction_metadata -> 'chapter_validity' -> 'invalidReasonCounts' AS "invalidReasonCounts"
      FROM crawl_runs cr
      LEFT JOIN sources s ON s.id = cr.source_id
      WHERE cr.extraction_metadata ? 'chapter_search'
      ORDER BY cr.started_at DESC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );

  return rows.map((row) => ({
    ...row,
    rejectionReasonCounts: row.rejectionReasonCounts ?? {},
    invalidReasonCounts: row.invalidReasonCounts ?? {},
  }));
}

export async function listOpsAlertsForAgentOps(limit = 50): Promise<OpsAlert[]> {
  return listOpsAlerts(limit);
}

export async function getAgentOpsSummary(): Promise<AgentOpsSummary> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    request_queue_queued: string | number;
    request_queue_running: string | number;
    request_awaiting_confirmation: string | number;
    request_completed: string | number;
    graph_runs_total: string | number;
    graph_runs_running: string | number;
    graph_runs_paused: string | number;
    graph_runs_failed: string | number;
    graph_runs_succeeded: string | number;
    field_jobs_queued: string | number;
    field_jobs_actionable: string | number;
    field_jobs_running: string | number;
    field_jobs_deferred: string | number;
    field_jobs_blocked_invalid: string | number;
    field_jobs_blocked_repairable: string | number;
    chapter_repair_queued: string | number;
    chapter_repair_running: string | number;
    chapter_repair_completed: string | number;
    chapter_repair_historical_reconciled: string | number;
    field_jobs_terminal_no_signal: string | number;
    field_jobs_review_required: string | number;
    field_jobs_updated: string | number;
    provisional_open: string | number;
    provisional_promoted: string | number;
    provisional_review: string | number;
    provisional_rejected: string | number;
    provisional_oldest_open_hours: string | number | null;
    evidence_total: string | number;
    evidence_review: string | number;
    evidence_write: string | number;
    chapter_search_runs: string | number;
    chapter_search_canonical: string | number;
    chapter_search_provisional: string | number;
    chapter_search_chapter_owned_skipped: string | number;
    chapter_validity_invalid: string | number;
    chapter_validity_repairable: string | number;
    chapter_validity_blocked_invalid: string | number;
    chapter_validity_blocked_repairable: string | number;
  }>(
    `
      SELECT
        (SELECT COUNT(*) FROM fraternity_crawl_requests WHERE status = 'queued') AS request_queue_queued,
        (SELECT COUNT(*) FROM fraternity_crawl_requests WHERE status = 'running') AS request_queue_running,
        (SELECT COUNT(*) FROM fraternity_crawl_requests WHERE stage = 'awaiting_confirmation') AS request_awaiting_confirmation,
        (SELECT COUNT(*) FROM fraternity_crawl_requests WHERE status = 'succeeded' AND stage = 'completed') AS request_completed,
        (SELECT COUNT(*) FROM request_graph_runs) AS graph_runs_total,
        (SELECT COUNT(*) FROM request_graph_runs WHERE status = 'running') AS graph_runs_running,
        (SELECT COUNT(*) FROM request_graph_runs WHERE status = 'paused') AS graph_runs_paused,
        (SELECT COUNT(*) FROM request_graph_runs WHERE status = 'failed') AS graph_runs_failed,
        (SELECT COUNT(*) FROM request_graph_runs WHERE status = 'succeeded') AS graph_runs_succeeded,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued') AS field_jobs_queued,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable') AS field_jobs_actionable,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'running') AS field_jobs_running,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred') AS field_jobs_deferred,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_invalid') AS field_jobs_blocked_invalid,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_repairable') AS field_jobs_blocked_repairable,
        (SELECT COUNT(*) FROM chapter_repair_jobs WHERE status = 'queued') AS chapter_repair_queued,
        (SELECT COUNT(*) FROM chapter_repair_jobs WHERE status = 'running') AS chapter_repair_running,
        (SELECT COUNT(*) FROM chapter_repair_jobs WHERE status = 'done') AS chapter_repair_completed,
        (
          SELECT COUNT(*)
          FROM chapter_repair_jobs
          WHERE CASE
            WHEN payload ? 'origin' THEN payload ->> 'origin'
            ELSE 'historical_queue_reconciliation'
          END = 'historical_queue_reconciliation'
        ) AS chapter_repair_historical_reconciled,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'terminal_no_signal') AS field_jobs_terminal_no_signal,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'review_required') AS field_jobs_review_required,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'updated') AS field_jobs_updated,
        (SELECT COUNT(*) FROM provisional_chapters WHERE status = 'provisional') AS provisional_open,
        (SELECT COUNT(*) FROM provisional_chapters WHERE status = 'promoted') AS provisional_promoted,
        (SELECT COUNT(*) FROM provisional_chapters WHERE status = 'review') AS provisional_review,
        (SELECT COUNT(*) FROM provisional_chapters WHERE status = 'rejected') AS provisional_rejected,
        (SELECT COALESCE(FLOOR(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) / 3600), 0)::int FROM provisional_chapters WHERE status = 'provisional') AS provisional_oldest_open_hours,
        (SELECT COUNT(*) FROM chapter_evidence) AS evidence_total,
        (SELECT COUNT(*) FROM chapter_evidence WHERE evidence_status = 'review') AS evidence_review,
        (SELECT COUNT(*) FROM chapter_evidence WHERE evidence_status = 'write') AS evidence_write,
        (SELECT COUNT(*) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_search') AS chapter_search_runs,
        (SELECT COALESCE(SUM(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'canonicalChaptersCreated', '')::integer), 0) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_search') AS chapter_search_canonical,
        (SELECT COALESCE(SUM(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'provisionalChaptersCreated', '')::integer), 0) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_search') AS chapter_search_provisional,
        (SELECT COALESCE(SUM(NULLIF(cr.extraction_metadata -> 'chapter_search' ->> 'chapterOwnedTargetsSkipped', '')::integer), 0) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_search') AS chapter_search_chapter_owned_skipped,
        (SELECT COALESCE(SUM(NULLIF(cr.extraction_metadata -> 'chapter_validity' ->> 'invalidCount', '')::integer), 0) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_validity') AS chapter_validity_invalid,
        (SELECT COALESCE(SUM(NULLIF(cr.extraction_metadata -> 'chapter_validity' ->> 'repairableCount', '')::integer), 0) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_validity') AS chapter_validity_repairable,
        (SELECT COALESCE(SUM(NULLIF(cr.extraction_metadata -> 'chapter_validity' -> 'contactAdmission' ->> 'blocked_invalid', '')::integer), 0) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_validity') AS chapter_validity_blocked_invalid,
        (SELECT COALESCE(SUM(NULLIF(cr.extraction_metadata -> 'chapter_validity' -> 'contactAdmission' ->> 'blocked_repairable', '')::integer), 0) FROM crawl_runs cr WHERE cr.extraction_metadata ? 'chapter_validity') AS chapter_validity_blocked_repairable
    `
  );

  const row = rows[0];
  const [opsAlerts, accuracyRecovery] = await Promise.all([getOpsAlertSummary(), getAccuracyRecoveryMetrics()]);
  return {
    requestQueueQueued: Number(row?.request_queue_queued ?? 0),
    requestQueueRunning: Number(row?.request_queue_running ?? 0),
    requestAwaitingConfirmation: Number(row?.request_awaiting_confirmation ?? 0),
    requestCompleted: Number(row?.request_completed ?? 0),
    graphRunsTotal: Number(row?.graph_runs_total ?? 0),
    graphRunsRunning: Number(row?.graph_runs_running ?? 0),
    graphRunsPaused: Number(row?.graph_runs_paused ?? 0),
    graphRunsFailed: Number(row?.graph_runs_failed ?? 0),
    graphRunsSucceeded: Number(row?.graph_runs_succeeded ?? 0),
    fieldJobsQueued: Number(row?.field_jobs_queued ?? 0),
    fieldJobsActionable: Number(row?.field_jobs_actionable ?? 0),
    fieldJobsRunning: Number(row?.field_jobs_running ?? 0),
    fieldJobsDeferred: Number(row?.field_jobs_deferred ?? 0),
    fieldJobsBlockedInvalid: Number(row?.field_jobs_blocked_invalid ?? 0),
    fieldJobsBlockedRepairable: Number(row?.field_jobs_blocked_repairable ?? 0),
    chapterRepairQueued: Number(row?.chapter_repair_queued ?? 0),
    chapterRepairRunning: Number(row?.chapter_repair_running ?? 0),
    chapterRepairCompleted: Number(row?.chapter_repair_completed ?? 0),
    chapterRepairHistoricalReconciled: Number(row?.chapter_repair_historical_reconciled ?? 0),
    fieldJobsTerminalNoSignal: Number(row?.field_jobs_terminal_no_signal ?? 0),
    fieldJobsReviewRequired: Number(row?.field_jobs_review_required ?? 0),
    fieldJobsUpdated: Number(row?.field_jobs_updated ?? 0),
    provisionalOpen: Number(row?.provisional_open ?? 0),
    provisionalPromoted: Number(row?.provisional_promoted ?? 0),
    provisionalReview: Number(row?.provisional_review ?? 0),
    provisionalRejected: Number(row?.provisional_rejected ?? 0),
    evidenceTotal: Number(row?.evidence_total ?? 0),
    evidenceReview: Number(row?.evidence_review ?? 0),
    evidenceWrite: Number(row?.evidence_write ?? 0),
    chapterSearchRuns: Number(row?.chapter_search_runs ?? 0),
    chapterSearchCanonical: Number(row?.chapter_search_canonical ?? 0),
    chapterSearchProvisional: Number(row?.chapter_search_provisional ?? 0),
    chapterSearchChapterOwnedSkipped: Number(row?.chapter_search_chapter_owned_skipped ?? 0),
    chapterValidityInvalid: Number(row?.chapter_validity_invalid ?? 0),
    chapterValidityRepairable: Number(row?.chapter_validity_repairable ?? 0),
    chapterValidityBlockedInvalid: Number(row?.chapter_validity_blocked_invalid ?? 0),
    chapterValidityBlockedRepairable: Number(row?.chapter_validity_blocked_repairable ?? 0),
    accuracyRecovery,
    opsAlertsOpen: opsAlerts.openTotal,
    opsAlertsCritical: opsAlerts.openCritical,
    opsAlertsWarning: opsAlerts.openWarning,
    opsAlertsResolvedLast24h: opsAlerts.resolvedLast24h,
    opsAlertsOldestOpenMinutes: opsAlerts.oldestOpenMinutes,
    provisionalOldestOpenHours: Number(row?.provisional_oldest_open_hours ?? 0),
  };
}
