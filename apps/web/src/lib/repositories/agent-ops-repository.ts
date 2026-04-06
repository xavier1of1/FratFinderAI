import { getDbPool } from "../db";
import { normalizeInstagramUrl } from "../social";
import type { AgentOpsSummary, ChapterEvidence, ChapterSearchRun, ProvisionalChapter, RequestGraphRun } from "../types";

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
    metadata: row.metadata ?? {}
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
    field_jobs_running: string | number;
    field_jobs_deferred: string | number;
    field_jobs_terminal_no_signal: string | number;
    field_jobs_review_required: string | number;
    field_jobs_updated: string | number;
    provisional_open: string | number;
    provisional_promoted: string | number;
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
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'running') AS field_jobs_running,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued' AND COALESCE(payload -> 'contactResolution' ->> 'queueState', 'actionable') = 'deferred') AS field_jobs_deferred,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(completed_payload ->> 'status', '') = 'terminal_no_signal') AS field_jobs_terminal_no_signal,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(completed_payload ->> 'status', '') = 'review_required') AS field_jobs_review_required,
        (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(completed_payload ->> 'status', '') = 'updated') AS field_jobs_updated,
        (SELECT COUNT(*) FROM provisional_chapters WHERE status = 'provisional') AS provisional_open,
        (SELECT COUNT(*) FROM provisional_chapters WHERE status = 'promoted') AS provisional_promoted,
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
    fieldJobsRunning: Number(row?.field_jobs_running ?? 0),
    fieldJobsDeferred: Number(row?.field_jobs_deferred ?? 0),
    fieldJobsTerminalNoSignal: Number(row?.field_jobs_terminal_no_signal ?? 0),
    fieldJobsReviewRequired: Number(row?.field_jobs_review_required ?? 0),
    fieldJobsUpdated: Number(row?.field_jobs_updated ?? 0),
    provisionalOpen: Number(row?.provisional_open ?? 0),
    provisionalPromoted: Number(row?.provisional_promoted ?? 0),
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
    chapterValidityBlockedRepairable: Number(row?.chapter_validity_blocked_repairable ?? 0)
  };
}
