import { getDbPool } from "../db";
import type { CrawlRunListItem } from "../types";

export async function failStaleCrawlRuns(maxAgeMinutes = 120): Promise<number> {
  const dbPool = getDbPool();
  const { rowCount } = await dbPool.query(
    `
      UPDATE crawl_runs
      SET
        status = 'failed',
        finished_at = NOW(),
        last_error = COALESCE(last_error, 'Crawl run stalled before completion')
      WHERE status = 'running'
        AND finished_at IS NULL
        AND started_at < NOW() - ($1::int * INTERVAL '1 minute')
    `,
    [Math.max(15, maxAgeMinutes)]
  );
  return Number(rowCount ?? 0);
}

export async function listCrawlRuns(limit = 100): Promise<CrawlRunListItem[]> {
  const dbPool = getDbPool();
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
        cr.extraction_metadata ->> 'runtime_mode' AS "runtimeMode",
        cr.extraction_metadata ->> 'stop_reason' AS "stopReason",
        cr.extraction_metadata -> 'chapter_search' AS "chapterSearch",
        COALESCE(cs.session_count, 0) AS "crawlSessionCount",
        NULLIF(cr.extraction_metadata ->> 'page_level_confidence', '')::double precision AS "pageLevelConfidence",
        COALESCE(NULLIF(cr.extraction_metadata ->> 'llm_calls_used', '')::integer, 0) AS "llmCallsUsed"
      FROM crawl_runs cr
      LEFT JOIN sources s ON s.id = cr.source_id
      LEFT JOIN (
        SELECT crawl_run_id, COUNT(*)::int AS session_count
        FROM crawl_sessions
        GROUP BY crawl_run_id
      ) cs ON cs.crawl_run_id = cr.id
      ORDER BY cr.started_at DESC
      LIMIT $1
    `,
    [limit]
  );

  return rows.map((row) => ({
    ...row,
    chapterSearch: row.chapterSearch ?? null,
  }));
}
