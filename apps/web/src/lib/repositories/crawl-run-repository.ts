import { getDbPool } from "../db";
import type { CrawlRunListItem } from "../types";

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
        NULLIF(cr.extraction_metadata ->> 'page_level_confidence', '')::double precision AS "pageLevelConfidence",
        COALESCE(NULLIF(cr.extraction_metadata ->> 'llm_calls_used', '')::integer, 0) AS "llmCallsUsed"
      FROM crawl_runs cr
      LEFT JOIN sources s ON s.id = cr.source_id
      ORDER BY cr.started_at DESC
      LIMIT $1
    `,
    [limit]
  );

  return rows;
}
