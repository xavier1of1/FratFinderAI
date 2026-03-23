import { getDbPool } from "../db";
import type { FieldJobListItem } from "../types";

export async function listFieldJobs(limit = 100): Promise<FieldJobListItem[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<FieldJobListItem>(
    `
      SELECT
        fj.id,
        c.slug AS "chapterSlug",
        fj.field_name AS "fieldName",
        fj.status,
        fj.terminal_failure AS "terminalFailure",
        fj.claimed_by AS "claimedBy",
        fj.attempts,
        fj.max_attempts AS "maxAttempts",
        fj.scheduled_at AS "scheduledAt",
        fj.started_at AS "startedAt",
        fj.finished_at AS "finishedAt",
        fj.last_error AS "lastError"
      FROM field_jobs fj
      JOIN chapters c ON c.id = fj.chapter_id
      ORDER BY fj.scheduled_at ASC
      LIMIT $1
    `,
    [limit]
  );

  return rows;
}
