import { getDbPool } from "../db";
import type { ReviewItemAuditLog, ReviewItemListItem, ReviewStatus } from "../types";

const allowedTransitions: Record<ReviewStatus, ReviewStatus[]> = {
  open: ["triaged", "ignored"],
  triaged: ["resolved", "ignored"],
  resolved: [],
  ignored: []
};

export async function listReviewItems(limit = 100): Promise<ReviewItemListItem[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ReviewItemListItem>(
    `
      SELECT
        ri.id,
        s.slug AS "sourceSlug",
        c.slug AS "chapterSlug",
        ri.item_type AS "itemType",
        ri.status,
        ri.reason,
        ri.payload ->> 'candidateValue' AS "candidateValue",
        NULLIF(ri.payload ->> 'confidence', '')::double precision AS "confidence",
        ri.payload ->> 'sourceUrl' AS "sourceUrl",
        ri.payload ->> 'query' AS "query",
        ri.payload -> 'rejectionSummary' AS "rejectionSummary",
        ri.payload ->> 'extractionNotes' AS "extractionNotes",
        ri.triage_notes AS "triageNotes",
        ri.created_at AS "createdAt",
        ri.updated_at AS "updatedAt",
        audit.actor AS "lastActor",
        audit.action AS "lastAction",
        audit.created_at AS "lastActionAt"
      FROM review_items ri
      LEFT JOIN sources s ON s.id = ri.source_id
      LEFT JOIN chapters c ON c.id = ri.chapter_id
      LEFT JOIN LATERAL (
        SELECT actor, action, created_at
        FROM review_item_audit_logs
        WHERE review_item_id = ri.id
        ORDER BY created_at DESC
        LIMIT 1
      ) audit ON TRUE
      ORDER BY ri.created_at DESC
      LIMIT $1
    `,
    [limit]
  );

  return rows;
}

export async function getReviewItemStatusCounts(): Promise<Record<ReviewStatus, number>> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ status: ReviewStatus; count: string | number }>(
    `
      SELECT
        status,
        COUNT(*)::int AS count
      FROM review_items
      GROUP BY status
    `
  );

  const counts: Record<ReviewStatus, number> = {
    open: 0,
    triaged: 0,
    resolved: 0,
    ignored: 0
  };

  for (const row of rows) {
    counts[row.status] = Number(row.count ?? 0);
  }

  return counts;
}

export async function listReviewItemAuditLogs(reviewItemId: string): Promise<ReviewItemAuditLog[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ReviewItemAuditLog>(
    `
      SELECT
        id,
        review_item_id AS "reviewItemId",
        actor,
        action,
        from_status AS "fromStatus",
        to_status AS "toStatus",
        notes,
        created_at AS "createdAt"
      FROM review_item_audit_logs
      WHERE review_item_id = $1
      ORDER BY created_at DESC
    `,
    [reviewItemId]
  );

  return rows;
}

export async function updateReviewItemStatusWithAudit(params: {
  id: string;
  status: ReviewStatus;
  actor: string;
  triageNotes?: string;
  resolvedBy?: string;
  notes?: string;
}): Promise<{ id: string; status: ReviewStatus }> {
  const dbPool = getDbPool();
  const client = await dbPool.connect();

  try {
    await client.query("BEGIN");

    const selectResult = await client.query<{ status: ReviewStatus }>(
      `
        SELECT status
        FROM review_items
        WHERE id = $1
        FOR UPDATE
      `,
      [params.id]
    );

    if (selectResult.rowCount === 0) {
      throw new Error(`Review item ${params.id} not found`);
    }

    const currentRow = selectResult.rows[0];
    if (!currentRow) {
      throw new Error(`Review item ${params.id} not found`);
    }

    const currentStatus = currentRow.status;
    if (currentStatus !== params.status && !allowedTransitions[currentStatus].includes(params.status)) {
      throw new Error(`Invalid review status transition from ${currentStatus} to ${params.status}`);
    }

    const shouldResolve = params.status === "resolved";

    await client.query(
      `
        UPDATE review_items
        SET
          status = $2,
          triage_notes = COALESCE($3, triage_notes),
          resolved_by = CASE WHEN $4 THEN COALESCE($5, resolved_by) ELSE resolved_by END,
          resolved_at = CASE WHEN $4 THEN NOW() ELSE resolved_at END
        WHERE id = $1
      `,
      [params.id, params.status, params.triageNotes ?? null, shouldResolve, params.resolvedBy ?? null]
    );

    const action = currentStatus === params.status ? "note_updated" : "status_transition";
    await client.query(
      `
        INSERT INTO review_item_audit_logs (
          review_item_id,
          actor,
          action,
          from_status,
          to_status,
          notes,
          metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `,
      [
        params.id,
        params.actor,
        action,
        currentStatus,
        params.status,
        params.notes ?? params.triageNotes ?? null,
        {}
      ]
    );

    await client.query("COMMIT");
    return { id: params.id, status: params.status };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
