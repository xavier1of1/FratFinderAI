import { getDbPool } from "../db";
import type { ChapterListItem } from "../types";

export async function listChapters(params: {
  search?: string;
  limit?: number;
  offset?: number;
}): Promise<ChapterListItem[]> {
  const search = (params.search ?? "").trim();
  const limit = params.limit ?? 50;
  const offset = params.offset ?? 0;

  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ChapterListItem>(
    `
      SELECT
        c.id,
        f.slug AS "fraternitySlug",
        c.slug,
        c.name,
        c.university_name AS "universityName",
        c.city,
        c.state,
        c.country,
        c.website_url AS "websiteUrl",
        c.instagram_url AS "instagramUrl",
        c.contact_email AS "contactEmail",
        c.chapter_status AS "chapterStatus",
        c.field_states AS "fieldStates",
        c.updated_at AS "updatedAt"
      FROM chapters c
      JOIN fraternities f ON f.id = c.fraternity_id
      WHERE ($1 = '' OR c.name ILIKE '%' || $1 || '%' OR c.university_name ILIKE '%' || $1 || '%')
      ORDER BY c.updated_at DESC
      LIMIT $2 OFFSET $3
    `,
    [search, limit, offset]
  );

  return rows;
}
