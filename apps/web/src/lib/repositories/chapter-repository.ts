import { getDbPool } from "../db";
import type { ChapterActionResult, ChapterFieldName, ChapterListItem, ChapterMapStateSummary } from "../types";

const STATE_NORMALIZATION_CASE = `
  CASE
    WHEN c.state IS NULL OR btrim(c.state) = '' THEN NULL
    WHEN upper(btrim(c.state)) IN (
      'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME',
      'MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
      'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
    ) THEN upper(btrim(c.state))
    ELSE CASE regexp_replace(lower(btrim(c.state)), '[^a-z ]', '', 'g')
      WHEN 'alabama' THEN 'AL'
      WHEN 'alaska' THEN 'AK'
      WHEN 'arizona' THEN 'AZ'
      WHEN 'arkansas' THEN 'AR'
      WHEN 'california' THEN 'CA'
      WHEN 'colorado' THEN 'CO'
      WHEN 'connecticut' THEN 'CT'
      WHEN 'delaware' THEN 'DE'
      WHEN 'district of columbia' THEN 'DC'
      WHEN 'florida' THEN 'FL'
      WHEN 'georgia' THEN 'GA'
      WHEN 'hawaii' THEN 'HI'
      WHEN 'idaho' THEN 'ID'
      WHEN 'illinois' THEN 'IL'
      WHEN 'indiana' THEN 'IN'
      WHEN 'iowa' THEN 'IA'
      WHEN 'kansas' THEN 'KS'
      WHEN 'kentucky' THEN 'KY'
      WHEN 'louisiana' THEN 'LA'
      WHEN 'maine' THEN 'ME'
      WHEN 'maryland' THEN 'MD'
      WHEN 'massachusetts' THEN 'MA'
      WHEN 'michigan' THEN 'MI'
      WHEN 'minnesota' THEN 'MN'
      WHEN 'mississippi' THEN 'MS'
      WHEN 'missouri' THEN 'MO'
      WHEN 'montana' THEN 'MT'
      WHEN 'nebraska' THEN 'NE'
      WHEN 'nevada' THEN 'NV'
      WHEN 'new hampshire' THEN 'NH'
      WHEN 'new jersey' THEN 'NJ'
      WHEN 'new mexico' THEN 'NM'
      WHEN 'new york' THEN 'NY'
      WHEN 'north carolina' THEN 'NC'
      WHEN 'north dakota' THEN 'ND'
      WHEN 'ohio' THEN 'OH'
      WHEN 'oklahoma' THEN 'OK'
      WHEN 'oregon' THEN 'OR'
      WHEN 'pennsylvania' THEN 'PA'
      WHEN 'rhode island' THEN 'RI'
      WHEN 'south carolina' THEN 'SC'
      WHEN 'south dakota' THEN 'SD'
      WHEN 'tennessee' THEN 'TN'
      WHEN 'texas' THEN 'TX'
      WHEN 'utah' THEN 'UT'
      WHEN 'vermont' THEN 'VT'
      WHEN 'virginia' THEN 'VA'
      WHEN 'washington' THEN 'WA'
      WHEN 'west virginia' THEN 'WV'
      WHEN 'wisconsin' THEN 'WI'
      WHEN 'wyoming' THEN 'WY'
      ELSE NULL
    END
  END
`;

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
        latest_source.slug AS "sourceSlug",
        c.slug,
        c.name,
        c.university_name AS "universityName",
        c.city,
        c.state,
        c.country,
        CASE WHEN c.website_url ~* '^https?://' THEN c.website_url ELSE NULL END AS "websiteUrl",
        c.instagram_url AS "instagramUrl",
        c.contact_email AS "contactEmail",
        c.chapter_status AS "chapterStatus",
        c.field_states AS "fieldStates",
        c.updated_at AS "updatedAt"
      FROM chapters c
      JOIN fraternities f ON f.id = c.fraternity_id
      LEFT JOIN LATERAL (
        SELECT s.slug
        FROM chapter_provenance cp
        JOIN sources s ON s.id = cp.source_id
        WHERE cp.chapter_id = c.id
        ORDER BY cp.extracted_at DESC, cp.created_at DESC
        LIMIT 1
      ) latest_source ON TRUE
      WHERE ($1 = '' OR c.name ILIKE '%' || $1 || '%' OR c.university_name ILIKE '%' || $1 || '%')
      ORDER BY c.updated_at DESC
      LIMIT $2 OFFSET $3
    `,
    [search, limit, offset]
  );

  return rows;
}

export async function listChapterMapSummary(): Promise<ChapterMapStateSummary[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ChapterMapStateSummary>(
    `
      WITH normalized AS (
        SELECT ${STATE_NORMALIZATION_CASE} AS state_code
        FROM chapters c
      )
      SELECT
        state_code AS "stateCode",
        COUNT(*)::int AS "chapterCount"
      FROM normalized
      WHERE state_code IS NOT NULL
      GROUP BY state_code
      ORDER BY state_code ASC
    `
  );

  return rows;
}

export async function updateChapterRecord(params: {
  id: string;
  name: string;
  universityName: string | null;
  city: string | null;
  state: string | null;
  chapterStatus: string;
  websiteUrl: string | null;
  instagramUrl: string | null;
  contactEmail: string | null;
}): Promise<ChapterListItem | null> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ChapterListItem>(
    `
      WITH updated AS (
        UPDATE chapters c
        SET
          name = $2,
          university_name = $3,
          city = $4,
          state = $5,
          chapter_status = $6,
          website_url = $7,
          instagram_url = $8,
          contact_email = $9,
          field_states = jsonb_strip_nulls(
            COALESCE(c.field_states, '{}'::jsonb)
            || jsonb_build_object(
              'website_url', CASE WHEN $7::text IS NULL OR $7::text = '' THEN 'missing' ELSE 'found' END,
              'instagram_url', CASE WHEN $8::text IS NULL OR $8::text = '' THEN 'missing' ELSE 'found' END,
              'contact_email', CASE WHEN $9::text IS NULL OR $9::text = '' THEN 'missing' ELSE 'found' END
            )
          ),
          updated_at = NOW()
        WHERE c.id = $1
        RETURNING c.*
      )
      SELECT
        u.id,
        f.slug AS "fraternitySlug",
        latest_source.slug AS "sourceSlug",
        u.slug,
        u.name,
        u.university_name AS "universityName",
        u.city,
        u.state,
        u.country,
        CASE WHEN u.website_url ~* '^https?://' THEN u.website_url ELSE NULL END AS "websiteUrl",
        u.instagram_url AS "instagramUrl",
        u.contact_email AS "contactEmail",
        u.chapter_status AS "chapterStatus",
        u.field_states AS "fieldStates",
        u.updated_at AS "updatedAt"
      FROM updated u
      JOIN fraternities f ON f.id = u.fraternity_id
      LEFT JOIN LATERAL (
        SELECT s.slug
        FROM chapter_provenance cp
        JOIN sources s ON s.id = cp.source_id
        WHERE cp.chapter_id = u.id
        ORDER BY cp.extracted_at DESC, cp.created_at DESC
        LIMIT 1
      ) latest_source ON TRUE
    `,
    [
      params.id,
      params.name,
      params.universityName,
      params.city,
      params.state,
      params.chapterStatus,
      params.websiteUrl,
      params.instagramUrl,
      params.contactEmail
    ]
  );

  return rows[0] ?? null;
}

export async function deleteChapterRecords(ids: string[]): Promise<ChapterActionResult> {
  const dbPool = getDbPool();
  const uniqueIds = Array.from(new Set(ids));
  if (uniqueIds.length === 0) {
    return { affectedCount: 0, requestedCount: 0 };
  }

  const { rowCount } = await dbPool.query(
    `
      DELETE FROM chapters
      WHERE id = ANY($1::uuid[])
    `,
    [uniqueIds]
  );

  return {
    requestedCount: uniqueIds.length,
    affectedCount: Number(rowCount ?? 0),
    skippedCount: Math.max(0, uniqueIds.length - Number(rowCount ?? 0))
  };
}

export async function enqueueChapterReruns(params: {
  chapterIds: string[];
  fieldNames: ChapterFieldName[];
  priority?: number;
}): Promise<ChapterActionResult> {
  const dbPool = getDbPool();
  const uniqueIds = Array.from(new Set(params.chapterIds));
  const uniqueFields = Array.from(new Set(params.fieldNames));
  if (uniqueIds.length === 0 || uniqueFields.length === 0) {
    return { affectedCount: 0, requestedCount: uniqueIds.length };
  }

  const priority = params.priority ?? 90;
  const requestedCount = uniqueIds.length * uniqueFields.length;

  const { rows } = await dbPool.query<{
    affected_count: number;
    missing_source_count: number;
  }>(
    `
      WITH selected_chapters AS (
        SELECT c.id, c.slug
        FROM chapters c
        WHERE c.id = ANY($1::uuid[])
      ),
      chapter_sources AS (
        SELECT
          sc.id AS chapter_id,
          sc.slug AS chapter_slug,
          latest.source_id,
          latest.crawl_run_id,
          s.slug AS source_slug
        FROM selected_chapters sc
        LEFT JOIN LATERAL (
          SELECT cp.source_id, cp.crawl_run_id
          FROM chapter_provenance cp
          WHERE cp.chapter_id = sc.id
          ORDER BY cp.extracted_at DESC, cp.created_at DESC
          LIMIT 1
        ) latest ON TRUE
        LEFT JOIN sources s ON s.id = latest.source_id
      ),
      field_requests AS (
        SELECT
          cs.chapter_id,
          cs.chapter_slug,
          cs.crawl_run_id,
          cs.source_slug,
          field_name
        FROM chapter_sources cs
        CROSS JOIN unnest($2::text[]) AS field_name
        WHERE cs.source_slug IS NOT NULL
      ),
      upserted AS (
        INSERT INTO field_jobs (
          chapter_id,
          crawl_run_id,
          field_name,
          status,
          payload,
          attempts,
          max_attempts,
          scheduled_at,
          last_error,
          terminal_failure,
          priority,
          completed_payload,
          claimed_by,
          claim_token,
          started_at,
          finished_at
        )
        SELECT
          fr.chapter_id,
          fr.crawl_run_id,
          fr.field_name,
          'queued',
          jsonb_build_object('sourceSlug', fr.source_slug, 'chapterSlug', fr.chapter_slug),
          0,
          3,
          NOW(),
          NULL,
          false,
          $3,
          '{}'::jsonb,
          NULL,
          NULL,
          NULL,
          NULL
        FROM field_requests fr
        ON CONFLICT (chapter_id, field_name) WHERE status IN ('queued', 'running')
        DO UPDATE SET
          crawl_run_id = COALESCE(EXCLUDED.crawl_run_id, field_jobs.crawl_run_id),
          status = 'queued',
          payload = COALESCE(field_jobs.payload, '{}'::jsonb) || EXCLUDED.payload,
          attempts = 0,
          max_attempts = GREATEST(field_jobs.max_attempts, EXCLUDED.max_attempts),
          scheduled_at = NOW(),
          started_at = NULL,
          finished_at = NULL,
          last_error = NULL,
          terminal_failure = false,
          priority = GREATEST(field_jobs.priority, EXCLUDED.priority),
          completed_payload = '{}'::jsonb,
          claimed_by = NULL,
          claim_token = NULL
        RETURNING chapter_id
      )
      SELECT
        (SELECT COUNT(*)::int FROM upserted) AS affected_count,
        (SELECT COUNT(*)::int FROM chapter_sources WHERE source_slug IS NULL) AS missing_source_count
    `,
    [uniqueIds, uniqueFields, priority]
  );

  const summary = rows[0] ?? { affected_count: 0, missing_source_count: uniqueIds.length };

  return {
    requestedCount,
    affectedCount: Number(summary.affected_count ?? 0),
    missingSourceCount: Number(summary.missing_source_count ?? 0),
    skippedCount: Math.max(0, requestedCount - Number(summary.affected_count ?? 0))
  };
}
