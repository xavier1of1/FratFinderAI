import { getDbPool } from "../db";
import { normalizeInstagramUrl } from "../social";
import type { ChapterActionResult, ChapterFieldName, ChapterListItem, ChapterListResponse, ChapterMapStateSummary } from "../types";

type ChapterListQueryRow = ChapterListItem & {
  contactProvenance: Record<string, unknown> | null;
  nationalEmail: string | null;
  nationalInstagramUrl: string | null;
};

const STATE_CODES = new Set([
  "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
  "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
  "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]);

const CHAPTER_VISIBILITY_CLAUSE = `
  NOT (
    COALESCE(c.field_states ->> 'website_url', '') = 'invalid_entity'
    AND COALESCE(c.field_states ->> 'instagram_url', '') = 'invalid_entity'
    AND COALESCE(c.field_states ->> 'contact_email', '') = 'invalid_entity'
  )
`;

const STATE_NAME_TO_CODE: Record<string, string> = {
  ALABAMA: "AL", ALASKA: "AK", ARIZONA: "AZ", ARKANSAS: "AR", CALIFORNIA: "CA", COLORADO: "CO", CONNECTICUT: "CT", DELAWARE: "DE", "DISTRICT OF COLUMBIA": "DC",
  FLORIDA: "FL", GEORGIA: "GA", HAWAII: "HI", IDAHO: "ID", ILLINOIS: "IL", INDIANA: "IN", IOWA: "IA", KANSAS: "KS", KENTUCKY: "KY", LOUISIANA: "LA",
  MAINE: "ME", MARYLAND: "MD", MASSACHUSETTS: "MA", MICHIGAN: "MI", MINNESOTA: "MN", MISSISSIPPI: "MS", MISSOURI: "MO", MONTANA: "MT", NEBRASKA: "NE", NEVADA: "NV",
  "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", OHIO: "OH", OKLAHOMA: "OK", OREGON: "OR", PENNSYLVANIA: "PA",
  "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", TENNESSEE: "TN", TEXAS: "TX", UTAH: "UT", VERMONT: "VT", VIRGINIA: "VA", WASHINGTON: "WA", "WEST VIRGINIA": "WV",
  WISCONSIN: "WI", WYOMING: "WY"
};

function normalizeStateValue(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const upper = trimmed.toUpperCase();
  if (STATE_CODES.has(upper)) {
    return upper;
  }
  const collapsed = trimmed.replace(/[^A-Za-z ]/g, " ").replace(/\s+/g, " ").trim().toUpperCase();
  return STATE_NAME_TO_CODE[collapsed] ?? null;
}

function buildStateNormalizationCase(stateRef: string) {
  return `
  CASE
    WHEN ${stateRef} IS NULL OR btrim(${stateRef}) = '' THEN NULL
    WHEN upper(btrim(${stateRef})) IN (
      'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME',
      'MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
      'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
    ) THEN upper(btrim(${stateRef}))
    ELSE CASE regexp_replace(lower(btrim(${stateRef})), '[^a-z ]', '', 'g')
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
}

const CHAPTER_STATE_NORMALIZATION_CASE = buildStateNormalizationCase("c.state");
const CONSENSUS_STATE_NORMALIZATION_CASE = buildStateNormalizationCase("c2.state");

const UNIVERSITY_STATE_CONSENSUS_CTE = `
  university_state_consensus AS (
    SELECT
      lower(btrim(c2.university_name)) AS university_key,
      MIN(${CONSENSUS_STATE_NORMALIZATION_CASE}) AS inferred_state,
      COUNT(*) FILTER (WHERE ${CONSENSUS_STATE_NORMALIZATION_CASE} IS NOT NULL) AS known_rows,
      COUNT(DISTINCT ${CONSENSUS_STATE_NORMALIZATION_CASE}) FILTER (WHERE ${CONSENSUS_STATE_NORMALIZATION_CASE} IS NOT NULL) AS distinct_states
    FROM chapters c2
    WHERE ${CHAPTER_VISIBILITY_CLAUSE.replaceAll("c.", "c2.")}
      AND NULLIF(btrim(c2.university_name), '') IS NOT NULL
    GROUP BY lower(btrim(c2.university_name))
  )
`;

function normalizeCompact(value: string | null | undefined) {
  return (value ?? "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function hasTrustedContactProvenance(raw: unknown) {
  if (!raw || typeof raw !== "object") {
    return false;
  }
  const value = raw as Record<string, unknown>;
  const specificity = typeof value.contactProvenanceType === "string" ? value.contactProvenanceType : null;
  return specificity === "chapter_specific" || specificity === "school_specific" || specificity === "national_specific_to_chapter";
}

function looksLikeGenericOfficeEmail(email: string) {
  const localPart = (email.split("@", 1)[0] ?? "").toLowerCase();
  const markers = [
    "fsl",
    "graduateprogram",
    "graduateprograms",
    "greeklife",
    "greek.life",
    "hq",
    "ihq",
    "leadership",
    "national",
    "nationals",
    "office",
    "ofsl",
    "reslife",
    "studentaffairs",
    "studentengagement",
    "studentinvolvement",
    "studentlife",
    "studentorg",
    "studentorganization",
    "studentorganizations"
  ];
  return markers.some((marker) => localPart.includes(marker));
}

function looksLikeNationalGenericInstagram(instagramUrl: string) {
  const handle = normalizeCompact(normalizeInstagramUrl(instagramUrl)?.split("/").pop());
  return ["hq", "ihq", "national", "nationals", "officialhq"].some((marker) => handle.includes(marker));
}

function sanitizeChapterForDisplay(row: ChapterListQueryRow): ChapterListItem | null {
  const fieldStates = row.fieldStates ?? {};
  const allInvalid =
    fieldStates.website_url === "invalid_entity" &&
    fieldStates.instagram_url === "invalid_entity" &&
    fieldStates.contact_email === "invalid_entity";
  if (allInvalid) {
    return null;
  }

  const contactProvenance = row.contactProvenance ?? {};
  const emailProvenance = (contactProvenance as Record<string, unknown>).contact_email;
  const instagramProvenance = (contactProvenance as Record<string, unknown>).instagram_url;

  let contactEmail = row.contactEmail;
  let instagramUrl = normalizeInstagramUrl(row.instagramUrl);
  const nextFieldStates = { ...fieldStates };

  const nationalEmail = row.nationalEmail?.trim().toLowerCase() ?? null;
  if (contactEmail) {
    const normalizedEmail = contactEmail.trim().toLowerCase();
    const trusted = hasTrustedContactProvenance(emailProvenance);
    if ((!trusted && nationalEmail && normalizedEmail === nationalEmail) || (!trusted && looksLikeGenericOfficeEmail(normalizedEmail))) {
      contactEmail = null;
      nextFieldStates.contact_email = "missing";
    }
  }

  const nationalInstagram = normalizeInstagramUrl(row.nationalInstagramUrl);
  if (instagramUrl) {
    const trusted = hasTrustedContactProvenance(instagramProvenance);
    if (
      (!trusted && nationalInstagram && normalizeCompact(instagramUrl) === normalizeCompact(nationalInstagram)) ||
      (!trusted && looksLikeNationalGenericInstagram(instagramUrl))
    ) {
      instagramUrl = null;
      nextFieldStates.instagram_url = "missing";
    }
  }

  return {
    ...row,
    contactEmail,
    instagramUrl,
    fieldStates: nextFieldStates,
    contactProvenance: (row.contactProvenance ?? undefined) as ChapterListItem["contactProvenance"]
  };
}

export async function listChapters(params: {
  search?: string;
  limit?: number;
  offset?: number;
}): Promise<ChapterListItem[]> {
  const search = (params.search ?? "").trim();
  const limit = params.limit ?? 50;
  const offset = params.offset ?? 0;

  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ChapterListQueryRow>(
    `
      WITH ${UNIVERSITY_STATE_CONSENSUS_CTE}
      SELECT
        c.id,
        f.slug AS "fraternitySlug",
        latest_source.slug AS "sourceSlug",
        c.slug,
        c.name,
        c.university_name AS "universityName",
        c.city,
        COALESCE(${CHAPTER_STATE_NORMALIZATION_CASE}, usc.inferred_state) AS state,
        c.country,
        CASE WHEN c.website_url ~* '^https?://' THEN c.website_url ELSE NULL END AS "websiteUrl",
        c.instagram_url AS "instagramUrl",
        c.contact_email AS "contactEmail",
        c.chapter_status AS "chapterStatus",
        c.field_states AS "fieldStates",
        c.contact_provenance AS "contactProvenance",
        np.contact_email AS "nationalEmail",
        np.instagram_url AS "nationalInstagramUrl",
        c.updated_at AS "updatedAt"
      FROM chapters c
      JOIN fraternities f ON f.id = c.fraternity_id
      LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
      LEFT JOIN university_state_consensus usc
        ON lower(btrim(c.university_name)) = usc.university_key
        AND usc.known_rows > 0
        AND usc.distinct_states = 1
      LEFT JOIN LATERAL (
        SELECT s.slug
        FROM chapter_provenance cp
        JOIN sources s ON s.id = cp.source_id
        WHERE cp.chapter_id = c.id
        ORDER BY cp.extracted_at DESC, cp.created_at DESC
        LIMIT 1
      ) latest_source ON TRUE
      WHERE ($1 = '' OR c.name ILIKE '%' || $1 || '%' OR c.university_name ILIKE '%' || $1 || '%')
        AND ${CHAPTER_VISIBILITY_CLAUSE}
      ORDER BY c.updated_at DESC, c.id DESC
      LIMIT $2 OFFSET $3
    `,
    [search, limit, offset]
  );

  return rows
    .map((row) => sanitizeChapterForDisplay(row))
    .filter((row): row is ChapterListItem => row !== null);
}

export async function getChapterListMetadata(params: {
  search?: string;
}): Promise<Omit<ChapterListResponse, "items">> {
  const search = (params.search ?? "").trim();

  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    total_count: number;
    fraternity_slugs: string[] | null;
    state_options: string[] | null;
    chapter_statuses: string[] | null;
    with_website_count: number;
    with_instagram_count: number;
    with_email_count: number;
  }>(
    `
      WITH ${UNIVERSITY_STATE_CONSENSUS_CTE},
      filtered AS (
        SELECT
          f.slug AS fraternity_slug,
          COALESCE(${CHAPTER_STATE_NORMALIZATION_CASE}, usc.inferred_state) AS state,
          NULLIF(btrim(c.chapter_status), '') AS chapter_status,
          NULLIF(btrim(c.website_url), '') AS website_url,
          NULLIF(btrim(c.instagram_url), '') AS instagram_url,
          NULLIF(btrim(c.contact_email), '') AS contact_email,
          c.contact_provenance AS contact_provenance,
          NULLIF(btrim(np.contact_email), '') AS national_email,
          NULLIF(btrim(np.instagram_url), '') AS national_instagram_url
        FROM chapters c
        JOIN fraternities f ON f.id = c.fraternity_id
        LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
        LEFT JOIN university_state_consensus usc
          ON lower(btrim(c.university_name)) = usc.university_key
          AND usc.known_rows > 0
          AND usc.distinct_states = 1
        WHERE ($1 = '' OR c.name ILIKE '%' || $1 || '%' OR c.university_name ILIKE '%' || $1 || '%')
          AND ${CHAPTER_VISIBILITY_CLAUSE}
      )
      SELECT
        COUNT(*)::int AS total_count,
        COALESCE(array_agg(DISTINCT fraternity_slug ORDER BY fraternity_slug), ARRAY[]::text[]) AS fraternity_slugs,
        COALESCE(array_agg(DISTINCT state ORDER BY state) FILTER (WHERE state IS NOT NULL), ARRAY[]::text[]) AS state_options,
        COALESCE(array_agg(DISTINCT chapter_status ORDER BY chapter_status) FILTER (WHERE chapter_status IS NOT NULL), ARRAY[]::text[]) AS chapter_statuses,
        COUNT(*) FILTER (WHERE website_url IS NOT NULL AND website_url ~* '^https?://')::int AS with_website_count,
        COUNT(*) FILTER (
          WHERE instagram_url IS NOT NULL
            AND NOT (
              (
                COALESCE(contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') NOT IN (
                  'chapter_specific',
                  'school_specific',
                  'national_specific_to_chapter'
                )
              )
              AND national_instagram_url IS NOT NULL
              AND lower(regexp_replace(instagram_url, '[^a-z0-9]+', '', 'g')) = lower(regexp_replace(national_instagram_url, '[^a-z0-9]+', '', 'g'))
            )
            AND NOT (
              COALESCE(contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') NOT IN (
                'chapter_specific',
                'school_specific',
                'national_specific_to_chapter'
              )
              AND lower(regexp_replace(instagram_url, '[^a-z0-9]+', '', 'g')) ~ '(hq|ihq|national|nationals|officialhq)'
            )
        )::int AS with_instagram_count,
        COUNT(*) FILTER (
          WHERE contact_email IS NOT NULL
            AND NOT (
              (
                COALESCE(contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') NOT IN (
                  'chapter_specific',
                  'school_specific',
                  'national_specific_to_chapter'
                )
              )
              AND national_email IS NOT NULL
              AND lower(contact_email) = lower(national_email)
            )
            AND NOT (
              COALESCE(contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') NOT IN (
                'chapter_specific',
                'school_specific',
                'national_specific_to_chapter'
              )
              AND split_part(lower(contact_email), '@', 1) ~ '(fsl|graduateprogram|graduateprograms|greeklife|greek\\.life|hq|ihq|leadership|national|nationals|office|ofsl|reslife|studentaffairs|studentengagement|studentinvolvement|studentlife|studentorg|studentorganization|studentorganizations)'
            )
        )::int AS with_email_count
      FROM filtered
    `,
    [search]
  );

  const summary = rows[0] ?? {
    total_count: 0,
    fraternity_slugs: [],
    state_options: [],
    chapter_statuses: [],
    with_website_count: 0,
    with_instagram_count: 0,
    with_email_count: 0
  };

  return {
    totalCount: Number(summary.total_count ?? 0),
    fraternitySlugs: summary.fraternity_slugs ?? [],
    stateOptions: summary.state_options ?? [],
    chapterStatuses: summary.chapter_statuses ?? [],
    withWebsiteCount: Number(summary.with_website_count ?? 0),
    withInstagramCount: Number(summary.with_instagram_count ?? 0),
    withEmailCount: Number(summary.with_email_count ?? 0)
  };
}

export async function listChapterMapSummary(): Promise<ChapterMapStateSummary[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<ChapterMapStateSummary>(
    `
      WITH ${UNIVERSITY_STATE_CONSENSUS_CTE},
      normalized AS (
        SELECT COALESCE(${CHAPTER_STATE_NORMALIZATION_CASE}, usc.inferred_state) AS state_code
        FROM chapters c
        LEFT JOIN university_state_consensus usc
          ON lower(btrim(c.university_name)) = usc.university_key
          AND usc.known_rows > 0
          AND usc.distinct_states = 1
        WHERE ${CHAPTER_VISIBILITY_CLAUSE}
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
  const normalizedState = normalizeStateValue(params.state);
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
      normalizedState,
      params.chapterStatus,
      params.websiteUrl,
      params.instagramUrl,
      params.contactEmail
    ]
  );

  const row = rows[0] ?? null;
  if (!row) {
    return null;
  }
  return {
    ...row,
    instagramUrl: normalizeInstagramUrl(row.instagramUrl)
  };
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
          queue_state,
          blocked_reason,
          terminal_outcome,
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
          'actionable',
          NULL,
          NULL,
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
          queue_state = 'actionable',
          blocked_reason = NULL,
          terminal_outcome = NULL,
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
