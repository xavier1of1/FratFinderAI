import { getDbPool } from "../db";
import { normalizeInstagramUrl } from "../social";
import type { NationalProfile } from "../types";

export async function listNationalProfiles(limit = 250): Promise<NationalProfile[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<NationalProfile>(
    `
      SELECT
        fraternity_slug AS "fraternitySlug",
        fraternity_name AS "fraternityName",
        national_url AS "nationalUrl",
        national_url_confidence AS "nationalUrlConfidence",
        national_url_provenance_type AS "nationalUrlProvenanceType",
        national_url_reason_code AS "nationalUrlReasonCode",
        contact_email AS "contactEmail",
        contact_email_confidence AS "contactEmailConfidence",
        contact_email_provenance_type AS "contactEmailProvenanceType",
        contact_email_reason_code AS "contactEmailReasonCode",
        instagram_url AS "instagramUrl",
        instagram_confidence AS "instagramConfidence",
        instagram_provenance_type AS "instagramProvenanceType",
        instagram_reason_code AS "instagramReasonCode",
        phone,
        phone_confidence AS "phoneConfidence",
        phone_provenance_type AS "phoneProvenanceType",
        phone_reason_code AS "phoneReasonCode",
        address_text AS "addressText",
        address_confidence AS "addressConfidence",
        address_provenance_type AS "addressProvenanceType",
        address_reason_code AS "addressReasonCode",
        metadata,
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM national_profiles
      ORDER BY fraternity_name ASC
      LIMIT $1
    `,
    [Math.max(1, limit)]
  );

  return rows.map((row) => ({
    ...row,
    nationalUrlConfidence: Number(row.nationalUrlConfidence ?? 0),
    contactEmailConfidence: Number(row.contactEmailConfidence ?? 0),
    instagramConfidence: Number(row.instagramConfidence ?? 0),
    phoneConfidence: Number(row.phoneConfidence ?? 0),
    addressConfidence: Number(row.addressConfidence ?? 0),
    instagramUrl: normalizeInstagramUrl(row.instagramUrl),
    metadata: row.metadata ?? {}
  }));
}

export async function getNationalProfileCounts(): Promise<{
  total: number;
  withEmail: number;
  withInstagram: number;
  withPhone: number;
}> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{
    total: string | number;
    withEmail: string | number;
    withInstagram: string | number;
    withPhone: string | number;
  }>(
    `
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE contact_email IS NOT NULL AND BTRIM(contact_email) <> '')::int AS "withEmail",
        COUNT(*) FILTER (WHERE instagram_url IS NOT NULL AND BTRIM(instagram_url) <> '')::int AS "withInstagram",
        COUNT(*) FILTER (WHERE phone IS NOT NULL AND BTRIM(phone) <> '')::int AS "withPhone"
      FROM national_profiles
    `
  );

  const row = rows[0];
  return {
    total: Number(row?.total ?? 0),
    withEmail: Number(row?.withEmail ?? 0),
    withInstagram: Number(row?.withInstagram ?? 0),
    withPhone: Number(row?.withPhone ?? 0)
  };
}
