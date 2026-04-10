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
