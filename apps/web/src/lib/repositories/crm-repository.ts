import { randomUUID } from "crypto";

import { getDbPool } from "../db";
import { buildCrmTemplateContext, normalizeCrmFilters, renderCrmTemplate, type CrmRecipientSeed, defaultCrmMessage, defaultCrmSubject } from "../crm";
import { deliverOutlookMessage } from "../outreach-email";
import type {
  CrmCampaign,
  CrmCampaignFilters,
  CrmCampaignRecipient,
  CrmCampaignStatus,
  CrmChannel,
  CrmDeliveryMode,
  CrmDispatchMode,
  CrmRecipientStatus
} from "../types";

interface CrmCampaignRow {
  id: string;
  name: string;
  channel: CrmChannel;
  status: CrmCampaignStatus;
  deliveryMode: CrmDeliveryMode;
  subjectTemplate: string | null;
  messageTemplate: string;
  filters: CrmCampaignFilters;
  recipientCount: number;
  queuedCount: number;
  draftedCount: number;
  sentCount: number;
  failedCount: number;
  launchedAt: string | null;
  completedAt: string | null;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

interface CrmRecipientRow {
  id: string;
  campaignId: string;
  chapterId: string;
  fraternitySlug: string;
  fraternityName: string;
  chapterName: string;
  universityName: string | null;
  city: string | null;
  state: string | null;
  channel: CrmChannel;
  contactValue: string;
  subjectLine: string | null;
  messageBody: string;
  status: CrmRecipientStatus;
  lastError: string | null;
  sentAt: string | null;
  createdAt: string;
  updatedAt: string;
}

function mapCampaign(row: CrmCampaignRow, recipients: CrmCampaignRecipient[] = []): CrmCampaign {
  return {
    ...row,
    recipients
  };
}

function mapRecipient(row: CrmRecipientRow): CrmCampaignRecipient {
  return { ...row };
}

function buildRecipientWhere(filters: CrmCampaignFilters, channel: CrmChannel) {
  const where: string[] = [
    "NOT (COALESCE(c.field_states ->> 'website_url', '') = 'invalid_entity' AND COALESCE(c.field_states ->> 'instagram_url', '') = 'invalid_entity' AND COALESCE(c.field_states ->> 'contact_email', '') = 'invalid_entity')"
  ];
  const values: unknown[] = [];

  if (filters.chapterStatus && filters.chapterStatus !== "all") {
    values.push(filters.chapterStatus);
    where.push(`c.chapter_status = $${values.length}`);
  }
  if (filters.fraternitySlug) {
    values.push(filters.fraternitySlug);
    where.push(`f.slug = $${values.length}`);
  }
  if (filters.state) {
    values.push(filters.state);
    where.push(`upper(coalesce(c.state, '')) = upper($${values.length})`);
  }
  if (filters.search) {
    values.push(filters.search);
    where.push(`(c.name ILIKE '%' || $${values.length} || '%' OR c.university_name ILIKE '%' || $${values.length} || '%')`);
  }

  const channelGuard =
    channel === "email"
      ? `
        c.contact_email IS NOT NULL
        AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') IN (
          'chapter_specific','national_specific_to_chapter'
        )
        AND split_part(lower(c.contact_email), '@', 1) !~ '(greek|greeks|fraternit|sororit|student|campus|life|dean|ifc|panhell|interfraternity|office|admin)'
        AND split_part(lower(c.contact_email), '@', 2) !~ '(\\.old$|\\.related$)'
        AND NOT (
          (
            COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') NOT IN (
              'chapter_specific','school_specific','national_specific_to_chapter'
            )
          )
          AND np.contact_email IS NOT NULL
          AND lower(c.contact_email) = lower(np.contact_email)
        )
        AND NOT (
          COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') NOT IN (
            'chapter_specific','school_specific','national_specific_to_chapter'
          )
          AND split_part(lower(c.contact_email), '@', 1) ~ '(fsl|graduateprogram|graduateprograms|greeklife|greek\\.life|hq|ihq|leadership|national|nationals|office|ofsl|reslife|studentaffairs|studentengagement|studentinvolvement|studentlife|studentorg|studentorganization|studentorganizations)'
        )
      `
      : `
        c.instagram_url IS NOT NULL
        AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') IN (
          'chapter_specific','national_specific_to_chapter'
        )
        AND NOT (
          (
            COALESCE(c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') NOT IN (
              'chapter_specific','school_specific','national_specific_to_chapter'
            )
          )
          AND np.instagram_url IS NOT NULL
          AND lower(regexp_replace(c.instagram_url, '[^a-z0-9]+', '', 'g')) = lower(regexp_replace(np.instagram_url, '[^a-z0-9]+', '', 'g'))
        )
        AND NOT (
          COALESCE(c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') NOT IN (
            'chapter_specific','school_specific','national_specific_to_chapter'
          )
          AND lower(regexp_replace(c.instagram_url, '[^a-z0-9]+', '', 'g')) ~ '(hq|ihq|national|nationals|officialhq)'
        )
      `;

  where.push(channelGuard);
  return { where, values };
}

async function fetchRecipientsForCampaign(filters: CrmCampaignFilters, channel: CrmChannel): Promise<CrmRecipientSeed[]> {
  const normalizedFilters = normalizeCrmFilters(filters);
  const { where, values } = buildRecipientWhere(normalizedFilters, channel);
  values.push(normalizedFilters.limit ?? 50);

  const dbPool = getDbPool();
  const { rows } = await dbPool.query<CrmRecipientSeed & { email: string | null; instagramUrl: string | null }>(
    `
      SELECT
        c.id AS "chapterId",
        f.slug AS "fraternitySlug",
        f.name AS "fraternityName",
        c.name AS "chapterName",
        c.university_name AS "universityName",
        c.city,
        c.state,
        c.contact_email AS email,
        c.instagram_url AS "instagramUrl"
      FROM chapters c
      JOIN fraternities f ON f.id = c.fraternity_id
      LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
      WHERE ${where.map((item) => `(${item})`).join(" AND ")}
      ORDER BY c.updated_at DESC, c.id DESC
      LIMIT $${values.length}
    `,
    values
  );

  return rows;
}

async function refreshCampaignCounts(campaignId: string) {
  const dbPool = getDbPool();
  await dbPool.query(
    `
      UPDATE crm_campaigns campaign
      SET
        recipient_count = stats.recipient_count,
        queued_count = stats.queued_count,
        drafted_count = stats.drafted_count,
        sent_count = stats.sent_count,
        failed_count = stats.failed_count,
        updated_at = now(),
        completed_at = CASE
          WHEN stats.recipient_count > 0 AND stats.recipient_count = stats.sent_count + stats.failed_count THEN COALESCE(campaign.completed_at, now())
          ELSE NULL
        END,
        status = CASE
          WHEN stats.recipient_count = 0 THEN 'draft'
          WHEN stats.sent_count = stats.recipient_count THEN 'sent'
          WHEN stats.failed_count > 0 AND stats.sent_count > 0 THEN 'partial'
          WHEN stats.failed_count > 0 AND stats.sent_count = 0 AND stats.queued_count = 0 AND stats.drafted_count = 0 THEN 'failed'
          WHEN stats.drafted_count > 0 AND stats.queued_count = 0 THEN 'drafted'
          ELSE campaign.status
        END
      FROM (
        SELECT
          campaign_id,
          COUNT(*)::int AS recipient_count,
          COUNT(*) FILTER (WHERE status = 'queued')::int AS queued_count,
          COUNT(*) FILTER (WHERE status = 'drafted')::int AS drafted_count,
          COUNT(*) FILTER (WHERE status = 'sent')::int AS sent_count,
          COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_count
        FROM crm_campaign_recipients
        WHERE campaign_id = $1
        GROUP BY campaign_id
      ) stats
      WHERE campaign.id = stats.campaign_id
    `,
    [campaignId]
  );
}

export async function getCrmCampaignCounts(): Promise<{
  total: number;
  ready: number;
  sending: number;
  sent: number;
}> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<{ total: number; ready: number; sending: number; sent: number }>(
    `
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE status IN ('ready', 'drafted', 'partial'))::int AS ready,
        COUNT(*) FILTER (WHERE status = 'sending')::int AS sending,
        COUNT(*) FILTER (WHERE status = 'sent')::int AS sent
      FROM crm_campaigns
    `
  );

  return rows[0] ?? { total: 0, ready: 0, sending: 0, sent: 0 };
}

export async function listCrmCampaigns(limit = 50): Promise<CrmCampaign[]> {
  const dbPool = getDbPool();
  const { rows } = await dbPool.query<CrmCampaignRow>(
    `
      SELECT
        id,
        name,
        channel,
        status,
        delivery_mode AS "deliveryMode",
        subject_template AS "subjectTemplate",
        message_template AS "messageTemplate",
        filters,
        recipient_count AS "recipientCount",
        queued_count AS "queuedCount",
        drafted_count AS "draftedCount",
        sent_count AS "sentCount",
        failed_count AS "failedCount",
        launched_at AS "launchedAt",
        completed_at AS "completedAt",
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM crm_campaigns
      ORDER BY created_at DESC
      LIMIT $1
    `,
    [Math.max(1, Math.min(limit, 200))]
  );

  return rows.map((row) => mapCampaign(row));
}

export async function getCrmCampaign(id: string): Promise<CrmCampaign | null> {
  const dbPool = getDbPool();
  const campaignResult = await dbPool.query<CrmCampaignRow>(
    `
      SELECT
        id,
        name,
        channel,
        status,
        delivery_mode AS "deliveryMode",
        subject_template AS "subjectTemplate",
        message_template AS "messageTemplate",
        filters,
        recipient_count AS "recipientCount",
        queued_count AS "queuedCount",
        drafted_count AS "draftedCount",
        sent_count AS "sentCount",
        failed_count AS "failedCount",
        launched_at AS "launchedAt",
        completed_at AS "completedAt",
        last_error AS "lastError",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM crm_campaigns
      WHERE id = $1
      LIMIT 1
    `,
    [id]
  );

  const campaign = campaignResult.rows[0];
  if (!campaign) {
    return null;
  }

  const recipientsResult = await dbPool.query<CrmRecipientRow>(
    `
      SELECT
        id,
        campaign_id AS "campaignId",
        chapter_id AS "chapterId",
        fraternity_slug AS "fraternitySlug",
        fraternity_name AS "fraternityName",
        chapter_name AS "chapterName",
        university_name AS "universityName",
        city,
        state,
        channel,
        contact_value AS "contactValue",
        subject_line AS "subjectLine",
        message_body AS "messageBody",
        status,
        last_error AS "lastError",
        sent_at AS "sentAt",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM crm_campaign_recipients
      WHERE campaign_id = $1
      ORDER BY created_at ASC
    `,
    [id]
  );

  return mapCampaign(campaign, recipientsResult.rows.map((row) => mapRecipient(row)));
}

export async function createCrmCampaign(params: {
  name: string;
  channel: CrmChannel;
  deliveryMode?: CrmDeliveryMode;
  subjectTemplate?: string | null;
  messageTemplate?: string | null;
  filters?: Partial<CrmCampaignFilters>;
}): Promise<CrmCampaign> {
  const dbPool = getDbPool();
  const filters = normalizeCrmFilters(params.filters);
  const channel = params.channel;
  const deliveryMode = params.deliveryMode ?? (channel === "email" ? "outlook" : "operator");
  const subjectTemplate = channel === "email" ? (params.subjectTemplate?.trim() || defaultCrmSubject(channel)) : null;
  const messageTemplate = params.messageTemplate?.trim() || defaultCrmMessage(channel);
  const seeds = await fetchRecipientsForCampaign(filters, channel);

  const client = await dbPool.connect();
  try {
    await client.query("BEGIN");

    const campaignResult = await client.query<CrmCampaignRow>(
      `
        INSERT INTO crm_campaigns (
          id,
          name,
          channel,
          status,
          delivery_mode,
          subject_template,
          message_template,
          filters,
          recipient_count,
          queued_count,
          created_at,
          updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $9, now(), now())
        RETURNING
          id,
          name,
          channel,
          status,
          delivery_mode AS "deliveryMode",
          subject_template AS "subjectTemplate",
          message_template AS "messageTemplate",
          filters,
          recipient_count AS "recipientCount",
          queued_count AS "queuedCount",
          drafted_count AS "draftedCount",
          sent_count AS "sentCount",
          failed_count AS "failedCount",
          launched_at AS "launchedAt",
          completed_at AS "completedAt",
          last_error AS "lastError",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
      `,
      [
        randomUUID(),
        params.name.trim(),
        channel,
        seeds.length > 0 ? "ready" : "draft",
        deliveryMode,
        subjectTemplate,
        messageTemplate,
        JSON.stringify(filters),
        seeds.length
      ]
    );

    const campaign = campaignResult.rows[0];
    if (!campaign) {
      throw new Error("Failed to create CRM campaign.");
    }
    const recipients: CrmCampaignRecipient[] = [];

    for (const seed of seeds) {
      const context = buildCrmTemplateContext(seed);
      const contactValue = channel === "email" ? seed.email : seed.instagramUrl;
      if (!contactValue) {
        continue;
      }

      const subjectLine = channel === "email" && subjectTemplate ? renderCrmTemplate(subjectTemplate, context) : null;
      const messageBody = renderCrmTemplate(messageTemplate, context);

      const recipientResult = await client.query<CrmRecipientRow>(
        `
          INSERT INTO crm_campaign_recipients (
            id,
            campaign_id,
            chapter_id,
            fraternity_slug,
            fraternity_name,
            chapter_name,
            university_name,
            city,
            state,
            channel,
            contact_value,
            subject_line,
            message_body,
            status,
            created_at,
            updated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'queued', now(), now())
          RETURNING
            id,
            campaign_id AS "campaignId",
            chapter_id AS "chapterId",
            fraternity_slug AS "fraternitySlug",
            fraternity_name AS "fraternityName",
            chapter_name AS "chapterName",
            university_name AS "universityName",
            city,
            state,
            channel,
            contact_value AS "contactValue",
            subject_line AS "subjectLine",
            message_body AS "messageBody",
            status,
            last_error AS "lastError",
            sent_at AS "sentAt",
            created_at AS "createdAt",
            updated_at AS "updatedAt"
        `,
        [
          randomUUID(),
          campaign.id,
          seed.chapterId,
          seed.fraternitySlug,
          seed.fraternityName,
          seed.chapterName,
          seed.universityName,
          seed.city,
          seed.state,
          channel,
          contactValue,
          subjectLine,
          messageBody
        ]
      );
      const recipientRow = recipientResult.rows[0];
      if (!recipientRow) {
        throw new Error("Failed to create CRM recipient.");
      }
      recipients.push(mapRecipient(recipientRow));
    }

    await client.query("COMMIT");
    return mapCampaign(campaign, recipients);
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function updateCrmRecipientStatus(params: {
  recipientId: string;
  status: CrmRecipientStatus;
  lastError?: string | null;
}): Promise<CrmCampaignRecipient | null> {
  const dbPool = getDbPool();
  const result = await dbPool.query<CrmRecipientRow>(
    `
      UPDATE crm_campaign_recipients
      SET
        status = $2,
        last_error = $3,
        sent_at = CASE WHEN $2 IN ('drafted', 'sent') THEN COALESCE(sent_at, now()) ELSE NULL END,
        updated_at = now()
      WHERE id = $1
      RETURNING
        id,
        campaign_id AS "campaignId",
        chapter_id AS "chapterId",
        fraternity_slug AS "fraternitySlug",
        fraternity_name AS "fraternityName",
        chapter_name AS "chapterName",
        university_name AS "universityName",
        city,
        state,
        channel,
        contact_value AS "contactValue",
        subject_line AS "subjectLine",
        message_body AS "messageBody",
        status,
        last_error AS "lastError",
        sent_at AS "sentAt",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
    `,
    [params.recipientId, params.status, params.lastError ?? null]
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }
  await refreshCampaignCounts(row.campaignId);
  return mapRecipient(row);
}

export async function dispatchCrmCampaign(params: {
  id: string;
  mode: CrmDispatchMode;
}): Promise<{
  campaign: CrmCampaign;
  processed: number;
  drafted: number;
  sent: number;
  failed: number;
}> {
  const dbPool = getDbPool();
  const campaign = await getCrmCampaign(params.id);
  if (!campaign) {
    throw new Error("CRM campaign not found");
  }
  if (campaign.channel !== "email") {
    throw new Error("Only email campaigns support direct dispatch.");
  }

  await dbPool.query(
    `
      UPDATE crm_campaigns
      SET status = 'sending', launched_at = COALESCE(launched_at, now()), updated_at = now(), last_error = NULL
      WHERE id = $1
    `,
    [params.id]
  );

  let drafted = 0;
  let sent = 0;
  let failed = 0;

  for (const recipient of campaign.recipients.filter((item) => item.status === "queued")) {
    try {
      const result = await deliverOutlookMessage({
        to: recipient.contactValue,
        subject: recipient.subjectLine ?? "",
        body: recipient.messageBody,
        mode: params.mode
      });

      await dbPool.query(
        `
          UPDATE crm_campaign_recipients
          SET
            status = $2,
            last_error = NULL,
            sent_at = now(),
            updated_at = now()
          WHERE id = $1
        `,
        [recipient.id, result.action === "drafted" ? "drafted" : "sent"]
      );
      if (result.action === "drafted") {
        drafted += 1;
      } else {
        sent += 1;
      }
    } catch (error) {
      failed += 1;
      await dbPool.query(
        `
          UPDATE crm_campaign_recipients
          SET
            status = 'failed',
            last_error = $2,
            updated_at = now()
          WHERE id = $1
        `,
        [recipient.id, error instanceof Error ? error.message : String(error)]
      );
    }
  }

  await refreshCampaignCounts(params.id);
  const refreshed = await getCrmCampaign(params.id);
  if (!refreshed) {
    throw new Error("CRM campaign not found after dispatch");
  }

  return {
    campaign: refreshed,
    processed: drafted + sent + failed,
    drafted,
    sent,
    failed
  };
}
