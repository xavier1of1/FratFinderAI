CREATE TABLE IF NOT EXISTS crm_campaigns (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('email', 'instagram')),
    status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'ready', 'sending', 'drafted', 'sent', 'partial', 'failed')),
    delivery_mode text NOT NULL DEFAULT 'operator' CHECK (delivery_mode IN ('operator', 'outlook')),
    subject_template text,
    message_template text NOT NULL,
    filters jsonb NOT NULL DEFAULT '{}'::jsonb,
    recipient_count integer NOT NULL DEFAULT 0,
    queued_count integer NOT NULL DEFAULT 0,
    drafted_count integer NOT NULL DEFAULT 0,
    sent_count integer NOT NULL DEFAULT 0,
    failed_count integer NOT NULL DEFAULT 0,
    launched_at timestamptz,
    completed_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crm_campaign_recipients (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id uuid NOT NULL REFERENCES crm_campaigns(id) ON DELETE CASCADE,
    chapter_id uuid NOT NULL REFERENCES chapters(id) ON DELETE CASCADE,
    fraternity_slug text NOT NULL,
    fraternity_name text NOT NULL,
    chapter_name text NOT NULL,
    university_name text,
    city text,
    state text,
    channel text NOT NULL CHECK (channel IN ('email', 'instagram')),
    contact_value text NOT NULL,
    subject_line text,
    message_body text NOT NULL,
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'drafted', 'sent', 'failed')),
    last_error text,
    sent_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, chapter_id, channel)
);

CREATE INDEX IF NOT EXISTS crm_campaigns_status_idx
    ON crm_campaigns (status, created_at DESC);

CREATE INDEX IF NOT EXISTS crm_campaign_recipients_campaign_idx
    ON crm_campaign_recipients (campaign_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS crm_campaign_recipients_channel_idx
    ON crm_campaign_recipients (channel, status, created_at DESC);
