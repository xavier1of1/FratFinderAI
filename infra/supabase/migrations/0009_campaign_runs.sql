BEGIN;

CREATE TABLE IF NOT EXISTS campaign_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('draft', 'queued', 'running', 'succeeded', 'failed', 'canceled')),
  scheduled_for TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  telemetry JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_campaign_runs_status_scheduled
ON campaign_runs(status, scheduled_for ASC);

CREATE INDEX IF NOT EXISTS idx_campaign_runs_created_at
ON campaign_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS campaign_run_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_run_id UUID NOT NULL REFERENCES campaign_runs(id) ON DELETE CASCADE,
  fraternity_name TEXT NOT NULL,
  fraternity_slug TEXT NOT NULL,
  request_id UUID REFERENCES fraternity_crawl_requests(id) ON DELETE SET NULL,
  cohort TEXT NOT NULL CHECK (cohort IN ('new', 'control')),
  status TEXT NOT NULL CHECK (status IN ('planned', 'request_created', 'queued', 'running', 'completed', 'failed', 'skipped', 'canceled')),
  selection_reason TEXT,
  scorecard JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (campaign_run_id, fraternity_slug)
);

CREATE INDEX IF NOT EXISTS idx_campaign_run_items_campaign_id
ON campaign_run_items(campaign_run_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_campaign_run_items_request_id
ON campaign_run_items(request_id);

CREATE TABLE IF NOT EXISTS campaign_run_events (
  id BIGSERIAL PRIMARY KEY,
  campaign_run_id UUID NOT NULL REFERENCES campaign_runs(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  message TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_campaign_run_events_campaign_id
ON campaign_run_events(campaign_run_id, created_at DESC);

DROP TRIGGER IF EXISTS trg_campaign_runs_updated_at ON campaign_runs;
CREATE TRIGGER trg_campaign_runs_updated_at
BEFORE UPDATE ON campaign_runs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_campaign_run_items_updated_at ON campaign_run_items;
CREATE TRIGGER trg_campaign_run_items_updated_at
BEFORE UPDATE ON campaign_run_items
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
