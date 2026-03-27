BEGIN;

ALTER TABLE field_jobs
  ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_field_jobs_priority_claim
ON field_jobs(status, scheduled_at, priority DESC)
WHERE status = 'queued';

CREATE TABLE IF NOT EXISTS fraternity_crawl_requests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fraternity_name TEXT NOT NULL,
  fraternity_slug TEXT NOT NULL,
  source_slug TEXT,
  source_url TEXT,
  source_confidence NUMERIC(5, 4),
  status TEXT NOT NULL CHECK (status IN ('draft', 'queued', 'running', 'succeeded', 'failed', 'canceled')),
  stage TEXT NOT NULL CHECK (stage IN ('discovery', 'awaiting_confirmation', 'crawl_run', 'enrichment', 'completed', 'failed')),
  scheduled_for TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  priority INTEGER NOT NULL DEFAULT 0,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  progress JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraternity_crawl_requests_status_scheduled
ON fraternity_crawl_requests(status, scheduled_for ASC);

CREATE INDEX IF NOT EXISTS idx_fraternity_crawl_requests_created_at
ON fraternity_crawl_requests(created_at DESC);

CREATE TABLE IF NOT EXISTS fraternity_crawl_request_events (
  id BIGSERIAL PRIMARY KEY,
  request_id UUID NOT NULL REFERENCES fraternity_crawl_requests(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  message TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraternity_crawl_request_events_request_id
ON fraternity_crawl_request_events(request_id, created_at DESC);

DROP TRIGGER IF EXISTS trg_fraternity_crawl_requests_updated_at ON fraternity_crawl_requests;
CREATE TRIGGER trg_fraternity_crawl_requests_updated_at
BEFORE UPDATE ON fraternity_crawl_requests
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
