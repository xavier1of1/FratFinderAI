BEGIN;

CREATE TABLE IF NOT EXISTS crawl_enrichment_observations (
  id BIGSERIAL PRIMARY KEY,
  field_job_id UUID REFERENCES field_jobs(id) ON DELETE SET NULL,
  chapter_id UUID NOT NULL REFERENCES chapters(id) ON DELETE CASCADE,
  chapter_slug TEXT NOT NULL,
  fraternity_slug TEXT,
  source_slug TEXT,
  field_name TEXT NOT NULL,
  queue_state TEXT NOT NULL DEFAULT 'actionable',
  runtime_mode TEXT NOT NULL,
  policy_version TEXT,
  policy_mode TEXT NOT NULL DEFAULT 'shadow',
  recommended_action TEXT,
  deterministic_action TEXT,
  recommended_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  context_features JSONB NOT NULL DEFAULT '{}'::jsonb,
  provider_window_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  outcome JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_enrichment_observations_created
  ON crawl_enrichment_observations (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_enrichment_observations_source_field
  ON crawl_enrichment_observations (source_slug, field_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_enrichment_observations_actions
  ON crawl_enrichment_observations (recommended_action, deterministic_action, created_at DESC);

COMMIT;
