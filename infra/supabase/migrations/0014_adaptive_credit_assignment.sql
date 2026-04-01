BEGIN;

ALTER TABLE crawl_page_observations
  ADD COLUMN IF NOT EXISTS structural_template_signature TEXT,
  ADD COLUMN IF NOT EXISTS parent_observation_id BIGINT REFERENCES crawl_page_observations(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS path_depth INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS risk_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS guardrail_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS context_bucket TEXT;

ALTER TABLE crawl_reward_events
  ADD COLUMN IF NOT EXISTS reward_stage TEXT NOT NULL DEFAULT 'immediate',
  ADD COLUMN IF NOT EXISTS attributed_observation_id BIGINT REFERENCES crawl_page_observations(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS discount_factor DOUBLE PRECISION NOT NULL DEFAULT 1.0;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'crawl_reward_events_reward_stage_check'
  ) THEN
    ALTER TABLE crawl_reward_events
      ADD CONSTRAINT crawl_reward_events_reward_stage_check
      CHECK (reward_stage IN ('immediate', 'delayed', 'terminal'));
  END IF;
END $$;

COMMIT;
