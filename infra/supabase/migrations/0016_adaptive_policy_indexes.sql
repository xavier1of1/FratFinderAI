BEGIN;

CREATE INDEX IF NOT EXISTS idx_crawl_page_observations_created_at
  ON crawl_page_observations (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_page_observations_session_parent
  ON crawl_page_observations (crawl_session_id, parent_observation_id);

CREATE INDEX IF NOT EXISTS idx_crawl_page_observations_context_bucket
  ON crawl_page_observations (context_bucket, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_page_observations_structural_signature
  ON crawl_page_observations (structural_template_signature, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_reward_events_stage_created
  ON crawl_reward_events (reward_stage, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_reward_events_attributed_observation
  ON crawl_reward_events (attributed_observation_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_policy_snapshots_policy_runtime_created
  ON crawl_policy_snapshots (policy_version, runtime_mode, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_epoch_metrics_policy_runtime_created
  ON crawl_epoch_metrics (policy_version, runtime_mode, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawl_epoch_metrics_cohort_created
  ON crawl_epoch_metrics (cohort_label, created_at DESC);

COMMIT;
