BEGIN;

CREATE TABLE IF NOT EXISTS crawl_epoch_metrics (
  id BIGSERIAL PRIMARY KEY,
  epoch INTEGER NOT NULL,
  policy_version TEXT NOT NULL,
  runtime_mode TEXT NOT NULL,
  train_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
  eval_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
  kpis JSONB NOT NULL DEFAULT '{}'::jsonb,
  deltas JSONB NOT NULL DEFAULT '{}'::jsonb,
  slopes JSONB NOT NULL DEFAULT '{}'::jsonb,
  cohort_label TEXT NOT NULL DEFAULT 'default',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
