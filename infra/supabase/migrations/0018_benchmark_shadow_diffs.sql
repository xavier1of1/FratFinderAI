BEGIN;

CREATE TABLE IF NOT EXISTS benchmark_shadow_diffs (
  id BIGSERIAL PRIMARY KEY,
  benchmark_run_id UUID NOT NULL REFERENCES benchmark_runs(id) ON DELETE CASCADE,
  cycle INTEGER NOT NULL,
  runtime_mode TEXT NOT NULL,
  observed_jobs INTEGER NOT NULL DEFAULT 0,
  decision_mismatch_count INTEGER NOT NULL DEFAULT 0,
  status_mismatch_count INTEGER NOT NULL DEFAULT 0,
  mismatch_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (benchmark_run_id, cycle)
);

CREATE INDEX IF NOT EXISTS idx_benchmark_shadow_diffs_run_cycle
ON benchmark_shadow_diffs (benchmark_run_id, cycle DESC);

CREATE INDEX IF NOT EXISTS idx_benchmark_shadow_diffs_created
ON benchmark_shadow_diffs (created_at DESC);

COMMIT;
