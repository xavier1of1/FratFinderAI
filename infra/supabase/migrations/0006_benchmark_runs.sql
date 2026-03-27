BEGIN;

CREATE TABLE IF NOT EXISTS benchmark_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
  target_field_name TEXT NOT NULL CHECK (target_field_name IN ('find_website', 'find_email', 'find_instagram', 'all')),
  source_slug TEXT,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary JSONB,
  samples JSONB NOT NULL DEFAULT '[]'::jsonb,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_created_at
ON benchmark_runs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_status
ON benchmark_runs(status);

DROP TRIGGER IF EXISTS trg_benchmark_runs_updated_at ON benchmark_runs;
CREATE TRIGGER trg_benchmark_runs_updated_at
BEFORE UPDATE ON benchmark_runs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;