CREATE TABLE IF NOT EXISTS evaluation_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_kind TEXT NOT NULL CHECK (job_kind IN ('benchmark_run', 'campaign_run')),
  status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'canceled')),
  benchmark_run_id UUID REFERENCES benchmark_runs(id) ON DELETE CASCADE,
  campaign_run_id UUID REFERENCES campaign_runs(id) ON DELETE CASCADE,
  source_slug TEXT,
  evaluation_phase TEXT,
  isolation_mode TEXT NOT NULL DEFAULT 'shared_live_observed'
    CHECK (isolation_mode IN ('shared_live_observed', 'strict_live_isolated')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  preconditions JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempts INTEGER NOT NULL DEFAULT 0,
  priority INTEGER NOT NULL DEFAULT 100,
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  last_error TEXT,
  runtime_worker_id TEXT,
  runtime_lease_token TEXT,
  runtime_lease_expires_at TIMESTAMPTZ,
  runtime_last_heartbeat_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (
    (benchmark_run_id IS NOT NULL AND campaign_run_id IS NULL)
    OR (benchmark_run_id IS NULL AND campaign_run_id IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_evaluation_jobs_claim
ON evaluation_jobs (status, scheduled_at ASC, priority ASC, runtime_lease_expires_at ASC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_evaluation_jobs_benchmark
ON evaluation_jobs (benchmark_run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_evaluation_jobs_campaign
ON evaluation_jobs (campaign_run_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_evaluation_jobs_active_benchmark
ON evaluation_jobs (benchmark_run_id)
WHERE benchmark_run_id IS NOT NULL AND status IN ('queued', 'running');

CREATE UNIQUE INDEX IF NOT EXISTS idx_evaluation_jobs_active_campaign
ON evaluation_jobs (campaign_run_id)
WHERE campaign_run_id IS NOT NULL AND status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS idx_evaluation_jobs_isolation
ON evaluation_jobs (job_kind, isolation_mode, source_slug, status, created_at DESC);

DROP TRIGGER IF EXISTS trg_evaluation_jobs_updated_at ON evaluation_jobs;
CREATE TRIGGER trg_evaluation_jobs_updated_at
BEFORE UPDATE ON evaluation_jobs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
