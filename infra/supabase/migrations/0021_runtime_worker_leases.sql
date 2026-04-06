BEGIN;

CREATE TABLE IF NOT EXISTS worker_processes (
  worker_id TEXT PRIMARY KEY,
  workload_lane TEXT NOT NULL CHECK (workload_lane IN ('request', 'campaign', 'benchmark', 'evaluation', 'contact_resolution', 'chapter_repair')),
  runtime_owner TEXT NOT NULL,
  hostname TEXT,
  process_id INTEGER,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'idle', 'stopped', 'failed')),
  lease_expires_at TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_processes_lane_status
ON worker_processes(workload_lane, status, last_heartbeat_at DESC);

ALTER TABLE benchmark_runs
  ADD COLUMN IF NOT EXISTS runtime_worker_id TEXT REFERENCES worker_processes(worker_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS runtime_lease_token TEXT,
  ADD COLUMN IF NOT EXISTS runtime_lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS runtime_last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_runtime_lease
ON benchmark_runs(status, runtime_lease_expires_at);

ALTER TABLE campaign_runs
  ADD COLUMN IF NOT EXISTS runtime_worker_id TEXT REFERENCES worker_processes(worker_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS runtime_lease_token TEXT,
  ADD COLUMN IF NOT EXISTS runtime_lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS runtime_last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_campaign_runs_runtime_lease
ON campaign_runs(status, scheduled_for, runtime_lease_expires_at);

ALTER TABLE fraternity_crawl_requests
  ADD COLUMN IF NOT EXISTS runtime_worker_id TEXT REFERENCES worker_processes(worker_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS runtime_lease_token TEXT,
  ADD COLUMN IF NOT EXISTS runtime_lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS runtime_last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fraternity_crawl_requests_runtime_lease
ON fraternity_crawl_requests(status, scheduled_for, runtime_lease_expires_at);

DROP TRIGGER IF EXISTS trg_worker_processes_updated_at ON worker_processes;
CREATE TRIGGER trg_worker_processes_updated_at
BEFORE UPDATE ON worker_processes
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
