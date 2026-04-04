BEGIN;

CREATE TABLE IF NOT EXISTS field_job_graph_runs (
  id BIGSERIAL PRIMARY KEY,
  worker_id TEXT NOT NULL,
  runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('legacy', 'langgraph_shadow', 'langgraph_primary')),
  source_slug TEXT,
  field_name TEXT,
  requested_limit INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'succeeded', 'failed', 'partial', 'stopped')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS field_job_graph_events (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES field_job_graph_runs(id) ON DELETE CASCADE,
  job_id UUID,
  attempt INTEGER,
  node_name TEXT NOT NULL,
  phase TEXT NOT NULL,
  status TEXT NOT NULL,
  latency_ms INTEGER NOT NULL DEFAULT 0,
  metrics_delta JSONB NOT NULL DEFAULT '{}'::jsonb,
  diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS field_job_graph_checkpoints (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES field_job_graph_runs(id) ON DELETE CASCADE,
  job_id UUID NOT NULL,
  attempt INTEGER NOT NULL,
  node_name TEXT NOT NULL,
  state JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, job_id, attempt)
);

CREATE TABLE IF NOT EXISTS field_job_graph_decisions (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES field_job_graph_runs(id) ON DELETE CASCADE,
  job_id UUID NOT NULL,
  attempt INTEGER NOT NULL,
  field_name TEXT NOT NULL,
  decision_status TEXT NOT NULL,
  confidence DOUBLE PRECISION,
  candidate_kind TEXT,
  candidate_value TEXT,
  reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  write_allowed BOOLEAN NOT NULL DEFAULT FALSE,
  requires_review BOOLEAN NOT NULL DEFAULT FALSE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_field_job_graph_runs_mode_created
  ON field_job_graph_runs (runtime_mode, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_field_job_graph_events_run_created
  ON field_job_graph_events (run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_field_job_graph_events_job_node
  ON field_job_graph_events (job_id, node_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_field_job_graph_checkpoints_run_job
  ON field_job_graph_checkpoints (run_id, job_id, attempt);

CREATE INDEX IF NOT EXISTS idx_field_job_graph_decisions_run_field
  ON field_job_graph_decisions (run_id, field_name, created_at DESC);

DROP TRIGGER IF EXISTS trg_field_job_graph_runs_updated_at ON field_job_graph_runs;
CREATE TRIGGER trg_field_job_graph_runs_updated_at
BEFORE UPDATE ON field_job_graph_runs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_field_job_graph_checkpoints_updated_at ON field_job_graph_checkpoints;
CREATE TRIGGER trg_field_job_graph_checkpoints_updated_at
BEFORE UPDATE ON field_job_graph_checkpoints
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
