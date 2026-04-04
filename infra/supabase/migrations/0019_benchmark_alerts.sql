BEGIN;

CREATE TABLE IF NOT EXISTS benchmark_alerts (
  id BIGSERIAL PRIMARY KEY,
  benchmark_run_id UUID REFERENCES benchmark_runs(id) ON DELETE SET NULL,
  alert_type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
  message TEXT NOT NULL,
  fingerprint TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_benchmark_alerts_created
ON benchmark_alerts (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_benchmark_alerts_status
ON benchmark_alerts (status, severity, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_benchmark_alerts_benchmark
ON benchmark_alerts (benchmark_run_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_benchmark_alerts_open_fingerprint
ON benchmark_alerts (fingerprint)
WHERE status = 'open' AND fingerprint IS NOT NULL;

DROP TRIGGER IF EXISTS trg_benchmark_alerts_updated_at ON benchmark_alerts;
CREATE TRIGGER trg_benchmark_alerts_updated_at
BEFORE UPDATE ON benchmark_alerts
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
