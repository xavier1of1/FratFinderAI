CREATE TABLE IF NOT EXISTS ops_alerts (
  id BIGSERIAL PRIMARY KEY,
  alert_scope TEXT NOT NULL CHECK (alert_scope IN ('benchmark', 'campaign', 'queue', 'repair', 'provider', 'system')),
  alert_type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
  benchmark_run_id UUID REFERENCES benchmark_runs(id) ON DELETE SET NULL,
  campaign_run_id UUID REFERENCES campaign_runs(id) ON DELETE SET NULL,
  request_id UUID REFERENCES fraternity_crawl_requests(id) ON DELETE SET NULL,
  source_slug TEXT,
  message TEXT NOT NULL,
  fingerprint TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ops_alerts_status
ON ops_alerts (status, severity, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ops_alerts_scope
ON ops_alerts (alert_scope, status, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_alerts_open_fingerprint
ON ops_alerts (fingerprint)
WHERE status = 'open' AND fingerprint IS NOT NULL;

DROP TRIGGER IF EXISTS trg_ops_alerts_updated_at ON ops_alerts;
CREATE TRIGGER trg_ops_alerts_updated_at
BEFORE UPDATE ON ops_alerts
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
