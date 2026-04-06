BEGIN;

CREATE TABLE IF NOT EXISTS request_graph_runs (
  id BIGSERIAL PRIMARY KEY,
  request_id UUID NOT NULL REFERENCES fraternity_crawl_requests(id) ON DELETE CASCADE,
  worker_id TEXT NOT NULL,
  runtime_mode TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'succeeded', 'partial', 'paused', 'failed')),
  active_node TEXT,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS request_graph_events (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES request_graph_runs(id) ON DELETE CASCADE,
  request_id UUID NOT NULL REFERENCES fraternity_crawl_requests(id) ON DELETE CASCADE,
  node_name TEXT NOT NULL,
  phase TEXT NOT NULL,
  status TEXT NOT NULL,
  latency_ms INTEGER NOT NULL DEFAULT 0,
  metrics_delta JSONB NOT NULL DEFAULT '{}'::jsonb,
  diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS request_graph_checkpoints (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES request_graph_runs(id) ON DELETE CASCADE,
  request_id UUID NOT NULL REFERENCES fraternity_crawl_requests(id) ON DELETE CASCADE,
  node_name TEXT NOT NULL,
  state JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, node_name)
);

CREATE TABLE IF NOT EXISTS chapter_evidence (
  id BIGSERIAL PRIMARY KEY,
  chapter_id UUID REFERENCES chapters(id) ON DELETE CASCADE,
  chapter_slug TEXT NOT NULL,
  fraternity_slug TEXT,
  source_slug TEXT,
  request_id UUID REFERENCES fraternity_crawl_requests(id) ON DELETE SET NULL,
  crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
  field_name TEXT NOT NULL,
  candidate_value TEXT,
  confidence NUMERIC(5, 4),
  trust_tier TEXT NOT NULL DEFAULT 'medium' CHECK (trust_tier IN ('strong_official', 'high', 'medium', 'low')),
  evidence_status TEXT NOT NULL DEFAULT 'observed' CHECK (evidence_status IN ('observed', 'accepted', 'review', 'rejected', 'promoted')),
  source_url TEXT,
  source_snippet TEXT,
  provider TEXT,
  query TEXT,
  related_website_url TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS provisional_chapters (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fraternity_id UUID NOT NULL REFERENCES fraternities(id) ON DELETE CASCADE,
  source_id UUID REFERENCES sources(id) ON DELETE SET NULL,
  request_id UUID REFERENCES fraternity_crawl_requests(id) ON DELETE SET NULL,
  promoted_chapter_id UUID REFERENCES chapters(id) ON DELETE SET NULL,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  university_name TEXT,
  city TEXT,
  state TEXT,
  country TEXT NOT NULL DEFAULT 'USA',
  website_url TEXT,
  instagram_url TEXT,
  contact_email TEXT,
  status TEXT NOT NULL DEFAULT 'provisional' CHECK (status IN ('provisional', 'promoted', 'review', 'rejected')),
  promotion_reason TEXT,
  evidence_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (fraternity_id, slug)
);

CREATE TABLE IF NOT EXISTS provider_health_snapshots (
  id BIGSERIAL PRIMARY KEY,
  request_id UUID REFERENCES fraternity_crawl_requests(id) ON DELETE CASCADE,
  source_slug TEXT,
  provider TEXT NOT NULL,
  healthy BOOLEAN NOT NULL DEFAULT FALSE,
  success_rate NUMERIC(5, 4),
  probe_count INTEGER NOT NULL DEFAULT 0,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_request_graph_runs_request_created
  ON request_graph_runs (request_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_request_graph_runs_status_created
  ON request_graph_runs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_request_graph_events_run_created
  ON request_graph_events (run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_request_graph_checkpoints_run_node
  ON request_graph_checkpoints (run_id, node_name);

CREATE INDEX IF NOT EXISTS idx_chapter_evidence_chapter_created
  ON chapter_evidence (chapter_slug, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_chapter_evidence_request_created
  ON chapter_evidence (request_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_provisional_chapters_status_created
  ON provisional_chapters (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_provider_health_snapshots_request_created
  ON provider_health_snapshots (request_id, created_at DESC);

DROP TRIGGER IF EXISTS trg_request_graph_runs_updated_at ON request_graph_runs;
CREATE TRIGGER trg_request_graph_runs_updated_at
BEFORE UPDATE ON request_graph_runs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_request_graph_checkpoints_updated_at ON request_graph_checkpoints;
CREATE TRIGGER trg_request_graph_checkpoints_updated_at
BEFORE UPDATE ON request_graph_checkpoints
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_provisional_chapters_updated_at ON provisional_chapters;
CREATE TRIGGER trg_provisional_chapters_updated_at
BEFORE UPDATE ON provisional_chapters
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
