BEGIN;

CREATE TABLE IF NOT EXISTS search_provider_attempts (
  id BIGSERIAL PRIMARY KEY,
  context_type TEXT NOT NULL,
  context_id TEXT,
  request_id UUID REFERENCES fraternity_crawl_requests(id) ON DELETE SET NULL,
  source_slug TEXT,
  field_job_id UUID REFERENCES field_jobs(id) ON DELETE SET NULL,
  provider TEXT NOT NULL,
  provider_endpoint TEXT,
  query TEXT,
  status TEXT NOT NULL,
  failure_type TEXT,
  http_status INT,
  latency_ms INT,
  result_count INT,
  fallback_taken BOOLEAN NOT NULL DEFAULT FALSE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_search_provider_attempts_context
ON search_provider_attempts (context_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_search_provider_attempts_provider
ON search_provider_attempts (provider, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_search_provider_attempts_request
ON search_provider_attempts (request_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_search_provider_attempts_field_job
ON search_provider_attempts (field_job_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_search_provider_attempts_source
ON search_provider_attempts (source_slug, created_at DESC);

COMMIT;
