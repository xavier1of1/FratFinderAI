BEGIN;

CREATE TABLE IF NOT EXISTS search_result_cache (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  provider_endpoint TEXT NOT NULL DEFAULT '',
  engine_profile TEXT NOT NULL DEFAULT '',
  engines TEXT NOT NULL DEFAULT '',
  query_hash TEXT NOT NULL,
  normalized_query TEXT NOT NULL,
  max_results INT NOT NULL,
  results JSONB NOT NULL,
  result_count INT NOT NULL DEFAULT 0,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_search_result_cache_key
ON search_result_cache (
  provider,
  provider_endpoint,
  engine_profile,
  engines,
  query_hash,
  max_results
);

CREATE INDEX IF NOT EXISTS idx_search_result_cache_expires
ON search_result_cache (expires_at);

CREATE TABLE IF NOT EXISTS search_provider_admission_state (
  provider TEXT NOT NULL,
  provider_endpoint TEXT NOT NULL DEFAULT '',
  in_flight INT NOT NULL DEFAULT 0,
  next_allowed_at TIMESTAMPTZ,
  cooldown_until TIMESTAMPTZ,
  failure_count INT NOT NULL DEFAULT 0,
  success_count INT NOT NULL DEFAULT 0,
  last_failure_type TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (provider, provider_endpoint)
);

CREATE TABLE IF NOT EXISTS search_preflight_cache (
  cache_key TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  healthy BOOLEAN NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_search_preflight_cache_expires
ON search_preflight_cache (expires_at);

COMMIT;
