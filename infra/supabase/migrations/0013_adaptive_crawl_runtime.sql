BEGIN;

CREATE TABLE IF NOT EXISTS crawl_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
  source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('legacy', 'adaptive_shadow', 'adaptive_assisted', 'adaptive_primary')),
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'succeeded', 'partial', 'failed', 'stopped')),
  seed_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
  budget_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  stop_reason TEXT,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_crawl_sessions_run_id ON crawl_sessions(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_sessions_source_mode ON crawl_sessions(source_id, runtime_mode, created_at DESC);

CREATE TABLE IF NOT EXISTS crawl_frontier_items (
  id BIGSERIAL PRIMARY KEY,
  crawl_session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  parent_url TEXT,
  depth INTEGER NOT NULL DEFAULT 0,
  anchor_text TEXT,
  discovered_from TEXT NOT NULL DEFAULT 'seed',
  state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'visited', 'skipped', 'dead')),
  score_total DOUBLE PRECISION NOT NULL DEFAULT 0,
  score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
  selected_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (crawl_session_id, canonical_url)
);

CREATE INDEX IF NOT EXISTS idx_crawl_frontier_items_next ON crawl_frontier_items(crawl_session_id, state, score_total DESC, depth ASC, created_at ASC);

CREATE TABLE IF NOT EXISTS crawl_page_observations (
  id BIGSERIAL PRIMARY KEY,
  crawl_session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  template_signature TEXT NOT NULL,
  http_status INTEGER,
  latency_ms INTEGER NOT NULL DEFAULT 0,
  page_analysis JSONB NOT NULL DEFAULT '{}'::jsonb,
  classification JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedded_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  candidate_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  selected_action TEXT,
  selected_action_score DOUBLE PRECISION,
  selected_action_score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
  outcome JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_page_observations_session ON crawl_page_observations(crawl_session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crawl_page_observations_signature ON crawl_page_observations(template_signature, created_at DESC);

CREATE TABLE IF NOT EXISTS crawl_reward_events (
  id BIGSERIAL PRIMARY KEY,
  crawl_session_id UUID NOT NULL REFERENCES crawl_sessions(id) ON DELETE CASCADE,
  page_observation_id BIGINT REFERENCES crawl_page_observations(id) ON DELETE SET NULL,
  action_type TEXT NOT NULL,
  reward_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  reward_components JSONB NOT NULL DEFAULT '{}'::jsonb,
  delayed BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_reward_events_session ON crawl_reward_events(crawl_session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crawl_reward_events_action ON crawl_reward_events(action_type, created_at DESC);

CREATE TABLE IF NOT EXISTS crawl_template_profiles (
  template_signature TEXT NOT NULL,
  host_family TEXT NOT NULL,
  page_role_guess TEXT,
  best_action_family TEXT,
  best_extraction_family TEXT,
  visit_count INTEGER NOT NULL DEFAULT 0,
  chapter_yield DOUBLE PRECISION NOT NULL DEFAULT 0,
  contact_yield DOUBLE PRECISION NOT NULL DEFAULT 0,
  empty_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
  timeout_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (template_signature, host_family)
);

CREATE TABLE IF NOT EXISTS crawl_policy_snapshots (
  id BIGSERIAL PRIMARY KEY,
  policy_version TEXT NOT NULL,
  runtime_mode TEXT NOT NULL,
  feature_schema_version TEXT NOT NULL,
  model_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_crawl_sessions_updated_at ON crawl_sessions;
CREATE TRIGGER trg_crawl_sessions_updated_at
BEFORE UPDATE ON crawl_sessions
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_crawl_frontier_items_updated_at ON crawl_frontier_items;
CREATE TRIGGER trg_crawl_frontier_items_updated_at
BEFORE UPDATE ON crawl_frontier_items
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
