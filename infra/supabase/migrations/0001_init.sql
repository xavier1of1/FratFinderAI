BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE fraternities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  nic_affiliated BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fraternity_id UUID NOT NULL REFERENCES fraternities(id) ON DELETE CASCADE,
  slug TEXT NOT NULL UNIQUE,
  source_type TEXT NOT NULL CHECK (source_type IN ('html_directory', 'json_api', 'unsupported')),
  parser_key TEXT NOT NULL,
  base_url TEXT NOT NULL,
  list_path TEXT,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sources_fraternity_id ON sources(fraternity_id);

CREATE TABLE crawl_runs (
  id BIGSERIAL PRIMARY KEY,
  source_id UUID REFERENCES sources(id) ON DELETE SET NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'partial')),
  correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  pages_processed INTEGER NOT NULL DEFAULT 0,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_upserted INTEGER NOT NULL DEFAULT 0,
  review_items_created INTEGER NOT NULL DEFAULT 0,
  field_jobs_created INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_crawl_runs_source_id_started_at ON crawl_runs(source_id, started_at DESC);
CREATE INDEX idx_crawl_runs_status ON crawl_runs(status);

CREATE TABLE chapters (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fraternity_id UUID NOT NULL REFERENCES fraternities(id) ON DELETE CASCADE,
  external_id TEXT,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  university_name TEXT,
  city TEXT,
  state TEXT,
  country TEXT NOT NULL DEFAULT 'USA',
  website_url TEXT,
  chapter_status TEXT NOT NULL DEFAULT 'active',
  normalized_address JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_chapters_fraternity_slug UNIQUE (fraternity_id, slug)
);

CREATE UNIQUE INDEX uq_chapters_fraternity_external_id
ON chapters(fraternity_id, external_id)
WHERE external_id IS NOT NULL;

CREATE INDEX idx_chapters_fraternity_id ON chapters(fraternity_id);
CREATE INDEX idx_chapters_university_name ON chapters(university_name);

CREATE TABLE chapter_provenance (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chapter_id UUID NOT NULL REFERENCES chapters(id) ON DELETE CASCADE,
  source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
  field_name TEXT NOT NULL,
  field_value TEXT,
  source_url TEXT NOT NULL,
  source_snippet TEXT,
  confidence NUMERIC(5, 4) NOT NULL DEFAULT 1.0,
  extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chapter_provenance_chapter_id ON chapter_provenance(chapter_id);
CREATE INDEX idx_chapter_provenance_source_id ON chapter_provenance(source_id);
CREATE INDEX idx_chapter_provenance_crawl_run_id ON chapter_provenance(crawl_run_id);

CREATE TABLE review_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID REFERENCES sources(id) ON DELETE SET NULL,
  crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
  chapter_id UUID REFERENCES chapters(id) ON DELETE SET NULL,
  item_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'triaged', 'resolved', 'ignored')),
  reason TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  triage_notes TEXT,
  resolved_by TEXT,
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_review_items_status ON review_items(status);
CREATE INDEX idx_review_items_source_id ON review_items(source_id);
CREATE INDEX idx_review_items_crawl_run_id ON review_items(crawl_run_id);

CREATE TABLE field_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chapter_id UUID NOT NULL REFERENCES chapters(id) ON DELETE CASCADE,
  crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
  field_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done', 'failed')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_field_jobs_status_scheduled_at ON field_jobs(status, scheduled_at);
CREATE INDEX idx_field_jobs_chapter_id ON field_jobs(chapter_id);

CREATE TRIGGER trg_fraternities_updated_at
BEFORE UPDATE ON fraternities
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_sources_updated_at
BEFORE UPDATE ON sources
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_chapters_updated_at
BEFORE UPDATE ON chapters
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_review_items_updated_at
BEFORE UPDATE ON review_items
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_field_jobs_updated_at
BEFORE UPDATE ON field_jobs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;