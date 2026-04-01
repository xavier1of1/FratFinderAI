BEGIN;

CREATE TABLE IF NOT EXISTS verified_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fraternity_slug TEXT NOT NULL UNIQUE,
  fraternity_name TEXT NOT NULL,
  national_url TEXT NOT NULL,
  origin TEXT NOT NULL,
  confidence NUMERIC(5, 4) NOT NULL DEFAULT 0.0,
  http_status INTEGER,
  checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_verified_sources_fraternity_slug
ON verified_sources(fraternity_slug);

CREATE INDEX IF NOT EXISTS idx_verified_sources_is_active
ON verified_sources(is_active);

CREATE INDEX IF NOT EXISTS idx_verified_sources_checked_at_desc
ON verified_sources(checked_at DESC);

DROP TRIGGER IF EXISTS trg_verified_sources_updated_at ON verified_sources;
CREATE TRIGGER trg_verified_sources_updated_at
BEFORE UPDATE ON verified_sources
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
