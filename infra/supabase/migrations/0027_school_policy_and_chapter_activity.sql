BEGIN;

CREATE TABLE IF NOT EXISTS school_greek_life_registry (
  school_slug TEXT PRIMARY KEY,
  school_name TEXT NOT NULL,
  greek_life_status TEXT NOT NULL DEFAULT 'unknown' CHECK (greek_life_status IN ('unknown', 'allowed', 'banned')),
  confidence NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  evidence_url TEXT,
  evidence_source_type TEXT,
  reason_code TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_school_greek_life_registry_status
ON school_greek_life_registry(greek_life_status);

CREATE TABLE IF NOT EXISTS fraternity_school_activity_cache (
  fraternity_slug TEXT NOT NULL,
  school_slug TEXT NOT NULL,
  school_name TEXT NOT NULL,
  chapter_activity_status TEXT NOT NULL CHECK (chapter_activity_status IN ('confirmed_active', 'confirmed_inactive')),
  confidence NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  evidence_url TEXT,
  evidence_source_type TEXT,
  reason_code TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (fraternity_slug, school_slug)
);

CREATE INDEX IF NOT EXISTS idx_fraternity_school_activity_status
ON fraternity_school_activity_cache(chapter_activity_status);

DROP TRIGGER IF EXISTS trg_school_greek_life_registry_updated_at ON school_greek_life_registry;
CREATE TRIGGER trg_school_greek_life_registry_updated_at
BEFORE UPDATE ON school_greek_life_registry
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_fraternity_school_activity_cache_updated_at ON fraternity_school_activity_cache;
CREATE TRIGGER trg_fraternity_school_activity_cache_updated_at
BEFORE UPDATE ON fraternity_school_activity_cache
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
