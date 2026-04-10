BEGIN;

ALTER TABLE chapters
  ADD COLUMN IF NOT EXISTS contact_provenance JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS national_profiles (
  fraternity_slug TEXT PRIMARY KEY REFERENCES fraternities(slug) ON DELETE CASCADE,
  fraternity_name TEXT NOT NULL,
  national_url TEXT NOT NULL,
  national_url_confidence NUMERIC(5, 4) NOT NULL DEFAULT 0.0,
  national_url_provenance_type TEXT,
  national_url_reason_code TEXT,
  contact_email TEXT,
  contact_email_confidence NUMERIC(5, 4) NOT NULL DEFAULT 0.0,
  contact_email_provenance_type TEXT,
  contact_email_reason_code TEXT,
  instagram_url TEXT,
  instagram_confidence NUMERIC(5, 4) NOT NULL DEFAULT 0.0,
  instagram_provenance_type TEXT,
  instagram_reason_code TEXT,
  phone TEXT,
  phone_confidence NUMERIC(5, 4) NOT NULL DEFAULT 0.0,
  phone_provenance_type TEXT,
  phone_reason_code TEXT,
  address_text TEXT,
  address_confidence NUMERIC(5, 4) NOT NULL DEFAULT 0.0,
  address_provenance_type TEXT,
  address_reason_code TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_national_profiles_name
  ON national_profiles(fraternity_name);

DROP TRIGGER IF EXISTS trg_national_profiles_updated_at ON national_profiles;
CREATE TRIGGER trg_national_profiles_updated_at
BEFORE UPDATE ON national_profiles
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

INSERT INTO national_profiles (
  fraternity_slug,
  fraternity_name,
  national_url,
  national_url_confidence,
  national_url_provenance_type,
  metadata
)
SELECT
  vs.fraternity_slug,
  vs.fraternity_name,
  vs.national_url,
  COALESCE(vs.confidence, 0.0),
  CASE
    WHEN COALESCE(vs.origin, '') <> '' THEN vs.origin
    ELSE 'verified_source_registry'
  END,
  jsonb_build_object(
    'httpStatus', vs.http_status,
    'checkedAt', vs.checked_at,
    'verifiedSourceOrigin', vs.origin,
    'isActive', vs.is_active,
    'verifiedSourceMetadata', COALESCE(vs.metadata, '{}'::jsonb)
  )
FROM verified_sources vs
ON CONFLICT (fraternity_slug)
DO UPDATE SET
  fraternity_name = EXCLUDED.fraternity_name,
  national_url = EXCLUDED.national_url,
  national_url_confidence = EXCLUDED.national_url_confidence,
  national_url_provenance_type = EXCLUDED.national_url_provenance_type,
  metadata = COALESCE(national_profiles.metadata, '{}'::jsonb) || EXCLUDED.metadata,
  updated_at = NOW();

COMMIT;
