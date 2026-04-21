BEGIN;

CREATE TABLE IF NOT EXISTS campus_status_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  school_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_host TEXT NOT NULL,
  source_type TEXT NOT NULL,
  authority_tier INT NOT NULL CHECK (authority_tier >= 0 AND authority_tier <= 9),
  currentness_score NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  completeness_score NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  parse_completeness_score NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  is_official_school_source BOOLEAN NOT NULL DEFAULT FALSE,
  last_fetched_at TIMESTAMPTZ,
  content_hash TEXT,
  title TEXT NOT NULL DEFAULT '',
  text_excerpt TEXT NOT NULL DEFAULT '',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (school_name, source_url)
);

CREATE INDEX IF NOT EXISTS idx_campus_status_sources_school_name
ON campus_status_sources (school_name);

CREATE INDEX IF NOT EXISTS idx_campus_status_sources_source_type
ON campus_status_sources (source_type);

CREATE TABLE IF NOT EXISTS campus_status_zones (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  campus_status_source_id UUID NOT NULL REFERENCES campus_status_sources(id) ON DELETE CASCADE,
  zone_type TEXT NOT NULL,
  zone_heading TEXT,
  dom_path TEXT,
  zone_text TEXT NOT NULL DEFAULT '',
  links JSONB NOT NULL DEFAULT '[]'::jsonb,
  confidence NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  parser_version TEXT NOT NULL DEFAULT 'campus_status_v1',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_campus_status_zones_source
ON campus_status_zones (campus_status_source_id);

CREATE INDEX IF NOT EXISTS idx_campus_status_zones_type
ON campus_status_zones (zone_type);

CREATE TABLE IF NOT EXISTS chapter_status_evidence (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chapter_id UUID REFERENCES chapters(id) ON DELETE CASCADE,
  fraternity_name TEXT NOT NULL,
  school_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  authority_tier INT NOT NULL,
  evidence_type TEXT NOT NULL,
  status_signal TEXT NOT NULL,
  matched_text TEXT,
  matched_alias TEXT,
  zone_type TEXT,
  match_confidence NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  evidence_confidence NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chapter_status_evidence_chapter
ON chapter_status_evidence (chapter_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_chapter_status_evidence_signal
ON chapter_status_evidence (status_signal);

CREATE TABLE IF NOT EXISTS chapter_status_decisions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chapter_id UUID NOT NULL REFERENCES chapters(id) ON DELETE CASCADE,
  final_status TEXT NOT NULL,
  school_recognition_status TEXT NOT NULL,
  national_status TEXT,
  confidence NUMERIC(5,4) NOT NULL DEFAULT 0.0,
  reason_code TEXT NOT NULL,
  conflict_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
  decision_trace JSONB NOT NULL DEFAULT '{}'::jsonb,
  review_required BOOLEAN NOT NULL DEFAULT FALSE,
  decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chapter_status_decisions_chapter
ON chapter_status_decisions (chapter_id, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_chapter_status_decisions_final_status
ON chapter_status_decisions (final_status);

COMMIT;
