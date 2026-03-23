BEGIN;

ALTER TABLE crawl_runs
  ADD COLUMN IF NOT EXISTS page_analysis JSONB,
  ADD COLUMN IF NOT EXISTS classification JSONB,
  ADD COLUMN IF NOT EXISTS extraction_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMIT;
