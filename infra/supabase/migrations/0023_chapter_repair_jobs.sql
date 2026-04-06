BEGIN;

CREATE TABLE IF NOT EXISTS chapter_repair_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chapter_id UUID NOT NULL REFERENCES chapters(id) ON DELETE CASCADE,
  source_slug TEXT,
  status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done', 'failed')),
  repair_state TEXT NOT NULL DEFAULT 'queued'
    CHECK (repair_state IN ('queued', 'running', 'promoted_to_canonical_valid', 'downgraded_to_provisional', 'confirmed_invalid', 'repair_exhausted')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  claimed_by TEXT,
  claim_token UUID,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chapter_repair_jobs_status_scheduled
ON chapter_repair_jobs(status, scheduled_at, priority DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chapter_repair_jobs_active_chapter
ON chapter_repair_jobs(chapter_id)
WHERE status IN ('queued', 'running');

DROP TRIGGER IF EXISTS trg_chapter_repair_jobs_updated_at ON chapter_repair_jobs;
CREATE TRIGGER trg_chapter_repair_jobs_updated_at
BEFORE UPDATE ON chapter_repair_jobs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
