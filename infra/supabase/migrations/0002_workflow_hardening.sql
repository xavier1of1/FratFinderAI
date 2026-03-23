BEGIN;

ALTER TABLE chapters
  ADD COLUMN IF NOT EXISTS instagram_url TEXT,
  ADD COLUMN IF NOT EXISTS contact_email TEXT;

ALTER TABLE field_jobs
  ADD COLUMN IF NOT EXISTS claimed_by TEXT,
  ADD COLUMN IF NOT EXISTS claim_token UUID,
  ADD COLUMN IF NOT EXISTS completed_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS terminal_failure BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS review_item_audit_logs (
  id BIGSERIAL PRIMARY KEY,
  review_item_id UUID NOT NULL REFERENCES review_items(id) ON DELETE CASCADE,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  from_status TEXT,
  to_status TEXT,
  notes TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_review_item_audit_logs_review_item_id
ON review_item_audit_logs(review_item_id, created_at DESC);

CREATE OR REPLACE FUNCTION validate_review_status_transition()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.status = OLD.status THEN
    RETURN NEW;
  END IF;

  IF OLD.status = 'open' AND NEW.status IN ('triaged', 'ignored') THEN
    RETURN NEW;
  END IF;

  IF OLD.status = 'triaged' AND NEW.status IN ('resolved', 'ignored') THEN
    RETURN NEW;
  END IF;

  RAISE EXCEPTION 'Invalid review status transition from % to %', OLD.status, NEW.status;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_review_item_transition_guard ON review_items;
CREATE TRIGGER trg_review_item_transition_guard
BEFORE UPDATE OF status ON review_items
FOR EACH ROW EXECUTE FUNCTION validate_review_status_transition();

CREATE UNIQUE INDEX IF NOT EXISTS uq_field_jobs_active_per_chapter_field
ON field_jobs(chapter_id, field_name)
WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS idx_field_jobs_claimable
ON field_jobs(status, scheduled_at, attempts, max_attempts)
WHERE status = 'queued';

COMMIT;