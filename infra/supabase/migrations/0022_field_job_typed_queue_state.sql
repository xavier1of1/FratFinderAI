BEGIN;

ALTER TABLE field_jobs
  ADD COLUMN IF NOT EXISTS queue_state TEXT NOT NULL DEFAULT 'actionable'
    CHECK (queue_state IN ('actionable', 'deferred', 'blocked_invalid', 'blocked_repairable')),
  ADD COLUMN IF NOT EXISTS validity_class TEXT
    CHECK (validity_class IN ('canonical_valid', 'repairable_candidate', 'provisional_candidate', 'invalid_non_chapter')),
  ADD COLUMN IF NOT EXISTS repair_state TEXT
    CHECK (repair_state IN ('queued', 'running', 'promoted_to_canonical_valid', 'downgraded_to_provisional', 'confirmed_invalid', 'repair_exhausted')),
  ADD COLUMN IF NOT EXISTS blocked_reason TEXT,
  ADD COLUMN IF NOT EXISTS terminal_outcome TEXT;

UPDATE field_jobs
SET
  queue_state = COALESCE(NULLIF(payload -> 'contactResolution' ->> 'queueState', ''), 'actionable'),
  validity_class = NULLIF(payload -> 'contactResolution' ->> 'validityClass', ''),
  repair_state = NULLIF(payload -> 'chapterRepair' ->> 'state', ''),
  blocked_reason = COALESCE(
    NULLIF(payload -> 'contactResolution' ->> 'blockedReason', ''),
    NULLIF(payload -> 'queueTriage' ->> 'reason', '')
  ),
  terminal_outcome = NULLIF(completed_payload ->> 'status', '');

CREATE INDEX IF NOT EXISTS idx_field_jobs_queue_state_claim
ON field_jobs(status, queue_state, scheduled_at, priority DESC)
WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_field_jobs_terminal_outcome
ON field_jobs(status, terminal_outcome);

COMMIT;
