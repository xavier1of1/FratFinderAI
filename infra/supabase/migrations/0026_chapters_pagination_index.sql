BEGIN;

CREATE INDEX IF NOT EXISTS idx_chapters_updated_at_id_desc
  ON chapters (updated_at DESC, id DESC);

COMMIT;
