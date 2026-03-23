-- Run after migrations/seeds to verify core schema assumptions.

SELECT COUNT(*) AS fraternity_count FROM fraternities;
SELECT COUNT(*) AS source_count FROM sources;

-- Ensures status check constraints accept canonical values.
INSERT INTO crawl_runs (source_id, status)
SELECT id, 'pending' FROM sources LIMIT 1;

SELECT status, COUNT(*)
FROM crawl_runs
GROUP BY status;

-- Review transition guard and audit table smoke.
DO $$
DECLARE
  src_id UUID;
  run_id BIGINT;
  review_id UUID;
BEGIN
  SELECT id INTO src_id FROM sources LIMIT 1;
  INSERT INTO crawl_runs (source_id, status) VALUES (src_id, 'running') RETURNING id INTO run_id;

  INSERT INTO review_items (source_id, crawl_run_id, item_type, reason, status)
  VALUES (src_id, run_id, 'smoke', 'smoke transition', 'open')
  RETURNING id INTO review_id;

  UPDATE review_items SET status = 'triaged' WHERE id = review_id;

  INSERT INTO review_item_audit_logs (review_item_id, actor, action, from_status, to_status, notes)
  VALUES (review_id, 'smoke-test', 'status_transition', 'open', 'triaged', 'smoke');

  DELETE FROM review_item_audit_logs WHERE review_item_id = review_id;
  DELETE FROM review_items WHERE id = review_id;
  DELETE FROM crawl_runs WHERE id = run_id;
END $$;

-- Cleanup smoke insert.
DELETE FROM crawl_runs WHERE status = 'pending' AND records_seen = 0 AND records_upserted = 0;