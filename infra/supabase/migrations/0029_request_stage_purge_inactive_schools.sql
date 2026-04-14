BEGIN;

ALTER TABLE fraternity_crawl_requests
  DROP CONSTRAINT IF EXISTS fraternity_crawl_requests_stage_check;

ALTER TABLE fraternity_crawl_requests
  ADD CONSTRAINT fraternity_crawl_requests_stage_check
  CHECK (
    stage IN (
      'discovery',
      'awaiting_confirmation',
      'crawl_run',
      'purge_inactive_schools',
      'enrichment',
      'completed',
      'failed'
    )
  );

COMMIT;
