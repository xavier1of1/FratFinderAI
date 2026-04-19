BEGIN;

ALTER TABLE field_jobs
  DROP CONSTRAINT IF EXISTS field_jobs_queue_state_check;

ALTER TABLE field_jobs
  ADD CONSTRAINT field_jobs_queue_state_check
  CHECK (
    queue_state = ANY (
      ARRAY[
        'actionable'::text,
        'deferred'::text,
        'blocked_invalid'::text,
        'blocked_repairable'::text,
        'blocked_provider'::text,
        'blocked_dependency'::text
      ]
    )
  );

COMMIT;
