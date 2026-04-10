"""Deep analysis queries for stress run data."""
import psycopg
from psycopg.rows import dict_row
import json

RUN_ID = 'stress-20260409-full'
conn = psycopg.connect('postgresql://postgres:postgres@localhost:5433/fratfinder', row_factory=dict_row)
cur = conn.cursor()

# Provider attempts breakdown
cur.execute("""
SELECT pa->>'provider' as provider, pa->>'status' as pa_status,
  (pa->>'circuit_open')::boolean as circuit_open, count(*)::int as cnt
FROM field_jobs fj, jsonb_array_elements(fj.payload->'provider_attempts') AS pa
WHERE fj.payload->>'stressRunId'=%s AND fj.status='queued' AND fj.queue_state='deferred'
GROUP BY 1,2,3 ORDER BY cnt DESC
""", (RUN_ID,))
print('=== PROVIDER ATTEMPT BREAKDOWN ===')
for r in cur.fetchall(): print(json.dumps(r, default=str))

# Website presence correlation
cur.execute("""
SELECT CASE WHEN ch.website_url IS NOT NULL AND ch.website_url != '' THEN 'has_website' ELSE 'no_website' END as ws,
  count(distinct ch.id)::int as chapters, count(fj.id)::int as jobs,
  count(fj.id) filter(where fj.status='done')::int as done_jobs
FROM field_jobs fj JOIN chapters ch ON ch.id=fj.chapter_id
WHERE fj.payload->>'stressRunId'=%s GROUP BY 1 ORDER BY 1
""", (RUN_ID,))
print('\n=== WEBSITE PRESENCE CORRELATION ===')
for r in cur.fetchall(): print(json.dumps(r))

# Completion timeline
cur.execute("""
SELECT date_trunc('hour', fj.updated_at) as hour, count(*)::int as completed
FROM field_jobs fj
WHERE fj.payload->>'stressRunId'=%s AND fj.status='done'
GROUP BY 1 ORDER BY 1
""", (RUN_ID,))
print('\n=== COMPLETION TIMELINE ===')
for r in cur.fetchall(): print(json.dumps(r, default=str))

# Terminal no signal search attempt patterns
cur.execute("""
SELECT fj.field_name,
  coalesce((fj.completed_payload->'decision_trace'->'search'->>'attempted')::int, 0) as attempted,
  coalesce((fj.completed_payload->'decision_trace'->'search'->>'succeeded')::int, 0) as succeeded,
  count(*)::int as cnt
FROM field_jobs fj
WHERE fj.payload->>'stressRunId'=%s AND fj.status='done'
  AND fj.completed_payload->>'status'='terminal_no_signal'
GROUP BY 1,2,3 ORDER BY cnt DESC
""", (RUN_ID,))
print('\n=== TERMINAL NO SIGNAL: SEARCH ATTEMPTS ===')
for r in cur.fetchall(): print(json.dumps(r))

# Resolved from authoritative source
cur.execute("""
SELECT fj.field_name, left(fj.completed_payload->>'sourceUrl', 80) as src_url, count(*)::int as cnt
FROM field_jobs fj
WHERE fj.payload->>'stressRunId'=%s AND fj.status='done'
  AND fj.completed_payload->>'status'='resolved_from_authoritative_source'
GROUP BY 1,2 ORDER BY cnt DESC LIMIT 15
""", (RUN_ID,))
print('\n=== RESOLVED FROM AUTHORITATIVE ===')
for r in cur.fetchall(): print(json.dumps(r))

# Done by fraternity
cur.execute("""
SELECT f.slug, count(*)::int as done_cnt,
  array_agg(distinct fj.completed_payload->>'status') as outcomes
FROM field_jobs fj JOIN chapters ch ON ch.id=fj.chapter_id
LEFT JOIN fraternities f ON f.id=ch.fraternity_id
WHERE fj.payload->>'stressRunId'=%s AND fj.status='done'
GROUP BY f.slug ORDER BY done_cnt DESC
""", (RUN_ID,))
print('\n=== DONE BY FRATERNITY ===')
for r in cur.fetchall(): print(json.dumps(r, default=str))

# Deferred reason co-occurrence: how many chapters have MULTIPLE different reason codes
cur.execute("""
SELECT reason_count, count(*)::int as chapter_cnt FROM (
  SELECT fj.chapter_id, count(distinct coalesce(fj.payload->'contactResolution'->>'reasonCode','unknown')) as reason_count
  FROM field_jobs fj
  WHERE fj.payload->>'stressRunId'=%s AND fj.status='queued' AND fj.queue_state='deferred'
  GROUP BY fj.chapter_id
) sub GROUP BY 1 ORDER BY 1
""", (RUN_ID,))
print('\n=== REASON CODE DIVERSITY PER CHAPTER ===')
for r in cur.fetchall(): print(json.dumps(r))

# Cross-tabulation: identity_semantically_incomplete vs queued_for_entity_repair overlap
cur.execute("""
SELECT 
  count(distinct fj.chapter_id) filter(
    where fj.payload->'contactResolution'->>'reasonCode' = 'identity_semantically_incomplete'
  )::int as identity_incomplete_chapters,
  count(distinct fj.chapter_id) filter(
    where fj.payload->'contactResolution'->>'reasonCode' = 'queued_for_entity_repair'
  )::int as repair_queued_chapters,
  count(distinct fj.chapter_id) filter(
    where fj.payload->'contactResolution'->>'reasonCode' = 'provider_degraded'
  )::int as provider_degraded_chapters
FROM field_jobs fj
WHERE fj.payload->>'stressRunId'=%s AND fj.status='queued' AND fj.queue_state='deferred'
""", (RUN_ID,))
print('\n=== UNIQUE CHAPTER COUNT BY TOP REASON ===')
for r in cur.fetchall(): print(json.dumps(r))

conn.close()
