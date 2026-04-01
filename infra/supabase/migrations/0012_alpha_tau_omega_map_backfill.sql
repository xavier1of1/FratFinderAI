BEGIN;

UPDATE verified_sources
SET national_url = 'https://ato.org/home-2/ato-map/',
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'manual_backfill', 'benchmark_response_ato_mapdata_2026-04-01',
      'selected_reason', 'official_ato_map_payload',
      'final_url', 'https://ato.org/home-2/ato-map/'
    ),
    checked_at = NOW(),
    updated_at = NOW()
WHERE fraternity_slug = 'alpha-tau-omega';

UPDATE sources s
SET base_url = 'https://ato.org',
    list_path = 'https://ato.org/home-2/ato-map/',
    metadata = COALESCE(s.metadata, '{}'::jsonb) || jsonb_build_object(
      'manualBackfill', 'benchmark_response_ato_mapdata_2026-04-01',
      'extractionHints', jsonb_build_object(
        'chapterIndexMode', 'direct_chapter_list',
        'primaryStrategy', 'script_json',
        'fallbackStrategies', jsonb_build_array('review')
      )
    )
FROM fraternities f
WHERE f.id = s.fraternity_id
  AND f.slug = 'alpha-tau-omega';

COMMIT;
