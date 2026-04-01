BEGIN;

UPDATE verified_sources
SET national_url = 'https://chipsi.org/where-we-are/',
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'manual_backfill', 'benchmark_response_targeted_validation_2026-04-01',
      'selected_reason', 'official_where_we_are_directory',
      'final_url', 'https://chipsi.org/where-we-are/'
    ),
    checked_at = NOW(),
    updated_at = NOW()
WHERE fraternity_slug = 'chi-psi';

UPDATE sources s
SET base_url = 'https://chipsi.org',
    list_path = 'https://chipsi.org/where-we-are/',
    metadata = COALESCE(s.metadata, '{}'::jsonb) || jsonb_build_object(
      'manualBackfill', 'benchmark_response_targeted_validation_2026-04-01',
      'extractionHints', jsonb_build_object(
        'chapterIndexMode', 'direct_chapter_list',
        'primaryStrategy', 'table',
        'fallbackStrategies', jsonb_build_array('repeated_block', 'review'),
        'stubStrategies', jsonb_build_array('table', 'repeated_block')
      )
    )
FROM fraternities f
WHERE f.id = s.fraternity_id
  AND f.slug = 'chi-psi';

COMMIT;
