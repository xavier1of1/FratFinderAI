BEGIN;

UPDATE verified_sources
SET national_url = 'https://sigmachi.org/chapters/',
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'manual_backfill', 'benchmark_response_2026-04-01',
      'selected_reason', 'manual_chapter_directory_override',
      'final_url', 'https://sigmachi.org/chapters/'
    ),
    checked_at = NOW(),
    updated_at = NOW()
WHERE fraternity_slug = 'sigma-chi';

UPDATE sources s
SET base_url = 'https://sigmachi.org',
    list_path = 'https://sigmachi.org/chapters/',
    metadata = COALESCE(s.metadata, '{}'::jsonb) || jsonb_build_object(
      'manualBackfill', 'benchmark_response_2026-04-01',
      'extractionHints', jsonb_build_object(
        'chapterIndexMode', 'direct_chapter_list',
        'primaryStrategy', 'repeated_block',
        'fallbackStrategies', jsonb_build_array('table', 'script_json', 'review')
      )
    )
FROM fraternities f
WHERE f.id = s.fraternity_id
  AND f.slug = 'sigma-chi';

UPDATE sources s
SET metadata = COALESCE(s.metadata, '{}'::jsonb) || jsonb_build_object(
      'manualBackfill', 'benchmark_response_2026-04-01',
      'extractionHints', jsonb_build_object(
        'chapterIndexMode', 'direct_chapter_list',
        'primaryStrategy', 'repeated_block',
        'fallbackStrategies', jsonb_build_array('table', 'review'),
        'stubStrategies', jsonb_build_array('repeated_block', 'table'),
        'directorySelectors', jsonb_build_object(
          'cardSelectors', jsonb_build_array('.grid-item .card', '.card.h-100'),
          'nameSelectors', jsonb_build_array('.card-title a', '.card-title')
        )
      )
    )
FROM fraternities f
WHERE f.id = s.fraternity_id
  AND f.slug = 'kappa-delta-rho';

COMMIT;
