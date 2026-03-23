BEGIN;

ALTER TABLE sources
  DROP CONSTRAINT IF EXISTS sources_source_type_check;

ALTER TABLE sources
  ADD CONSTRAINT sources_source_type_check
  CHECK (source_type IN ('html_directory', 'json_api', 'unsupported', 'script_embedded', 'locator_api'));

COMMIT;
