BEGIN;

INSERT INTO fraternities (slug, name)
VALUES
  ('beta-theta-pi', 'Beta Theta Pi'),
  ('sigma-chi', 'Sigma Chi')
ON CONFLICT (slug) DO NOTHING;

INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, metadata)
SELECT id, 'beta-theta-pi-main', 'html_directory', 'directory_v1', 'https://www.betathetapi.org', '/chapters', '{"notes":"primary chapter directory"}'::jsonb
FROM fraternities
WHERE slug = 'beta-theta-pi'
ON CONFLICT (slug) DO NOTHING;

INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, metadata)
SELECT id, 'sigma-chi-main', 'html_directory', 'directory_v1', 'https://sigmachi.org', '/home/what-is-sigma-chi/undergraduate-groups/', '{"notes":"primary chapter directory"}'::jsonb
FROM fraternities
WHERE slug = 'sigma-chi'
ON CONFLICT (slug) DO NOTHING;

COMMIT;
