BEGIN;

INSERT INTO fraternities (slug, name, nic_affiliated)
VALUES
  ('delta-chi', 'Delta Chi', TRUE),
  ('kappa-alpha-psi', 'Kappa Alpha Psi', FALSE)
ON CONFLICT (slug) DO UPDATE
SET name = EXCLUDED.name,
    nic_affiliated = EXCLUDED.nic_affiliated,
    updated_at = NOW();

INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, active, metadata)
SELECT id,
       'delta-chi-curated-demo',
       'unsupported',
       'unsupported',
       'https://deltachi.org',
       '/',
       FALSE,
       '{"notes":"Curated starter data from official university and chapter pages for the demo."}'::jsonb
FROM fraternities
WHERE slug = 'delta-chi'
ON CONFLICT (slug) DO UPDATE
SET source_type = EXCLUDED.source_type,
    parser_key = EXCLUDED.parser_key,
    base_url = EXCLUDED.base_url,
    list_path = EXCLUDED.list_path,
    active = EXCLUDED.active,
    metadata = EXCLUDED.metadata,
    updated_at = NOW();

INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, active, metadata)
SELECT id,
       'kappa-alpha-psi-curated-demo',
       'unsupported',
       'unsupported',
       'https://www.kappaalphapsi1911.com',
       '/',
       FALSE,
       '{"notes":"Curated starter data from official university, chapter, and province pages for the demo."}'::jsonb
FROM fraternities
WHERE slug = 'kappa-alpha-psi'
ON CONFLICT (slug) DO UPDATE
SET source_type = EXCLUDED.source_type,
    parser_key = EXCLUDED.parser_key,
    base_url = EXCLUDED.base_url,
    list_path = EXCLUDED.list_path,
    active = EXCLUDED.active,
    metadata = EXCLUDED.metadata,
    updated_at = NOW();

WITH chapter_seed(fraternity_slug, chapter_slug, chapter_name, university_name, city, state, website_url, instagram_url, contact_email) AS (
    VALUES
      ('delta-chi', 'florida-chapter-university-of-florida', 'Florida Chapter', 'University of Florida', 'Gainesville', 'FL', 'https://www.ufdeltachi.org/', 'https://www.instagram.com/ufdeltachi/', 'gatordeltachi@gmail.com'),
      ('delta-chi', 'tennessee-chapter-university-of-tennessee-knoxville', 'Tennessee Chapter', 'University of Tennessee, Knoxville', 'Knoxville', 'TN', NULL, 'https://www.instagram.com/utkdeltachi/', NULL),
      ('delta-chi', 'alpha-chapter-cornell-university', 'Alpha Chapter', 'Cornell University', 'Ithaca', 'NY', 'https://cornelldeltachi.com/', 'https://www.instagram.com/deltachicornell', NULL),
      ('delta-chi', 'william-and-mary-chapter-william-and-mary', 'William & Mary Chapter', 'William & Mary', 'Williamsburg', 'VA', 'https://www.wmdeltachi.com/', 'https://www.instagram.com/wmdeltachi/', NULL),
      ('delta-chi', 'auburn-university-chapter-auburn-university', 'Auburn University Chapter', 'Auburn University', 'Auburn', 'AL', 'https://www.auburndeltachi.com/', 'https://www.instagram.com/auburndeltachi/', NULL),
      ('delta-chi', 'illinois-chapter-university-of-illinois-urbana-champaign', 'Illinois Chapter', 'University of Illinois Urbana-Champaign', 'Champaign', 'IL', 'https://one.illinois.edu/DeltaChi/', NULL, NULL),
      ('delta-chi', 'tri-state-chapter-trine-university', 'Tri-State Chapter', 'Trine University', 'Angola', 'IN', 'https://tristatedeltachi.org/', 'https://www.instagram.com/deltachitrine/', NULL),
      ('kappa-alpha-psi', 'omicron-gamma-florida-atlantic-university', 'Omicron Gamma', 'Florida Atlantic University', 'Boca Raton', 'FL', NULL, 'https://www.instagram.com/og_nupes/', 'ognupes@gmail.com'),
      ('kappa-alpha-psi', 'alpha-iota-morgan-state-university', 'Alpha Iota', 'Morgan State University', 'Baltimore', 'MD', 'https://ainupes1931.com/', NULL, NULL),
      ('kappa-alpha-psi', 'alpha-omicron-university-of-louisville', 'Alpha Omicron', 'University of Louisville', 'Louisville', 'KY', 'https://aokappaalphapsi.wixsite.com/alphaomicronchapter', NULL, NULL),
      ('kappa-alpha-psi', 'alpha-theta-tennessee-state-university', 'Alpha Theta', 'Tennessee State University', 'Nashville', 'TN', 'https://atng1931.weebly.com/', NULL, NULL),
      ('kappa-alpha-psi', 'epsilon-rho-western-kentucky-university', 'Epsilon Rho', 'Western Kentucky University', 'Bowling Green', 'KY', 'https://orgs.wku.edu/epnupes/', NULL, NULL),
      ('kappa-alpha-psi', 'eta-gamma-middle-tennessee-state-university', 'Eta Gamma', 'Middle Tennessee State University', 'Murfreesboro', 'TN', 'https://www.egnupes.com/', NULL, NULL),
      ('kappa-alpha-psi', 'mu-rho-university-of-tennessee-knoxville', 'Mu Rho', 'University of Tennessee, Knoxville', 'Knoxville', 'TN', NULL, 'https://www.instagram.com/murhonupes/', NULL),
      ('kappa-alpha-psi', 'theta-beta-austin-peay-state-university', 'Theta Beta', 'Austin Peay State University', 'Clarksville', 'TN', NULL, 'https://www.instagram.com/thetabeta_nupes/', NULL)
)
INSERT INTO chapters (
    fraternity_id,
    slug,
    name,
    university_name,
    city,
    state,
    website_url,
    instagram_url,
    contact_email,
    chapter_status,
    field_states
)
SELECT
    f.id,
    cs.chapter_slug,
    cs.chapter_name,
    cs.university_name,
    cs.city,
    cs.state,
    cs.website_url,
    cs.instagram_url,
    cs.contact_email,
    'active',
    jsonb_build_object(
        'university_name', CASE WHEN cs.university_name IS NOT NULL THEN 'found' ELSE 'missing' END,
        'city', CASE WHEN cs.city IS NOT NULL THEN 'found' ELSE 'missing' END,
        'state', CASE WHEN cs.state IS NOT NULL THEN 'found' ELSE 'missing' END,
        'website_url', CASE WHEN cs.website_url IS NOT NULL THEN 'found' ELSE 'missing' END,
        'instagram_url', CASE WHEN cs.instagram_url IS NOT NULL THEN 'found' ELSE 'missing' END,
        'contact_email', CASE WHEN cs.contact_email IS NOT NULL THEN 'found' ELSE 'missing' END
    )
FROM chapter_seed cs
JOIN fraternities f ON f.slug = cs.fraternity_slug
ON CONFLICT (fraternity_id, slug) DO UPDATE
SET name = EXCLUDED.name,
    university_name = EXCLUDED.university_name,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    website_url = COALESCE(EXCLUDED.website_url, chapters.website_url),
    instagram_url = COALESCE(EXCLUDED.instagram_url, chapters.instagram_url),
    contact_email = COALESCE(EXCLUDED.contact_email, chapters.contact_email),
    chapter_status = EXCLUDED.chapter_status,
    field_states = EXCLUDED.field_states,
    last_seen_at = NOW(),
    updated_at = NOW();

WITH field_seed(fraternity_slug, source_slug, chapter_slug, field_name, field_value, source_url, source_snippet, confidence) AS (
    VALUES
      ('delta-chi', 'delta-chi-curated-demo', 'florida-chapter-university-of-florida', 'website_url', 'https://www.ufdeltachi.org/', 'https://www.ufdeltachi.org/', 'Official chapter website with recruitment contact and social links.', 0.99),
      ('delta-chi', 'delta-chi-curated-demo', 'florida-chapter-university-of-florida', 'instagram_url', 'https://www.instagram.com/ufdeltachi/', 'https://www.ufdeltachi.org/', 'Official chapter website with recruitment contact and social links.', 0.98),
      ('delta-chi', 'delta-chi-curated-demo', 'florida-chapter-university-of-florida', 'contact_email', 'gatordeltachi@gmail.com', 'https://www.ufdeltachi.org/', 'Official chapter website with recruitment contact and social links.', 0.98),
      ('delta-chi', 'delta-chi-curated-demo', 'tennessee-chapter-university-of-tennessee-knoxville', 'instagram_url', 'https://www.instagram.com/utkdeltachi/', 'https://fsl.utk.edu/organizations/delta-chi/', 'Official university Greek life page listing the chapter and Instagram.', 0.98),
      ('delta-chi', 'delta-chi-curated-demo', 'alpha-chapter-cornell-university', 'website_url', 'https://cornelldeltachi.com/', 'https://cornelldeltachi.com/', 'Official chapter website with chapter social links.', 0.99),
      ('delta-chi', 'delta-chi-curated-demo', 'alpha-chapter-cornell-university', 'instagram_url', 'https://www.instagram.com/deltachicornell', 'https://cornelldeltachi.com/', 'Official chapter website with chapter social links.', 0.98),
      ('delta-chi', 'delta-chi-curated-demo', 'william-and-mary-chapter-william-and-mary', 'website_url', 'https://www.wmdeltachi.com/', 'https://www.wmdeltachi.com/', 'Official chapter website with chapter social links.', 0.99),
      ('delta-chi', 'delta-chi-curated-demo', 'william-and-mary-chapter-william-and-mary', 'instagram_url', 'https://www.instagram.com/wmdeltachi/', 'https://www.wmdeltachi.com/', 'Official chapter website with chapter social links.', 0.98),
      ('delta-chi', 'delta-chi-curated-demo', 'auburn-university-chapter-auburn-university', 'website_url', 'https://www.auburndeltachi.com/', 'https://www.auburndeltachi.com/', 'Official chapter website with chapter social links.', 0.99),
      ('delta-chi', 'delta-chi-curated-demo', 'auburn-university-chapter-auburn-university', 'instagram_url', 'https://www.instagram.com/auburndeltachi/', 'https://www.auburndeltachi.com/', 'Official chapter website with chapter social links.', 0.98),
      ('delta-chi', 'delta-chi-curated-demo', 'illinois-chapter-university-of-illinois-urbana-champaign', 'website_url', 'https://one.illinois.edu/DeltaChi/', 'https://one.illinois.edu/DeltaChi/', 'Official campus organization page for the chapter.', 0.97),
      ('delta-chi', 'delta-chi-curated-demo', 'tri-state-chapter-trine-university', 'website_url', 'https://tristatedeltachi.org/', 'https://www.trine.edu/campus-life/clubs-organizations/delta-chi.aspx', 'Official university page linking to the chapter website, advisor, and Instagram.', 0.98),
      ('delta-chi', 'delta-chi-curated-demo', 'tri-state-chapter-trine-university', 'instagram_url', 'https://www.instagram.com/deltachitrine/', 'https://www.trine.edu/campus-life/clubs-organizations/delta-chi.aspx', 'Official university page linking to the chapter website, advisor, and Instagram.', 0.97),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'omicron-gamma-florida-atlantic-university', 'instagram_url', 'https://www.instagram.com/og_nupes/', 'https://www.fau.edu/fslife/about/chapters/fraternities/kappa-alpha-psi/', 'Official FAU chapter page with chapter email and Instagram handle.', 0.98),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'omicron-gamma-florida-atlantic-university', 'contact_email', 'ognupes@gmail.com', 'https://www.fau.edu/fslife/about/chapters/fraternities/kappa-alpha-psi/', 'Official FAU chapter page with chapter email and Instagram handle.', 0.98),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'alpha-iota-morgan-state-university', 'website_url', 'https://ainupes1931.com/', 'https://ainupes1931.com/', 'Official chapter website for Alpha Iota Chapter.', 0.98),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'alpha-omicron-university-of-louisville', 'website_url', 'https://aokappaalphapsi.wixsite.com/alphaomicronchapter', 'https://scpkapsi.org/chapters/', 'Official South Central Province chapter roster listing the chapter website.', 0.97),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'alpha-theta-tennessee-state-university', 'website_url', 'https://atng1931.weebly.com/', 'https://scpkapsi.org/chapters/', 'Official South Central Province chapter roster listing the chapter website.', 0.97),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'epsilon-rho-western-kentucky-university', 'website_url', 'https://orgs.wku.edu/epnupes/', 'https://scpkapsi.org/chapters/', 'Official South Central Province chapter roster listing the chapter website.', 0.97),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'eta-gamma-middle-tennessee-state-university', 'website_url', 'https://www.egnupes.com/', 'https://scpkapsi.org/chapters/', 'Official South Central Province chapter roster listing the chapter website.', 0.97),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'mu-rho-university-of-tennessee-knoxville', 'instagram_url', 'https://www.instagram.com/murhonupes/', 'https://scpkapsi.org/chapters/', 'Official South Central Province chapter roster listing the chapter Instagram.', 0.97),
      ('kappa-alpha-psi', 'kappa-alpha-psi-curated-demo', 'theta-beta-austin-peay-state-university', 'instagram_url', 'https://www.instagram.com/thetabeta_nupes/', 'https://scpkapsi.org/chapters/', 'Official South Central Province chapter roster listing the chapter Instagram.', 0.97)
)
INSERT INTO chapter_provenance (
    chapter_id,
    source_id,
    field_name,
    field_value,
    source_url,
    source_snippet,
    confidence
)
SELECT
    c.id,
    s.id,
    fs.field_name,
    fs.field_value,
    fs.source_url,
    fs.source_snippet,
    fs.confidence
FROM field_seed fs
JOIN fraternities f ON f.slug = fs.fraternity_slug
JOIN chapters c ON c.fraternity_id = f.id AND c.slug = fs.chapter_slug
JOIN sources s ON s.slug = fs.source_slug
WHERE NOT EXISTS (
    SELECT 1
    FROM chapter_provenance cp
    WHERE cp.chapter_id = c.id
      AND cp.field_name = fs.field_name
      AND cp.field_value = fs.field_value
      AND cp.source_url = fs.source_url
);

COMMIT;
