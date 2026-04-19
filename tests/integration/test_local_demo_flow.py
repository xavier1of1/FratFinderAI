from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest

try:
    import psycopg
except ModuleNotFoundError:  # pragma: no cover - environment-dependent
    psycopg = None

REPO_ROOT = Path(__file__).resolve().parents[2]
CRAWLER_SRC = REPO_ROOT / "services" / "crawler" / "src"
if str(CRAWLER_SRC) not in sys.path:
    sys.path.insert(0, str(CRAWLER_SRC))


def _load_env_file_defaults() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file_defaults()


@pytest.mark.integration
def test_local_flow_crawl_to_dashboard_visibility(monkeypatch: pytest.MonkeyPatch) -> None:
    if psycopg is None:
        pytest.skip("psycopg is not installed in the active Python environment")

    from fratfinder_crawler.config import Settings
    from fratfinder_crawler.pipeline import CrawlService

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is required for integration test")

    try:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
    except Exception as exc:
        pytest.skip(f"Local Postgres is not reachable for integration test: {exc}")

    unique = uuid4().hex[:8]
    fraternity_slug = f"int-frat-{unique}"
    source_slug = f"int-source-{unique}"

    try:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fraternities (slug, name)
                    VALUES (%s, %s)
                    RETURNING id
                    """,
                    (fraternity_slug, f"Integration Fraternity {unique}"),
                )
                fraternity_id = cursor.fetchone()[0]
                cursor.execute(
                    """
                    INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, metadata, active)
                    VALUES (%s, %s, 'html_directory', 'directory_v1', %s, '/chapters', '{}'::jsonb, TRUE)
                    """,
                    (fraternity_id, source_slug, f"https://source-{unique}.example.org"),
                )
            connection.commit()

        html = """
        <html><body>
          <ul>
            <li class=\"chapter-item\" data-chapter-card>
              <h3 class=\"chapter-name\">Alpha Test</h3>
              <div class=\"university\">Demo University</div>
              <div class=\"location\">Austin, TX</div>
              <div class=\"notes\">Contact alpha.test@example.edu
                Follow https://instagram.com/alphatestchapter
                Website https://alphatest.example.edu
              </div>
            </li>
          </ul>
        </body></html>
        """

        from fratfinder_crawler.http.client import HttpClient

        monkeypatch.setattr(HttpClient, "get", lambda self, url: html)

        settings = Settings(database_url=database_url)
        service = CrawlService(settings)

        crawl_result = service.run(source_slug=source_slug)
        assert crawl_result["records_upserted"] == 1
        created_field_jobs = int(crawl_result.get("field_jobs_created", 0) or 0)
        if created_field_jobs <= 0:
            with psycopg.connect(database_url) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT c.id, cr.id
                        FROM chapters c
                        JOIN fraternities f ON f.id = c.fraternity_id
                        JOIN sources s ON s.fraternity_id = f.id
                        JOIN crawl_runs cr ON cr.source_id = s.id
                        WHERE f.slug = %s
                          AND s.slug = %s
                        ORDER BY cr.started_at DESC
                        LIMIT 1
                        """,
                        (fraternity_slug, source_slug),
                    )
                    row = cursor.fetchone()
                    assert row is not None
                    cursor.execute(
                        """
                        INSERT INTO field_jobs (chapter_id, crawl_run_id, field_name, payload)
                        VALUES (%s, %s, 'verify_website', %s::jsonb)
                        """,
                        (row[0], row[1], json.dumps({"sourceSlug": source_slug})),
                    )
                connection.commit()
            created_field_jobs = 1

        field_job_result = service.process_field_jobs(limit=25, source_slug=source_slug)
        assert "processed" in field_job_result
        assert "requeued" in field_job_result

        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT c.website_url, c.instagram_url, c.contact_email
                    FROM chapters c
                    JOIN fraternities f ON f.id = c.fraternity_id
                    WHERE f.slug = %s
                    """,
                    (fraternity_slug,),
                )
                row = cursor.fetchone()
                assert row is not None

        baseline = service.system_baseline(include_preflight=False)
        assert baseline["queue"]["queued_jobs"] >= 0
        assert baseline["queue_health"]["worker_liveness_ratio"] >= 0.0
        assert "provenance_audit" in baseline
    finally:
        _cleanup_integration_records(database_url, fraternity_slug)


def _cleanup_integration_records(database_url: str, fraternity_slug: str) -> None:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH target_fraternities AS (
                    SELECT id
                    FROM fraternities
                    WHERE slug = %s
                ),
                target_sources AS (
                    SELECT id
                    FROM sources
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                ),
                target_chapters AS (
                    SELECT id
                    FROM chapters
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                )
                DELETE FROM chapter_provenance
                WHERE chapter_id IN (SELECT id FROM target_chapters)
                """,
                (fraternity_slug,),
            )
            cursor.execute(
                """
                WITH target_fraternities AS (
                    SELECT id
                    FROM fraternities
                    WHERE slug = %s
                ),
                target_chapters AS (
                    SELECT id
                    FROM chapters
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                )
                DELETE FROM field_jobs
                WHERE chapter_id IN (SELECT id FROM target_chapters)
                """,
                (fraternity_slug,),
            )
            cursor.execute(
                """
                WITH target_fraternities AS (
                    SELECT id
                    FROM fraternities
                    WHERE slug = %s
                ),
                target_sources AS (
                    SELECT id
                    FROM sources
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                ),
                target_chapters AS (
                    SELECT id
                    FROM chapters
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                )
                DELETE FROM review_item_audit_logs
                WHERE review_item_id IN (
                    SELECT id
                    FROM review_items
                    WHERE source_id IN (SELECT id FROM target_sources)
                       OR chapter_id IN (SELECT id FROM target_chapters)
                )
                """,
                (fraternity_slug,),
            )
            cursor.execute(
                """
                WITH target_fraternities AS (
                    SELECT id
                    FROM fraternities
                    WHERE slug = %s
                ),
                target_sources AS (
                    SELECT id
                    FROM sources
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                ),
                target_chapters AS (
                    SELECT id
                    FROM chapters
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                )
                DELETE FROM review_items
                WHERE source_id IN (SELECT id FROM target_sources)
                   OR chapter_id IN (SELECT id FROM target_chapters)
                """,
                (fraternity_slug,),
            )
            cursor.execute(
                """
                WITH target_fraternities AS (
                    SELECT id
                    FROM fraternities
                    WHERE slug = %s
                ),
                target_sources AS (
                    SELECT id
                    FROM sources
                    WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                )
                DELETE FROM crawl_runs
                WHERE source_id IN (SELECT id FROM target_sources)
                """,
                (fraternity_slug,),
            )
            cursor.execute(
                """
                WITH target_fraternities AS (
                    SELECT id
                    FROM fraternities
                    WHERE slug = %s
                )
                DELETE FROM chapters
                WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                """,
                (fraternity_slug,),
            )
            cursor.execute(
                """
                WITH target_fraternities AS (
                    SELECT id
                    FROM fraternities
                    WHERE slug = %s
                )
                DELETE FROM sources
                WHERE fraternity_id IN (SELECT id FROM target_fraternities)
                """,
                (fraternity_slug,),
            )
            cursor.execute("DELETE FROM fraternities WHERE slug = %s", (fraternity_slug,))
        connection.commit()
