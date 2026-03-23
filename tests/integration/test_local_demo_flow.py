from __future__ import annotations

import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any
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
        assert crawl_result["field_jobs_created"] >= 3

        field_job_result = service.process_field_jobs(limit=25, source_slug=source_slug)
        assert field_job_result["processed"] >= 3

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
                assert row[0] is not None
                assert row[1] is not None
                assert row[2] is not None

        port = _get_free_port()
        web_proc = _start_web_server(database_url=database_url, port=port)
        try:
            _skip_if_web_subprocess_is_sandbox_blocked(web_proc)
            _wait_for_ready(f"http://127.0.0.1:{port}/api/health/readiness", timeout_seconds=90)

            chapters_payload = _get_json(f"http://127.0.0.1:{port}/api/chapters?limit=500")
            review_payload = _get_json(f"http://127.0.0.1:{port}/api/review-items?limit=500")
            jobs_payload = _get_json(f"http://127.0.0.1:{port}/api/field-jobs?limit=500")

            assert chapters_payload["success"] is True
            assert review_payload["success"] is True
            assert jobs_payload["success"] is True

            chapter_rows = chapters_payload["data"]
            assert any(item["fraternitySlug"] == fraternity_slug for item in chapter_rows)
            assert len(jobs_payload["data"]) >= 1
        finally:
            _stop_process(web_proc)
    finally:
        _cleanup_integration_records(database_url, fraternity_slug)


def _start_web_server(database_url: str, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["NEXT_PUBLIC_API_BASE_URL"] = f"http://127.0.0.1:{port}"
    env["PORT"] = str(port)

    if platform.system() == "Windows":
        return subprocess.Popen(
            [
                "powershell",
                "-Command",
                f"pnpm --filter @fratfinder/web dev --port {port}",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    pnpm_executable = shutil.which("pnpm") or shutil.which("pnpm.cmd")
    if pnpm_executable is None:
        pytest.skip("pnpm executable is not available for integration test")

    return subprocess.Popen(
        [pnpm_executable, "--filter", "@fratfinder/web", "dev", "--port", str(port)],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _skip_if_web_subprocess_is_sandbox_blocked(process: subprocess.Popen[str]) -> None:
    time.sleep(2)
    if process.poll() is None:
        return

    output = process.stdout.read() if process.stdout is not None else ""
    lowered = output.lower()
    if "eperm" in lowered and ("lstat 'c:\\users" in lowered or "syscall: 'spawn'" in lowered or "syscall: 'lstat'" in lowered):
        pytest.skip("Next.js dev subprocess is blocked by the current Windows sandbox")

    raise RuntimeError(f"Web server exited before readiness check. Output:\n{output}")


def _wait_for_ready(url: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            payload = _get_json(url)
            if payload.get("success"):
                return
        except Exception:
            time.sleep(1)
            continue
        time.sleep(1)

    raise TimeoutError(f"Timed out waiting for readiness: {url}")


def _get_json(url: str, timeout: int = 30) -> dict[str, Any]:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
