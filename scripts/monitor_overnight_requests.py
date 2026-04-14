from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/fratfinder"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Periodically snapshot a bounded set of fraternity crawl requests.")
    parser.add_argument("--request-ids", required=True, help="Comma-separated request IDs to monitor")
    parser.add_argument("--output-dir", required=True, help="Directory where manifest and snapshots will be written")
    parser.add_argument("--interval-seconds", type=int, default=300, help="Polling interval in seconds")
    parser.add_argument("--duration-hours", type=float, default=8.0, help="How long to monitor before exiting")
    return parser.parse_args()


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _serialize(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    return value


def fetch_request_rows(database_url: str, request_ids: list[str]) -> list[dict[str, Any]]:
    query = """
        SELECT
            id,
            fraternity_name,
            fraternity_slug,
            source_slug,
            source_url,
            status,
            stage,
            started_at,
            finished_at,
            updated_at,
            last_error,
            progress
        FROM fraternity_crawl_requests
        WHERE id = ANY(%s)
        ORDER BY fraternity_name
    """
    with psycopg.connect(database_url, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(query, (request_ids,))
        return [_serialize(dict(row)) for row in cur.fetchall()]


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    request_ids = [value.strip() for value in args.request_ids.split(",") if value.strip()]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "intervalSeconds": int(args.interval_seconds),
        "durationHours": float(args.duration_hours),
        "requestIds": request_ids,
    }
    write_json(output_dir / "monitor_manifest.json", manifest)

    deadline = time.time() + max(1.0, float(args.duration_hours)) * 3600.0
    snapshot_index = 0
    while time.time() <= deadline:
        snapshot_index += 1
        captured_at = datetime.now(timezone.utc)
        rows = fetch_request_rows(database_url, request_ids)
        payload = {
            "capturedAt": captured_at.isoformat(),
            "snapshotIndex": snapshot_index,
            "requests": rows,
        }
        write_json(output_dir / f"snapshot_{snapshot_index:04d}.json", payload)
        time.sleep(max(5, int(args.interval_seconds)))


if __name__ == "__main__":
    main()
