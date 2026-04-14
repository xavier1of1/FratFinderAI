from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
CRAWLER_SRC = REPO_ROOT / "services" / "crawler" / "src"
if str(CRAWLER_SRC) not in sys.path:
    sys.path.insert(0, str(CRAWLER_SRC))

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.logging_utils import configure_logging
from fratfinder_crawler.pipeline import CrawlService

FIELD_NAMES = ("find_website", "find_email", "find_instagram")
COHORT_SQL = """
    (
        (
            COALESCE(c.website_url, '') = ''
            AND COALESCE(c.contact_email, '') = ''
            AND COALESCE(c.instagram_url, '') = ''
            AND COALESCE(c.field_states ->> 'website_url', 'missing') NOT IN ('inactive', 'confirmed_absent', 'invalid_entity')
            AND COALESCE(c.field_states ->> 'contact_email', 'missing') NOT IN ('inactive', 'confirmed_absent', 'invalid_entity')
            AND COALESCE(c.field_states ->> 'instagram_url', 'missing') NOT IN ('inactive', 'confirmed_absent', 'invalid_entity')
        )
        OR c.chapter_status = 'inactive'
    )
"""


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return now_utc().replace(microsecond=0).isoformat()


def default_run_id() -> str:
    return f"stress-{now_utc().strftime('%Y%m%dT%H%M%SZ')}"


def get_connection():
    settings = get_settings()
    return psycopg.connect(settings.database_url, row_factory=dict_row)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")


def enqueue_stress_run(run_id: str, priority: int) -> dict[str, int]:
    sql = f"""
        WITH cohort AS (
            SELECT
                c.id AS chapter_id,
                c.slug AS chapter_slug,
                f.slug AS fraternity_slug,
                c.chapter_status,
                c.website_url,
                c.contact_email,
                c.instagram_url,
                c.field_states,
                latest.crawl_run_id,
                latest.source_slug
            FROM chapters c
            JOIN fraternities f ON f.id = c.fraternity_id
            LEFT JOIN LATERAL (
                SELECT
                    cp.crawl_run_id,
                    s.slug AS source_slug
                FROM chapter_provenance cp
                JOIN sources s ON s.id = cp.source_id
                WHERE cp.chapter_id = c.id
                ORDER BY cp.extracted_at DESC, cp.created_at DESC
                LIMIT 1
            ) latest ON TRUE
            WHERE {COHORT_SQL}
        ),
        field_requests AS (
            SELECT
                cohort.chapter_id,
                cohort.chapter_slug,
                cohort.fraternity_slug,
                cohort.crawl_run_id,
                cohort.source_slug,
                field_name
            FROM cohort
            CROSS JOIN unnest(%s::text[]) AS field_name
            WHERE cohort.source_slug IS NOT NULL
              AND (
                    (field_name = 'find_website' AND COALESCE(cohort.website_url, '') = '' AND COALESCE(cohort.field_states ->> 'website_url', 'missing') NOT IN ('inactive', 'confirmed_absent', 'invalid_entity'))
                 OR (field_name = 'find_email' AND COALESCE(cohort.contact_email, '') = '' AND COALESCE(cohort.field_states ->> 'contact_email', 'missing') NOT IN ('inactive', 'confirmed_absent', 'invalid_entity'))
                 OR (field_name = 'find_instagram' AND COALESCE(cohort.instagram_url, '') = '' AND COALESCE(cohort.field_states ->> 'instagram_url', 'missing') NOT IN ('inactive', 'confirmed_absent', 'invalid_entity'))
              )
        ),
        upserted AS (
            INSERT INTO field_jobs (
                chapter_id,
                crawl_run_id,
                field_name,
                status,
                payload,
                attempts,
                max_attempts,
                scheduled_at,
                last_error,
                terminal_failure,
                priority,
                queue_state,
                blocked_reason,
                terminal_outcome,
                completed_payload,
                claimed_by,
                claim_token,
                started_at,
                finished_at
            )
            SELECT
                fr.chapter_id,
                fr.crawl_run_id,
                fr.field_name,
                'queued',
                jsonb_build_object(
                    'sourceSlug', fr.source_slug,
                    'chapterSlug', fr.chapter_slug,
                    'fraternitySlug', fr.fraternity_slug,
                    'stressRunId', %s::text,
                    'stressMode', 'all_missing_or_inactive'
                ),
                0,
                3,
                NOW(),
                NULL,
                false,
                %s,
                'actionable',
                NULL,
                NULL,
                '{{}}'::jsonb,
                NULL,
                NULL,
                NULL,
                NULL
            FROM field_requests fr
            ON CONFLICT (chapter_id, field_name) WHERE status IN ('queued', 'running')
            DO UPDATE SET
                crawl_run_id = COALESCE(EXCLUDED.crawl_run_id, field_jobs.crawl_run_id),
                status = 'queued',
                payload = COALESCE(field_jobs.payload, '{{}}'::jsonb) || EXCLUDED.payload,
                attempts = 0,
                max_attempts = GREATEST(field_jobs.max_attempts, EXCLUDED.max_attempts),
                scheduled_at = NOW(),
                started_at = NULL,
                finished_at = NULL,
                last_error = NULL,
                terminal_failure = false,
                priority = GREATEST(field_jobs.priority, EXCLUDED.priority),
                queue_state = 'actionable',
                blocked_reason = NULL,
                terminal_outcome = NULL,
                completed_payload = '{{}}'::jsonb,
                claimed_by = NULL,
                claim_token = NULL
            RETURNING chapter_id
        )
        SELECT
            (SELECT COUNT(*)::int FROM cohort) AS cohort_chapters,
            (SELECT COUNT(*)::int FROM field_requests) AS requested_jobs,
            (SELECT COUNT(*)::int FROM upserted) AS affected_jobs,
            (
                SELECT COUNT(*)::int
                FROM cohort
                WHERE source_slug IS NULL
            ) AS missing_source_chapters;
    """
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(sql, [list(FIELD_NAMES), run_id, priority])
        row = cursor.fetchone() or {}
        connection.commit()
    return {
        "cohort_chapters": int(row.get("cohort_chapters") or 0),
        "requested_jobs": int(row.get("requested_jobs") or 0),
        "affected_jobs": int(row.get("affected_jobs") or 0),
        "missing_source_chapters": int(row.get("missing_source_chapters") or 0),
    }


def cohort_summary(run_id: str) -> dict[str, Any]:
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*)::int AS job_count,
                COUNT(DISTINCT chapter_id)::int AS chapter_count,
                COUNT(*) FILTER (WHERE status = 'done')::int AS done_jobs,
                COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs,
                COUNT(*) FILTER (WHERE status = 'queued' AND queue_state = 'actionable')::int AS queued_actionable,
                COUNT(*) FILTER (WHERE status = 'queued' AND queue_state = 'deferred')::int AS queued_deferred,
                COUNT(*) FILTER (WHERE status = 'running')::int AS running_jobs
            FROM field_jobs
            WHERE payload ->> 'stressRunId' = %s
            """,
            [run_id],
        )
        counts = cursor.fetchone() or {}
        cursor.execute(
            """
            SELECT field_name, status, queue_state, COUNT(*)::int AS count
            FROM field_jobs
            WHERE payload ->> 'stressRunId' = %s
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """,
            [run_id],
        )
        breakdown = list(cursor.fetchall())
    return {"counts": counts, "breakdown": breakdown}


def batch_delta(run_id: str, since: datetime | None) -> dict[str, Any]:
    with get_connection() as connection, connection.cursor() as cursor:
        if since is None:
            since_clause = ""
            params: list[Any] = [run_id]
        else:
            since_clause = "AND fj.updated_at > %s"
            params = [run_id, since]
        cursor.execute(
            f"""
            SELECT
                COUNT(*)::int AS touched_jobs,
                COUNT(*) FILTER (WHERE status = 'done')::int AS done_jobs,
                COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs,
                COUNT(*) FILTER (WHERE status = 'queued' AND queue_state = 'deferred')::int AS deferred_jobs,
                COUNT(*) FILTER (WHERE status = 'queued' AND queue_state = 'actionable')::int AS actionable_jobs
            FROM field_jobs fj
            WHERE fj.payload ->> 'stressRunId' = %s
            {since_clause}
            """,
            params,
        )
        counts = cursor.fetchone() or {}
        cursor.execute(
            f"""
            SELECT
                COALESCE(last_error, completed_payload ->> 'status', queue_state, status) AS outcome,
                COUNT(*)::int AS count
            FROM field_jobs fj
            WHERE fj.payload ->> 'stressRunId' = %s
            {since_clause}
            GROUP BY 1
            ORDER BY count DESC, outcome ASC
            LIMIT 12
            """,
            params,
        )
        outcomes = list(cursor.fetchall())
        cursor.execute(
            f"""
            SELECT
                fr.slug AS fraternity_slug,
                COALESCE(fj.payload ->> 'sourceSlug', s.slug, '') AS source_slug,
                c.slug AS chapter_slug,
                fj.field_name,
                fj.status,
                fj.queue_state,
                COALESCE(last_error, completed_payload ->> 'status', '') AS outcome,
                completed_payload ->> 'query' AS query,
                completed_payload ->> 'sourceUrl' AS source_url,
                COALESCE((completed_payload -> 'decision_trace' -> 'search' ->> 'attempted')::int, 0) AS queries_attempted,
                COALESCE((completed_payload -> 'decision_trace' -> 'search' ->> 'succeeded')::int, 0) AS queries_succeeded,
                COALESCE((completed_payload -> 'decision_trace' -> 'search' ->> 'failed')::int, 0) AS queries_failed,
                fj.updated_at
            FROM field_jobs fj
            JOIN chapters c ON c.id = fj.chapter_id
            JOIN fraternities fr ON fr.id = c.fraternity_id
            LEFT JOIN sources s ON s.slug = fj.payload ->> 'sourceSlug'
            WHERE fj.payload ->> 'stressRunId' = %s
            {since_clause}
            AND (
                fj.status = 'failed'
                OR fj.queue_state = 'deferred'
                OR fj.last_error IS NOT NULL
                OR (fj.completed_payload ->> 'status') IN ('review_required', 'terminal_no_signal', 'provider_degraded')
            )
            ORDER BY updated_at DESC
            LIMIT 20
            """,
            params,
        )
        samples = list(cursor.fetchall())
    return {"counts": counts, "outcomes": outcomes, "samples": samples}


def adjust_workers(current_workers: int, delta: dict[str, Any], max_workers: int) -> int:
    touched = int(delta["counts"].get("touched_jobs") or 0)
    if touched <= 0:
        return current_workers
    deferred = int(delta["counts"].get("deferred_jobs") or 0)
    failed = int(delta["counts"].get("failed_jobs") or 0)
    strain_ratio = (deferred + failed) / max(1, touched)
    if strain_ratio >= 0.55:
        return max(2, math.floor(current_workers / 2))
    if strain_ratio >= 0.35:
        return max(2, current_workers - 2)
    if strain_ratio <= 0.12 and current_workers < max_workers:
        return min(max_workers, current_workers + 1)
    return current_workers


def _batch_provider_window_success_rate(batch_result: dict[str, Any]) -> float:
    provider_window_state = batch_result.get("provider_window_state")
    if not isinstance(provider_window_state, dict):
        return 1.0
    general_lane = provider_window_state.get("general_web_search")
    if not isinstance(general_lane, dict):
        return 1.0
    try:
        return float(general_lane.get("window_success_rate", 1.0) or 0.0)
    except (TypeError, ValueError):
        return 1.0


def should_run_authoritative_recovery(batch_result: dict[str, Any], delta: dict[str, Any], *, limit: int) -> bool:
    processed = int(batch_result.get("processed") or 0)
    requeued = int(batch_result.get("requeued") or 0)
    provider_success = _batch_provider_window_success_rate(batch_result)
    outcomes = delta.get("outcomes") or []
    networkish = 0
    for row in outcomes:
        if not isinstance(row, dict):
            continue
        outcome = str(row.get("outcome") or "").lower()
        count = int(row.get("count") or 0)
        if "search provider or network unavailable" in outcome or "transient_network" in outcome:
            networkish += count

    if processed <= 2 and requeued >= max(1, math.floor(limit * 0.9)) and provider_success < 0.2:
        return True
    if processed == 0 and networkish >= max(10, math.floor(limit * 0.5)):
        return True
    return False


def adaptive_recovery_promotion_limit(
    *,
    base_limit: int,
    batch_limit: int,
    degraded_streak: int,
    provider_success: float,
) -> int:
    effective_base = max(1, int(base_limit))
    effective_batch_limit = max(1, int(batch_limit))
    streak_scale = min(max(1, int(degraded_streak)), 4)
    if provider_success <= 0.1:
        streak_scale = min(4, streak_scale + 1)
    return min(effective_batch_limit, effective_base * streak_scale)


def run_authoritative_recovery(
    *,
    promotion_limit: int,
    workers: int,
    preflight_snapshot: dict[str, Any] | None = None,
    provider_window_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    service = CrawlService(settings)
    promotion = service.enrichment_promote_verify_school_candidates(
        limit=promotion_limit,
        include_preflight=False,
        preflight_snapshot=preflight_snapshot,
        provider_window_state=provider_window_state,
        apply_changes=True,
    )
    promoted = int(promotion.get("promotedVerifySchoolJobs") or 0)
    processing: dict[str, Any] = {
        "processed": 0,
        "requeued": 0,
        "failed_terminal": 0,
        "runtime_mode_used": "legacy",
    }
    if promoted > 0:
        processing = service.process_field_jobs(
            limit=promoted,
            source_slug=None,
            field_name="verify_school_match",
            workers=max(1, workers),
            require_healthy_search=False,
            run_preflight=False,
            runtime_mode="legacy",
            graph_durability="sync",
        )
    return {
        "type": "authoritative_recovery",
        "timestamp": iso_now(),
        "promotion": promotion,
        "processing": processing,
    }


def run_batch(limit: int, workers: int, graph_durability: str, run_preflight: bool) -> dict[str, Any]:
    settings = get_settings()
    service = CrawlService(settings)
    started = time.perf_counter()
    result = service.process_field_jobs(
        limit=limit,
        source_slug=None,
        field_name=None,
        workers=workers,
        require_healthy_search=False,
        run_preflight=run_preflight,
        runtime_mode="langgraph_primary",
        graph_durability=graph_durability,
    )
    elapsed = time.perf_counter() - started
    result["elapsed_seconds"] = round(elapsed, 3)
    result["jobs_per_second"] = round((int(result.get("processed") or 0) + int(result.get("requeued") or 0) + int(result.get("failed_terminal") or 0)) / max(elapsed, 0.001), 3)
    result["jobs_per_minute"] = round(result["jobs_per_second"] * 60.0, 3)
    return result


def format_progress_line(batch_index: int, run_id: str, workers: int, limit: int, batch_result: dict[str, Any], delta: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "batch_progress",
        "timestamp": iso_now(),
        "run_id": run_id,
        "batch_index": batch_index,
        "workers": workers,
        "limit": limit,
        "batch_result": batch_result,
        "delta": delta,
        "summary": summary,
    }


def investigate_failure_mode(run_id: str, failure_text: str) -> dict[str, Any]:
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                fr.slug AS fraternity_slug,
                fj.payload ->> 'sourceSlug' AS source_slug,
                c.slug AS chapter_slug,
                fj.field_name,
                fj.status,
                fj.queue_state,
                fj.last_error,
                fj.completed_payload ->> 'status' AS payload_status,
                fj.completed_payload ->> 'query' AS query,
                fj.completed_payload ->> 'sourceUrl' AS source_url,
                COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'attempted')::int, 0) AS queries_attempted,
                COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'succeeded')::int, 0) AS queries_succeeded,
                COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'failed')::int, 0) AS queries_failed,
                fj.completed_payload -> 'decision_trace' -> 'rejections' AS rejections,
                fj.updated_at
            FROM field_jobs fj
            JOIN chapters c ON c.id = fj.chapter_id
            JOIN fraternities fr ON fr.id = c.fraternity_id
            WHERE fj.payload ->> 'stressRunId' = %s
              AND COALESCE(fj.last_error, fj.completed_payload ->> 'status', '') = %s
            ORDER BY fj.updated_at DESC
            LIMIT 25
            """,
            [run_id, failure_text],
        )
        return {"failure_text": failure_text, "samples": list(cursor.fetchall())}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a tagged field-job stress test across all chapters with no contact info or inactive status.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--limit", type=int, default=240)
    parser.add_argument("--batches", type=int, default=6)
    parser.add_argument("--priority", type=int, default=950)
    parser.add_argument("--graph-durability", choices=["exit", "async", "sync"], default="sync")
    parser.add_argument("--skip-enqueue", action="store_true")
    parser.add_argument("--run-preflight", action="store_true")
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--disable-authoritative-recovery", action="store_true")
    parser.add_argument("--recovery-promotion-limit", type=int, default=48)
    parser.add_argument("--recovery-workers", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.crawler_log_level)

    run_id = args.run_id or default_run_id()
    report_path = Path(args.report_path) if args.report_path else REPO_ROOT / "docs" / "reports" / "stress" / f"{run_id}.jsonl"

    header = {"type": "run_started", "timestamp": iso_now(), "run_id": run_id, "args": vars(args)}
    append_jsonl(report_path, header)
    print(json.dumps(header, default=str))

    if not args.skip_enqueue:
        enqueue_result = enqueue_stress_run(run_id, args.priority)
        payload = {"type": "enqueue", "timestamp": iso_now(), "run_id": run_id, **enqueue_result}
        append_jsonl(report_path, payload)
        print(json.dumps(payload, default=str))

    workers = max(2, min(args.workers, args.max_workers))
    last_snapshot_at: datetime | None = None
    degraded_streak = 0

    for batch_index in range(1, args.batches + 1):
        batch_result = run_batch(
            limit=args.limit,
            workers=workers,
            graph_durability=args.graph_durability,
            run_preflight=args.run_preflight,
        )
        delta = batch_delta(run_id, last_snapshot_at)
        summary = cohort_summary(run_id)
        progress = format_progress_line(batch_index, run_id, workers, args.limit, batch_result, delta, summary)
        append_jsonl(report_path, progress)
        print(json.dumps(progress, default=str))

        provider_success = _batch_provider_window_success_rate(batch_result)
        if provider_success < 0.2:
            degraded_streak += 1
        else:
            degraded_streak = 0

        if not args.disable_authoritative_recovery and should_run_authoritative_recovery(batch_result, delta, limit=args.limit):
            promotion_limit = adaptive_recovery_promotion_limit(
                base_limit=args.recovery_promotion_limit,
                batch_limit=args.limit,
                degraded_streak=degraded_streak,
                provider_success=provider_success,
            )
            recovery = run_authoritative_recovery(
                promotion_limit=promotion_limit,
                workers=max(1, args.recovery_workers),
                preflight_snapshot=batch_result.get("preflight") if isinstance(batch_result.get("preflight"), dict) else None,
                provider_window_state=batch_result.get("provider_window_state") if isinstance(batch_result.get("provider_window_state"), dict) else None,
            )
            recovery["run_id"] = run_id
            recovery["batch_index"] = batch_index
            recovery["degraded_streak"] = degraded_streak
            recovery["recovery_promotion_limit"] = promotion_limit
            append_jsonl(report_path, recovery)
            print(json.dumps(recovery, default=str))

        last_snapshot_at = now_utc()
        workers = adjust_workers(workers, delta, max_workers=max(2, args.max_workers))

        remaining = summary["counts"]
        if int(remaining.get("queued_actionable") or 0) == 0 and int(remaining.get("running_jobs") or 0) == 0:
            break

    final_summary = cohort_summary(run_id)
    finish = {"type": "run_finished", "timestamp": iso_now(), "run_id": run_id, "summary": final_summary}
    append_jsonl(report_path, finish)
    print(json.dumps(finish, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
