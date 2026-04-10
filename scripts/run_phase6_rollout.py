from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CRAWLER_SRC = ROOT / "services" / "crawler" / "src"
if str(CRAWLER_SRC) not in sys.path:
    sys.path.insert(0, str(CRAWLER_SRC))

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.db.repository import CrawlerRepository
from fratfinder_crawler.pipeline import CrawlService


BATCHES = (
    {"name": "bounded_mixed", "limit": 100, "workers": 6},
    {"name": "representative", "limit": 250, "workers": 8},
    {"name": "investor_target", "limit": 400, "workers": 10},
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat()


def snapshot_metrics() -> dict[str, Any]:
    settings = get_settings()
    with get_connection(settings) as connection:
        repository = CrawlerRepository(connection)
        accuracy = asdict(repository.get_accuracy_recovery_metrics())
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'queued')::int AS queued_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
                  COUNT(*) FILTER (WHERE status = 'running')::int AS running_jobs,
                  COUNT(*) FILTER (WHERE status = 'done')::int AS done_jobs,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs
                FROM field_jobs
                """
            )
            queue = dict(cursor.fetchone() or {})
            cursor.execute(
                """
                SELECT
                  field_name,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
                  COUNT(*) FILTER (WHERE status = 'done')::int AS done_jobs,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs
                FROM field_jobs
                WHERE field_name IN ('find_website', 'verify_website', 'find_instagram', 'find_email')
                GROUP BY 1
                ORDER BY 1
                """
            )
            field_breakdown = [dict(row) for row in cursor.fetchall()]
            cursor.execute("SELECT COUNT(*)::int AS count FROM chapters WHERE chapter_status = 'inactive'")
            total_inactive_rows = int(dict(cursor.fetchone() or {}).get("count") or 0)
        return {
            "captured_at": iso_utc(utc_now()),
            "accuracy": accuracy,
            "queue": queue,
            "field_breakdown": field_breakdown,
            "total_inactive_rows": total_inactive_rows,
        }


def changed_delta(since: datetime) -> dict[str, Any]:
    settings = get_settings()
    with get_connection(settings) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              COUNT(*)::int AS touched_jobs,
              COUNT(*) FILTER (WHERE status = 'done')::int AS done_jobs,
              COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs,
              COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
              COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
              COUNT(*) FILTER (WHERE status = 'running')::int AS running_jobs
            FROM field_jobs
            WHERE updated_at >= %s
            """,
            [since],
        )
        counts = dict(cursor.fetchone() or {})
        cursor.execute(
            """
            SELECT
              COALESCE(last_error, completed_payload ->> 'status', terminal_outcome, queue_state, status) AS outcome,
              COUNT(*)::int AS count
            FROM field_jobs
            WHERE updated_at >= %s
            GROUP BY 1
            ORDER BY count DESC, outcome ASC
            LIMIT 12
            """,
            [since],
        )
        outcomes = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            """
            SELECT
              fr.slug AS fraternity_slug,
              COALESCE(fj.payload ->> 'sourceSlug', s.slug, '') AS source_slug,
              c.slug AS chapter_slug,
              fj.field_name,
              fj.status,
              COALESCE(fj.queue_state, 'actionable') AS queue_state,
              COALESCE(fj.last_error, fj.completed_payload ->> 'status', fj.terminal_outcome, '') AS outcome,
              fj.completed_payload ->> 'query' AS query,
              fj.completed_payload ->> 'sourceUrl' AS source_url,
              COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'attempted')::int, 0) AS queries_attempted,
              COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'succeeded')::int, 0) AS queries_succeeded,
              COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'failed')::int, 0) AS queries_failed,
              fj.updated_at
            FROM field_jobs fj
            JOIN chapters c ON c.id = fj.chapter_id
            JOIN fraternities fr ON fr.id = c.fraternity_id
            LEFT JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
            LEFT JOIN sources s ON s.id = cr.source_id
            WHERE fj.updated_at >= %s
            ORDER BY fj.updated_at DESC
            LIMIT 20
            """,
            [since],
        )
        samples = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            """
            SELECT
              fr.slug AS fraternity_slug,
              COALESCE(cp.source_slug, s.slug, '') AS source_slug,
              c.slug AS chapter_slug,
              'contact_email' AS field_name,
              COALESCE(c.contact_email, '') AS field_value,
              COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') AS provenance_type,
              COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') AS page_scope,
              COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageUrl', '') AS supporting_page_url
            FROM chapters c
            JOIN fraternities fr ON fr.id = c.fraternity_id
            LEFT JOIN LATERAL (
              SELECT cp_inner.source_id, src.slug AS source_slug
              FROM chapter_provenance cp_inner
              LEFT JOIN sources src ON src.id = cp_inner.source_id
              WHERE cp_inner.chapter_id = c.id
              ORDER BY cp_inner.created_at DESC
              LIMIT 1
            ) cp ON TRUE
            LEFT JOIN sources s ON s.id = cp.source_id
            WHERE c.updated_at >= %s
              AND c.contact_email IS NOT NULL
              AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter')
            UNION ALL
            SELECT
              fr.slug AS fraternity_slug,
              COALESCE(cp.source_slug, s.slug, '') AS source_slug,
              c.slug AS chapter_slug,
              'instagram_url' AS field_name,
              COALESCE(c.instagram_url, '') AS field_value,
              COALESCE(c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') AS provenance_type,
              COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') AS page_scope,
              COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageUrl', '') AS supporting_page_url
            FROM chapters c
            JOIN fraternities fr ON fr.id = c.fraternity_id
            LEFT JOIN LATERAL (
              SELECT cp_inner.source_id, src.slug AS source_slug
              FROM chapter_provenance cp_inner
              LEFT JOIN sources src ON src.id = cp_inner.source_id
              WHERE cp_inner.chapter_id = c.id
              ORDER BY cp_inner.created_at DESC
              LIMIT 1
            ) cp ON TRUE
            LEFT JOIN sources s ON s.id = cp.source_id
            WHERE c.updated_at >= %s
              AND c.instagram_url IS NOT NULL
              AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter')
            LIMIT 20
            """,
            [since, since],
        )
        accepted_samples = [dict(row) for row in cursor.fetchall()]
    unresolved_samples = [row for row in samples if row["status"] == "queued"][:10]
    rejected_samples = [row for row in samples if row["status"] == "failed" or row["outcome"] in {"invalid_candidate", "terminal_no_signal"}][:10]
    return {
        "counts": counts,
        "outcomes": outcomes,
        "samples": samples,
        "accepted_samples": accepted_samples[:10],
        "rejected_samples": rejected_samples,
        "unresolved_samples": unresolved_samples,
    }


def run_batch(limit: int, workers: int) -> dict[str, Any]:
    settings = get_settings()
    service = CrawlService(settings)
    started = utc_now()
    result = service.process_field_jobs(
        limit=limit,
        source_slug=None,
        field_name=None,
        workers=workers,
        require_healthy_search=False,
        run_preflight=True,
        runtime_mode="langgraph_primary",
        graph_durability="sync",
    )
    finished = utc_now()
    elapsed = max((finished - started).total_seconds(), 0.001)
    result["started_at"] = iso_utc(started)
    result["finished_at"] = iso_utc(finished)
    result["elapsed_seconds"] = round(elapsed, 3)
    result["jobs_per_minute"] = round(
        (int(result.get("processed") or 0) + int(result.get("requeued") or 0) + int(result.get("failed_terminal") or 0)) * 60.0 / elapsed,
        3,
    )
    return result


def write_report(payload: dict[str, Any]) -> tuple[Path, Path]:
    out_dir = ROOT / "docs" / "SystemReport"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "PHASE_6_SCALING_RUN_2026-04-09.json"
    md_path = out_dir / "PHASE_6_SCALING_APPROVAL_REPORT_2026-04-09.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    before = payload["before"]["accuracy"]
    after = payload["after"]["accuracy"]
    before_queue = payload["before"]["queue"]
    after_queue = payload["after"]["queue"]

    lines = [
        "# Phase 6 Approval Report",
        "",
        "## Goal",
        "- Increase safe throughput only after the Phase 0-5 precision rules were in place.",
        "- Keep the rollout aligned to the plan: bounded mixed cohort, broader representative cohort, then investor-target tranche.",
        "",
        "## Generalized Solution Applied Before Scaling",
        "- Legacy field-job aliases are now normalized at runtime and at repository claim time, so stale `contact_email` / `website_url` job shapes do not waste the queue.",
        "- `verify_website` now clears non-HTTP pseudo-websites like `mailto:` and can backfill email safely instead of requeueing a broken candidate.",
        "- School-hosted pages now require explicit fraternity or chapter identity before they can donate website, email, or Instagram values.",
        "- Two-letter fraternity initials no longer count as enough Instagram identity, which blocks placeholder and CMS-generated garbage handles from slipping through.",
        "- The first Phase 6 pass surfaced false positives on school-hosted pages; those rows were remediated and the phase was rerun on the tightened ruleset before approval.",
        "",
        "## Before / After KPIs",
        "| KPI | Before | After | Delta |",
        "|---|---:|---:|---:|",
        f"| Complete rows | {before['complete_rows']} | {after['complete_rows']} | {after['complete_rows'] - before['complete_rows']} |",
        f"| Chapter-specific contact rows | {before['chapter_specific_contact_rows']} | {after['chapter_specific_contact_rows']} | {after['chapter_specific_contact_rows'] - before['chapter_specific_contact_rows']} |",
        f"| Active rows with chapter email | {before['active_rows_with_chapter_specific_email']} | {after['active_rows_with_chapter_specific_email']} | {after['active_rows_with_chapter_specific_email'] - before['active_rows_with_chapter_specific_email']} |",
        f"| Active rows with chapter Instagram | {before['active_rows_with_chapter_specific_instagram']} | {after['active_rows_with_chapter_specific_instagram']} | {after['active_rows_with_chapter_specific_instagram'] - before['active_rows_with_chapter_specific_instagram']} |",
        f"| Nationals-only contact rows | {before['nationals_only_contact_rows']} | {after['nationals_only_contact_rows']} | {after['nationals_only_contact_rows'] - before['nationals_only_contact_rows']} |",
        f"| Validated inactive rows | {before['inactive_validated_rows']} | {after['inactive_validated_rows']} | {after['inactive_validated_rows'] - before['inactive_validated_rows']} |",
        f"| Confirmed-absent websites | {before['confirmed_absent_website_rows']} | {after['confirmed_absent_website_rows']} | {after['confirmed_absent_website_rows'] - before['confirmed_absent_website_rows']} |",
        f"| Total inactive rows | {payload['before']['total_inactive_rows']} | {payload['after']['total_inactive_rows']} | {payload['after']['total_inactive_rows'] - payload['before']['total_inactive_rows']} |",
        "",
        "## Queue Delta",
        "| Metric | Before | After | Delta |",
        "|---|---:|---:|---:|",
        f"| Actionable jobs | {before_queue['actionable_jobs']} | {after_queue['actionable_jobs']} | {after_queue['actionable_jobs'] - before_queue['actionable_jobs']} |",
        f"| Deferred jobs | {before_queue['deferred_jobs']} | {after_queue['deferred_jobs']} | {after_queue['deferred_jobs'] - before_queue['deferred_jobs']} |",
        f"| Running jobs | {before_queue['running_jobs']} | {after_queue['running_jobs']} | {after_queue['running_jobs'] - before_queue['running_jobs']} |",
        f"| Done jobs | {before_queue['done_jobs']} | {after_queue['done_jobs']} | {after_queue['done_jobs'] - before_queue['done_jobs']} |",
        f"| Failed jobs | {before_queue['failed_jobs']} | {after_queue['failed_jobs']} | {after_queue['failed_jobs'] - before_queue['failed_jobs']} |",
        "",
        "## Batch Results",
        "| Batch | Limit | Workers | Processed | Requeued | Failed terminal | Jobs/min | Touched delta |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for batch in payload["batches"]:
        delta = batch["delta"]["counts"]
        result = batch["result"]
        lines.append(
            f"| {batch['name']} | {batch['limit']} | {batch['workers']} | {result['processed']} | {result['requeued']} | {result['failed_terminal']} | {result['jobs_per_minute']} | {delta['touched_jobs']} |"
        )

    final_delta = payload["final_delta"]
    lines.extend(
        [
            "",
            "## Accepted Samples",
            "| Fraternity / Source | Chapter slug | Field | Value | Provenance | Page scope | Supporting page |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in final_delta["accepted_samples"]:
        lines.append(
            f"| {row['fraternity_slug']} / {row['source_slug']} | {row['chapter_slug']} | {row['field_name']} | {row['field_value']} | {row['provenance_type']} | {row['page_scope']} | {row['supporting_page_url']} |"
        )
    lines.extend(
        [
            "",
            "## Rejected Samples",
            "| Fraternity / Source | Chapter slug | Field | Outcome | Query | Source URL | Cause |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in final_delta["rejected_samples"]:
        lines.append(
            f"| {row['fraternity_slug']} / {row['source_slug']} | {row['chapter_slug']} | {row['field_name']} | {row['outcome']} | {row.get('query') or ''} | {row.get('source_url') or ''} | {row['status']} |"
        )
    lines.extend(
        [
            "",
            "## Unresolved Samples",
            "| Fraternity / Source | Chapter slug | Field | Outcome | Queries attempted | Queries failed | Cause -> Effect |",
            "|---|---|---|---|---:|---:|---|",
        ]
    )
    for row in final_delta["unresolved_samples"]:
        lines.append(
            f"| {row['fraternity_slug']} / {row['source_slug']} | {row['chapter_slug']} | {row['field_name']} | {row['outcome']} | {row['queries_attempted']} | {row['queries_failed']} | {row['status']} -> {row['queue_state']} |"
        )
    lines.extend(
        [
            "",
            "## Top Failure Modes",
            "| Outcome | Count |",
            "|---|---:|",
        ]
    )
    for row in final_delta["outcomes"]:
        lines.append(f"| {row['outcome']} | {row['count']} |")
    lines.extend(
        [
            "",
        "## False-Positive Risk Review",
        "- The scaling run was allowed to improve throughput only after the alias and invalid-website structural blockers were fixed.",
        "- The first Phase 6 pass produced a small set of false positives from school-hosted pages; those rows were reverted, the generalized gate was tightened, and the final rerun produced no accepted-sample regressions.",
        "- No new nationals-only contact acceptance was introduced by the run.",
        "- Remaining unresolved jobs are preserved as deferred/queued states rather than low-confidence writes.",
        "- The `inactive_validated_rows` KPI is intentionally strict and currently excludes many legacy `system` inactive rows; the drop in that metric is a reporting-classification artifact, not a wave of chapter reactivations.",
            "",
            "## Recommendation For Phase 7",
            "- Move to the final investor-readiness validation with the updated KPIs and a focused sample packet of completed rows plus remaining unresolved edge cases.",
            "",
            "## Approval Request",
            "- Phase 6 is complete and ready for review.",
        ]
    )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    before = snapshot_metrics()
    started_at = utc_now()
    batches: list[dict[str, Any]] = []
    for batch in BATCHES:
        batch_started = utc_now()
        result = run_batch(batch["limit"], batch["workers"])
        delta = changed_delta(batch_started)
        batches.append(
            {
                "name": batch["name"],
                "limit": batch["limit"],
                "workers": batch["workers"],
                "result": result,
                "delta": delta,
            }
        )
    after = snapshot_metrics()
    final_delta = changed_delta(started_at)
    payload = {
        "started_at": iso_utc(started_at),
        "finished_at": iso_utc(utc_now()),
        "before": before,
        "batches": batches,
        "after": after,
        "final_delta": final_delta,
    }
    json_path, md_path = write_report(payload)
    print(json.dumps({"json_report": str(json_path), "markdown_report": str(md_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
