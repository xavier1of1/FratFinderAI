from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "crawler" / "src"))

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.db.repository import CrawlerRepository


def main() -> None:
    settings = get_settings()
    output_dir = ROOT / "docs" / "SystemReport"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"PHASE_0_BASELINE_{datetime.now().strftime('%Y-%m-%d')}.md"

    with get_connection(settings) as connection:
        repository = CrawlerRepository(connection)
        metrics = repository.get_accuracy_recovery_metrics()
        national_profiles = repository.list_national_profiles(limit=1000)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued')::int AS queued_jobs,
                  (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
                  (SELECT COUNT(*) FROM field_jobs WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
                  (SELECT COUNT(*) FROM field_jobs WHERE status = 'running')::int AS running_jobs,
                  (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'updated')::int AS updated_jobs,
                  (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'review_required')::int AS review_jobs,
                  (SELECT COUNT(*) FROM field_jobs WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'terminal_no_signal')::int AS terminal_no_signal_jobs,
                  (SELECT COUNT(*) FROM chapters WHERE chapter_status = 'inactive')::int AS inactive_rows,
                  (SELECT COUNT(*) FROM chapters WHERE COALESCE(field_states ->> 'website_url', '') = 'confirmed_absent')::int AS confirmed_absent_website_rows,
                  (
                    SELECT COUNT(*)
                    FROM chapters
                    WHERE (contact_email IS NOT NULL OR instagram_url IS NOT NULL)
                      AND NOT (contact_provenance ? 'contact_email' OR contact_provenance ? 'instagram_url')
                  )::int AS rows_needing_provenance_backfill
                """
            )
            summary = cursor.fetchone()
            cursor.execute(
                """
                SELECT
                  COALESCE(metadata ->> 'reasonCode', 'unknown') AS reason_code,
                  COUNT(*)::int AS count
                FROM chapter_evidence
                GROUP BY 1
                ORDER BY 2 DESC, 1 ASC
                LIMIT 12
                """
            )
            reason_rows = cursor.fetchall()

    national_with_email = sum(1 for item in national_profiles if item.contact_email)
    national_with_instagram = sum(1 for item in national_profiles if item.instagram_url)
    national_with_phone = sum(1 for item in national_profiles if item.phone)

    lines = [
        "# Phase 0 Baseline",
        "",
        "## Goal",
        "- Lock the live baseline before the later accuracy-recovery mutations widen.",
        "",
        "## Locked Definitions",
        "- `complete_row`: active chapter plus at least one accurate chapter-supported email or Instagram.",
        "- `chapter_specific_contact_row`: chapter row supported by `chapter_specific`, `school_specific`, or `national_specific_to_chapter` evidence.",
        "- `nationals_only_contact_row`: chapter row whose present contact data is only supported by `national_generic` evidence.",
        "- `inactive_validated_row`: inactive chapter with school/activity validation evidence.",
        "- `confirmed_absent_website_row`: website intentionally resolved absent rather than merely missing.",
        "",
        "## Baseline Metrics",
        "| Metric | Value |",
        "|---|---:|",
        f"| Total chapters | {metrics.total_chapters} |",
        f"| Complete rows | {metrics.complete_rows} |",
        f"| Chapter-specific contact rows | {metrics.chapter_specific_contact_rows} |",
        f"| Nationals-only contact rows | {metrics.nationals_only_contact_rows} |",
        f"| Validated inactive rows | {metrics.inactive_validated_rows} |",
        f"| Confirmed-absent website rows | {metrics.confirmed_absent_website_rows} |",
        f"| Active rows with chapter email | {metrics.active_rows_with_chapter_specific_email} |",
        f"| Active rows with chapter Instagram | {metrics.active_rows_with_chapter_specific_instagram} |",
        f"| Active rows with any contact | {metrics.active_rows_with_any_contact} |",
        "",
        "## Queue Snapshot",
        "| Metric | Value |",
        "|---|---:|",
        f"| Queued field jobs | {int(summary['queued_jobs'] or 0)} |",
        f"| Actionable field jobs | {int(summary['actionable_jobs'] or 0)} |",
        f"| Deferred field jobs | {int(summary['deferred_jobs'] or 0)} |",
        f"| Running field jobs | {int(summary['running_jobs'] or 0)} |",
        f"| Updated field jobs | {int(summary['updated_jobs'] or 0)} |",
        f"| Review-required field jobs | {int(summary['review_jobs'] or 0)} |",
        f"| Terminal-no-signal field jobs | {int(summary['terminal_no_signal_jobs'] or 0)} |",
        "",
        "## Nationals Profile Coverage",
        "| Metric | Value |",
        "|---|---:|",
        f"| Nationals profiles | {len(national_profiles)} |",
        f"| Nationals with email | {national_with_email} |",
        f"| Nationals with Instagram | {national_with_instagram} |",
        f"| Nationals with phone | {national_with_phone} |",
        "",
        "## Instrumentation Gaps",
        "| Metric | Value |",
        "|---|---:|",
        f"| Inactive chapter rows | {int(summary['inactive_rows'] or 0)} |",
        f"| Rows needing provenance backfill | {int(summary['rows_needing_provenance_backfill'] or 0)} |",
        "",
        "## Top Evidence Reason Codes",
        "| Reason code | Count |",
        "|---|---:|",
    ]
    lines.extend(f"| {row['reason_code']} | {int(row['count'] or 0)} |" for row in reason_rows)
    lines.extend(
        [
            "",
            "## Locked Cohort Bounds",
            "- Analysis cohort size: `25`",
            "- Write cohort size: `100`",
            "- Accepted / rejected / unresolved sample pack target: `10 / 10 / 10`",
            "- Rollback rule: stop widening if `nationals_only_contact_row` increases or accepted-sample precision regresses.",
            "",
            "## Notes",
            "- These metrics are now backed by first-class `contact_provenance` instrumentation where available.",
            "- Legacy chapter rows without the new provenance envelope are counted conservatively and surfaced as `rows needing provenance backfill`.",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
