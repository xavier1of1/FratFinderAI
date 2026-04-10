from __future__ import annotations

import json
import re
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
CRAWLER_SRC = ROOT / "services" / "crawler" / "src"
if str(CRAWLER_SRC) not in sys.path:
    sys.path.insert(0, str(CRAWLER_SRC))

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.db.repository import CrawlerRepository

OUT_DIR = ROOT / "docs" / "SystemReport"
DATE_TAG = "2026-04-09"
TRUSTED_TYPES = {"chapter_specific", "school_specific", "national_specific_to_chapter"}
GENERIC_EMAIL_PATTERNS = (
    "greeks@",
    "fsl@",
    "sfl@",
    "admission@",
    "admissions@",
    "enroll@",
    "orientation@",
    "studentlife@",
    "student-life@",
)
GENERIC_EMAIL_LOCAL_MARKERS = (
    "admission",
    "admissions",
    "enroll",
    "greek",
    "office",
    "orientation",
    "panhellenic",
    "studentaffairs",
    "studentengagement",
    "studentgov",
    "studentlife",
    "student-life",
    "sfl",
    "fsl",
)
SUSPICIOUS_WEBSITE_MARKERS = (
    "campuslabs.com/engage",
    "presence.io/organization",
    "/book/export/html/",
)
SUSPICIOUS_INSTAGRAM_MARKERS = (
    "instagram.com/node",
    "instagram.com/umbraco.cms.core.models.link",
    "instagram.com/accounts/login",
)
_SCHOOL_ALIAS_STOPWORDS = {"university", "college", "institute", "school", "of", "the", "at", "state", "and"}
GENERIC_NATIONAL_WEBSITE_MARKERS = (
    "/find-a-chapter",
    "/join-a-chapter",
    "/join-tke/find-a-chapter",
)
SUSPICIOUS_IDENTITY_MARKERS = (
    "chapter map",
    "follow us on social media",
    "how to join",
    "select content",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _is_suspicious_instagram(url: str) -> bool:
    lowered = _norm(url).lower()
    return any(marker in lowered for marker in SUSPICIOUS_INSTAGRAM_MARKERS)


def _is_suspicious_website(url: str) -> bool:
    lowered = _norm(url).lower()
    return any(marker in lowered for marker in SUSPICIOUS_WEBSITE_MARKERS)


def _is_generic_school_email(email: str) -> bool:
    lowered = _norm(email).lower()
    if any(pattern in lowered for pattern in GENERIC_EMAIL_PATTERNS):
        return True
    local_part = lowered.split("@", 1)[0]
    return any(marker in local_part for marker in GENERIC_EMAIL_LOCAL_MARKERS)


def _school_aliases(school_name: str | None) -> set[str]:
    normalized = re.sub(r"[^a-z0-9]+", " ", _norm(school_name).lower()).strip()
    if not normalized:
        return set()
    broad_tokens = [token for token in normalized.split() if token and token not in {"of", "the", "at", "and"}]
    filtered_tokens = [token for token in normalized.split() if token and token not in _SCHOOL_ALIAS_STOPWORDS]
    aliases: set[str] = set()
    aliases.update(token[:6] for token in filtered_tokens if len(token) >= 4)
    if len(filtered_tokens) >= 2:
        aliases.add("".join(token[0] for token in filtered_tokens[:4]))
    if len(broad_tokens) >= 2:
        aliases.add("".join(token[0] for token in broad_tokens[:4]))
    return {alias for alias in aliases if len(alias) >= 2}


def _email_local_has_identity(row: dict[str, Any], email: str) -> bool:
    local_part = re.sub(r"[^a-z0-9]+", "", email.split("@", 1)[0].lower())
    fraternity = re.sub(r"[^a-z0-9]+", "", _norm(row.get("fraternity_slug")).lower())
    if fraternity and fraternity in local_part:
        return True
    for token in re.sub(r"[^a-z0-9]+", " ", _norm(row.get("name")).lower()).split():
        if len(token) >= 4 and token in local_part:
            return True
    return False


def _chapter_tokens(row: dict[str, Any]) -> list[str]:
    return [
        token
        for token in re.sub(r"[^a-z0-9]+", " ", _norm(row.get("name")).lower()).split()
        if len(token) >= 4 and token not in _SCHOOL_ALIAS_STOPWORDS
    ]


def _is_generic_national_instagram_risk(row: dict[str, Any]) -> bool:
    instagram = _norm(row.get("instagram_url")).lower()
    if not instagram:
        return False
    handle = instagram.rstrip("/").rsplit("/", 1)[-1]
    compact_handle = re.sub(r"[^a-z0-9]+", "", handle)
    if not compact_handle:
        return True
    compact_fraternity = re.sub(r"[^a-z0-9]+", "", _norm(row.get("fraternity_slug")).lower())
    school_aliases = _school_aliases(row.get("university_name"))
    chapter_tokens = _chapter_tokens(row)
    has_school_signal = any(alias in compact_handle for alias in school_aliases if alias)
    has_chapter_signal = any(token in compact_handle for token in chapter_tokens)
    if handle.endswith(".org") and not has_school_signal and not has_chapter_signal:
        return True
    if compact_fraternity and compact_handle in {compact_fraternity, f"{compact_fraternity}hq"}:
        return True
    return False


def _identity_looks_suspicious(row: dict[str, Any]) -> bool:
    university = _norm(row.get("university_name"))
    combined = " ".join(
        _norm(row.get(key)).lower()
        for key in ("name", "university_name", "chapter_slug")
        if _norm(row.get(key))
    )
    if not university:
        return True
    if any(marker in combined for marker in SUSPICIOUS_IDENTITY_MARKERS):
        return True
    return len(university.split()) > 10


def _is_cross_school_email_risk(row: dict[str, Any]) -> bool:
    email = _norm(row.get("contact_email")).lower()
    if "@" not in email:
        return False
    email_type = _norm(row.get("email_type"))
    if email_type not in TRUSTED_TYPES:
        return False
    local_part, domain = email.split("@", 1)
    if not domain.endswith(".edu"):
        return False
    school_aliases = _school_aliases(row.get("university_name"))
    if school_aliases and any(alias in domain for alias in school_aliases):
        return False
    support_host = (urlparse(_norm(row.get("email_support"))).netloc or "").lower()
    website_host = (urlparse(_norm(row.get("website_url"))).netloc or "").lower()
    if support_host and domain in support_host:
        return False
    if website_host and domain in website_host:
        return False
    return not _email_local_has_identity(row, email)


def _safe_email(row: dict[str, Any]) -> bool:
    email = _norm(row.get("contact_email"))
    email_type = _norm(row.get("email_type"))
    support = _norm(row.get("email_support"))
    if not email or email_type not in TRUSTED_TYPES:
        return False
    if _is_generic_school_email(email):
        return False
    if _is_cross_school_email_risk(row):
        return False
    if not support:
        return False
    return True


def _safe_instagram(row: dict[str, Any]) -> bool:
    insta = _norm(row.get("instagram_url"))
    insta_type = _norm(row.get("insta_type"))
    support = _norm(row.get("insta_support"))
    if not insta or insta_type not in TRUSTED_TYPES:
        return False
    if _is_suspicious_instagram(insta):
        return False
    if _is_generic_national_instagram_risk(row):
        return False
    if not support:
        return False
    return True


def _safe_website_for_demo(row: dict[str, Any]) -> bool:
    website = _norm(row.get("website_url"))
    if not website:
        return True
    if _is_suspicious_website(website):
        return False
    lowered = website.lower()
    if any(marker in lowered for marker in GENERIC_NATIONAL_WEBSITE_MARKERS):
        return False
    # Allow chapter-specific nationals pages and chapter sites; avoid obviously generic roots.
    support = _norm(row.get("website_support"))
    website_type = _norm(row.get("website_type"))
    if website_type in TRUSTED_TYPES:
        return True
    if support and not _is_suspicious_website(support):
        return True
    return True


def _showcase_ready(row: dict[str, Any]) -> bool:
    if not _presentation_safe_complete(row):
        return False
    website = _norm(row.get("website_url")).lower()
    if website.endswith(".pdf") or "judicial" in website or "constitution" in website:
        return False
    safe_email = _safe_email(row)
    safe_instagram = _safe_instagram(row)
    email_type = _norm(row.get("email_type"))
    insta_type = _norm(row.get("insta_type"))
    preferred_contact = (
        (safe_email and email_type in {"chapter_specific", "school_specific"})
        or (safe_instagram and insta_type in {"chapter_specific", "school_specific"})
    )
    if not preferred_contact:
        return False
    if not website:
        return True
    website_type = _norm(row.get("website_type"))
    if website_type in {"chapter_specific", "school_specific", "national_specific_to_chapter"}:
        return True
    host_text = re.sub(r"[^a-z0-9]+", "", (urlparse(website).netloc or "").lower())
    if any(alias in host_text for alias in _school_aliases(row.get("university_name")) if alias):
        return True
    if any(token in host_text for token in _chapter_tokens(row)):
        return True
    return False


def _presentation_safe_complete(row: dict[str, Any]) -> bool:
    if _norm(row.get("chapter_status")) != "active":
        return False
    if _identity_looks_suspicious(row):
        return False
    if not _safe_website_for_demo(row):
        return False
    return _safe_email(row) or _safe_instagram(row)


def _row_score(row: dict[str, Any]) -> tuple[int, int, str, str]:
    safe_email = 1 if _safe_email(row) else 0
    safe_instagram = 1 if _safe_instagram(row) else 0
    safe_website = 1 if _safe_website_for_demo(row) and _norm(row.get("website_url")) else 0
    return (safe_email + safe_instagram + safe_website, safe_email + safe_instagram, _norm(row.get("fraternity_slug")), _norm(row.get("chapter_slug")))


def _fetch_rows() -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    settings = get_settings()
    with get_connection(settings) as conn:
        repo = CrawlerRepository(conn)
        metrics = asdict(repo.get_accuracy_recovery_metrics())
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_count,
                  COUNT(*) FILTER (WHERE status = 'queued' AND queue_state = 'deferred')::int AS deferred_count
                FROM field_jobs
                """
            )
            queue_snapshot = dict(cur.fetchone())
            cur.execute(
                """
                SELECT
                  f.slug AS fraternity_slug,
                  c.slug AS chapter_slug,
                  c.name,
                  c.university_name,
                  c.chapter_status,
                  c.website_url,
                  c.contact_email,
                  c.instagram_url,
                  c.field_states,
                  c.contact_provenance,
                  COALESCE(c.contact_provenance -> 'website_url' ->> 'contactProvenanceType','') AS website_type,
                  COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType','') AS email_type,
                  COALESCE(c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType','') AS insta_type,
                  COALESCE(c.contact_provenance -> 'website_url' ->> 'supportingPageUrl','') AS website_support,
                  COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageUrl','') AS email_support,
                  COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageUrl','') AS insta_support
                FROM chapters c
                JOIN fraternities f ON f.id = c.fraternity_id
                ORDER BY f.slug, c.slug
                """
            )
            chapter_rows = [dict(r) for r in cur.fetchall()]
            cur.execute(
                """
                WITH latest_status AS (
                  SELECT DISTINCT ON (ce.chapter_id)
                    ce.chapter_id,
                    ce.source_url,
                    ce.metadata
                  FROM chapter_evidence ce
                  WHERE ce.field_name = 'chapter_status'
                  ORDER BY ce.chapter_id, ce.created_at DESC
                )
                SELECT
                  f.slug AS fraternity_slug,
                  c.slug AS chapter_slug,
                  c.name,
                  c.university_name,
                  c.chapter_status,
                  COALESCE(c.contact_provenance -> 'chapter_status' ->> 'sourceType', latest_status.metadata ->> 'evidenceSourceType', '') AS source_type,
                  COALESCE(c.contact_provenance -> 'chapter_status' ->> 'reasonCode', latest_status.metadata ->> 'reasonCode', '') AS reason_code,
                  COALESCE(c.contact_provenance -> 'chapter_status' ->> 'supportingPageUrl', latest_status.source_url, '') AS evidence_url
                FROM chapters c
                JOIN fraternities f ON f.id = c.fraternity_id
                LEFT JOIN latest_status ON latest_status.chapter_id = c.id
                WHERE c.chapter_status = 'inactive'
                ORDER BY c.updated_at DESC, c.slug
                """
            )
            inactive_rows = [dict(r) for r in cur.fetchall()]
            cur.execute(
                """
                SELECT
                  fr.slug AS fraternity_slug,
                  COALESCE(fj.payload ->> 'sourceSlug', s.slug, '') AS source_slug,
                  c.slug AS chapter_slug,
                  fj.field_name,
                  COALESCE(fj.last_error, fj.completed_payload ->> 'status', fj.terminal_outcome, '') AS outcome,
                  COALESCE(fj.queue_state, 'actionable') AS queue_state,
                  COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'attempted')::int, 0) AS queries_attempted,
                  COALESCE((fj.completed_payload -> 'decision_trace' -> 'search' ->> 'failed')::int, 0) AS queries_failed,
                  fj.updated_at
                FROM field_jobs fj
                JOIN chapters c ON c.id = fj.chapter_id
                JOIN fraternities fr ON fr.id = c.fraternity_id
                LEFT JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
                LEFT JOIN sources s ON s.id = cr.source_id
                WHERE fj.status = 'queued'
                ORDER BY fj.priority DESC, fj.updated_at DESC
                LIMIT 30
                """
            )
            unresolved_rows = [dict(r) for r in cur.fetchall()]
    metrics["actionable_queue_count"] = int(queue_snapshot.get("actionable_count") or 0)
    metrics["deferred_queue_count"] = int(queue_snapshot.get("deferred_count") or 0)
    return metrics, chapter_rows, inactive_rows, unresolved_rows


def _top_risk_examples(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    risks: list[dict[str, Any]] = []
    for row in rows:
        email = _norm(row.get("contact_email"))
        insta = _norm(row.get("instagram_url"))
        website = _norm(row.get("website_url"))
        reasons: list[str] = []
        if _is_suspicious_instagram(insta):
            reasons.append("instagram_placeholder")
        if _is_suspicious_website(website):
            reasons.append("generic_org_portal_website")
        if _is_generic_school_email(email):
            reasons.append("generic_school_office_email")
        if _is_cross_school_email_risk(row):
            reasons.append("cross_school_email")
        if _is_generic_national_instagram_risk(row):
            reasons.append("generic_national_instagram")
        if _identity_looks_suspicious(row):
            reasons.append("suspicious_identity")
        if reasons:
            risks.append(
                {
                    "fraternity_slug": row["fraternity_slug"],
                    "chapter_slug": row["chapter_slug"],
                    "name": row["name"],
                    "university_name": row["university_name"],
                    "contact_email": row["contact_email"],
                    "instagram_url": row["instagram_url"],
                    "website_url": row["website_url"],
                    "risk_reasons": reasons,
                }
            )
    return risks


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUT_DIR / f"PHASE_7_INVESTOR_READINESS_REPORT_{DATE_TAG}.md"
    packet_path = OUT_DIR / f"PHASE_7_INVESTOR_SAMPLE_PACKET_{DATE_TAG}.json"
    notes_path = OUT_DIR / f"PHASE_7_DEMO_NOTES_{DATE_TAG}.md"

    metrics, rows, inactive_rows, unresolved_rows = _fetch_rows()

    presentation_safe_rows = [row for row in rows if _presentation_safe_complete(row)]
    presentation_safe_rows.sort(key=_row_score, reverse=True)
    showcase_rows = [row for row in presentation_safe_rows if _showcase_ready(row)]
    showcase_rows.sort(key=_row_score, reverse=True)

    national_chapter_rows = [
        row
        for row in presentation_safe_rows
        if row["email_type"] == "national_specific_to_chapter"
        or row["insta_type"] == "national_specific_to_chapter"
        or row["website_type"] == "national_specific_to_chapter"
    ]
    inactive_validated_rows = [
        row for row in inactive_rows if _norm(row.get("source_type")) in {"official_school", "school_activity_validation", "school_policy_validation"}
    ]
    risky_rows = _top_risk_examples(rows)

    risk_counts = {
        "presentation_safe_complete_rows": len(presentation_safe_rows),
        "system_complete_rows": metrics["complete_rows"],
        "presentation_gap": metrics["complete_rows"] - len(presentation_safe_rows),
        "instagram_placeholder_rows": sum(1 for row in rows if _is_suspicious_instagram(_norm(row.get("instagram_url")))),
        "generic_org_portal_websites": sum(1 for row in rows if _is_suspicious_website(_norm(row.get("website_url")))),
        "generic_school_office_emails": sum(1 for row in rows if _is_generic_school_email(_norm(row.get("contact_email")))),
        "cross_school_email_risks": sum(1 for row in rows if _is_cross_school_email_risk(row)),
        "active_rows_with_any_contact": metrics["active_rows_with_any_contact"],
    }

    packet = {
        "generated_at": now_iso(),
        "system_metrics": metrics,
        "presentation_metrics": risk_counts,
        "accepted_samples": showcase_rows[:25],
        "inactive_validated_samples": inactive_validated_rows[:15],
        "national_chapter_page_samples": national_chapter_rows[:20],
        "unresolved_samples": unresolved_rows[:15],
        "top_risk_samples": risky_rows[:25],
    }
    packet_path.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")

    lines = [
        "# Phase 7 Investor Readiness Report",
        "",
        f"Generated: `{packet['generated_at']}`",
        "",
        "## Executive Read",
        "- This packet distinguishes between `system complete` and `presentation safe` rows.",
        "- `system complete` is the program metric used through the approval-gated plan.",
        "- `presentation safe` is stricter and excludes rows with placeholder Instagram handles, generic org-portal websites, or other obviously weak visible fields.",
        "- The curated accepted sample uses a `supporting page` column, because some strong rows are backed by official school or national chapter pages rather than a chapter-owned website.",
        "- The environment is demo-safe only if we present the curated sample packet and avoid treating the whole dataset as uniformly investor-ready.",
        "",
        "## Core Metrics",
        "| Metric | Value |",
        "|---|---:|",
        f"| Total chapters | {metrics['total_chapters']} |",
        f"| Active rows | {sum(1 for row in rows if _norm(row.get('chapter_status')) == 'active')} |",
        f"| Inactive rows | {sum(1 for row in rows if _norm(row.get('chapter_status')) == 'inactive')} |",
        f"| System complete rows | {metrics['complete_rows']} |",
        f"| Presentation-safe complete rows | {risk_counts['presentation_safe_complete_rows']} |",
        f"| Presentation gap | {risk_counts['presentation_gap']} |",
        f"| Active rows with any contact | {metrics['active_rows_with_any_contact']} |",
        f"| Active rows with chapter-specific email | {metrics['active_rows_with_chapter_specific_email']} |",
        f"| Active rows with chapter-specific Instagram | {metrics['active_rows_with_chapter_specific_instagram']} |",
        f"| Nationals-only contact rows | {metrics['nationals_only_contact_rows']} |",
        f"| Validated inactive rows (strict KPI) | {metrics['inactive_validated_rows']} |",
        "",
        "## Queue Snapshot",
        "| Queue state | Count |",
        "|---|---:|",
        f"| actionable | {metrics['actionable_queue_count']} |",
        f"| deferred | {metrics['deferred_queue_count']} |",
        "",
        "## Presentation Risks Still In Data",
        "| Risk category | Count |",
        "|---|---:|",
        f"| Placeholder / broken Instagram handles | {risk_counts['instagram_placeholder_rows']} |",
        f"| Generic org-portal websites still visible on rows | {risk_counts['generic_org_portal_websites']} |",
        f"| Generic school-office emails still visible on rows | {risk_counts['generic_school_office_emails']} |",
        f"| Cross-school trusted email risks | {risk_counts['cross_school_email_risks']} |",
        "",
        "## Demo Recommendation",
        "- Show curated rows from the sample packet only.",
        f"- If asked about coverage, say the system currently has `{metrics['complete_rows']}` strict complete rows and `{risk_counts['presentation_safe_complete_rows']}` presentation-safe complete rows, with `{metrics['active_rows_with_any_contact']}` active rows containing at least some contact signal.",
        "- Do not claim a few thousand completed rows. That target is not met in the live environment.",
        "- Lean on explainability, provenance, chapter-specific national-page handling, and inactive validation rather than raw coverage.",
        "",
        "## Accepted Sample Preview",
        "| Fraternity | Chapter slug | University | Safe email | Safe Instagram | Supporting page |",
        "|---|---|---|---|---|---|",
    ]

    for row in showcase_rows[:15]:
        lines.append(
            f"| {row['fraternity_slug']} | {row['chapter_slug']} | {row['university_name']} | {row['contact_email'] or ''} | {row['instagram_url'] or ''} | {row['website_url'] or ''} |"
        )

    lines.extend(
        [
            "",
            "## Validated Inactive Preview",
            "| Fraternity | Chapter slug | University | Source type | Evidence URL |",
            "|---|---|---|---|---|",
        ]
    )
    for row in inactive_validated_rows[:10]:
        lines.append(
            f"| {row['fraternity_slug']} | {row['chapter_slug']} | {row['university_name'] or ''} | {row['source_type']} | {row['evidence_url'] or ''} |"
        )

    lines.extend(
        [
            "",
            "## Unresolved Preview",
            "| Fraternity / Source | Chapter slug | Field | Outcome | Queue state | Queries attempted |",
            "|---|---|---|---|---|---:|",
        ]
    )
    for row in unresolved_rows[:10]:
        lines.append(
            f"| {row['fraternity_slug']} / {row['source_slug']} | {row['chapter_slug']} | {row['field_name']} | {row['outcome']} | {row['queue_state']} | {row['queries_attempted']} |"
        )

    lines.extend(
        [
            "",
            "## Top Risks To Avoid Showing",
            "| Fraternity | Chapter slug | University | Risk reasons |",
            "|---|---|---|---|",
        ]
    )
    for row in risky_rows[:15]:
        lines.append(
            f"| {row['fraternity_slug']} | {row['chapter_slug']} | {row['university_name'] or ''} | {', '.join(row['risk_reasons'])} |"
        )

    lines.extend(
        [
            "",
            "## Final Call",
            "- Demo-ready for a curated, accuracy-first investor walkthrough.",
            "- Not demo-ready for unrestricted browsing of all populated rows.",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")

    notes = [
        "# Phase 7 Demo Notes",
        "",
        "## What To Show",
        "- The Agent Ops / accuracy metrics surfaces for the high-level counts.",
        "- The Nationals page to explain how national truth is separated from chapter truth.",
        f"- Curated rows from `{packet_path.name}` only.",
        "- A few validated inactive examples with official-school evidence.",
        "",
        "## What To Avoid",
        "- Do not browse arbitrary populated rows live.",
        "- Do not present placeholder Instagram rows such as `instagram.com/node`.",
        "- Do not use generic org-portal websites as proof of chapter-owned web presence.",
        "- Treat the curated sample packet's last URL as a supporting page, not automatically as a chapter-owned website.",
        "- Do not claim that the system has already reached a few thousand complete rows.",
        "",
        "## Suggested Framing",
        "- Emphasize that the system now prefers explainable, chapter-specific truth over inflated coverage.",
        "- Explain that generic nationals contact is no longer allowed to masquerade as chapter contact.",
        "- Explain that some approved rows are supported by official school pages or chapter-specific national pages when a chapter-owned site does not exist.",
        "- Explain that remaining backlog is mostly provider-degraded search, website prerequisites, and repair debt.",
        "",
        "## Numbers To Use",
        f"- Strict complete rows: `{metrics['complete_rows']}`",
        f"- Presentation-safe complete rows: `{risk_counts['presentation_safe_complete_rows']}`",
        f"- Active rows with any contact signal: `{metrics['active_rows_with_any_contact']}`",
        f"- Validated inactive rows (strict KPI): `{metrics['inactive_validated_rows']}`",
    ]
    notes_path.write_text("\n".join(notes), encoding="utf-8")

    print(json.dumps({"report": str(report_path), "packet": str(packet_path), "notes": str(notes_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
