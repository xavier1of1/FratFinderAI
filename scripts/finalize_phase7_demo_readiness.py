from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
CRAWLER_SRC = ROOT / "services" / "crawler" / "src"
if str(CRAWLER_SRC) not in sys.path:
    sys.path.insert(0, str(CRAWLER_SRC))

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.db.connection import get_connection

DATE_TAG = "2026-04-09"
OUT_DIR = ROOT / "docs" / "SystemReport"

_INSTAGRAM_PLACEHOLDER_MARKERS = (
    "instagram.com/node",
    "instagram.com/accounts/login",
    "instagram.com/umbraco.cms.core.models.link",
)
_WEBSITE_EXPORT_MARKERS = ("/book/export/html/",)
_SCHOOL_ALIAS_STOPWORDS = {"university", "college", "institute", "school", "of", "the", "at", "state", "and"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize(value: Any) -> str:
    return str(value or "").strip()


def _school_aliases(school_name: str | None) -> set[str]:
    normalized = re.sub(r"[^a-z0-9]+", " ", _normalize(school_name).lower()).strip()
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


def _email_local_has_identity(local_part: str, fraternity_slug: str | None, chapter_name: str | None) -> bool:
    compact_local = re.sub(r"[^a-z0-9]+", "", _normalize(local_part).lower())
    compact_fraternity = re.sub(r"[^a-z0-9]+", "", _normalize(fraternity_slug).lower())
    if compact_fraternity and compact_fraternity in compact_local:
        return True
    for token in re.sub(r"[^a-z0-9]+", " ", _normalize(chapter_name).lower()).split():
        if len(token) >= 4 and token in compact_local:
            return True
    return False


def _rejected_provenance(field_name: str, previous_value: str | None, supporting_url: str | None, reason_code: str) -> dict[str, Any]:
    supporting_scope = "unrelated"
    if field_name == "contact_email":
        contact_type = "ambiguous"
    elif field_name == "instagram_url":
        contact_type = "ambiguous"
    else:
        contact_type = "ambiguous"
    return {
        "updatedAt": _now_iso(),
        "confidence": 1.0,
        "reasonCode": reason_code,
        "sourceType": "phase7_demo_safety_cleanup",
        "decisionStage": "phase7_demo_safety_cleanup",
        "previousValue": previous_value,
        "candidateValue": None,
        "decisionOutcome": "rejected",
        "supportingPageUrl": supporting_url,
        "supportingPageScope": supporting_scope,
        "fieldResolutionState": "missing",
        "contactProvenanceType": contact_type,
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUT_DIR / f"PHASE_7_DEMO_SAFETY_CLEANUP_{DATE_TAG}.json"
    settings = get_settings()
    cleanup_rows: list[dict[str, Any]] = []

    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.id,
                  c.slug AS chapter_slug,
                  c.name,
                  c.university_name,
                  c.chapter_status,
                  c.website_url,
                  c.contact_email,
                  c.instagram_url,
                  c.field_states,
                  c.contact_provenance,
                  f.slug AS fraternity_slug,
                  COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageUrl','') AS email_support_url,
                  COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType','') AS email_contact_type
                FROM chapters c
                JOIN fraternities f ON f.id = c.fraternity_id
                WHERE c.chapter_status = 'active'
                  AND (
                    lower(COALESCE(c.instagram_url,'')) LIKE '%instagram.com/node%'
                    OR lower(COALESCE(c.instagram_url,'')) LIKE '%instagram.com/accounts/login%'
                    OR lower(COALESCE(c.instagram_url,'')) LIKE '%instagram.com/umbraco.cms.core.models.link%'
                    OR lower(COALESCE(c.website_url,'')) LIKE '%/book/export/html/%'
                    OR (
                      c.contact_email IS NOT NULL
                      AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType','') = 'national_specific_to_chapter'
                    )
                  )
                ORDER BY f.slug, c.slug
                """
            )
            rows = [dict(row) for row in cur.fetchall()]

            for row in rows:
                chapter_updates: dict[str, Any] = {}
                field_states = dict(row.get("field_states") or {})
                contact_provenance = dict(row.get("contact_provenance") or {})
                reasons: list[str] = []

                instagram_url = _normalize(row.get("instagram_url")).lower()
                if any(marker in instagram_url for marker in _INSTAGRAM_PLACEHOLDER_MARKERS):
                    chapter_updates["instagram_url"] = None
                    field_states["instagram_url"] = "missing"
                    contact_provenance["instagram_url"] = _rejected_provenance(
                        "instagram_url",
                        row.get("instagram_url"),
                        (
                            (contact_provenance.get("instagram_url") or {}).get("supportingPageUrl")
                            if isinstance(contact_provenance.get("instagram_url"), dict)
                            else None
                        ),
                        "phase7_placeholder_instagram",
                    )
                    reasons.append("placeholder_instagram")

                website_url = _normalize(row.get("website_url")).lower()
                if any(marker in website_url for marker in _WEBSITE_EXPORT_MARKERS):
                    chapter_updates["website_url"] = None
                    field_states["website_url"] = "missing"
                    contact_provenance["website_url"] = _rejected_provenance(
                        "website_url",
                        row.get("website_url"),
                        (
                            (contact_provenance.get("website_url") or {}).get("supportingPageUrl")
                            if isinstance(contact_provenance.get("website_url"), dict)
                            else row.get("website_url")
                        ),
                        "phase7_archival_export_website",
                    )
                    reasons.append("archival_export_website")

                contact_email = _normalize(row.get("contact_email")).lower()
                if "@" in contact_email and row.get("email_contact_type") == "national_specific_to_chapter" and row.get("university_name"):
                    local_part, domain = contact_email.split("@", 1)
                    school_aliases = _school_aliases(row.get("university_name"))
                    if (
                        domain.endswith(".edu")
                        and not any(alias and alias in domain for alias in school_aliases)
                        and not _email_local_has_identity(local_part, row.get("fraternity_slug"), row.get("name"))
                    ):
                        chapter_updates["contact_email"] = None
                        field_states["contact_email"] = "missing"
                        contact_provenance["contact_email"] = _rejected_provenance(
                            "contact_email",
                            row.get("contact_email"),
                            row.get("email_support_url"),
                            "phase7_cross_school_national_page_email",
                        )
                        reasons.append("cross_school_national_page_email")

                if not reasons:
                    continue

                cleanup_rows.append(
                    {
                        "fraternity_slug": row["fraternity_slug"],
                        "chapter_slug": row["chapter_slug"],
                        "reasons": reasons,
                        "before": {
                            "website_url": row.get("website_url"),
                            "contact_email": row.get("contact_email"),
                            "instagram_url": row.get("instagram_url"),
                        },
                    }
                )
                cur.execute(
                    """
                    UPDATE chapters
                    SET
                      website_url = CASE WHEN %(set_website)s THEN %(website_url)s ELSE website_url END,
                      contact_email = CASE WHEN %(set_email)s THEN %(contact_email)s ELSE contact_email END,
                      instagram_url = CASE WHEN %(set_instagram)s THEN %(instagram_url)s ELSE instagram_url END,
                      field_states = %(field_states)s,
                      contact_provenance = %(contact_provenance)s,
                      updated_at = NOW()
                    WHERE id = %(chapter_id)s
                    """,
                    {
                        "website_url": chapter_updates.get("website_url"),
                        "contact_email": chapter_updates.get("contact_email"),
                        "instagram_url": chapter_updates.get("instagram_url"),
                        "set_website": "website_url" in chapter_updates,
                        "set_email": "contact_email" in chapter_updates,
                        "set_instagram": "instagram_url" in chapter_updates,
                        "field_states": Jsonb(field_states),
                        "contact_provenance": Jsonb(contact_provenance),
                        "chapter_id": row["id"],
                    },
                )

        conn.commit()

    report_path.write_text(
        json.dumps(
            {
                "generated_at": _now_iso(),
                "rows_cleaned": len(cleanup_rows),
                "cleanup_rows": cleanup_rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps({"cleanup_report": str(report_path), "rows_cleaned": len(cleanup_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
