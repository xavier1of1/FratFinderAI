from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.types.json import Jsonb
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
CRAWLER_SRC = ROOT / "services" / "crawler" / "src"
if str(CRAWLER_SRC) not in sys.path:
    sys.path.insert(0, str(CRAWLER_SRC))

from fratfinder_crawler.field_jobs import (
    SearchDocument,
    _email_local_part_looks_generic_office,
    _email_looks_relevant_to_job,
    _instagram_handle_has_fraternity_token,
    _instagram_handle_looks_like_school_brand,
    _instagram_handle_looks_national_generic,
    _instagram_looks_relevant_to_job,
)
from fratfinder_crawler.models import ExtractedChapter, FieldJob
from fratfinder_crawler.normalization.normalizer import classify_chapter_validity
from fratfinder_crawler.precision_tools import tool_official_domain_verifier


DELETABLE_INVALID_REASONS = {
    "invalid_entity_legacy",
    "year_or_percentage_as_identity",
    "ranking_or_report_row",
    "history_or_timeline_row",
    "demographic_or_statistic_row",
    "award_or_honor_row",
    "school_division_or_department",
    "expansion_or_installment_row",
    "other_greek_organization_row",
    "navigation_or_chrome",
}

SAFE_WEBSITE_REJECTION_REASONS = {
    "missing_target_school_context",
    "cross_fraternity_conflict",
    "map_export_url",
    "archival_url",
    "low_signal_path",
    "blocked_host",
    "generic_school_root",
    "generic_school_directory",
    "page_missing",
}

_CANDIDATE_PAGE_CACHE: dict[str, dict[str, str] | None] = {}


def _source_class(url: str | None) -> str:
    lowered = str(url or "").lower()
    if "wikipedia.org/" in lowered:
        return "wider_web"
    if lowered.endswith(".edu") or ".edu/" in lowered:
        return "institutional"
    if lowered:
        return "national"
    return "national"


def _field_state(raw: dict[str, Any] | None, field_name: str) -> str:
    return str((raw or {}).get(field_name) or "").strip().lower()


def _all_contact_states_invalid(raw: dict[str, Any] | None) -> bool:
    return all(_field_state(raw, field_name) == "invalid_entity" for field_name in ("website_url", "instagram_url", "contact_email"))


def _trusted_provenance_type(raw: dict[str, Any] | None, field_name: str) -> bool:
    field_provenance = (raw or {}).get(field_name)
    if not isinstance(field_provenance, dict):
        return False
    specificity = str(field_provenance.get("contactProvenanceType") or "").strip()
    return specificity in {"chapter_specific", "school_specific", "national_specific_to_chapter"}


def _build_job(row: dict[str, Any], *, field_name: str) -> FieldJob:
    return FieldJob(
        id=f"cleanup-{field_name}-{row['id']}",
        chapter_id=str(row["id"]),
        chapter_slug=str(row["slug"] or ""),
        chapter_name=str(row["name"] or ""),
        field_name=field_name,
        payload={"candidateSchoolName": row.get("university_name") or ""},
        attempts=0,
        max_attempts=1,
        claim_token="cleanup",
        source_base_url=row.get("website_url") or row.get("national_url"),
        website_url=row.get("website_url"),
        instagram_url=row.get("instagram_url"),
        contact_email=row.get("contact_email"),
        fraternity_slug=row.get("fraternity_slug"),
        source_slug=row.get("source_slug"),
        university_name=row.get("university_name"),
        field_states=row.get("field_states") or {},
        chapter_status=str(row.get("chapter_status") or "active"),
    )


def _legacy_invalid_reason(row: dict[str, Any]) -> str | None:
    if _all_contact_states_invalid(row.get("field_states")):
        return "invalid_entity_legacy"

    record = ExtractedChapter(
        name=str(row.get("name") or ""),
        university_name=row.get("university_name"),
        website_url=row.get("website_url"),
        instagram_url=row.get("instagram_url"),
        contact_email=row.get("contact_email"),
        source_url=str(row.get("latest_source_url") or ""),
        source_snippet=row.get("latest_source_snippet"),
        source_confidence=float(row.get("latest_confidence") or 0.72),
    )
    decision = classify_chapter_validity(record, source_class=_source_class(row.get("latest_source_url")))
    if decision.validity_class == "invalid_non_chapter" and str(decision.invalid_reason or "") in DELETABLE_INVALID_REASONS:
        return str(decision.invalid_reason or "invalid_non_chapter")
    return None


def _supporting_document(row: dict[str, Any]) -> SearchDocument | None:
    text = str(row.get("latest_source_snippet") or "").strip()
    url = str(row.get("latest_source_url") or row.get("website_url") or row.get("national_url") or "").strip()
    if not text and not url:
        return None
    provider = "provenance"
    lowered_url = url.lower()
    if row.get("national_url") and str(row.get("national_url")).strip().lower() in lowered_url:
        provider = "nationals_directory"
    return SearchDocument(
        text=text,
        links=[],
        url=url or None,
        title="",
        provider=provider,
    )


def _candidate_page_context(url: str | None) -> dict[str, str] | None:
    normalized_url = str(url or "").strip()
    if not normalized_url:
        return None
    cache_key = normalized_url.rstrip("/").lower()
    if cache_key in _CANDIDATE_PAGE_CACHE:
        return _CANDIDATE_PAGE_CACHE[cache_key]
    try:
        response = requests.get(
            normalized_url,
            timeout=8,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (FratFinderAI cleanup)"},
        )
    except requests.RequestException:
        _CANDIDATE_PAGE_CACHE[cache_key] = None
        return None

    html = getattr(response, "text", "") or ""
    final_url = str(getattr(response, "url", "") or normalized_url)
    title = ""
    text = ""
    if html:
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        text = " ".join(soup.stripped_strings)
    payload = {
        "url": final_url,
        "title": title,
        "text": text,
        "html": html,
    }
    _CANDIDATE_PAGE_CACHE[cache_key] = payload
    return payload


def _legacy_email_quarantine_reason(row: dict[str, Any]) -> str | None:
    email = str(row.get("contact_email") or "").strip()
    if not email:
        return None
    if _trusted_provenance_type(row.get("contact_provenance"), "contact_email"):
        return None
    national_email = str(row.get("national_email") or "").strip().lower()
    if national_email and email.lower() == national_email:
        return "legacy_nationals_generic_contact"
    document = _supporting_document(row)
    if document is not None:
        relevant = _email_looks_relevant_to_job(email, _build_job(row, field_name="find_email"), document=document)
    else:
        relevant = _email_looks_relevant_to_job(email, _build_job(row, field_name="find_email"))
    if not relevant and _email_local_part_looks_generic_office(email):
        return "legacy_email_failed_chapter_specificity"
    return None


def _legacy_instagram_quarantine_reason(row: dict[str, Any]) -> str | None:
    instagram = str(row.get("instagram_url") or "").strip()
    if not instagram:
        return None
    if _trusted_provenance_type(row.get("contact_provenance"), "instagram_url"):
        return None
    national_instagram = str(row.get("national_instagram_url") or "").strip().rstrip("/").lower()
    if national_instagram and instagram.rstrip("/").lower() == national_instagram:
        return "legacy_nationals_generic_contact"
    job = _build_job(row, field_name="find_instagram")
    if _instagram_handle_looks_national_generic(instagram, job):
        return "legacy_nationals_generic_contact"
    document = _supporting_document(row)
    if document is not None:
        relevant = _instagram_looks_relevant_to_job(instagram, job, document=document)
    else:
        relevant = _instagram_looks_relevant_to_job(instagram, job)
    if not relevant and (
        _instagram_handle_looks_like_school_brand(instagram, job)
        or not _instagram_handle_has_fraternity_token(instagram, job)
    ):
        return "legacy_instagram_failed_chapter_specificity"
    return None


def _legacy_website_quarantine_reason(row: dict[str, Any]) -> str | None:
    website = str(row.get("website_url") or "").strip()
    if not website:
        return None
    if _trusted_provenance_type(row.get("contact_provenance"), "website_url"):
        return None
    document = _supporting_document(row)
    initial_verification = tool_official_domain_verifier(
        candidate_url=website,
        fraternity_name=str(row.get("fraternity_name") or row.get("fraternity_slug") or ""),
        fraternity_slug=str(row.get("fraternity_slug") or ""),
        chapter_name=str(row.get("name") or ""),
        university_name=row.get("university_name"),
        source_url=str(row.get("latest_source_url") or row.get("national_url") or "") or None,
        document_url=document.url if document else None,
        document_title=document.title if document else "",
        document_text=document.text if document else "",
    )
    if initial_verification.decision != "reject":
        return None
    initial_reasons = set(initial_verification.reason_codes)
    if initial_reasons & SAFE_WEBSITE_REJECTION_REASONS and not _website_candidate_needs_candidate_fetch(row, initial_verification.reason_codes):
        return "legacy_website_failed_official_verification"

    candidate_page = _candidate_page_context(website)
    verification_url = str((candidate_page or {}).get("url") or website)
    document_url = str((candidate_page or {}).get("url") or (document.url if document else "") or "") or None
    document_title = str((candidate_page or {}).get("title") or "")
    document_text = str((candidate_page or {}).get("text") or (document.text if document else "") or "")
    document_html = str((candidate_page or {}).get("html") or "")
    verification = tool_official_domain_verifier(
        candidate_url=verification_url,
        fraternity_name=str(row.get("fraternity_name") or row.get("fraternity_slug") or ""),
        fraternity_slug=str(row.get("fraternity_slug") or ""),
        chapter_name=str(row.get("name") or ""),
        university_name=row.get("university_name"),
        source_url=str(row.get("latest_source_url") or row.get("national_url") or "") or None,
        document_url=document_url,
        document_title=document_title,
        document_text=document_text,
        document_html=document_html,
    )
    if verification.decision == "reject" and any(reason in SAFE_WEBSITE_REJECTION_REASONS for reason in verification.reason_codes):
        return "legacy_website_failed_official_verification"
    return None


def _legacy_website_initial_reason(row: dict[str, Any]) -> str | None:
    website = str(row.get("website_url") or "").strip()
    if not website:
        return None
    if _trusted_provenance_type(row.get("contact_provenance"), "website_url"):
        return None
    document = _supporting_document(row)
    verification = tool_official_domain_verifier(
        candidate_url=website,
        fraternity_name=str(row.get("fraternity_name") or row.get("fraternity_slug") or ""),
        fraternity_slug=str(row.get("fraternity_slug") or ""),
        chapter_name=str(row.get("name") or ""),
        university_name=row.get("university_name"),
        source_url=str(row.get("latest_source_url") or row.get("national_url") or "") or None,
        document_url=document.url if document else None,
        document_title=document.title if document else "",
        document_text=document.text if document else "",
    )
    if verification.decision == "reject" and any(reason in SAFE_WEBSITE_REJECTION_REASONS for reason in verification.reason_codes):
        return "legacy_website_failed_official_verification"
    return None


def _website_candidate_needs_candidate_fetch(row: dict[str, Any], reason_codes: list[str] | None) -> bool:
    lowered = str(row.get("website_url") or "").lower()
    reasons = {str(reason).strip().lower() for reason in reason_codes or []}
    if reasons & {"map_export_url", "archival_url", "blocked_host", "low_signal_path"}:
        return False
    rescueable_reasons = {"missing_target_school_context", "generic_school_directory", "generic_school_root"}
    if not reasons.intersection(rescueable_reasons):
        return False
    compact_url = re.sub(r"[^a-z0-9]", "", lowered)
    fraternity_compact = re.sub(r"[^a-z0-9]", "", str(row.get("fraternity_slug") or ""))
    chapter_tokens = [
        token
        for token in re.split(r"[^a-z0-9]+", str(row.get("name") or "").lower())
        if len(token) >= 4 and token not in {"chapter", "active", "inactive", "mother"}
    ]
    host = (urlparse(lowered).netloc or "").lower()
    path = (urlparse(lowered).path or "").lower()
    profile_markers = ("/organization/", "/chapter-page/", "/chapter_page/", "/project/")
    host_labels = [label for label in host.split(".") if label]
    leading_label = host_labels[0] if host_labels else ""
    likely_chapter_subdomain = host.endswith(".edu") and leading_label not in {
        "www",
        "campuslife",
        "fsl",
        "greeks",
        "greeklife",
        "ofsl",
        "studentaffairs",
        "studentengagement",
        "studentinvolvement",
        "students",
        "terplink",
        "highlanderlink",
    }
    has_identity_hint = bool(fraternity_compact and fraternity_compact in compact_url) or any(token in compact_url for token in chapter_tokens)
    return any(marker in lowered for marker in ("terplink.", "campuslabs.com", "presence.io", "highlanderlink.", "sites.")) or any(
        marker in path for marker in profile_markers
    ) or likely_chapter_subdomain or has_identity_hint


def _cleanup_provenance_entry(*, reason_code: str, previous_value: str | None, supporting_url: str | None, page_scope: str, contact_type: str) -> dict[str, Any]:
    return {
        "supportingPageUrl": supporting_url,
        "supportingPageScope": page_scope,
        "contactProvenanceType": contact_type,
        "decisionStage": "legacy_contact_quarantine",
        "sourceType": "cleanup_legacy_contact",
        "reasonCode": reason_code,
        "confidence": 1.0,
        "decisionOutcome": "rejected",
        "fieldResolutionState": "missing",
        "candidateValue": None,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "previousValue": previous_value,
    }


def _fetch_rows(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                c.id,
                c.slug,
                c.name,
                c.university_name,
                c.chapter_status,
                c.website_url,
                c.instagram_url,
                c.contact_email,
                c.field_states,
                c.contact_provenance,
                f.slug AS fraternity_slug,
                f.name AS fraternity_name,
                np.national_url,
                np.contact_email AS national_email,
                np.instagram_url AS national_instagram_url,
                latest_cp.source_url AS latest_source_url,
                latest_cp.source_snippet AS latest_source_snippet,
                latest_cp.confidence AS latest_confidence,
                latest_source.slug AS source_slug
            FROM chapters c
            JOIN fraternities f ON f.id = c.fraternity_id
            LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
            LEFT JOIN LATERAL (
                SELECT cp.source_url, cp.source_snippet, cp.confidence, cp.source_id
                FROM chapter_provenance cp
                WHERE cp.chapter_id = c.id
                ORDER BY cp.extracted_at DESC NULLS LAST, cp.created_at DESC
                LIMIT 1
            ) latest_cp ON TRUE
            LEFT JOIN sources latest_source ON latest_source.id = latest_cp.source_id
            ORDER BY c.updated_at DESC, c.id DESC
            """
        )
        return list(cur.fetchall())


def _apply_contact_quarantine(
    conn: psycopg.Connection,
    chapter_id: str,
    *,
    clear_website: bool,
    clear_email: bool,
    clear_instagram: bool,
    website_reason: str | None,
    email_reason: str | None,
    instagram_reason: str | None,
    row: dict[str, Any],
) -> None:
    field_state_patch: dict[str, str] = {}
    provenance_patch: dict[str, Any] = {}

    if clear_website:
        field_state_patch["website_url"] = "missing"
        provenance_patch["website_url"] = _cleanup_provenance_entry(
            reason_code=str(website_reason or "legacy_contact_quarantined"),
            previous_value=str(row.get("website_url") or ""),
            supporting_url=row.get("latest_source_url") or row.get("national_url"),
            page_scope="unrelated",
            contact_type="ambiguous",
        )
    if clear_email:
        field_state_patch["contact_email"] = "missing"
        provenance_patch["contact_email"] = _cleanup_provenance_entry(
            reason_code=str(email_reason or "legacy_contact_quarantined"),
            previous_value=str(row.get("contact_email") or ""),
            supporting_url=row.get("national_url") or row.get("latest_source_url"),
            page_scope="nationals_generic" if email_reason == "legacy_nationals_generic_contact" else "unrelated",
            contact_type="national_generic" if email_reason == "legacy_nationals_generic_contact" else "ambiguous",
        )
    if clear_instagram:
        field_state_patch["instagram_url"] = "missing"
        provenance_patch["instagram_url"] = _cleanup_provenance_entry(
            reason_code=str(instagram_reason or "legacy_contact_quarantined"),
            previous_value=str(row.get("instagram_url") or ""),
            supporting_url=row.get("national_url") or row.get("latest_source_url"),
            page_scope="nationals_generic" if instagram_reason == "legacy_nationals_generic_contact" else "unrelated",
            contact_type="national_generic" if instagram_reason == "legacy_nationals_generic_contact" else "ambiguous",
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE chapters
            SET
                website_url = CASE WHEN %s THEN NULL ELSE website_url END,
                contact_email = CASE WHEN %s THEN NULL ELSE contact_email END,
                instagram_url = CASE WHEN %s THEN NULL ELSE instagram_url END,
                field_states = COALESCE(field_states, '{}'::jsonb) || %s,
                contact_provenance = COALESCE(contact_provenance, '{}'::jsonb) || %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                clear_website,
                clear_email,
                clear_instagram,
                Jsonb(field_state_patch),
                Jsonb(provenance_patch),
                chapter_id,
            ),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply bounded Phase 3/5 accuracy cleanup to legacy polluted rows.")
    parser.add_argument("--apply", action="store_true", help="Apply the cleanup instead of running in dry-run mode.")
    parser.add_argument(
        "--report",
        default="docs/SystemReport/PHASE_3_5_CLEANUP_SUMMARY_2026-04-09.json",
        help="Path to write the cleanup summary JSON.",
    )
    args = parser.parse_args()

    load_dotenv(".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured")

    conn = psycopg.connect(database_url)
    rows = _fetch_rows(conn)

    invalid_rows: list[dict[str, Any]] = []
    website_quarantined_rows: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []
    retained_rows: list[tuple[dict[str, Any], str | None, str | None]] = []
    website_rows_for_deep_check: list[dict[str, Any]] = []
    website_reason_by_id: dict[str, str] = {}

    for row in rows:
        invalid_reason = _legacy_invalid_reason(row)
        if invalid_reason:
            invalid_rows.append(
                {
                    "id": str(row["id"]),
                    "fraternitySlug": row["fraternity_slug"],
                    "chapterSlug": row["slug"],
                    "name": row["name"],
                    "universityName": row["university_name"],
                    "reasonCode": invalid_reason,
                    "latestSourceUrl": row.get("latest_source_url"),
                }
            )
            continue

        if _legacy_website_initial_reason(row):
            website_rows_for_deep_check.append(row)
        email_reason = _legacy_email_quarantine_reason(row)
        instagram_reason = _legacy_instagram_quarantine_reason(row)
        retained_rows.append((row, email_reason, instagram_reason))

    for row in website_rows_for_deep_check:
        website_reason = _legacy_website_quarantine_reason(row)
        if not website_reason:
            continue
        website_reason_by_id[str(row["id"])] = website_reason
        website_quarantined_rows.append(
            {
                "id": str(row["id"]),
                "fraternitySlug": row["fraternity_slug"],
                "chapterSlug": row["slug"],
                "name": row["name"],
                "universityName": row["university_name"],
                "websiteReason": website_reason,
                "websiteUrl": row.get("website_url"),
            }
        )

    for row, email_reason, instagram_reason in retained_rows:
        website_reason = website_reason_by_id.get(str(row["id"]))
        if website_reason or email_reason or instagram_reason:
            quarantined_rows.append(
                {
                    "id": str(row["id"]),
                    "fraternitySlug": row["fraternity_slug"],
                    "chapterSlug": row["slug"],
                    "name": row["name"],
                    "universityName": row["university_name"],
                    "websiteReason": website_reason,
                    "emailReason": email_reason,
                    "instagramReason": instagram_reason,
                    "websiteUrl": row.get("website_url"),
                    "email": row.get("contact_email"),
                    "instagramUrl": row.get("instagram_url"),
                }
            )

    summary = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "mode": "apply" if args.apply else "dry_run",
        "invalidRowsToDelete": len(invalid_rows),
        "legacyWebsiteRowsToQuarantine": len(website_quarantined_rows),
        "legacyContactRowsToQuarantine": len(quarantined_rows),
        "invalidSamples": invalid_rows[:50],
        "websiteQuarantineSamples": website_quarantined_rows[:50],
        "quarantineSamples": quarantined_rows[:50],
    }

    if args.apply:
        invalid_ids = [row["id"] for row in invalid_rows]
        if invalid_ids:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM chapters WHERE id = ANY(%s::uuid[])", (invalid_ids,))
        for item in quarantined_rows:
            original = next(row for row in rows if str(row["id"]) == item["id"])
            _apply_contact_quarantine(
                conn,
                item["id"],
                clear_website=bool(item["websiteReason"]),
                clear_email=bool(item["emailReason"]),
                clear_instagram=bool(item["instagramReason"]),
                website_reason=item["websiteReason"],
                email_reason=item["emailReason"],
                instagram_reason=item["instagramReason"],
                row=original,
            )
        conn.commit()

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
