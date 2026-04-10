from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
CRAWLER_SRC = ROOT / "services" / "crawler" / "src"
if str(CRAWLER_SRC) not in sys.path:
    sys.path.insert(0, str(CRAWLER_SRC))

from fratfinder_crawler.field_jobs import (  # noqa: E402
    SearchDocument,
    _chapter_designation_signal,
    _chapter_matches,
    _email_domain,
    _email_domain_matches_known_school_or_website,
    _email_local_part_has_identity,
    _email_local_part_looks_generic_office,
    _email_looks_relevant_to_job,
    _fraternity_matches,
    _has_nongeneric_chapter_signal,
    _instagram_handle_has_fraternity_token,
    _instagram_handle_has_local_identity,
    _instagram_handle_looks_like_school_brand,
    _instagram_handle_looks_national_generic,
    _instagram_looks_relevant_to_job,
    _normalize_instagram_candidate,
    _normalized_match_text,
    _school_matches,
    _url_has_job_identity,
)
from fratfinder_crawler.models import (  # noqa: E402
    CONTACT_SPECIFICITY_CHAPTER,
    CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
    CONTACT_SPECIFICITY_NATIONAL_GENERIC,
    CONTACT_SPECIFICITY_SCHOOL,
    CONTACT_SPECIFICITY_AMBIGUOUS,
    FieldJob,
    PAGE_SCOPE_CHAPTER_SITE,
    PAGE_SCOPE_DIRECTORY,
    PAGE_SCOPE_NATIONALS_CHAPTER,
    PAGE_SCOPE_NATIONALS_GENERIC,
    PAGE_SCOPE_SCHOOL_AFFILIATION,
    PAGE_SCOPE_UNRELATED,
)
from fratfinder_crawler.precision_tools import tool_site_scope_classifier  # noqa: E402


_PAGE_CACHE: dict[str, dict[str, str] | None] = {}
_GENERIC_NATIONAL_PATH_MARKERS = (
    "/chapter-directory",
    "/chapter-roll",
    "/chapters",
    "/directory",
    "/find-a-chapter",
    "/join-a-chapter",
    "/locations",
    "/locator",
    "/our-chapters",
)


@dataclass(slots=True)
class ContactDecision:
    action: str
    reason_code: str
    supporting_page_url: str | None
    supporting_page_scope: str
    contact_specificity: str
    confidence: float
    normalized_value: str | None


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
            WHERE c.contact_email IS NOT NULL OR c.instagram_url IS NOT NULL
            ORDER BY c.updated_at DESC, c.id DESC
            """
        )
        return list(cur.fetchall())


def _trusted_provenance_type(raw: dict[str, Any] | None, field_name: str) -> bool:
    field_provenance = (raw or {}).get(field_name)
    if not isinstance(field_provenance, dict):
        return False
    specificity = str(field_provenance.get("contactProvenanceType") or "").strip()
    return specificity in {
        CONTACT_SPECIFICITY_CHAPTER,
        CONTACT_SPECIFICITY_SCHOOL,
        CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
    }


def _build_job(row: dict[str, Any], *, field_name: str) -> FieldJob:
    return FieldJob(
        id=f"phase5-{field_name}-{row['id']}",
        chapter_id=str(row["id"]),
        chapter_slug=str(row["slug"] or ""),
        chapter_name=str(row["name"] or ""),
        field_name=field_name,
        payload={"candidateSchoolName": row.get("university_name") or ""},
        attempts=0,
        max_attempts=1,
        claim_token="phase5",
        source_base_url=row.get("website_url") or row.get("latest_source_url") or row.get("national_url"),
        website_url=row.get("website_url"),
        instagram_url=row.get("instagram_url"),
        contact_email=row.get("contact_email"),
        fraternity_slug=row.get("fraternity_slug"),
        source_slug=row.get("source_slug"),
        university_name=row.get("university_name"),
        field_states=row.get("field_states") or {},
        chapter_status=str(row.get("chapter_status") or "active"),
    )


def _page_context(url: str | None) -> dict[str, str] | None:
    normalized_url = str(url or "").strip()
    if not normalized_url:
        return None
    cache_key = normalized_url.rstrip("/").lower()
    if cache_key in _PAGE_CACHE:
        return _PAGE_CACHE[cache_key]
    try:
        response = requests.get(
            normalized_url,
            timeout=10,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (FratFinderAI phase5)"},
        )
    except requests.RequestException:
        _PAGE_CACHE[cache_key] = None
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
    _PAGE_CACHE[cache_key] = payload
    return payload


def _source_document(row: dict[str, Any]) -> SearchDocument:
    source_url = str(row.get("latest_source_url") or row.get("national_url") or row.get("website_url") or "").strip()
    source_snippet = str(row.get("latest_source_snippet") or "").strip()
    page_context = _page_context(source_url)
    final_url = str((page_context or {}).get("url") or source_url)
    if "instagram.com/accounts/login" in final_url and "instagram.com" in source_url:
        final_url = source_url
    title = str((page_context or {}).get("title") or "")
    page_text = str((page_context or {}).get("text") or "")
    html = str((page_context or {}).get("html") or "")
    text = "\n".join(part for part in [source_snippet, page_text] if part)
    return SearchDocument(
        text=text,
        links=[],
        url=final_url or None,
        title=title,
        provider="provenance",
        html=html or None,
    )


def _same_host(left: str | None, right: str | None) -> bool:
    left_host = (urlparse(str(left or "")).netloc or "").lower()
    right_host = (urlparse(str(right or "")).netloc or "").lower()
    return bool(left_host and right_host and (left_host == right_host or left_host.endswith(f".{right_host}") or right_host.endswith(f".{left_host}")))


def _contact_scope(row: dict[str, Any], *, job: FieldJob, document: SearchDocument, candidate_present: bool) -> str:
    source_url = document.url or str(row.get("latest_source_url") or "")
    national_url = str(row.get("national_url") or "")
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:4000], source_url] if part))
    fraternity_match = _fraternity_matches(job, combined)
    school_match = _school_matches(job, combined)
    chapter_match = bool(_has_nongeneric_chapter_signal(job) and (_chapter_matches(job, combined) or _chapter_designation_signal(job, combined) > 0))
    path_text = _normalized_match_text(f"{urlparse(source_url).path} {urlparse(source_url).query}")
    national_host_match = _same_host(source_url, national_url)
    path_identity = _url_has_job_identity(job, source_url) or school_match or chapter_match or (job.chapter_slug and _normalized_match_text(job.chapter_slug) in path_text)
    generic_national_path = any(marker in path_text for marker in _GENERIC_NATIONAL_PATH_MARKERS)

    if national_host_match:
        if candidate_present and (school_match or chapter_match or fraternity_match or path_identity):
            return PAGE_SCOPE_NATIONALS_CHAPTER
        if not generic_national_path and (school_match or chapter_match or path_identity):
            return PAGE_SCOPE_NATIONALS_CHAPTER
        return PAGE_SCOPE_NATIONALS_GENERIC

    scope_decision = tool_site_scope_classifier(
        page_url=source_url,
        title=document.title or "",
        text=document.text,
        fraternity_name=str(row.get("fraternity_name") or ""),
        school_name=row.get("university_name"),
        chapter_name=str(row.get("name") or ""),
    ).decision

    if scope_decision == "chapter_site" and (candidate_present or fraternity_match or chapter_match or school_match):
        return PAGE_SCOPE_CHAPTER_SITE
    if scope_decision == "school_affiliation" and candidate_present and (fraternity_match or chapter_match):
        return PAGE_SCOPE_SCHOOL_AFFILIATION
    if scope_decision == "school_affiliation":
        return PAGE_SCOPE_DIRECTORY
    return PAGE_SCOPE_UNRELATED


def _email_present_in_document(email: str, document: SearchDocument) -> bool:
    lowered = email.lower().strip()
    blobs = [document.text or "", document.title or "", document.url or "", document.html or ""]
    return any(lowered and lowered in blob.lower() for blob in blobs if blob)


def _supporting_page_has_local_identity(job: FieldJob, document: SearchDocument) -> bool:
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.url or ""] if part))
    if _url_has_job_identity(job, document.url):
        return True
    if _fraternity_matches(job, combined):
        return True
    if _has_nongeneric_chapter_signal(job) and (_chapter_matches(job, combined) or _chapter_designation_signal(job, combined) > 0):
        return True
    return False


def _instagram_present_in_document(instagram_url: str, document: SearchDocument) -> bool:
    normalized = _normalize_instagram_candidate(instagram_url)
    if not normalized:
        return False
    handle = normalized.rstrip("/").rsplit("/", 1)[-1].lower()
    blobs = [document.text or "", document.title or "", document.url or "", document.html or ""]
    for blob in blobs:
        lowered = blob.lower()
        if normalized.lower() in lowered:
            return True
        if f"@{handle}" in lowered:
            return True
        if handle and handle in lowered:
            return True
    return False


def _accepted_provenance_entry(
    *,
    value: str,
    supporting_url: str | None,
    page_scope: str,
    contact_type: str,
    reason_code: str,
    confidence: float,
) -> dict[str, Any]:
    return {
        "supportingPageUrl": supporting_url,
        "supportingPageScope": page_scope,
        "contactProvenanceType": contact_type,
        "decisionStage": "legacy_contact_reconciliation",
        "sourceType": "cleanup_legacy_contact",
        "reasonCode": reason_code,
        "confidence": confidence,
        "decisionOutcome": "accepted",
        "fieldResolutionState": "resolved",
        "candidateValue": value,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "previousValue": value,
    }


def _rejected_provenance_entry(
    *,
    previous_value: str | None,
    supporting_url: str | None,
    page_scope: str,
    contact_type: str,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "supportingPageUrl": supporting_url,
        "supportingPageScope": page_scope,
        "contactProvenanceType": contact_type,
        "decisionStage": "legacy_contact_reconciliation",
        "sourceType": "cleanup_legacy_contact",
        "reasonCode": reason_code,
        "confidence": 1.0,
        "decisionOutcome": "rejected",
        "fieldResolutionState": "missing",
        "candidateValue": None,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "previousValue": previous_value,
    }


def _email_decision(row: dict[str, Any]) -> ContactDecision | None:
    email = str(row.get("contact_email") or "").strip()
    if not email:
        return None
    if _trusted_provenance_type(row.get("contact_provenance"), "contact_email"):
        return None
    national_email = str(row.get("national_email") or "").strip().lower()
    if national_email and email.lower() == national_email:
        return ContactDecision(
            action="clear",
            reason_code="legacy_nationals_generic_contact",
            supporting_page_url=row.get("national_url"),
            supporting_page_scope=PAGE_SCOPE_NATIONALS_GENERIC,
            contact_specificity=CONTACT_SPECIFICITY_NATIONAL_GENERIC,
            confidence=1.0,
            normalized_value=None,
        )

    job = _build_job(row, field_name="find_email")
    document = _source_document(row)
    present = _email_present_in_document(email, document)
    scope = _contact_scope(row, job=job, document=document, candidate_present=present)
    relevant = _email_looks_relevant_to_job(email, job, document=document)
    identity_email = _email_local_part_has_identity(email, job)
    generic_office_email = _email_local_part_looks_generic_office(email)
    domain_matches = _email_domain_matches_known_school_or_website(job, _email_domain(email))
    supporting_identity = _supporting_page_has_local_identity(job, document)

    if scope == PAGE_SCOPE_CHAPTER_SITE and present and (identity_email or (relevant and not generic_office_email) or (domain_matches and not generic_office_email)):
        return ContactDecision(
            action="accept",
            reason_code="legacy_contact_backfilled_from_chapter_site",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_CHAPTER_SITE,
            contact_specificity=CONTACT_SPECIFICITY_CHAPTER,
            confidence=0.93,
            normalized_value=email,
        )
    if scope == PAGE_SCOPE_NATIONALS_CHAPTER and present and (identity_email or (relevant and not generic_office_email)):
        return ContactDecision(
            action="accept",
            reason_code="legacy_contact_backfilled_from_nationals_chapter_page",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_NATIONALS_CHAPTER,
            contact_specificity=CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
            confidence=0.9,
            normalized_value=email,
        )
    if scope == PAGE_SCOPE_SCHOOL_AFFILIATION and present and identity_email and relevant and not generic_office_email:
        return ContactDecision(
            action="accept",
            reason_code="legacy_contact_backfilled_from_school_affiliation_page",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_SCHOOL_AFFILIATION,
            contact_specificity=CONTACT_SPECIFICITY_SCHOOL,
            confidence=0.88,
            normalized_value=email,
        )
    if scope == PAGE_SCOPE_SCHOOL_AFFILIATION and present and not supporting_identity:
        return ContactDecision(
            action="clear",
            reason_code="legacy_email_failed_chapter_specificity",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_SCHOOL_AFFILIATION,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    if scope in {PAGE_SCOPE_UNRELATED, PAGE_SCOPE_DIRECTORY, PAGE_SCOPE_NATIONALS_GENERIC}:
        return ContactDecision(
            action="clear",
            reason_code="legacy_email_failed_chapter_specificity",
            supporting_page_url=document.url,
            supporting_page_scope=scope if scope != PAGE_SCOPE_DIRECTORY else PAGE_SCOPE_UNRELATED,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    if generic_office_email and not present:
        return ContactDecision(
            action="clear",
            reason_code="legacy_email_failed_chapter_specificity",
            supporting_page_url=document.url,
            supporting_page_scope=scope,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    if not present and not relevant:
        return ContactDecision(
            action="clear",
            reason_code="legacy_email_unsupported_on_page",
            supporting_page_url=document.url,
            supporting_page_scope=scope,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    return ContactDecision(
        action="review",
        reason_code="legacy_email_requires_review",
        supporting_page_url=document.url,
        supporting_page_scope=scope,
        contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
        confidence=0.4,
        normalized_value=email,
    )


def _instagram_decision(row: dict[str, Any]) -> ContactDecision | None:
    instagram = str(row.get("instagram_url") or "").strip()
    if not instagram:
        return None
    if _trusted_provenance_type(row.get("contact_provenance"), "instagram_url"):
        return None
    normalized_instagram = _normalize_instagram_candidate(instagram)
    if not normalized_instagram:
        return ContactDecision(
            action="clear",
            reason_code="legacy_instagram_failed_chapter_specificity",
            supporting_page_url=row.get("latest_source_url") or row.get("national_url"),
            supporting_page_scope=PAGE_SCOPE_UNRELATED,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    national_instagram = str(row.get("national_instagram_url") or "").strip().rstrip("/").lower()
    if national_instagram and normalized_instagram.rstrip("/").lower() == national_instagram:
        return ContactDecision(
            action="clear",
            reason_code="legacy_nationals_generic_contact",
            supporting_page_url=row.get("national_url"),
            supporting_page_scope=PAGE_SCOPE_NATIONALS_GENERIC,
            contact_specificity=CONTACT_SPECIFICITY_NATIONAL_GENERIC,
            confidence=1.0,
            normalized_value=None,
        )

    job = _build_job(row, field_name="find_instagram")
    document = _source_document(row)
    present = _instagram_present_in_document(normalized_instagram, document)
    scope = _contact_scope(row, job=job, document=document, candidate_present=present)
    relevant = _instagram_looks_relevant_to_job(normalized_instagram, job, document=document)
    local_identity = _instagram_handle_has_local_identity(normalized_instagram, job)
    fraternity_token = _instagram_handle_has_fraternity_token(normalized_instagram, job)
    school_brand = _instagram_handle_looks_like_school_brand(normalized_instagram, job)
    supporting_identity = _supporting_page_has_local_identity(job, document)

    if _instagram_handle_looks_national_generic(normalized_instagram, job):
        return ContactDecision(
            action="clear",
            reason_code="legacy_nationals_generic_contact",
            supporting_page_url=document.url or row.get("national_url"),
            supporting_page_scope=PAGE_SCOPE_NATIONALS_GENERIC,
            contact_specificity=CONTACT_SPECIFICITY_NATIONAL_GENERIC,
            confidence=1.0,
            normalized_value=None,
        )

    if scope == PAGE_SCOPE_CHAPTER_SITE and present and (relevant or local_identity or fraternity_token):
        return ContactDecision(
            action="accept",
            reason_code="legacy_contact_backfilled_from_chapter_site",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_CHAPTER_SITE,
            contact_specificity=CONTACT_SPECIFICITY_CHAPTER,
            confidence=0.93,
            normalized_value=normalized_instagram,
        )
    if scope == PAGE_SCOPE_NATIONALS_CHAPTER and present and ((local_identity and fraternity_token) or relevant):
        return ContactDecision(
            action="accept",
            reason_code="legacy_contact_backfilled_from_nationals_chapter_page",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_NATIONALS_CHAPTER,
            contact_specificity=CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
            confidence=0.9,
            normalized_value=normalized_instagram,
        )
    if scope == PAGE_SCOPE_SCHOOL_AFFILIATION and present and relevant and (fraternity_token or local_identity) and not (school_brand and not fraternity_token):
        return ContactDecision(
            action="accept",
            reason_code="legacy_contact_backfilled_from_school_affiliation_page",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_SCHOOL_AFFILIATION,
            contact_specificity=CONTACT_SPECIFICITY_SCHOOL,
            confidence=0.88,
            normalized_value=normalized_instagram,
        )
    if scope == PAGE_SCOPE_SCHOOL_AFFILIATION and present and not supporting_identity:
        return ContactDecision(
            action="clear",
            reason_code="legacy_instagram_failed_chapter_specificity",
            supporting_page_url=document.url,
            supporting_page_scope=PAGE_SCOPE_SCHOOL_AFFILIATION,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    if scope in {PAGE_SCOPE_UNRELATED, PAGE_SCOPE_DIRECTORY, PAGE_SCOPE_NATIONALS_GENERIC}:
        return ContactDecision(
            action="clear",
            reason_code="legacy_instagram_failed_chapter_specificity",
            supporting_page_url=document.url,
            supporting_page_scope=scope if scope != PAGE_SCOPE_DIRECTORY else PAGE_SCOPE_UNRELATED,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    if school_brand and not fraternity_token:
        return ContactDecision(
            action="clear",
            reason_code="legacy_instagram_failed_chapter_specificity",
            supporting_page_url=document.url,
            supporting_page_scope=scope,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    if not present and not relevant:
        return ContactDecision(
            action="clear",
            reason_code="legacy_instagram_unsupported_on_page",
            supporting_page_url=document.url,
            supporting_page_scope=scope,
            contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
            confidence=1.0,
            normalized_value=None,
        )

    return ContactDecision(
        action="review",
        reason_code="legacy_instagram_requires_review",
        supporting_page_url=document.url,
        supporting_page_scope=scope,
        contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
        confidence=0.4,
        normalized_value=normalized_instagram,
    )


def _apply_contact_decisions(
    conn: psycopg.Connection,
    row: dict[str, Any],
    *,
    email_decision: ContactDecision | None,
    instagram_decision: ContactDecision | None,
) -> None:
    field_state_patch: dict[str, str] = {}
    provenance_patch: dict[str, Any] = {}
    clear_email = email_decision is not None and email_decision.action == "clear"
    clear_instagram = instagram_decision is not None and instagram_decision.action == "clear"
    update_email = email_decision is not None and email_decision.action == "accept"
    update_instagram = instagram_decision is not None and instagram_decision.action == "accept"

    if email_decision is not None:
        if clear_email:
            field_state_patch["contact_email"] = "missing"
            provenance_patch["contact_email"] = _rejected_provenance_entry(
                previous_value=str(row.get("contact_email") or ""),
                supporting_url=email_decision.supporting_page_url,
                page_scope=email_decision.supporting_page_scope,
                contact_type=email_decision.contact_specificity,
                reason_code=email_decision.reason_code,
            )
        elif update_email:
            field_state_patch["contact_email"] = "found"
            provenance_patch["contact_email"] = _accepted_provenance_entry(
                value=str(email_decision.normalized_value or ""),
                supporting_url=email_decision.supporting_page_url,
                page_scope=email_decision.supporting_page_scope,
                contact_type=email_decision.contact_specificity,
                reason_code=email_decision.reason_code,
                confidence=email_decision.confidence,
            )

    if instagram_decision is not None:
        if clear_instagram:
            field_state_patch["instagram_url"] = "missing"
            provenance_patch["instagram_url"] = _rejected_provenance_entry(
                previous_value=str(row.get("instagram_url") or ""),
                supporting_url=instagram_decision.supporting_page_url,
                page_scope=instagram_decision.supporting_page_scope,
                contact_type=instagram_decision.contact_specificity,
                reason_code=instagram_decision.reason_code,
            )
        elif update_instagram:
            field_state_patch["instagram_url"] = "found"
            provenance_patch["instagram_url"] = _accepted_provenance_entry(
                value=str(instagram_decision.normalized_value or ""),
                supporting_url=instagram_decision.supporting_page_url,
                page_scope=instagram_decision.supporting_page_scope,
                contact_type=instagram_decision.contact_specificity,
                reason_code=instagram_decision.reason_code,
                confidence=instagram_decision.confidence,
            )

    if not field_state_patch and not provenance_patch:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE chapters
            SET
                contact_email = CASE
                    WHEN %s THEN NULL
                    WHEN %s THEN %s
                    ELSE contact_email
                END,
                instagram_url = CASE
                    WHEN %s THEN NULL
                    WHEN %s THEN %s
                    ELSE instagram_url
                END,
                field_states = COALESCE(field_states, '{}'::jsonb) || %s,
                contact_provenance = COALESCE(contact_provenance, '{}'::jsonb) || %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                clear_email,
                update_email,
                email_decision.normalized_value if update_email and email_decision else None,
                clear_instagram,
                update_instagram,
                instagram_decision.normalized_value if update_instagram and instagram_decision else None,
                Jsonb(field_state_patch),
                Jsonb(provenance_patch),
                str(row["id"]),
            ),
        )


def _sample_row(row: dict[str, Any], email_decision: ContactDecision | None, instagram_decision: ContactDecision | None) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "fraternitySlug": row["fraternity_slug"],
        "chapterSlug": row["slug"],
        "name": row["name"],
        "universityName": row["university_name"],
        "email": row.get("contact_email"),
        "instagramUrl": row.get("instagram_url"),
        "latestSourceUrl": row.get("latest_source_url"),
        "emailDecision": None if email_decision is None else {
            "action": email_decision.action,
            "reasonCode": email_decision.reason_code,
            "pageScope": email_decision.supporting_page_scope,
            "contactSpecificity": email_decision.contact_specificity,
            "supportingPageUrl": email_decision.supporting_page_url,
            "normalizedValue": email_decision.normalized_value,
        },
        "instagramDecision": None if instagram_decision is None else {
            "action": instagram_decision.action,
            "reasonCode": instagram_decision.reason_code,
            "pageScope": instagram_decision.supporting_page_scope,
            "contactSpecificity": instagram_decision.contact_specificity,
            "supportingPageUrl": instagram_decision.supporting_page_url,
            "normalizedValue": instagram_decision.normalized_value,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Bounded Phase 5 reconciliation for legacy email and Instagram contacts.")
    parser.add_argument("--apply", action="store_true", help="Apply the reconciliation instead of running in dry-run mode.")
    parser.add_argument(
        "--report",
        default="docs/SystemReport/PHASE_5_CONTACT_RECONCILIATION_2026-04-09.json",
        help="Path to write the reconciliation summary JSON.",
    )
    args = parser.parse_args()

    load_dotenv(".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured")

    conn = psycopg.connect(database_url)
    rows = _fetch_rows(conn)

    accepted_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    for row in rows:
        email_decision = _email_decision(row)
        instagram_decision = _instagram_decision(row)

        accepted = any(decision is not None and decision.action == "accept" for decision in (email_decision, instagram_decision))
        rejected = any(decision is not None and decision.action == "clear" for decision in (email_decision, instagram_decision))
        review = any(decision is not None and decision.action == "review" for decision in (email_decision, instagram_decision))

        if accepted:
            accepted_rows.append(_sample_row(row, email_decision, instagram_decision))
        if rejected:
            rejected_rows.append(_sample_row(row, email_decision, instagram_decision))
        if review and not accepted and not rejected:
            review_rows.append(_sample_row(row, email_decision, instagram_decision))

        if args.apply and (accepted or rejected):
            _apply_contact_decisions(conn, row, email_decision=email_decision, instagram_decision=instagram_decision)

    if args.apply:
        conn.commit()
    conn.close()

    summary = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "mode": "apply" if args.apply else "dry_run",
        "acceptedCount": len(accepted_rows),
        "rejectedCount": len(rejected_rows),
        "reviewCount": len(review_rows),
        "acceptedSamples": accepted_rows[:50],
        "rejectedSamples": rejected_rows[:50],
        "reviewSamples": review_rows[:50],
    }

    report_path = ROOT / args.report
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
