from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.types.json import Jsonb

from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.contracts import ContractValidator
from fratfinder_crawler.models import (
    AccuracyRecoveryMetrics,
    ChapterActivityRecord,
    ChapterEvidenceRecord,
    ChapterRepairJob,
    CONTACT_SPECIFICITY_AMBIGUOUS,
    CONTACT_SPECIFICITY_CHAPTER,
    CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
    CONTACT_SPECIFICITY_NATIONAL_GENERIC,
    CONTACT_SPECIFICITY_SCHOOL,
    CONTACT_SPECIFICITY_VALUES,
    CrawlMetrics,
    DecisionEvidence,
    DECISION_OUTCOME_ACCEPTED,
    DECISION_OUTCOME_DEFERRED,
    DECISION_OUTCOME_REJECTED,
    DECISION_OUTCOME_REVIEW_REQUIRED,
    EpochMetric,
    ExistingSourceCandidate,
    EnrichmentObservation,
    FIELD_RESOLUTION_CONFIRMED_ABSENT,
    FIELD_RESOLUTION_DEFERRED,
    FIELD_RESOLUTION_INACTIVE,
    FIELD_RESOLUTION_MISSING,
    FIELD_RESOLUTION_RESOLVED,
    FrontierItem,
    FieldJob,
    FieldJobDecision,
    FIELD_TO_CHAPTER_COLUMN,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    FIELD_JOB_TYPES,
    NationalProfileRecord,
    NormalizedChapter,
    PageObservation,
    PAGE_SCOPE_CHAPTER_SITE,
    PAGE_SCOPE_DIRECTORY,
    PAGE_SCOPE_NATIONALS_CHAPTER,
    PAGE_SCOPE_NATIONALS_GENERIC,
    PAGE_SCOPE_SCHOOL_AFFILIATION,
    PAGE_SCOPE_UNRELATED,
    PAGE_SCOPE_VALUES,
    ProvenanceRecord,
    ProvisionalChapterRecord,
    RewardEvent,
    ReviewItemCandidate,
    SchoolPolicyRecord,
    TemplateProfile,
    SourceRecord,
    VerifiedSourceRecord,
)
from fratfinder_crawler.normalization.state_normalizer import normalize_us_state
from fratfinder_crawler.status.models import (
    CampusStatusSource,
    ChapterStatusDecision,
    ChapterStatusEvidence,
    NationalStatusValue,
    SchoolRecognitionStatus,
    StatusZone,
)


def _normalize_field_job_queue_state(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized in {
        "actionable",
        "deferred",
        "blocked_invalid",
        "blocked_repairable",
        "blocked_provider",
        "blocked_dependency",
    }:
        return normalized
    return "actionable"


def _normalize_field_job_validity_class(value: Any) -> str | None:
    normalized = str(value or "").strip()
    if normalized in {"canonical_valid", "repairable_candidate", "provisional_candidate", "invalid_non_chapter"}:
        return normalized
    return None


def _extract_field_job_typed_state(
    payload_patch: dict[str, Any] | None = None,
    *,
    completed_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    patch = payload_patch or {}
    contact_resolution = patch.get("contactResolution") if isinstance(patch.get("contactResolution"), dict) else {}
    chapter_repair = patch.get("chapterRepair") if isinstance(patch.get("chapterRepair"), dict) else {}
    queue_triage = patch.get("queueTriage") if isinstance(patch.get("queueTriage"), dict) else {}

    blocked_reason = (
        contact_resolution.get("blockedReason")
        or contact_resolution.get("reasonCode")
        or queue_triage.get("reason")
        or queue_triage.get("repairReason")
    )

    typed: dict[str, Any] = {
        "queue_state": _normalize_field_job_queue_state(contact_resolution.get("queueState"))
        if contact_resolution.get("queueState") is not None
        else None,
        "validity_class": _normalize_field_job_validity_class(contact_resolution.get("validityClass")),
        "repair_state": chapter_repair.get("state") if chapter_repair.get("state") else None,
        "blocked_reason": str(blocked_reason).strip() or None if blocked_reason is not None else None,
        "terminal_outcome": completed_payload.get("status") if isinstance(completed_payload, dict) else None,
    }
    return typed


def _normalize_school_slug(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    normalized = "".join(ch if ch.isalnum() else "-" for ch in text)
    normalized = "-".join(part for part in normalized.split("-") if part)
    return normalized or None


def _normalize_page_scope(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized in PAGE_SCOPE_VALUES:
        return normalized
    return PAGE_SCOPE_UNRELATED


def _normalize_contact_specificity(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized in CONTACT_SPECIFICITY_VALUES:
        return normalized
    return CONTACT_SPECIFICITY_AMBIGUOUS


def _field_resolution_state_from_value(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"found", "resolved"}:
        return FIELD_RESOLUTION_RESOLVED
    if normalized == "inactive":
        return FIELD_RESOLUTION_INACTIVE
    if normalized == "confirmed_absent":
        return FIELD_RESOLUTION_CONFIRMED_ABSENT
    if normalized == "deferred":
        return FIELD_RESOLUTION_DEFERRED
    return FIELD_RESOLUTION_MISSING


def _decision_outcome_from_status(status: Any) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"updated", "verified", "resolved_from_authoritative_source"}:
        return DECISION_OUTCOME_ACCEPTED
    if normalized in {"review_required"}:
        return DECISION_OUTCOME_REVIEW_REQUIRED
    if normalized in {"provider_degraded", "confirmed_absent"}:
        return DECISION_OUTCOME_DEFERRED
    return DECISION_OUTCOME_REJECTED


def _host(url: str | None) -> str:
    return (urlparse(str(url or "")).netloc or "").lower()


def _is_root_like(url: str | None) -> bool:
    parsed = urlparse(str(url or ""))
    path = (parsed.path or "").strip("/")
    return path == ""


def _build_decision_evidence(
    completed_payload: dict[str, Any] | None,
    *,
    fallback_confidence: float | None = None,
    fallback_reason_code: str | None = None,
) -> DecisionEvidence:
    payload = completed_payload or {}
    raw = payload.get("resolutionEvidence")
    if isinstance(raw, dict):
        return DecisionEvidence(
            decision_stage=str(raw.get("decisionStage") or payload.get("field") or "field_job_resolution"),
            evidence_url=str(raw.get("evidenceUrl") or payload.get("source_url") or payload.get("sourceUrl") or "").strip() or None,
            source_type=str(raw.get("sourceType") or "").strip() or None,
            page_scope=_normalize_page_scope(raw.get("pageScope")),
            contact_specificity=_normalize_contact_specificity(raw.get("contactSpecificity")),
            confidence=float(raw.get("confidence")) if raw.get("confidence") is not None else fallback_confidence,
            reason_code=str(raw.get("reasonCode") or payload.get("reasonCode") or fallback_reason_code or "").strip() or None,
            metadata=dict(raw.get("metadata") or {}),
        )
    return DecisionEvidence(
        decision_stage=str(payload.get("field") or "field_job_resolution"),
        evidence_url=str(payload.get("source_url") or payload.get("sourceUrl") or "").strip() or None,
        page_scope=PAGE_SCOPE_UNRELATED,
        contact_specificity=CONTACT_SPECIFICITY_AMBIGUOUS,
        confidence=fallback_confidence,
        reason_code=str(payload.get("reasonCode") or fallback_reason_code or "").strip() or None,
        metadata={},
    )


def _build_contact_provenance_patch(
    *,
    chapter_updates: dict[str, str] | None,
    field_state_updates: dict[str, str] | None,
    completed_payload: dict[str, Any] | None,
    provenance_records: list[ProvenanceRecord] | None,
) -> dict[str, Any]:
    chapter_updates = chapter_updates or {}
    field_state_updates = field_state_updates or {}
    provenance_records = provenance_records or []
    if not chapter_updates and not field_state_updates:
        return {}

    decision = _build_decision_evidence(completed_payload, fallback_reason_code=str((completed_payload or {}).get("reasonCode") or "") or None)
    records_by_field = {record.field_name: record for record in provenance_records}
    patch: dict[str, Any] = {}

    for field_name, value in chapter_updates.items():
        record = records_by_field.get(field_name)
        evidence_url = (
            decision.evidence_url
            or (record.source_url if record is not None else None)
            or str((completed_payload or {}).get("source_url") or (completed_payload or {}).get("sourceUrl") or "").strip()
            or None
        )
        patch[field_name] = {
            "supportingPageUrl": evidence_url,
            "supportingPageScope": decision.page_scope,
            "contactProvenanceType": decision.contact_specificity,
            "decisionStage": decision.decision_stage,
            "sourceType": decision.source_type,
            "reasonCode": decision.reason_code,
            "confidence": round(float(record.confidence if record is not None else (decision.confidence or 0.0)), 4),
            "decisionOutcome": _decision_outcome_from_status((completed_payload or {}).get("status")),
            "fieldResolutionState": _field_resolution_state_from_value(field_state_updates.get(field_name)),
            "candidateValue": value,
            "updatedAt": datetime.utcnow().isoformat(),
        }

    if "chapter_status" in chapter_updates:
        patch["chapter_status"] = {
            "supportingPageUrl": decision.evidence_url,
            "supportingPageScope": decision.page_scope,
            "contactProvenanceType": decision.contact_specificity,
            "decisionStage": decision.decision_stage,
            "sourceType": decision.source_type,
            "reasonCode": decision.reason_code,
            "confidence": round(float(decision.confidence or 0.0), 4),
            "decisionOutcome": _decision_outcome_from_status((completed_payload or {}).get("status")),
            "fieldResolutionState": _field_resolution_state_from_value(field_state_updates.get("chapter_status")),
            "candidateValue": chapter_updates["chapter_status"],
            "updatedAt": datetime.utcnow().isoformat(),
        }

    return patch


class CrawlerRepository:
    def __init__(self, connection: psycopg.Connection):
        self._connection = connection
        self._contracts = ContractValidator()

    def _upsert_national_profile_from_verified_source(
        self,
        cursor: psycopg.Cursor,
        *,
        fraternity_slug: str,
        fraternity_name: str,
        national_url: str,
        confidence: float,
        origin: str | None,
        http_status: int | None,
        is_active: bool,
        metadata: dict[str, Any] | None,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO national_profiles (
                fraternity_slug,
                fraternity_name,
                national_url,
                national_url_confidence,
                national_url_provenance_type,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (fraternity_slug)
            DO UPDATE SET
                fraternity_name = EXCLUDED.fraternity_name,
                national_url = EXCLUDED.national_url,
                national_url_confidence = EXCLUDED.national_url_confidence,
                national_url_provenance_type = EXCLUDED.national_url_provenance_type,
                metadata = COALESCE(national_profiles.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                updated_at = NOW()
            """,
            (
                fraternity_slug,
                fraternity_name,
                national_url,
                max(0.0, min(float(confidence), 0.99)),
                str(origin or "verified_source_registry"),
                Jsonb(
                    {
                        "verifiedSourceOrigin": origin,
                        "httpStatus": http_status,
                        "isActive": bool(is_active),
                        "verifiedSourceMetadata": metadata or {},
                    }
                ),
            ),
        )

    def load_sources(self, source_slug: str | None = None) -> list[SourceRecord]:
        base_query = """
            SELECT
                s.id,
                s.fraternity_id,
                f.slug AS fraternity_slug,
                s.slug AS source_slug,
                s.source_type,
                s.parser_key,
                s.base_url,
                s.list_path,
                s.metadata
            FROM sources s
            JOIN fraternities f ON f.id = s.fraternity_id
            WHERE s.active = TRUE
        """
        order_clause = """
            ORDER BY s.slug
        """

        with self._connection.cursor() as cursor:
            if source_slug is None:
                cursor.execute(f"{base_query}{order_clause}")
            else:
                cursor.execute(
                    f"{base_query} AND s.slug = %(source_slug)s {order_clause}",
                    {"source_slug": source_slug},
                )
            rows = cursor.fetchall()

        return [
            SourceRecord(
                id=str(row["id"]),
                fraternity_id=str(row["fraternity_id"]),
                fraternity_slug=row["fraternity_slug"],
                source_slug=row["source_slug"],
                source_type=row["source_type"],
                parser_key=row["parser_key"],
                base_url=row["base_url"],
                list_path=row["list_path"],
                metadata=row["metadata"] or {},
            )
            for row in rows
        ]

    def get_verified_source_by_slug(self, fraternity_slug: str) -> VerifiedSourceRecord | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                FROM verified_sources
                WHERE fraternity_slug = %s
                LIMIT 1
                """,
                (fraternity_slug,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return VerifiedSourceRecord(
            fraternity_slug=row["fraternity_slug"],
            fraternity_name=row["fraternity_name"],
            national_url=row["national_url"],
            origin=row["origin"],
            confidence=float(row["confidence"] or 0.0),
            http_status=int(row["http_status"]) if row["http_status"] is not None else None,
            checked_at=row["checked_at"].isoformat() if row.get("checked_at") else None,
            is_active=bool(row["is_active"]),
            metadata=row["metadata"] or {},
        )

    def list_verified_sources(self, limit: int = 200) -> list[VerifiedSourceRecord]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                FROM verified_sources
                ORDER BY checked_at DESC, fraternity_slug ASC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            rows = cursor.fetchall()
        return [
            VerifiedSourceRecord(
                fraternity_slug=row["fraternity_slug"],
                fraternity_name=row["fraternity_name"],
                national_url=row["national_url"],
                origin=row["origin"],
                confidence=float(row["confidence"] or 0.0),
                http_status=int(row["http_status"]) if row["http_status"] is not None else None,
                checked_at=row["checked_at"].isoformat() if row.get("checked_at") else None,
                is_active=bool(row["is_active"]),
                metadata=row["metadata"] or {},
            )
            for row in rows
        ]

    def upsert_verified_source(
        self,
        *,
        fraternity_slug: str,
        fraternity_name: str,
        national_url: str,
        origin: str,
        confidence: float,
        http_status: int | None,
        checked_at: str | None = None,
        is_active: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> VerifiedSourceRecord:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO verified_sources (
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s::timestamptz, NOW()), %s, %s)
                ON CONFLICT (fraternity_slug)
                DO UPDATE SET
                    fraternity_name = EXCLUDED.fraternity_name,
                    national_url = EXCLUDED.national_url,
                    origin = EXCLUDED.origin,
                    confidence = EXCLUDED.confidence,
                    http_status = EXCLUDED.http_status,
                    checked_at = EXCLUDED.checked_at,
                    is_active = EXCLUDED.is_active,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                """,
                (
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    max(0.0, min(float(confidence), 0.99)),
                    http_status,
                    checked_at,
                    is_active,
                    Jsonb(metadata or {}),
                ),
            )
            row = cursor.fetchone()
            self._upsert_national_profile_from_verified_source(
                cursor,
                fraternity_slug=fraternity_slug,
                fraternity_name=fraternity_name,
                national_url=national_url,
                confidence=confidence,
                origin=origin,
                http_status=http_status,
                is_active=is_active,
                metadata=metadata,
            )
        self._connection.commit()
        return VerifiedSourceRecord(
            fraternity_slug=row["fraternity_slug"],
            fraternity_name=row["fraternity_name"],
            national_url=row["national_url"],
            origin=row["origin"],
            confidence=float(row["confidence"] or 0.0),
            http_status=int(row["http_status"]) if row["http_status"] is not None else None,
            checked_at=row["checked_at"].isoformat() if row.get("checked_at") else None,
            is_active=bool(row["is_active"]),
            metadata=row["metadata"] or {},
        )

    def get_existing_source_candidates(self, fraternity_slug: str) -> list[ExistingSourceCandidate]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.slug AS source_slug,
                    s.base_url,
                    s.list_path,
                    s.source_type,
                    s.parser_key,
                    s.active,
                    MAX(cr.started_at) FILTER (WHERE cr.status = 'succeeded') AS last_success_at,
                    (
                        ARRAY_REMOVE(
                            ARRAY_AGG(cr.status ORDER BY cr.started_at DESC),
                            NULL
                        )
                    )[1] AS last_run_status
                FROM sources s
                JOIN fraternities f ON f.id = s.fraternity_id
                LEFT JOIN crawl_runs cr ON cr.source_id = s.id
                WHERE f.slug = %s
                GROUP BY s.slug, s.base_url, s.list_path, s.source_type, s.parser_key, s.active, s.updated_at
                ORDER BY
                    s.active DESC,
                    MAX(cr.started_at) FILTER (WHERE cr.status = 'succeeded') DESC NULLS LAST,
                    s.updated_at DESC,
                    s.slug ASC
                """,
                (fraternity_slug,),
            )
            rows = cursor.fetchall()
        candidates: list[ExistingSourceCandidate] = []
        for row in rows:
            list_path = row["list_path"]
            base_url = row["base_url"]
            if isinstance(list_path, str) and list_path.startswith("http"):
                list_url = list_path
            elif isinstance(list_path, str) and list_path:
                list_url = f"{base_url.rstrip('/')}/{list_path.lstrip('/')}"
            else:
                list_url = base_url

            last_status = row["last_run_status"]
            health_confidence = 0.60
            if last_status == "succeeded":
                health_confidence = 0.90
            elif last_status == "partial":
                health_confidence = 0.40 if row["last_success_at"] is None else 0.75
            elif last_status == "failed":
                health_confidence = 0.25 if row["last_success_at"] is None else 0.50

            if not row["active"]:
                health_confidence -= 0.20

            candidates.append(
                ExistingSourceCandidate(
                    source_slug=row["source_slug"],
                    list_url=list_url,
                    base_url=base_url,
                    source_type=row["source_type"],
                    parser_key=row["parser_key"],
                    active=bool(row["active"]),
                    last_run_status=last_status,
                    last_success_at=row["last_success_at"].isoformat() if row["last_success_at"] else None,
                    confidence=max(0.0, min(0.99, health_confidence)),
                )
            )
        return candidates

    def list_national_profiles(self, limit: int = 250) -> list[NationalProfileRecord]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    national_url_confidence,
                    national_url_provenance_type,
                    national_url_reason_code,
                    contact_email,
                    contact_email_confidence,
                    contact_email_provenance_type,
                    contact_email_reason_code,
                    instagram_url,
                    instagram_confidence,
                    instagram_provenance_type,
                    instagram_reason_code,
                    phone,
                    phone_confidence,
                    phone_provenance_type,
                    phone_reason_code,
                    address_text,
                    address_confidence,
                    address_provenance_type,
                    address_reason_code,
                    metadata,
                    created_at::text AS created_at,
                    updated_at::text AS updated_at
                FROM national_profiles
                ORDER BY fraternity_name ASC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            rows = cursor.fetchall()
        return [
            NationalProfileRecord(
                fraternity_slug=row["fraternity_slug"],
                fraternity_name=row["fraternity_name"],
                national_url=row["national_url"],
                national_url_confidence=float(row["national_url_confidence"] or 0.0),
                national_url_provenance_type=row["national_url_provenance_type"],
                national_url_reason_code=row["national_url_reason_code"],
                contact_email=row["contact_email"],
                contact_email_confidence=float(row["contact_email_confidence"] or 0.0),
                contact_email_provenance_type=row["contact_email_provenance_type"],
                contact_email_reason_code=row["contact_email_reason_code"],
                instagram_url=row["instagram_url"],
                instagram_confidence=float(row["instagram_confidence"] or 0.0),
                instagram_provenance_type=row["instagram_provenance_type"],
                instagram_reason_code=row["instagram_reason_code"],
                phone=row["phone"],
                phone_confidence=float(row["phone_confidence"] or 0.0),
                phone_provenance_type=row["phone_provenance_type"],
                phone_reason_code=row["phone_reason_code"],
                address_text=row["address_text"],
                address_confidence=float(row["address_confidence"] or 0.0),
                address_provenance_type=row["address_provenance_type"],
                address_reason_code=row["address_reason_code"],
                metadata=row["metadata"] or {},
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    def upsert_national_profile(
        self,
        *,
        fraternity_slug: str,
        fraternity_name: str,
        national_url: str,
        national_url_confidence: float = 0.0,
        national_url_provenance_type: str | None = None,
        national_url_reason_code: str | None = None,
        contact_email: str | None = None,
        contact_email_confidence: float = 0.0,
        contact_email_provenance_type: str | None = None,
        contact_email_reason_code: str | None = None,
        instagram_url: str | None = None,
        instagram_confidence: float = 0.0,
        instagram_provenance_type: str | None = None,
        instagram_reason_code: str | None = None,
        phone: str | None = None,
        phone_confidence: float = 0.0,
        phone_provenance_type: str | None = None,
        phone_reason_code: str | None = None,
        address_text: str | None = None,
        address_confidence: float = 0.0,
        address_provenance_type: str | None = None,
        address_reason_code: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> NationalProfileRecord:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO national_profiles (
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    national_url_confidence,
                    national_url_provenance_type,
                    national_url_reason_code,
                    contact_email,
                    contact_email_confidence,
                    contact_email_provenance_type,
                    contact_email_reason_code,
                    instagram_url,
                    instagram_confidence,
                    instagram_provenance_type,
                    instagram_reason_code,
                    phone,
                    phone_confidence,
                    phone_provenance_type,
                    phone_reason_code,
                    address_text,
                    address_confidence,
                    address_provenance_type,
                    address_reason_code,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fraternity_slug)
                DO UPDATE SET
                    fraternity_name = EXCLUDED.fraternity_name,
                    national_url = EXCLUDED.national_url,
                    national_url_confidence = EXCLUDED.national_url_confidence,
                    national_url_provenance_type = EXCLUDED.national_url_provenance_type,
                    national_url_reason_code = EXCLUDED.national_url_reason_code,
                    contact_email = COALESCE(EXCLUDED.contact_email, national_profiles.contact_email),
                    contact_email_confidence = GREATEST(national_profiles.contact_email_confidence, EXCLUDED.contact_email_confidence),
                    contact_email_provenance_type = COALESCE(EXCLUDED.contact_email_provenance_type, national_profiles.contact_email_provenance_type),
                    contact_email_reason_code = COALESCE(EXCLUDED.contact_email_reason_code, national_profiles.contact_email_reason_code),
                    instagram_url = COALESCE(EXCLUDED.instagram_url, national_profiles.instagram_url),
                    instagram_confidence = GREATEST(national_profiles.instagram_confidence, EXCLUDED.instagram_confidence),
                    instagram_provenance_type = COALESCE(EXCLUDED.instagram_provenance_type, national_profiles.instagram_provenance_type),
                    instagram_reason_code = COALESCE(EXCLUDED.instagram_reason_code, national_profiles.instagram_reason_code),
                    phone = COALESCE(EXCLUDED.phone, national_profiles.phone),
                    phone_confidence = GREATEST(national_profiles.phone_confidence, EXCLUDED.phone_confidence),
                    phone_provenance_type = COALESCE(EXCLUDED.phone_provenance_type, national_profiles.phone_provenance_type),
                    phone_reason_code = COALESCE(EXCLUDED.phone_reason_code, national_profiles.phone_reason_code),
                    address_text = COALESCE(EXCLUDED.address_text, national_profiles.address_text),
                    address_confidence = GREATEST(national_profiles.address_confidence, EXCLUDED.address_confidence),
                    address_provenance_type = COALESCE(EXCLUDED.address_provenance_type, national_profiles.address_provenance_type),
                    address_reason_code = COALESCE(EXCLUDED.address_reason_code, national_profiles.address_reason_code),
                    metadata = COALESCE(national_profiles.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    national_url_confidence,
                    national_url_provenance_type,
                    national_url_reason_code,
                    contact_email,
                    contact_email_confidence,
                    contact_email_provenance_type,
                    contact_email_reason_code,
                    instagram_url,
                    instagram_confidence,
                    instagram_provenance_type,
                    instagram_reason_code,
                    phone,
                    phone_confidence,
                    phone_provenance_type,
                    phone_reason_code,
                    address_text,
                    address_confidence,
                    address_provenance_type,
                    address_reason_code,
                    metadata,
                    created_at::text AS created_at,
                    updated_at::text AS updated_at
                """,
                (
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    max(0.0, min(float(national_url_confidence), 0.99)),
                    national_url_provenance_type,
                    national_url_reason_code,
                    contact_email,
                    max(0.0, min(float(contact_email_confidence), 0.99)),
                    contact_email_provenance_type,
                    contact_email_reason_code,
                    instagram_url,
                    max(0.0, min(float(instagram_confidence), 0.99)),
                    instagram_provenance_type,
                    instagram_reason_code,
                    phone,
                    max(0.0, min(float(phone_confidence), 0.99)),
                    phone_provenance_type,
                    phone_reason_code,
                    address_text,
                    max(0.0, min(float(address_confidence), 0.99)),
                    address_provenance_type,
                    address_reason_code,
                    Jsonb(metadata or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return NationalProfileRecord(
            fraternity_slug=row["fraternity_slug"],
            fraternity_name=row["fraternity_name"],
            national_url=row["national_url"],
            national_url_confidence=float(row["national_url_confidence"] or 0.0),
            national_url_provenance_type=row["national_url_provenance_type"],
            national_url_reason_code=row["national_url_reason_code"],
            contact_email=row["contact_email"],
            contact_email_confidence=float(row["contact_email_confidence"] or 0.0),
            contact_email_provenance_type=row["contact_email_provenance_type"],
            contact_email_reason_code=row["contact_email_reason_code"],
            instagram_url=row["instagram_url"],
            instagram_confidence=float(row["instagram_confidence"] or 0.0),
            instagram_provenance_type=row["instagram_provenance_type"],
            instagram_reason_code=row["instagram_reason_code"],
            phone=row["phone"],
            phone_confidence=float(row["phone_confidence"] or 0.0),
            phone_provenance_type=row["phone_provenance_type"],
            phone_reason_code=row["phone_reason_code"],
            address_text=row["address_text"],
            address_confidence=float(row["address_confidence"] or 0.0),
            address_provenance_type=row["address_provenance_type"],
            address_reason_code=row["address_reason_code"],
            metadata=row["metadata"] or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def get_school_policy(self, school_name: str | None) -> SchoolPolicyRecord | None:
        school_slug = _normalize_school_slug(school_name)
        if school_slug is None:
            return None
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    school_slug,
                    school_name,
                    greek_life_status,
                    confidence,
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    metadata,
                    last_verified_at::text AS last_verified_at,
                    created_at::text AS created_at,
                    updated_at::text AS updated_at
                FROM school_greek_life_registry
                WHERE school_slug = %s
                LIMIT 1
                """,
                (school_slug,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return SchoolPolicyRecord(
            school_slug=row["school_slug"],
            school_name=row["school_name"],
            greek_life_status=row["greek_life_status"],
            confidence=float(row["confidence"] or 0.0),
            evidence_url=row["evidence_url"],
            evidence_source_type=row["evidence_source_type"],
            reason_code=row["reason_code"],
            metadata=row["metadata"] or {},
            last_verified_at=row["last_verified_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_school_policy(
        self,
        *,
        school_name: str,
        greek_life_status: str,
        confidence: float,
        evidence_url: str | None = None,
        evidence_source_type: str | None = None,
        reason_code: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SchoolPolicyRecord:
        school_slug = _normalize_school_slug(school_name)
        if school_slug is None:
            raise ValueError("school_name is required for school policy upsert")
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO school_greek_life_registry (
                    school_slug,
                    school_name,
                    greek_life_status,
                    confidence,
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    metadata,
                    last_verified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (school_slug)
                DO UPDATE SET
                    school_name = EXCLUDED.school_name,
                    greek_life_status = EXCLUDED.greek_life_status,
                    confidence = EXCLUDED.confidence,
                    evidence_url = EXCLUDED.evidence_url,
                    evidence_source_type = EXCLUDED.evidence_source_type,
                    reason_code = EXCLUDED.reason_code,
                    metadata = COALESCE(school_greek_life_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    last_verified_at = NOW(),
                    updated_at = NOW()
                RETURNING
                    school_slug,
                    school_name,
                    greek_life_status,
                    confidence,
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    metadata,
                    last_verified_at::text AS last_verified_at,
                    created_at::text AS created_at,
                    updated_at::text AS updated_at
                """,
                (
                    school_slug,
                    school_name,
                    greek_life_status,
                    max(0.0, min(float(confidence), 0.99)),
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    Jsonb(metadata or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return SchoolPolicyRecord(
            school_slug=row["school_slug"],
            school_name=row["school_name"],
            greek_life_status=row["greek_life_status"],
            confidence=float(row["confidence"] or 0.0),
            evidence_url=row["evidence_url"],
            evidence_source_type=row["evidence_source_type"],
            reason_code=row["reason_code"],
            metadata=row["metadata"] or {},
            last_verified_at=row["last_verified_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None) -> ChapterActivityRecord | None:
        school_slug = _normalize_school_slug(school_name)
        fraternity_slug = str(fraternity_slug or "").strip()
        if school_slug is None or not fraternity_slug:
            return None
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fraternity_slug,
                    school_slug,
                    school_name,
                    chapter_activity_status,
                    confidence,
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    metadata,
                    last_verified_at::text AS last_verified_at,
                    created_at::text AS created_at,
                    updated_at::text AS updated_at
                FROM fraternity_school_activity_cache
                WHERE fraternity_slug = %s
                  AND school_slug = %s
                LIMIT 1
                """,
                (fraternity_slug, school_slug),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return ChapterActivityRecord(
            fraternity_slug=row["fraternity_slug"],
            school_slug=row["school_slug"],
            school_name=row["school_name"],
            chapter_activity_status=row["chapter_activity_status"],
            confidence=float(row["confidence"] or 0.0),
            evidence_url=row["evidence_url"],
            evidence_source_type=row["evidence_source_type"],
            reason_code=row["reason_code"],
            metadata=row["metadata"] or {},
            last_verified_at=row["last_verified_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def get_accuracy_recovery_metrics(self) -> AccuracyRecoveryMetrics:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH latest_evidence AS (
                  SELECT DISTINCT ON (ce.chapter_id, ce.field_name)
                    ce.chapter_id::text AS chapter_id,
                    ce.field_name,
                    ce.metadata,
                    ce.source_url,
                    ce.created_at
                  FROM chapter_evidence ce
                  WHERE ce.field_name IN ('contact_email', 'instagram_url', 'website_url', 'chapter_status')
                  ORDER BY ce.chapter_id, ce.field_name, ce.created_at DESC
                ),
                enriched AS (
                  SELECT
                    c.id::text AS chapter_id,
                    c.chapter_status,
                    c.field_states,
                    c.contact_provenance,
                    c.website_url,
                    c.contact_email,
                    c.instagram_url,
                    COALESCE(
                      c.contact_provenance -> 'contact_email' ->> 'contactProvenanceType',
                      le_email.metadata ->> 'contactSpecificity'
                    ) AS email_specificity,
                    COALESCE(
                      c.contact_provenance -> 'instagram_url' ->> 'contactProvenanceType',
                      le_instagram.metadata ->> 'contactSpecificity'
                    ) AS instagram_specificity,
                    COALESCE(
                      c.contact_provenance -> 'chapter_status' ->> 'sourceType',
                      le_status.metadata ->> 'evidenceSourceType'
                    ) AS chapter_status_source_type
                  FROM chapters c
                  LEFT JOIN latest_evidence le_email
                    ON le_email.chapter_id = c.id::text
                   AND le_email.field_name = 'contact_email'
                  LEFT JOIN latest_evidence le_instagram
                    ON le_instagram.chapter_id = c.id::text
                   AND le_instagram.field_name = 'instagram_url'
                  LEFT JOIN latest_evidence le_status
                    ON le_status.chapter_id = c.id::text
                   AND le_status.field_name = 'chapter_status'
                )
                SELECT
                  COUNT(*)::int AS total_chapters,
                  COUNT(*) FILTER (
                    WHERE chapter_status = 'active'
                      AND (
                        (contact_email IS NOT NULL AND email_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
                        OR
                        (instagram_url IS NOT NULL AND instagram_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
                      )
                  )::int AS complete_rows,
                  COUNT(*) FILTER (
                    WHERE chapter_status = 'active'
                      AND (
                        (contact_email IS NOT NULL AND email_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
                        OR
                        (instagram_url IS NOT NULL AND instagram_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter'))
                      )
                  )::int AS chapter_specific_contact_rows,
                  COUNT(*) FILTER (
                    WHERE (contact_email IS NOT NULL OR instagram_url IS NOT NULL)
                      AND (contact_email IS NULL OR email_specificity = 'national_generic')
                      AND (instagram_url IS NULL OR instagram_specificity = 'national_generic')
                  )::int AS nationals_only_contact_rows,
                  COUNT(*) FILTER (
                    WHERE chapter_status = 'inactive'
                      AND chapter_status_source_type IN ('official_school', 'school_activity_validation', 'school_policy_validation')
                  )::int AS inactive_validated_rows,
                  COUNT(*) FILTER (
                    WHERE COALESCE(field_states ->> 'website_url', '') = 'confirmed_absent'
                  )::int AS confirmed_absent_website_rows,
                  COUNT(*) FILTER (
                    WHERE chapter_status = 'active'
                      AND contact_email IS NOT NULL
                      AND email_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter')
                  )::int AS active_rows_with_chapter_specific_email,
                  COUNT(*) FILTER (
                    WHERE chapter_status = 'active'
                      AND instagram_url IS NOT NULL
                      AND instagram_specificity IN ('chapter_specific', 'school_specific', 'national_specific_to_chapter')
                  )::int AS active_rows_with_chapter_specific_instagram,
                  COUNT(*) FILTER (
                    WHERE chapter_status = 'active'
                      AND (website_url IS NOT NULL OR contact_email IS NOT NULL OR instagram_url IS NOT NULL)
                  )::int AS active_rows_with_any_contact
                FROM enriched
                """
            )
            row = cursor.fetchone()
        return AccuracyRecoveryMetrics(
            complete_rows=int(row["complete_rows"] or 0),
            chapter_specific_contact_rows=int(row["chapter_specific_contact_rows"] or 0),
            nationals_only_contact_rows=int(row["nationals_only_contact_rows"] or 0),
            inactive_validated_rows=int(row["inactive_validated_rows"] or 0),
            confirmed_absent_website_rows=int(row["confirmed_absent_website_rows"] or 0),
            active_rows_with_chapter_specific_email=int(row["active_rows_with_chapter_specific_email"] or 0),
            active_rows_with_chapter_specific_instagram=int(row["active_rows_with_chapter_specific_instagram"] or 0),
            active_rows_with_any_contact=int(row["active_rows_with_any_contact"] or 0),
            total_chapters=int(row["total_chapters"] or 0),
        )

    def upsert_chapter_activity(
        self,
        *,
        fraternity_slug: str,
        school_name: str,
        chapter_activity_status: str,
        confidence: float,
        evidence_url: str | None = None,
        evidence_source_type: str | None = None,
        reason_code: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ChapterActivityRecord:
        school_slug = _normalize_school_slug(school_name)
        if school_slug is None:
            raise ValueError("school_name is required for chapter activity upsert")
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fraternity_school_activity_cache (
                    fraternity_slug,
                    school_slug,
                    school_name,
                    chapter_activity_status,
                    confidence,
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    metadata,
                    last_verified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (fraternity_slug, school_slug)
                DO UPDATE SET
                    school_name = EXCLUDED.school_name,
                    chapter_activity_status = EXCLUDED.chapter_activity_status,
                    confidence = EXCLUDED.confidence,
                    evidence_url = EXCLUDED.evidence_url,
                    evidence_source_type = EXCLUDED.evidence_source_type,
                    reason_code = EXCLUDED.reason_code,
                    metadata = COALESCE(fraternity_school_activity_cache.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    last_verified_at = NOW(),
                    updated_at = NOW()
                RETURNING
                    fraternity_slug,
                    school_slug,
                    school_name,
                    chapter_activity_status,
                    confidence,
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    metadata,
                    last_verified_at::text AS last_verified_at,
                    created_at::text AS created_at,
                    updated_at::text AS updated_at
                """,
                (
                    fraternity_slug,
                    school_slug,
                    school_name,
                    chapter_activity_status,
                    max(0.0, min(float(confidence), 0.99)),
                    evidence_url,
                    evidence_source_type,
                    reason_code,
                    Jsonb(metadata or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return ChapterActivityRecord(
            fraternity_slug=row["fraternity_slug"],
            school_slug=row["school_slug"],
            school_name=row["school_name"],
            chapter_activity_status=row["chapter_activity_status"],
            confidence=float(row["confidence"] or 0.0),
            evidence_url=row["evidence_url"],
            evidence_source_type=row["evidence_source_type"],
            reason_code=row["reason_code"],
            metadata=row["metadata"] or {},
            last_verified_at=row["last_verified_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_campus_status_source(self, source: CampusStatusSource) -> str:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO campus_status_sources (
                    school_name,
                    source_url,
                    source_host,
                    source_type,
                    authority_tier,
                    currentness_score,
                    completeness_score,
                    parse_completeness_score,
                    is_official_school_source,
                    last_fetched_at,
                    content_hash,
                    title,
                    text_excerpt,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (school_name, source_url)
                DO UPDATE SET
                    source_host = EXCLUDED.source_host,
                    source_type = EXCLUDED.source_type,
                    authority_tier = EXCLUDED.authority_tier,
                    currentness_score = EXCLUDED.currentness_score,
                    completeness_score = EXCLUDED.completeness_score,
                    parse_completeness_score = EXCLUDED.parse_completeness_score,
                    is_official_school_source = EXCLUDED.is_official_school_source,
                    last_fetched_at = EXCLUDED.last_fetched_at,
                    content_hash = EXCLUDED.content_hash,
                    title = EXCLUDED.title,
                    text_excerpt = EXCLUDED.text_excerpt,
                    metadata = EXCLUDED.metadata
                RETURNING id
                """,
                (
                    source.school_name,
                    source.source_url,
                    source.source_host,
                    str(source.source_type),
                    source.authority_tier,
                    float(source.currentness_score),
                    float(source.completeness_score),
                    float(source.parse_completeness_score),
                    bool(source.is_official_school_source),
                    source.last_fetched_at,
                    source.content_hash,
                    source.title[:5000],
                    (source.text or "")[:16000],
                    Jsonb(dict(source.metadata or {})),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"])

    def replace_campus_status_zones(self, *, campus_status_source_id: str, zones: list[StatusZone]) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute("DELETE FROM campus_status_zones WHERE campus_status_source_id = %s", (campus_status_source_id,))
            inserted = 0
            for zone in zones:
                cursor.execute(
                    """
                    INSERT INTO campus_status_zones (
                        campus_status_source_id,
                        zone_type,
                        zone_heading,
                        dom_path,
                        zone_text,
                        links,
                        confidence,
                        parser_version,
                        metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        campus_status_source_id,
                        str(zone.zone_type),
                        zone.heading,
                        zone.dom_path,
                        zone.text[:20000],
                        Jsonb(list(zone.links)),
                        float(zone.confidence),
                        zone.parser_version,
                        Jsonb(dict(zone.metadata or {})),
                    ),
                )
                inserted += 1
        self._connection.commit()
        return inserted

    def insert_chapter_status_evidence(self, evidence: ChapterStatusEvidence) -> str:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapter_status_evidence (
                    chapter_id,
                    fraternity_name,
                    school_name,
                    source_url,
                    authority_tier,
                    evidence_type,
                    status_signal,
                    matched_text,
                    matched_alias,
                    zone_type,
                    match_confidence,
                    evidence_confidence,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    evidence.chapter_id,
                    evidence.fraternity_name,
                    evidence.school_name,
                    evidence.source_url,
                    evidence.authority_tier,
                    evidence.evidence_type,
                    evidence.status_signal,
                    evidence.matched_text,
                    evidence.matched_alias,
                    evidence.zone_type,
                    float(evidence.match_confidence),
                    float(evidence.evidence_confidence),
                    Jsonb(dict(evidence.metadata or {})),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"])

    def insert_chapter_status_decision(self, *, chapter_id: str, decision: ChapterStatusDecision) -> ChapterStatusDecision:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapter_status_decisions (
                    chapter_id,
                    final_status,
                    school_recognition_status,
                    national_status,
                    confidence,
                    reason_code,
                    conflict_flags,
                    evidence_ids,
                    decision_trace,
                    review_required
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, decided_at
                """,
                (
                    chapter_id,
                    str(decision.final_status),
                    str(decision.school_recognition_status),
                    str(decision.national_status),
                    float(decision.confidence),
                    decision.reason_code,
                    Jsonb(list(decision.conflict_flags)),
                    list(decision.evidence_ids),
                    Jsonb(dict(decision.decision_trace or {})),
                    bool(decision.review_required),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return decision.model_copy(
            update={
                "id": str(row["id"]),
                "chapter_id": chapter_id,
                "decided_at": row["decided_at"].isoformat() if row["decided_at"] else None,
            }
        )

    def get_latest_chapter_status_decision(self, chapter_id: str) -> ChapterStatusDecision | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    chapter_id,
                    final_status,
                    school_recognition_status,
                    national_status,
                    confidence,
                    reason_code,
                    conflict_flags,
                    evidence_ids,
                    decision_trace,
                    review_required,
                    decided_at
                FROM chapter_status_decisions
                WHERE chapter_id = %s
                ORDER BY decided_at DESC, id DESC
                LIMIT 1
                """,
                (chapter_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return ChapterStatusDecision(
            id=str(row["id"]),
            chapter_id=str(row["chapter_id"]),
            final_status=str(row["final_status"] or "unknown"),
            school_recognition_status=str(row["school_recognition_status"] or "unknown"),
            national_status=str(row["national_status"] or NationalStatusValue.UNKNOWN.value),
            confidence=float(row["confidence"] or 0.0),
            reason_code=str(row["reason_code"] or ""),
            conflict_flags=list(row["conflict_flags"] or []),
            evidence_ids=[str(value) for value in list(row["evidence_ids"] or [])],
            decision_trace=dict(row["decision_trace"] or {}),
            review_required=bool(row["review_required"]),
            decided_at=row["decided_at"].isoformat() if row["decided_at"] else None,
        )

    def upsert_fraternity(self, slug: str, name: str, nic_affiliated: bool = True) -> tuple[str, str]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fraternities (slug, name, nic_affiliated)
                VALUES (%s, %s, %s)
                ON CONFLICT (slug)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    nic_affiliated = EXCLUDED.nic_affiliated,
                    updated_at = NOW()
                RETURNING id, slug
                """,
                (slug, name, nic_affiliated),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"]), row["slug"]

    def upsert_source(
        self,
        *,
        fraternity_id: str,
        slug: str,
        base_url: str,
        list_path: str | None = None,
        source_type: str = "unsupported",
        parser_key: str = "unsupported",
        active: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, active, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug)
                DO UPDATE SET
                    fraternity_id = EXCLUDED.fraternity_id,
                    source_type = EXCLUDED.source_type,
                    parser_key = EXCLUDED.parser_key,
                    base_url = EXCLUDED.base_url,
                    list_path = EXCLUDED.list_path,
                    active = EXCLUDED.active,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING id, slug
                """,
                (
                    fraternity_id,
                    slug,
                    source_type,
                    parser_key,
                    base_url,
                    list_path,
                    active,
                    Jsonb(metadata or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"]), row["slug"]

    def start_crawl_run(self, source_id: str) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_runs (source_id, status)
                VALUES (%s, 'running')
                RETURNING id
                """,
                (source_id,),
            )
            run_id = int(cursor.fetchone()["id"])
        self._connection.commit()
        return run_id

    def finish_crawl_run(
        self,
        run_id: int,
        status: str,
        metrics: CrawlMetrics,
        last_error: str | None = None,
        *,
        page_analysis: dict[str, Any] | None = None,
        classification: dict[str, Any] | None = None,
        extraction_metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE crawl_runs
                SET
                    status = %(status)s,
                    finished_at = NOW(),
                    pages_processed = %(pages_processed)s,
                    records_seen = %(records_seen)s,
                    records_upserted = %(records_upserted)s,
                    review_items_created = %(review_items_created)s,
                    field_jobs_created = %(field_jobs_created)s,
                    last_error = %(last_error)s,
                    page_analysis = %(page_analysis)s,
                    classification = %(classification)s,
                    extraction_metadata = %(extraction_metadata)s
                WHERE id = %(run_id)s
                """,
                {
                    "run_id": run_id,
                    "status": status,
                    "pages_processed": metrics.pages_processed,
                    "records_seen": metrics.records_seen,
                    "records_upserted": metrics.records_upserted,
                    "review_items_created": metrics.review_items_created,
                    "field_jobs_created": metrics.field_jobs_created,
                    "last_error": last_error,
                    "page_analysis": Jsonb(page_analysis) if page_analysis is not None else None,
                    "classification": Jsonb(classification) if classification is not None else None,
                    "extraction_metadata": Jsonb(extraction_metadata or {}),
                },
            )
        self._connection.commit()

    def upsert_chapter(self, source: SourceRecord, chapter: NormalizedChapter) -> str:
        normalized_state = normalize_us_state(chapter.state)
        self._contracts.validate_chapter(
            {
                "fraternitySlug": chapter.fraternity_slug,
                "sourceSlug": chapter.source_slug,
                "externalId": chapter.external_id,
                "slug": chapter.slug,
                "name": chapter.name,
                "universityName": chapter.university_name,
                "city": chapter.city,
                "state": normalized_state,
                "country": chapter.country,
                "websiteUrl": chapter.website_url,
                "chapterStatus": chapter.chapter_status,
                "missingOptionalFields": chapter.missing_optional_fields,
                "fieldStates": chapter.field_states,
            }
        )

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapters (
                    fraternity_id,
                    external_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    chapter_status,
                    field_states,
                    normalized_address,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    %(fraternity_id)s,
                    %(external_id)s,
                    %(slug)s,
                    %(name)s,
                    %(university_name)s,
                    %(city)s,
                    %(state)s,
                    %(country)s,
                    %(website_url)s,
                    %(instagram_url)s,
                    %(contact_email)s,
                    %(chapter_status)s,
                    %(field_states)s,
                    '{}'::jsonb,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (fraternity_id, slug)
                DO UPDATE SET
                    external_id = COALESCE(EXCLUDED.external_id, chapters.external_id),
                    name = EXCLUDED.name,
                    university_name = EXCLUDED.university_name,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    country = EXCLUDED.country,
                    website_url = COALESCE(EXCLUDED.website_url, chapters.website_url),
                    instagram_url = COALESCE(EXCLUDED.instagram_url, chapters.instagram_url),
                    contact_email = COALESCE(EXCLUDED.contact_email, chapters.contact_email),
                    chapter_status = EXCLUDED.chapter_status,
                    field_states = EXCLUDED.field_states,
                    last_seen_at = NOW()
                RETURNING id
                """,
                {
                    "fraternity_id": source.fraternity_id,
                    "external_id": chapter.external_id,
                    "slug": chapter.slug,
                    "name": chapter.name,
                    "university_name": chapter.university_name,
                    "city": chapter.city,
                    "state": normalized_state,
                    "country": chapter.country,
                    "website_url": chapter.website_url,
                    "instagram_url": chapter.instagram_url,
                    "contact_email": chapter.contact_email,
                    "chapter_status": chapter.chapter_status,
                    "field_states": Jsonb(chapter.field_states),
                },
            )
            chapter_id = str(cursor.fetchone()["id"])
        self._connection.commit()
        return chapter_id

    def upsert_chapter_discovery(self, source: SourceRecord, chapter: NormalizedChapter) -> str:
        normalized_state = normalize_us_state(chapter.state)
        self._contracts.validate_chapter(
            {
                "fraternitySlug": chapter.fraternity_slug,
                "sourceSlug": chapter.source_slug,
                "externalId": chapter.external_id,
                "slug": chapter.slug,
                "name": chapter.name,
                "universityName": chapter.university_name,
                "city": chapter.city,
                "state": normalized_state,
                "country": chapter.country,
                "websiteUrl": chapter.website_url,
                "chapterStatus": chapter.chapter_status,
                "missingOptionalFields": chapter.missing_optional_fields,
                "fieldStates": chapter.field_states,
            }
        )
        field_states = {key: value for key, value in (chapter.field_states or {}).items() if value == "found"}
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapters (
                    fraternity_id,
                    external_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    chapter_status,
                    field_states,
                    normalized_address,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    %(fraternity_id)s,
                    %(external_id)s,
                    %(slug)s,
                    %(name)s,
                    %(university_name)s,
                    %(city)s,
                    %(state)s,
                    %(country)s,
                    %(website_url)s,
                    %(instagram_url)s,
                    %(contact_email)s,
                    %(chapter_status)s,
                    %(field_states)s,
                    '{}'::jsonb,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (fraternity_id, slug)
                DO UPDATE SET
                    external_id = COALESCE(chapters.external_id, EXCLUDED.external_id),
                    name = COALESCE(chapters.name, EXCLUDED.name),
                    university_name = COALESCE(chapters.university_name, EXCLUDED.university_name),
                    city = COALESCE(chapters.city, EXCLUDED.city),
                    state = COALESCE(chapters.state, EXCLUDED.state),
                    country = COALESCE(chapters.country, EXCLUDED.country),
                    website_url = COALESCE(chapters.website_url, EXCLUDED.website_url),
                    instagram_url = COALESCE(chapters.instagram_url, EXCLUDED.instagram_url),
                    contact_email = COALESCE(chapters.contact_email, EXCLUDED.contact_email),
                    chapter_status = COALESCE(chapters.chapter_status, EXCLUDED.chapter_status),
                    field_states = COALESCE(chapters.field_states, '{}'::jsonb) || EXCLUDED.field_states,
                    last_seen_at = NOW()
                RETURNING id
                """,
                {
                    "fraternity_id": source.fraternity_id,
                    "external_id": chapter.external_id,
                    "slug": chapter.slug,
                    "name": chapter.name,
                    "university_name": chapter.university_name,
                    "city": chapter.city,
                    "state": normalized_state,
                    "country": chapter.country,
                    "website_url": chapter.website_url,
                    "instagram_url": chapter.instagram_url,
                    "contact_email": chapter.contact_email,
                    "chapter_status": chapter.chapter_status,
                    "field_states": Jsonb(field_states),
                },
            )
            chapter_id = str(cursor.fetchone()["id"])
        self._connection.commit()
        return chapter_id

    def insert_provenance(
        self,
        chapter_id: str,
        source_id: str,
        crawl_run_id: int,
        records: list[ProvenanceRecord],
    ) -> None:
        if not records:
            return

        with self._connection.cursor() as cursor:
            for record in records:
                payload = asdict(record)
                self._contracts.validate_provenance(
                    {
                        "sourceSlug": payload["source_slug"],
                        "sourceUrl": payload["source_url"],
                        "fieldName": payload["field_name"],
                        "fieldValue": payload["field_value"],
                        "sourceSnippet": payload["source_snippet"],
                        "confidence": payload["confidence"],
                    }
                )
                cursor.execute(
                    """
                    INSERT INTO chapter_provenance (
                        chapter_id,
                        source_id,
                        crawl_run_id,
                        field_name,
                        field_value,
                        source_url,
                        source_snippet,
                        confidence
                    )
                    VALUES (%(chapter_id)s, %(source_id)s, %(crawl_run_id)s, %(field_name)s, %(field_value)s, %(source_url)s, %(source_snippet)s, %(confidence)s)
                    """,
                    {
                        "chapter_id": chapter_id,
                        "source_id": source_id,
                        "crawl_run_id": crawl_run_id,
                        "field_name": record.field_name,
                        "field_value": record.field_value,
                        "source_url": record.source_url,
                        "source_snippet": record.source_snippet,
                        "confidence": record.confidence,
                    },
                )
        self._connection.commit()

    def create_review_item(self, source_id: str | None, crawl_run_id: int | None, candidate: ReviewItemCandidate, chapter_id: str | None = None) -> None:
        review_payload = {
            "itemType": candidate.item_type,
            "reason": candidate.reason,
            "sourceSlug": candidate.source_slug,
            "chapterSlug": candidate.chapter_slug,
            "payload": candidate.payload,
        }
        extraction_notes = candidate.payload.get("extractionNotes") if isinstance(candidate.payload, dict) else None
        if isinstance(extraction_notes, str) and extraction_notes.strip():
            review_payload["extractionNotes"] = extraction_notes

        self._contracts.validate_review_item(review_payload)

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO review_items (source_id, crawl_run_id, chapter_id, item_type, reason, payload)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    source_id,
                    crawl_run_id,
                    chapter_id,
                    candidate.item_type,
                    candidate.reason,
                    Jsonb(candidate.payload),
                ),
            )
        self._connection.commit()

    def insert_chapter_evidence(self, record: ChapterEvidenceRecord) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapter_evidence (
                    chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    request_id,
                    crawl_run_id,
                    field_name,
                    candidate_value,
                    confidence,
                    trust_tier,
                    evidence_status,
                    source_url,
                    source_snippet,
                    provider,
                    query,
                    related_website_url,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    record.chapter_id,
                    record.chapter_slug,
                    record.fraternity_slug,
                    record.source_slug,
                    record.request_id,
                    record.crawl_run_id,
                    record.field_name,
                    record.candidate_value,
                    record.confidence,
                    record.trust_tier,
                    record.evidence_status,
                    record.source_url,
                    record.source_snippet,
                    record.provider,
                    record.query,
                    record.related_website_url,
                    Jsonb(record.metadata),
                ),
            )
        self._connection.commit()

    def fetch_instagram_candidates_for_chapters(self, chapter_ids: list[str]) -> list[ChapterEvidenceRecord]:
        normalized_ids = [str(chapter_id).strip() for chapter_id in chapter_ids if str(chapter_id).strip()]
        if not normalized_ids:
            return []
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    chapter_id::text AS chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    request_id,
                    crawl_run_id,
                    field_name,
                    candidate_value,
                    confidence,
                    trust_tier,
                    evidence_status,
                    source_url,
                    source_snippet,
                    provider,
                    query,
                    related_website_url,
                    metadata
                FROM chapter_evidence
                WHERE field_name = 'instagram_url'
                  AND chapter_id = ANY(%s::uuid[])
                ORDER BY chapter_id, created_at DESC
                """,
                (normalized_ids,),
            )
            rows = cursor.fetchall()
        return [
            ChapterEvidenceRecord(
                chapter_id=str(row["chapter_id"]) if row["chapter_id"] is not None else None,
                chapter_slug=row["chapter_slug"] or "",
                fraternity_slug=row["fraternity_slug"],
                source_slug=row["source_slug"],
                request_id=row["request_id"],
                crawl_run_id=row["crawl_run_id"],
                field_name=row["field_name"],
                candidate_value=row["candidate_value"],
                confidence=float(row["confidence"]) if row["confidence"] is not None else None,
                trust_tier=row["trust_tier"] or "medium",
                evidence_status=row["evidence_status"] or "observed",
                source_url=row["source_url"],
                source_snippet=row["source_snippet"],
                provider=row["provider"],
                query=row["query"],
                related_website_url=row["related_website_url"],
                metadata=row["metadata"] or {},
            )
            for row in rows
        ]

    def normalize_instagram_candidate_source_types(self) -> dict[str, Any]:
        valid_source_types = [
            "existing_db_value",
            "provenance_supporting_page",
            "authoritative_bundle",
            "nationals_chapter_entry",
            "nationals_chapter_page",
            "nationals_directory_row",
            "official_school_chapter_page",
            "official_school_directory_row",
            "verified_chapter_website",
            "chapter_website_structured_data",
            "chapter_website_social_link",
            "search_result_profile",
            "generated_handle_search",
            "national_following_seed",
            "review_override",
        ]
        legacy_aliases = [
            "official_school",
            "official_school_page",
            "school_page",
            "school_directory",
            "chapter_site",
            "chapter_website",
            "chapter_website_structured_data",
            "chapter_website_social_link",
            "nationals",
            "nationals_page",
            "nationals_row",
            "national_directory_row",
            "search",
            "search_result",
            "instagram_search",
            "generated_handle",
            "provenance",
            "supporting_page",
            "source_page",
            "existing_db",
        ]
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*)::int AS total_rows,
                    COUNT(*) FILTER (
                        WHERE NULLIF(BTRIM(COALESCE(metadata ->> 'evidenceSourceType', metadata ->> 'sourceType', '')), '') IS NULL
                    )::int AS missing_source_type,
                    COUNT(*) FILTER (
                        WHERE NULLIF(BTRIM(COALESCE(metadata ->> 'evidenceSourceType', metadata ->> 'sourceType', '')), '') IS NOT NULL
                          AND LOWER(BTRIM(COALESCE(metadata ->> 'evidenceSourceType', metadata ->> 'sourceType', ''))) = ANY(%s::text[])
                    )::int AS legacy_source_type
                FROM chapter_evidence
                WHERE field_name = 'instagram_url'
                """,
                (legacy_aliases,),
            )
            before = dict(cursor.fetchone() or {})
            cursor.execute(
                """
                WITH candidates AS (
                    SELECT
                        ce.id,
                        CASE
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) = 'official_school'
                                THEN 'official_school_chapter_page'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) IN ('official_school_page', 'school_page')
                                THEN 'official_school_chapter_page'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) = 'school_directory'
                                THEN 'official_school_directory_row'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) IN ('chapter_site', 'chapter_website')
                                THEN 'verified_chapter_website'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) = 'chapter_website_structured_data'
                                THEN 'chapter_website_structured_data'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) = 'chapter_website_social_link'
                                THEN 'chapter_website_social_link'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) IN ('nationals', 'nationals_page')
                                THEN 'nationals_chapter_page'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) IN ('nationals_row', 'national_directory_row')
                                THEN 'nationals_directory_row'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) IN ('search', 'search_result', 'instagram_search')
                                THEN 'search_result_profile'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) = 'generated_handle'
                                THEN 'generated_handle_search'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) IN ('provenance', 'supporting_page', 'source_page')
                                THEN 'provenance_supporting_page'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) = 'existing_db'
                                THEN 'existing_db_value'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'contactSpecificity', ''))) = 'school_specific'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'pageScope', ''))) LIKE 'school%%'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'supportingPageUrl', ce.source_url, ''))) LIKE '%%.edu%%'
                                THEN 'official_school_chapter_page'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'contactSpecificity', ''))) = 'national_specific_to_chapter'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'pageScope', ''))) LIKE '%%nation%%'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'supportingPageUrl', ce.source_url, ''))) LIKE '%%chapter-directory%%'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'supportingPageUrl', ce.source_url, ''))) LIKE '%%find-a-chapter%%'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'supportingPageUrl', ce.source_url, ''))) LIKE '%%/chapters%%'
                                THEN 'nationals_chapter_page'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'contactSpecificity', ''))) = 'chapter_specific'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'pageScope', ''))) LIKE '%%chapter_website%%'
                                OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'pageScope', ''))) LIKE '%%chapter_site%%'
                                THEN 'verified_chapter_website'
                            WHEN LOWER(BTRIM(COALESCE(ce.metadata ->> 'supportingPageUrl', ce.source_url, ''))) LIKE '%%instagram.com/%%'
                                OR NULLIF(BTRIM(COALESCE(ce.provider, '')), '') IS NOT NULL
                                OR NULLIF(BTRIM(COALESCE(ce.query, '')), '') IS NOT NULL
                                THEN 'search_result_profile'
                            WHEN NULLIF(BTRIM(COALESCE(ce.metadata ->> 'supportingPageUrl', ce.source_url, '')), '') IS NOT NULL
                                THEN 'provenance_supporting_page'
                            ELSE 'search_result_profile'
                        END AS normalized_source_type
                    FROM chapter_evidence ce
                    WHERE ce.field_name = 'instagram_url'
                      AND (
                        NULLIF(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', '')), '') IS NULL
                        OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) = ANY(%s::text[])
                        OR LOWER(BTRIM(COALESCE(ce.metadata ->> 'evidenceSourceType', ce.metadata ->> 'sourceType', ''))) <> ALL(%s::text[])
                      )
                ),
                applied AS (
                    UPDATE chapter_evidence ce
                    SET metadata = jsonb_set(
                        jsonb_set(COALESCE(ce.metadata, '{}'::jsonb), '{evidenceSourceType}', to_jsonb(candidates.normalized_source_type), true),
                        '{sourceType}',
                        to_jsonb(candidates.normalized_source_type),
                        true
                    )
                    FROM candidates
                    WHERE ce.id = candidates.id
                    RETURNING ce.id
                )
                SELECT COUNT(*)::int AS updated_rows FROM applied
                """,
                (legacy_aliases, valid_source_types),
            )
            updated = dict(cursor.fetchone() or {})
            cursor.execute(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE NULLIF(BTRIM(COALESCE(metadata ->> 'evidenceSourceType', metadata ->> 'sourceType', '')), '') IS NULL
                    )::int AS missing_source_type,
                    COUNT(*) FILTER (
                        WHERE LOWER(BTRIM(COALESCE(metadata ->> 'evidenceSourceType', metadata ->> 'sourceType', ''))) <> ALL(%s::text[])
                    )::int AS invalid_source_type
                FROM chapter_evidence
                WHERE field_name = 'instagram_url'
                """,
                (valid_source_types,),
            )
            after = dict(cursor.fetchone() or {})
            cursor.execute(
                """
                SELECT
                    COALESCE(NULLIF(BTRIM(COALESCE(metadata ->> 'evidenceSourceType', metadata ->> 'sourceType', '')), ''), 'missing') AS source_type,
                    COUNT(*)::int AS count
                FROM chapter_evidence
                WHERE field_name = 'instagram_url'
                GROUP BY source_type
                ORDER BY count DESC, source_type ASC
                """,
            )
            distribution = [
                {
                    "sourceType": str(row["source_type"]),
                    "count": int(row["count"] or 0),
                }
                for row in cursor.fetchall()
            ]
        self._connection.commit()
        return {
            "totalRows": int(before.get("total_rows") or 0),
            "missingSourceTypeBefore": int(before.get("missing_source_type") or 0),
            "legacySourceTypeBefore": int(before.get("legacy_source_type") or 0),
            "updatedRows": int(updated.get("updated_rows") or 0),
            "missingSourceTypeAfter": int(after.get("missing_source_type") or 0),
            "invalidSourceTypeAfter": int(after.get("invalid_source_type") or 0),
            "distribution": distribution,
        }

    def apply_instagram_resolution(
        self,
        *,
        chapter_id: str,
        chapter_slug: str,
        fraternity_slug: str | None,
        source_slug: str | None,
        crawl_run_id: int | None,
        request_id: str | None,
        instagram_url: str,
        confidence: float,
        source_url: str | None,
        source_snippet: str | None,
        reason_code: str,
        page_scope: str | None,
        contact_specificity: str | None,
        source_type: str | None,
        decision_stage: str,
        allow_replace: bool = False,
        previous_url: str | None = None,
    ) -> bool:
        normalized_instagram = sanitize_as_instagram(instagram_url)
        if normalized_instagram is None:
            return False
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT chapter_status, instagram_url
                FROM chapters
                WHERE id = %s
                """,
                (chapter_id,),
            )
            chapter_row = cursor.fetchone()
            if chapter_row is None:
                return False
            current_status = str(chapter_row["chapter_status"] or "").strip().lower()
            if current_status != "active":
                return False
            current_instagram = sanitize_as_instagram(chapter_row["instagram_url"])
            if current_instagram and current_instagram != normalized_instagram and not allow_replace:
                return False

            contact_provenance_patch = {
                "instagram_url": {
                    "supportingPageUrl": source_url,
                    "supportingPageScope": _normalize_page_scope(page_scope),
                    "contactProvenanceType": _normalize_contact_specificity(contact_specificity),
                    "decisionStage": decision_stage,
                    "sourceType": source_type,
                    "reasonCode": reason_code,
                    "confidence": round(float(confidence or 0.0), 4),
                    "decisionOutcome": DECISION_OUTCOME_ACCEPTED,
                    "fieldResolutionState": FIELD_RESOLUTION_RESOLVED,
                    "candidateValue": normalized_instagram,
                    "updatedAt": datetime.utcnow().isoformat(),
                }
            }
            cursor.execute(
                """
                UPDATE chapters
                SET
                    instagram_url = CASE
                        WHEN %(allow_replace)s THEN %(instagram_url)s
                        ELSE COALESCE(instagram_url, %(instagram_url)s)
                    END,
                    field_states = COALESCE(field_states, '{}'::jsonb) || %(field_states)s,
                    contact_provenance = COALESCE(contact_provenance, '{}'::jsonb) || %(contact_provenance)s,
                    updated_at = NOW()
                WHERE id = %(chapter_id)s
                """,
                {
                    "chapter_id": chapter_id,
                    "instagram_url": normalized_instagram,
                    "allow_replace": bool(allow_replace),
                    "field_states": Jsonb({"instagram_url": "found"}),
                    "contact_provenance": Jsonb(contact_provenance_patch),
                },
            )

            source_id: str | None = None
            if crawl_run_id is not None:
                cursor.execute("SELECT source_id FROM crawl_runs WHERE id = %s", (crawl_run_id,))
                source_row = cursor.fetchone()
                if source_row is not None and source_row["source_id"] is not None:
                    source_id = str(source_row["source_id"])
            if source_id is not None and crawl_run_id is not None:
                cursor.execute(
                    """
                    INSERT INTO chapter_provenance (
                        chapter_id,
                        source_id,
                        crawl_run_id,
                        field_name,
                        field_value,
                        source_url,
                        source_snippet,
                        confidence
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        chapter_id,
                        source_id,
                        crawl_run_id,
                        "instagram_url",
                        normalized_instagram,
                        source_url,
                        (source_snippet or reason_code or normalized_instagram)[:400],
                        float(confidence or 0.0),
                    ),
                )

            trust_tier = (
                "strong_official"
                if float(confidence or 0.0) >= 0.95
                else "high"
                if float(confidence or 0.0) >= 0.85
                else "medium"
            )
            cursor.execute(
                """
                INSERT INTO chapter_evidence (
                    chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    request_id,
                    crawl_run_id,
                    field_name,
                    candidate_value,
                    confidence,
                    trust_tier,
                    evidence_status,
                    source_url,
                    source_snippet,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    request_id,
                    crawl_run_id,
                    "instagram_url",
                    normalized_instagram,
                    float(confidence or 0.0),
                    trust_tier,
                    "accepted",
                    source_url,
                    (source_snippet or normalized_instagram)[:400],
                    Jsonb(
                        {
                            "reasonCode": reason_code,
                            "pageScope": _normalize_page_scope(page_scope),
                            "contactSpecificity": _normalize_contact_specificity(contact_specificity),
                            "evidenceSourceType": source_type,
                            "supportingPageUrl": source_url,
                            "supportingConfidence": round(float(confidence or 0.0), 4),
                            "decisionStage": decision_stage,
                            "allowReplaceExisting": bool(allow_replace),
                            "previousUrl": previous_url,
                            "requestId": request_id,
                        }
                    ),
                ),
            )
        self._connection.commit()
        return True

    def upsert_provisional_chapter(
        self,
        *,
        fraternity_id: str,
        slug: str,
        name: str,
        status: str = "provisional",
        source_id: str | None = None,
        request_id: str | None = None,
        university_name: str | None = None,
        city: str | None = None,
        state: str | None = None,
        country: str = "USA",
        website_url: str | None = None,
        instagram_url: str | None = None,
        contact_email: str | None = None,
        promotion_reason: str | None = None,
        promoted_chapter_id: str | None = None,
        evidence_payload: dict[str, Any] | None = None,
    ) -> str:
        normalized_state = normalize_us_state(state)
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO provisional_chapters (
                    fraternity_id,
                    source_id,
                    request_id,
                    promoted_chapter_id,
                    slug,
                    name,
                    university_name,
                    city,
                    normalized_state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    status,
                    promotion_reason,
                    evidence_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fraternity_id, slug)
                DO UPDATE SET
                    source_id = COALESCE(EXCLUDED.source_id, provisional_chapters.source_id),
                    request_id = COALESCE(EXCLUDED.request_id, provisional_chapters.request_id),
                    promoted_chapter_id = COALESCE(EXCLUDED.promoted_chapter_id, provisional_chapters.promoted_chapter_id),
                    name = EXCLUDED.name,
                    university_name = COALESCE(EXCLUDED.university_name, provisional_chapters.university_name),
                    city = COALESCE(EXCLUDED.city, provisional_chapters.city),
                    state = COALESCE(EXCLUDED.state, provisional_chapters.state),
                    country = COALESCE(EXCLUDED.country, provisional_chapters.country),
                    website_url = COALESCE(EXCLUDED.website_url, provisional_chapters.website_url),
                    instagram_url = COALESCE(EXCLUDED.instagram_url, provisional_chapters.instagram_url),
                    contact_email = COALESCE(EXCLUDED.contact_email, provisional_chapters.contact_email),
                    status = EXCLUDED.status,
                    promotion_reason = COALESCE(EXCLUDED.promotion_reason, provisional_chapters.promotion_reason),
                    evidence_payload = COALESCE(provisional_chapters.evidence_payload, '{}'::jsonb) || EXCLUDED.evidence_payload,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    fraternity_id,
                    source_id,
                    request_id,
                    promoted_chapter_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    status,
                    promotion_reason,
                    Jsonb(evidence_payload or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"])

    def apply_inline_enrichment_result(
        self,
        *,
        chapter_id: str,
        chapter_slug: str,
        fraternity_slug: str | None,
        source_slug: str | None,
        source_id: str | None,
        crawl_run_id: int | None,
        chapter_updates: dict[str, str],
        completed_payload: dict[str, Any],
        field_state_updates: dict[str, str] | None = None,
        provenance_records: list[ProvenanceRecord] | None = None,
    ) -> None:
        field_state_updates = field_state_updates or {}
        provenance_records = provenance_records or []
        decision_evidence = _build_decision_evidence(completed_payload)
        contact_field_writes = any(
            field_name in chapter_updates for field_name in ("website_url", "instagram_url", "contact_email")
        )
        status_decision_id = str(
            decision_evidence.metadata.get("statusDecisionId")
            or completed_payload.get("statusDecisionId")
            or ""
        ).strip() or None
        operator_override_reason = str(
            decision_evidence.metadata.get("operatorOverrideReason")
            or completed_payload.get("operatorOverrideReason")
            or ""
        ).strip() or None
        contact_provenance_patch = _build_contact_provenance_patch(
            chapter_updates=chapter_updates,
            field_state_updates=field_state_updates,
            completed_payload=completed_payload,
            provenance_records=provenance_records,
        )
        with self._connection.transaction(), self._connection.cursor() as cursor:
            if contact_field_writes and not operator_override_reason:
                cursor.execute(
                    """
                    SELECT id, final_status
                    FROM chapter_status_decisions
                    WHERE chapter_id = %s
                    ORDER BY decided_at DESC, id DESC
                    LIMIT 1
                    """,
                    (chapter_id,),
                )
                status_row = cursor.fetchone()
                if status_row is None:
                    raise ValueError("inline contact writes require an existing chapter_status_decision")
                latest_status_decision_id = str(status_row["id"])
                latest_final_status = str(status_row["final_status"] or "").strip().lower()
                if latest_final_status != "active":
                    raise ValueError("inactive/unknown/review chapters cannot receive new contact writes")
                if status_decision_id is not None and latest_status_decision_id != status_decision_id:
                    raise ValueError("inline contact write statusDecisionId does not match the latest chapter_status_decision")
            if chapter_updates or field_state_updates:
                cursor.execute(
                    """
                    UPDATE chapters
                    SET
                        website_url = CASE
                            WHEN %(website_url)s::text IS NULL THEN website_url
                            WHEN website_url IS NULL THEN %(website_url)s::text
                            WHEN website_url !~* '^https?://' THEN %(website_url)s::text
                            ELSE website_url
                        END,
                        instagram_url = COALESCE(instagram_url, %(instagram_url)s),
                        contact_email = COALESCE(contact_email, %(contact_email)s),
                        university_name = COALESCE(university_name, %(university_name)s),
                        chapter_status = COALESCE(%(chapter_status)s, chapter_status),
                        field_states = COALESCE(field_states, '{}'::jsonb) || %(field_states)s,
                        contact_provenance = COALESCE(contact_provenance, '{}'::jsonb) || %(contact_provenance)s,
                        updated_at = NOW()
                    WHERE id = %(chapter_id)s
                    """,
                    {
                        "chapter_id": chapter_id,
                        "website_url": chapter_updates.get("website_url"),
                        "instagram_url": chapter_updates.get("instagram_url"),
                        "contact_email": chapter_updates.get("contact_email"),
                        "university_name": chapter_updates.get("university_name"),
                        "chapter_status": chapter_updates.get("chapter_status"),
                        "field_states": Jsonb(field_state_updates),
                        "contact_provenance": Jsonb(contact_provenance_patch),
                    },
                )

            completed_status = str(completed_payload.get("status") or "observed")
            provider = completed_payload.get("provider") or completed_payload.get("source_provider")
            query = completed_payload.get("query")
            related_website_url = completed_payload.get("related_website_url")

            for record in provenance_records:
                payload = asdict(record)
                self._contracts.validate_provenance(
                    {
                        "sourceSlug": payload["source_slug"],
                        "sourceUrl": payload["source_url"],
                        "fieldName": payload["field_name"],
                        "fieldValue": payload["field_value"],
                        "sourceSnippet": payload["source_snippet"],
                        "confidence": payload["confidence"],
                    }
                )
                if source_id is None or crawl_run_id is None:
                    continue
                cursor.execute(
                    """
                    INSERT INTO chapter_provenance (
                        chapter_id,
                        source_id,
                        crawl_run_id,
                        field_name,
                        field_value,
                        source_url,
                        source_snippet,
                        confidence
                    )
                    VALUES (%(chapter_id)s, %(source_id)s, %(crawl_run_id)s, %(field_name)s, %(field_value)s, %(source_url)s, %(source_snippet)s, %(confidence)s)
                    """,
                    {
                        "chapter_id": chapter_id,
                        "source_id": source_id,
                        "crawl_run_id": crawl_run_id,
                        "field_name": record.field_name,
                        "field_value": record.field_value,
                        "source_url": record.source_url,
                        "source_snippet": record.source_snippet,
                        "confidence": record.confidence,
                    },
                )
                evidence_status = "accepted" if completed_status == "updated" else "review" if completed_status == "review_required" else "observed"
                trust_tier = "strong_official" if record.confidence >= 0.95 else "high" if record.confidence >= 0.85 else "medium" if record.confidence >= 0.7 else "low"
                cursor.execute(
                    """
                    INSERT INTO chapter_evidence (
                        chapter_id,
                        chapter_slug,
                        fraternity_slug,
                        source_slug,
                        crawl_run_id,
                        field_name,
                        candidate_value,
                        confidence,
                        trust_tier,
                        evidence_status,
                        source_url,
                        source_snippet,
                        provider,
                        query,
                        related_website_url,
                        metadata
                    )
                    VALUES (%(chapter_id)s, %(chapter_slug)s, %(fraternity_slug)s, %(source_slug)s, %(crawl_run_id)s, %(field_name)s, %(candidate_value)s, %(confidence)s, %(trust_tier)s, %(evidence_status)s, %(source_url)s, %(source_snippet)s, %(provider)s, %(query)s, %(related_website_url)s, %(metadata)s)
                    """,
                    {
                        "chapter_id": chapter_id,
                        "chapter_slug": chapter_slug,
                        "fraternity_slug": fraternity_slug,
                        "source_slug": source_slug,
                        "crawl_run_id": crawl_run_id,
                        "field_name": record.field_name,
                        "candidate_value": record.field_value,
                        "confidence": record.confidence,
                        "trust_tier": trust_tier,
                        "evidence_status": evidence_status,
                        "source_url": record.source_url,
                        "source_snippet": record.source_snippet,
                        "provider": provider,
                        "query": query,
                        "related_website_url": related_website_url if record.field_name != "website_url" else None,
                        "metadata": Jsonb(
                            {
                                "runtime": "inline_v3",
                                "completedStatus": completed_status,
                                "fieldState": field_state_updates.get(record.field_name),
                                "decisionStage": decision_evidence.decision_stage,
                                "pageScope": decision_evidence.page_scope,
                                "contactSpecificity": decision_evidence.contact_specificity,
                                "evidenceSourceType": decision_evidence.source_type,
                                "reasonCode": decision_evidence.reason_code,
                                "supportingPageUrl": decision_evidence.evidence_url,
                                "supportingConfidence": decision_evidence.confidence,
                                **decision_evidence.metadata,
                            }
                        ),
                    },
                )

    def list_chapters_for_crawl_run(self, crawl_run_id: int) -> list[dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (c.id)
                    c.id::text AS chapter_id,
                    c.slug AS chapter_slug,
                    c.name AS chapter_name,
                    c.university_name,
                    c.chapter_status,
                    c.website_url,
                    c.instagram_url,
                    c.contact_email,
                    c.field_states,
                    f.slug AS fraternity_slug
                FROM chapter_provenance cp
                JOIN chapters c ON c.id = cp.chapter_id
                JOIN fraternities f ON f.id = c.fraternity_id
                WHERE cp.crawl_run_id = %s
                ORDER BY c.id, cp.extracted_at DESC, cp.created_at DESC
                """,
                (crawl_run_id,),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def apply_chapter_inactive_status(
        self,
        *,
        chapter_id: str,
        chapter_slug: str,
        fraternity_slug: str | None,
        source_slug: str | None,
        crawl_run_id: int | None,
        reason_code: str,
        evidence_url: str | None = None,
        evidence_source_type: str | None = None,
        source_snippet: str | None = None,
        provider: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        field_states = {
            "website_url": "inactive",
            "instagram_url": "inactive",
            "contact_email": "inactive",
        }
        contact_provenance = {
            "chapter_status": {
                "supportingPageUrl": evidence_url,
                "supportingPageScope": PAGE_SCOPE_SCHOOL_AFFILIATION,
                "contactProvenanceType": CONTACT_SPECIFICITY_SCHOOL,
                "decisionStage": "chapter_activity_validation",
                "sourceType": evidence_source_type,
                "reasonCode": reason_code,
                "confidence": 0.95,
                "decisionOutcome": DECISION_OUTCOME_ACCEPTED,
                "fieldResolutionState": FIELD_RESOLUTION_INACTIVE,
                "candidateValue": "inactive",
                "updatedAt": datetime.utcnow().isoformat(),
            },
            "website_url": {
                "supportingPageUrl": evidence_url,
                "supportingPageScope": PAGE_SCOPE_SCHOOL_AFFILIATION,
                "contactProvenanceType": CONTACT_SPECIFICITY_SCHOOL,
                "decisionStage": "chapter_activity_validation",
                "sourceType": evidence_source_type,
                "reasonCode": reason_code,
                "confidence": 0.95,
                "decisionOutcome": DECISION_OUTCOME_ACCEPTED,
                "fieldResolutionState": FIELD_RESOLUTION_INACTIVE,
                "candidateValue": None,
                "updatedAt": datetime.utcnow().isoformat(),
            },
            "instagram_url": {
                "supportingPageUrl": evidence_url,
                "supportingPageScope": PAGE_SCOPE_SCHOOL_AFFILIATION,
                "contactProvenanceType": CONTACT_SPECIFICITY_SCHOOL,
                "decisionStage": "chapter_activity_validation",
                "sourceType": evidence_source_type,
                "reasonCode": reason_code,
                "confidence": 0.95,
                "decisionOutcome": DECISION_OUTCOME_ACCEPTED,
                "fieldResolutionState": FIELD_RESOLUTION_INACTIVE,
                "candidateValue": None,
                "updatedAt": datetime.utcnow().isoformat(),
            },
            "contact_email": {
                "supportingPageUrl": evidence_url,
                "supportingPageScope": PAGE_SCOPE_SCHOOL_AFFILIATION,
                "contactProvenanceType": CONTACT_SPECIFICITY_SCHOOL,
                "decisionStage": "chapter_activity_validation",
                "sourceType": evidence_source_type,
                "reasonCode": reason_code,
                "confidence": 0.95,
                "decisionOutcome": DECISION_OUTCOME_ACCEPTED,
                "fieldResolutionState": FIELD_RESOLUTION_INACTIVE,
                "candidateValue": None,
                "updatedAt": datetime.utcnow().isoformat(),
            },
        }
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE chapters
                SET
                    chapter_status = 'inactive',
                    website_url = NULL,
                    instagram_url = NULL,
                    contact_email = NULL,
                    field_states = COALESCE(field_states, '{}'::jsonb) || %s,
                    contact_provenance = COALESCE(contact_provenance, '{}'::jsonb) || %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (Jsonb(field_states), Jsonb(contact_provenance), chapter_id),
            )
            if crawl_run_id is not None:
                cursor.execute("SELECT source_id FROM crawl_runs WHERE id = %s", (crawl_run_id,))
                source_row = cursor.fetchone()
                source_id = str(source_row["source_id"]) if source_row and source_row["source_id"] is not None else None
                if source_id is not None:
                    cursor.execute(
                        """
                        INSERT INTO chapter_provenance (
                            chapter_id,
                            source_id,
                            crawl_run_id,
                            field_name,
                            field_value,
                            source_url,
                            source_snippet,
                            confidence
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            chapter_id,
                            source_id,
                            crawl_run_id,
                            "chapter_status",
                            "inactive",
                            evidence_url or source_slug or "validation",
                            (source_snippet or reason_code)[:400],
                            0.95,
                        ),
                    )
            cursor.execute(
                """
                INSERT INTO chapter_evidence (
                    chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    crawl_run_id,
                    field_name,
                    candidate_value,
                    confidence,
                    trust_tier,
                    evidence_status,
                    source_url,
                    source_snippet,
                    provider,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    crawl_run_id,
                    "chapter_status",
                    "inactive",
                    0.95,
                    "strong_official" if evidence_source_type == "official_school" else "high",
                    "accepted",
                    evidence_url or source_slug,
                    (source_snippet or reason_code)[:400],
                    provider,
                    Jsonb(
                        {
                            "reasonCode": reason_code,
                            "evidenceSourceType": evidence_source_type,
                            "decisionStage": "chapter_activity_validation",
                            "pageScope": PAGE_SCOPE_SCHOOL_AFFILIATION,
                            "contactSpecificity": CONTACT_SPECIFICITY_SCHOOL,
                            "supportingPageUrl": evidence_url,
                            "supportingConfidence": 0.95,
                            **(metadata or {}),
                        }
                    ),
                ),
            )
        self._connection.commit()

    def complete_pending_field_jobs_for_chapter(
        self,
        *,
        chapter_id: str,
        reason_code: str,
        status: str,
        chapter_updates: dict[str, str] | None = None,
        field_states: dict[str, str] | None = None,
        field_names: list[str] | None = None,
    ) -> int:
        chapter_updates = chapter_updates or {}
        field_states = field_states or {}
        field_names = [name for name in (field_names or []) if name]
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE chapters
                SET
                    website_url = COALESCE(%(website_url)s, website_url),
                    instagram_url = COALESCE(%(instagram_url)s, instagram_url),
                    contact_email = COALESCE(%(contact_email)s, contact_email),
                    university_name = COALESCE(%(university_name)s, university_name),
                    chapter_status = COALESCE(%(chapter_status)s, chapter_status),
                    field_states = COALESCE(field_states, '{}'::jsonb) || %(field_states)s,
                    updated_at = NOW()
                WHERE id = %(chapter_id)s
                """,
                {
                    "website_url": chapter_updates.get("website_url"),
                    "instagram_url": chapter_updates.get("instagram_url"),
                    "contact_email": chapter_updates.get("contact_email"),
                    "university_name": chapter_updates.get("university_name"),
                    "chapter_status": chapter_updates.get("chapter_status"),
                    "field_states": Jsonb(field_states),
                    "chapter_id": chapter_id,
                },
            )
            cursor.execute(
                """
                UPDATE field_jobs
                SET
                    status = 'done',
                    terminal_outcome = %s,
                    finished_at = NOW(),
                    last_error = NULL,
                    completed_payload = %s,
                    claim_token = NULL,
                    terminal_failure = FALSE
                WHERE chapter_id = %s
                  AND status IN ('queued', 'running')
                  AND (
                    cardinality(%s::text[]) = 0
                    OR field_name = ANY(%s::text[])
                  )
                """,
                (
                    status,
                    Jsonb({"status": status, "reasonCode": reason_code}),
                    chapter_id,
                    field_names,
                    field_names,
                ),
            )
            affected = cursor.rowcount or 0
        self._connection.commit()
        return int(affected)

    def create_field_jobs(
        self,
        chapter_id: str,
        crawl_run_id: int,
        chapter_slug: str,
        source_slug: str,
        missing_fields: list[str],
    ) -> int:
        requested_jobs = {self._normalize_field_job_name(name) for name in missing_fields}
        requested_jobs = {name for name in requested_jobs if name in FIELD_JOB_TYPES}

        if not requested_jobs:
            return 0

        created = 0
        with self._connection.cursor() as cursor:
            for field_name in sorted(requested_jobs):
                cursor.execute(
                    """
                    SELECT chapter_status, field_states
                    FROM chapters
                    WHERE id = %s
                    """,
                    (chapter_id,),
                )
                chapter_row = cursor.fetchone()
                if chapter_row is not None:
                    chapter_status = str(chapter_row["chapter_status"] or "active").strip().lower()
                    field_states = dict(chapter_row["field_states"] or {})
                    state_key = FIELD_TO_CHAPTER_COLUMN.get(field_name)
                    if chapter_status == "inactive":
                        continue
                    if state_key and str(field_states.get(state_key) or "").strip().lower() in {
                        "inactive",
                        "confirmed_absent",
                        "invalid_entity",
                    }:
                        continue
                if field_name in {FIELD_JOB_FIND_WEBSITE, FIELD_JOB_FIND_INSTAGRAM, FIELD_JOB_FIND_EMAIL}:
                    if self._is_field_already_populated(cursor, chapter_id, field_name):
                        continue

                payload = {
                    "chapterSlug": chapter_slug,
                    "fieldName": field_name,
                    "sourceSlug": source_slug,
                    "payload": {"sourceSlug": source_slug, "chapterSlug": chapter_slug},
                }
                self._contracts.validate_field_job(payload)
                cursor.execute(
                    """
                    INSERT INTO field_jobs (chapter_id, crawl_run_id, field_name, payload)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chapter_id, field_name)
                    WHERE status IN ('queued', 'running')
                    DO NOTHING
                    RETURNING id
                    """,
                    (chapter_id, crawl_run_id, field_name, Jsonb(payload["payload"])),
                )
                if cursor.fetchone() is not None:
                    created += 1
        self._connection.commit()
        return created

    def claim_next_field_job(
        self,
        worker_id: str,
        source_slug: str | None = None,
        field_name: str | None = None,
        require_confident_website_for_email: bool = False,
        *,
        degraded_mode: bool = False,
    ) -> FieldJob | None:
        source_filter = ""
        field_name_filter = ""
        email_dependency_filter = ""
        degraded_claim_filter = ""
        field_priority_case = """
                        CASE fj.field_name
                            WHEN 'verify_school_match' THEN 0
                            WHEN 'verify_website' THEN 1
                            WHEN 'find_website' THEN 2
                            WHEN 'find_instagram' THEN 3
                            WHEN 'find_email' THEN 4
                            ELSE 5
                        END
        """
        params: dict[str, Any] = {"worker_id": worker_id}
        if source_slug is not None:
            source_filter = """
                      AND EXISTS (
                          SELECT 1
                          FROM crawl_runs cr
                          JOIN sources s ON s.id = cr.source_id
                          WHERE cr.id = fj.crawl_run_id
                            AND s.slug = %(source_slug)s
                      )
            """
            params["source_slug"] = source_slug
        if field_name is not None:
            field_name_filter = """
                      AND fj.field_name = %(field_name)s
            """
            params["field_name"] = field_name

        if require_confident_website_for_email:
            email_dependency_filter = """
                      AND (
                          fj.field_name <> 'find_email'
                          OR (
                              c.website_url ~* '^https?://'
                              AND COALESCE(c.field_states->>'website_url', '') NOT IN ('low_confidence', 'missing')
                          )
                      )
            """
        if degraded_mode:
            degraded_claim_filter = """
                      AND (
                          (
                              fj.field_name = 'verify_school_match'
                              AND (
                                  EXISTS (
                                      SELECT 1
                                      FROM school_greek_life_registry sgr
                                      WHERE sgr.school_slug = regexp_replace(lower(COALESCE(c.university_name, '')), '[^a-z0-9]+', '-', 'g')
                                        AND sgr.evidence_source_type = 'official_school'
                                        AND (
                                            sgr.greek_life_status IN ('allowed', 'banned')
                                            OR NULLIF(BTRIM(COALESCE(sgr.evidence_url, '')), '') IS NOT NULL
                                        )
                                  )
                                  OR EXISTS (
                                      SELECT 1
                                      FROM fraternity_school_activity_cache fsac
                                      JOIN fraternities f2 ON f2.id = c.fraternity_id
                                      WHERE fsac.fraternity_slug = f2.slug
                                        AND fsac.school_slug = regexp_replace(lower(COALESCE(c.university_name, '')), '[^a-z0-9]+', '-', 'g')
                                        AND fsac.evidence_source_type = 'official_school'
                                        AND (
                                            fsac.chapter_activity_status IN ('confirmed_active', 'confirmed_inactive')
                                            OR NULLIF(BTRIM(COALESCE(fsac.evidence_url, '')), '') IS NOT NULL
                                        )
                                  )
                              )
                          )
                          OR (
                              fj.field_name = 'verify_website'
                              AND (
                                  (
                                      c.website_url ~* '^https?://'
                                      AND COALESCE(c.field_states->>'website_url', '') NOT IN ('low_confidence', 'missing')
                                  )
                                  OR (
                                      NULLIF(BTRIM(COALESCE(fj.payload -> 'contactResolution' ->> 'supportingPageUrl', '')), '') IS NOT NULL
                                      AND LOWER(COALESCE(fj.payload -> 'contactResolution' ->> 'supportingPageScope', fj.payload -> 'contactResolution' ->> 'pageScope', '')) IN (
                                          'chapter_site',
                                          'school_affiliation_page',
                                          'nationals_chapter_page'
                                      )
                                  )
                              )
                          )
                          OR (
                              fj.field_name = 'find_instagram'
                              AND (
                                  (
                                      c.website_url ~* '^https?://'
                                      AND COALESCE(c.field_states->>'website_url', '') NOT IN ('low_confidence', 'missing')
                                  )
                                  OR (
                                      NULLIF(BTRIM(COALESCE(fj.payload -> 'contactResolution' ->> 'supportingPageUrl', '')), '') IS NOT NULL
                                      AND LOWER(COALESCE(fj.payload -> 'contactResolution' ->> 'supportingPageScope', fj.payload -> 'contactResolution' ->> 'pageScope', '')) IN (
                                          'chapter_site',
                                          'school_affiliation_page',
                                          'nationals_chapter_page'
                                      )
                                  )
                              )
                          )
                          OR (
                              fj.field_name = 'find_email'
                              AND (
                                  (
                                      c.website_url ~* '^https?://'
                                      AND COALESCE(c.field_states->>'website_url', '') NOT IN ('low_confidence', 'missing')
                                  )
                                  OR (
                                      NULLIF(BTRIM(COALESCE(fj.payload -> 'contactResolution' ->> 'supportingPageUrl', '')), '') IS NOT NULL
                                      AND LOWER(COALESCE(fj.payload -> 'contactResolution' ->> 'supportingPageScope', fj.payload -> 'contactResolution' ->> 'pageScope', '')) IN (
                                          'chapter_site',
                                          'school_affiliation_page',
                                          'nationals_chapter_page'
                                      )
                                  )
                              )
                          )
                      )
            """
            field_priority_case = """
                        CASE fj.field_name
                            WHEN 'verify_school_match' THEN 0
                            WHEN 'verify_website' THEN 1
                            WHEN 'find_instagram' THEN 2
                            WHEN 'find_email' THEN 3
                            ELSE 4
                        END
            """
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                WITH next_job AS (
                    SELECT
                        fj.id
                    FROM field_jobs fj
                    JOIN chapters c ON c.id = fj.chapter_id
                    WHERE fj.status = 'queued'
                      AND COALESCE(fj.queue_state, 'actionable') = 'actionable'
                      AND fj.scheduled_at <= NOW()
                      AND fj.attempts < fj.max_attempts
{source_filter}
{field_name_filter}
 {email_dependency_filter}
 {degraded_claim_filter}
                    ORDER BY
                        fj.priority DESC,
                        CASE
                            WHEN fj.queue_state = 'deferred' THEN 1
                            ELSE 0
                        END ASC,
                        fj.scheduled_at ASC,
                        {field_priority_case} ASC,
                        fj.id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                ),
                claimed_job AS (
                    UPDATE field_jobs fj
                    SET
                        status = 'running',
                        claimed_by = %(worker_id)s,
                        claim_token = gen_random_uuid(),
                        started_at = NOW(),
                        finished_at = NULL,
                        attempts = attempts + 1,
                        terminal_failure = FALSE
                    FROM next_job
                    WHERE fj.id = next_job.id
                    RETURNING
                        fj.id,
                        fj.chapter_id,
                        fj.crawl_run_id,
                        fj.field_name,
                        fj.payload,
                        fj.attempts,
                        fj.max_attempts,
                        fj.priority,
                        fj.queue_state,
                        fj.validity_class,
                        fj.repair_state,
                        fj.blocked_reason,
                        fj.terminal_outcome,
                        fj.claim_token
                )
                SELECT
                    cj.id,
                    cj.chapter_id,
                    cj.crawl_run_id,
                    c.slug AS chapter_slug,
                    c.name AS chapter_name,
                    f.slug AS fraternity_slug,
                    s.id AS source_id,
                    s.slug AS source_slug,
                    cj.field_name,
                    cj.payload,
                    cj.attempts,
                    cj.max_attempts,
                    cj.priority,
                    cj.queue_state,
                    cj.validity_class,
                    cj.repair_state,
                    cj.blocked_reason,
                    cj.terminal_outcome,
                    cj.claim_token,
                    c.website_url,
                    c.instagram_url,
                    c.contact_email,
                    c.university_name,
                    c.chapter_status,
                    c.field_states,
                    s.base_url AS source_base_url,
                    s.list_path AS source_list_path
                FROM claimed_job cj
                JOIN chapters c ON c.id = cj.chapter_id
                JOIN fraternities f ON f.id = c.fraternity_id
                LEFT JOIN crawl_runs cr ON cr.id = cj.crawl_run_id
                LEFT JOIN sources s ON s.id = cr.source_id
                """,
                params,
            )
            row = cursor.fetchone()
            if row is None:
                return None

            payload = dict(row["payload"] or {})
            source_base_url = row["source_base_url"]
            source_list_path = row["source_list_path"]
            if isinstance(source_list_path, str) and source_list_path.startswith("http"):
                payload.setdefault("sourceListUrl", source_list_path)
            elif isinstance(source_list_path, str) and source_list_path and source_base_url:
                payload.setdefault("sourceListUrl", f"{source_base_url.rstrip('/')}/{source_list_path.lstrip('/')}")
            elif source_base_url:
                payload.setdefault("sourceListUrl", source_base_url)

            return FieldJob(
                id=str(row["id"]),
                chapter_id=str(row["chapter_id"]),
                chapter_slug=row["chapter_slug"],
                chapter_name=row["chapter_name"],
                field_name=self._normalize_field_job_name(str(row["field_name"] or "")),
                payload=payload,
                attempts=int(row["attempts"]),
                max_attempts=int(row["max_attempts"]),
                priority=int(row["priority"]),
                claim_token=str(row["claim_token"]),
                source_base_url=source_base_url,
                website_url=row["website_url"],
                instagram_url=row["instagram_url"],
                contact_email=row["contact_email"],
                fraternity_slug=row["fraternity_slug"],
                source_id=str(row["source_id"]) if row["source_id"] is not None else None,
                source_slug=row["source_slug"],
                university_name=row["university_name"],
                crawl_run_id=int(row["crawl_run_id"]) if row["crawl_run_id"] is not None else None,
                chapter_status=row["chapter_status"] or "active",
                field_states=row["field_states"] or {},
                queue_state=row["queue_state"] or "actionable",
                validity_class=row["validity_class"],
                repair_state=row["repair_state"],
                blocked_reason=row["blocked_reason"],
                terminal_outcome=row["terminal_outcome"],
            )

    def get_field_job_worker_process_stats(self, workload_lane: str = "contact_resolution") -> dict[str, int]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  COUNT(*) FILTER (
                    WHERE workload_lane = %s
                      AND status = 'active'
                      AND (lease_expires_at IS NULL OR lease_expires_at > NOW())
                  )::int AS active_workers,
                  COUNT(*) FILTER (
                    WHERE workload_lane = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at <= NOW()
                  )::int AS stale_workers
                FROM worker_processes
                """,
                (workload_lane, workload_lane),
            )
            row = cursor.fetchone() or {}
        return {
            "active_workers": int(row.get("active_workers") or 0),
            "stale_workers": int(row.get("stale_workers") or 0),
        }

    def get_field_job_queue_counts(self) -> dict[str, int]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'queued')::int AS queued_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_provider')::int AS blocked_provider_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_dependency')::int AS blocked_dependency_jobs,
                  COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_repairable')::int AS blocked_repairable_jobs,
                  COUNT(*) FILTER (WHERE status = 'running')::int AS running_jobs
                FROM field_jobs
                WHERE field_name IN ('find_website', 'verify_website', 'find_instagram', 'find_email', 'verify_school_match')
                """
            )
            row = cursor.fetchone() or {}
        return {key: int(value or 0) for key, value in dict(row).items()}

    def get_reusable_official_school_evidence_url(self, *, fraternity_slug: str | None, school_name: str | None) -> str | None:
        school_slug = _normalize_school_slug(school_name)
        fraternity = str(fraternity_slug or "").strip()
        if school_slug and fraternity:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT evidence_url
                    FROM fraternity_school_activity_cache
                    WHERE fraternity_slug = %s
                      AND school_slug = %s
                      AND evidence_source_type = 'official_school'
                      AND NULLIF(BTRIM(COALESCE(evidence_url, '')), '') IS NOT NULL
                    ORDER BY
                      CASE
                        WHEN chapter_activity_status IN ('confirmed_active', 'confirmed_inactive') THEN 0
                        ELSE 1
                      END,
                      updated_at DESC
                    LIMIT 1
                    """,
                    (fraternity, school_slug),
                )
                row = cursor.fetchone()
                if row and row.get("evidence_url"):
                    return str(row["evidence_url"])
        if school_slug:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT evidence_url
                    FROM school_greek_life_registry
                    WHERE school_slug = %s
                      AND evidence_source_type = 'official_school'
                      AND NULLIF(BTRIM(COALESCE(evidence_url, '')), '') IS NOT NULL
                    ORDER BY
                      CASE
                        WHEN greek_life_status IN ('allowed', 'banned') THEN 0
                        ELSE 1
                      END,
                      updated_at DESC
                    LIMIT 1
                    """,
                    (school_slug,),
                )
                row = cursor.fetchone()
                if row and row.get("evidence_url"):
                    return str(row["evidence_url"])
        return None

    def backfill_field_job_typed_queue_state(self) -> dict[str, int]:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH updated AS (
                    UPDATE field_jobs fj
                    SET
                        blocked_reason = COALESCE(
                            NULLIF(BTRIM(fj.blocked_reason), ''),
                            NULLIF(BTRIM(fj.payload #>> '{contactResolution,blockedReason}'), ''),
                            NULLIF(BTRIM(fj.payload #>> '{contactResolution,reasonCode}'), ''),
                            NULLIF(BTRIM(fj.payload #>> '{queueTriage,reason}'), ''),
                            NULLIF(BTRIM(fj.payload #>> '{queueTriage,repairReason}'), '')
                        ),
                        queue_state = CASE
                            WHEN COALESCE(fj.queue_state, 'actionable') IN ('actionable', 'deferred')
                                 AND COALESCE(
                                     NULLIF(BTRIM(fj.blocked_reason), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{contactResolution,blockedReason}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{contactResolution,reasonCode}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{queueTriage,reason}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{queueTriage,repairReason}'), '')
                                 ) IN ('queued_for_entity_repair', 'identity_semantically_incomplete', 'repair_exhausted')
                                THEN 'blocked_repairable'
                            WHEN COALESCE(fj.queue_state, 'actionable') IN ('actionable', 'deferred')
                                 AND COALESCE(
                                     NULLIF(BTRIM(fj.blocked_reason), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{contactResolution,blockedReason}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{contactResolution,reasonCode}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{queueTriage,reason}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{queueTriage,repairReason}'), '')
                                 ) IN ('provider_degraded', 'transient_network', 'provider_low_signal')
                                THEN 'blocked_provider'
                            WHEN COALESCE(fj.queue_state, 'actionable') IN ('actionable', 'deferred')
                                 AND COALESCE(
                                     NULLIF(BTRIM(fj.blocked_reason), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{contactResolution,blockedReason}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{contactResolution,reasonCode}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{queueTriage,reason}'), ''),
                                     NULLIF(BTRIM(fj.payload #>> '{queueTriage,repairReason}'), '')
                                 ) IN ('dependency_wait', 'website_required')
                                THEN 'blocked_dependency'
                            ELSE COALESCE(fj.queue_state, 'actionable')
                        END
                    WHERE fj.status = 'queued'
                    RETURNING
                        CASE
                            WHEN COALESCE(NULLIF(BTRIM(blocked_reason), ''), NULLIF(BTRIM(fj.payload #>> '{contactResolution,blockedReason}'), ''), NULLIF(BTRIM(fj.payload #>> '{contactResolution,reasonCode}'), ''), NULLIF(BTRIM(fj.payload #>> '{queueTriage,reason}'), ''), NULLIF(BTRIM(fj.payload #>> '{queueTriage,repairReason}'), '')) IS NOT NULL
                            THEN 1 ELSE 0
                        END AS reason_present,
                        CASE
                            WHEN queue_state = 'blocked_repairable' THEN 1 ELSE 0
                        END AS moved_blocked_repairable
                )
                SELECT
                    COUNT(*) FILTER (WHERE reason_present = 1)::int AS blocked_reason_populated,
                    COUNT(*) FILTER (WHERE moved_blocked_repairable = 1)::int AS blocked_repairable_rows
                FROM updated
                """
            )
            row = cursor.fetchone() or {}
        return {
            "blocked_reason_populated": int(row.get("blocked_reason_populated") or 0),
            "blocked_repairable_rows": int(row.get("blocked_repairable_rows") or 0),
        }

    def list_queued_field_jobs_for_triage(
        self,
        *,
        limit: int = 200,
        source_slug: str | None = None,
        field_name: str | None = None,
    ) -> list[FieldJob]:
        source_filter = ""
        field_name_filter = ""
        params: dict[str, Any] = {"limit": max(1, limit)}
        if source_slug is not None:
            source_filter = "AND s.slug = %(source_slug)s"
            params["source_slug"] = source_slug
        if field_name is not None:
            field_name_filter = "AND fj.field_name = %(field_name)s"
            params["field_name"] = field_name

        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    fj.id,
                    fj.chapter_id,
                    fj.crawl_run_id,
                    c.slug AS chapter_slug,
                    c.name AS chapter_name,
                    f.slug AS fraternity_slug,
                    s.id AS source_id,
                    s.slug AS source_slug,
                    fj.field_name,
                    fj.payload,
                    fj.attempts,
                    fj.max_attempts,
                    fj.priority,
                    fj.queue_state,
                    fj.validity_class,
                    fj.repair_state,
                    fj.blocked_reason,
                    fj.terminal_outcome,
                    c.website_url,
                    c.instagram_url,
                    c.contact_email,
                    c.university_name,
                    c.chapter_status,
                    c.field_states,
                    s.base_url AS source_base_url,
                    s.list_path AS source_list_path
                FROM field_jobs fj
                JOIN chapters c ON c.id = fj.chapter_id
                JOIN fraternities f ON f.id = c.fraternity_id
                LEFT JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
                LEFT JOIN sources s ON s.id = cr.source_id
                WHERE fj.status = 'queued'
                  AND fj.scheduled_at <= NOW()
                  AND fj.attempts < fj.max_attempts
                  {field_name_filter}
                  {source_filter}
                ORDER BY
                    fj.priority DESC,
                    CASE
                        WHEN fj.queue_state = 'blocked_provider' THEN 0
                        WHEN fj.queue_state = 'blocked_dependency' THEN 1
                        WHEN fj.queue_state = 'blocked_repairable' THEN 2
                        WHEN fj.queue_state = 'deferred' THEN 3
                        ELSE 4
                    END ASC,
                    fj.scheduled_at ASC,
                    fj.id ASC
                LIMIT %(limit)s
                """,
                params,
            )
            rows = cursor.fetchall()

        jobs: list[FieldJob] = []
        for row in rows:
            payload = dict(row["payload"] or {})
            source_base_url = row["source_base_url"]
            source_list_path = row["source_list_path"]
            if isinstance(source_list_path, str) and source_list_path.startswith("http"):
                payload.setdefault("sourceListUrl", source_list_path)
            elif isinstance(source_list_path, str) and source_list_path and source_base_url:
                payload.setdefault("sourceListUrl", f"{source_base_url.rstrip('/')}/{source_list_path.lstrip('/')}")
            elif source_base_url:
                payload.setdefault("sourceListUrl", source_base_url)

            jobs.append(
                FieldJob(
                    id=str(row["id"]),
                    chapter_id=str(row["chapter_id"]),
                    chapter_slug=row["chapter_slug"],
                    chapter_name=row["chapter_name"],
                    field_name=row["field_name"],
                    payload=payload,
                    attempts=int(row["attempts"]),
                    max_attempts=int(row["max_attempts"]),
                    priority=int(row["priority"]),
                    claim_token="",
                    source_base_url=source_base_url,
                    website_url=row["website_url"],
                    instagram_url=row["instagram_url"],
                    contact_email=row["contact_email"],
                    fraternity_slug=row["fraternity_slug"],
                    source_id=str(row["source_id"]) if row["source_id"] is not None else None,
                    source_slug=row["source_slug"],
                    university_name=row["university_name"],
                    crawl_run_id=int(row["crawl_run_id"]) if row["crawl_run_id"] is not None else None,
                    chapter_status=row["chapter_status"] or "active",
                    field_states=row["field_states"] or {},
                    queue_state=row["queue_state"] or "actionable",
                    validity_class=row["validity_class"],
                    repair_state=row["repair_state"],
                    blocked_reason=row["blocked_reason"],
                    terminal_outcome=row["terminal_outcome"],
                )
            )
        return jobs

    def patch_queued_field_job(
        self,
        field_job_id: str,
        *,
        payload_patch: dict[str, Any] | None = None,
        scheduled_delay_seconds: int | None = None,
        status: str | None = None,
        last_error: str | None = None,
        terminal_failure: bool | None = None,
        completed_payload: dict[str, Any] | None = None,
    ) -> bool:
        assignments = ["payload = COALESCE(payload, '{}'::jsonb) || %(payload_patch)s"]
        typed_state = _extract_field_job_typed_state(payload_patch, completed_payload=completed_payload)
        params: dict[str, Any] = {
            "field_job_id": field_job_id,
            "payload_patch": Jsonb(payload_patch or {}),
            "queue_state": typed_state["queue_state"],
            "validity_class": typed_state["validity_class"],
            "repair_state": typed_state["repair_state"],
            "blocked_reason": typed_state["blocked_reason"],
            "terminal_outcome": typed_state["terminal_outcome"],
        }
        assignments.append("queue_state = COALESCE(%(queue_state)s, queue_state, 'actionable')")
        assignments.append("validity_class = COALESCE(%(validity_class)s, validity_class)")
        assignments.append("repair_state = COALESCE(%(repair_state)s, repair_state)")
        assignments.append("blocked_reason = COALESCE(%(blocked_reason)s, blocked_reason)")
        if scheduled_delay_seconds is not None:
            assignments.append("scheduled_at = NOW() + (%(scheduled_delay_seconds)s * INTERVAL '1 second')")
            params["scheduled_delay_seconds"] = max(0, int(scheduled_delay_seconds))
        if status is not None:
            assignments.append("status = %(status)s")
            params["status"] = status
            if status == "failed":
                assignments.append("finished_at = NOW()")
                assignments.append("claim_token = NULL")
            elif status == "queued":
                assignments.append("started_at = NULL")
                assignments.append("finished_at = NULL")
                assignments.append("claim_token = NULL")
                assignments.append("terminal_outcome = NULL")
        if last_error is not None:
            assignments.append("last_error = %(last_error)s")
            params["last_error"] = last_error
        if terminal_failure is not None:
            assignments.append("terminal_failure = %(terminal_failure)s")
            params["terminal_failure"] = terminal_failure
        if completed_payload is not None:
            assignments.append("completed_payload = %(completed_payload)s")
            params["completed_payload"] = Jsonb(completed_payload)
            assignments.append("terminal_outcome = COALESCE(%(terminal_outcome)s, terminal_outcome)")

        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE field_jobs
                SET {", ".join(assignments)}
                WHERE id = %(field_job_id)s
                  AND status = 'queued'
                """,
                params,
            )
            updated = cursor.rowcount
        self._connection.commit()
        return updated > 0

    def update_chapter_identity_repair(
        self,
        *,
        chapter_id: str,
        university_name: str | None = None,
        field_state_updates: dict[str, str] | None = None,
        validity_class: str | None = None,
        repair_metadata: dict[str, Any] | None = None,
    ) -> bool:
        field_state_updates = field_state_updates or {}
        repair_metadata = dict(repair_metadata or {})
        if validity_class is not None:
            repair_metadata["validityClass"] = validity_class
        if not university_name and not field_state_updates and not repair_metadata:
            return False
        repair_metadata["repairedAt"] = datetime.now().isoformat()
        if repair_metadata:
            field_state_updates.setdefault("entity_repair", "completed")
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE chapters
                SET
                    university_name = COALESCE(%(university_name)s, university_name),
                    field_states = COALESCE(field_states, '{}'::jsonb) || %(field_states)s,
                    updated_at = NOW()
                WHERE id = %(chapter_id)s
                """,
                {
                    "chapter_id": chapter_id,
                    "university_name": university_name,
                    "field_states": Jsonb(field_state_updates),
                },
            )
            updated = cursor.rowcount
        self._connection.commit()
        return updated > 0

    def enqueue_chapter_repair_job(
        self,
        *,
        chapter_id: str,
        source_slug: str | None,
        payload: dict[str, Any],
        priority: int = 0,
        max_attempts: int = 3,
    ) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapter_repair_jobs (
                    chapter_id,
                    source_slug,
                    status,
                    repair_state,
                    payload,
                    priority,
                    max_attempts
                )
                VALUES (%s, %s, 'queued', 'queued', %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (
                    chapter_id,
                    source_slug,
                    Jsonb(payload or {}),
                    max(0, int(priority)),
                    max(1, int(max_attempts)),
                ),
            )
            created = cursor.fetchone() is not None
        self._connection.commit()
        return created

    def claim_next_chapter_repair_job(self, worker_id: str, source_slug: str | None = None) -> ChapterRepairJob | None:
        source_filter = ""
        params: dict[str, Any] = {"worker_id": worker_id}
        if source_slug is not None:
            source_filter = "AND crj.source_slug = %(source_slug)s"
            params["source_slug"] = source_slug

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                WITH next_job AS (
                    SELECT crj.id
                    FROM chapter_repair_jobs crj
                    WHERE crj.status = 'queued'
                      AND crj.scheduled_at <= NOW()
                      AND crj.attempts < crj.max_attempts
                      {source_filter}
                    ORDER BY crj.priority DESC, crj.scheduled_at ASC, crj.id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                ),
                claimed_job AS (
                    UPDATE chapter_repair_jobs crj
                    SET
                        status = 'running',
                        repair_state = 'running',
                        claimed_by = %(worker_id)s,
                        claim_token = gen_random_uuid(),
                        started_at = NOW(),
                        finished_at = NULL,
                        attempts = attempts + 1
                    FROM next_job
                    WHERE crj.id = next_job.id
                    RETURNING crj.id, crj.chapter_id, crj.source_slug, crj.payload, crj.attempts, crj.max_attempts, crj.priority, crj.claim_token, crj.repair_state
                )
                SELECT
                    cj.id,
                    cj.chapter_id,
                    c.slug AS chapter_slug,
                    c.name AS chapter_name,
                    cj.source_slug,
                    cj.payload,
                    cj.attempts,
                    cj.max_attempts,
                    cj.priority,
                    cj.claim_token,
                    cj.repair_state,
                    c.university_name,
                    c.website_url,
                    c.instagram_url,
                    c.contact_email
                FROM claimed_job cj
                JOIN chapters c ON c.id = cj.chapter_id
                """,
                params,
            )
            row = cursor.fetchone()
            if row is None:
                return None

        return ChapterRepairJob(
            id=str(row["id"]),
            chapter_id=str(row["chapter_id"]),
            chapter_slug=row["chapter_slug"],
            chapter_name=row["chapter_name"],
            source_slug=row["source_slug"],
            payload=dict(row["payload"] or {}),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            priority=int(row["priority"]),
            claim_token=str(row["claim_token"]),
            repair_state=row["repair_state"],
            university_name=row["university_name"],
            website_url=row["website_url"],
            instagram_url=row["instagram_url"],
            contact_email=row["contact_email"],
        )

    def complete_chapter_repair_job(
        self,
        job: ChapterRepairJob,
        *,
        repair_state: str,
        result_payload: dict[str, Any] | None = None,
        error: str | None = None,
        final_status: str = "done",
    ) -> None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE chapter_repair_jobs
                SET
                    status = %s,
                    repair_state = %s,
                    finished_at = NOW(),
                    last_error = %s,
                    result_payload = %s,
                    claim_token = NULL
                WHERE id = %s
                  AND status = 'running'
                  AND claim_token = %s
                """,
                (
                    final_status,
                    repair_state,
                    error,
                    Jsonb(result_payload or {}),
                    job.id,
                    job.claim_token,
                ),
            )

    def list_queued_field_jobs_for_chapter(self, chapter_id: str) -> list[FieldJob]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fj.id,
                    fj.chapter_id,
                    fj.crawl_run_id,
                    c.slug AS chapter_slug,
                    c.name AS chapter_name,
                    f.slug AS fraternity_slug,
                    s.id AS source_id,
                    s.slug AS source_slug,
                    fj.field_name,
                    fj.payload,
                    fj.attempts,
                    fj.max_attempts,
                    fj.priority,
                    fj.queue_state,
                    fj.validity_class,
                    fj.repair_state,
                    fj.blocked_reason,
                    fj.terminal_outcome,
                    c.website_url,
                    c.instagram_url,
                    c.contact_email,
                    c.university_name,
                    c.chapter_status,
                    c.field_states,
                    s.base_url AS source_base_url,
                    s.list_path AS source_list_path
                FROM field_jobs fj
                JOIN chapters c ON c.id = fj.chapter_id
                JOIN fraternities f ON f.id = c.fraternity_id
                LEFT JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
                LEFT JOIN sources s ON s.id = cr.source_id
                WHERE fj.chapter_id = %s
                  AND fj.status = 'queued'
                ORDER BY fj.priority DESC, fj.scheduled_at ASC, fj.id ASC
                """,
                (chapter_id,),
            )
            rows = cursor.fetchall()

        jobs: list[FieldJob] = []
        for row in rows:
            payload = dict(row["payload"] or {})
            source_base_url = row["source_base_url"]
            source_list_path = row["source_list_path"]
            if isinstance(source_list_path, str) and source_list_path.startswith("http"):
                payload.setdefault("sourceListUrl", source_list_path)
            elif isinstance(source_list_path, str) and source_list_path and source_base_url:
                payload.setdefault("sourceListUrl", f"{source_base_url.rstrip('/')}/{source_list_path.lstrip('/')}")
            elif source_base_url:
                payload.setdefault("sourceListUrl", source_base_url)

            jobs.append(
                FieldJob(
                    id=str(row["id"]),
                    chapter_id=str(row["chapter_id"]),
                    chapter_slug=row["chapter_slug"],
                    chapter_name=row["chapter_name"],
                    field_name=row["field_name"],
                    payload=payload,
                    attempts=int(row["attempts"]),
                    max_attempts=int(row["max_attempts"]),
                    priority=int(row["priority"]),
                    claim_token="",
                    source_base_url=source_base_url,
                    website_url=row["website_url"],
                    instagram_url=row["instagram_url"],
                    contact_email=row["contact_email"],
                    fraternity_slug=row["fraternity_slug"],
                    source_id=str(row["source_id"]) if row["source_id"] is not None else None,
                    source_slug=row["source_slug"],
                    university_name=row["university_name"],
                    crawl_run_id=int(row["crawl_run_id"]) if row["crawl_run_id"] is not None else None,
                    chapter_status=row["chapter_status"] or "active",
                    field_states=row["field_states"] or {},
                    queue_state=row["queue_state"] or "actionable",
                    validity_class=row["validity_class"],
                    repair_state=row["repair_state"],
                    blocked_reason=row["blocked_reason"],
                    terminal_outcome=row["terminal_outcome"],
                )
            )
        return jobs

    def reconcile_stale_field_jobs(self, max_age_minutes: int = 60) -> int:
        stale_minutes = max(1, int(max_age_minutes))
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH stale_jobs AS (
                    SELECT id
                    FROM field_jobs
                    WHERE status = 'running'
                      AND started_at IS NOT NULL
                      AND started_at < NOW() - (%s * INTERVAL '1 minute')
                )
                UPDATE field_jobs fj
                SET
                    status = 'queued',
                    scheduled_at = NOW(),
                    started_at = NULL,
                    finished_at = NULL,
                    claimed_by = NULL,
                    claim_token = NULL,
                    terminal_failure = FALSE,
                    attempts = GREATEST(fj.attempts - 1, 0),
                    last_error = %s,
                    payload = COALESCE(fj.payload, '{}'::jsonb)
                        || jsonb_build_object(
                            'staleRecovery',
                            jsonb_build_object(
                                'recoveredAt', NOW(),
                                'reason', 'stale_claim_timeout',
                                'staleMinutes', %s
                            )
                        )
                FROM stale_jobs
                WHERE fj.id = stale_jobs.id
                """,
                (
                    stale_minutes,
                    f"Recovered stale field job claim after {stale_minutes} minutes without completion",
                    stale_minutes,
                ),
            )
            updated = cursor.rowcount
        self._connection.commit()
        return max(updated, 0)

    def reconcile_stale_field_job_graph_runs(self, max_age_minutes: int = 60) -> int:
        stale_minutes = max(1, int(max_age_minutes))
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE field_job_graph_runs
                SET
                    status = 'failed',
                    error_message = COALESCE(
                        error_message,
                        %s
                    ),
                    summary = COALESCE(summary, '{}'::jsonb)
                        || jsonb_build_object(
                            'staleRunRecovery', TRUE,
                            'staleMinutes', %s
                        ),
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE status = 'running'
                  AND COALESCE(updated_at, created_at) < NOW() - (%s * INTERVAL '1 minute')
                """,
                (
                    f"Recovered stale field-job graph run after {stale_minutes} minutes without completion",
                    stale_minutes,
                    stale_minutes,
                ),
            )
            updated = cursor.rowcount
        self._connection.commit()
        return max(updated, 0)

    def fetch_provenance_snippets(self, chapter_id: str) -> list[str]:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_snippet
                FROM chapter_provenance
                WHERE chapter_id = %s
                  AND source_snippet IS NOT NULL
                ORDER BY extracted_at DESC
                LIMIT 20
                """,
                (chapter_id,),
            )
            rows = cursor.fetchall()
        return [row["source_snippet"] for row in rows if row["source_snippet"]]

    def fetch_latest_provenance_context(self, chapter_id: str) -> dict[str, Any] | None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    source_url,
                    source_snippet,
                    field_name,
                    confidence
                FROM chapter_provenance
                WHERE chapter_id = %s
                ORDER BY extracted_at DESC, created_at DESC
                LIMIT 1
                """,
                (chapter_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "source_url": row["source_url"],
            "source_snippet": row["source_snippet"],
            "field_name": row["field_name"],
            "confidence": float(row["confidence"] or 0.0),
        }

    def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM field_jobs
                WHERE chapter_id = %s
                  AND field_name = %s
                  AND status IN ('queued', 'running')
                  AND attempts < max_attempts
                LIMIT 1
                """,
                (chapter_id, field_name),
            )
            return cursor.fetchone() is not None

    def has_recent_transient_website_failures(self, chapter_id: str, min_failures: int = 2) -> bool:
        threshold = max(1, min_failures)
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM field_jobs
                WHERE chapter_id = %s
                  AND field_name = 'find_website'
                  AND (
                      COALESCE(last_error, '') ILIKE '%%provider or network unavailable%%'
                      OR (
                          CASE
                              WHEN COALESCE(payload->>'transient_provider_failures', '') ~ '^[0-9]+$'
                              THEN (payload->>'transient_provider_failures')::int
                              ELSE 0
                          END
                      ) >= %s
                  )
                  AND attempts >= %s
                LIMIT 1
                """,
                (chapter_id, threshold, threshold),
            )
            return cursor.fetchone() is not None

    def create_field_job_review_item(self, job: FieldJob, candidate: ReviewItemCandidate) -> None:
        source_id: str | None = job.source_id
        if source_id is None and job.crawl_run_id is not None:
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT source_id FROM crawl_runs WHERE id = %s", (job.crawl_run_id,))
                row = cursor.fetchone()
                if row is not None:
                    source_id = str(row["source_id"]) if row["source_id"] is not None else None
        self.create_review_item(source_id, job.crawl_run_id, candidate, chapter_id=job.chapter_id)

    def complete_field_job(
        self,
        job: FieldJob,
        chapter_updates: dict[str, str],
        completed_payload: dict[str, Any],
        field_state_updates: dict[str, str] | None = None,
        provenance_records: list[ProvenanceRecord] | None = None,
    ) -> None:
        field_state_updates = field_state_updates or {}
        provenance_records = provenance_records or []
        decision_evidence = _build_decision_evidence(completed_payload)
        contact_field_writes = any(
            field_name in chapter_updates for field_name in ("website_url", "instagram_url", "contact_email")
        )
        status_decision_id = str(
            decision_evidence.metadata.get("statusDecisionId")
            or completed_payload.get("statusDecisionId")
            or ""
        ).strip() or None
        allow_instagram_replace = bool(
            decision_evidence.metadata.get("allowReplaceExisting")
            or completed_payload.get("allowReplaceExisting")
        )
        operator_override_reason = str(
            decision_evidence.metadata.get("operatorOverrideReason")
            or completed_payload.get("operatorOverrideReason")
            or ""
        ).strip() or None
        contact_provenance_patch = _build_contact_provenance_patch(
            chapter_updates=chapter_updates,
            field_state_updates=field_state_updates,
            completed_payload=completed_payload,
            provenance_records=provenance_records,
        )
        with self._connection.transaction(), self._connection.cursor() as cursor:
            self._verify_claim(cursor, job.id, job.claim_token)
            if contact_field_writes and not operator_override_reason:
                cursor.execute(
                    """
                    SELECT id, final_status
                    FROM chapter_status_decisions
                    WHERE chapter_id = %s
                    ORDER BY decided_at DESC, id DESC
                    LIMIT 1
                    """,
                    (job.chapter_id,),
                )
                status_row = cursor.fetchone()
                if status_row is None:
                    raise ValueError("contact writes require an existing chapter_status_decision")
                latest_status_decision_id = str(status_row["id"])
                latest_final_status = str(status_row["final_status"] or "").strip().lower()
                if latest_final_status != "active":
                    raise ValueError("inactive/unknown/review chapters cannot receive new contact writes")
                if status_decision_id is not None and latest_status_decision_id != status_decision_id:
                    raise ValueError("contact write statusDecisionId does not match the latest chapter_status_decision")
            if chapter_updates or field_state_updates:
                cursor.execute(
                    """
                    UPDATE chapters
                    SET
                        website_url = CASE
                            WHEN %(website_url)s::text IS NULL THEN website_url
                            WHEN website_url IS NULL THEN %(website_url)s::text
                            WHEN website_url !~* '^https?://' THEN %(website_url)s::text
                            ELSE website_url
                        END,
                        instagram_url = CASE
                            WHEN %(instagram_url)s::text IS NULL THEN instagram_url
                            WHEN instagram_url IS NULL THEN %(instagram_url)s::text
                            WHEN %(allow_instagram_replace)s THEN %(instagram_url)s::text
                            ELSE instagram_url
                        END,
                        contact_email = COALESCE(contact_email, %(contact_email)s),
                        university_name = COALESCE(university_name, %(university_name)s),
                        chapter_status = COALESCE(%(chapter_status)s, chapter_status),
                        field_states = COALESCE(field_states, '{}'::jsonb) || %(field_states)s,
                        contact_provenance = COALESCE(contact_provenance, '{}'::jsonb) || %(contact_provenance)s,
                        updated_at = NOW()
                    WHERE id = %(chapter_id)s
                    """,
                    {
                        "chapter_id": job.chapter_id,
                        "website_url": chapter_updates.get("website_url"),
                        "instagram_url": chapter_updates.get("instagram_url"),
                        "allow_instagram_replace": allow_instagram_replace,
                        "contact_email": chapter_updates.get("contact_email"),
                        "university_name": chapter_updates.get("university_name"),
                        "chapter_status": chapter_updates.get("chapter_status"),
                        "field_states": Jsonb(field_state_updates),
                        "contact_provenance": Jsonb(contact_provenance_patch),
                    },
                )

            completed_status = str(completed_payload.get("status") or "observed")
            provider = completed_payload.get("provider") or completed_payload.get("source_provider")
            query = completed_payload.get("query")
            related_website_url = completed_payload.get("related_website_url")

            for record in provenance_records:
                payload = asdict(record)
                self._contracts.validate_provenance(
                    {
                        "sourceSlug": payload["source_slug"],
                        "sourceUrl": payload["source_url"],
                        "fieldName": payload["field_name"],
                        "fieldValue": payload["field_value"],
                        "sourceSnippet": payload["source_snippet"],
                        "confidence": payload["confidence"],
                    }
                )
                if job.source_id is None or job.crawl_run_id is None:
                    continue
                cursor.execute(
                    """
                    INSERT INTO chapter_provenance (
                        chapter_id,
                        source_id,
                        crawl_run_id,
                        field_name,
                        field_value,
                        source_url,
                        source_snippet,
                        confidence
                    )
                    VALUES (%(chapter_id)s, %(source_id)s, %(crawl_run_id)s, %(field_name)s, %(field_value)s, %(source_url)s, %(source_snippet)s, %(confidence)s)
                    """,
                    {
                        "chapter_id": job.chapter_id,
                        "source_id": job.source_id,
                        "crawl_run_id": job.crawl_run_id,
                        "field_name": record.field_name,
                        "field_value": record.field_value,
                        "source_url": record.source_url,
                        "source_snippet": record.source_snippet,
                        "confidence": record.confidence,
                    },
                )
                evidence_status = "accepted" if completed_status == "updated" else "review" if completed_status == "review_required" else "observed"
                trust_tier = "strong_official" if record.confidence >= 0.95 else "high" if record.confidence >= 0.85 else "medium" if record.confidence >= 0.7 else "low"
                cursor.execute(
                    """
                    INSERT INTO chapter_evidence (
                        chapter_id,
                        chapter_slug,
                        fraternity_slug,
                        source_slug,
                        crawl_run_id,
                        field_name,
                        candidate_value,
                        confidence,
                        trust_tier,
                        evidence_status,
                        source_url,
                        source_snippet,
                        provider,
                        query,
                        related_website_url,
                        metadata
                    )
                    VALUES (%(chapter_id)s, %(chapter_slug)s, %(fraternity_slug)s, %(source_slug)s, %(crawl_run_id)s, %(field_name)s, %(candidate_value)s, %(confidence)s, %(trust_tier)s, %(evidence_status)s, %(source_url)s, %(source_snippet)s, %(provider)s, %(query)s, %(related_website_url)s, %(metadata)s)
                    """,
                    {
                        "chapter_id": job.chapter_id,
                        "chapter_slug": job.chapter_slug,
                        "fraternity_slug": job.fraternity_slug,
                        "source_slug": job.source_slug,
                        "crawl_run_id": job.crawl_run_id,
                        "field_name": record.field_name,
                        "candidate_value": record.field_value,
                        "confidence": record.confidence,
                        "trust_tier": trust_tier,
                        "evidence_status": evidence_status,
                        "source_url": record.source_url,
                        "source_snippet": record.source_snippet,
                        "provider": provider,
                        "query": query,
                        "related_website_url": related_website_url if record.field_name != "website_url" else None,
                        "metadata": Jsonb(
                            {
                                "fieldJobId": job.id,
                                "completedStatus": completed_status,
                                "fieldState": field_state_updates.get(record.field_name),
                                "decisionStage": decision_evidence.decision_stage,
                                "pageScope": decision_evidence.page_scope,
                                "contactSpecificity": decision_evidence.contact_specificity,
                                "evidenceSourceType": decision_evidence.source_type,
                                "reasonCode": decision_evidence.reason_code,
                                "supportingPageUrl": decision_evidence.evidence_url,
                                "supportingConfidence": decision_evidence.confidence,
                                **decision_evidence.metadata,
                            }
                        ),
                    },
                )

            cursor.execute(
                """
                UPDATE field_jobs
                SET
                    status = 'done',
                    terminal_outcome = %s,
                    finished_at = NOW(),
                    last_error = NULL,
                    completed_payload = %s,
                    claim_token = NULL
                WHERE id = %s
                """,
                (completed_status, Jsonb(completed_payload), job.id),
            )

    def requeue_field_job(
        self,
        job: FieldJob,
        error: str,
        delay_seconds: int,
        preserve_attempt: bool = False,
        payload_patch: dict[str, Any] | None = None,
    ) -> None:
        payload_patch = payload_patch or {}
        typed_state = _extract_field_job_typed_state(payload_patch)
        with self._connection.transaction(), self._connection.cursor() as cursor:
            self._verify_claim(cursor, job.id, job.claim_token)
            cursor.execute(
                """
                UPDATE field_jobs
                SET
                    status = 'queued',
                    queue_state = COALESCE(%s, queue_state),
                    validity_class = COALESCE(%s, validity_class),
                    repair_state = COALESCE(%s, repair_state),
                    blocked_reason = COALESCE(%s, blocked_reason),
                    terminal_outcome = NULL,
                    scheduled_at = NOW() + (%s * INTERVAL '1 second'),
                    started_at = NULL,
                    finished_at = NULL,
                    last_error = %s,
                    payload = COALESCE(payload, '{}'::jsonb) || %s,
                    claim_token = NULL,
                    terminal_failure = FALSE,
                    attempts = CASE WHEN %s THEN GREATEST(attempts - 1, 0) ELSE attempts END
                WHERE id = %s
                """,
                (
                    typed_state.get("queue_state"),
                    typed_state.get("validity_class"),
                    typed_state.get("repair_state"),
                    typed_state.get("blocked_reason"),
                    delay_seconds,
                    error,
                    Jsonb(payload_patch),
                    preserve_attempt,
                    job.id,
                ),
            )

    def fail_field_job_terminal(self, job: FieldJob, error: str) -> None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            self._verify_claim(cursor, job.id, job.claim_token)
            cursor.execute(
                """
                UPDATE field_jobs
                SET
                    status = 'failed',
                    terminal_outcome = 'failed',
                    finished_at = NOW(),
                    last_error = %s,
                    claim_token = NULL,
                    terminal_failure = TRUE
                WHERE id = %s
                """,
                (error, job.id),
            )

    def _verify_claim(self, cursor: psycopg.Cursor, field_job_id: str, claim_token: str) -> None:
        cursor.execute(
            """
            SELECT id
            FROM field_jobs
            WHERE id = %s
              AND status = 'running'
              AND claim_token = %s
            FOR UPDATE
            """,
            (field_job_id, claim_token),
        )
        if cursor.fetchone() is None:
            raise RuntimeError(f"Field job {field_job_id} is no longer claimable for this worker")

    def _is_field_already_populated(self, cursor: psycopg.Cursor, chapter_id: str, field_name: str) -> bool:
        chapter_column = FIELD_TO_CHAPTER_COLUMN.get(field_name)
        if chapter_column is None:
            return False

        cursor.execute(
            f"""
            SELECT {chapter_column}
            FROM chapters
            WHERE id = %s
            """,
            (chapter_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return False

        value = row[chapter_column]
        if value is None:
            return False

        if field_name == FIELD_JOB_FIND_WEBSITE:
            return sanitize_as_website(value) is not None
        if field_name == FIELD_JOB_FIND_INSTAGRAM:
            return sanitize_as_instagram(value) is not None
        if field_name == FIELD_JOB_FIND_EMAIL:
            return sanitize_as_email(value) is not None
        return True

    def _normalize_field_job_name(self, raw_name: str) -> str:
        mapping = {
            "websiteurl": FIELD_JOB_FIND_WEBSITE,
            "website_url": FIELD_JOB_FIND_WEBSITE,
            "find_website": FIELD_JOB_FIND_WEBSITE,
            "verify_website": FIELD_JOB_VERIFY_WEBSITE,
            "instagramurl": FIELD_JOB_FIND_INSTAGRAM,
            "instagram_url": FIELD_JOB_FIND_INSTAGRAM,
            "find_instagram": FIELD_JOB_FIND_INSTAGRAM,
            "email": FIELD_JOB_FIND_EMAIL,
            "contact_email": FIELD_JOB_FIND_EMAIL,
            "find_email": FIELD_JOB_FIND_EMAIL,
            "verify_school_match": FIELD_JOB_VERIFY_SCHOOL,
            "school_match": FIELD_JOB_VERIFY_SCHOOL,
        }
        return mapping.get(raw_name.lower(), raw_name.lower())









    def field_job_graph_tables_ready(self) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    to_regclass('public.field_job_graph_runs') IS NOT NULL
                    AND to_regclass('public.field_job_graph_events') IS NOT NULL
                    AND to_regclass('public.field_job_graph_checkpoints') IS NOT NULL
                    AND to_regclass('public.field_job_graph_decisions') IS NOT NULL AS ready
                """
            )
            row = cursor.fetchone()
        return bool(row and row["ready"])


    def start_field_job_graph_run(
        self,
        *,
        worker_id: str,
        runtime_mode: str,
        source_slug: str | None,
        field_name: str | None,
        limit: int,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO field_job_graph_runs (
                    worker_id,
                    runtime_mode,
                    source_slug,
                    field_name,
                    requested_limit,
                    status,
                    metadata,
                    summary
                )
                VALUES (%s, %s, %s, %s, %s, 'running', %s, '{}'::jsonb)
                RETURNING id
                """,
                (worker_id, runtime_mode, source_slug, field_name, max(1, limit), Jsonb(metadata or {})),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return int(row["id"])

    def append_field_job_graph_event(
        self,
        *,
        run_id: int,
        node_name: str,
        phase: str,
        status: str,
        latency_ms: int,
        job_id: str | None = None,
        attempt: int | None = None,
        metrics_delta: dict[str, Any] | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO field_job_graph_events (
                    run_id,
                    job_id,
                    attempt,
                    node_name,
                    phase,
                    status,
                    latency_ms,
                    metrics_delta,
                    diagnostics
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    job_id,
                    attempt,
                    node_name,
                    phase,
                    status,
                    max(0, latency_ms),
                    Jsonb(metrics_delta or {}),
                    Jsonb(diagnostics or {}),
                ),
            )
        self._connection.commit()

    def upsert_field_job_graph_checkpoint(
        self,
        *,
        run_id: int,
        job_id: str,
        attempt: int,
        node_name: str,
        state: dict[str, Any],
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO field_job_graph_checkpoints (run_id, job_id, attempt, node_name, state)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id, job_id, attempt)
                DO UPDATE SET
                    node_name = EXCLUDED.node_name,
                    state = EXCLUDED.state,
                    updated_at = NOW()
                """,
                (run_id, job_id, max(1, attempt), node_name, Jsonb(state)),
            )
        self._connection.commit()

    def insert_field_job_graph_decision(
        self,
        *,
        run_id: int,
        job_id: str,
        attempt: int,
        field_name: str,
        decision: FieldJobDecision,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO field_job_graph_decisions (
                    run_id,
                    job_id,
                    attempt,
                    field_name,
                    decision_status,
                    confidence,
                    candidate_kind,
                    candidate_value,
                    reason_codes,
                    write_allowed,
                    requires_review,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    job_id,
                    max(1, attempt),
                    field_name,
                    decision.status,
                    decision.confidence,
                    decision.candidate_kind,
                    decision.candidate_value,
                    Jsonb(decision.reason_codes),
                    decision.write_allowed,
                    decision.requires_review,
                    Jsonb(metadata or {}),
                ),
            )
        self._connection.commit()

    def finish_field_job_graph_run(
        self,
        run_id: int,
        *,
        status: str,
        summary: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE field_job_graph_runs
                SET
                    status = %s,
                    summary = %s,
                    error_message = %s,
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (status, Jsonb(summary), error_message, run_id),
            )
        self._connection.commit()

    def start_crawl_session(
        self,
        *,
        crawl_run_id: int,
        source_id: str,
        runtime_mode: str,
        seed_urls: list[str],
        budget_config: dict[str, Any],
    ) -> str:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_sessions (crawl_run_id, source_id, runtime_mode, status, seed_urls, budget_config, summary)
                VALUES (%s, %s, %s, 'running', %s, %s, '{}'::jsonb)
                RETURNING id
                """,
                (crawl_run_id, source_id, runtime_mode, Jsonb(seed_urls), Jsonb(budget_config)),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"])

    def finish_crawl_session(
        self,
        crawl_session_id: str,
        *,
        status: str,
        stop_reason: str | None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE crawl_sessions
                SET
                    status = %s,
                    stop_reason = %s,
                    summary = COALESCE(%s, summary),
                    finished_at = NOW()
                WHERE id = %s
                """,
                (status, stop_reason, Jsonb(summary or {}), crawl_session_id),
            )
        self._connection.commit()

    def load_recent_crawl_session(self, crawl_run_id: int) -> dict[str, Any] | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, crawl_run_id, source_id, runtime_mode, status, seed_urls, budget_config, stop_reason, summary
                FROM crawl_sessions
                WHERE crawl_run_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (crawl_run_id,),
            )
            row = cursor.fetchone()
        return dict(row) if row is not None else None

    def enqueue_frontier_items(self, crawl_session_id: str, items: list[FrontierItem]) -> int:
        if not items:
            return 0
        created = 0
        with self._connection.cursor() as cursor:
            for item in items:
                cursor.execute(
                    """
                    INSERT INTO crawl_frontier_items (
                        crawl_session_id,
                        url,
                        canonical_url,
                        parent_url,
                        depth,
                        anchor_text,
                        discovered_from,
                        state,
                        score_total,
                        score_components,
                        selected_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (crawl_session_id, canonical_url)
                    DO UPDATE SET
                        score_total = GREATEST(crawl_frontier_items.score_total, EXCLUDED.score_total),
                        score_components = CASE
                            WHEN EXCLUDED.score_total >= crawl_frontier_items.score_total THEN EXCLUDED.score_components
                            ELSE crawl_frontier_items.score_components
                        END,
                        anchor_text = COALESCE(crawl_frontier_items.anchor_text, EXCLUDED.anchor_text),
                        parent_url = COALESCE(crawl_frontier_items.parent_url, EXCLUDED.parent_url),
                        discovered_from = CASE
                            WHEN EXCLUDED.score_total >= crawl_frontier_items.score_total THEN EXCLUDED.discovered_from
                            ELSE crawl_frontier_items.discovered_from
                        END
                    RETURNING id
                    """,
                    (
                        crawl_session_id,
                        item.url,
                        item.canonical_url,
                        item.parent_url,
                        item.depth,
                        item.anchor_text,
                        item.discovered_from,
                        item.state,
                        item.score_total,
                        Jsonb(item.score_components),
                        item.selected_count,
                    ),
                )
                if cursor.fetchone() is not None:
                    created += 1
        self._connection.commit()
        return created

    def pop_next_frontier_item(self, crawl_session_id: str) -> FrontierItem | None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH next_item AS (
                    SELECT id
                    FROM crawl_frontier_items
                    WHERE crawl_session_id = %s
                      AND state = 'queued'
                    ORDER BY score_total DESC, depth ASC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                ),
                claimed AS (
                    UPDATE crawl_frontier_items cfi
                    SET
                        state = 'visited',
                        selected_count = selected_count + 1,
                        updated_at = NOW()
                    FROM next_item
                    WHERE cfi.id = next_item.id
                    RETURNING cfi.*
                )
                SELECT * FROM claimed
                """,
                (crawl_session_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return FrontierItem(
            id=str(row["id"]),
            url=row["url"],
            canonical_url=row["canonical_url"],
            parent_url=row["parent_url"],
            depth=int(row["depth"]),
            anchor_text=row["anchor_text"],
            discovered_from=row["discovered_from"],
            state=row["state"],
            score_total=float(row["score_total"] or 0.0),
            score_components=row["score_components"] or {},
            selected_count=int(row["selected_count"] or 0),
        )

    def count_frontier_items(self, crawl_session_id: str, state: str = 'queued') -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*)::int AS count FROM crawl_frontier_items WHERE crawl_session_id = %s AND state = %s",
                (crawl_session_id, state),
            )
            row = cursor.fetchone()
        return int(row["count"] or 0)

    def append_page_observation(self, observation: PageObservation) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_page_observations (
                    crawl_session_id,
                    url,
                    template_signature,
                    structural_template_signature,
                    http_status,
                    latency_ms,
                    page_analysis,
                    classification,
                    embedded_data,
                    candidate_actions,
                    selected_action,
                    selected_action_score,
                    selected_action_score_components,
                    parent_observation_id,
                    path_depth,
                    risk_score,
                    guardrail_flags,
                    context_bucket,
                    outcome
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    observation.crawl_session_id,
                    observation.url,
                    observation.template_signature,
                    observation.structural_template_signature,
                    observation.http_status,
                    observation.latency_ms,
                    Jsonb(observation.page_analysis),
                    Jsonb(observation.classification),
                    Jsonb(observation.embedded_data),
                    Jsonb(observation.candidate_actions),
                    observation.selected_action,
                    observation.selected_action_score,
                    Jsonb(observation.selected_action_score_components),
                    observation.parent_observation_id,
                    observation.path_depth,
                    observation.risk_score,
                    Jsonb(observation.guardrail_flags),
                    observation.context_bucket,
                    Jsonb(observation.outcome),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return int(row["id"])

    def append_enrichment_observation(self, observation: EnrichmentObservation) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_enrichment_observations (
                    field_job_id,
                    chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    field_name,
                    queue_state,
                    runtime_mode,
                    policy_version,
                    policy_mode,
                    recommended_action,
                    deterministic_action,
                    recommended_actions,
                    context_features,
                    provider_window_state,
                    outcome
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    observation.field_job_id,
                    observation.chapter_id,
                    observation.chapter_slug,
                    observation.fraternity_slug,
                    observation.source_slug,
                    observation.field_name,
                    observation.queue_state,
                    observation.runtime_mode,
                    observation.policy_version,
                    observation.policy_mode,
                    observation.recommended_action,
                    observation.deterministic_action,
                    Jsonb(observation.recommended_actions),
                    Jsonb(observation.context_features),
                    Jsonb(observation.provider_window_state),
                    Jsonb(observation.outcome),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return int(row["id"])

    def append_reward_event(self, crawl_session_id: str, page_observation_id: int | None, event: RewardEvent) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_reward_events (
                    crawl_session_id,
                    page_observation_id,
                    action_type,
                    reward_value,
                    reward_components,
                    delayed,
                    reward_stage,
                    attributed_observation_id,
                    discount_factor
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    crawl_session_id,
                    page_observation_id,
                    event.action_type,
                    event.reward_value,
                    Jsonb(event.reward_components),
                    event.delayed,
                    event.reward_stage,
                    event.attributed_observation_id,
                    event.discount_factor,
                ),
            )
        self._connection.commit()

    def get_template_profile(self, template_signature: str, host_family: str) -> TemplateProfile | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    template_signature,
                    host_family,
                    page_role_guess,
                    best_action_family,
                    best_extraction_family,
                    visit_count,
                    chapter_yield,
                    contact_yield,
                    empty_rate,
                    timeout_rate,
                    updated_at
                FROM crawl_template_profiles
                WHERE template_signature = %s
                  AND host_family = %s
                LIMIT 1
                """,
                (template_signature, host_family),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return TemplateProfile(
            template_signature=row["template_signature"],
            host_family=row["host_family"],
            page_role_guess=row["page_role_guess"],
            best_action_family=row["best_action_family"],
            best_extraction_family=row["best_extraction_family"],
            visit_count=int(row["visit_count"] or 0),
            chapter_yield=float(row["chapter_yield"] or 0.0),
            contact_yield=float(row["contact_yield"] or 0.0),
            empty_rate=float(row["empty_rate"] or 0.0),
            timeout_rate=float(row["timeout_rate"] or 0.0),
            updated_at=row["updated_at"].isoformat() if row["updated_at"] else None,
        )

    def upsert_template_profile(
        self,
        *,
        template_signature: str,
        host_family: str,
        page_role_guess: str | None,
        action_type: str,
        extraction_family: str | None,
        chapter_yield: int,
        contact_yield: int,
        timeout: bool,
        empty: bool,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_template_profiles (
                    template_signature,
                    host_family,
                    page_role_guess,
                    best_action_family,
                    best_extraction_family,
                    visit_count,
                    chapter_yield,
                    contact_yield,
                    empty_rate,
                    timeout_rate,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 1, %s, %s, %s, %s, NOW())
                ON CONFLICT (template_signature, host_family)
                DO UPDATE SET
                    page_role_guess = COALESCE(EXCLUDED.page_role_guess, crawl_template_profiles.page_role_guess),
                    best_action_family = CASE
                        WHEN EXCLUDED.chapter_yield + EXCLUDED.contact_yield >= crawl_template_profiles.chapter_yield + crawl_template_profiles.contact_yield
                        THEN EXCLUDED.best_action_family
                        ELSE crawl_template_profiles.best_action_family
                    END,
                    best_extraction_family = CASE
                        WHEN EXCLUDED.chapter_yield + EXCLUDED.contact_yield >= crawl_template_profiles.chapter_yield + crawl_template_profiles.contact_yield
                        THEN EXCLUDED.best_extraction_family
                        ELSE crawl_template_profiles.best_extraction_family
                    END,
                    visit_count = crawl_template_profiles.visit_count + 1,
                    chapter_yield = ((crawl_template_profiles.chapter_yield * crawl_template_profiles.visit_count) + EXCLUDED.chapter_yield)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    contact_yield = ((crawl_template_profiles.contact_yield * crawl_template_profiles.visit_count) + EXCLUDED.contact_yield)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    empty_rate = ((crawl_template_profiles.empty_rate * crawl_template_profiles.visit_count) + EXCLUDED.empty_rate)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    timeout_rate = ((crawl_template_profiles.timeout_rate * crawl_template_profiles.visit_count) + EXCLUDED.timeout_rate)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    updated_at = NOW()
                """,
                (
                    template_signature,
                    host_family,
                    page_role_guess,
                    action_type,
                    extraction_family,
                    float(chapter_yield),
                    float(contact_yield),
                    1.0 if empty else 0.0,
                    1.0 if timeout else 0.0,
                ),
            )
        self._connection.commit()

    def save_policy_snapshot(
        self,
        *,
        policy_version: str,
        runtime_mode: str,
        feature_schema_version: str,
        model_payload: dict[str, Any],
        metrics: dict[str, Any],
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_policy_snapshots (
                    policy_version,
                    runtime_mode,
                    feature_schema_version,
                    model_payload,
                    metrics
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (policy_version, runtime_mode, feature_schema_version, Jsonb(model_payload), Jsonb(metrics)),
            )
        self._connection.commit()


    def load_latest_policy_snapshot(
        self,
        *,
        policy_version: str,
        runtime_mode: str | None = None,
    ) -> dict[str, Any] | None:
        with self._connection.cursor() as cursor:
            if runtime_mode is None:
                cursor.execute(
                    """
                    SELECT
                        id,
                        policy_version,
                        runtime_mode,
                        feature_schema_version,
                        model_payload,
                        metrics,
                        created_at
                    FROM crawl_policy_snapshots
                    WHERE policy_version = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (policy_version,),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        id,
                        policy_version,
                        runtime_mode,
                        feature_schema_version,
                        model_payload,
                        metrics,
                        created_at
                    FROM crawl_policy_snapshots
                    WHERE policy_version = %s
                      AND runtime_mode = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (policy_version, runtime_mode),
                )
            row = cursor.fetchone()
        if row is None:
            return None
        payload = dict(row)
        created_at = payload.get("created_at")
        if created_at is not None:
            payload["created_at"] = created_at.isoformat()
        return payload

    def list_crawl_run_metrics(
        self,
        *,
        source_slug: str,
        runtime_mode: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    cr.id,
                    s.slug AS source_slug,
                    cr.started_at,
                    cr.finished_at,
                    cr.status,
                    cr.pages_processed,
                    cr.records_seen,
                    cr.records_upserted,
                    cr.review_items_created,
                    cr.field_jobs_created,
                    EXTRACT(EPOCH FROM (COALESCE(cr.finished_at, NOW()) - cr.started_at)) * 1000 AS duration_ms
                FROM crawl_runs cr
                JOIN sources s ON s.id = cr.source_id
                WHERE s.slug = %s
                  AND COALESCE(cr.extraction_metadata ->> 'runtime_mode', 'legacy') = %s
                ORDER BY cr.started_at DESC
                LIMIT %s
                """,
                (source_slug, runtime_mode, max(1, limit)),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def export_crawl_observations(
        self,
        *,
        source_slug: str | None = None,
        crawl_session_id: str | None = None,
        runtime_mode: str | None = None,
        window_days: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if source_slug is not None:
            params.append(source_slug)
            filters.append("s.slug = %s")
        if crawl_session_id is not None:
            params.append(crawl_session_id)
            filters.append("cpo.crawl_session_id = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("cs.runtime_mode = %s")
        if window_days is not None:
            params.append(max(1, int(window_days)))
            filters.append("cpo.created_at >= NOW() - (%s * INTERVAL '1 day')")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    cpo.id,
                    cpo.crawl_session_id,
                    s.slug AS source_slug,
                    cs.runtime_mode,
                    cpo.url,
                    cpo.template_signature,
                    cpo.structural_template_signature,
                    cpo.parent_observation_id,
                    cpo.path_depth,
                    cpo.risk_score,
                    cpo.guardrail_flags,
                    cpo.context_bucket,
                    cpo.http_status,
                    cpo.latency_ms,
                    cpo.page_analysis,
                    cpo.classification,
                    cpo.embedded_data,
                    cpo.candidate_actions,
                    cpo.selected_action,
                    cpo.selected_action_score,
                    cpo.selected_action_score_components,
                    cpo.outcome,
                    cpo.created_at
                FROM crawl_page_observations cpo
                JOIN crawl_sessions cs ON cs.id = cpo.crawl_session_id
                JOIN sources s ON s.id = cs.source_id
                {where_clause}
                ORDER BY cpo.created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def export_enrichment_observations(
        self,
        *,
        source_slug: str | None = None,
        field_name: str | None = None,
        window_days: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if source_slug is not None:
            params.append(source_slug)
            filters.append("ceo.source_slug = %s")
        if field_name is not None:
            params.append(field_name)
            filters.append("ceo.field_name = %s")
        if window_days is not None:
            params.append(max(1, int(window_days)))
            filters.append("ceo.created_at >= NOW() - (%s * INTERVAL '1 day')")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    ceo.id,
                    ceo.field_job_id,
                    ceo.chapter_id,
                    ceo.chapter_slug,
                    ceo.fraternity_slug,
                    ceo.source_slug,
                    ceo.field_name,
                    ceo.queue_state,
                    ceo.runtime_mode,
                    ceo.policy_version,
                    ceo.policy_mode,
                    ceo.recommended_action,
                    ceo.deterministic_action,
                    ceo.recommended_actions,
                    ceo.context_features,
                    ceo.provider_window_state,
                    ceo.outcome,
                    ceo.created_at
                FROM crawl_enrichment_observations ceo
                {where_clause}
                ORDER BY ceo.created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def build_policy_report(self, limit: int = 25) -> dict[str, Any]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    template_signature,
                    host_family,
                    page_role_guess,
                    best_action_family,
                    best_extraction_family,
                    visit_count,
                    chapter_yield,
                    contact_yield,
                    empty_rate,
                    timeout_rate,
                    updated_at
                FROM crawl_template_profiles
                ORDER BY updated_at DESC, visit_count DESC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            templates = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT
                    action_type,
                    COUNT(*)::int AS event_count,
                    COALESCE(AVG(reward_value), 0) AS avg_reward,
                    COALESCE(SUM(reward_value), 0) AS total_reward
                FROM crawl_reward_events
                GROUP BY action_type
                ORDER BY avg_reward DESC, event_count DESC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            action_summary = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT
                    reward_stage,
                    COUNT(*)::int AS event_count,
                    COALESCE(AVG(reward_value), 0) AS avg_reward
                FROM crawl_reward_events
                GROUP BY reward_stage
                ORDER BY event_count DESC
                """
            )
            reward_stage_summary = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT
                    COALESCE(context_bucket, 'unknown') AS context_bucket,
                    COUNT(*)::int AS visit_count,
                    COALESCE(AVG(risk_score), 0) AS avg_risk
                FROM crawl_page_observations
                GROUP BY COALESCE(context_bucket, 'unknown')
                ORDER BY visit_count DESC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            context_summary = [dict(row) for row in cursor.fetchall()]
        return {
            "templateProfiles": templates,
            "actionSummary": action_summary,
            "rewardStageSummary": reward_stage_summary,
            "contextSummary": context_summary,
        }





    def export_reward_events(
        self,
        *,
        source_slug: str | None = None,
        runtime_mode: str | None = None,
        window_days: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if source_slug is not None:
            params.append(source_slug)
            filters.append("s.slug = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("cs.runtime_mode = %s")
        if window_days is not None:
            params.append(max(1, int(window_days)))
            filters.append("cre.created_at >= NOW() - (%s * INTERVAL '1 day')")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    cre.id,
                    cre.crawl_session_id,
                    s.slug AS source_slug,
                    cs.runtime_mode,
                    cre.page_observation_id,
                    cre.action_type,
                    cre.reward_value,
                    cre.reward_components,
                    cre.delayed,
                    cre.reward_stage,
                    cre.attributed_observation_id,
                    cre.discount_factor,
                    cre.created_at
                FROM crawl_reward_events cre
                JOIN crawl_sessions cs ON cs.id = cre.crawl_session_id
                JOIN sources s ON s.id = cs.source_id
                {where_clause}
                ORDER BY cre.created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def summarize_contact_coverage_for_runs(self, *, crawl_run_ids: list[int]) -> dict[str, int]:
        ids = [int(value) for value in crawl_run_ids if value is not None]
        if not ids:
            return {
                "chapters": 0,
                "any_contact": 0,
                "website": 0,
                "email": 0,
                "instagram": 0,
                "all_three": 0,
            }

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH scoped_chapters AS (
                    SELECT DISTINCT cp.chapter_id
                    FROM chapter_provenance cp
                    WHERE cp.crawl_run_id = ANY(%s)
                )
                SELECT
                    COUNT(*)::int AS chapters,
                    COUNT(*) FILTER (
                        WHERE COALESCE(c.website_url, '') <> ''
                           OR COALESCE(c.contact_email, '') <> ''
                           OR COALESCE(c.instagram_url, '') <> ''
                    )::int AS any_contact,
                    COUNT(*) FILTER (WHERE COALESCE(c.website_url, '') <> '')::int AS website,
                    COUNT(*) FILTER (WHERE COALESCE(c.contact_email, '') <> '')::int AS email,
                    COUNT(*) FILTER (WHERE COALESCE(c.instagram_url, '') <> '')::int AS instagram,
                    COUNT(*) FILTER (
                        WHERE COALESCE(c.website_url, '') <> ''
                          AND COALESCE(c.contact_email, '') <> ''
                          AND COALESCE(c.instagram_url, '') <> ''
                    )::int AS all_three
                FROM scoped_chapters sc
                JOIN chapters c ON c.id = sc.chapter_id
                """,
                (ids,),
            )
            row = cursor.fetchone()

        if row is None:
            return {
                "chapters": 0,
                "any_contact": 0,
                "website": 0,
                "email": 0,
                "instagram": 0,
                "all_three": 0,
            }
        return {
            "chapters": int(row["chapters"] or 0),
            "any_contact": int(row["any_contact"] or 0),
            "website": int(row["website"] or 0),
            "email": int(row["email"] or 0),
            "instagram": int(row["instagram"] or 0),
            "all_three": int(row["all_three"] or 0),
        }

    def get_chapter_completion_signal(self, chapter_id: str) -> dict[str, bool]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    chapter_status,
                    contact_email,
                    instagram_url,
                    COALESCE(contact_provenance -> 'contact_email' ->> 'contactProvenanceType', '') AS email_specificity,
                    COALESCE(contact_provenance -> 'instagram_url' ->> 'contactProvenanceType', '') AS instagram_specificity
                FROM chapters
                WHERE id = %s
                LIMIT 1
                """,
                (chapter_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return {
                "validated_active": False,
                "chapter_safe_email": False,
                "chapter_safe_instagram": False,
                "complete_row": False,
            }
        email_safe = bool(row["contact_email"]) and str(row["email_specificity"] or "") in {
            CONTACT_SPECIFICITY_CHAPTER,
            CONTACT_SPECIFICITY_SCHOOL,
            CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
        }
        instagram_safe = bool(row["instagram_url"]) and str(row["instagram_specificity"] or "") in {
            CONTACT_SPECIFICITY_CHAPTER,
            CONTACT_SPECIFICITY_SCHOOL,
            CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
        }
        validated_active = str(row["chapter_status"] or "").strip().lower() == "active"
        return {
            "validated_active": validated_active,
            "chapter_safe_email": email_safe,
            "chapter_safe_instagram": instagram_safe,
            "complete_row": bool(validated_active and (email_safe or instagram_safe)),
        }

    def insert_epoch_metric(self, metric: EpochMetric) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_epoch_metrics (
                    epoch,
                    policy_version,
                    runtime_mode,
                    train_sources,
                    eval_sources,
                    kpis,
                    deltas,
                    slopes,
                    cohort_label,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    metric.epoch,
                    metric.policy_version,
                    metric.runtime_mode,
                    Jsonb(metric.train_sources),
                    Jsonb(metric.eval_sources),
                    Jsonb(metric.kpis),
                    Jsonb(metric.deltas),
                    Jsonb(metric.slopes),
                    metric.cohort_label,
                    Jsonb(metric.metadata),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return int(row["id"])

    def insert_search_provider_attempt(
        self,
        *,
        context_type: str,
        context_id: str | None = None,
        request_id: str | None = None,
        source_slug: str | None = None,
        field_job_id: str | None = None,
        provider: str,
        provider_endpoint: str | None = None,
        query: str | None = None,
        status: str,
        failure_type: str | None = None,
        http_status: int | None = None,
        latency_ms: int | None = None,
        result_count: int | None = None,
        fallback_taken: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO search_provider_attempts (
                    context_type,
                    context_id,
                    request_id,
                    source_slug,
                    field_job_id,
                    provider,
                    provider_endpoint,
                    query,
                    status,
                    failure_type,
                    http_status,
                    latency_ms,
                    result_count,
                    fallback_taken,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    context_type,
                    context_id,
                    request_id,
                    source_slug,
                    field_job_id,
                    provider,
                    provider_endpoint,
                    query,
                    status,
                    failure_type,
                    http_status,
                    latency_ms,
                    result_count,
                    fallback_taken,
                    Jsonb(metadata or {}),
                ),
            )
        self._connection.commit()

    def insert_search_provider_attempts(self, attempts: list[dict[str, Any]]) -> None:
        if not attempts:
            return
        with self._connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO search_provider_attempts (
                    context_type,
                    context_id,
                    request_id,
                    source_slug,
                    field_job_id,
                    provider,
                    provider_endpoint,
                    query,
                    status,
                    failure_type,
                    http_status,
                    latency_ms,
                    result_count,
                    fallback_taken,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        attempt.get("context_type"),
                        attempt.get("context_id"),
                        attempt.get("request_id"),
                        attempt.get("source_slug"),
                        attempt.get("field_job_id"),
                        attempt.get("provider"),
                        attempt.get("provider_endpoint"),
                        attempt.get("query"),
                        attempt.get("status"),
                        attempt.get("failure_type"),
                        attempt.get("http_status"),
                        attempt.get("latency_ms"),
                        attempt.get("result_count"),
                        bool(attempt.get("fallback_taken", False)),
                        Jsonb(dict(attempt.get("metadata") or {})),
                    )
                    for attempt in attempts
                ],
            )
        self._connection.commit()

    def summarize_search_provider_attempts(
        self,
        *,
        context_type: str | None = None,
        source_slug: str | None = None,
        request_id: str | None = None,
        provider: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        filters: list[str] = []
        params: list[Any] = []
        if context_type is not None:
            filters.append("context_type = %s")
            params.append(context_type)
        if source_slug is not None:
            filters.append("source_slug = %s")
            params.append(source_slug)
        if request_id is not None:
            filters.append("request_id = %s")
            params.append(request_id)
        if provider is not None:
            filters.append("provider = %s")
            params.append(provider)
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    provider,
                    COALESCE(provider_endpoint, '') AS provider_endpoint,
                    COUNT(*)::int AS attempts,
                    COUNT(*) FILTER (WHERE status = 'success')::int AS successes,
                    COUNT(*) FILTER (WHERE status = 'request_error')::int AS request_errors,
                    COUNT(*) FILTER (WHERE status = 'unavailable')::int AS unavailable,
                    COUNT(*) FILTER (WHERE status = 'low_signal')::int AS low_signal,
                    COALESCE(AVG(latency_ms), 0)::float AS avg_latency_ms
                FROM search_provider_attempts
                {where_clause}
                GROUP BY provider, COALESCE(provider_endpoint, '')
                ORDER BY attempts DESC, provider ASC, provider_endpoint ASC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def list_epoch_metrics(
        self,
        *,
        policy_version: str | None = None,
        runtime_mode: str | None = None,
        cohort_label: str | None = None,
        limit: int = 120,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if policy_version is not None:
            params.append(policy_version)
            filters.append("policy_version = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("runtime_mode = %s")
        if cohort_label is not None:
            params.append(cohort_label)
            filters.append("cohort_label = %s")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id,
                    epoch,
                    policy_version,
                    runtime_mode,
                    train_sources,
                    eval_sources,
                    kpis,
                    deltas,
                    slopes,
                    cohort_label,
                    metadata,
                    created_at
                FROM crawl_epoch_metrics
                {where_clause}
                ORDER BY created_at DESC, epoch DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def list_policy_snapshots(
        self,
        *,
        policy_version: str | None = None,
        runtime_mode: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if policy_version is not None:
            params.append(policy_version)
            filters.append("policy_version = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("runtime_mode = %s")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id,
                    policy_version,
                    runtime_mode,
                    feature_schema_version,
                    model_payload,
                    metrics,
                    created_at
                FROM crawl_policy_snapshots
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def adaptive_policy_diff(self, snapshot_id_a: int, snapshot_id_b: int) -> dict[str, Any]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, policy_version, runtime_mode, feature_schema_version, model_payload, metrics, created_at
                FROM crawl_policy_snapshots
                WHERE id IN (%s, %s)
                """,
                (snapshot_id_a, snapshot_id_b),
            )
            rows = cursor.fetchall()
        snapshots = {int(row["id"]): dict(row) for row in rows}
        left = snapshots.get(snapshot_id_a)
        right = snapshots.get(snapshot_id_b)
        if left is None or right is None:
            return {"found": False, "left": left, "right": right}

        left_payload = left.get("model_payload") or {}
        right_payload = right.get("model_payload") or {}

        def _extract_actions(payload: dict[str, Any]) -> dict[str, dict[str, float]]:
            actions: dict[str, dict[str, float]] = {}
            for bucket_name in ("navigationActions", "extractionActions", "actions"):
                bucket = payload.get(bucket_name)
                if not isinstance(bucket, dict):
                    continue
                for action, values in bucket.items():
                    if not isinstance(values, dict):
                        continue
                    actions[str(action)] = {
                        "count": float(values.get("count") or 0.0),
                        "avgReward": float(values.get("avgReward") or 0.0),
                    }
            return actions

        left_actions = _extract_actions(left_payload if isinstance(left_payload, dict) else {})
        right_actions = _extract_actions(right_payload if isinstance(right_payload, dict) else {})

        action_keys = sorted(set(left_actions.keys()) | set(right_actions.keys()))
        action_deltas = []
        for key in action_keys:
            left_values = left_actions.get(key, {"count": 0.0, "avgReward": 0.0})
            right_values = right_actions.get(key, {"count": 0.0, "avgReward": 0.0})
            action_deltas.append(
                {
                    "actionType": key,
                    "countDelta": round(right_values["count"] - left_values["count"], 4),
                    "avgRewardDelta": round(right_values["avgReward"] - left_values["avgReward"], 4),
                    "left": left_values,
                    "right": right_values,
                }
            )

        return {
            "found": True,
            "left": left,
            "right": right,
            "actionDeltas": action_deltas,
        }




