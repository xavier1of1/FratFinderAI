from __future__ import annotations

from .models import (
    ACTIVE_SCHOOL_RECOGNITION,
    ChapterStatusDecision,
    ChapterStatusFinal,
    LegacyStatusMappingResult,
    SchoolRecognitionStatus,
)


def active_school_statuses() -> set[str]:
    return {status.value for status in ACTIVE_SCHOOL_RECOGNITION}


def school_policy_status_from_decision(decision: ChapterStatusDecision) -> str:
    if decision.school_recognition_status == SchoolRecognitionStatus.BANNED_NO_GREEK_LIFE:
        return "banned"
    if decision.final_status == ChapterStatusFinal.ACTIVE:
        return "allowed"
    return "unknown"


def chapter_activity_status_from_decision(decision: ChapterStatusDecision) -> str:
    if decision.final_status == ChapterStatusFinal.ACTIVE:
        return "confirmed_active"
    if decision.final_status == ChapterStatusFinal.INACTIVE:
        return "confirmed_inactive"
    return "unknown"


def legacy_status_to_decision(
    *,
    legacy_status: str,
    reason_code: str | None = None,
    evidence_ids: list[str] | None = None,
    decision_trace: dict[str, object] | None = None,
    school_recognition_status: str | None = None,
) -> LegacyStatusMappingResult:
    normalized = str(legacy_status or "unknown").strip().lower()
    normalized_reason = str(reason_code or "").strip()
    normalized_school_status = str(school_recognition_status or "unknown").strip()
    if normalized == "active" and evidence_ids:
        return LegacyStatusMappingResult(
            final_status=ChapterStatusFinal.ACTIVE,
            school_recognition_status=SchoolRecognitionStatus(normalized_school_status or SchoolRecognitionStatus.RECOGNIZED.value),
            reason_code=normalized_reason or "legacy_active_with_evidence",
            evidence_ids=list(evidence_ids),
            decision_trace=dict(decision_trace or {}),
        )
    if normalized == "inactive" and evidence_ids and normalized_reason:
        return LegacyStatusMappingResult(
            final_status=ChapterStatusFinal.INACTIVE,
            school_recognition_status=SchoolRecognitionStatus(normalized_school_status or SchoolRecognitionStatus.UNRECOGNIZED.value),
            reason_code=normalized_reason,
            evidence_ids=list(evidence_ids),
            decision_trace=dict(decision_trace or {}),
        )
    return LegacyStatusMappingResult(
        final_status=ChapterStatusFinal.UNKNOWN,
        school_recognition_status=SchoolRecognitionStatus.UNKNOWN,
        reason_code=normalized_reason or "legacy_unknown_without_evidence",
        evidence_ids=list(evidence_ids or []),
        decision_trace=dict(decision_trace or {}),
    )
