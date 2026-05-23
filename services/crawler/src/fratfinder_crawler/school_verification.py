from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Protocol

from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.status.models import ChapterStatusFinal, SchoolRecognitionStatus


_OFFICIAL_SOURCE_TYPE = "official_school"
_ACTIVE_STATUS = str(ChapterStatusFinal.ACTIVE.value)
_INACTIVE_STATUS = str(ChapterStatusFinal.INACTIVE.value)
_REVIEW_STATUS = str(ChapterStatusFinal.REVIEW.value)


class SchoolVerificationRepository(Protocol):
    def get_latest_chapter_status_decision(self, chapter_id: str): ...

    def get_chapter_activity(self, *, fraternity_slug: str | None, school_name: str | None): ...

    def get_school_policy(self, school_name: str | None): ...


@dataclass(slots=True)
class CachedSchoolVerificationResult:
    outcome: str
    reason_code: str
    stored_school_name: str | None = None
    candidate_school_name: str | None = None
    evidence_url: str | None = None
    evidence_source_type: str | None = None
    source_snippet: str | None = None
    confidence: float = 0.0
    status_decision_id: str | None = None
    review_required: bool = False
    conflict_flags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def resolve_cached_school_verification(
    *,
    job: FieldJob,
    repository: SchoolVerificationRepository,
) -> CachedSchoolVerificationResult:
    """Resolve school verification only from candidate identity and cached DB evidence."""
    stored_school_name = canonical_school_name(job.university_name)
    candidate_school_name = canonical_school_name(job.payload.get("candidateSchoolName"))
    stored_school_slug = school_slug(stored_school_name)
    candidate_school_slug = school_slug(candidate_school_name)

    if stored_school_slug and candidate_school_slug and stored_school_slug == candidate_school_slug:
        return CachedSchoolVerificationResult(
            outcome="verified",
            reason_code="candidate_school_exact_match",
            stored_school_name=stored_school_name or job.university_name,
            candidate_school_name=candidate_school_name,
            metadata={"decisionSource": "candidate_school_name"},
        )

    if stored_school_slug and candidate_school_slug and stored_school_slug != candidate_school_slug:
        return CachedSchoolVerificationResult(
            outcome="review_required",
            reason_code="candidate_school_mismatch",
            stored_school_name=stored_school_name or job.university_name,
            candidate_school_name=candidate_school_name,
            review_required=True,
            metadata={"decisionSource": "candidate_school_name"},
        )

    decision = _get_latest_decision(repository, job.chapter_id)
    if decision is not None and _status_decision_has_school_evidence(decision):
        final_status = _enum_value(getattr(decision, "final_status", ""))
        if final_status in {_ACTIVE_STATUS, _INACTIVE_STATUS, _REVIEW_STATUS}:
            trace = dict(getattr(decision, "decision_trace", {}) or {})
            evidence_url = _first_non_empty(
                trace.get("winning_evidence_url"),
                trace.get("source_url"),
                trace.get("evidence_url"),
            )
            result_outcome = {
                _ACTIVE_STATUS: "verified",
                _INACTIVE_STATUS: "inactive",
                _REVIEW_STATUS: "review_required",
            }[final_status]
            return CachedSchoolVerificationResult(
                outcome=result_outcome,
                reason_code=str(getattr(decision, "reason_code", "") or f"chapter_status_{final_status}"),
                stored_school_name=stored_school_name or job.university_name,
                evidence_url=evidence_url,
                evidence_source_type=_OFFICIAL_SOURCE_TYPE,
                source_snippet=str(trace.get("final_status_basis") or "") or None,
                confidence=float(getattr(decision, "confidence", 0.0) or 0.0),
                status_decision_id=str(getattr(decision, "id", "") or "") or None,
                review_required=bool(getattr(decision, "review_required", False)) or final_status == _REVIEW_STATUS,
                conflict_flags=list(getattr(decision, "conflict_flags", []) or []),
                metadata={
                    "decisionSource": "chapter_status_decision",
                    "finalStatus": final_status,
                    "schoolRecognitionStatus": _enum_value(getattr(decision, "school_recognition_status", "")),
                    "decisionTrace": trace,
                },
            )

    activity = repository.get_chapter_activity(
        fraternity_slug=job.fraternity_slug,
        school_name=job.university_name,
    )
    if _official(activity, "evidence_source_type"):
        activity_status = str(getattr(activity, "chapter_activity_status", "") or "").strip().lower()
        if activity_status in {"confirmed_active", "confirmed_inactive"}:
            return CachedSchoolVerificationResult(
                outcome="verified" if activity_status == "confirmed_active" else "inactive",
                reason_code=str(getattr(activity, "reason_code", "") or activity_status),
                stored_school_name=stored_school_name or job.university_name,
                evidence_url=str(getattr(activity, "evidence_url", "") or "") or None,
                evidence_source_type=_OFFICIAL_SOURCE_TYPE,
                source_snippet=str((getattr(activity, "metadata", {}) or {}).get("sourceSnippet") or "") or None,
                confidence=float(getattr(activity, "confidence", 0.0) or 0.0),
                metadata={
                    "decisionSource": "fraternity_school_activity_cache",
                    "chapterActivityStatus": activity_status,
                    **dict(getattr(activity, "metadata", {}) or {}),
                },
            )

    policy = repository.get_school_policy(job.university_name)
    if _official(policy, "evidence_source_type"):
        policy_status = str(getattr(policy, "greek_life_status", "") or "").strip().lower()
        if policy_status == "banned":
            return CachedSchoolVerificationResult(
                outcome="inactive",
                reason_code=str(getattr(policy, "reason_code", "") or "school_policy_banned"),
                stored_school_name=stored_school_name or job.university_name,
                evidence_url=str(getattr(policy, "evidence_url", "") or "") or None,
                evidence_source_type=_OFFICIAL_SOURCE_TYPE,
                source_snippet=str((getattr(policy, "metadata", {}) or {}).get("sourceSnippet") or "") or None,
                confidence=float(getattr(policy, "confidence", 0.0) or 0.0),
                metadata={
                    "decisionSource": "school_greek_life_registry",
                    "schoolPolicyStatus": policy_status,
                    **dict(getattr(policy, "metadata", {}) or {}),
                },
            )
        if policy_status == "allowed":
            return CachedSchoolVerificationResult(
                outcome="school_identity_only",
                reason_code=str(getattr(policy, "reason_code", "") or "school_policy_allowed_activity_unknown"),
                stored_school_name=stored_school_name or job.university_name,
                evidence_url=str(getattr(policy, "evidence_url", "") or "") or None,
                evidence_source_type=_OFFICIAL_SOURCE_TYPE,
                source_snippet=str((getattr(policy, "metadata", {}) or {}).get("sourceSnippet") or "") or None,
                confidence=float(getattr(policy, "confidence", 0.0) or 0.0),
                metadata={
                    "decisionSource": "school_greek_life_registry",
                    "schoolPolicyStatus": policy_status,
                    "chapterActivityStatus": "unknown",
                    **dict(getattr(policy, "metadata", {}) or {}),
                },
            )

    return CachedSchoolVerificationResult(
        outcome="evidence_missing",
        reason_code="school_evidence_missing",
        stored_school_name=stored_school_name or job.university_name,
        metadata={"decisionSource": "cache_miss"},
    )


def canonical_school_name(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text


def school_slug(value: Any) -> str:
    text = canonical_school_name(value).lower()
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def cached_school_verification_is_claimable(*, job: FieldJob, repository: SchoolVerificationRepository) -> bool:
    if school_slug(job.payload.get("candidateSchoolName")):
        return True
    result = resolve_cached_school_verification(job=job, repository=repository)
    return result.outcome != "evidence_missing"


def _get_latest_decision(repository: SchoolVerificationRepository, chapter_id: str):
    getter = getattr(repository, "get_latest_chapter_status_decision", None)
    if not callable(getter):
        return None
    return getter(chapter_id)


def _status_decision_has_school_evidence(decision: object) -> bool:
    school_recognition = _enum_value(getattr(decision, "school_recognition_status", ""))
    if school_recognition and school_recognition != str(SchoolRecognitionStatus.UNKNOWN.value):
        return True
    reason_code = str(getattr(decision, "reason_code", "") or "").strip().lower()
    return reason_code.startswith("official_school_") or "official_school" in reason_code


def _official(record: object | None, attr: str) -> bool:
    if record is None:
        return False
    return str(getattr(record, attr, "") or "").strip().lower() == _OFFICIAL_SOURCE_TYPE


def _enum_value(value: object) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _first_non_empty(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None
