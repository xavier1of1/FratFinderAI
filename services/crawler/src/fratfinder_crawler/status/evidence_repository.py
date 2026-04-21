from __future__ import annotations

from .models import ChapterStatusDecision, ChapterStatusEvidence


def status_decision_metadata(decision: ChapterStatusDecision) -> dict[str, object]:
    return {
        "statusDecisionId": decision.id,
        "finalStatus": decision.final_status,
        "schoolRecognitionStatus": decision.school_recognition_status,
        "nationalStatus": decision.national_status,
        "conflictFlags": list(decision.conflict_flags),
        "reviewRequired": bool(decision.review_required),
    }


def evidence_metadata(evidence: ChapterStatusEvidence) -> dict[str, object]:
    return {
        "authorityTier": evidence.authority_tier,
        "evidenceType": evidence.evidence_type,
        "statusSignal": evidence.status_signal,
        "zoneType": evidence.zone_type,
        "matchConfidence": evidence.match_confidence,
        "evidenceConfidence": evidence.evidence_confidence,
        **dict(evidence.metadata or {}),
    }
