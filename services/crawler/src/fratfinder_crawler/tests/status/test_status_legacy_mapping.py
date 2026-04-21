from __future__ import annotations

from fratfinder_crawler.status.validators import legacy_status_to_decision


def test_legacy_active_maps_to_school_recognized_when_school_evidence_exists():
    decision = legacy_status_to_decision(
        legacy_status="active",
        reason_code="legacy_active",
        evidence_ids=["evidence-1"],
        school_recognition_status="recognized",
    )
    assert decision.final_status == "active"
    assert decision.school_recognition_status == "recognized"


def test_legacy_inactive_maps_to_unknown_if_reason_code_missing():
    decision = legacy_status_to_decision(legacy_status="inactive", evidence_ids=["evidence-1"])
    assert decision.final_status == "unknown"


def test_legacy_unknown_maps_to_unknown_without_data_loss():
    decision = legacy_status_to_decision(
        legacy_status="unknown",
        reason_code="legacy_unknown_without_evidence",
        decision_trace={"legacy": True},
    )
    assert decision.final_status == "unknown"
    assert decision.decision_trace == {"legacy": True}


def test_legacy_field_job_payload_status_can_be_normalized():
    decision = legacy_status_to_decision(
        legacy_status="inactive",
        reason_code="official_school_negative_status",
        evidence_ids=["evidence-1"],
        school_recognition_status="unrecognized",
    )
    assert decision.final_status == "inactive"
    assert decision.reason_code == "official_school_negative_status"
