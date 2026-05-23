from fratfinder_crawler.db.repository import _build_contact_provenance_patch, _extract_field_job_typed_state, _strip_postgres_nuls
from fratfinder_crawler.models import ProvenanceRecord


def test_extract_field_job_typed_state_uses_reason_code_for_blocked_reason():
    typed_state = _extract_field_job_typed_state(
        {
            "contactResolution": {
                "queueState": "deferred",
                "reasonCode": "provider_degraded",
            }
        }
    )

    assert typed_state["queue_state"] == "deferred"
    assert typed_state["blocked_reason"] == "provider_degraded"


def test_extract_field_job_typed_state_prefers_explicit_blocked_reason():
    typed_state = _extract_field_job_typed_state(
        {
            "contactResolution": {
                "queueState": "deferred",
                "blockedReason": "website_required",
                "reasonCode": "dependency_wait",
            },
            "queueTriage": {
                "reason": "triage_reason_should_not_win",
            },
        }
    )

    assert typed_state["blocked_reason"] == "website_required"


def test_extract_field_job_typed_state_clears_blocked_reason_when_actionable():
    typed_state = _extract_field_job_typed_state(
        {
            "contactResolution": {
                "queueState": "actionable",
                "reasonCode": "status_dependency_unmet",
            }
        }
    )

    assert typed_state["queue_state"] == "actionable"
    assert typed_state["blocked_reason"] is None


def test_extract_field_job_typed_state_infers_blocked_provider_from_reason_only():
    typed_state = _extract_field_job_typed_state(
        {
            "contactResolution": {
                "reasonCode": "transient_network",
            }
        }
    )

    assert typed_state["queue_state"] == "blocked_provider"
    assert typed_state["blocked_reason"] == "transient_network"


def test_extract_field_job_typed_state_infers_blocked_dependency_from_reason_only():
    typed_state = _extract_field_job_typed_state(
        {
            "contactResolution": {
                "reasonCode": "status_dependency_unmet",
            }
        }
    )

    assert typed_state["queue_state"] == "blocked_dependency"
    assert typed_state["blocked_reason"] == "status_dependency_unmet"


def test_extract_field_job_typed_state_treats_school_evidence_missing_as_dependency():
    typed_state = _extract_field_job_typed_state(
        {
            "contactResolution": {
                "reasonCode": "school_evidence_missing",
            }
        }
    )

    assert typed_state["queue_state"] == "blocked_dependency"
    assert typed_state["blocked_reason"] == "school_evidence_missing"


def test_strip_postgres_nuls_recursively_cleans_json_values():
    payload = {
        "title": "Alpha\x00Beta",
        "items": ("one\x00", {"nested\x00": "two\x00"}),
        "links": {"x\x00", "y"},
    }

    cleaned = _strip_postgres_nuls(payload)

    assert cleaned["title"] == "AlphaBeta"
    assert cleaned["items"] == ["one", {"nested": "two"}]
    assert sorted(cleaned["links"]) == ["x", "y"]


def test_contact_provenance_patch_strips_nuls_from_provenance_records():
    patch = _build_contact_provenance_patch(
        chapter_updates={"website_url": "https://example.edu/alpha\x00"},
        field_state_updates={"website_url": "found"},
        completed_payload={"status": "updated", "reasonCode": "accepted\x00"},
        provenance_records=[
            ProvenanceRecord(
                source_slug="alpha-main",
                source_url="https://example.edu/list\x00",
                field_name="website_url",
                field_value="https://example.edu/alpha\x00",
                source_snippet="chapter block\x00",
                confidence=0.93,
            )
        ],
    )

    assert "\x00" not in str(patch)
    assert patch["website_url"]["candidateValue"] == "https://example.edu/alpha"
