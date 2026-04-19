from fratfinder_crawler.db.repository import _extract_field_job_typed_state


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
