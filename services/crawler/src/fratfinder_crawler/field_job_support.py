from __future__ import annotations

from fratfinder_crawler.models import FieldJob


def job_supporting_page_ready(job: FieldJob) -> bool:
    field_states = dict(job.field_states or {})
    website_state = str(field_states.get("website_url") or "").strip().lower()
    if job.website_url and website_state not in {"", "missing", "low_confidence"}:
        return True

    contact_resolution = job.payload.get("contactResolution") if isinstance(job.payload.get("contactResolution"), dict) else {}
    supporting_page_url = str(contact_resolution.get("supportingPageUrl") or "").strip()
    supporting_page_scope = str(contact_resolution.get("supportingPageScope") or contact_resolution.get("pageScope") or "").strip().lower()
    if supporting_page_url and supporting_page_scope in {
        "chapter_site",
        "school_affiliation_page",
        "nationals_chapter_page",
    }:
        return True

    if website_state == "confirmed_absent":
        if job.contact_email or job.instagram_url:
            return True
        if supporting_page_url and supporting_page_scope in {"school_affiliation_page", "nationals_chapter_page"}:
            return True
    return False
