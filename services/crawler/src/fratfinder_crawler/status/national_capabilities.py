from __future__ import annotations

import re

from .models import NationalDirectoryCapability, NationalStatusEvidence, NationalStatusValue


def _normalize(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def classify_national_directory_capability(
    *,
    page_url: str,
    title: str = "",
    text: str = "",
    html: str = "",
) -> tuple[NationalDirectoryCapability, dict[str, object]]:
    combined = _normalize(" ".join(part for part in [page_url, title, text[:8000], html[:8000]] if part))
    has_all_status_labels = all(marker in combined for marker in ("active", "inactive"))
    if any(marker in combined for marker in ("dormant chapters", "active chapters and colonies")):
        return NationalDirectoryCapability.ACTIVE_DORMANT_SPLIT, {"reason": "split_active_dormant_labels"}
    if any(marker in combined for marker in ("all chapters and associate chapters", "designated", "their status")) and has_all_status_labels:
        return NationalDirectoryCapability.ALL_STATUS, {"reason": "all_status_labels_present"}
    if has_all_status_labels:
        return NationalDirectoryCapability.ALL_STATUS, {"reason": "mixed_status_labels_present"}
    if any(marker in combined for marker in ("map", "locator", "chapter map")) and combined.count("chapter") < 5:
        return NationalDirectoryCapability.MAP_WITH_STATUS_FILTER, {"reason": "locator_shell"}
    if any(marker in combined for marker in ("history", "historical", "chapter roll", "archives")):
        return NationalDirectoryCapability.HISTORY_ROLL, {"reason": "historical_roll"}
    if any(marker in combined for marker in ("don t see a chapter at your school", "bring", "start a chapter", "our chapters", "chapter directory")):
        return NationalDirectoryCapability.ACTIVE_ONLY, {"reason": "active_only_language"}
    return NationalDirectoryCapability.UNKNOWN, {"reason": "capability_unclear"}


def infer_national_status_from_page(
    *,
    fraternity_name: str,
    school_name: str,
    page_url: str,
    title: str = "",
    text: str = "",
    html: str = "",
) -> NationalStatusEvidence:
    capability, metadata = classify_national_directory_capability(page_url=page_url, title=title, text=text, html=html)
    combined = _normalize(" ".join(part for part in [title, text[:12000]] if part))
    school = _normalize(school_name)
    if capability == NationalDirectoryCapability.HISTORY_ROLL:
        return NationalStatusEvidence(
            status=NationalStatusValue.UNKNOWN,
            capability=capability,
            evidence_url=page_url,
            confidence=0.2,
            reason_code="national_history_roll_not_current",
            metadata=metadata,
        )
    if school and school in combined and "dormant" in combined:
        return NationalStatusEvidence(
            status=NationalStatusValue.DORMANT,
            capability=capability,
            evidence_url=page_url,
            confidence=0.84,
            reason_code="national_directory_lists_dormant",
            metadata=metadata,
        )
    if school and school in combined and "inactive" in combined:
        return NationalStatusEvidence(
            status=NationalStatusValue.INACTIVE,
            capability=capability,
            evidence_url=page_url,
            confidence=0.82,
            reason_code="national_directory_lists_inactive",
            metadata=metadata,
        )
    if school and school in combined and "associate" in combined:
        return NationalStatusEvidence(
            status=NationalStatusValue.ASSOCIATE,
            capability=capability,
            evidence_url=page_url,
            confidence=0.8,
            reason_code="national_directory_lists_associate",
            metadata=metadata,
        )
    if school and school in combined and any(marker in combined for marker in ("active", "chapter", "colony")):
        return NationalStatusEvidence(
            status=NationalStatusValue.ACTIVE,
            capability=capability,
            evidence_url=page_url,
            confidence=0.76,
            reason_code="national_directory_lists_active",
            metadata=metadata,
        )
    if capability == NationalDirectoryCapability.ACTIVE_ONLY:
        return NationalStatusEvidence(
            status=NationalStatusValue.NOT_LISTED_ON_ACTIVE_ONLY_DIRECTORY,
            capability=capability,
            evidence_url=page_url,
            confidence=0.5,
            reason_code="national_active_only_absence_not_conclusive",
            metadata=metadata,
        )
    if capability == NationalDirectoryCapability.ALL_STATUS:
        return NationalStatusEvidence(
            status=NationalStatusValue.NOT_LISTED_ON_ALL_STATUS_DIRECTORY,
            capability=capability,
            evidence_url=page_url,
            confidence=0.6,
            reason_code="national_all_status_absence",
            metadata=metadata,
        )
    return NationalStatusEvidence(
        status=NationalStatusValue.UNKNOWN,
        capability=capability,
        evidence_url=page_url,
        confidence=0.25,
        reason_code="national_status_unknown",
        metadata=metadata,
    )
