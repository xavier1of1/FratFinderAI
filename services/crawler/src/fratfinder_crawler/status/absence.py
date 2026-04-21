from __future__ import annotations

from pydantic import BaseModel, Field

from .entity_matcher import match_fraternity_in_zone
from .models import (
    CampusStatusIndex,
    NationalStatusEvidence,
    SchoolRecognitionStatus,
    StatusZoneType,
)

MIN_ORG_COUNT_FOR_CONCLUSIVE_ROSTER = 5
MIN_CURRENTNESS_SCORE = 0.70
MIN_PARSE_COMPLETENESS_SCORE = 0.85
MAX_AUTHORITY_TIER_FOR_CONCLUSIVE_ABSENCE = 2


class AbsenceDecision(BaseModel):
    final_status: str
    school_recognition_status: SchoolRecognitionStatus
    reason_code: str
    confidence: float = Field(ge=0.0, le=1.0)
    review_required: bool = False
    metadata: dict[str, object] = Field(default_factory=dict)


def is_conclusive_active_roster(index: CampusStatusIndex) -> bool:
    source_map = index.source_by_url()
    greek_tokens = (
        "alpha",
        "beta",
        "gamma",
        "delta",
        "epsilon",
        "zeta",
        "eta",
        "theta",
        "iota",
        "kappa",
        "lambda",
        "mu",
        "nu",
        "xi",
        "omicron",
        "pi",
        "rho",
        "sigma",
        "tau",
        "upsilon",
        "phi",
        "chi",
        "psi",
        "omega",
    )
    for source in index.sources:
        if source.authority_tier > MAX_AUTHORITY_TIER_FOR_CONCLUSIVE_ABSENCE:
            continue
        if source.currentness_score < MIN_CURRENTNESS_SCORE:
            continue
        if source.parse_completeness_score < MIN_PARSE_COMPLETENESS_SCORE:
            continue
        zone_count = len(
            [
                zone
                for zone in index.zones
                if zone.source_url == source.source_url
                and zone.zone_type
                in {
                    StatusZoneType.ACTIVE,
                    StatusZoneType.RECOGNIZED,
                    StatusZoneType.PROBATIONARY_RECOGNITION,
                    StatusZoneType.SUSPENDED,
                    StatusZoneType.CLOSED,
                    StatusZoneType.UNRECOGNIZED,
                }
            ]
        )
        org_count = sum(
            zone.text.lower().count(token)
            for zone in index.zones
            if zone.source_url == source.source_url
            for token in greek_tokens
        )
        recognition_language = any(
            marker in f"{source.title.lower()} {source.text.lower()}"
            for marker in (
                "recognized chapters",
                "chapters below are recognized",
                "current chapters",
                "active chapters",
                "chapter status",
                "community scorecard",
            )
        )
        if recognition_language and zone_count >= 1 and org_count >= MIN_ORG_COUNT_FOR_CONCLUSIVE_ROSTER:
            return True
        source_map.get(source.source_url)
    return False


def infer_absence_status(
    *,
    fraternity_name: str,
    fraternity_slug: str | None,
    school_name: str,
    index: CampusStatusIndex,
    aliases: list[str] | None = None,
    national_evidence: NationalStatusEvidence | None = None,
) -> AbsenceDecision:
    if index.no_greek_policy is not None:
        return AbsenceDecision(
            final_status="inactive",
            school_recognition_status=SchoolRecognitionStatus.BANNED_NO_GREEK_LIFE,
            reason_code="official_school_policy_prohibits_fraternities",
            confidence=0.98,
        )

    for zone in index.zones:
        if zone.zone_type not in {
            StatusZoneType.SUSPENDED,
            StatusZoneType.CLOSED,
            StatusZoneType.UNRECOGNIZED,
            StatusZoneType.DISMISSED,
            StatusZoneType.EXPELLED,
            StatusZoneType.INTERIM_SUSPENSION,
        }:
            continue
        match = match_fraternity_in_zone(
            fraternity_name=fraternity_name,
            fraternity_slug=fraternity_slug,
            aliases=aliases,
            school_name=school_name,
            zone=zone,
        )
        if match.matched:
            mapping = {
                StatusZoneType.SUSPENDED: SchoolRecognitionStatus.SUSPENDED,
                StatusZoneType.CLOSED: SchoolRecognitionStatus.CLOSED,
                StatusZoneType.UNRECOGNIZED: SchoolRecognitionStatus.UNRECOGNIZED,
                StatusZoneType.DISMISSED: SchoolRecognitionStatus.DISMISSED,
                StatusZoneType.EXPELLED: SchoolRecognitionStatus.EXPELLED,
                StatusZoneType.INTERIM_SUSPENSION: SchoolRecognitionStatus.INTERIM_SUSPENSION,
            }
            return AbsenceDecision(
                final_status="inactive",
                school_recognition_status=mapping.get(zone.zone_type, SchoolRecognitionStatus.UNRECOGNIZED),
                reason_code="present_in_suspended_closed_or_unrecognized_zone",
                confidence=0.96,
            )

    for zone in index.zones:
        if zone.zone_type not in {StatusZoneType.ACTIVE, StatusZoneType.RECOGNIZED, StatusZoneType.PROBATIONARY_RECOGNITION}:
            continue
        match = match_fraternity_in_zone(
            fraternity_name=fraternity_name,
            fraternity_slug=fraternity_slug,
            aliases=aliases,
            school_name=school_name,
            zone=zone,
        )
        if match.matched:
            recognition = (
                SchoolRecognitionStatus.PROBATIONARY_RECOGNITION
                if zone.zone_type == StatusZoneType.PROBATIONARY_RECOGNITION
                else SchoolRecognitionStatus.RECOGNIZED
            )
            return AbsenceDecision(
                final_status="active",
                school_recognition_status=recognition,
                reason_code="present_in_recognized_or_active_zone",
                confidence=0.96,
            )

    if is_conclusive_active_roster(index):
        return AbsenceDecision(
            final_status="inactive",
            school_recognition_status=SchoolRecognitionStatus.NOT_FOUND_ON_CONCLUSIVE_ROSTER,
            reason_code="absent_from_current_official_complete_school_roster",
            confidence=0.91,
            metadata={
                "currentnessScore": index.currentness_score,
                "parseCompletenessScore": index.parse_completeness_score,
                "hasConclusiveRoster": True,
                "nationalStatus": national_evidence.status if national_evidence is not None else "unknown",
            },
        )
    return AbsenceDecision(
        final_status="unknown",
        school_recognition_status=SchoolRecognitionStatus.UNKNOWN,
        reason_code="absence_not_conclusive",
        confidence=0.35,
        review_required=False,
        metadata={
            "currentnessScore": index.currentness_score,
            "parseCompletenessScore": index.parse_completeness_score,
            "hasConclusiveRoster": False,
        },
    )
