from __future__ import annotations

from .absence import infer_absence_status
from .entity_matcher import match_fraternity_in_zone
from .models import (
    ACTIVE_SCHOOL_RECOGNITION,
    NEGATIVE_ZONE_TYPES,
    POSITIVE_ZONE_TYPES,
    CampusStatusIndex,
    ChapterStatusDecision,
    ChapterStatusFinal,
    NationalStatusEvidence,
    NationalStatusValue,
    SchoolRecognitionStatus,
    StatusZoneType,
)


def _map_positive_zone(zone_type: StatusZoneType) -> SchoolRecognitionStatus:
    if zone_type == StatusZoneType.PROBATIONARY_RECOGNITION:
        return SchoolRecognitionStatus.PROBATIONARY_RECOGNITION
    if zone_type in {StatusZoneType.CONDUCT_GOOD, StatusZoneType.CONDUCT_PROBATION, StatusZoneType.DEFERRED_SUSPENSION}:
        return SchoolRecognitionStatus.ACTIVE_UNDER_CONDUCT_SANCTION
    return SchoolRecognitionStatus.RECOGNIZED


def _map_negative_zone(zone_type: StatusZoneType) -> SchoolRecognitionStatus:
    mapping = {
        StatusZoneType.INTERIM_SUSPENSION: SchoolRecognitionStatus.INTERIM_SUSPENSION,
        StatusZoneType.SUSPENDED: SchoolRecognitionStatus.SUSPENDED,
        StatusZoneType.CLOSED: SchoolRecognitionStatus.CLOSED,
        StatusZoneType.DISMISSED: SchoolRecognitionStatus.DISMISSED,
        StatusZoneType.EXPELLED: SchoolRecognitionStatus.EXPELLED,
        StatusZoneType.UNRECOGNIZED: SchoolRecognitionStatus.UNRECOGNIZED,
        StatusZoneType.NO_GREEK_POLICY: SchoolRecognitionStatus.BANNED_NO_GREEK_LIFE,
    }
    return mapping.get(zone_type, SchoolRecognitionStatus.UNKNOWN)


def _best_match(
    *,
    fraternity_name: str,
    fraternity_slug: str | None,
    school_name: str,
    index: CampusStatusIndex,
    positive: bool,
    aliases: list[str] | None,
):
    source_map = index.source_by_url()
    zone_types = POSITIVE_ZONE_TYPES if positive else NEGATIVE_ZONE_TYPES
    best = None
    best_score = -1.0
    for zone in index.zones:
        if zone.zone_type not in zone_types:
            continue
        match = match_fraternity_in_zone(
            fraternity_name=fraternity_name,
            fraternity_slug=fraternity_slug,
            school_name=school_name,
            aliases=aliases,
            zone=zone,
        )
        if not match.matched:
            continue
        source = source_map.get(zone.source_url)
        if source is None:
            continue
        score = (1.1 - (source.authority_tier * 0.08)) + (source.currentness_score * 0.5) + (zone.confidence * 0.5) + (match.confidence * 0.5)
        if score > best_score:
            best_score = score
            best = (zone, source, match)
    return best


def decide_chapter_status(
    *,
    fraternity_name: str,
    fraternity_slug: str | None,
    school_name: str,
    index: CampusStatusIndex,
    national_evidence: NationalStatusEvidence | None = None,
    aliases: list[str] | None = None,
) -> ChapterStatusDecision:
    if index.no_greek_policy is not None:
        source = index.no_greek_policy
        return ChapterStatusDecision(
            final_status=ChapterStatusFinal.INACTIVE,
            school_recognition_status=SchoolRecognitionStatus.BANNED_NO_GREEK_LIFE,
            national_status=national_evidence.status if national_evidence is not None else NationalStatusValue.UNKNOWN,
            reason_code="official_school_policy_prohibits_fraternities",
            confidence=0.98,
            evidence_ids=[source.source_url],
            decision_trace={
                "authority_order": ["school_policy", "school_status", "national_directory"],
                "winning_evidence_id": source.source_url,
                "final_status_basis": "official_school_policy",
            },
        )

    positive = _best_match(
        fraternity_name=fraternity_name,
        fraternity_slug=fraternity_slug,
        school_name=school_name,
        index=index,
        positive=True,
        aliases=aliases,
    )
    negative = _best_match(
        fraternity_name=fraternity_name,
        fraternity_slug=fraternity_slug,
        school_name=school_name,
        index=index,
        positive=False,
        aliases=aliases,
    )

    conflict_flags: list[str] = []
    if positive is not None and national_evidence is not None and national_evidence.status in {
        NationalStatusValue.INACTIVE,
        NationalStatusValue.DORMANT,
        NationalStatusValue.CLOSED,
    }:
        conflict_flags.append("school_active_national_inactive_conflict")
    if negative is not None and national_evidence is not None and national_evidence.status == NationalStatusValue.ACTIVE:
        conflict_flags.append("school_inactive_national_active_conflict")

    if positive is not None and negative is not None:
        _, positive_source, _ = positive
        _, negative_source, _ = negative
        positive_score = positive_source.currentness_score + (1.0 - positive_source.authority_tier * 0.1)
        negative_score = negative_source.currentness_score + (1.0 - negative_source.authority_tier * 0.1)
        if positive_score >= negative_score:
            zone, source, match = positive
            return ChapterStatusDecision(
                final_status=ChapterStatusFinal.ACTIVE,
                school_recognition_status=_map_positive_zone(zone.zone_type),
                national_status=national_evidence.status if national_evidence is not None else NationalStatusValue.UNKNOWN,
                reason_code="official_school_current_recognition",
                confidence=min(0.97, max(match.confidence, zone.confidence)),
                evidence_ids=[source.source_url],
                conflict_flags=conflict_flags + ["stale_source_conflict"],
                decision_trace={
                    "authority_order": ["school_status", "school_conduct", "national_directory", "chapter_site"],
                    "winning_evidence_id": source.source_url,
                    "conflicting_evidence_ids": [negative_source.source_url],
                    "conflict_flags": conflict_flags + ["stale_source_conflict"],
                    "final_status_basis": "official_school_current_recognition",
                },
            )
        zone, source, match = negative
        return ChapterStatusDecision(
            final_status=ChapterStatusFinal.INACTIVE,
            school_recognition_status=_map_negative_zone(zone.zone_type),
            national_status=national_evidence.status if national_evidence is not None else NationalStatusValue.UNKNOWN,
            reason_code="official_school_negative_status",
            confidence=min(0.98, max(match.confidence, zone.confidence)),
            evidence_ids=[source.source_url],
            conflict_flags=conflict_flags + ["stale_source_conflict"],
            decision_trace={
                "authority_order": ["school_status", "school_conduct", "national_directory", "chapter_site"],
                "winning_evidence_id": source.source_url,
                "conflicting_evidence_ids": [positive_source.source_url],
                "conflict_flags": conflict_flags + ["stale_source_conflict"],
                "final_status_basis": "official_school_negative_status",
            },
        )

    if positive is not None:
        zone, source, match = positive
        return ChapterStatusDecision(
            final_status=ChapterStatusFinal.ACTIVE,
            school_recognition_status=_map_positive_zone(zone.zone_type),
            national_status=national_evidence.status if national_evidence is not None else NationalStatusValue.UNKNOWN,
            reason_code="official_school_current_recognition",
            confidence=min(0.97, max(match.confidence, zone.confidence)),
            evidence_ids=[source.source_url],
            conflict_flags=conflict_flags,
            decision_trace={
                "authority_order": ["school_status", "school_conduct", "national_directory", "chapter_site"],
                "winning_evidence_id": source.source_url,
                "conflicting_evidence_ids": [],
                "conflict_flags": conflict_flags,
                "final_status_basis": "official_school_current_recognition",
            },
        )

    if negative is not None:
        zone, source, match = negative
        return ChapterStatusDecision(
            final_status=ChapterStatusFinal.INACTIVE,
            school_recognition_status=_map_negative_zone(zone.zone_type),
            national_status=national_evidence.status if national_evidence is not None else NationalStatusValue.UNKNOWN,
            reason_code="official_school_negative_status",
            confidence=min(0.98, max(match.confidence, zone.confidence)),
            evidence_ids=[source.source_url],
            conflict_flags=conflict_flags,
            decision_trace={
                "authority_order": ["school_status", "school_conduct", "national_directory", "chapter_site"],
                "winning_evidence_id": source.source_url,
                "conflicting_evidence_ids": [],
                "conflict_flags": conflict_flags,
                "final_status_basis": "official_school_negative_status",
            },
        )

    absence = infer_absence_status(
        fraternity_name=fraternity_name,
        fraternity_slug=fraternity_slug,
        school_name=school_name,
        index=index,
        aliases=aliases,
        national_evidence=national_evidence,
    )
    if absence.final_status == "inactive":
        extra_conflicts = list(conflict_flags)
        if national_evidence is not None and national_evidence.status == NationalStatusValue.ACTIVE:
            extra_conflicts.append("school_roster_absence_national_active_conflict")
        return ChapterStatusDecision(
            final_status=ChapterStatusFinal.INACTIVE,
            school_recognition_status=absence.school_recognition_status,
            national_status=national_evidence.status if national_evidence is not None else NationalStatusValue.UNKNOWN,
            reason_code=absence.reason_code,
            confidence=absence.confidence,
            evidence_ids=[source.source_url for source in index.sources if source.authority_tier <= 2][:4],
            conflict_flags=extra_conflicts,
            decision_trace={
                "authority_order": ["school_status", "school_conduct", "national_directory", "chapter_site"],
                "winning_evidence_id": (index.sources[0].source_url if index.sources else None),
                "conflicting_evidence_ids": [national_evidence.evidence_url] if national_evidence is not None and national_evidence.evidence_url else [],
                "conflict_flags": extra_conflicts,
                "final_status_basis": "official_school_roster_absence",
            },
        )

    if national_evidence is not None and national_evidence.status in {
        NationalStatusValue.INACTIVE,
        NationalStatusValue.DORMANT,
        NationalStatusValue.CLOSED,
    }:
        return ChapterStatusDecision(
            final_status=ChapterStatusFinal.REVIEW,
            school_recognition_status=SchoolRecognitionStatus.UNKNOWN,
            national_status=national_evidence.status,
            reason_code="national_all_status_directory_inactive_school_unknown",
            confidence=min(0.82, national_evidence.confidence),
            evidence_ids=[national_evidence.evidence_url] if national_evidence.evidence_url else ["national-evidence"],
            review_required=True,
            decision_trace={
                "authority_order": ["school_status", "school_conduct", "national_directory", "chapter_site"],
                "winning_evidence_id": national_evidence.evidence_url,
                "conflicting_evidence_ids": [],
                "conflict_flags": conflict_flags,
                "final_status_basis": "national_directory_only",
            },
        )

    return ChapterStatusDecision(
        final_status=ChapterStatusFinal.UNKNOWN,
        school_recognition_status=SchoolRecognitionStatus.UNKNOWN,
        national_status=national_evidence.status if national_evidence is not None else NationalStatusValue.UNKNOWN,
        reason_code="no_conclusive_school_status_evidence",
        confidence=0.3,
        evidence_ids=["unknown-status-placeholder"],
        review_required=False,
        decision_trace={
            "authority_order": ["school_status", "school_conduct", "national_directory", "chapter_site"],
            "winning_evidence_id": None,
            "conflicting_evidence_ids": [],
            "conflict_flags": conflict_flags,
            "final_status_basis": "no_conclusive_school_status_evidence",
        },
    )
