from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from fratfinder_crawler.models import ChapterEvidenceRecord
from fratfinder_crawler.social.instagram_models import InstagramCandidate, InstagramSourceType
from fratfinder_crawler.social.instagram_normalizer import canonicalize_instagram_profile, extract_instagram_handle


_LEGACY_SOURCE_TYPE_ALIASES: dict[str, InstagramSourceType] = {
    "official_school": InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
    "official_school_page": InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
    "school_page": InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
    "school_directory": InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
    "chapter_site": InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
    "chapter_website": InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
    "chapter_website_structured_data": InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA,
    "chapter_website_social_link": InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK,
    "nationals": InstagramSourceType.NATIONALS_CHAPTER_PAGE,
    "nationals_page": InstagramSourceType.NATIONALS_CHAPTER_PAGE,
    "nationals_row": InstagramSourceType.NATIONALS_DIRECTORY_ROW,
    "national_directory_row": InstagramSourceType.NATIONALS_DIRECTORY_ROW,
    "search": InstagramSourceType.SEARCH_RESULT_PROFILE,
    "search_result": InstagramSourceType.SEARCH_RESULT_PROFILE,
    "instagram_search": InstagramSourceType.SEARCH_RESULT_PROFILE,
    "search_result_profile": InstagramSourceType.SEARCH_RESULT_PROFILE,
    "generated_handle": InstagramSourceType.GENERATED_HANDLE_SEARCH,
    "generated_handle_search": InstagramSourceType.GENERATED_HANDLE_SEARCH,
    "provenance": InstagramSourceType.PROVENANCE_SUPPORTING_PAGE,
    "supporting_page": InstagramSourceType.PROVENANCE_SUPPORTING_PAGE,
    "source_page": InstagramSourceType.PROVENANCE_SUPPORTING_PAGE,
    "existing_db": InstagramSourceType.EXISTING_DB_VALUE,
}


def _source_type_from_metadata(metadata: dict[str, Any]) -> InstagramSourceType:
    raw = str(metadata.get("evidenceSourceType") or metadata.get("sourceType") or "").strip().lower()
    if raw in _LEGACY_SOURCE_TYPE_ALIASES:
        return _LEGACY_SOURCE_TYPE_ALIASES[raw]
    try:
        return InstagramSourceType(raw)
    except Exception:
        page_scope = str(metadata.get("pageScope") or "").strip().lower()
        contact_specificity = str(metadata.get("contactSpecificity") or "").strip().lower()
        source_url = str(metadata.get("supportingPageUrl") or metadata.get("sourceUrl") or "").strip().lower()
        provider = str(metadata.get("provider") or "").strip().lower()
        query = str(metadata.get("query") or "").strip().lower()

        if (
            contact_specificity == "school_specific"
            or "school" in page_scope
            or ".edu/" in source_url
            or source_url.endswith(".edu")
        ):
            return InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE
        if (
            contact_specificity == "national_specific_to_chapter"
            or "nation" in page_scope
            or "chapter-directory" in source_url
            or "find-a-chapter" in source_url
            or "chapters" in source_url
        ):
            return InstagramSourceType.NATIONALS_CHAPTER_PAGE
        if (
            contact_specificity == "chapter_specific"
            or "chapter_website" in page_scope
            or "chapter_site" in page_scope
        ):
            return InstagramSourceType.VERIFIED_CHAPTER_WEBSITE
        if "instagram.com/" in source_url or provider or query:
            return InstagramSourceType.SEARCH_RESULT_PROFILE
        if source_url:
            return InstagramSourceType.PROVENANCE_SUPPORTING_PAGE
        return InstagramSourceType.SEARCH_RESULT_PROFILE


def candidate_from_chapter_evidence(record: ChapterEvidenceRecord) -> InstagramCandidate | None:
    profile_url = canonicalize_instagram_profile(record.candidate_value)
    handle = extract_instagram_handle(record.candidate_value)
    if not profile_url or not handle:
        return None
    metadata = dict(record.metadata or {})
    return InstagramCandidate(
        handle=handle,
        profile_url=profile_url,
        source_type=_source_type_from_metadata(metadata),
        source_url=record.source_url,
        evidence_url=metadata.get("supportingPageUrl") or record.source_url,
        page_scope=metadata.get("pageScope"),
        contact_specificity=metadata.get("contactSpecificity"),
        source_snippet=record.source_snippet,
        local_container_text=record.source_snippet,
        score=float(record.confidence or 0.0),
        confidence=float(record.confidence or 0.0),
        metadata={"chapterId": record.chapter_id, **metadata},
    )


@dataclass(slots=True)
class InstagramCandidateBank:
    _by_chapter: dict[str, list[InstagramCandidate]] = field(default_factory=lambda: defaultdict(list))

    def add_candidate(self, chapter_id: str, candidate: InstagramCandidate) -> None:
        existing = {item.profile_url for item in self._by_chapter[chapter_id]}
        if candidate.profile_url not in existing:
            self._by_chapter[chapter_id].append(candidate)

    def get_candidates_for_chapter(self, chapter_id: str) -> list[InstagramCandidate]:
        return list(self._by_chapter.get(chapter_id, []))

    def get_candidates_for_fraternity(self, fraternity_slug: str) -> list[InstagramCandidate]:
        matches: list[InstagramCandidate] = []
        for candidates in self._by_chapter.values():
            for candidate in candidates:
                if candidate.metadata.get("fraternitySlug") == fraternity_slug:
                    matches.append(candidate)
        return matches

    def dedupe_by_handle_and_source(self) -> None:
        for chapter_id, candidates in list(self._by_chapter.items()):
            deduped: dict[tuple[str, str], InstagramCandidate] = {}
            for candidate in candidates:
                key = (candidate.handle.lower(), str(candidate.source_url or ""))
                existing = deduped.get(key)
                if existing is None or candidate.confidence > existing.confidence:
                    deduped[key] = candidate
            self._by_chapter[chapter_id] = list(deduped.values())

    def find_cross_chapter_conflicts(self) -> dict[str, list[str]]:
        assignments: dict[str, list[str]] = defaultdict(list)
        for chapter_id, candidates in self._by_chapter.items():
            for candidate in candidates:
                assignments[candidate.handle.lower()].append(chapter_id)
        return {handle: chapter_ids for handle, chapter_ids in assignments.items() if len(set(chapter_ids)) > 1}
