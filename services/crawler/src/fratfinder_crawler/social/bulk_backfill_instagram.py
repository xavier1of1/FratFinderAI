from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from fratfinder_crawler.config import Settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.db.repository import CrawlerRepository
from fratfinder_crawler.field_jobs import FieldJobEngine
from fratfinder_crawler.models import (
    CONTACT_SPECIFICITY_CHAPTER,
    FIELD_JOB_FIND_INSTAGRAM,
    FieldJob,
    PAGE_SCOPE_UNRELATED,
)
from fratfinder_crawler.social.instagram_extractor import extract_instagram_from_verified_chapter_website
from fratfinder_crawler.social.instagram_identity import build_chapter_instagram_identity
from fratfinder_crawler.social.instagram_models import InstagramCandidate, InstagramSourceType
from fratfinder_crawler.social.instagram_normalizer import canonicalize_instagram_profile, extract_instagram_handle
from fratfinder_crawler.social.instagram_resolver import instagram_write_threshold
from fratfinder_crawler.social.instagram_scorer import score_instagram_candidate


FOLLOWUP_KEYWORDS = ("contact", "rush", "recruit", "join", "about", "social")


@dataclass(slots=True)
class BackfillStats:
    mode: str
    chapters_examined: int = 0
    chapters_with_candidates: int = 0
    candidates_scored: int = 0
    accepted_candidates: int = 0
    applied: int = 0
    pending_jobs_completed: int = 0
    fetch_attempted: int = 0
    fetch_succeeded: int = 0
    fetch_failed: int = 0

    def as_dict(self) -> dict[str, int | str]:
        return {
            "mode": self.mode,
            "chaptersExamined": self.chapters_examined,
            "chaptersWithCandidates": self.chapters_with_candidates,
            "candidatesScored": self.candidates_scored,
            "acceptedCandidates": self.accepted_candidates,
            "applied": self.applied,
            "pendingJobsCompleted": self.pending_jobs_completed,
            "fetchAttempted": self.fetch_attempted,
            "fetchSucceeded": self.fetch_succeeded,
            "fetchFailed": self.fetch_failed,
        }


def _host(url: str | None) -> str:
    try:
        return (urlparse(str(url or "")).netloc or "").lower().removeprefix("www.")
    except Exception:
        return ""


def _chapter_coverage(repository: CrawlerRepository) -> dict[str, int]:
    with repository._connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE COALESCE(chapter_status, '') = 'active')::int AS active_total,
                COUNT(*) FILTER (
                    WHERE COALESCE(chapter_status, '') = 'active'
                      AND COALESCE(instagram_url, '') <> ''
                )::int AS active_with_instagram
            FROM chapters
            """
        )
        row = cursor.fetchone() or {}
    return {
        "activeTotal": int(row.get("active_total") or 0),
        "activeWithInstagram": int(row.get("active_with_instagram") or 0),
    }


def _load_existing_assignments(repository: CrawlerRepository) -> dict[str, list[str]]:
    assignments: dict[str, list[str]] = defaultdict(list)
    with repository._connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id::text AS chapter_id, instagram_url
            FROM chapters
            WHERE COALESCE(instagram_url, '') <> ''
            """
        )
        for row in cursor.fetchall():
            normalized = canonicalize_instagram_profile(row["instagram_url"])
            if normalized:
                assignments[normalized].append(str(row["chapter_id"]))
    return assignments


def _build_identity(row: dict[str, Any]):
    return build_chapter_instagram_identity(
        fraternity_name=row.get("fraternity_name"),
        fraternity_slug=row.get("fraternity_slug"),
        school_name=row.get("university_name"),
        chapter_name=row.get("chapter_name"),
        city=row.get("city"),
        state=row.get("state"),
    )


def _candidate_from_provenance_row(row: dict[str, Any]) -> InstagramCandidate | None:
    normalized = canonicalize_instagram_profile(row.get("field_value"))
    handle = extract_instagram_handle(row.get("field_value"))
    if not normalized or not handle:
        return None

    source_url = str(row.get("source_url") or "")
    source_host = _host(source_url)
    website_host = _host(row.get("website_url"))

    if website_host and source_host and website_host == source_host:
        source_type = InstagramSourceType.VERIFIED_CHAPTER_WEBSITE
        contact_specificity = "chapter_specific"
        page_scope = "chapter_website"
    elif source_host.endswith(".edu"):
        source_type = InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE
        contact_specificity = "school_specific"
        page_scope = "school_affiliation_page"
    elif source_host == "instagram.com":
        source_type = InstagramSourceType.PROVENANCE_SUPPORTING_PAGE
        contact_specificity = "chapter_specific"
        page_scope = "instagram_profile"
    else:
        source_type = InstagramSourceType.PROVENANCE_SUPPORTING_PAGE
        contact_specificity = "national_specific_to_chapter"
        page_scope = "nationals_chapter_page"

    return InstagramCandidate(
        handle=handle,
        profile_url=normalized,
        source_type=source_type,
        source_url=source_url or None,
        evidence_url=source_url or None,
        page_scope=page_scope,
        contact_specificity=contact_specificity,
        source_snippet=str(row.get("source_snippet") or "")[:400] or None,
        surrounding_text=str(row.get("source_snippet") or "")[:400] or None,
        local_container_text=str(row.get("source_snippet") or "")[:800] or None,
        local_container_kind="chapter_provenance",
    )


def _followup_links(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    base_host = _host(base_url)
    links: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        label = " ".join(anchor.stripped_strings).lower()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if _host(absolute) != base_host:
            continue
        if not any(keyword in (absolute.lower() + " " + label) for keyword in FOLLOWUP_KEYWORDS):
            continue
        normalized = absolute.rstrip("/")
        if normalized in seen:
            continue
        seen.add(normalized)
        links.append(absolute)
        if len(links) >= 2:
            break
    return links


def _website_candidates(
    session: requests.Session,
    row: dict[str, Any],
    stats: BackfillStats,
) -> list[InstagramCandidate]:
    website_url = str(row.get("website_url") or "")
    if not website_url:
        return []

    def fetch(url: str) -> tuple[requests.Response | None, BeautifulSoup | None]:
        stats.fetch_attempted += 1
        try:
            response = session.get(url, timeout=10, allow_redirects=True)
            if response.status_code >= 400 or not response.text:
                stats.fetch_failed += 1
                return None, None
        except requests.RequestException:
            stats.fetch_failed += 1
            return None, None
        stats.fetch_succeeded += 1
        return response, BeautifulSoup(response.text, "html.parser")

    candidates: list[InstagramCandidate] = []
    homepage, homepage_soup = fetch(website_url)
    if homepage is None or homepage_soup is None:
        return []

    def extract_from_response(response: requests.Response, soup: BeautifulSoup, kind: str) -> None:
        candidates.extend(
            extract_instagram_from_verified_chapter_website(
                text=" ".join(soup.stripped_strings),
                links=[href.strip() for href in (node.get("href") for node in soup.select("a[href]")) if href and href.strip()],
                html=response.text,
                source_url=str(response.url),
                page_scope="chapter_website",
                contact_specificity="chapter_specific",
                source_title=soup.title.get_text(" ", strip=True) if soup.title else None,
                source_snippet=None,
                local_container_kind=kind,
            )
        )

    extract_from_response(homepage, homepage_soup, "homepage")
    for link in _followup_links(str(homepage.url), homepage.text):
        followup, followup_soup = fetch(link)
        if followup is None or followup_soup is None:
            continue
        extract_from_response(followup, followup_soup, "followup_page")
    return candidates


def _apply_best_candidates(
    repository: CrawlerRepository,
    rows_by_chapter: dict[str, list[dict[str, Any]]],
    *,
    existing_assignments: dict[str, list[str]],
    candidate_builder,
    close_pending_jobs: bool,
    dry_run: bool,
    provenance_threshold_override: float | None,
    stats: BackfillStats,
) -> None:
    for chapter_id, rows in rows_by_chapter.items():
        stats.chapters_examined += 1
        identity = _build_identity(rows[0])
        candidates: list[InstagramCandidate] = []
        for row in rows:
            built = candidate_builder(row)
            if built is None:
                continue
            duplicates = [assigned for assigned in existing_assignments.get(built.profile_url, []) if assigned != chapter_id]
            if duplicates:
                built.already_assigned_to_other_chapter_ids = duplicates
            scored = score_instagram_candidate(built, identity)
            stats.candidates_scored += 1
            candidates.append(scored)
        if candidates:
            stats.chapters_with_candidates += 1
        best: InstagramCandidate | None = None
        for candidate in sorted(candidates, key=lambda item: item.confidence, reverse=True):
            threshold = instagram_write_threshold(candidate)
            if provenance_threshold_override is not None and candidate.source_type == InstagramSourceType.PROVENANCE_SUPPORTING_PAGE:
                threshold = provenance_threshold_override
            if not candidate.reject_reasons and candidate.confidence >= threshold:
                best = candidate
                break
        if best is None:
            continue
        stats.accepted_candidates += 1
        if dry_run:
            continue
        row = rows[0]
        applied = repository.apply_instagram_resolution(
            chapter_id=chapter_id,
            chapter_slug=row["chapter_slug"],
            fraternity_slug=row["fraternity_slug"],
            source_slug=row.get("source_slug"),
            crawl_run_id=row.get("crawl_run_id"),
            request_id=row.get("request_id"),
            instagram_url=best.profile_url,
            confidence=best.confidence,
            source_url=best.evidence_url or best.source_url,
            source_snippet=best.source_snippet or best.local_container_text or best.handle,
            reason_code="accepted_bulk_provenance_backfill" if stats.mode == "provenance" else "accepted_bulk_chapter_website_instagram",
            page_scope=best.page_scope,
            contact_specificity=best.contact_specificity,
            source_type=best.source_type.value,
            decision_stage=f"bulk_{stats.mode}_backfill",
            allow_replace=False,
        )
        if not applied:
            continue
        stats.applied += 1
        existing_assignments[best.profile_url].append(chapter_id)
        if close_pending_jobs:
            stats.pending_jobs_completed += repository.complete_pending_field_jobs_for_chapter(
                chapter_id=chapter_id,
                reason_code="resolved_by_bulk_instagram_backfill",
                status="updated",
                chapter_updates={"instagram_url": best.profile_url},
                field_states={"instagram_url": "found"},
                field_names=["find_instagram"],
            )


def _group_rows(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["chapter_id"])].append(row)
    return grouped


def _fetch_provenance_rows(repository: CrawlerRepository, limit: int | None) -> list[dict[str, Any]]:
    with repository._connection.cursor() as cursor:
        cursor.execute(
            f"""
            WITH active_missing AS (
                SELECT
                    c.id,
                    c.slug AS chapter_slug,
                    c.name AS chapter_name,
                    c.university_name,
                    c.city,
                    c.state,
                    c.website_url,
                    f.slug AS fraternity_slug,
                    f.name AS fraternity_name
                FROM chapters c
                JOIN fraternities f ON f.id = c.fraternity_id
                WHERE COALESCE(c.chapter_status, '') = 'active'
                  AND COALESCE(c.instagram_url, '') = ''
            )
            SELECT DISTINCT ON (am.id, cp.field_value)
                am.id::text AS chapter_id,
                am.chapter_slug,
                am.chapter_name,
                am.university_name,
                am.city,
                am.state,
                am.website_url,
                am.fraternity_slug,
                am.fraternity_name,
                cp.field_value,
                cp.source_url,
                cp.source_snippet,
                cp.confidence,
                cp.crawl_run_id,
                NULL::text AS request_id,
                s.slug AS source_slug
            FROM active_missing am
            JOIN chapter_provenance cp
              ON cp.chapter_id = am.id
             AND cp.field_name = 'instagram_url'
            LEFT JOIN crawl_runs cr ON cr.id = cp.crawl_run_id
            LEFT JOIN sources s ON s.id = cr.source_id
            WHERE COALESCE(cp.field_value, '') <> ''
            ORDER BY am.id, cp.field_value, cp.confidence DESC NULLS LAST, cp.created_at DESC NULLS LAST
            {f"LIMIT {int(limit)}" if limit else ""}
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def _fetch_website_rows(repository: CrawlerRepository, limit: int | None) -> list[dict[str, Any]]:
    with repository._connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                c.id::text AS chapter_id,
                c.slug AS chapter_slug,
                c.name AS chapter_name,
                c.university_name,
                c.city,
                c.state,
                c.website_url,
                f.slug AS fraternity_slug,
                f.name AS fraternity_name,
                NULL::text AS source_slug,
                NULL::int AS crawl_run_id,
                NULL::text AS request_id
            FROM chapters c
            JOIN fraternities f ON f.id = c.fraternity_id
            WHERE COALESCE(c.chapter_status, '') = 'active'
              AND COALESCE(c.instagram_url, '') = ''
              AND COALESCE(c.website_url, '') ~* '^https?://'
              AND COALESCE(c.field_states ->> 'website_url', '') = 'found'
            ORDER BY c.updated_at DESC NULLS LAST
            {f"LIMIT {int(limit)}" if limit else ""}
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def _fetch_probe_rows(repository: CrawlerRepository, limit: int | None) -> list[dict[str, Any]]:
    with repository._connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                c.id::text AS chapter_id,
                c.slug AS chapter_slug,
                c.name AS chapter_name,
                c.university_name,
                c.city,
                c.state,
                c.website_url,
                c.chapter_status,
                c.field_states,
                f.slug AS fraternity_slug,
                f.name AS fraternity_name,
                source_info.source_slug,
                source_info.base_url AS source_base_url,
                latest_run.crawl_run_id,
                NULL::text AS request_id
            FROM chapters c
            JOIN fraternities f ON f.id = c.fraternity_id
            LEFT JOIN LATERAL (
                SELECT
                    s.slug AS source_slug,
                    s.base_url
                FROM sources s
                WHERE s.fraternity_id = c.fraternity_id
                  AND COALESCE(s.active, TRUE)
                ORDER BY s.id DESC
                LIMIT 1
            ) source_info ON TRUE
            LEFT JOIN LATERAL (
                SELECT cr.id AS crawl_run_id
                FROM crawl_runs cr
                JOIN sources s2 ON s2.id = cr.source_id
                WHERE s2.fraternity_id = c.fraternity_id
                ORDER BY cr.id DESC
                LIMIT 1
            ) latest_run ON TRUE
            WHERE COALESCE(c.chapter_status, '') = 'active'
              AND COALESCE(c.instagram_url, '') = ''
            ORDER BY
                CASE WHEN COALESCE(c.website_url, '') ~* '^https?://' THEN 0 ELSE 1 END,
                c.updated_at DESC NULLS LAST,
                c.id DESC
            {f"LIMIT {int(limit)}" if limit else ""}
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def _fetch_duplicate_rows(repository: CrawlerRepository, limit: int | None) -> list[dict[str, Any]]:
    with repository._connection.cursor() as cursor:
        cursor.execute(
            f"""
            WITH active AS (
                SELECT
                    c.id,
                    c.slug,
                    c.name,
                    c.university_name,
                    c.website_url,
                    c.instagram_url,
                    c.chapter_status,
                    c.field_states,
                    c.contact_provenance,
                    c.fraternity_id,
                    f.slug AS fraternity_slug,
                    f.name AS fraternity_name,
                    lower(regexp_replace(COALESCE(c.university_name, ''), '[^a-z0-9]+', '', 'g')) AS school_key
                FROM chapters c
                JOIN fraternities f ON f.id = c.fraternity_id
                WHERE COALESCE(c.chapter_status, '') = 'active'
            ),
            donor_groups AS (
                SELECT
                    fraternity_id,
                    school_key,
                    COUNT(DISTINCT instagram_url) FILTER (WHERE COALESCE(instagram_url, '') <> '') AS distinct_instagrams
                FROM active
                WHERE school_key <> ''
                GROUP BY fraternity_id, school_key
            ),
            donors AS (
                SELECT DISTINCT ON (a.fraternity_id, a.school_key)
                    a.fraternity_id,
                    a.school_key,
                    a.slug AS donor_chapter_slug,
                    a.instagram_url AS donor_instagram_url,
                    COALESCE(a.contact_provenance -> 'instagram_url' ->> 'supportingPageUrl', a.instagram_url) AS donor_source_url
                FROM active a
                JOIN donor_groups dg
                  ON dg.fraternity_id = a.fraternity_id
                 AND dg.school_key = a.school_key
                WHERE dg.distinct_instagrams = 1
                  AND COALESCE(a.instagram_url, '') <> ''
                ORDER BY a.fraternity_id, a.school_key, a.id DESC
            )
            SELECT
                m.id::text AS chapter_id,
                m.slug AS chapter_slug,
                m.name AS chapter_name,
                m.university_name,
                m.website_url,
                m.chapter_status,
                m.field_states,
                m.fraternity_slug,
                m.fraternity_name,
                d.donor_chapter_slug,
                d.donor_instagram_url,
                d.donor_source_url,
                source_info.source_slug,
                source_info.base_url AS source_base_url,
                latest_run.crawl_run_id,
                NULL::text AS request_id
            FROM active m
            JOIN donors d
              ON d.fraternity_id = m.fraternity_id
             AND d.school_key = m.school_key
            LEFT JOIN LATERAL (
                SELECT
                    s.slug AS source_slug,
                    s.base_url
                FROM sources s
                WHERE s.fraternity_id = m.fraternity_id
                  AND COALESCE(s.active, TRUE)
                ORDER BY s.id DESC
                LIMIT 1
            ) source_info ON TRUE
            LEFT JOIN LATERAL (
                SELECT cr.id AS crawl_run_id
                FROM crawl_runs cr
                JOIN sources s2 ON s2.id = cr.source_id
                WHERE s2.fraternity_id = m.fraternity_id
                ORDER BY cr.id DESC
                LIMIT 1
            ) latest_run ON TRUE
            WHERE COALESCE(m.instagram_url, '') = ''
              AND m.school_key <> ''
            ORDER BY m.slug
            {f"LIMIT {int(limit)}" if limit else ""}
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def _probe_job_from_row(row: dict[str, Any]) -> FieldJob:
    payload = {
        "candidateSchoolName": row.get("university_name"),
        "sourceSlug": row.get("source_slug") or row.get("fraternity_slug"),
    }
    return FieldJob(
        id=f"bulk-probe-{row['chapter_id']}",
        chapter_id=str(row["chapter_id"]),
        chapter_slug=str(row["chapter_slug"]),
        chapter_name=str(row["chapter_name"]),
        field_name=FIELD_JOB_FIND_INSTAGRAM,
        payload=payload,
        attempts=0,
        max_attempts=3,
        claim_token="bulk-probe",
        source_base_url=str(row.get("source_base_url") or "") or None,
        website_url=str(row.get("website_url") or "") or None,
        instagram_url=None,
        contact_email=None,
        fraternity_slug=str(row.get("fraternity_slug") or "") or None,
        source_id="bulk-probe",
        source_slug=str(row.get("source_slug") or "") or None,
        university_name=str(row.get("university_name") or "") or None,
        crawl_run_id=int(row["crawl_run_id"]) if row.get("crawl_run_id") is not None else None,
        chapter_status=str(row.get("chapter_status") or "active"),
        field_states=dict(row.get("field_states") or {}),
    )


def _apply_probe_backfill(
    repository: CrawlerRepository,
    rows: list[dict[str, Any]],
    *,
    existing_assignments: dict[str, list[str]],
    close_pending_jobs: bool,
    dry_run: bool,
    settings: Settings,
    stats: BackfillStats,
) -> None:
    engine = FieldJobEngine(
        repository,
        logging.getLogger("bulk_instagram_probe"),
        worker_id="bulk-instagram-probe",
        instagram_max_queries=max(1, settings.crawler_search_instagram_max_queries),
        enable_school_initials=settings.crawler_search_enable_school_initials,
        min_school_initial_length=settings.crawler_search_min_school_initial_length,
        enable_compact_fraternity=settings.crawler_search_enable_compact_fraternity,
        instagram_direct_probe_enabled=True,
        validate_existing_instagram=True,
    )

    for row in rows:
        stats.chapters_examined += 1
        job = _probe_job_from_row(row)
        cache_before = len(getattr(engine, "_search_document_cache", {}))
        matches = engine._probe_instagram_handle_candidates(job)
        cache_after = len(getattr(engine, "_search_document_cache", {}))
        stats.fetch_attempted += max(0, cache_after - cache_before)
        stats.candidates_scored += len(matches)
        if matches:
            stats.chapters_with_candidates += 1
        if not matches:
            continue
        best = max(matches, key=lambda item: item.confidence)
        threshold = engine._found_threshold(job, "instagram_url", best)
        if best.confidence < threshold:
            continue

        normalized = canonicalize_instagram_profile(best.value)
        if not normalized:
            continue
        duplicates = [assigned for assigned in existing_assignments.get(normalized, []) if assigned != row["chapter_id"]]
        if duplicates:
            continue

        stats.accepted_candidates += 1
        if dry_run:
            continue

        applied = repository.apply_instagram_resolution(
            chapter_id=str(row["chapter_id"]),
            chapter_slug=str(row["chapter_slug"]),
            fraternity_slug=str(row.get("fraternity_slug") or "") or None,
            source_slug=str(row.get("source_slug") or "") or None,
            crawl_run_id=int(row["crawl_run_id"]) if row.get("crawl_run_id") is not None else None,
            request_id=str(row.get("request_id") or "") or None,
            instagram_url=normalized,
            confidence=float(best.confidence or 0.0),
            source_url=best.source_url or normalized,
            source_snippet=(best.source_snippet or best.query or normalized)[:400],
            reason_code="accepted_bulk_probe_instagram",
            page_scope=PAGE_SCOPE_UNRELATED,
            contact_specificity=CONTACT_SPECIFICITY_CHAPTER,
            source_type="instagram_probe",
            decision_stage="bulk_probe_backfill",
            allow_replace=False,
        )
        if not applied:
            continue

        stats.applied += 1
        existing_assignments.setdefault(normalized, []).append(str(row["chapter_id"]))
        if close_pending_jobs:
            stats.pending_jobs_completed += repository.complete_pending_field_jobs_for_chapter(
                chapter_id=str(row["chapter_id"]),
                reason_code="resolved_by_bulk_instagram_probe",
                status="updated",
                chapter_updates={"instagram_url": normalized},
                field_states={"instagram_url": "found"},
                field_names=[FIELD_JOB_FIND_INSTAGRAM],
            )


def _apply_duplicate_backfill(
    repository: CrawlerRepository,
    rows: list[dict[str, Any]],
    *,
    existing_assignments: dict[str, list[str]],
    close_pending_jobs: bool,
    dry_run: bool,
    stats: BackfillStats,
) -> None:
    for row in rows:
        stats.chapters_examined += 1
        donor_instagram = canonicalize_instagram_profile(row.get("donor_instagram_url"))
        if not donor_instagram:
            continue
        stats.chapters_with_candidates += 1
        stats.candidates_scored += 1
        stats.accepted_candidates += 1
        if dry_run:
            continue

        source_url = str(row.get("donor_source_url") or donor_instagram) or donor_instagram
        applied = repository.apply_instagram_resolution(
            chapter_id=str(row["chapter_id"]),
            chapter_slug=str(row["chapter_slug"]),
            fraternity_slug=str(row.get("fraternity_slug") or "") or None,
            source_slug=str(row.get("source_slug") or "") or None,
            crawl_run_id=int(row["crawl_run_id"]) if row.get("crawl_run_id") is not None else None,
            request_id=str(row.get("request_id") or "") or None,
            instagram_url=donor_instagram,
            confidence=0.97,
            source_url=source_url,
            source_snippet=f"Propagated from active sibling chapter row {row.get('donor_chapter_slug') or 'unknown'} with the same fraternity and school.",
            reason_code="accepted_same_school_duplicate_instagram",
            page_scope=PAGE_SCOPE_UNRELATED,
            contact_specificity=CONTACT_SPECIFICITY_CHAPTER,
            source_type="duplicate_chapter_row",
            decision_stage="bulk_duplicate_backfill",
            allow_replace=False,
        )
        if not applied:
            continue

        stats.applied += 1
        existing_assignments.setdefault(donor_instagram, []).append(str(row["chapter_id"]))
        if close_pending_jobs:
            stats.pending_jobs_completed += repository.complete_pending_field_jobs_for_chapter(
                chapter_id=str(row["chapter_id"]),
                reason_code="resolved_by_bulk_duplicate_instagram",
                status="updated",
                chapter_updates={"instagram_url": donor_instagram},
                field_states={"instagram_url": "found"},
                field_names=[FIELD_JOB_FIND_INSTAGRAM],
            )


def run_bulk_backfill(
    *,
    settings: Settings,
    modes: list[str],
    limit: int | None,
    dry_run: bool,
    close_pending_jobs: bool,
    provenance_threshold_override: float | None = None,
) -> dict[str, Any]:
    with get_connection(settings) as connection:
        repository = CrawlerRepository(connection)
        before = _chapter_coverage(repository)
        existing_assignments = _load_existing_assignments(repository)
        results: list[dict[str, Any]] = []

        if "provenance" in modes:
            stats = BackfillStats(mode="provenance")
            rows = _fetch_provenance_rows(repository, limit)
            _apply_best_candidates(
                repository,
                _group_rows(rows),
                existing_assignments=existing_assignments,
                candidate_builder=_candidate_from_provenance_row,
                close_pending_jobs=close_pending_jobs and not dry_run,
                dry_run=dry_run,
                provenance_threshold_override=provenance_threshold_override,
                stats=stats,
            )
            results.append(stats.as_dict())

        if "website" in modes:
            session = requests.Session()
            session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FratFinderAI/1.0; +https://example.com)"})
            stats = BackfillStats(mode="website")
            website_rows = _fetch_website_rows(repository, limit)
            grouped_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in website_rows:
                candidates = _website_candidates(session, row, stats)
                payload_rows = []
                for candidate in candidates:
                    payload_rows.append(
                        {
                            **row,
                            "field_value": candidate.profile_url,
                            "source_url": candidate.source_url,
                            "source_snippet": candidate.source_snippet,
                            "source_candidate": candidate,
                        }
                    )
                if payload_rows:
                    grouped_candidates[row["chapter_id"]].extend(payload_rows)

            def from_website_row(row: dict[str, Any]) -> InstagramCandidate | None:
                candidate = row.get("source_candidate")
                return candidate if isinstance(candidate, InstagramCandidate) else None

            _apply_best_candidates(
                repository,
                grouped_candidates,
                existing_assignments=existing_assignments,
                candidate_builder=from_website_row,
                close_pending_jobs=close_pending_jobs and not dry_run,
                dry_run=dry_run,
                provenance_threshold_override=None,
                stats=stats,
            )
            results.append(stats.as_dict())

        if "probe" in modes:
            stats = BackfillStats(mode="probe")
            probe_rows = _fetch_probe_rows(repository, limit)
            _apply_probe_backfill(
                repository,
                probe_rows,
                existing_assignments=existing_assignments,
                close_pending_jobs=close_pending_jobs and not dry_run,
                dry_run=dry_run,
                settings=settings,
                stats=stats,
            )
            results.append(stats.as_dict())

        if "duplicate" in modes:
            stats = BackfillStats(mode="duplicate")
            duplicate_rows = _fetch_duplicate_rows(repository, limit)
            _apply_duplicate_backfill(
                repository,
                duplicate_rows,
                existing_assignments=existing_assignments,
                close_pending_jobs=close_pending_jobs and not dry_run,
                dry_run=dry_run,
                stats=stats,
            )
            results.append(stats.as_dict())

        after = before if dry_run else _chapter_coverage(repository)
        coverage_before = (before["activeWithInstagram"] / before["activeTotal"]) if before["activeTotal"] else 0.0
        coverage_after = (after["activeWithInstagram"] / after["activeTotal"]) if after["activeTotal"] else 0.0
        return {
            "before": before,
            "after": after,
            "coverageBefore": round(coverage_before, 4),
            "coverageAfter": round(coverage_after, 4),
            "modes": results,
            "dryRun": dry_run,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk backfill Instagram URLs from existing trustworthy evidence.")
    parser.add_argument(
        "--mode",
        action="append",
        choices=("provenance", "website", "probe", "duplicate"),
        help="Backfill mode to run. Can be specified more than once. Defaults to provenance.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit per mode.")
    parser.add_argument("--dry-run", action="store_true", help="Score candidates without writing to the database.")
    parser.add_argument(
        "--provenance-threshold-override",
        type=float,
        default=None,
        help="Optional temporary threshold override for provenance-backed Instagram candidates.",
    )
    parser.add_argument(
        "--close-pending-jobs",
        action="store_true",
        help="Mark pending find_instagram jobs done for chapters resolved by the backfill.",
    )
    args = parser.parse_args()

    settings = Settings()
    summary = run_bulk_backfill(
        settings=settings,
        modes=args.mode or ["provenance"],
        limit=args.limit,
        dry_run=bool(args.dry_run),
        close_pending_jobs=bool(args.close_pending_jobs),
        provenance_threshold_override=args.provenance_threshold_override,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
