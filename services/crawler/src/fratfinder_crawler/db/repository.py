from __future__ import annotations

from dataclasses import asdict
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.contracts import ContractValidator
from fratfinder_crawler.models import (
    CrawlMetrics,
    EpochMetric,
    ExistingSourceCandidate,
    FrontierItem,
    FieldJob,
    FIELD_TO_CHAPTER_COLUMN,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    FIELD_JOB_TYPES,
    NormalizedChapter,
    PageObservation,
    ProvenanceRecord,
    RewardEvent,
    ReviewItemCandidate,
    TemplateProfile,
    SourceRecord,
    VerifiedSourceRecord,
)


class CrawlerRepository:
    def __init__(self, connection: psycopg.Connection):
        self._connection = connection
        self._contracts = ContractValidator()

    def load_sources(self, source_slug: str | None = None) -> list[SourceRecord]:
        base_query = """
            SELECT
                s.id,
                s.fraternity_id,
                f.slug AS fraternity_slug,
                s.slug AS source_slug,
                s.source_type,
                s.parser_key,
                s.base_url,
                s.list_path,
                s.metadata
            FROM sources s
            JOIN fraternities f ON f.id = s.fraternity_id
            WHERE s.active = TRUE
        """
        order_clause = """
            ORDER BY s.slug
        """

        with self._connection.cursor() as cursor:
            if source_slug is None:
                cursor.execute(f"{base_query}{order_clause}")
            else:
                cursor.execute(
                    f"{base_query} AND s.slug = %(source_slug)s {order_clause}",
                    {"source_slug": source_slug},
                )
            rows = cursor.fetchall()

        return [
            SourceRecord(
                id=str(row["id"]),
                fraternity_id=str(row["fraternity_id"]),
                fraternity_slug=row["fraternity_slug"],
                source_slug=row["source_slug"],
                source_type=row["source_type"],
                parser_key=row["parser_key"],
                base_url=row["base_url"],
                list_path=row["list_path"],
                metadata=row["metadata"] or {},
            )
            for row in rows
        ]

    def get_verified_source_by_slug(self, fraternity_slug: str) -> VerifiedSourceRecord | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                FROM verified_sources
                WHERE fraternity_slug = %s
                LIMIT 1
                """,
                (fraternity_slug,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return VerifiedSourceRecord(
            fraternity_slug=row["fraternity_slug"],
            fraternity_name=row["fraternity_name"],
            national_url=row["national_url"],
            origin=row["origin"],
            confidence=float(row["confidence"] or 0.0),
            http_status=int(row["http_status"]) if row["http_status"] is not None else None,
            checked_at=row["checked_at"].isoformat() if row.get("checked_at") else None,
            is_active=bool(row["is_active"]),
            metadata=row["metadata"] or {},
        )

    def list_verified_sources(self, limit: int = 200) -> list[VerifiedSourceRecord]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                FROM verified_sources
                ORDER BY checked_at DESC, fraternity_slug ASC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            rows = cursor.fetchall()
        return [
            VerifiedSourceRecord(
                fraternity_slug=row["fraternity_slug"],
                fraternity_name=row["fraternity_name"],
                national_url=row["national_url"],
                origin=row["origin"],
                confidence=float(row["confidence"] or 0.0),
                http_status=int(row["http_status"]) if row["http_status"] is not None else None,
                checked_at=row["checked_at"].isoformat() if row.get("checked_at") else None,
                is_active=bool(row["is_active"]),
                metadata=row["metadata"] or {},
            )
            for row in rows
        ]

    def upsert_verified_source(
        self,
        *,
        fraternity_slug: str,
        fraternity_name: str,
        national_url: str,
        origin: str,
        confidence: float,
        http_status: int | None,
        checked_at: str | None = None,
        is_active: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> VerifiedSourceRecord:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO verified_sources (
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s::timestamptz, NOW()), %s, %s)
                ON CONFLICT (fraternity_slug)
                DO UPDATE SET
                    fraternity_name = EXCLUDED.fraternity_name,
                    national_url = EXCLUDED.national_url,
                    origin = EXCLUDED.origin,
                    confidence = EXCLUDED.confidence,
                    http_status = EXCLUDED.http_status,
                    checked_at = EXCLUDED.checked_at,
                    is_active = EXCLUDED.is_active,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    confidence,
                    http_status,
                    checked_at,
                    is_active,
                    metadata
                """,
                (
                    fraternity_slug,
                    fraternity_name,
                    national_url,
                    origin,
                    max(0.0, min(float(confidence), 0.99)),
                    http_status,
                    checked_at,
                    is_active,
                    Jsonb(metadata or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return VerifiedSourceRecord(
            fraternity_slug=row["fraternity_slug"],
            fraternity_name=row["fraternity_name"],
            national_url=row["national_url"],
            origin=row["origin"],
            confidence=float(row["confidence"] or 0.0),
            http_status=int(row["http_status"]) if row["http_status"] is not None else None,
            checked_at=row["checked_at"].isoformat() if row.get("checked_at") else None,
            is_active=bool(row["is_active"]),
            metadata=row["metadata"] or {},
        )

    def get_existing_source_candidates(self, fraternity_slug: str) -> list[ExistingSourceCandidate]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    s.slug AS source_slug,
                    s.base_url,
                    s.list_path,
                    s.source_type,
                    s.parser_key,
                    s.active,
                    MAX(cr.started_at) FILTER (WHERE cr.status = 'succeeded') AS last_success_at,
                    (
                        ARRAY_REMOVE(
                            ARRAY_AGG(cr.status ORDER BY cr.started_at DESC),
                            NULL
                        )
                    )[1] AS last_run_status
                FROM sources s
                JOIN fraternities f ON f.id = s.fraternity_id
                LEFT JOIN crawl_runs cr ON cr.source_id = s.id
                WHERE f.slug = %s
                GROUP BY s.slug, s.base_url, s.list_path, s.source_type, s.parser_key, s.active, s.updated_at
                ORDER BY
                    s.active DESC,
                    MAX(cr.started_at) FILTER (WHERE cr.status = 'succeeded') DESC NULLS LAST,
                    s.updated_at DESC,
                    s.slug ASC
                """,
                (fraternity_slug,),
            )
            rows = cursor.fetchall()
        candidates: list[ExistingSourceCandidate] = []
        for row in rows:
            list_path = row["list_path"]
            base_url = row["base_url"]
            if isinstance(list_path, str) and list_path.startswith("http"):
                list_url = list_path
            elif isinstance(list_path, str) and list_path:
                list_url = f"{base_url.rstrip('/')}/{list_path.lstrip('/')}"
            else:
                list_url = base_url

            last_status = row["last_run_status"]
            health_confidence = 0.60
            if last_status == "succeeded":
                health_confidence = 0.90
            elif last_status == "partial":
                health_confidence = 0.75
            elif last_status == "failed":
                health_confidence = 0.50

            if not row["active"]:
                health_confidence -= 0.20

            candidates.append(
                ExistingSourceCandidate(
                    source_slug=row["source_slug"],
                    list_url=list_url,
                    base_url=base_url,
                    source_type=row["source_type"],
                    parser_key=row["parser_key"],
                    active=bool(row["active"]),
                    last_run_status=last_status,
                    last_success_at=row["last_success_at"].isoformat() if row["last_success_at"] else None,
                    confidence=max(0.0, min(0.99, health_confidence)),
                )
            )
        return candidates

    def upsert_fraternity(self, slug: str, name: str, nic_affiliated: bool = True) -> tuple[str, str]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fraternities (slug, name, nic_affiliated)
                VALUES (%s, %s, %s)
                ON CONFLICT (slug)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    nic_affiliated = EXCLUDED.nic_affiliated,
                    updated_at = NOW()
                RETURNING id, slug
                """,
                (slug, name, nic_affiliated),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"]), row["slug"]

    def upsert_source(
        self,
        *,
        fraternity_id: str,
        slug: str,
        base_url: str,
        list_path: str | None = None,
        source_type: str = "unsupported",
        parser_key: str = "unsupported",
        active: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO sources (fraternity_id, slug, source_type, parser_key, base_url, list_path, active, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug)
                DO UPDATE SET
                    fraternity_id = EXCLUDED.fraternity_id,
                    source_type = EXCLUDED.source_type,
                    parser_key = EXCLUDED.parser_key,
                    base_url = EXCLUDED.base_url,
                    list_path = EXCLUDED.list_path,
                    active = EXCLUDED.active,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING id, slug
                """,
                (
                    fraternity_id,
                    slug,
                    source_type,
                    parser_key,
                    base_url,
                    list_path,
                    active,
                    Jsonb(metadata or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"]), row["slug"]

    def start_crawl_run(self, source_id: str) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_runs (source_id, status)
                VALUES (%s, 'running')
                RETURNING id
                """,
                (source_id,),
            )
            run_id = int(cursor.fetchone()["id"])
        self._connection.commit()
        return run_id

    def finish_crawl_run(
        self,
        run_id: int,
        status: str,
        metrics: CrawlMetrics,
        last_error: str | None = None,
        *,
        page_analysis: dict[str, Any] | None = None,
        classification: dict[str, Any] | None = None,
        extraction_metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE crawl_runs
                SET
                    status = %(status)s,
                    finished_at = NOW(),
                    pages_processed = %(pages_processed)s,
                    records_seen = %(records_seen)s,
                    records_upserted = %(records_upserted)s,
                    review_items_created = %(review_items_created)s,
                    field_jobs_created = %(field_jobs_created)s,
                    last_error = %(last_error)s,
                    page_analysis = %(page_analysis)s,
                    classification = %(classification)s,
                    extraction_metadata = %(extraction_metadata)s
                WHERE id = %(run_id)s
                """,
                {
                    "run_id": run_id,
                    "status": status,
                    "pages_processed": metrics.pages_processed,
                    "records_seen": metrics.records_seen,
                    "records_upserted": metrics.records_upserted,
                    "review_items_created": metrics.review_items_created,
                    "field_jobs_created": metrics.field_jobs_created,
                    "last_error": last_error,
                    "page_analysis": Jsonb(page_analysis) if page_analysis is not None else None,
                    "classification": Jsonb(classification) if classification is not None else None,
                    "extraction_metadata": Jsonb(extraction_metadata or {}),
                },
            )
        self._connection.commit()

    def upsert_chapter(self, source: SourceRecord, chapter: NormalizedChapter) -> str:
        self._contracts.validate_chapter(
            {
                "fraternitySlug": chapter.fraternity_slug,
                "sourceSlug": chapter.source_slug,
                "externalId": chapter.external_id,
                "slug": chapter.slug,
                "name": chapter.name,
                "universityName": chapter.university_name,
                "city": chapter.city,
                "state": chapter.state,
                "country": chapter.country,
                "websiteUrl": chapter.website_url,
                "chapterStatus": chapter.chapter_status,
                "missingOptionalFields": chapter.missing_optional_fields,
                "fieldStates": chapter.field_states,
            }
        )

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapters (
                    fraternity_id,
                    external_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    chapter_status,
                    field_states,
                    normalized_address,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    %(fraternity_id)s,
                    %(external_id)s,
                    %(slug)s,
                    %(name)s,
                    %(university_name)s,
                    %(city)s,
                    %(state)s,
                    %(country)s,
                    %(website_url)s,
                    %(instagram_url)s,
                    %(contact_email)s,
                    %(chapter_status)s,
                    %(field_states)s,
                    '{}'::jsonb,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (fraternity_id, slug)
                DO UPDATE SET
                    external_id = COALESCE(EXCLUDED.external_id, chapters.external_id),
                    name = EXCLUDED.name,
                    university_name = EXCLUDED.university_name,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    country = EXCLUDED.country,
                    website_url = COALESCE(EXCLUDED.website_url, chapters.website_url),
                    instagram_url = COALESCE(EXCLUDED.instagram_url, chapters.instagram_url),
                    contact_email = COALESCE(EXCLUDED.contact_email, chapters.contact_email),
                    chapter_status = EXCLUDED.chapter_status,
                    field_states = EXCLUDED.field_states,
                    last_seen_at = NOW()
                RETURNING id
                """,
                {
                    "fraternity_id": source.fraternity_id,
                    "external_id": chapter.external_id,
                    "slug": chapter.slug,
                    "name": chapter.name,
                    "university_name": chapter.university_name,
                    "city": chapter.city,
                    "state": chapter.state,
                    "country": chapter.country,
                    "website_url": chapter.website_url,
                    "instagram_url": chapter.instagram_url,
                    "contact_email": chapter.contact_email,
                    "chapter_status": chapter.chapter_status,
                    "field_states": Jsonb(chapter.field_states),
                },
            )
            chapter_id = str(cursor.fetchone()["id"])
        self._connection.commit()
        return chapter_id

    def upsert_chapter_discovery(self, source: SourceRecord, chapter: NormalizedChapter) -> str:
        self._contracts.validate_chapter(
            {
                "fraternitySlug": chapter.fraternity_slug,
                "sourceSlug": chapter.source_slug,
                "externalId": chapter.external_id,
                "slug": chapter.slug,
                "name": chapter.name,
                "universityName": chapter.university_name,
                "city": chapter.city,
                "state": chapter.state,
                "country": chapter.country,
                "websiteUrl": chapter.website_url,
                "chapterStatus": chapter.chapter_status,
                "missingOptionalFields": chapter.missing_optional_fields,
                "fieldStates": chapter.field_states,
            }
        )
        field_states = {key: value for key, value in (chapter.field_states or {}).items() if value == "found"}
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapters (
                    fraternity_id,
                    external_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    chapter_status,
                    field_states,
                    normalized_address,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    %(fraternity_id)s,
                    %(external_id)s,
                    %(slug)s,
                    %(name)s,
                    %(university_name)s,
                    %(city)s,
                    %(state)s,
                    %(country)s,
                    %(website_url)s,
                    %(instagram_url)s,
                    %(contact_email)s,
                    %(chapter_status)s,
                    %(field_states)s,
                    '{}'::jsonb,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (fraternity_id, slug)
                DO UPDATE SET
                    external_id = COALESCE(chapters.external_id, EXCLUDED.external_id),
                    name = COALESCE(chapters.name, EXCLUDED.name),
                    university_name = COALESCE(chapters.university_name, EXCLUDED.university_name),
                    city = COALESCE(chapters.city, EXCLUDED.city),
                    state = COALESCE(chapters.state, EXCLUDED.state),
                    country = COALESCE(chapters.country, EXCLUDED.country),
                    website_url = COALESCE(chapters.website_url, EXCLUDED.website_url),
                    instagram_url = COALESCE(chapters.instagram_url, EXCLUDED.instagram_url),
                    contact_email = COALESCE(chapters.contact_email, EXCLUDED.contact_email),
                    chapter_status = COALESCE(chapters.chapter_status, EXCLUDED.chapter_status),
                    field_states = COALESCE(chapters.field_states, '{}'::jsonb) || EXCLUDED.field_states,
                    last_seen_at = NOW()
                RETURNING id
                """,
                {
                    "fraternity_id": source.fraternity_id,
                    "external_id": chapter.external_id,
                    "slug": chapter.slug,
                    "name": chapter.name,
                    "university_name": chapter.university_name,
                    "city": chapter.city,
                    "state": chapter.state,
                    "country": chapter.country,
                    "website_url": chapter.website_url,
                    "instagram_url": chapter.instagram_url,
                    "contact_email": chapter.contact_email,
                    "chapter_status": chapter.chapter_status,
                    "field_states": Jsonb(field_states),
                },
            )
            chapter_id = str(cursor.fetchone()["id"])
        self._connection.commit()
        return chapter_id

    def insert_provenance(
        self,
        chapter_id: str,
        source_id: str,
        crawl_run_id: int,
        records: list[ProvenanceRecord],
    ) -> None:
        if not records:
            return

        with self._connection.cursor() as cursor:
            for record in records:
                payload = asdict(record)
                self._contracts.validate_provenance(
                    {
                        "sourceSlug": payload["source_slug"],
                        "sourceUrl": payload["source_url"],
                        "fieldName": payload["field_name"],
                        "fieldValue": payload["field_value"],
                        "sourceSnippet": payload["source_snippet"],
                        "confidence": payload["confidence"],
                    }
                )
                cursor.execute(
                    """
                    INSERT INTO chapter_provenance (
                        chapter_id,
                        source_id,
                        crawl_run_id,
                        field_name,
                        field_value,
                        source_url,
                        source_snippet,
                        confidence
                    )
                    VALUES (%(chapter_id)s, %(source_id)s, %(crawl_run_id)s, %(field_name)s, %(field_value)s, %(source_url)s, %(source_snippet)s, %(confidence)s)
                    """,
                    {
                        "chapter_id": chapter_id,
                        "source_id": source_id,
                        "crawl_run_id": crawl_run_id,
                        "field_name": record.field_name,
                        "field_value": record.field_value,
                        "source_url": record.source_url,
                        "source_snippet": record.source_snippet,
                        "confidence": record.confidence,
                    },
                )
        self._connection.commit()

    def create_review_item(self, source_id: str | None, crawl_run_id: int | None, candidate: ReviewItemCandidate, chapter_id: str | None = None) -> None:
        review_payload = {
            "itemType": candidate.item_type,
            "reason": candidate.reason,
            "sourceSlug": candidate.source_slug,
            "chapterSlug": candidate.chapter_slug,
            "payload": candidate.payload,
        }
        extraction_notes = candidate.payload.get("extractionNotes") if isinstance(candidate.payload, dict) else None
        if isinstance(extraction_notes, str) and extraction_notes.strip():
            review_payload["extractionNotes"] = extraction_notes

        self._contracts.validate_review_item(review_payload)

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO review_items (source_id, crawl_run_id, chapter_id, item_type, reason, payload)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    source_id,
                    crawl_run_id,
                    chapter_id,
                    candidate.item_type,
                    candidate.reason,
                    Jsonb(candidate.payload),
                ),
            )
        self._connection.commit()

    def create_field_jobs(
        self,
        chapter_id: str,
        crawl_run_id: int,
        chapter_slug: str,
        source_slug: str,
        missing_fields: list[str],
    ) -> int:
        requested_jobs = {self._normalize_field_job_name(name) for name in missing_fields}
        requested_jobs = {name for name in requested_jobs if name in FIELD_JOB_TYPES}

        if not requested_jobs:
            return 0

        created = 0
        with self._connection.cursor() as cursor:
            for field_name in sorted(requested_jobs):
                if field_name in {FIELD_JOB_FIND_WEBSITE, FIELD_JOB_FIND_INSTAGRAM, FIELD_JOB_FIND_EMAIL}:
                    if self._is_field_already_populated(cursor, chapter_id, field_name):
                        continue

                payload = {
                    "chapterSlug": chapter_slug,
                    "fieldName": field_name,
                    "sourceSlug": source_slug,
                    "payload": {"sourceSlug": source_slug, "chapterSlug": chapter_slug},
                }
                self._contracts.validate_field_job(payload)
                cursor.execute(
                    """
                    INSERT INTO field_jobs (chapter_id, crawl_run_id, field_name, payload)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chapter_id, field_name)
                    WHERE status IN ('queued', 'running')
                    DO NOTHING
                    RETURNING id
                    """,
                    (chapter_id, crawl_run_id, field_name, Jsonb(payload["payload"])),
                )
                if cursor.fetchone() is not None:
                    created += 1
        self._connection.commit()
        return created

    def claim_next_field_job(self, worker_id: str, source_slug: str | None = None, field_name: str | None = None, require_confident_website_for_email: bool = False) -> FieldJob | None:
        source_filter = ""
        field_name_filter = ""
        email_dependency_filter = ""
        params: dict[str, Any] = {"worker_id": worker_id}
        if source_slug is not None:
            source_filter = """
                      AND EXISTS (
                          SELECT 1
                          FROM crawl_runs cr
                          JOIN sources s ON s.id = cr.source_id
                          WHERE cr.id = fj.crawl_run_id
                            AND s.slug = %(source_slug)s
                      )
            """
            params["source_slug"] = source_slug
        if field_name is not None:
            field_name_filter = """
                      AND fj.field_name = %(field_name)s
            """
            params["field_name"] = field_name

        if require_confident_website_for_email:
            email_dependency_filter = """
                      AND (
                          fj.field_name <> 'find_email'
                          OR (
                              c.website_url ~* '^https?://'
                              AND COALESCE(c.field_states->>'website_url', '') NOT IN ('low_confidence', 'missing')
                          )
                      )
            """
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                WITH next_job AS (
                    SELECT
                        fj.id
                    FROM field_jobs fj
                    JOIN chapters c ON c.id = fj.chapter_id
                    WHERE fj.status = 'queued'
                      AND fj.scheduled_at <= NOW()
                      AND fj.attempts < fj.max_attempts
{source_filter}
{field_name_filter}
 {email_dependency_filter}
                    ORDER BY
                        fj.priority DESC,
                        fj.scheduled_at ASC,
                        CASE fj.field_name
                            WHEN 'find_website' THEN 0
                            WHEN 'verify_website' THEN 1
                            WHEN 'find_email' THEN 2
                            WHEN 'find_instagram' THEN 3
                            ELSE 4
                        END ASC,
                        fj.id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                ),
                claimed_job AS (
                    UPDATE field_jobs fj
                    SET
                        status = 'running',
                        claimed_by = %(worker_id)s,
                        claim_token = gen_random_uuid(),
                        started_at = NOW(),
                        finished_at = NULL,
                        attempts = attempts + 1,
                        terminal_failure = FALSE
                    FROM next_job
                    WHERE fj.id = next_job.id
                    RETURNING
                        fj.id,
                        fj.chapter_id,
                        fj.crawl_run_id,
                        fj.field_name,
                        fj.payload,
                        fj.attempts,
                        fj.max_attempts,
                        fj.priority,
                        fj.claim_token
                )
                SELECT
                    cj.id,
                    cj.chapter_id,
                    cj.crawl_run_id,
                    c.slug AS chapter_slug,
                    c.name AS chapter_name,
                    f.slug AS fraternity_slug,
                    s.id AS source_id,
                    s.slug AS source_slug,
                    cj.field_name,
                    cj.payload,
                    cj.attempts,
                    cj.max_attempts,
                    cj.priority,
                    cj.claim_token,
                    c.website_url,
                    c.instagram_url,
                    c.contact_email,
                    c.university_name,
                    c.field_states,
                    s.base_url AS source_base_url,
                    s.list_path AS source_list_path
                FROM claimed_job cj
                JOIN chapters c ON c.id = cj.chapter_id
                JOIN fraternities f ON f.id = c.fraternity_id
                LEFT JOIN crawl_runs cr ON cr.id = cj.crawl_run_id
                LEFT JOIN sources s ON s.id = cr.source_id
                """,
                params,
            )
            row = cursor.fetchone()
            if row is None:
                return None

            payload = dict(row["payload"] or {})
            source_base_url = row["source_base_url"]
            source_list_path = row["source_list_path"]
            if isinstance(source_list_path, str) and source_list_path.startswith("http"):
                payload.setdefault("sourceListUrl", source_list_path)
            elif isinstance(source_list_path, str) and source_list_path and source_base_url:
                payload.setdefault("sourceListUrl", f"{source_base_url.rstrip('/')}/{source_list_path.lstrip('/')}")
            elif source_base_url:
                payload.setdefault("sourceListUrl", source_base_url)

            return FieldJob(
                id=str(row["id"]),
                chapter_id=str(row["chapter_id"]),
                chapter_slug=row["chapter_slug"],
                chapter_name=row["chapter_name"],
                field_name=row["field_name"],
                payload=payload,
                attempts=int(row["attempts"]),
                max_attempts=int(row["max_attempts"]),
                priority=int(row["priority"]),
                claim_token=str(row["claim_token"]),
                source_base_url=source_base_url,
                website_url=row["website_url"],
                instagram_url=row["instagram_url"],
                contact_email=row["contact_email"],
                fraternity_slug=row["fraternity_slug"],
                source_id=str(row["source_id"]) if row["source_id"] is not None else None,
                source_slug=row["source_slug"],
                university_name=row["university_name"],
                crawl_run_id=int(row["crawl_run_id"]) if row["crawl_run_id"] is not None else None,
                field_states=row["field_states"] or {},
            )

    def fetch_provenance_snippets(self, chapter_id: str) -> list[str]:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_snippet
                FROM chapter_provenance
                WHERE chapter_id = %s
                  AND source_snippet IS NOT NULL
                ORDER BY extracted_at DESC
                LIMIT 20
                """,
                (chapter_id,),
            )
            rows = cursor.fetchall()
        return [row["source_snippet"] for row in rows if row["source_snippet"]]

    def has_pending_field_job(self, chapter_id: str, field_name: str) -> bool:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM field_jobs
                WHERE chapter_id = %s
                  AND field_name = %s
                  AND status IN ('queued', 'running')
                  AND attempts < max_attempts
                LIMIT 1
                """,
                (chapter_id, field_name),
            )
            return cursor.fetchone() is not None

    def has_recent_transient_website_failures(self, chapter_id: str, min_failures: int = 2) -> bool:
        threshold = max(1, min_failures)
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM field_jobs
                WHERE chapter_id = %s
                  AND field_name = 'find_website'
                  AND (
                      COALESCE(last_error, '') ILIKE '%%provider or network unavailable%%'
                      OR (
                          CASE
                              WHEN COALESCE(payload->>'transient_provider_failures', '') ~ '^[0-9]+$'
                              THEN (payload->>'transient_provider_failures')::int
                              ELSE 0
                          END
                      ) >= %s
                  )
                  AND attempts >= %s
                LIMIT 1
                """,
                (chapter_id, threshold, threshold),
            )
            return cursor.fetchone() is not None

    def create_field_job_review_item(self, job: FieldJob, candidate: ReviewItemCandidate) -> None:
        source_id: str | None = job.source_id
        if source_id is None and job.crawl_run_id is not None:
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT source_id FROM crawl_runs WHERE id = %s", (job.crawl_run_id,))
                row = cursor.fetchone()
                if row is not None:
                    source_id = str(row["source_id"]) if row["source_id"] is not None else None
        self.create_review_item(source_id, job.crawl_run_id, candidate, chapter_id=job.chapter_id)

    def complete_field_job(
        self,
        job: FieldJob,
        chapter_updates: dict[str, str],
        completed_payload: dict[str, Any],
        field_state_updates: dict[str, str] | None = None,
        provenance_records: list[ProvenanceRecord] | None = None,
    ) -> None:
        field_state_updates = field_state_updates or {}
        provenance_records = provenance_records or []
        with self._connection.transaction(), self._connection.cursor() as cursor:
            self._verify_claim(cursor, job.id, job.claim_token)
            if chapter_updates or field_state_updates:
                cursor.execute(
                    """
                    UPDATE chapters
                    SET
                        website_url = CASE
                            WHEN %(website_url)s::text IS NULL THEN website_url
                            WHEN website_url IS NULL THEN %(website_url)s::text
                            WHEN website_url !~* '^https?://' THEN %(website_url)s::text
                            ELSE website_url
                        END,
                        instagram_url = COALESCE(instagram_url, %(instagram_url)s),
                        contact_email = COALESCE(contact_email, %(contact_email)s),
                        university_name = COALESCE(university_name, %(university_name)s),
                        chapter_status = COALESCE(%(chapter_status)s, chapter_status),
                        field_states = COALESCE(field_states, '{}'::jsonb) || %(field_states)s,
                        updated_at = NOW()
                    WHERE id = %(chapter_id)s
                    """,
                    {
                        "chapter_id": job.chapter_id,
                        "website_url": chapter_updates.get("website_url"),
                        "instagram_url": chapter_updates.get("instagram_url"),
                        "contact_email": chapter_updates.get("contact_email"),
                        "university_name": chapter_updates.get("university_name"),
                        "chapter_status": chapter_updates.get("chapter_status"),
                        "field_states": Jsonb(field_state_updates),
                    },
                )

            for record in provenance_records:
                payload = asdict(record)
                self._contracts.validate_provenance(
                    {
                        "sourceSlug": payload["source_slug"],
                        "sourceUrl": payload["source_url"],
                        "fieldName": payload["field_name"],
                        "fieldValue": payload["field_value"],
                        "sourceSnippet": payload["source_snippet"],
                        "confidence": payload["confidence"],
                    }
                )
                if job.source_id is None or job.crawl_run_id is None:
                    continue
                cursor.execute(
                    """
                    INSERT INTO chapter_provenance (
                        chapter_id,
                        source_id,
                        crawl_run_id,
                        field_name,
                        field_value,
                        source_url,
                        source_snippet,
                        confidence
                    )
                    VALUES (%(chapter_id)s, %(source_id)s, %(crawl_run_id)s, %(field_name)s, %(field_value)s, %(source_url)s, %(source_snippet)s, %(confidence)s)
                    """,
                    {
                        "chapter_id": job.chapter_id,
                        "source_id": job.source_id,
                        "crawl_run_id": job.crawl_run_id,
                        "field_name": record.field_name,
                        "field_value": record.field_value,
                        "source_url": record.source_url,
                        "source_snippet": record.source_snippet,
                        "confidence": record.confidence,
                    },
                )

            cursor.execute(
                """
                UPDATE field_jobs
                SET
                    status = 'done',
                    finished_at = NOW(),
                    last_error = NULL,
                    completed_payload = %s,
                    claim_token = NULL
                WHERE id = %s
                """,
                (Jsonb(completed_payload), job.id),
            )

    def requeue_field_job(
        self,
        job: FieldJob,
        error: str,
        delay_seconds: int,
        preserve_attempt: bool = False,
        payload_patch: dict[str, Any] | None = None,
    ) -> None:
        payload_patch = payload_patch or {}
        with self._connection.transaction(), self._connection.cursor() as cursor:
            self._verify_claim(cursor, job.id, job.claim_token)
            cursor.execute(
                """
                UPDATE field_jobs
                SET
                    status = 'queued',
                    scheduled_at = NOW() + (%s * INTERVAL '1 second'),
                    started_at = NULL,
                    finished_at = NULL,
                    last_error = %s,
                    payload = COALESCE(payload, '{}'::jsonb) || %s,
                    claim_token = NULL,
                    terminal_failure = FALSE,
                    attempts = CASE WHEN %s THEN GREATEST(attempts - 1, 0) ELSE attempts END
                WHERE id = %s
                """,
                (delay_seconds, error, Jsonb(payload_patch), preserve_attempt, job.id),
            )

    def fail_field_job_terminal(self, job: FieldJob, error: str) -> None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            self._verify_claim(cursor, job.id, job.claim_token)
            cursor.execute(
                """
                UPDATE field_jobs
                SET
                    status = 'failed',
                    finished_at = NOW(),
                    last_error = %s,
                    claim_token = NULL,
                    terminal_failure = TRUE
                WHERE id = %s
                """,
                (error, job.id),
            )

    def _verify_claim(self, cursor: psycopg.Cursor, field_job_id: str, claim_token: str) -> None:
        cursor.execute(
            """
            SELECT id
            FROM field_jobs
            WHERE id = %s
              AND status = 'running'
              AND claim_token = %s
            FOR UPDATE
            """,
            (field_job_id, claim_token),
        )
        if cursor.fetchone() is None:
            raise RuntimeError(f"Field job {field_job_id} is no longer claimable for this worker")

    def _is_field_already_populated(self, cursor: psycopg.Cursor, chapter_id: str, field_name: str) -> bool:
        chapter_column = FIELD_TO_CHAPTER_COLUMN.get(field_name)
        if chapter_column is None:
            return False

        cursor.execute(
            f"""
            SELECT {chapter_column}
            FROM chapters
            WHERE id = %s
            """,
            (chapter_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return False

        value = row[chapter_column]
        if value is None:
            return False

        if field_name == FIELD_JOB_FIND_WEBSITE:
            return sanitize_as_website(value) is not None
        if field_name == FIELD_JOB_FIND_INSTAGRAM:
            return sanitize_as_instagram(value) is not None
        if field_name == FIELD_JOB_FIND_EMAIL:
            return sanitize_as_email(value) is not None
        return True

    def _normalize_field_job_name(self, raw_name: str) -> str:
        mapping = {
            "websiteurl": FIELD_JOB_FIND_WEBSITE,
            "website_url": FIELD_JOB_FIND_WEBSITE,
            "find_website": FIELD_JOB_FIND_WEBSITE,
            "verify_website": FIELD_JOB_VERIFY_WEBSITE,
            "instagramurl": FIELD_JOB_FIND_INSTAGRAM,
            "instagram_url": FIELD_JOB_FIND_INSTAGRAM,
            "find_instagram": FIELD_JOB_FIND_INSTAGRAM,
            "email": FIELD_JOB_FIND_EMAIL,
            "contact_email": FIELD_JOB_FIND_EMAIL,
            "find_email": FIELD_JOB_FIND_EMAIL,
            "verify_school_match": FIELD_JOB_VERIFY_SCHOOL,
            "school_match": FIELD_JOB_VERIFY_SCHOOL,
        }
        return mapping.get(raw_name.lower(), raw_name.lower())








    def start_crawl_session(
        self,
        *,
        crawl_run_id: int,
        source_id: str,
        runtime_mode: str,
        seed_urls: list[str],
        budget_config: dict[str, Any],
    ) -> str:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_sessions (crawl_run_id, source_id, runtime_mode, status, seed_urls, budget_config, summary)
                VALUES (%s, %s, %s, 'running', %s, %s, '{}'::jsonb)
                RETURNING id
                """,
                (crawl_run_id, source_id, runtime_mode, Jsonb(seed_urls), Jsonb(budget_config)),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row["id"])

    def finish_crawl_session(
        self,
        crawl_session_id: str,
        *,
        status: str,
        stop_reason: str | None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE crawl_sessions
                SET
                    status = %s,
                    stop_reason = %s,
                    summary = COALESCE(%s, summary),
                    finished_at = NOW()
                WHERE id = %s
                """,
                (status, stop_reason, Jsonb(summary or {}), crawl_session_id),
            )
        self._connection.commit()

    def load_recent_crawl_session(self, crawl_run_id: int) -> dict[str, Any] | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, crawl_run_id, source_id, runtime_mode, status, seed_urls, budget_config, stop_reason, summary
                FROM crawl_sessions
                WHERE crawl_run_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (crawl_run_id,),
            )
            row = cursor.fetchone()
        return dict(row) if row is not None else None

    def enqueue_frontier_items(self, crawl_session_id: str, items: list[FrontierItem]) -> int:
        if not items:
            return 0
        created = 0
        with self._connection.cursor() as cursor:
            for item in items:
                cursor.execute(
                    """
                    INSERT INTO crawl_frontier_items (
                        crawl_session_id,
                        url,
                        canonical_url,
                        parent_url,
                        depth,
                        anchor_text,
                        discovered_from,
                        state,
                        score_total,
                        score_components,
                        selected_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (crawl_session_id, canonical_url)
                    DO UPDATE SET
                        score_total = GREATEST(crawl_frontier_items.score_total, EXCLUDED.score_total),
                        score_components = CASE
                            WHEN EXCLUDED.score_total >= crawl_frontier_items.score_total THEN EXCLUDED.score_components
                            ELSE crawl_frontier_items.score_components
                        END,
                        anchor_text = COALESCE(crawl_frontier_items.anchor_text, EXCLUDED.anchor_text),
                        parent_url = COALESCE(crawl_frontier_items.parent_url, EXCLUDED.parent_url),
                        discovered_from = CASE
                            WHEN EXCLUDED.score_total >= crawl_frontier_items.score_total THEN EXCLUDED.discovered_from
                            ELSE crawl_frontier_items.discovered_from
                        END
                    RETURNING id
                    """,
                    (
                        crawl_session_id,
                        item.url,
                        item.canonical_url,
                        item.parent_url,
                        item.depth,
                        item.anchor_text,
                        item.discovered_from,
                        item.state,
                        item.score_total,
                        Jsonb(item.score_components),
                        item.selected_count,
                    ),
                )
                if cursor.fetchone() is not None:
                    created += 1
        self._connection.commit()
        return created

    def pop_next_frontier_item(self, crawl_session_id: str) -> FrontierItem | None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH next_item AS (
                    SELECT id
                    FROM crawl_frontier_items
                    WHERE crawl_session_id = %s
                      AND state = 'queued'
                    ORDER BY score_total DESC, depth ASC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                ),
                claimed AS (
                    UPDATE crawl_frontier_items cfi
                    SET
                        state = 'visited',
                        selected_count = selected_count + 1,
                        updated_at = NOW()
                    FROM next_item
                    WHERE cfi.id = next_item.id
                    RETURNING cfi.*
                )
                SELECT * FROM claimed
                """,
                (crawl_session_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return FrontierItem(
            id=str(row["id"]),
            url=row["url"],
            canonical_url=row["canonical_url"],
            parent_url=row["parent_url"],
            depth=int(row["depth"]),
            anchor_text=row["anchor_text"],
            discovered_from=row["discovered_from"],
            state=row["state"],
            score_total=float(row["score_total"] or 0.0),
            score_components=row["score_components"] or {},
            selected_count=int(row["selected_count"] or 0),
        )

    def count_frontier_items(self, crawl_session_id: str, state: str = 'queued') -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*)::int AS count FROM crawl_frontier_items WHERE crawl_session_id = %s AND state = %s",
                (crawl_session_id, state),
            )
            row = cursor.fetchone()
        return int(row["count"] or 0)

    def append_page_observation(self, observation: PageObservation) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_page_observations (
                    crawl_session_id,
                    url,
                    template_signature,
                    structural_template_signature,
                    http_status,
                    latency_ms,
                    page_analysis,
                    classification,
                    embedded_data,
                    candidate_actions,
                    selected_action,
                    selected_action_score,
                    selected_action_score_components,
                    parent_observation_id,
                    path_depth,
                    risk_score,
                    guardrail_flags,
                    context_bucket,
                    outcome
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    observation.crawl_session_id,
                    observation.url,
                    observation.template_signature,
                    observation.structural_template_signature,
                    observation.http_status,
                    observation.latency_ms,
                    Jsonb(observation.page_analysis),
                    Jsonb(observation.classification),
                    Jsonb(observation.embedded_data),
                    Jsonb(observation.candidate_actions),
                    observation.selected_action,
                    observation.selected_action_score,
                    Jsonb(observation.selected_action_score_components),
                    observation.parent_observation_id,
                    observation.path_depth,
                    observation.risk_score,
                    Jsonb(observation.guardrail_flags),
                    observation.context_bucket,
                    Jsonb(observation.outcome),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return int(row["id"])

    def append_reward_event(self, crawl_session_id: str, page_observation_id: int | None, event: RewardEvent) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_reward_events (
                    crawl_session_id,
                    page_observation_id,
                    action_type,
                    reward_value,
                    reward_components,
                    delayed,
                    reward_stage,
                    attributed_observation_id,
                    discount_factor
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    crawl_session_id,
                    page_observation_id,
                    event.action_type,
                    event.reward_value,
                    Jsonb(event.reward_components),
                    event.delayed,
                    event.reward_stage,
                    event.attributed_observation_id,
                    event.discount_factor,
                ),
            )
        self._connection.commit()

    def get_template_profile(self, template_signature: str, host_family: str) -> TemplateProfile | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    template_signature,
                    host_family,
                    page_role_guess,
                    best_action_family,
                    best_extraction_family,
                    visit_count,
                    chapter_yield,
                    contact_yield,
                    empty_rate,
                    timeout_rate,
                    updated_at
                FROM crawl_template_profiles
                WHERE template_signature = %s
                  AND host_family = %s
                LIMIT 1
                """,
                (template_signature, host_family),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return TemplateProfile(
            template_signature=row["template_signature"],
            host_family=row["host_family"],
            page_role_guess=row["page_role_guess"],
            best_action_family=row["best_action_family"],
            best_extraction_family=row["best_extraction_family"],
            visit_count=int(row["visit_count"] or 0),
            chapter_yield=float(row["chapter_yield"] or 0.0),
            contact_yield=float(row["contact_yield"] or 0.0),
            empty_rate=float(row["empty_rate"] or 0.0),
            timeout_rate=float(row["timeout_rate"] or 0.0),
            updated_at=row["updated_at"].isoformat() if row["updated_at"] else None,
        )

    def upsert_template_profile(
        self,
        *,
        template_signature: str,
        host_family: str,
        page_role_guess: str | None,
        action_type: str,
        extraction_family: str | None,
        chapter_yield: int,
        contact_yield: int,
        timeout: bool,
        empty: bool,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_template_profiles (
                    template_signature,
                    host_family,
                    page_role_guess,
                    best_action_family,
                    best_extraction_family,
                    visit_count,
                    chapter_yield,
                    contact_yield,
                    empty_rate,
                    timeout_rate,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 1, %s, %s, %s, %s, NOW())
                ON CONFLICT (template_signature, host_family)
                DO UPDATE SET
                    page_role_guess = COALESCE(EXCLUDED.page_role_guess, crawl_template_profiles.page_role_guess),
                    best_action_family = CASE
                        WHEN EXCLUDED.chapter_yield + EXCLUDED.contact_yield >= crawl_template_profiles.chapter_yield + crawl_template_profiles.contact_yield
                        THEN EXCLUDED.best_action_family
                        ELSE crawl_template_profiles.best_action_family
                    END,
                    best_extraction_family = CASE
                        WHEN EXCLUDED.chapter_yield + EXCLUDED.contact_yield >= crawl_template_profiles.chapter_yield + crawl_template_profiles.contact_yield
                        THEN EXCLUDED.best_extraction_family
                        ELSE crawl_template_profiles.best_extraction_family
                    END,
                    visit_count = crawl_template_profiles.visit_count + 1,
                    chapter_yield = ((crawl_template_profiles.chapter_yield * crawl_template_profiles.visit_count) + EXCLUDED.chapter_yield)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    contact_yield = ((crawl_template_profiles.contact_yield * crawl_template_profiles.visit_count) + EXCLUDED.contact_yield)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    empty_rate = ((crawl_template_profiles.empty_rate * crawl_template_profiles.visit_count) + EXCLUDED.empty_rate)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    timeout_rate = ((crawl_template_profiles.timeout_rate * crawl_template_profiles.visit_count) + EXCLUDED.timeout_rate)
                        / NULLIF(crawl_template_profiles.visit_count + 1, 0),
                    updated_at = NOW()
                """,
                (
                    template_signature,
                    host_family,
                    page_role_guess,
                    action_type,
                    extraction_family,
                    float(chapter_yield),
                    float(contact_yield),
                    1.0 if empty else 0.0,
                    1.0 if timeout else 0.0,
                ),
            )
        self._connection.commit()

    def save_policy_snapshot(
        self,
        *,
        policy_version: str,
        runtime_mode: str,
        feature_schema_version: str,
        model_payload: dict[str, Any],
        metrics: dict[str, Any],
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_policy_snapshots (
                    policy_version,
                    runtime_mode,
                    feature_schema_version,
                    model_payload,
                    metrics
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (policy_version, runtime_mode, feature_schema_version, Jsonb(model_payload), Jsonb(metrics)),
            )
        self._connection.commit()


    def load_latest_policy_snapshot(
        self,
        *,
        policy_version: str,
        runtime_mode: str | None = None,
    ) -> dict[str, Any] | None:
        with self._connection.cursor() as cursor:
            if runtime_mode is None:
                cursor.execute(
                    """
                    SELECT
                        id,
                        policy_version,
                        runtime_mode,
                        feature_schema_version,
                        model_payload,
                        metrics,
                        created_at
                    FROM crawl_policy_snapshots
                    WHERE policy_version = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (policy_version,),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        id,
                        policy_version,
                        runtime_mode,
                        feature_schema_version,
                        model_payload,
                        metrics,
                        created_at
                    FROM crawl_policy_snapshots
                    WHERE policy_version = %s
                      AND runtime_mode = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (policy_version, runtime_mode),
                )
            row = cursor.fetchone()
        if row is None:
            return None
        payload = dict(row)
        created_at = payload.get("created_at")
        if created_at is not None:
            payload["created_at"] = created_at.isoformat()
        return payload

    def list_crawl_run_metrics(
        self,
        *,
        source_slug: str,
        runtime_mode: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    cr.id,
                    s.slug AS source_slug,
                    cr.started_at,
                    cr.finished_at,
                    cr.status,
                    cr.pages_processed,
                    cr.records_seen,
                    cr.records_upserted,
                    cr.review_items_created,
                    cr.field_jobs_created,
                    EXTRACT(EPOCH FROM (COALESCE(cr.finished_at, NOW()) - cr.started_at)) * 1000 AS duration_ms
                FROM crawl_runs cr
                JOIN sources s ON s.id = cr.source_id
                WHERE s.slug = %s
                  AND COALESCE(cr.extraction_metadata ->> 'runtime_mode', 'legacy') = %s
                ORDER BY cr.started_at DESC
                LIMIT %s
                """,
                (source_slug, runtime_mode, max(1, limit)),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def export_crawl_observations(
        self,
        *,
        source_slug: str | None = None,
        crawl_session_id: str | None = None,
        runtime_mode: str | None = None,
        window_days: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if source_slug is not None:
            params.append(source_slug)
            filters.append("s.slug = %s")
        if crawl_session_id is not None:
            params.append(crawl_session_id)
            filters.append("cpo.crawl_session_id = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("cs.runtime_mode = %s")
        if window_days is not None:
            params.append(max(1, int(window_days)))
            filters.append("cpo.created_at >= NOW() - (%s * INTERVAL '1 day')")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    cpo.id,
                    cpo.crawl_session_id,
                    s.slug AS source_slug,
                    cs.runtime_mode,
                    cpo.url,
                    cpo.template_signature,
                    cpo.structural_template_signature,
                    cpo.parent_observation_id,
                    cpo.path_depth,
                    cpo.risk_score,
                    cpo.guardrail_flags,
                    cpo.context_bucket,
                    cpo.http_status,
                    cpo.latency_ms,
                    cpo.page_analysis,
                    cpo.classification,
                    cpo.embedded_data,
                    cpo.candidate_actions,
                    cpo.selected_action,
                    cpo.selected_action_score,
                    cpo.selected_action_score_components,
                    cpo.outcome,
                    cpo.created_at
                FROM crawl_page_observations cpo
                JOIN crawl_sessions cs ON cs.id = cpo.crawl_session_id
                JOIN sources s ON s.id = cs.source_id
                {where_clause}
                ORDER BY cpo.created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]
    def build_policy_report(self, limit: int = 25) -> dict[str, Any]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    template_signature,
                    host_family,
                    page_role_guess,
                    best_action_family,
                    best_extraction_family,
                    visit_count,
                    chapter_yield,
                    contact_yield,
                    empty_rate,
                    timeout_rate,
                    updated_at
                FROM crawl_template_profiles
                ORDER BY updated_at DESC, visit_count DESC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            templates = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT
                    action_type,
                    COUNT(*)::int AS event_count,
                    COALESCE(AVG(reward_value), 0) AS avg_reward,
                    COALESCE(SUM(reward_value), 0) AS total_reward
                FROM crawl_reward_events
                GROUP BY action_type
                ORDER BY avg_reward DESC, event_count DESC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            action_summary = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT
                    reward_stage,
                    COUNT(*)::int AS event_count,
                    COALESCE(AVG(reward_value), 0) AS avg_reward
                FROM crawl_reward_events
                GROUP BY reward_stage
                ORDER BY event_count DESC
                """
            )
            reward_stage_summary = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT
                    COALESCE(context_bucket, 'unknown') AS context_bucket,
                    COUNT(*)::int AS visit_count,
                    COALESCE(AVG(risk_score), 0) AS avg_risk
                FROM crawl_page_observations
                GROUP BY COALESCE(context_bucket, 'unknown')
                ORDER BY visit_count DESC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            context_summary = [dict(row) for row in cursor.fetchall()]
        return {
            "templateProfiles": templates,
            "actionSummary": action_summary,
            "rewardStageSummary": reward_stage_summary,
            "contextSummary": context_summary,
        }





    def export_reward_events(
        self,
        *,
        source_slug: str | None = None,
        runtime_mode: str | None = None,
        window_days: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if source_slug is not None:
            params.append(source_slug)
            filters.append("s.slug = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("cs.runtime_mode = %s")
        if window_days is not None:
            params.append(max(1, int(window_days)))
            filters.append("cre.created_at >= NOW() - (%s * INTERVAL '1 day')")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    cre.id,
                    cre.crawl_session_id,
                    s.slug AS source_slug,
                    cs.runtime_mode,
                    cre.page_observation_id,
                    cre.action_type,
                    cre.reward_value,
                    cre.reward_components,
                    cre.delayed,
                    cre.reward_stage,
                    cre.attributed_observation_id,
                    cre.discount_factor,
                    cre.created_at
                FROM crawl_reward_events cre
                JOIN crawl_sessions cs ON cs.id = cre.crawl_session_id
                JOIN sources s ON s.id = cs.source_id
                {where_clause}
                ORDER BY cre.created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def summarize_contact_coverage_for_runs(self, *, crawl_run_ids: list[int]) -> dict[str, int]:
        ids = [int(value) for value in crawl_run_ids if value is not None]
        if not ids:
            return {
                "chapters": 0,
                "any_contact": 0,
                "website": 0,
                "email": 0,
                "instagram": 0,
                "all_three": 0,
            }

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH scoped_chapters AS (
                    SELECT DISTINCT cp.chapter_id
                    FROM chapter_provenance cp
                    WHERE cp.crawl_run_id = ANY(%s)
                )
                SELECT
                    COUNT(*)::int AS chapters,
                    COUNT(*) FILTER (
                        WHERE COALESCE(c.website_url, '') <> ''
                           OR COALESCE(c.contact_email, '') <> ''
                           OR COALESCE(c.instagram_url, '') <> ''
                    )::int AS any_contact,
                    COUNT(*) FILTER (WHERE COALESCE(c.website_url, '') <> '')::int AS website,
                    COUNT(*) FILTER (WHERE COALESCE(c.contact_email, '') <> '')::int AS email,
                    COUNT(*) FILTER (WHERE COALESCE(c.instagram_url, '') <> '')::int AS instagram,
                    COUNT(*) FILTER (
                        WHERE COALESCE(c.website_url, '') <> ''
                          AND COALESCE(c.contact_email, '') <> ''
                          AND COALESCE(c.instagram_url, '') <> ''
                    )::int AS all_three
                FROM scoped_chapters sc
                JOIN chapters c ON c.id = sc.chapter_id
                """,
                (ids,),
            )
            row = cursor.fetchone()

        if row is None:
            return {
                "chapters": 0,
                "any_contact": 0,
                "website": 0,
                "email": 0,
                "instagram": 0,
                "all_three": 0,
            }
        return {
            "chapters": int(row["chapters"] or 0),
            "any_contact": int(row["any_contact"] or 0),
            "website": int(row["website"] or 0),
            "email": int(row["email"] or 0),
            "instagram": int(row["instagram"] or 0),
            "all_three": int(row["all_three"] or 0),
        }

    def insert_epoch_metric(self, metric: EpochMetric) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_epoch_metrics (
                    epoch,
                    policy_version,
                    runtime_mode,
                    train_sources,
                    eval_sources,
                    kpis,
                    deltas,
                    slopes,
                    cohort_label,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    metric.epoch,
                    metric.policy_version,
                    metric.runtime_mode,
                    Jsonb(metric.train_sources),
                    Jsonb(metric.eval_sources),
                    Jsonb(metric.kpis),
                    Jsonb(metric.deltas),
                    Jsonb(metric.slopes),
                    metric.cohort_label,
                    Jsonb(metric.metadata),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return int(row["id"])

    def list_epoch_metrics(
        self,
        *,
        policy_version: str | None = None,
        runtime_mode: str | None = None,
        cohort_label: str | None = None,
        limit: int = 120,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if policy_version is not None:
            params.append(policy_version)
            filters.append("policy_version = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("runtime_mode = %s")
        if cohort_label is not None:
            params.append(cohort_label)
            filters.append("cohort_label = %s")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id,
                    epoch,
                    policy_version,
                    runtime_mode,
                    train_sources,
                    eval_sources,
                    kpis,
                    deltas,
                    slopes,
                    cohort_label,
                    metadata,
                    created_at
                FROM crawl_epoch_metrics
                {where_clause}
                ORDER BY created_at DESC, epoch DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def list_policy_snapshots(
        self,
        *,
        policy_version: str | None = None,
        runtime_mode: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if policy_version is not None:
            params.append(policy_version)
            filters.append("policy_version = %s")
        if runtime_mode is not None:
            params.append(runtime_mode)
            filters.append("runtime_mode = %s")
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(max(1, limit))
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id,
                    policy_version,
                    runtime_mode,
                    feature_schema_version,
                    model_payload,
                    metrics,
                    created_at
                FROM crawl_policy_snapshots
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def adaptive_policy_diff(self, snapshot_id_a: int, snapshot_id_b: int) -> dict[str, Any]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, policy_version, runtime_mode, feature_schema_version, model_payload, metrics, created_at
                FROM crawl_policy_snapshots
                WHERE id IN (%s, %s)
                """,
                (snapshot_id_a, snapshot_id_b),
            )
            rows = cursor.fetchall()
        snapshots = {int(row["id"]): dict(row) for row in rows}
        left = snapshots.get(snapshot_id_a)
        right = snapshots.get(snapshot_id_b)
        if left is None or right is None:
            return {"found": False, "left": left, "right": right}

        left_payload = left.get("model_payload") or {}
        right_payload = right.get("model_payload") or {}

        def _extract_actions(payload: dict[str, Any]) -> dict[str, dict[str, float]]:
            actions: dict[str, dict[str, float]] = {}
            for bucket_name in ("navigationActions", "extractionActions", "actions"):
                bucket = payload.get(bucket_name)
                if not isinstance(bucket, dict):
                    continue
                for action, values in bucket.items():
                    if not isinstance(values, dict):
                        continue
                    actions[str(action)] = {
                        "count": float(values.get("count") or 0.0),
                        "avgReward": float(values.get("avgReward") or 0.0),
                    }
            return actions

        left_actions = _extract_actions(left_payload if isinstance(left_payload, dict) else {})
        right_actions = _extract_actions(right_payload if isinstance(right_payload, dict) else {})

        action_keys = sorted(set(left_actions.keys()) | set(right_actions.keys()))
        action_deltas = []
        for key in action_keys:
            left_values = left_actions.get(key, {"count": 0.0, "avgReward": 0.0})
            right_values = right_actions.get(key, {"count": 0.0, "avgReward": 0.0})
            action_deltas.append(
                {
                    "actionType": key,
                    "countDelta": round(right_values["count"] - left_values["count"], 4),
                    "avgRewardDelta": round(right_values["avgReward"] - left_values["avgReward"], 4),
                    "left": left_values,
                    "right": right_values,
                }
            )

        return {
            "found": True,
            "left": left,
            "right": right,
            "actionDeltas": action_deltas,
        }


