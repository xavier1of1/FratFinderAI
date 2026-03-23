from __future__ import annotations

from dataclasses import asdict
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from fratfinder_crawler.contracts import ContractValidator
from fratfinder_crawler.models import (
    CrawlMetrics,
    FieldJob,
    FIELD_TO_CHAPTER_COLUMN,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    FIELD_JOB_TYPES,
    NormalizedChapter,
    ProvenanceRecord,
    ReviewItemCandidate,
    SourceRecord,
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

    def create_review_item(self, source_id: str, crawl_run_id: int, candidate: ReviewItemCandidate, chapter_id: str | None = None) -> None:
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
                    "payload": {},
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
                    (chapter_id, crawl_run_id, field_name, Jsonb({})),
                )
                if cursor.fetchone() is not None:
                    created += 1
        self._connection.commit()
        return created

    def claim_next_field_job(self, worker_id: str, source_slug: str | None = None) -> FieldJob | None:
        source_filter = ""
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

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                WITH next_job AS (
                    SELECT
                        fj.id
                    FROM field_jobs fj
                    WHERE fj.status = 'queued'
                      AND fj.scheduled_at <= NOW()
                      AND fj.attempts < fj.max_attempts
{source_filter}
                    ORDER BY fj.scheduled_at ASC, fj.id ASC
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
                        fj.claim_token
                )
                SELECT
                    cj.id,
                    cj.chapter_id,
                    cj.crawl_run_id,
                    c.slug AS chapter_slug,
                    cj.field_name,
                    cj.payload,
                    cj.attempts,
                    cj.max_attempts,
                    cj.claim_token,
                    c.website_url,
                    c.instagram_url,
                    c.contact_email,
                    c.university_name,
                    c.field_states,
                    s.base_url AS source_base_url
                FROM claimed_job cj
                JOIN chapters c ON c.id = cj.chapter_id
                LEFT JOIN crawl_runs cr ON cr.id = cj.crawl_run_id
                LEFT JOIN sources s ON s.id = cr.source_id
                """,
                params,
            )
            row = cursor.fetchone()
            if row is None:
                return None

            return FieldJob(
                id=str(row["id"]),
                chapter_id=str(row["chapter_id"]),
                chapter_slug=row["chapter_slug"],
                field_name=row["field_name"],
                payload=row["payload"] or {},
                attempts=int(row["attempts"]),
                max_attempts=int(row["max_attempts"]),
                claim_token=str(row["claim_token"]),
                source_base_url=row["source_base_url"],
                website_url=row["website_url"],
                instagram_url=row["instagram_url"],
                contact_email=row["contact_email"],
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

    def create_field_job_review_item(self, job: FieldJob, candidate: ReviewItemCandidate) -> None:
        source_id: str | None = None
        if job.crawl_run_id is not None:
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
    ) -> None:
        field_state_updates = field_state_updates or {}
        with self._connection.transaction(), self._connection.cursor() as cursor:
            self._verify_claim(cursor, job.id, job.claim_token)
            if chapter_updates or field_state_updates:
                cursor.execute(
                    """
                    UPDATE chapters
                    SET
                        website_url = COALESCE(website_url, %(website_url)s),
                        instagram_url = COALESCE(instagram_url, %(instagram_url)s),
                        contact_email = COALESCE(contact_email, %(contact_email)s),
                        university_name = COALESCE(university_name, %(university_name)s),
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
                        "field_states": Jsonb(field_state_updates),
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

    def requeue_field_job(self, job: FieldJob, error: str, delay_seconds: int) -> None:
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
                    claim_token = NULL,
                    terminal_failure = FALSE
                WHERE id = %s
                """,
                (delay_seconds, error, job.id),
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
        return row is not None and row[chapter_column] is not None

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
