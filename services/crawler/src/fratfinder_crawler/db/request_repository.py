from __future__ import annotations

from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from fratfinder_crawler.models import ChapterEvidenceRecord, FraternityCrawlRequestRecord, ProvisionalChapterRecord, RequestGraphRunRecord


class RequestGraphRepository:
    def __init__(self, connection: psycopg.Connection):
        self._connection = connection

    def upsert_worker_process(
        self,
        *,
        worker_id: str,
        workload_lane: str,
        runtime_owner: str,
        status: str = "active",
        lease_seconds: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO worker_processes (
                    worker_id,
                    workload_lane,
                    runtime_owner,
                    hostname,
                    process_id,
                    status,
                    lease_expires_at,
                    last_heartbeat_at,
                    metadata
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    inet_client_addr()::text,
                    pg_backend_pid(),
                    %s,
                    CASE
                      WHEN %s::int IS NULL THEN NULL
                      ELSE NOW() + (%s::int * INTERVAL '1 second')
                    END,
                    NOW(),
                    %s
                )
                ON CONFLICT (worker_id)
                DO UPDATE SET
                    workload_lane = EXCLUDED.workload_lane,
                    runtime_owner = EXCLUDED.runtime_owner,
                    hostname = EXCLUDED.hostname,
                    process_id = EXCLUDED.process_id,
                    status = EXCLUDED.status,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    last_heartbeat_at = NOW(),
                    metadata = EXCLUDED.metadata
                """,
                (
                    worker_id,
                    workload_lane,
                    runtime_owner,
                    status,
                    lease_seconds,
                    lease_seconds,
                    Jsonb(metadata or {}),
                ),
            )
        self._connection.commit()

    def heartbeat_worker_process(self, worker_id: str, lease_seconds: int | None = None) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE worker_processes
                SET
                    last_heartbeat_at = NOW(),
                    lease_expires_at = CASE
                      WHEN %s::int IS NULL THEN lease_expires_at
                      ELSE NOW() + (%s::int * INTERVAL '1 second')
                    END,
                    status = 'active'
                WHERE worker_id = %s
                """,
                (lease_seconds, lease_seconds, worker_id),
            )
        self._connection.commit()

    def stop_worker_process(self, worker_id: str, status: str = "stopped") -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE worker_processes
                SET
                    status = %s,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NOW()
                WHERE worker_id = %s
                """,
                (status, worker_id),
            )
        self._connection.commit()

    def claim_next_due_request(
        self,
        worker_id: str,
        *,
        lease_token: str | None = None,
        lease_seconds: int | None = None,
    ) -> FraternityCrawlRequestRecord | None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH next_request AS (
                    SELECT id
                    FROM fraternity_crawl_requests
                    WHERE status = 'queued'
                      AND scheduled_for <= NOW()
                      AND (
                        runtime_worker_id IS NULL
                        OR runtime_lease_expires_at IS NULL
                        OR runtime_lease_expires_at < NOW()
                      )
                    ORDER BY priority DESC, scheduled_for ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE fraternity_crawl_requests r
                SET
                    status = 'running',
                    started_at = COALESCE(started_at, NOW()),
                    finished_at = NULL,
                    last_error = NULL,
                    runtime_worker_id = %s,
                    runtime_lease_token = %s,
                    runtime_lease_expires_at = CASE
                      WHEN %s::int IS NULL THEN NULL
                      ELSE NOW() + (%s::int * INTERVAL '1 second')
                    END,
                    runtime_last_heartbeat_at = NOW(),
                    updated_at = NOW()
                FROM next_request
                WHERE r.id = next_request.id
                RETURNING
                    r.id,
                    r.fraternity_name,
                    r.fraternity_slug,
                    r.source_slug,
                    r.source_url,
                    r.source_confidence,
                    r.status,
                    r.stage,
                    r.scheduled_for,
                    r.started_at,
                    r.finished_at,
                    r.runtime_worker_id,
                    r.runtime_lease_expires_at,
                    r.runtime_last_heartbeat_at,
                    r.priority,
                    r.config,
                    r.progress,
                    r.last_error,
                    r.created_at,
                    r.updated_at
                """,
                (worker_id, lease_token, lease_seconds, lease_seconds),
            )
            row = cursor.fetchone()
        return self._map_request(row) if row else None

    def get_request(self, request_id: str) -> FraternityCrawlRequestRecord | None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    fraternity_name,
                    fraternity_slug,
                    source_slug,
                    source_url,
                    source_confidence,
                    status,
                    stage,
                    scheduled_for,
                    started_at,
                    finished_at,
                    runtime_worker_id,
                    runtime_lease_expires_at,
                    runtime_last_heartbeat_at,
                    priority,
                    config,
                    progress,
                    last_error,
                    created_at,
                    updated_at
                FROM fraternity_crawl_requests
                WHERE id = %s
                LIMIT 1
                """,
                (request_id,),
            )
            row = cursor.fetchone()
        return self._map_request(row) if row else None

    def update_request(
        self,
        request_id: str,
        *,
        source_slug: str | None = None,
        source_url: str | None = None,
        source_confidence: float | None = None,
        status: str | None = None,
        stage: str | None = None,
        scheduled_for: str | None = None,
        priority: int | None = None,
        config: dict[str, Any] | None = None,
        progress: dict[str, Any] | None = None,
        last_error: str | None = None,
        started_at_now: bool = False,
        finished_at_now: bool = False,
        clear_finished_at: bool = False,
    ) -> None:
        updates: list[str] = []
        values: list[Any] = []

        def push(fragment: str, value: Any) -> None:
            values.append(value)
            updates.append(f"{fragment} = %s")

        if source_slug is not None:
            push("source_slug", source_slug)
        if source_url is not None:
            push("source_url", source_url)
        if source_confidence is not None:
            push("source_confidence", source_confidence)
        if status is not None:
            push("status", status)
        if stage is not None:
            push("stage", stage)
        if scheduled_for is not None:
            push("scheduled_for", scheduled_for)
        if priority is not None:
            push("priority", priority)
        if config is not None:
            push("config", Jsonb(config))
        if progress is not None:
            push("progress", Jsonb(progress))
        if last_error is not None:
            push("last_error", last_error)
        if started_at_now:
            updates.append("started_at = NOW()")
        if finished_at_now:
            updates.append("finished_at = NOW()")
        if clear_finished_at:
            updates.append("finished_at = NULL")
        updates.append("updated_at = NOW()")
        values.append(request_id)

        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE fraternity_crawl_requests
                SET {', '.join(updates)}
                WHERE id = %s
                """,
                values,
            )
        self._connection.commit()

    def append_request_event(self, request_id: str, event_type: str, message: str, payload: dict[str, Any] | None = None) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fraternity_crawl_request_events (request_id, event_type, message, payload)
                VALUES (%s, %s, %s, %s)
                """,
                (request_id, event_type, message, Jsonb(payload or {})),
            )
        self._connection.commit()

    def reconcile_stale_requests(self, max_age_minutes: int = 45) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fraternity_crawl_requests
                SET
                    status = 'failed',
                    stage = 'failed',
                    finished_at = NOW(),
                    last_error = COALESCE(last_error, %s),
                    runtime_worker_id = NULL,
                    runtime_lease_token = NULL,
                    runtime_lease_expires_at = NULL
                WHERE status = 'running'
                  AND (
                    (runtime_lease_expires_at IS NOT NULL AND runtime_lease_expires_at < NOW())
                    OR (runtime_lease_expires_at IS NULL AND updated_at < NOW() - (%s::int * INTERVAL '1 minute'))
                  )
                """,
                (
                    'Fraternity crawl request stalled before completion',
                    max(1, int(max_age_minutes)),
                ),
            )
            count = cursor.rowcount
        self._connection.commit()
        return int(count or 0)

    def heartbeat_request_lease(
        self,
        *,
        request_id: str,
        worker_id: str,
        lease_token: str,
        lease_seconds: int,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fraternity_crawl_requests
                SET
                    runtime_lease_expires_at = NOW() + (%s::int * INTERVAL '1 second'),
                    runtime_last_heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND runtime_worker_id = %s
                  AND runtime_lease_token = %s
                """,
                (max(15, int(lease_seconds)), request_id, worker_id, lease_token),
            )
        self._connection.commit()

    def release_request_lease(self, *, request_id: str, worker_id: str, lease_token: str) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fraternity_crawl_requests
                SET
                    runtime_worker_id = NULL,
                    runtime_lease_token = NULL,
                    runtime_lease_expires_at = NULL
                WHERE id = %s
                  AND runtime_worker_id = %s
                  AND runtime_lease_token = %s
                """,
                (request_id, worker_id, lease_token),
            )
        self._connection.commit()

    def get_latest_crawl_run_for_source(
        self,
        source_slug: str,
        *,
        started_after: str | None = None,
        exclude_run_id: int | None = None,
    ) -> dict[str, Any] | None:
        where_clauses = ["s.slug = %s"]
        params: list[Any] = [source_slug]
        if started_after is not None:
            where_clauses.append("cr.started_at >= %s::timestamptz")
            params.append(started_after)
        if exclude_run_id is not None:
            where_clauses.append("cr.id <> %s")
            params.append(exclude_run_id)
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    cr.id,
                    s.slug AS source_slug,
                    cr.status,
                    cr.started_at,
                    cr.finished_at,
                    cr.pages_processed,
                    cr.records_seen,
                    cr.records_upserted,
                    cr.review_items_created,
                    cr.field_jobs_created,
                    cr.last_error,
                    cr.extraction_metadata,
                    cr.extraction_metadata ->> 'strategy_used' AS strategy_used,
                    cr.extraction_metadata ->> 'runtime_mode' AS runtime_mode,
                    cr.extraction_metadata ->> 'stop_reason' AS stop_reason,
                    COALESCE(cs.session_count, 0) AS crawl_session_count,
                    NULLIF(cr.extraction_metadata ->> 'page_level_confidence', '')::double precision AS page_level_confidence,
                    COALESCE(NULLIF(cr.extraction_metadata ->> 'llm_calls_used', '')::integer, 0) AS llm_calls_used
                FROM crawl_runs cr
                JOIN sources s ON s.id = cr.source_id
                LEFT JOIN (
                    SELECT crawl_run_id, COUNT(*)::int AS session_count
                    FROM crawl_sessions
                    GROUP BY crawl_run_id
                ) cs ON cs.crawl_run_id = cr.id
                WHERE {' AND '.join(where_clauses)}
                ORDER BY cr.started_at DESC
                LIMIT 1
                """,
                params,
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    def get_source_field_job_snapshot(self, source_slug: str) -> list[dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    fj.field_name,
                    COUNT(*) FILTER (WHERE fj.status = 'queued')::int AS queued,
                    COUNT(*) FILTER (WHERE fj.status = 'running')::int AS running,
                    COUNT(*) FILTER (WHERE fj.status = 'done')::int AS done,
                    COUNT(*) FILTER (WHERE fj.status = 'failed')::int AS failed,
                    COUNT(*) FILTER (
                        WHERE fj.status = 'queued'
                          AND COALESCE(fj.queue_state, 'actionable') = 'actionable'
                    )::int AS queued_actionable,
                    COUNT(*) FILTER (
                        WHERE fj.status = 'queued'
                          AND COALESCE(fj.queue_state, 'actionable') = 'deferred'
                    )::int AS queued_deferred,
                    COUNT(*) FILTER (WHERE fj.status = 'done' AND COALESCE(fj.terminal_outcome, '') = 'updated')::int AS done_updated,
                    COUNT(*) FILTER (WHERE fj.status = 'done' AND COALESCE(fj.terminal_outcome, '') = 'review_required')::int AS done_review_required,
                    COUNT(*) FILTER (WHERE fj.status = 'done' AND COALESCE(fj.terminal_outcome, '') = 'terminal_no_signal')::int AS done_terminal_no_signal,
                    COUNT(*) FILTER (WHERE fj.status = 'done' AND COALESCE(fj.terminal_outcome, '') = 'provider_degraded')::int AS done_provider_degraded
                FROM field_jobs fj
                JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
                JOIN sources s ON s.id = cr.source_id
                WHERE s.slug = %s
                  AND fj.field_name IN ('find_website', 'find_email', 'find_instagram')
                GROUP BY fj.field_name
                """,
                (source_slug,),
            )
            rows = cursor.fetchall()
        grouped = {
            'find_website': {'queued': 0, 'running': 0, 'done': 0, 'failed': 0, 'queued_actionable': 0, 'queued_deferred': 0, 'done_updated': 0, 'done_review_required': 0, 'done_terminal_no_signal': 0, 'done_provider_degraded': 0},
            'find_email': {'queued': 0, 'running': 0, 'done': 0, 'failed': 0, 'queued_actionable': 0, 'queued_deferred': 0, 'done_updated': 0, 'done_review_required': 0, 'done_terminal_no_signal': 0, 'done_provider_degraded': 0},
            'find_instagram': {'queued': 0, 'running': 0, 'done': 0, 'failed': 0, 'queued_actionable': 0, 'queued_deferred': 0, 'done_updated': 0, 'done_review_required': 0, 'done_terminal_no_signal': 0, 'done_provider_degraded': 0},
        }
        for row in rows:
            grouped[row['field_name']] = {
                'queued': int(row['queued'] or 0),
                'running': int(row['running'] or 0),
                'done': int(row['done'] or 0),
                'failed': int(row['failed'] or 0),
                'queued_actionable': int(row['queued_actionable'] or 0),
                'queued_deferred': int(row['queued_deferred'] or 0),
                'done_updated': int(row['done_updated'] or 0),
                'done_review_required': int(row['done_review_required'] or 0),
                'done_terminal_no_signal': int(row['done_terminal_no_signal'] or 0),
                'done_provider_degraded': int(row['done_provider_degraded'] or 0),
            }
        return [
            {'field': field_name, **counts}
            for field_name, counts in grouped.items()
        ]

    def start_request_graph_run(
        self,
        *,
        request_id: str,
        worker_id: str,
        runtime_mode: str,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO request_graph_runs (request_id, worker_id, runtime_mode, status, metadata, summary)
                VALUES (%s, %s, %s, 'running', %s, '{}'::jsonb)
                RETURNING id
                """,
                (request_id, worker_id, runtime_mode, Jsonb(metadata or {})),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return int(row['id'])

    def append_request_graph_event(
        self,
        *,
        run_id: int,
        request_id: str,
        node_name: str,
        phase: str,
        status: str,
        latency_ms: int,
        metrics_delta: dict[str, Any] | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO request_graph_events (
                    run_id,
                    request_id,
                    node_name,
                    phase,
                    status,
                    latency_ms,
                    metrics_delta,
                    diagnostics
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    request_id,
                    node_name,
                    phase,
                    status,
                    max(0, int(latency_ms)),
                    Jsonb(metrics_delta or {}),
                    Jsonb(diagnostics or {}),
                ),
            )
        self._connection.commit()

    def upsert_request_graph_checkpoint(
        self,
        *,
        run_id: int,
        request_id: str,
        node_name: str,
        state: dict[str, Any],
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO request_graph_checkpoints (run_id, request_id, node_name, state)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, node_name)
                DO UPDATE SET
                    state = EXCLUDED.state,
                    updated_at = NOW()
                """,
                (run_id, request_id, node_name, Jsonb(state)),
            )
        self._connection.commit()

    def finish_request_graph_run(
        self,
        run_id: int,
        *,
        status: str,
        summary: dict[str, Any],
        error_message: str | None = None,
        active_node: str | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE request_graph_runs
                SET
                    status = %s,
                    active_node = %s,
                    summary = %s,
                    error_message = %s,
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (status, active_node, Jsonb(summary), error_message, run_id),
            )
        self._connection.commit()

    def touch_request_graph_run(self, run_id: int, *, active_node: str | None = None, summary: dict[str, Any] | None = None) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE request_graph_runs
                SET
                    active_node = COALESCE(%s, active_node),
                    summary = CASE WHEN %s IS NULL THEN summary ELSE %s END,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    active_node,
                    Jsonb(summary) if summary is not None else None,
                    Jsonb(summary) if summary is not None else None,
                    run_id,
                ),
            )
        self._connection.commit()

    def insert_chapter_evidence(self, record: ChapterEvidenceRecord) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO chapter_evidence (
                    chapter_id,
                    chapter_slug,
                    fraternity_slug,
                    source_slug,
                    request_id,
                    crawl_run_id,
                    field_name,
                    candidate_value,
                    confidence,
                    trust_tier,
                    evidence_status,
                    source_url,
                    source_snippet,
                    provider,
                    query,
                    related_website_url,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    record.chapter_id,
                    record.chapter_slug,
                    record.fraternity_slug,
                    record.source_slug,
                    record.request_id,
                    record.crawl_run_id,
                    record.field_name,
                    record.candidate_value,
                    record.confidence,
                    record.trust_tier,
                    record.evidence_status,
                    record.source_url,
                    record.source_snippet,
                    record.provider,
                    record.query,
                    record.related_website_url,
                    Jsonb(record.metadata),
                ),
            )
        self._connection.commit()

    def upsert_provisional_chapter(
        self,
        *,
        fraternity_id: str,
        slug: str,
        name: str,
        status: str = 'provisional',
        source_id: str | None = None,
        request_id: str | None = None,
        university_name: str | None = None,
        city: str | None = None,
        state: str | None = None,
        country: str = 'USA',
        website_url: str | None = None,
        instagram_url: str | None = None,
        contact_email: str | None = None,
        promotion_reason: str | None = None,
        promoted_chapter_id: str | None = None,
        evidence_payload: dict[str, Any] | None = None,
    ) -> str:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO provisional_chapters (
                    fraternity_id,
                    source_id,
                    request_id,
                    promoted_chapter_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    status,
                    promotion_reason,
                    evidence_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fraternity_id, slug)
                DO UPDATE SET
                    source_id = COALESCE(EXCLUDED.source_id, provisional_chapters.source_id),
                    request_id = COALESCE(EXCLUDED.request_id, provisional_chapters.request_id),
                    promoted_chapter_id = COALESCE(EXCLUDED.promoted_chapter_id, provisional_chapters.promoted_chapter_id),
                    name = EXCLUDED.name,
                    university_name = COALESCE(EXCLUDED.university_name, provisional_chapters.university_name),
                    city = COALESCE(EXCLUDED.city, provisional_chapters.city),
                    state = COALESCE(EXCLUDED.state, provisional_chapters.state),
                    country = COALESCE(EXCLUDED.country, provisional_chapters.country),
                    website_url = COALESCE(EXCLUDED.website_url, provisional_chapters.website_url),
                    instagram_url = COALESCE(EXCLUDED.instagram_url, provisional_chapters.instagram_url),
                    contact_email = COALESCE(EXCLUDED.contact_email, provisional_chapters.contact_email),
                    status = EXCLUDED.status,
                    promotion_reason = COALESCE(EXCLUDED.promotion_reason, provisional_chapters.promotion_reason),
                    evidence_payload = COALESCE(provisional_chapters.evidence_payload, '{}'::jsonb) || EXCLUDED.evidence_payload,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    fraternity_id,
                    source_id,
                    request_id,
                    promoted_chapter_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    status,
                    promotion_reason,
                    Jsonb(evidence_payload or {}),
                ),
            )
            row = cursor.fetchone()
        self._connection.commit()
        return str(row['id'])

    def list_provisional_chapters_for_request(
        self,
        request_id: str,
        *,
        statuses: tuple[str, ...] = ("provisional",),
        limit: int = 200,
    ) -> list[ProvisionalChapterRecord]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id::text AS id,
                    fraternity_id,
                    source_id,
                    request_id,
                    promoted_chapter_id,
                    slug,
                    name,
                    university_name,
                    city,
                    state,
                    country,
                    website_url,
                    instagram_url,
                    contact_email,
                    status,
                    promotion_reason,
                    evidence_payload,
                    created_at::text AS created_at,
                    updated_at::text AS updated_at
                FROM provisional_chapters
                WHERE request_id = %s
                  AND status = ANY(%s)
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (request_id, list(statuses), max(1, limit)),
            )
            rows = cursor.fetchall()
        return [
            ProvisionalChapterRecord(
                id=str(row["id"]),
                fraternity_id=str(row["fraternity_id"]),
                source_id=str(row["source_id"]) if row.get("source_id") else None,
                request_id=str(row["request_id"]) if row.get("request_id") else None,
                promoted_chapter_id=str(row["promoted_chapter_id"]) if row.get("promoted_chapter_id") else None,
                slug=str(row["slug"]),
                name=str(row["name"]),
                university_name=row.get("university_name"),
                city=row.get("city"),
                state=row.get("state"),
                country=row.get("country") or "USA",
                website_url=row.get("website_url"),
                instagram_url=row.get("instagram_url"),
                contact_email=row.get("contact_email"),
                status=str(row["status"]),
                promotion_reason=row.get("promotion_reason"),
                evidence_payload=dict(row.get("evidence_payload") or {}),
                created_at=row.get("created_at"),
                updated_at=row.get("updated_at"),
            )
            for row in rows
        ]

    def update_provisional_chapter_status(
        self,
        provisional_id: str,
        *,
        status: str,
        promotion_reason: str | None = None,
        promoted_chapter_id: str | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE provisional_chapters
                SET
                    status = %s,
                    promotion_reason = COALESCE(%s, promotion_reason),
                    promoted_chapter_id = COALESCE(%s, promoted_chapter_id),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (status, promotion_reason, promoted_chapter_id, provisional_id),
            )
        self._connection.commit()

    def insert_provider_health_snapshot(
        self,
        *,
        request_id: str | None,
        source_slug: str | None,
        provider: str,
        healthy: bool,
        success_rate: float | None,
        probe_count: int,
        payload: dict[str, Any] | None = None,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO provider_health_snapshots (
                    request_id,
                    source_slug,
                    provider,
                    healthy,
                    success_rate,
                    probe_count,
                    payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    request_id,
                    source_slug,
                    provider,
                    healthy,
                    success_rate,
                    max(0, int(probe_count)),
                    Jsonb(payload or {}),
                ),
            )
        self._connection.commit()

    def _map_request(self, row: dict[str, Any]) -> FraternityCrawlRequestRecord:
        return FraternityCrawlRequestRecord(
            id=str(row['id']),
            fraternity_name=row['fraternity_name'],
            fraternity_slug=row['fraternity_slug'],
            source_slug=row['source_slug'],
            source_url=row['source_url'],
            source_confidence=float(row['source_confidence']) if row['source_confidence'] is not None else None,
            status=row['status'],
            stage=row['stage'],
            scheduled_for=row['scheduled_for'].isoformat(),
            started_at=row['started_at'].isoformat() if row.get('started_at') else None,
            finished_at=row['finished_at'].isoformat() if row.get('finished_at') else None,
            priority=int(row['priority'] or 0),
            runtime_worker_id=row.get('runtime_worker_id'),
            runtime_lease_expires_at=row['runtime_lease_expires_at'].isoformat() if row.get('runtime_lease_expires_at') else None,
            runtime_last_heartbeat_at=row['runtime_last_heartbeat_at'].isoformat() if row.get('runtime_last_heartbeat_at') else None,
            config=row['config'] or {},
            progress=row['progress'] or {},
            last_error=row['last_error'],
            created_at=row['created_at'].isoformat() if row.get('created_at') else None,
            updated_at=row['updated_at'].isoformat() if row.get('updated_at') else None,
        )
