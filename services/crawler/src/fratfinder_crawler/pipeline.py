from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from fratfinder_crawler.adapters import AdapterRegistry
from fratfinder_crawler.config import Settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.discovery import discover_source
from fratfinder_crawler.db.repository import CrawlerRepository
from fratfinder_crawler.field_jobs import FieldJobEngine
from fratfinder_crawler.http.client import HttpClient
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.search import SearchClient
from fratfinder_crawler.models import CrawlMetrics
from fratfinder_crawler.orchestration import CrawlOrchestrator

LOGGER = logging.getLogger(__name__)


class CrawlService:
    def __init__(self, settings: Settings):
        self._settings = settings

    def run(self, source_slug: str | None = None) -> dict[str, int]:
        aggregate = CrawlMetrics()

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            sources = repository.load_sources(source_slug=source_slug)
            orchestrator = CrawlOrchestrator(repository, HttpClient(self._settings), AdapterRegistry())

            log_event(
                LOGGER,
                "crawl_batch_started",
                requested_source_slug=source_slug,
                source_count=len(sources),
            )

            for source in sources:
                log_event(LOGGER, "source_crawl_started", source_slug=source.source_slug)
                metrics = orchestrator.run_for_source(source)
                aggregate.pages_processed += metrics.pages_processed
                aggregate.records_seen += metrics.records_seen
                aggregate.records_upserted += metrics.records_upserted
                aggregate.review_items_created += metrics.review_items_created
                aggregate.field_jobs_created += metrics.field_jobs_created
                log_event(
                    LOGGER,
                    "source_crawl_finished",
                    source_slug=source.source_slug,
                    pages_processed=metrics.pages_processed,
                    records_seen=metrics.records_seen,
                    records_upserted=metrics.records_upserted,
                    review_items_created=metrics.review_items_created,
                    field_jobs_created=metrics.field_jobs_created,
                )

        result = {
            "pages_processed": aggregate.pages_processed,
            "records_seen": aggregate.records_seen,
            "records_upserted": aggregate.records_upserted,
            "review_items_created": aggregate.review_items_created,
            "field_jobs_created": aggregate.field_jobs_created,
        }
        log_event(LOGGER, "crawl_batch_finished", **result)
        return result

    def process_field_jobs(
        self,
        limit: int = 25,
        source_slug: str | None = None,
        field_name: str | None = None,
        workers: int | None = None,
    ) -> dict[str, int]:
        worker_limits = _distribute_limit(limit, workers or self._settings.crawler_field_job_max_workers)
        if not worker_limits:
            result = {"processed": 0, "requeued": 0, "failed_terminal": 0}
            log_event(
                LOGGER,
                "field_job_batch_finished",
                limit=limit,
                source_slug=source_slug,
                field_name=field_name,
                workers=0,
                **result,
            )
            return result

        if len(worker_limits) == 1:
            result = self._process_field_job_chunk(
                limit=worker_limits[0],
                source_slug=source_slug,
                field_name=field_name,
                worker_index=1,
                total_workers=1,
            )
            log_event(
                LOGGER,
                "field_job_batch_finished",
                limit=limit,
                source_slug=source_slug,
                field_name=field_name,
                workers=1,
                **result,
            )
            return result

        with ThreadPoolExecutor(max_workers=len(worker_limits), thread_name_prefix="field-job-worker") as executor:
            futures = [
                executor.submit(
                    self._process_field_job_chunk,
                    worker_limit,
                    source_slug,
                    field_name,
                    index,
                    len(worker_limits),
                )
                for index, worker_limit in enumerate(worker_limits, start=1)
            ]

        aggregate = {"processed": 0, "requeued": 0, "failed_terminal": 0}
        for future in futures:
            chunk_result = future.result()
            for key in aggregate:
                aggregate[key] += chunk_result[key]

        log_event(
            LOGGER,
            "field_job_batch_finished",
            limit=limit,
            source_slug=source_slug,
            field_name=field_name,
            workers=len(worker_limits),
            **aggregate,
        )
        return aggregate

    def _process_field_job_chunk(
        self,
        limit: int,
        source_slug: str | None,
        field_name: str | None,
        worker_index: int,
        total_workers: int,
    ) -> dict[str, int]:
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            engine = FieldJobEngine(
                repository=repository,
                logger=LOGGER,
                worker_id=_worker_id(self._settings.crawler_field_job_worker_id, worker_index, total_workers),
                base_backoff_seconds=self._settings.crawler_field_job_base_backoff_seconds,
                source_slug=source_slug,
                field_name=field_name,
                search_client=SearchClient(self._settings),
                search_provider=self._settings.crawler_search_provider,
                max_search_pages=self._settings.crawler_search_max_pages_per_job,
                negative_result_cooldown_days=self._settings.crawler_search_negative_cooldown_days,
                dependency_wait_seconds=self._settings.crawler_search_dependency_wait_seconds,
                min_no_candidate_backoff_seconds=self._settings.crawler_search_min_no_candidate_backoff_seconds,
                email_max_queries=self._settings.crawler_search_email_max_queries,
                instagram_max_queries=self._settings.crawler_search_instagram_max_queries,
                enable_school_initials=self._settings.crawler_search_enable_school_initials,
                min_school_initial_length=self._settings.crawler_search_min_school_initial_length,
                enable_compact_fraternity=self._settings.crawler_search_enable_compact_fraternity,
                instagram_enable_handle_queries=self._settings.crawler_search_instagram_enable_handle_queries,
                greedy_collect_mode=self._settings.crawler_greedy_collect,
            )
            return engine.process(limit=limit)

    def discover_source(self, fraternity_name: str) -> dict[str, object]:
        search_client = SearchClient(self._settings)
        try:
            result = discover_source(fraternity_name, search_client)
            return result.as_dict()
        except Exception as exc:
            log_event(
                LOGGER,
                "source_discovery_failed",
                level=logging.WARNING,
                fraternity_name=fraternity_name,
                error=str(exc),
            )
            fallback_slug = fraternity_name.strip().lower().replace(" ", "-")
            return {
                "fraternity_name": fraternity_name,
                "fraternity_slug": fallback_slug,
                "selected_url": None,
                "selected_confidence": 0.0,
                "confidence_tier": "low",
                "candidates": [],
            }

    def liveness(self) -> dict[str, object]:
        return {"ok": True, "service": "crawler", "probe": "liveness"}

    def readiness(self) -> dict[str, object]:
        with get_connection(self._settings) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS ready")
            row = cursor.fetchone()
            if row is None or row["ready"] != 1:
                raise RuntimeError("Database readiness check failed")

        return {"ok": True, "service": "crawler", "probe": "readiness"}


def _distribute_limit(limit: int, workers: int) -> list[int]:
    effective_limit = max(0, limit)
    if effective_limit == 0:
        return []
    effective_workers = max(1, min(workers, effective_limit))
    base, remainder = divmod(effective_limit, effective_workers)
    return [base + (1 if index < remainder else 0) for index in range(effective_workers) if base + (1 if index < remainder else 0) > 0]


def _worker_id(base_worker_id: str, worker_index: int, total_workers: int) -> str:
    if total_workers <= 1:
        return base_worker_id
    return f"{base_worker_id}-{worker_index}"
