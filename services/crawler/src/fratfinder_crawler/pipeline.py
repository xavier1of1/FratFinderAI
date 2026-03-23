from __future__ import annotations

import logging

from fratfinder_crawler.adapters import AdapterRegistry
from fratfinder_crawler.config import Settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.db.repository import CrawlerRepository
from fratfinder_crawler.field_jobs import FieldJobEngine
from fratfinder_crawler.http.client import HttpClient
from fratfinder_crawler.logging_utils import log_event
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

    def process_field_jobs(self, limit: int = 25, source_slug: str | None = None) -> dict[str, int]:
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            engine = FieldJobEngine(
                repository=repository,
                logger=LOGGER,
                worker_id=self._settings.crawler_field_job_worker_id,
                base_backoff_seconds=self._settings.crawler_field_job_base_backoff_seconds,
                source_slug=source_slug,
            )
            result = engine.process(limit=limit)

        log_event(LOGGER, "field_job_batch_finished", limit=limit, source_slug=source_slug, **result)
        return result

    def liveness(self) -> dict[str, object]:
        return {"ok": True, "service": "crawler", "probe": "liveness"}

    def readiness(self) -> dict[str, object]:
        with get_connection(self._settings) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS ready")
            row = cursor.fetchone()
            if row is None or row["ready"] != 1:
                raise RuntimeError("Database readiness check failed")

        return {"ok": True, "service": "crawler", "probe": "readiness"}
