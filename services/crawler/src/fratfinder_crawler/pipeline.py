from __future__ import annotations

import json
import logging
import re
from threading import Event, Thread
from datetime import datetime, timezone
from pathlib import Path
import time
from urllib.parse import urlparse
from uuid import uuid4

import requests

from fratfinder_crawler.adapters import AdapterRegistry
from fratfinder_crawler.adaptive import AdaptivePolicy
from fratfinder_crawler.config import Settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.db import CrawlerRepository, RequestGraphRepository
from fratfinder_crawler.discovery import discover_source
from fratfinder_crawler.field_jobs import FIELD_JOB_FIND_EMAIL, FIELD_JOB_FIND_WEBSITE, FieldJobEngine
from fratfinder_crawler.http.client import HttpClient
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.normalization import classify_chapter_validity
from fratfinder_crawler.search import SearchClient, SearchUnavailableError
from fratfinder_crawler.models import (
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    CrawlMetrics,
    EpochMetric,
    ExtractedChapter,
    FieldJob,
)
from fratfinder_crawler.orchestration import (
    AdaptiveCrawlOrchestrator,
    CrawlOrchestrator,
    FieldJobGraphRuntime,
    FieldJobSupervisorGraphRuntime,
    RequestSupervisorGraphRuntime,
)

LOGGER = logging.getLogger(__name__)


class CrawlService:
    def __init__(self, settings: Settings):
        self._settings = settings

    def run(
        self,
        source_slug: str | None = None,
        runtime_mode: str | None = None,
        policy_mode: str = "live",
        policy_version: str | None = None,
    ) -> dict[str, object]:
        aggregate = CrawlMetrics()
        effective_runtime_mode = self._resolve_runtime_mode(runtime_mode)

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            sources = repository.load_sources(source_slug=source_slug)
            orchestrator = self._build_orchestrator(
                repository,
                effective_runtime_mode,
                policy_mode=policy_mode,
                policy_version=policy_version,
            )

            log_event(
                LOGGER,
                "crawl_batch_started",
                requested_source_slug=source_slug,
                source_count=len(sources),
                runtime_mode=effective_runtime_mode,
            )

            for source in sources:
                log_event(LOGGER, "source_crawl_started", source_slug=source.source_slug, runtime_mode=effective_runtime_mode, policy_mode=policy_mode)
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
                    runtime_mode=effective_runtime_mode,
                    pages_processed=metrics.pages_processed,
                    records_seen=metrics.records_seen,
                    records_upserted=metrics.records_upserted,
                    review_items_created=metrics.review_items_created,
                    field_jobs_created=metrics.field_jobs_created,
                )

        result = {
            "runtime_mode": effective_runtime_mode,
            "policy_mode": policy_mode,
            "policy_version": (policy_version or self._settings.crawler_policy_version),
            "pages_processed": aggregate.pages_processed,
            "records_seen": aggregate.records_seen,
            "records_upserted": aggregate.records_upserted,
            "review_items_created": aggregate.review_items_created,
            "field_jobs_created": aggregate.field_jobs_created,
        }
        log_event(LOGGER, "crawl_batch_finished", **result)
        return result

    def run_legacy(self, source_slug: str | None = None) -> dict[str, int]:
        return self.run(source_slug=source_slug, runtime_mode="legacy")

    def run_request(
        self,
        *,
        request_id: str,
        runtime_mode: str = "v3_request_supervisor",
        crawl_runtime_mode: str | None = None,
        field_job_runtime_mode: str | None = None,
        graph_durability: str | None = None,
    ) -> dict[str, object]:
        effective_crawl_runtime_mode = self._resolve_v3_crawl_runtime_mode(crawl_runtime_mode)
        effective_field_job_runtime_mode = self._resolve_v3_field_job_runtime_mode(field_job_runtime_mode)
        effective_graph_durability = self._resolve_field_job_graph_durability(
            graph_durability or self._settings.crawler_v3_field_job_graph_durability
        )

        with get_connection(self._settings) as connection:
            runtime = RequestSupervisorGraphRuntime(
                request_repository=RequestGraphRepository(connection),
                crawler_repository=CrawlerRepository(connection),
                worker_id=self._settings.crawler_v3_request_worker_id,
                runtime_mode=runtime_mode,
                crawl_runtime_mode=effective_crawl_runtime_mode,
                field_job_runtime_mode=effective_field_job_runtime_mode,
                field_job_graph_durability=effective_graph_durability,
                free_recovery_attempts=self._settings.crawler_v3_free_recovery_attempts,
                discover_source=self.discover_source,
                run_crawl=self.run,
                process_field_jobs=self.process_field_jobs,
                search_preflight=self.search_preflight,
                logger=LOGGER,
            )
            summary = runtime.run(request_id)

        log_event(
            LOGGER,
            "request_graph_run_finished",
            request_id=request_id,
            runtime_mode=runtime_mode,
            crawl_runtime_mode=effective_crawl_runtime_mode,
            field_job_runtime_mode=effective_field_job_runtime_mode,
            graph_durability=effective_graph_durability,
            status=summary.get("status"),
            terminal_reason=summary.get("terminalReason"),
            crawl_run_id=summary.get("crawlRunId"),
            records_seen=summary.get("recordsSeen"),
            queue_remaining=summary.get("queueRemaining"),
        )
        return summary

    def run_request_worker(
        self,
        *,
        once: bool = False,
        limit: int | None = None,
        poll_seconds: int | None = None,
        runtime_mode: str = "v3_request_supervisor",
    ) -> dict[str, object]:
        batch_limit = max(1, int(limit if limit is not None else self._settings.crawler_v3_request_batch_limit))
        effective_poll_seconds = max(
            1,
            int(poll_seconds if poll_seconds is not None else self._settings.crawler_v3_request_poll_seconds),
        )
        lease_seconds = max(30, int(self._settings.crawler_v3_request_worker_lease_seconds))
        heartbeat_seconds = max(5, min(int(self._settings.crawler_v3_request_worker_heartbeat_seconds), max(5, lease_seconds // 2)))
        summaries: list[dict[str, object]] = []
        stale_recovered_total = 0
        idle_cycles = 0

        log_event(
            LOGGER,
            "request_worker_started",
            worker_id=self._settings.crawler_v3_request_worker_id,
            runtime_mode=runtime_mode,
            once=once,
            batch_limit=batch_limit,
            poll_seconds=effective_poll_seconds,
            lease_seconds=lease_seconds,
            heartbeat_seconds=heartbeat_seconds,
        )

        try:
            with get_connection(self._settings) as connection:
                request_repository = RequestGraphRepository(connection)
                request_repository.upsert_worker_process(
                    worker_id=self._settings.crawler_v3_request_worker_id,
                    workload_lane="request",
                    runtime_owner=self._settings.crawler_v3_request_worker_runtime_owner,
                    lease_seconds=lease_seconds,
                    metadata={"runtimeMode": runtime_mode},
                )

            while True:
                with get_connection(self._settings) as connection:
                    request_repository = RequestGraphRepository(connection)
                    request_repository.heartbeat_worker_process(
                        self._settings.crawler_v3_request_worker_id,
                        lease_seconds=lease_seconds,
                    )
                    stale_recovered_total += request_repository.reconcile_stale_requests(
                        self._settings.crawler_v3_request_stale_minutes
                    )
                    request_lease_token = str(uuid4())
                    request = request_repository.claim_next_due_request(
                        self._settings.crawler_v3_request_worker_id,
                        lease_token=request_lease_token,
                        lease_seconds=lease_seconds,
                    )

                if request is None:
                    idle_cycles += 1
                    if once or len(summaries) >= batch_limit:
                        break
                    time.sleep(effective_poll_seconds)
                    continue

                idle_cycles = 0
                log_event(
                    LOGGER,
                    "request_worker_claimed_request",
                    worker_id=self._settings.crawler_v3_request_worker_id,
                    request_id=request.id,
                    fraternity_slug=request.fraternity_slug,
                    source_slug=request.source_slug,
                    stage=request.stage,
                )
                heartbeat_stop = Event()
                heartbeat_thread = Thread(
                    target=self._heartbeat_request_worker,
                    kwargs={
                        "request_id": request.id,
                        "lease_token": request_lease_token,
                        "lease_seconds": lease_seconds,
                        "heartbeat_seconds": heartbeat_seconds,
                        "stop_event": heartbeat_stop,
                        "runtime_mode": runtime_mode,
                    },
                    daemon=True,
                    name=f"request-worker-heartbeat-{request.id}",
                )
                heartbeat_thread.start()
                try:
                    summary = self.run_request(
                        request_id=request.id,
                        runtime_mode=runtime_mode,
                        crawl_runtime_mode=self._settings.crawler_v3_crawl_runtime_mode,
                        field_job_runtime_mode=self._settings.crawler_v3_field_job_runtime_mode,
                        graph_durability=self._settings.crawler_v3_field_job_graph_durability,
                    )
                finally:
                    heartbeat_stop.set()
                    heartbeat_thread.join(timeout=max(heartbeat_seconds, 5))
                    with get_connection(self._settings) as connection:
                        request_repository = RequestGraphRepository(connection)
                        request_repository.release_request_lease(
                            request_id=request.id,
                            worker_id=self._settings.crawler_v3_request_worker_id,
                            lease_token=request_lease_token,
                        )
                        request_repository.heartbeat_worker_process(
                            self._settings.crawler_v3_request_worker_id,
                            lease_seconds=lease_seconds,
                        )
                summaries.append(summary)
                if once and len(summaries) >= batch_limit:
                    break
        finally:
            with get_connection(self._settings) as connection:
                request_repository = RequestGraphRepository(connection)
                request_repository.stop_worker_process(self._settings.crawler_v3_request_worker_id, status="stopped")

        aggregate = {
            "workerId": self._settings.crawler_v3_request_worker_id,
            "runtimeMode": runtime_mode,
            "processed": len(summaries),
            "succeeded": sum(1 for item in summaries if item.get("status") == "succeeded"),
            "paused": sum(1 for item in summaries if item.get("status") == "paused"),
            "failed": sum(1 for item in summaries if item.get("status") == "failed"),
            "staleRecovered": stale_recovered_total,
            "idleCycles": idle_cycles,
            "summaries": summaries,
        }
        log_event(LOGGER, "request_worker_finished", **aggregate)
        return aggregate

    def _heartbeat_request_worker(
        self,
        *,
        request_id: str,
        lease_token: str,
        lease_seconds: int,
        heartbeat_seconds: int,
        stop_event: Event,
        runtime_mode: str,
    ) -> None:
        while not stop_event.wait(heartbeat_seconds):
            try:
                with get_connection(self._settings) as connection:
                    request_repository = RequestGraphRepository(connection)
                    request_repository.heartbeat_worker_process(
                        self._settings.crawler_v3_request_worker_id,
                        lease_seconds=lease_seconds,
                    )
                    request_repository.heartbeat_request_lease(
                        request_id=request_id,
                        worker_id=self._settings.crawler_v3_request_worker_id,
                        lease_token=lease_token,
                        lease_seconds=lease_seconds,
                    )
            except Exception as exc:  # pragma: no cover - defensive heartbeat logging
                log_event(
                    LOGGER,
                    "request_worker_heartbeat_failed",
                    worker_id=self._settings.crawler_v3_request_worker_id,
                    request_id=request_id,
                    runtime_mode=runtime_mode,
                    error=str(exc),
                )

    def run_field_job_worker(
        self,
        *,
        once: bool = False,
        limit: int | None = None,
        workers: int | None = None,
        poll_seconds: int | None = None,
        runtime_mode: str | None = None,
        graph_durability: str | None = None,
        run_preflight: bool = True,
    ) -> dict[str, object]:
        batch_limit = max(1, int(limit if limit is not None else self._settings.crawler_v3_request_batch_limit))
        effective_poll_seconds = max(1, int(poll_seconds if poll_seconds is not None else 15))
        effective_runtime_mode = self._resolve_field_job_runtime_mode(runtime_mode)
        effective_graph_durability = self._resolve_field_job_graph_durability(graph_durability)
        lease_seconds = max(30, int(self._settings.crawler_field_job_worker_lease_seconds))
        worker_id = self._settings.crawler_field_job_worker_id
        summaries: list[dict[str, object]] = []
        idle_cycles = 0

        log_event(
            LOGGER,
            "field_job_worker_started",
            worker_id=worker_id,
            once=once,
            batch_limit=batch_limit,
            poll_seconds=effective_poll_seconds,
            runtime_mode=effective_runtime_mode,
            graph_durability=effective_graph_durability,
        )

        try:
            with get_connection(self._settings) as connection:
                request_repository = RequestGraphRepository(connection)
                request_repository.upsert_worker_process(
                    worker_id=worker_id,
                    workload_lane="contact_resolution",
                    runtime_owner=f"python_field_job_worker_{effective_runtime_mode}",
                    lease_seconds=lease_seconds,
                    metadata={
                        "runtimeMode": effective_runtime_mode,
                        "graphDurability": effective_graph_durability,
                        "workerType": "field_job_supervisor",
                    },
                )

            while True:
                with get_connection(self._settings) as connection:
                    repository = CrawlerRepository(connection)
                    request_repository = RequestGraphRepository(connection)
                    request_repository.heartbeat_worker_process(worker_id, lease_seconds=lease_seconds)
                    queue_counts = repository.get_field_job_queue_counts()

                actionable_jobs = int(queue_counts.get("actionable_jobs", 0) or 0)
                running_jobs = int(queue_counts.get("running_jobs", 0) or 0)
                if actionable_jobs <= 0 and running_jobs <= 0:
                    idle_cycles += 1
                    if once or len(summaries) >= batch_limit:
                        break
                    time.sleep(effective_poll_seconds)
                    continue

                idle_cycles = 0
                summary = self.process_field_jobs(
                    limit=batch_limit,
                    workers=workers,
                    run_preflight=run_preflight,
                    runtime_mode=effective_runtime_mode,
                    graph_durability=effective_graph_durability,
                )
                summaries.append(summary)
                with get_connection(self._settings) as connection:
                    request_repository = RequestGraphRepository(connection)
                    request_repository.heartbeat_worker_process(worker_id, lease_seconds=lease_seconds)
                if once:
                    break
        finally:
            with get_connection(self._settings) as connection:
                request_repository = RequestGraphRepository(connection)
                request_repository.stop_worker_process(worker_id, status="stopped")

        aggregate = {
            "workerId": worker_id,
            "runtimeMode": effective_runtime_mode,
            "graphDurability": effective_graph_durability,
            "processedBatches": len(summaries),
            "processed": sum(int(item.get("processed", 0) or 0) for item in summaries),
            "requeued": sum(int(item.get("requeued", 0) or 0) for item in summaries),
            "failedTerminal": sum(int(item.get("failed_terminal", 0) or 0) for item in summaries),
            "fieldJobWorkerRecoveriesStarted": sum(int(item.get("field_job_worker_recoveries_started", 0) or 0) for item in summaries),
            "idleCycles": idle_cycles,
            "summaries": summaries,
        }
        log_event(LOGGER, "field_job_worker_finished", **aggregate)
        return aggregate

    def adaptive_train_eval(
        self,
        *,
        epochs: int,
        train_source_slugs: list[str],
        eval_source_slugs: list[str],
        runtime_mode: str = "adaptive_assisted",
        cohort_label: str = "target-cohort",
        replay_window_days: int | None = None,
        replay_batch_size: int | None = None,
        policy_version: str | None = None,
        report_path: str | None = None,
        eval_enrichment_limit_per_source: int | None = None,
        eval_enrichment_workers: int | None = None,
    ) -> dict[str, object]:
        effective_runtime_mode = self._resolve_runtime_mode(runtime_mode)
        if effective_runtime_mode == "legacy":
            effective_runtime_mode = "adaptive_assisted"

        effective_policy_version = (policy_version or self._settings.crawler_policy_version).strip() or self._settings.crawler_policy_version
        train_sources = _normalize_source_slugs(train_source_slugs)
        eval_sources = _normalize_source_slugs(eval_source_slugs)
        if not train_sources:
            raise ValueError("At least one train source slug is required")
        if not eval_sources:
            raise ValueError("At least one eval source slug is required")

        replay_days = replay_window_days if replay_window_days is not None else self._settings.crawler_adaptive_replay_window_days
        replay_size = replay_batch_size if replay_batch_size is not None else self._settings.crawler_adaptive_replay_batch_size
        enrichment_limit = (
            eval_enrichment_limit_per_source
            if eval_enrichment_limit_per_source is not None
            else self._settings.crawler_adaptive_eval_enrichment_limit_per_source
        )
        enrichment_workers = (
            eval_enrichment_workers
            if eval_enrichment_workers is not None
            else self._settings.crawler_adaptive_eval_enrichment_workers
        )

        weights = _balanced_kpi_weights(self._settings.crawler_adaptive_balanced_kpi_weights)

        epoch_rows: list[dict[str, object]] = []
        for epoch in range(1, max(1, epochs) + 1):
            train_metrics = self._run_sources_batch(train_sources, effective_runtime_mode, policy_mode="train")
            replay_summary = self._replay_update_policy(
                runtime_mode=effective_runtime_mode,
                policy_version=effective_policy_version,
                source_slugs=train_sources,
                window_days=replay_days,
                batch_size=replay_size,
            )

            skip_eval_enrichment, eval_enrichment_preflight = self._should_skip_eval_enrichment(eval_sources)

            eval_legacy = self._run_sources_batch(eval_sources, "legacy", policy_mode="live")
            eval_legacy_enrichment = self._run_eval_enrichment_for_sources(
                eval_sources,
                limit_per_source=enrichment_limit,
                workers=enrichment_workers,
                skip_provider_degraded=skip_eval_enrichment,
                preflight_snapshot=eval_enrichment_preflight,
            )
            legacy_coverage = self._summarize_batch_contact_coverage(eval_legacy.get("run_ids", []))
            if eval_legacy_enrichment.get("processed", 0) or eval_legacy_enrichment.get("requeued", 0) or eval_legacy_enrichment.get("failed_terminal", 0) or eval_legacy_enrichment.get("skipped_provider_degraded", 0):
                eval_legacy["enrichment"] = eval_legacy_enrichment
            eval_legacy["coverage"] = legacy_coverage

            eval_adaptive = self._run_sources_batch(eval_sources, effective_runtime_mode, policy_mode="live")
            eval_adaptive_enrichment = self._run_eval_enrichment_for_sources(
                eval_sources,
                limit_per_source=enrichment_limit,
                workers=enrichment_workers,
                skip_provider_degraded=skip_eval_enrichment,
                preflight_snapshot=eval_enrichment_preflight,
            )
            adaptive_coverage = self._summarize_batch_contact_coverage(eval_adaptive.get("run_ids", []))
            if eval_adaptive_enrichment.get("processed", 0) or eval_adaptive_enrichment.get("requeued", 0) or eval_adaptive_enrichment.get("failed_terminal", 0) or eval_adaptive_enrichment.get("skipped_provider_degraded", 0):
                eval_adaptive["enrichment"] = eval_adaptive_enrichment
            eval_adaptive["coverage"] = adaptive_coverage

            legacy_records_per_page = _safe_ratio(float(eval_legacy["records_seen"]), float(eval_legacy["pages_processed"]))
            adaptive_records_per_page = _safe_ratio(float(eval_adaptive["records_seen"]), float(eval_adaptive["pages_processed"]))
            legacy_pages_per_record = _safe_ratio(float(eval_legacy["pages_processed"]), float(eval_legacy["records_seen"]))
            adaptive_pages_per_record = _safe_ratio(float(eval_adaptive["pages_processed"]), float(eval_adaptive["records_seen"]))
            legacy_upsert_ratio = _safe_ratio(float(eval_legacy["records_upserted"]), float(eval_legacy["records_seen"]))
            adaptive_upsert_ratio = _safe_ratio(float(eval_adaptive["records_upserted"]), float(eval_adaptive["records_seen"]))
            legacy_jobs_per_min = float(eval_legacy.get("jobs_per_minute", 0.0))
            adaptive_jobs_per_min = float(eval_adaptive.get("jobs_per_minute", 0.0))
            legacy_review_rate = _safe_ratio(float(eval_legacy["review_items_created"]), max(float(eval_legacy["records_seen"]), 1.0))
            adaptive_review_rate = _safe_ratio(float(eval_adaptive["review_items_created"]), max(float(eval_adaptive["records_seen"]), 1.0))

            kpis = {
                "legacyRecordsPerPage": round(legacy_records_per_page, 4),
                "adaptiveRecordsPerPage": round(adaptive_records_per_page, 4),
                "recordsPerPageDelta": round(adaptive_records_per_page - legacy_records_per_page, 4),
                "legacyPagesPerRecord": round(legacy_pages_per_record, 4),
                "adaptivePagesPerRecord": round(adaptive_pages_per_record, 4),
                "pagesPerRecordDelta": round(adaptive_pages_per_record - legacy_pages_per_record, 4),
                "legacyUpsertRatio": round(legacy_upsert_ratio, 4),
                "adaptiveUpsertRatio": round(adaptive_upsert_ratio, 4),
                "upsertRatioDelta": round(adaptive_upsert_ratio - legacy_upsert_ratio, 4),
                "legacyJobsPerMinute": round(legacy_jobs_per_min, 4),
                "adaptiveJobsPerMinute": round(adaptive_jobs_per_min, 4),
                "jobsPerMinuteDelta": round(adaptive_jobs_per_min - legacy_jobs_per_min, 4),
                "legacyReviewRate": round(legacy_review_rate, 4),
                "adaptiveReviewRate": round(adaptive_review_rate, 4),
                "reviewRateDelta": round(adaptive_review_rate - legacy_review_rate, 4),
                "legacyAnyContactRate": round(float(legacy_coverage["any_contact_rate"]), 4),
                "adaptiveAnyContactRate": round(float(adaptive_coverage["any_contact_rate"]), 4),
                "anyContactRateDelta": round(float(adaptive_coverage["any_contact_rate"]) - float(legacy_coverage["any_contact_rate"]), 4),
                "legacyWebsiteRate": round(float(legacy_coverage["website_rate"]), 4),
                "adaptiveWebsiteRate": round(float(adaptive_coverage["website_rate"]), 4),
                "websiteRateDelta": round(float(adaptive_coverage["website_rate"]) - float(legacy_coverage["website_rate"]), 4),
                "legacyEmailRate": round(float(legacy_coverage["email_rate"]), 4),
                "adaptiveEmailRate": round(float(adaptive_coverage["email_rate"]), 4),
                "emailRateDelta": round(float(adaptive_coverage["email_rate"]) - float(legacy_coverage["email_rate"]), 4),
                "legacyInstagramRate": round(float(legacy_coverage["instagram_rate"]), 4),
                "adaptiveInstagramRate": round(float(adaptive_coverage["instagram_rate"]), 4),
                "instagramRateDelta": round(float(adaptive_coverage["instagram_rate"]) - float(legacy_coverage["instagram_rate"]), 4),
                "legacyAllThreeRate": round(float(legacy_coverage["all_three_rate"]), 4),
                "adaptiveAllThreeRate": round(float(adaptive_coverage["all_three_rate"]), 4),
                "allThreeRateDelta": round(float(adaptive_coverage["all_three_rate"]) - float(legacy_coverage["all_three_rate"]), 4),
            }
            balanced_score = _compute_balanced_score(kpis, weights)
            kpis["balancedScore"] = round(balanced_score, 4)

            row = {
                "epoch": epoch,
                "policyVersion": effective_policy_version,
                "runtimeMode": effective_runtime_mode,
                "train": train_metrics,
                "replay": replay_summary,
                "evalLegacy": eval_legacy,
                "evalAdaptive": eval_adaptive,
                "kpis": kpis,
            }
            epoch_rows.append(row)

            slope_snapshot = {
                "recordsPerPageDeltaSlope": round(_linear_slope([float(item["kpis"]["recordsPerPageDelta"]) for item in epoch_rows]), 6),
                "pagesPerRecordDeltaSlope": round(_linear_slope([float(item["kpis"]["pagesPerRecordDelta"]) for item in epoch_rows]), 6),
                "upsertRatioDeltaSlope": round(_linear_slope([float(item["kpis"]["upsertRatioDelta"]) for item in epoch_rows]), 6),
                "jobsPerMinuteDeltaSlope": round(_linear_slope([float(item["kpis"]["jobsPerMinuteDelta"]) for item in epoch_rows]), 6),
                "reviewRateDeltaSlope": round(_linear_slope([float(item["kpis"]["reviewRateDelta"]) for item in epoch_rows]), 6),
                "anyContactRateDeltaSlope": round(_linear_slope([float(item["kpis"].get("anyContactRateDelta", 0.0)) for item in epoch_rows]), 6),
                "balancedScoreSlope": round(_linear_slope([float(item["kpis"]["balancedScore"]) for item in epoch_rows]), 6),
            }
            row["slopes"] = slope_snapshot

            with get_connection(self._settings) as connection:
                repository = CrawlerRepository(connection)
                repository.insert_epoch_metric(
                    EpochMetric(
                        epoch=epoch,
                        policy_version=effective_policy_version,
                        runtime_mode=effective_runtime_mode,
                        train_sources=train_sources,
                        eval_sources=eval_sources,
                        kpis={str(k): float(v) for k, v in kpis.items()},
                        deltas={
                            "recordsPerPageDelta": float(kpis["recordsPerPageDelta"]),
                            "pagesPerRecordDelta": float(kpis["pagesPerRecordDelta"]),
                            "upsertRatioDelta": float(kpis["upsertRatioDelta"]),
                            "jobsPerMinuteDelta": float(kpis["jobsPerMinuteDelta"]),
                            "reviewRateDelta": float(kpis["reviewRateDelta"]),
                            "anyContactRateDelta": float(kpis["anyContactRateDelta"]),
                            "balancedScore": float(kpis["balancedScore"]),
                        },
                        slopes=slope_snapshot,
                        cohort_label=cohort_label,
                        metadata={
                            "weights": weights,
                            "replayWindowDays": replay_days,
                            "replayBatchSize": replay_size,
                            "evalEnrichmentLimitPerSource": enrichment_limit,
                            "evalEnrichmentWorkers": enrichment_workers,
                        },
                    )
                )

        slope = {
            "recordsPerPageDeltaSlope": round(_linear_slope([float(row["kpis"]["recordsPerPageDelta"]) for row in epoch_rows]), 6),
            "pagesPerRecordDeltaSlope": round(_linear_slope([float(row["kpis"]["pagesPerRecordDelta"]) for row in epoch_rows]), 6),
            "upsertRatioDeltaSlope": round(_linear_slope([float(row["kpis"]["upsertRatioDelta"]) for row in epoch_rows]), 6),
            "jobsPerMinuteDeltaSlope": round(_linear_slope([float(row["kpis"]["jobsPerMinuteDelta"]) for row in epoch_rows]), 6),
            "reviewRateDeltaSlope": round(_linear_slope([float(row["kpis"]["reviewRateDelta"]) for row in epoch_rows]), 6),
            "anyContactRateDeltaSlope": round(_linear_slope([float(row["kpis"].get("anyContactRateDelta", 0.0)) for row in epoch_rows]), 6),
            "balancedScoreSlope": round(_linear_slope([float(row["kpis"]["balancedScore"]) for row in epoch_rows]), 6),
        }

        final_report_path = report_path or _default_epoch_report_path()
        report_markdown = _render_epoch_report(
            epochs=max(1, epochs),
            runtime_mode=effective_runtime_mode,
            train_sources=train_sources,
            eval_sources=eval_sources,
            epoch_rows=epoch_rows,
            slope=slope,
        )
        report_file = _resolve_repo_root() / final_report_path
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report_file.write_text(report_markdown, encoding="utf-8")

        return {
            "epochs": max(1, epochs),
            "runtime_mode": effective_runtime_mode,
            "policy_mode": "train-live-eval",
            "policy_version": effective_policy_version,
            "cohort_label": cohort_label,
            "train_sources": train_sources,
            "eval_sources": eval_sources,
            "slope": slope,
            "rows": epoch_rows,
            "report_path": str(report_file),
        }

    def _run_sources_batch(
        self,
        source_slugs: list[str],
        runtime_mode: str,
        *,
        policy_mode: str = "live",
    ) -> dict[str, object]:
        aggregate = CrawlMetrics()
        effective_runtime_mode = self._resolve_runtime_mode(runtime_mode)
        requested = [slug for slug in source_slugs if slug]
        selected_count = 0
        run_ids: list[int] = []
        started_at = time.perf_counter()
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            orchestrator = self._build_orchestrator(repository, effective_runtime_mode, policy_mode=policy_mode)
            for slug in requested:
                matches = repository.load_sources(source_slug=slug)
                if not matches:
                    continue
                source = matches[0]
                selected_count += 1

                before_rows = repository.list_crawl_run_metrics(source_slug=slug, runtime_mode=effective_runtime_mode, limit=1)
                before_id = int(before_rows[0]["id"]) if before_rows else None

                metrics = orchestrator.run_for_source(source)
                aggregate.pages_processed += metrics.pages_processed
                aggregate.records_seen += metrics.records_seen
                aggregate.records_upserted += metrics.records_upserted
                aggregate.review_items_created += metrics.review_items_created
                aggregate.field_jobs_created += metrics.field_jobs_created

                after_rows = repository.list_crawl_run_metrics(source_slug=slug, runtime_mode=effective_runtime_mode, limit=1)
                if after_rows:
                    after_id = int(after_rows[0]["id"])
                    if before_id is None or after_id != before_id:
                        run_ids.append(after_id)

        elapsed_seconds = max(time.perf_counter() - started_at, 0.001)
        jobs_per_minute = (aggregate.records_upserted / elapsed_seconds) * 60.0
        return {
            "sourceCount": float(selected_count),
            "pages_processed": float(aggregate.pages_processed),
            "records_seen": float(aggregate.records_seen),
            "records_upserted": float(aggregate.records_upserted),
            "review_items_created": float(aggregate.review_items_created),
            "field_jobs_created": float(aggregate.field_jobs_created),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "jobs_per_minute": round(jobs_per_minute, 4),
            "run_ids": run_ids,
        }

    def _should_skip_eval_enrichment(self, source_slugs: list[str]) -> tuple[bool, dict[str, object] | None]:
        slugs = _normalize_source_slugs(source_slugs)
        if not slugs:
            return False, None

        run_preflight = bool(self._settings.crawler_adaptive_eval_enrichment_run_preflight)
        require_healthy = bool(self._settings.crawler_adaptive_eval_enrichment_require_healthy_search)
        if not (run_preflight and require_healthy and self._settings.crawler_search_enabled):
            return False, None

        preflight_snapshot = self.search_preflight()
        if not bool(preflight_snapshot.get("healthy", False)):
            return True, preflight_snapshot
        return False, preflight_snapshot

    def _run_eval_enrichment_for_sources(
        self,
        source_slugs: list[str],
        *,
        limit_per_source: int,
        workers: int,
        skip_provider_degraded: bool = False,
        preflight_snapshot: dict[str, object] | None = None,
    ) -> dict[str, object]:
        if limit_per_source <= 0:
            return {"processed": 0, "requeued": 0, "failed_terminal": 0, "skipped_provider_degraded": 0}

        slugs = _normalize_source_slugs(source_slugs)
        aggregate = {"processed": 0, "requeued": 0, "failed_terminal": 0, "skipped_provider_degraded": 0}
        if not slugs:
            return aggregate

        if skip_provider_degraded:
            aggregate["skipped_provider_degraded"] = len(slugs)
            log_event(
                LOGGER,
                "eval_enrichment_skipped_provider_degraded",
                source_count=len(slugs),
                preflight=preflight_snapshot,
            )
            return aggregate

        require_healthy = bool(self._settings.crawler_adaptive_eval_enrichment_require_healthy_search)
        for slug in slugs:
            result = self.process_field_jobs(
                limit=max(1, limit_per_source),
                source_slug=slug,
                field_name=None,
                workers=max(1, workers),
                require_healthy_search=require_healthy,
                run_preflight=False,
            )
            for key in ("processed", "requeued", "failed_terminal"):
                aggregate[key] += int(result.get(key, 0))
        return aggregate

    def _summarize_batch_contact_coverage(self, run_ids: list[int] | list[float] | None) -> dict[str, float | int]:
        normalized_ids = [int(value) for value in (run_ids or []) if value is not None]
        if not normalized_ids:
            return {
                "chapters": 0,
                "any_contact": 0,
                "website": 0,
                "email": 0,
                "instagram": 0,
                "all_three": 0,
                "any_contact_rate": 0.0,
                "website_rate": 0.0,
                "email_rate": 0.0,
                "instagram_rate": 0.0,
                "all_three_rate": 0.0,
            }

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            counts = repository.summarize_contact_coverage_for_runs(crawl_run_ids=normalized_ids)

        chapters = int(counts.get("chapters", 0))
        any_contact = int(counts.get("any_contact", 0))
        website = int(counts.get("website", 0))
        email = int(counts.get("email", 0))
        instagram = int(counts.get("instagram", 0))
        all_three = int(counts.get("all_three", 0))

        return {
            "chapters": chapters,
            "any_contact": any_contact,
            "website": website,
            "email": email,
            "instagram": instagram,
            "all_three": all_three,
            "any_contact_rate": _safe_ratio(float(any_contact), float(chapters)),
            "website_rate": _safe_ratio(float(website), float(chapters)),
            "email_rate": _safe_ratio(float(email), float(chapters)),
            "instagram_rate": _safe_ratio(float(instagram), float(chapters)),
            "all_three_rate": _safe_ratio(float(all_three), float(chapters)),
        }

    def run_adaptive(
        self,
        source_slug: str | None = None,
        runtime_mode: str = "adaptive_shadow",
        policy_mode: str = "live",
    ) -> dict[str, int]:
        return self.run(source_slug=source_slug, runtime_mode=runtime_mode, policy_mode=policy_mode)

    def export_crawl_observations(
        self,
        *,
        source_slug: str | None = None,
        crawl_session_id: str | None = None,
        runtime_mode: str | None = None,
        window_days: int | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            data = repository.export_crawl_observations(
                source_slug=source_slug,
                crawl_session_id=crawl_session_id,
                runtime_mode=runtime_mode,
                window_days=window_days,
                limit=limit or self._settings.crawler_replay_export_limit,
            )
        return {"count": len(data), "observations": data}

    def export_enrichment_observations(
        self,
        *,
        source_slug: str | None = None,
        field_name: str | None = None,
        window_days: int | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            data = repository.export_enrichment_observations(
                source_slug=source_slug,
                field_name=field_name,
                window_days=window_days,
                limit=limit or self._settings.crawler_replay_export_limit,
            )
        return {"count": len(data), "observations": data}

    def crawl_policy_report(self, limit: int = 25) -> dict[str, object]:
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            return repository.build_policy_report(limit=limit)

    def crawl_replay_policy(
        self,
        limit: int | None = None,
        source_slug: str | None = None,
        runtime_mode: str | None = None,
        window_days: int | None = None,
    ) -> dict[str, object]:
        snapshot = self.export_crawl_observations(
            source_slug=source_slug,
            runtime_mode=runtime_mode,
            window_days=window_days,
            limit=limit,
        )
        action_buckets: dict[str, dict[str, float]] = {}
        for item in snapshot["observations"]:
            selected_action = str(item.get("selected_action") or "unknown")
            bucket = action_buckets.setdefault(selected_action, {"count": 0.0, "records": 0.0, "avgSelectedScore": 0.0, "avgRisk": 0.0})
            bucket["count"] += 1.0
            outcome = item.get("outcome") or {}
            bucket["records"] += float((outcome or {}).get("recordsExtracted") or 0.0)
            bucket["avgSelectedScore"] += float(item.get("selected_action_score") or 0.0)
            bucket["avgRisk"] += float(item.get("risk_score") or 0.0)

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            reward_events = repository.export_reward_events(
                source_slug=source_slug,
                runtime_mode=runtime_mode,
                window_days=window_days,
                limit=limit or self._settings.crawler_replay_export_limit,
            )

        rewards_by_action: dict[str, dict[str, float]] = {}
        reward_stage_summary: dict[str, dict[str, float]] = {}
        terminal_components: dict[str, float] = {}
        for event in reward_events:
            action_type = str(event.get("action_type") or "unknown")
            action_bucket = rewards_by_action.setdefault(action_type, {"count": 0.0, "total": 0.0})
            action_bucket["count"] += 1.0
            action_bucket["total"] += float(event.get("reward_value") or 0.0)

            stage = str(event.get("reward_stage") or "unknown")
            stage_bucket = reward_stage_summary.setdefault(stage, {"count": 0.0, "total": 0.0})
            stage_bucket["count"] += 1.0
            stage_bucket["total"] += float(event.get("reward_value") or 0.0)

            if stage == "terminal":
                components = event.get("reward_components") or {}
                if isinstance(components, dict):
                    for key, value in components.items():
                        try:
                            terminal_components[str(key)] = round(
                                float(terminal_components.get(str(key), 0.0)) + float(value or 0.0),
                                4,
                            )
                        except (TypeError, ValueError):
                            continue

        replay = []
        for action, values in sorted(action_buckets.items(), key=lambda entry: (-entry[1]["records"], -entry[1]["count"])):
            reward_bucket = rewards_by_action.get(action, {"count": 0.0, "total": 0.0})
            replay.append(
                {
                    "actionType": action,
                    "count": int(values["count"]),
                    "avgRecords": round(values["records"] / max(values["count"], 1.0), 4),
                    "avgSelectedScore": round(values["avgSelectedScore"] / max(values["count"], 1.0), 4),
                    "avgRisk": round(values["avgRisk"] / max(values["count"], 1.0), 4),
                    "avgReward": round(reward_bucket["total"] / max(reward_bucket["count"], 1.0), 4),
                    "totalReward": round(reward_bucket["total"], 4),
                }
            )

        stage_rows = [
            {
                "stage": stage,
                "count": int(values["count"]),
                "avgReward": round(values["total"] / max(values["count"], 1.0), 4),
                "totalReward": round(values["total"], 4),
            }
            for stage, values in sorted(reward_stage_summary.items(), key=lambda item: -item[1]["count"])
        ]

        terminal_rows = [
            {"component": key, "total": round(value, 4)}
            for key, value in sorted(terminal_components.items(), key=lambda item: (-abs(item[1]), item[0]))
        ]

        return {
            "count": len(replay),
            "actions": replay,
            "rewardStages": stage_rows,
            "terminalBusinessSignals": terminal_rows,
        }

    def enrichment_replay_policy(
        self,
        *,
        limit: int | None = None,
        source_slug: str | None = None,
        field_name: str | None = None,
        window_days: int | None = None,
    ) -> dict[str, object]:
        snapshot = self.export_enrichment_observations(
            source_slug=source_slug,
            field_name=field_name,
            window_days=window_days,
            limit=limit,
        )
        observations = list(snapshot.get("observations") or [])
        recommended_counts: dict[str, int] = {}
        deterministic_counts: dict[str, int] = {}
        disagreement_counts: dict[str, int] = {}
        final_state_counts: dict[str, int] = {}
        business_signal_totals: dict[str, float] = {}
        samples: list[dict[str, object]] = []
        agreement_count = 0

        for item in observations:
            recommended = str(item.get("recommended_action") or "unknown")
            deterministic = str(item.get("deterministic_action") or "unknown")
            recommended_counts[recommended] = int(recommended_counts.get(recommended, 0) or 0) + 1
            deterministic_counts[deterministic] = int(deterministic_counts.get(deterministic, 0) or 0) + 1
            if recommended == deterministic:
                agreement_count += 1
            else:
                key = f"{deterministic}->{recommended}"
                disagreement_counts[key] = int(disagreement_counts.get(key, 0) or 0) + 1

            outcome = item.get("outcome") or {}
            final_state = str((outcome or {}).get("finalState") or "unknown")
            final_state_counts[final_state] = int(final_state_counts.get(final_state, 0) or 0) + 1
            signals = (outcome or {}).get("businessSignals") or {}
            if isinstance(signals, dict):
                for key, value in signals.items():
                    try:
                        business_signal_totals[str(key)] = round(float(business_signal_totals.get(str(key), 0.0)) + float(value or 0.0), 4)
                    except (TypeError, ValueError):
                        continue

            if len(samples) < min(max(5, int(limit or 25)), 15):
                samples.append(
                    {
                        "chapterSlug": item.get("chapter_slug"),
                        "fieldName": item.get("field_name"),
                        "recommendedAction": recommended,
                        "deterministicAction": deterministic,
                        "finalState": final_state,
                        "businessSignals": signals,
                    }
                )

        disagreement_rows = [
            {"transition": key, "count": value}
            for key, value in sorted(disagreement_counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        return {
            "count": len(observations),
            "agreementRate": round(agreement_count / max(len(observations), 1), 4),
            "recommendedActions": [
                {"actionType": key, "count": value}
                for key, value in sorted(recommended_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            "deterministicActions": [
                {"actionType": key, "count": value}
                for key, value in sorted(deterministic_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            "disagreements": disagreement_rows,
            "finalStates": [
                {"state": key, "count": value}
                for key, value in sorted(final_state_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            "businessSignals": [
                {"signal": key, "total": round(value, 4)}
                for key, value in sorted(business_signal_totals.items(), key=lambda item: (-abs(item[1]), item[0]))
            ],
            "samples": samples,
        }

    def enrichment_policy_compare_report(
        self,
        *,
        limit: int | None = None,
        source_slug: str | None = None,
        field_name: str | None = None,
        window_days: int | None = None,
    ) -> dict[str, object]:
        snapshot = self.export_enrichment_observations(
            source_slug=source_slug,
            field_name=field_name,
            window_days=window_days,
            limit=limit,
        )
        observations = list(snapshot.get("observations") or [])
        authoritative_actions = {"parse_supporting_page", "verify_school", "verify_website"}
        provider_actions = {"search_web", "search_social"}
        delay_actions = {"defer", "stop_no_signal", "review_required"}

        field_rows: dict[str, dict[str, object]] = {}
        source_rows: dict[str, dict[str, object]] = {}
        disagreement_rows: dict[str, dict[str, object]] = {}
        agreement_count = 0
        authoritative_recommended = 0
        authoritative_deterministic = 0
        provider_recommended = 0
        provider_deterministic = 0
        provider_waste_total = 0
        provider_waste_disagreements = 0
        provider_waste_authoritative_opportunities = 0
        provider_waste_delay_opportunities = 0
        disagreement_samples: list[dict[str, object]] = []

        for item in observations:
            recommended = str(item.get("recommended_action") or "unknown")
            deterministic = str(item.get("deterministic_action") or "unknown")
            field_key = str(item.get("field_name") or "unknown")
            source_key = str(item.get("source_slug") or "unknown")
            outcome = item.get("outcome") or {}
            signals = (outcome or {}).get("businessSignals") or {}
            final_state = str((outcome or {}).get("finalState") or "unknown")
            provider_waste = bool((signals or {}).get("provider_waste"))

            if recommended == deterministic:
                agreement_count += 1

            if recommended in authoritative_actions:
                authoritative_recommended += 1
            if deterministic in authoritative_actions:
                authoritative_deterministic += 1
            if recommended in provider_actions:
                provider_recommended += 1
            if deterministic in provider_actions:
                provider_deterministic += 1
            if provider_waste:
                provider_waste_total += 1

            field_bucket = field_rows.setdefault(
                field_key,
                {
                    "fieldName": field_key,
                    "count": 0,
                    "agreementCount": 0,
                    "providerWasteCount": 0,
                    "recommendedAuthoritativeCount": 0,
                    "deterministicAuthoritativeCount": 0,
                },
            )
            field_bucket["count"] = int(field_bucket["count"]) + 1
            if recommended == deterministic:
                field_bucket["agreementCount"] = int(field_bucket["agreementCount"]) + 1
            if provider_waste:
                field_bucket["providerWasteCount"] = int(field_bucket["providerWasteCount"]) + 1
            if recommended in authoritative_actions:
                field_bucket["recommendedAuthoritativeCount"] = int(field_bucket["recommendedAuthoritativeCount"]) + 1
            if deterministic in authoritative_actions:
                field_bucket["deterministicAuthoritativeCount"] = int(field_bucket["deterministicAuthoritativeCount"]) + 1

            source_bucket = source_rows.setdefault(
                source_key,
                {
                    "sourceSlug": source_key,
                    "count": 0,
                    "agreementCount": 0,
                    "providerWasteCount": 0,
                },
            )
            source_bucket["count"] = int(source_bucket["count"]) + 1
            if recommended == deterministic:
                source_bucket["agreementCount"] = int(source_bucket["agreementCount"]) + 1
            if provider_waste:
                source_bucket["providerWasteCount"] = int(source_bucket["providerWasteCount"]) + 1

            if recommended != deterministic:
                key = f"{deterministic}->{recommended}"
                disagreement_bucket = disagreement_rows.setdefault(
                    key,
                    {
                        "transition": key,
                        "count": 0,
                        "fieldBreakdown": {},
                        "providerWasteCount": 0,
                        "authoritativeShiftCount": 0,
                        "delayShiftCount": 0,
                    },
                )
                disagreement_bucket["count"] = int(disagreement_bucket["count"]) + 1
                field_breakdown = dict(disagreement_bucket.get("fieldBreakdown") or {})
                field_breakdown[field_key] = int(field_breakdown.get(field_key, 0) or 0) + 1
                disagreement_bucket["fieldBreakdown"] = field_breakdown
                if provider_waste:
                    disagreement_rows[key]["providerWasteCount"] = int(disagreement_bucket["providerWasteCount"]) + 1
                    provider_waste_disagreements += 1
                if recommended in authoritative_actions and deterministic not in authoritative_actions:
                    disagreement_rows[key]["authoritativeShiftCount"] = int(disagreement_bucket["authoritativeShiftCount"]) + 1
                    if provider_waste:
                        provider_waste_authoritative_opportunities += 1
                if recommended in delay_actions and deterministic in provider_actions:
                    disagreement_rows[key]["delayShiftCount"] = int(disagreement_bucket["delayShiftCount"]) + 1
                    if provider_waste:
                        provider_waste_delay_opportunities += 1
                if len(disagreement_samples) < min(max(5, int(limit or 25)), 20):
                    disagreement_samples.append(
                        {
                            "chapterSlug": item.get("chapter_slug"),
                            "sourceSlug": source_key,
                            "fieldName": field_key,
                            "recommendedAction": recommended,
                            "deterministicAction": deterministic,
                            "finalState": final_state,
                            "providerWaste": provider_waste,
                            "supportingPagePresent": bool((item.get("context_features") or {}).get("supporting_page_present")),
                            "providerWindowHealthy": bool((item.get("context_features") or {}).get("provider_window_healthy")),
                        }
                    )

        count = len(observations)
        return {
            "count": count,
            "agreementRate": round(agreement_count / max(count, 1), 4),
            "recommendedAuthoritativeRate": round(authoritative_recommended / max(count, 1), 4),
            "deterministicAuthoritativeRate": round(authoritative_deterministic / max(count, 1), 4),
            "recommendedProviderSearchRate": round(provider_recommended / max(count, 1), 4),
            "deterministicProviderSearchRate": round(provider_deterministic / max(count, 1), 4),
            "providerWasteRate": round(provider_waste_total / max(count, 1), 4),
            "providerWasteDisagreementRate": round(provider_waste_disagreements / max(count, 1), 4),
            "providerWasteAuthoritativeOpportunityRate": round(provider_waste_authoritative_opportunities / max(count, 1), 4),
            "providerWasteDelayOpportunityRate": round(provider_waste_delay_opportunities / max(count, 1), 4),
            "byField": [
                {
                    "fieldName": str(row["fieldName"]),
                    "count": int(row["count"]),
                    "agreementRate": round(int(row["agreementCount"]) / max(int(row["count"]), 1), 4),
                    "providerWasteRate": round(int(row["providerWasteCount"]) / max(int(row["count"]), 1), 4),
                    "recommendedAuthoritativeRate": round(
                        int(row["recommendedAuthoritativeCount"]) / max(int(row["count"]), 1),
                        4,
                    ),
                    "deterministicAuthoritativeRate": round(
                        int(row["deterministicAuthoritativeCount"]) / max(int(row["count"]), 1),
                        4,
                    ),
                }
                for row in sorted(
                    field_rows.values(),
                    key=lambda item: (-int(item["count"]), str(item["fieldName"])),
                )
            ],
            "bySource": [
                {
                    "sourceSlug": str(row["sourceSlug"]),
                    "count": int(row["count"]),
                    "agreementRate": round(int(row["agreementCount"]) / max(int(row["count"]), 1), 4),
                    "providerWasteRate": round(int(row["providerWasteCount"]) / max(int(row["count"]), 1), 4),
                }
                for row in sorted(
                    source_rows.values(),
                    key=lambda item: (-int(item["count"]), str(item["sourceSlug"])),
                )
            ],
            "disagreements": [
                {
                    "transition": str(row["transition"]),
                    "count": int(row["count"]),
                    "providerWasteCount": int(row["providerWasteCount"]),
                    "authoritativeShiftCount": int(row["authoritativeShiftCount"]),
                    "delayShiftCount": int(row["delayShiftCount"]),
                    "fieldBreakdown": [
                        {"fieldName": key, "count": value}
                        for key, value in sorted(
                            dict(row["fieldBreakdown"]).items(),
                            key=lambda item: (-int(item[1]), str(item[0])),
                        )
                    ],
                }
                for row in sorted(
                    disagreement_rows.values(),
                    key=lambda item: (-int(item["count"]), str(item["transition"])),
                )
            ],
            "samples": disagreement_samples,
        }

    def enrichment_promote_verify_school_candidates(
        self,
        *,
        limit: int = 50,
        source_slug: str | None = None,
        field_name: str | None = None,
        apply_changes: bool = False,
        include_preflight: bool = True,
        probes: int | None = None,
        preflight_snapshot: dict[str, object] | None = None,
        provider_window_state: dict[str, object] | None = None,
    ) -> dict[str, object]:
        if preflight_snapshot is None and include_preflight:
            preflight_snapshot = self.search_preflight(probes=probes)

        effective_provider_window_state = provider_window_state or _provider_window_state_from_preflight(preflight_snapshot)
        effective_runtime_mode = self._settings.crawler_adaptive_train_default_runtime_mode or "adaptive_assisted"

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            policy = AdaptivePolicy(
                epsilon=self._settings.crawler_adaptive_epsilon,
                policy_version=self._settings.crawler_policy_version,
                live_epsilon=self._settings.crawler_adaptive_live_epsilon,
                train_epsilon=self._settings.crawler_adaptive_train_epsilon,
                risk_timeout_weight=self._settings.crawler_adaptive_risk_timeout_weight,
                risk_requeue_weight=self._settings.crawler_adaptive_risk_requeue_weight,
            )
            snapshot = repository.load_latest_policy_snapshot(
                policy_version=policy.policy_version,
                runtime_mode=effective_runtime_mode,
            )
            payload = snapshot.get("model_payload") if isinstance(snapshot, dict) else None
            if isinstance(payload, dict):
                policy.load_snapshot(payload)

            jobs = repository.list_queued_field_jobs_for_triage(
                limit=max(1, int(limit)),
                source_slug=source_slug,
                field_name=field_name,
            )

            total_jobs_considered = 0
            candidates: list[dict[str, object]] = []
            promoted = 0

            for job in jobs:
                if job.field_name not in {FIELD_JOB_FIND_WEBSITE, FIELD_JOB_FIND_INSTAGRAM}:
                    continue
                total_jobs_considered += 1
                if _job_supporting_page_ready(job):
                    continue
                if not (job.chapter_name and job.university_name and job.fraternity_slug):
                    continue
                if repository.has_pending_field_job(job.chapter_id, FIELD_JOB_VERIFY_SCHOOL):
                    continue

                context = _build_enrichment_shadow_context(job, effective_provider_window_state)
                provider_window_healthy = bool(context.get("provider_window_healthy"))
                if not provider_window_healthy:
                    cached_school_policy = repository.get_school_policy(job.university_name)
                    cached_chapter_activity = repository.get_chapter_activity(
                        fraternity_slug=job.fraternity_slug,
                        school_name=job.university_name,
                    )
                    has_decisive_cached_school_policy = _cached_school_policy_is_decisive(cached_school_policy)
                    has_decisive_cached_chapter_activity = _cached_chapter_activity_is_decisive(cached_chapter_activity)
                    if not has_decisive_cached_school_policy and not has_decisive_cached_chapter_activity:
                        continue
                decisions = policy.choose_action(
                    [
                        "parse_supporting_page",
                        "verify_school",
                        "verify_website",
                        "search_web",
                        "search_social",
                        "defer",
                        "stop_no_signal",
                        "review_required",
                    ],
                    context=context,
                    template_profile=None,
                    mode="adaptive_shadow",
                )
                if not decisions:
                    continue
                top_decision = decisions[0]
                if str(top_decision.action_type) != "verify_school":
                    continue

                deterministic_action = "search_social" if job.field_name == FIELD_JOB_FIND_INSTAGRAM else "search_web"
                if job.queue_state == "deferred":
                    contact_resolution = job.payload.get("contactResolution") if isinstance(job.payload.get("contactResolution"), dict) else {}
                    reason_code = str(contact_resolution.get("reasonCode") or "").strip()
                    if reason_code in {"provider_degraded", "transient_network", "dependency_wait", "website_required", "provider_low_signal"}:
                        deterministic_action = "defer"

                if apply_changes and job.crawl_run_id is not None and job.source_slug:
                    promoted += repository.create_field_jobs(
                        job.chapter_id,
                        job.crawl_run_id,
                        job.chapter_slug,
                        job.source_slug,
                        [FIELD_JOB_VERIFY_SCHOOL],
                    )

                if len(candidates) < 25:
                    candidates.append(
                        {
                            "chapterSlug": job.chapter_slug,
                            "sourceSlug": job.source_slug,
                            "fieldName": job.field_name,
                            "queueState": job.queue_state,
                            "recommendedAction": "verify_school",
                            "deterministicAction": deterministic_action,
                            "topActions": [
                                {
                                    "actionType": decision.action_type,
                                    "score": round(float(decision.score), 4),
                                }
                                for decision in decisions[:3]
                            ],
                            "context": {
                                "supportingPagePresent": bool(context.get("supporting_page_present")),
                                "providerWindowHealthy": bool(context.get("provider_window_healthy")),
                                "identityComplete": bool(context.get("identity_complete")),
                                "priorQueryCount": int(context.get("prior_query_count") or 0),
                            },
                        }
                    )

        return {
            "captured_at": _utc_now_iso(),
            "applyChanges": bool(apply_changes),
            "jobsConsidered": int(total_jobs_considered),
            "candidateCount": len(candidates) if not apply_changes else max(len(candidates), int(promoted)),
            "promotedVerifySchoolJobs": int(promoted),
            "samples": candidates,
        }
    def adaptive_replay_window(
        self,
        *,
        source_slugs: list[str],
        runtime_mode: str = "adaptive_assisted",
        window_days: int | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        effective_runtime_mode = self._resolve_runtime_mode(runtime_mode)
        slugs = _normalize_source_slugs(source_slugs)
        days = window_days if window_days is not None else self._settings.crawler_adaptive_replay_window_days
        batch_limit = limit if limit is not None else self._settings.crawler_adaptive_replay_batch_size

        observations: list[dict[str, object]] = []
        rewards: list[dict[str, object]] = []
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            for slug in slugs:
                observations.extend(
                    repository.export_crawl_observations(
                        source_slug=slug,
                        runtime_mode=effective_runtime_mode,
                        window_days=days,
                        limit=batch_limit,
                    )
                )
                rewards.extend(
                    repository.export_reward_events(
                        source_slug=slug,
                        runtime_mode=effective_runtime_mode,
                        window_days=days,
                        limit=batch_limit,
                    )
                )
        return {
            "sourceSlugs": slugs,
            "runtimeMode": effective_runtime_mode,
            "windowDays": days,
            "observations": observations,
            "rewards": rewards,
            "observationCount": len(observations),
            "rewardCount": len(rewards),
        }

    def adaptive_policy_diff(self, snapshot_id_a: int, snapshot_id_b: int) -> dict[str, object]:
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            return repository.adaptive_policy_diff(snapshot_id_a, snapshot_id_b)

    def adaptive_train_loop(
        self,
        *,
        rounds: int,
        epochs_per_round: int,
        train_source_slugs: list[str],
        eval_source_slugs: list[str],
        runtime_mode: str = "adaptive_assisted",
        cohort_label: str = "target-cohort",
        report_dir: str | None = None,
        eval_enrichment_limit_per_source: int | None = None,
        eval_enrichment_workers: int | None = None,
    ) -> dict[str, object]:
        total_rounds = max(1, rounds)
        reports: list[dict[str, object]] = []
        for round_index in range(1, total_rounds + 1):
            report_name = None
            if report_dir:
                report_name = f"{report_dir.rstrip('/')}/ADAPTIVE_TRAIN_LOOP_ROUND_{round_index:02d}.md"
            result = self.adaptive_train_eval(
                epochs=max(1, epochs_per_round),
                train_source_slugs=train_source_slugs,
                eval_source_slugs=eval_source_slugs,
                runtime_mode=runtime_mode,
                cohort_label=cohort_label,
                report_path=report_name,
                eval_enrichment_limit_per_source=eval_enrichment_limit_per_source,
                eval_enrichment_workers=eval_enrichment_workers,
            )
            reports.append(result)
        return {
            "rounds": total_rounds,
            "epochs_per_round": max(1, epochs_per_round),
            "runtime_mode": self._resolve_runtime_mode(runtime_mode),
            "cohort_label": cohort_label,
            "results": reports,
        }

    def _replay_update_policy(
        self,
        *,
        runtime_mode: str,
        policy_version: str,
        source_slugs: list[str],
        window_days: int,
        batch_size: int,
    ) -> dict[str, object]:
        effective_runtime_mode = self._resolve_runtime_mode(runtime_mode)
        replay = self.adaptive_replay_window(
            source_slugs=source_slugs,
            runtime_mode=effective_runtime_mode,
            window_days=window_days,
            limit=batch_size,
        )
        reward_rows = list(replay.get("rewards") or [])
        policy = AdaptivePolicy(
            epsilon=self._settings.crawler_adaptive_epsilon,
            policy_version=policy_version,
            live_epsilon=self._settings.crawler_adaptive_live_epsilon,
            train_epsilon=self._settings.crawler_adaptive_train_epsilon,
            risk_timeout_weight=self._settings.crawler_adaptive_risk_timeout_weight,
            risk_requeue_weight=self._settings.crawler_adaptive_risk_requeue_weight,
        )

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            snapshot = repository.load_latest_policy_snapshot(policy_version=policy_version, runtime_mode=effective_runtime_mode)
            if snapshot is not None:
                payload = snapshot.get("model_payload") if isinstance(snapshot, dict) else None
                if isinstance(payload, dict):
                    policy.load_snapshot(payload)

            seen = 0
            for event in reward_rows:
                action_type = str(event.get("action_type") or "review_branch")
                reward_value = float(event.get("reward_value") or 0.0)
                discount_factor = float(event.get("discount_factor") or 1.0)
                policy.observe(action_type, reward_value * discount_factor)
                seen += 1

            repository.save_policy_snapshot(
                policy_version=policy.policy_version,
                runtime_mode=effective_runtime_mode,
                feature_schema_version="adaptive-v2.1",
                model_payload=policy.snapshot(),
                metrics={
                    "kind": "replay_update",
                    "sources": source_slugs,
                    "windowDays": window_days,
                    "batchSize": batch_size,
                    "eventsApplied": seen,
                },
            )

        return {
            "eventsApplied": seen,
            "windowDays": window_days,
            "batchSize": batch_size,
            "runtimeMode": effective_runtime_mode,
            "sourceCount": len(source_slugs),
        }

    def _resolve_runtime_mode(self, runtime_mode: str | None) -> str:
        mode = (runtime_mode or self._settings.crawler_runtime_mode or "legacy").strip().lower()
        allowed = {"legacy", "adaptive_shadow", "adaptive_assisted", "adaptive_primary"}
        if mode not in allowed:
            return "legacy"
        if mode != "legacy" and not self._settings.crawler_adaptive_enabled and runtime_mode is None:
            return "legacy"
        return mode

    def _resolve_v3_crawl_runtime_mode(self, runtime_mode: str | None = None) -> str:
        mode = (runtime_mode or self._settings.crawler_v3_crawl_runtime_mode or "adaptive_primary").strip().lower()
        if mode not in {"legacy", "adaptive_shadow", "adaptive_assisted", "adaptive_primary"}:
            return "adaptive_primary"
        return mode

    def _resolve_v3_field_job_runtime_mode(self, runtime_mode: str | None = None) -> str:
        return self._resolve_field_job_runtime_mode(runtime_mode or self._settings.crawler_v3_field_job_runtime_mode)

    def _build_orchestrator(
        self,
        repository: CrawlerRepository,
        runtime_mode: str,
        *,
        policy_mode: str = "live",
        policy_version: str | None = None,
    ):
        if runtime_mode == "legacy":
            return CrawlOrchestrator(repository, HttpClient(self._settings), AdapterRegistry())
        return AdaptiveCrawlOrchestrator(
            repository,
            HttpClient(self._settings),
            AdapterRegistry(),
            settings=self._settings,
            runtime_mode=runtime_mode,
            policy_mode=policy_mode,
            policy_version=policy_version,
        )

    def _resolve_field_job_runtime_mode(self, runtime_mode: str | None = None) -> str:
        mode = (runtime_mode or self._settings.crawler_field_job_runtime_mode or "langgraph_primary").strip().lower()
        if mode not in {"legacy", "langgraph_shadow", "langgraph_primary"}:
            return "langgraph_primary"
        return mode

    def _resolve_field_job_graph_durability(self, graph_durability: str | None = None) -> str:
        durability = (graph_durability or self._settings.crawler_field_job_graph_durability or "sync").strip().lower()
        if durability not in {"exit", "async", "sync"}:
            return "sync"
        return durability

    def _resolve_field_job_policy_pack(self, source_slug: str | None) -> dict[str, object]:
        if not source_slug:
            return {"name": "default"}
        packs: dict[str, dict[str, object]] = {
            "sigma-alpha-epsilon-main": {
                "name": "wide_search_no_signal",
                "worker_cap": 2,
                "max_search_pages": 1,
                "email_max_queries": 3,
                "instagram_max_queries": 3,
            },
            "alpha-delta-gamma-main": {
                "name": "invalid_heavy_directory",
                "worker_cap": 2,
                "max_search_pages": 1,
                "email_max_queries": 2,
                "instagram_max_queries": 2,
                "force_long_repair_cooldown": True,
            },
            "pi-kappa-alpha-main": {
                "name": "backlog_burn_preserve",
                "worker_cap": 4,
                "max_search_pages": 2,
                "email_max_queries": 4,
                "instagram_max_queries": 4,
            },
        }
        return packs.get(source_slug, {"name": "default"})

    def _reconcile_field_job_queue(
        self,
        repository: CrawlerRepository,
        *,
        source_slug: str | None,
        field_name: str | None,
        limit: int,
        policy_pack: dict[str, object],
        preflight_snapshot: dict[str, object] | None = None,
    ) -> tuple[dict[str, int | bool], dict[str, int]]:
        triage_summary: dict[str, int | bool] = {
            "triaged": 0,
            "invalidCancelled": 0,
            "deferredLongCooldown": 0,
            "dependencyDeferred": 0,
            "dependencyUnlockPromoted": 0,
            "dependencyReactivatedFromExistingSupport": 0,
            "dependencyPrerequisitesCreated": 0,
            "dependencyPrerequisitesAlreadyPending": 0,
            "dependencyJobsLeftBlocked": 0,
            "repairQueued": 0,
            "repairIsolated": 0,
            "actionableRetained": 0,
            "typedStateBackfilled": 0,
            "repairBackfillIsolated": 0,
            "providerRetryCandidatesConsidered": 0,
            "providerRetryCandidatesAdmitted": 0,
            "providerRetryCandidatesSkipped": 0,
            "sourceInvaliditySaturated": False,
        }
        repair_summary: dict[str, int] = {
            "queued": 0,
            "running": 0,
            "promotedToCanonical": 0,
            "downgradedToProvisional": 0,
            "confirmedInvalid": 0,
            "repairExhausted": 0,
            "reconciledHistorical": 0,
        }
        typed_state_backfill = repository.backfill_field_job_typed_queue_state()
        triage_summary["typedStateBackfilled"] = int(typed_state_backfill.get("blocked_reason_populated", 0) or 0)
        triage_summary["repairBackfillIsolated"] = int(typed_state_backfill.get("blocked_repairable_rows", 0) or 0)

        jobs = repository.list_queued_field_jobs_for_triage(limit=max(1, limit), source_slug=source_slug, field_name=field_name)
        if not jobs:
            return triage_summary, repair_summary

        repairable_jobs_by_chapter: dict[str, list[FieldJob]] = {}
        triage_summary["triaged"] = len(jobs)

        for job in jobs:
            decision = _classify_field_job_identity(job)
            if decision.validity_class == "invalid_non_chapter":
                repository.patch_queued_field_job(
                    job.id,
                    payload_patch={
                        "queueTriage": {
                            "outcome": "cancel_invalid",
                            "validityClass": decision.validity_class,
                            "invalidReason": decision.invalid_reason,
                        },
                        "contactResolution": {
                            "queueState": "blocked_invalid",
                            "validityClass": decision.validity_class,
                            "reasonCode": decision.invalid_reason or "identity_semantically_invalid",
                        },
                    },
                    status="failed",
                    last_error=f"Canceled invalid historical field job: {decision.invalid_reason or 'invalid chapter identity'}",
                    terminal_failure=True,
                    completed_payload={
                        "status": "blocked_invalid",
                        "reasonCode": decision.invalid_reason or "identity_semantically_invalid",
                        "validityClass": decision.validity_class,
                    },
                )
                triage_summary["invalidCancelled"] = int(triage_summary["invalidCancelled"]) + 1
                repair_summary["reconciledHistorical"] += 1
                continue
            website_field_state = str((job.field_states or {}).get("website_url") or "").strip().lower()
            has_confident_website = bool(
                job.website_url
                and website_field_state not in {"", "missing", "low_confidence"}
            )
            has_dependency_support = _job_supporting_page_ready(job)
            if (
                decision.validity_class == "canonical_valid"
                and job.field_name == FIELD_JOB_FIND_EMAIL
                and bool(getattr(self._settings, "crawler_search_require_confident_website_for_email", True))
                and not has_confident_website
                and not has_dependency_support
            ):
                has_pending_website_job = repository.has_pending_field_job(job.chapter_id, FIELD_JOB_FIND_WEBSITE)
                reason_code = "dependency_wait" if has_pending_website_job else "website_required"
                queue_outcome = "defer_email_until_website" if has_pending_website_job else "defer_email_without_website"
                delay_seconds = max(300, int(getattr(self._settings, "crawler_search_dependency_wait_seconds", 300)))
                if not has_pending_website_job:
                    delay_seconds = max(delay_seconds, 1_800)
                repository.patch_queued_field_job(
                    job.id,
                    payload_patch={
                        "queueTriage": {
                            "outcome": queue_outcome,
                            "validityClass": decision.validity_class,
                        },
                        "contactResolution": {
                            "queueState": "blocked_dependency",
                            "validityClass": decision.validity_class,
                            "reasonCode": reason_code,
                        },
                    },
                    status="queued",
                    scheduled_delay_seconds=delay_seconds,
                    last_error=(
                        "Deferred until confident website discovery is available for email enrichment"
                        if has_pending_website_job
                        else "Deferred because a confident website is required before email enrichment can continue"
                    ),
                    terminal_failure=False,
                )
                triage_summary["dependencyDeferred"] = int(triage_summary["dependencyDeferred"]) + 1
                repair_summary["reconciledHistorical"] += 1
                continue
            if decision.validity_class == "canonical_valid":
                current_queue_state = str(job.queue_state or "actionable").strip().lower() or "actionable"
                current_reason_code = ""
                if isinstance(job.payload.get("contactResolution"), dict):
                    current_reason_code = str((job.payload.get("contactResolution") or {}).get("reasonCode") or "").strip()
                if (
                    current_queue_state == "actionable"
                    and not _preflight_snapshot_is_healthy(preflight_snapshot)
                    and _job_has_provider_retry_signature(job)
                    and not _job_is_degraded_authoritative_candidate(repository, job)
                ):
                    current_queue_state = "blocked_provider"
                    current_reason_code = current_reason_code or str(job.payload.get("transient_provider_last_reason") or "transient_network")
                next_queue_state = current_queue_state
                queue_outcome = "keep_deferred" if current_queue_state == "deferred" else "keep_actionable"
                scheduled_delay_seconds = 0 if current_queue_state != "deferred" else None
                last_error = "" if current_queue_state != "deferred" else None
                if current_queue_state in {"deferred", "blocked_provider", "blocked_dependency"} and current_reason_code in {
                    "provider_degraded",
                    "transient_network",
                    "provider_low_signal",
                    "dependency_wait",
                    "website_required",
                }:
                    can_reactivate = False
                    if current_reason_code in {"provider_degraded", "transient_network", "provider_low_signal"}:
                        triage_summary["providerRetryCandidatesConsidered"] = int(triage_summary["providerRetryCandidatesConsidered"]) + 1
                        can_reactivate = _preflight_snapshot_is_healthy(preflight_snapshot) or _job_is_degraded_authoritative_candidate(repository, job)
                    else:
                        can_reactivate = _job_supporting_page_ready(job) or _job_has_reusable_official_school_evidence(repository, job)
                    if can_reactivate:
                        next_queue_state = "actionable"
                        if current_reason_code in {"provider_degraded", "transient_network", "provider_low_signal"}:
                            triage_summary["providerRetryCandidatesAdmitted"] = int(triage_summary["providerRetryCandidatesAdmitted"]) + 1
                        else:
                            triage_summary["dependencyReactivatedFromExistingSupport"] = int(triage_summary["dependencyReactivatedFromExistingSupport"]) + 1
                    elif current_reason_code in {"provider_degraded", "transient_network", "provider_low_signal"}:
                        next_queue_state = "blocked_provider"
                        triage_summary["providerRetryCandidatesSkipped"] = int(triage_summary["providerRetryCandidatesSkipped"]) + 1
                    else:
                        next_queue_state = "blocked_dependency"
                    queue_outcome = "resume_actionable" if can_reactivate else "keep_deferred"
                    scheduled_delay_seconds = 0 if can_reactivate else (
                        max(
                            int(getattr(self._settings, "crawler_search_transient_long_cooldown_seconds", 900)),
                            int(getattr(self._settings, "crawler_search_dependency_wait_seconds", 300)),
                        )
                        if current_reason_code in {"provider_degraded", "transient_network", "provider_low_signal"}
                        else None
                    )
                    last_error = "" if can_reactivate else None
                repository.patch_queued_field_job(
                    job.id,
                    payload_patch={
                        "queueTriage": {
                            "outcome": queue_outcome,
                            "validityClass": decision.validity_class,
                        },
                        "contactResolution": {
                            "queueState": next_queue_state,
                            "validityClass": decision.validity_class,
                            **({"reasonCode": current_reason_code} if next_queue_state != "actionable" and current_reason_code else {}),
                        },
                    },
                    status="queued",
                    scheduled_delay_seconds=scheduled_delay_seconds,
                    last_error=last_error,
                    terminal_failure=False,
                )
                triage_summary["actionableRetained"] = int(triage_summary["actionableRetained"]) + 1
                if next_queue_state == "blocked_dependency" and current_reason_code in {"dependency_wait", "website_required"}:
                    triage_summary["dependencyDeferred"] = int(triage_summary["dependencyDeferred"]) + 1
                    if job.field_name in {FIELD_JOB_FIND_EMAIL, FIELD_JOB_VERIFY_WEBSITE}:
                        missing_fields: list[str] = []
                        has_support_ready = _job_supporting_page_ready(job)
                        if not has_support_ready and not repository.has_pending_field_job(job.chapter_id, FIELD_JOB_FIND_WEBSITE):
                            missing_fields.append(FIELD_JOB_FIND_WEBSITE)
                        elif not has_support_ready:
                            triage_summary["dependencyPrerequisitesAlreadyPending"] = int(triage_summary["dependencyPrerequisitesAlreadyPending"]) + 1
                        if (
                            not has_support_ready
                            and job.university_name
                            and not repository.has_pending_field_job(job.chapter_id, FIELD_JOB_VERIFY_SCHOOL)
                        ):
                            missing_fields.append(FIELD_JOB_VERIFY_SCHOOL)
                        elif not has_support_ready and job.university_name:
                            triage_summary["dependencyPrerequisitesAlreadyPending"] = int(triage_summary["dependencyPrerequisitesAlreadyPending"]) + 1
                        if missing_fields:
                            created_count = repository.create_field_jobs(
                                job.chapter_id,
                                job.crawl_run_id,
                                job.chapter_slug,
                                job.source_slug,
                                missing_fields,
                            )
                            triage_summary["dependencyUnlockPromoted"] = int(triage_summary["dependencyUnlockPromoted"]) + created_count
                            triage_summary["dependencyPrerequisitesCreated"] = int(triage_summary["dependencyPrerequisitesCreated"]) + created_count
                        else:
                            triage_summary["dependencyJobsLeftBlocked"] = int(triage_summary["dependencyJobsLeftBlocked"]) + 1
                repair_summary["reconciledHistorical"] += 1
                continue
            if decision.validity_class == "provisional_candidate":
                repository.patch_queued_field_job(
                    job.id,
                    payload_patch={
                        "queueTriage": {
                            "outcome": "defer_long_cooldown",
                            "validityClass": decision.validity_class,
                            "repairReason": decision.repair_reason,
                        },
                        "contactResolution": {
                            "queueState": "deferred",
                            "validityClass": decision.validity_class,
                            "reasonCode": decision.repair_reason or "broader_web_gated",
                        },
                    },
                    status="queued",
                    scheduled_delay_seconds=86_400,
                    last_error="Deferred provisional chapter until canonical identity is established",
                    terminal_failure=False,
                )
                triage_summary["deferredLongCooldown"] = int(triage_summary["deferredLongCooldown"]) + 1
                repair_summary["reconciledHistorical"] += 1
                continue
            repairable_jobs_by_chapter.setdefault(job.chapter_id, []).append(job)

        for chapter_jobs in repairable_jobs_by_chapter.values():
            seed_job = chapter_jobs[0]
            repair_summary["queued"] += len(chapter_jobs)
            repository.enqueue_chapter_repair_job(
                chapter_id=seed_job.chapter_id,
                source_slug=seed_job.source_slug,
                priority=max(job.priority for job in chapter_jobs),
                payload=self._build_chapter_repair_payload(seed_job, policy_pack=policy_pack),
            )
            delay_seconds = 900
            for job in chapter_jobs:
                repository.patch_queued_field_job(
                    job.id,
                    payload_patch={
                        "queueTriage": {
                            "outcome": "requires_entity_repair",
                            "validityClass": "repairable_candidate",
                            "repairReason": "queued_for_entity_repair",
                        },
                        "chapterRepair": {
                            "state": "queued",
                            "sourceSlug": seed_job.source_slug,
                        },
                        "contactResolution": {
                            "queueState": "blocked_repairable",
                            "validityClass": "repairable_candidate",
                            "reasonCode": "queued_for_entity_repair",
                        },
                    },
                    status="queued",
                    scheduled_delay_seconds=delay_seconds,
                    last_error="Deferred until chapter repair queue finishes",
                    terminal_failure=False,
                )
                triage_summary["repairQueued"] = int(triage_summary["repairQueued"]) + 1
                triage_summary["repairIsolated"] = int(triage_summary["repairIsolated"]) + 1
                repair_summary["reconciledHistorical"] += 1

        triaged = int(triage_summary["triaged"])
        blocked = int(triage_summary["invalidCancelled"]) + int(triage_summary["repairQueued"]) + int(triage_summary["deferredLongCooldown"])
        if triaged >= 12 and blocked / max(triaged, 1) >= 0.7:
            triage_summary["sourceInvaliditySaturated"] = True

        return triage_summary, repair_summary

    def _repair_chapter_identity(
        self,
        repository: CrawlerRepository,
        job: FieldJob,
        *,
        policy_pack: dict[str, object],
    ) -> dict[str, str]:
        snippets = repository.fetch_provenance_snippets(job.chapter_id)
        repaired_university = _infer_university_name_for_job(job, snippets)
        repair_family = _infer_repair_family(job, repaired_university=repaired_university)
        if repaired_university:
            repaired_decision = classify_chapter_validity(
                ExtractedChapter(
                    name=job.chapter_name,
                    university_name=repaired_university,
                    website_url=job.website_url,
                    instagram_url=job.instagram_url,
                    contact_email=job.contact_email,
                    source_url=(job.payload.get("sourceListUrl") if isinstance(job.payload.get("sourceListUrl"), str) else job.source_base_url) or "",
                    source_confidence=0.9,
                ),
                source_class="national",
                provenance="queue_repair",
            )
            if repaired_decision.validity_class == "canonical_valid":
                repository.update_chapter_identity_repair(
                    chapter_id=job.chapter_id,
                    university_name=repaired_university,
                    field_state_updates={"university_name": "found"},
                    validity_class="canonical_valid",
                    repair_metadata={
                        "status": "promoted_to_canonical_valid",
                        "sourceSlug": job.source_slug,
                        "policyPack": str(policy_pack.get("name") or "default"),
                    },
                )
                return {
                    "status": "promoted_to_canonical_valid",
                    "reason": "institutional_pattern_repair",
                    "repair_family": repair_family,
                }

        current_decision = _classify_field_job_identity(job)
        if current_decision.validity_class == "invalid_non_chapter":
            return {
                "status": "confirmed_invalid",
                "reason": current_decision.invalid_reason or "identity_semantically_invalid",
                "repair_family": repair_family,
            }

        if repaired_university:
            return {
                "status": "downgraded_to_provisional",
                "reason": "repair_not_canonical",
                "repair_family": repair_family,
            }
        return {
            "status": "repair_exhausted",
            "reason": "identity_semantically_incomplete",
            "repair_family": repair_family,
        }

    def _build_chapter_repair_payload(self, job: FieldJob, *, policy_pack: dict[str, object]) -> dict[str, object]:
        return {
            "origin": "historical_queue_reconciliation",
            "chapterSlug": job.chapter_slug,
            "chapterName": job.chapter_name,
            "sourceSlug": job.source_slug,
            "universityName": job.university_name,
            "websiteUrl": job.website_url,
            "instagramUrl": job.instagram_url,
            "contactEmail": job.contact_email,
            "sourceBaseUrl": job.source_base_url,
            "sourceListUrl": job.payload.get("sourceListUrl") if isinstance(job.payload.get("sourceListUrl"), str) else None,
            "policyPack": str(policy_pack.get("name") or "default"),
            "repairFamily": _infer_repair_family(job),
        }

    def _process_chapter_repair_queue(
        self,
        repository: CrawlerRepository,
        *,
        source_slug: str | None,
        limit: int,
        policy_pack: dict[str, object],
    ) -> dict[str, int]:
        summary = {
            "queued": 0,
            "running": 0,
            "promotedToCanonical": 0,
            "downgradedToProvisional": 0,
            "confirmedInvalid": 0,
            "repairExhausted": 0,
            "reconciledHistorical": 0,
            "statePrefixResolver": 0,
            "schoolNameNormalizer": 0,
            "chapterDesignationRepair": 0,
            "duplicateIdentityMerge": 0,
        }
        for _ in range(max(0, limit)):
            repair_job = repository.claim_next_chapter_repair_job(self._settings.crawler_field_job_worker_id, source_slug=source_slug)
            if repair_job is None:
                break
            summary["running"] += 1
            synthetic_job = FieldJob(
                id=repair_job.id,
                chapter_id=repair_job.chapter_id,
                chapter_slug=repair_job.chapter_slug,
                chapter_name=repair_job.chapter_name,
                field_name="verify_school",
                payload=dict(repair_job.payload or {}),
                attempts=repair_job.attempts,
                max_attempts=repair_job.max_attempts,
                claim_token=repair_job.claim_token,
                source_base_url=repair_job.payload.get("sourceBaseUrl") if isinstance(repair_job.payload.get("sourceBaseUrl"), str) else None,
                website_url=repair_job.website_url,
                instagram_url=repair_job.instagram_url,
                contact_email=repair_job.contact_email,
                source_slug=repair_job.source_slug,
                university_name=repair_job.university_name,
            )
            outcome = self._repair_chapter_identity(repository, synthetic_job, policy_pack=policy_pack)
            repair_family = str(outcome.get("repair_family") or synthetic_job.payload.get("repairFamily") or _infer_repair_family(synthetic_job)).strip() or "chapter_designation_repair"
            related_jobs = repository.list_queued_field_jobs_for_chapter(repair_job.chapter_id)
            affected_count = max(1, len(related_jobs))

            if outcome["status"] == "promoted_to_canonical_valid":
                for job in related_jobs:
                    website_field_state = str((job.field_states or {}).get("website_url") or "").strip().lower()
                    has_confident_website = bool(
                        job.website_url
                        and website_field_state not in {"", "missing", "low_confidence"}
                    )
                    defer_email_for_missing_website = bool(
                        job.field_name == FIELD_JOB_FIND_EMAIL
                        and getattr(self._settings, "crawler_search_require_confident_website_for_email", True)
                        and not has_confident_website
                    )
                    repository.patch_queued_field_job(
                        job.id,
                        payload_patch={
                            "queueTriage": {
                                "outcome": "defer_email_without_website" if defer_email_for_missing_website else "keep_actionable",
                                "validityClass": "canonical_valid",
                                "repairOutcome": outcome["status"],
                                "repairFamily": repair_family,
                            },
                            "chapterRepair": {
                                "state": "promoted_to_canonical_valid",
                                "family": repair_family,
                            },
                            "contactResolution": {
                                "queueState": "deferred" if defer_email_for_missing_website else "actionable",
                                "validityClass": "canonical_valid",
                                **({"reasonCode": "website_required"} if defer_email_for_missing_website else {}),
                            },
                        },
                        status="queued",
                        scheduled_delay_seconds=1_800 if defer_email_for_missing_website else 0,
                        last_error=(
                            "Deferred because a confident website is required before email enrichment can continue"
                            if defer_email_for_missing_website
                            else ""
                        ),
                        terminal_failure=False,
                    )
                repository.complete_chapter_repair_job(
                    repair_job,
                    repair_state="promoted_to_canonical_valid",
                    result_payload=outcome,
                )
                summary["promotedToCanonical"] += affected_count
                _increment_repair_family_summary(summary, repair_family, affected_count)
                continue

            if outcome["status"] == "confirmed_invalid":
                for job in related_jobs:
                    repository.patch_queued_field_job(
                        job.id,
                        payload_patch={
                            "queueTriage": {
                                "outcome": "cancel_invalid",
                                "validityClass": "invalid_non_chapter",
                                "invalidReason": outcome.get("reason") or "identity_semantically_invalid",
                                "repairOutcome": outcome["status"],
                                "repairFamily": repair_family,
                            },
                            "chapterRepair": {
                                "state": "confirmed_invalid",
                                "family": repair_family,
                            },
                            "contactResolution": {
                                "queueState": "blocked_invalid",
                                "validityClass": "invalid_non_chapter",
                                "reasonCode": outcome.get("reason") or "identity_semantically_invalid",
                            },
                        },
                        status="failed",
                        last_error=f"Canceled invalid repair candidate: {outcome.get('reason') or 'identity_semantically_invalid'}",
                        terminal_failure=True,
                        completed_payload={
                            "status": "blocked_invalid",
                            "reasonCode": outcome.get("reason") or "identity_semantically_invalid",
                            "validityClass": "invalid_non_chapter",
                        },
                    )
                repository.complete_chapter_repair_job(
                    repair_job,
                    repair_state="confirmed_invalid",
                    result_payload=outcome,
                )
                summary["confirmedInvalid"] += affected_count
                _increment_repair_family_summary(summary, repair_family, affected_count)
                continue

            next_state = "downgraded_to_provisional" if outcome["status"] == "downgraded_to_provisional" else "repair_exhausted"
            delay_seconds = 86_400 if policy_pack.get("force_long_repair_cooldown") else 43_200
            for job in related_jobs:
                repository.patch_queued_field_job(
                    job.id,
                    payload_patch={
                        "queueTriage": {
                            "outcome": "requires_entity_repair",
                            "validityClass": "repairable_candidate",
                            "repairReason": outcome.get("reason") or "identity_semantically_incomplete",
                            "repairOutcome": next_state,
                            "repairFamily": repair_family,
                        },
                        "chapterRepair": {
                            "state": next_state,
                            "family": repair_family,
                        },
                        "contactResolution": {
                            "queueState": "blocked_repairable",
                            "validityClass": "repairable_candidate",
                            "reasonCode": outcome.get("reason") or "repair_exhausted",
                        },
                    },
                    status="queued",
                    scheduled_delay_seconds=delay_seconds,
                    last_error="Deferred until chapter repair queue finishes",
                    terminal_failure=False,
                )
            repository.complete_chapter_repair_job(
                repair_job,
                repair_state=next_state,
                result_payload=outcome,
            )
            if next_state == "downgraded_to_provisional":
                summary["downgradedToProvisional"] += affected_count
            else:
                summary["repairExhausted"] += affected_count
            _increment_repair_family_summary(summary, repair_family, affected_count)

        return summary

    def process_field_jobs(
        self,
        limit: int = 25,
        source_slug: str | None = None,
        field_name: str | None = None,
        workers: int | None = None,
        require_healthy_search: bool = False,
        run_preflight: bool | None = None,
        runtime_mode: str | None = None,
        graph_durability: str | None = None,
    ) -> dict[str, object]:
        effective_workers = workers or self._settings.crawler_field_job_max_workers
        effective_runtime_mode = self._resolve_field_job_runtime_mode(runtime_mode)
        effective_graph_durability = self._resolve_field_job_graph_durability(graph_durability)
        degraded_mode = False
        preflight_enabled = self._settings.crawler_search_preflight_enabled if run_preflight is None else run_preflight
        preflight_snapshot: dict[str, object] | None = None
        before_metrics = None

        stale_jobs_recovered = 0
        stale_graph_runs_recovered = 0
        triage_summary: dict[str, int | bool] = {
            "triaged": 0,
            "invalidCancelled": 0,
            "deferredLongCooldown": 0,
            "repairQueued": 0,
            "actionableRetained": 0,
            "dependencyDeferred": 0,
            "dependencyUnlockPromoted": 0,
            "dependencyReactivatedFromExistingSupport": 0,
            "dependencyPrerequisitesCreated": 0,
            "dependencyPrerequisitesAlreadyPending": 0,
            "dependencyJobsLeftBlocked": 0,
            "providerRetryCandidatesConsidered": 0,
            "providerRetryCandidatesAdmitted": 0,
            "providerRetryCandidatesSkipped": 0,
            "sourceInvaliditySaturated": False,
        }
        repair_summary: dict[str, int] = {
            "queued": 0,
            "running": 0,
            "promotedToCanonical": 0,
            "downgradedToProvisional": 0,
            "confirmedInvalid": 0,
            "repairExhausted": 0,
            "reconciledHistorical": 0,
        }
        policy_pack = self._resolve_field_job_policy_pack(source_slug)
        field_job_worker_recoveries_started = 0
        initial_worker_processes = {"active_workers": 0, "stale_workers": 0}
        initial_queue_counts = {
            "queued_jobs": 0,
            "actionable_jobs": 0,
            "deferred_jobs": 0,
            "blocked_provider_jobs": 0,
            "blocked_dependency_jobs": 0,
            "blocked_repairable_jobs": 0,
            "running_jobs": 0,
        }

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            before_metrics = repository.get_accuracy_recovery_metrics()
            stale_jobs_recovered = repository.reconcile_stale_field_jobs(
                self._settings.crawler_field_job_stale_claim_minutes
            )
            if repository.field_job_graph_tables_ready():
                stale_graph_runs_recovered = repository.reconcile_stale_field_job_graph_runs(
                    self._settings.crawler_field_job_graph_run_stale_minutes
                )
            initial_queue_counts = repository.get_field_job_queue_counts()
            initial_worker_processes = repository.get_field_job_worker_process_stats()
            if (
                int(initial_queue_counts.get("actionable_jobs", 0) or 0) > 0
                and int(initial_queue_counts.get("running_jobs", 0) or 0) == 0
                and int(initial_worker_processes.get("active_workers", 0) or 0) == 0
            ):
                field_job_worker_recoveries_started = 1

        if stale_jobs_recovered or stale_graph_runs_recovered:
            log_event(
                LOGGER,
                "field_job_stale_runtime_state_reconciled",
                stale_jobs_recovered=stale_jobs_recovered,
                stale_graph_runs_recovered=stale_graph_runs_recovered,
                stale_claim_minutes=self._settings.crawler_field_job_stale_claim_minutes,
                stale_graph_run_minutes=self._settings.crawler_field_job_graph_run_stale_minutes,
                source_slug=source_slug,
                field_name=field_name,
            )

        if preflight_enabled and self._settings.crawler_search_enabled:
            preflight_snapshot = self.search_preflight()
            healthy = bool(preflight_snapshot.get("healthy", False))
            if not healthy:
                if require_healthy_search:
                    aggregate = {
                        "processed": 0,
                        "requeued": 0,
                        "failed_terminal": 0,
                        "runtime_fallback_count": 0,
                        "runtime_mode_used": effective_runtime_mode,
                        "provider_degraded_deferred": 0,
                        "dependency_wait_deferred": 0,
                        "supporting_page_resolved": 0,
                        "supporting_page_contact_resolved": 0,
                        "external_search_contact_resolved": 0,
                        "enrichment_observations_logged": 0,
                        "mid_batch_provider_rechecks": 0,
                        "mid_batch_provider_reorders": 0,
                        "preflight_probe_queries": _probe_queries_from_preflight(preflight_snapshot),
                        "chapter_search_queries": [],
                        "field_job_workers_active": int(initial_worker_processes.get("active_workers", 0) or 0),
                        "field_job_workers_stale": int(initial_worker_processes.get("stale_workers", 0) or 0),
                        "field_job_worker_recoveries_started": field_job_worker_recoveries_started,
                        "field_job_worker_alert_open": bool(
                            int(initial_queue_counts.get("actionable_jobs", 0) or 0) > 0
                            and int(initial_queue_counts.get("running_jobs", 0) or 0) == 0
                            and int(initial_worker_processes.get("active_workers", 0) or 0) == 0
                        ),
                    }
                    aggregate.update(_field_job_batch_delta_payload(before_metrics, before_metrics, processed=0))
                    aggregate["stale_jobs_recovered"] = stale_jobs_recovered
                    aggregate["stale_graph_runs_recovered"] = stale_graph_runs_recovered
                    aggregate["queue_triage"] = triage_summary
                    aggregate["chapter_repair"] = repair_summary
                    aggregate["policy_pack"] = str(policy_pack.get("name") or "default")
                    log_event(
                        LOGGER,
                        "field_job_batch_skipped_provider_degraded",
                        limit=limit,
                        source_slug=source_slug,
                        field_name=field_name,
                        workers=effective_workers,
                        runtime_mode=effective_runtime_mode,
                        graph_durability=effective_graph_durability,
                        preflight=preflight_snapshot,
                    )
                    return aggregate
                degraded_mode = True
                effective_workers = max(1, min(effective_workers, self._settings.crawler_search_degraded_worker_cap))
        if policy_pack.get("worker_cap") is not None:
            effective_workers = max(1, min(effective_workers, int(policy_pack["worker_cap"])))

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            triage_summary, repair_summary = self._reconcile_field_job_queue(
                repository,
                source_slug=source_slug,
                field_name=field_name,
                limit=max(limit * 30, 2_000) if source_slug is None else max(limit * 20, 1_000),
                policy_pack=policy_pack,
                preflight_snapshot=preflight_snapshot,
            )
            queued_repair_summary = self._process_chapter_repair_queue(
                repository,
                source_slug=source_slug,
                limit=max(1, min(limit, max(1, effective_workers))),
                policy_pack=policy_pack,
            )
            for key, value in queued_repair_summary.items():
                repair_summary[key] = int(repair_summary.get(key, 0) or 0) + int(value or 0)

        worker_limits = _distribute_limit(limit, effective_workers)
        if not worker_limits:
            aggregate = {
                "processed": 0,
                "requeued": 0,
                "failed_terminal": 0,
                "runtime_fallback_count": 0,
                "runtime_mode_used": effective_runtime_mode,
                "provider_degraded_deferred": 0,
                "dependency_wait_deferred": 0,
                "supporting_page_resolved": 0,
                "supporting_page_contact_resolved": 0,
                "external_search_contact_resolved": 0,
                "enrichment_observations_logged": 0,
                "mid_batch_provider_rechecks": 0,
                "mid_batch_provider_reorders": 0,
                "preflight_probe_queries": _probe_queries_from_preflight(preflight_snapshot),
                "chapter_search_queries": [],
                "queue_triage": triage_summary,
                "chapter_repair": repair_summary,
                "policy_pack": str(policy_pack.get("name") or "default"),
                "field_job_workers_active": 0,
                "field_job_workers_stale": int(initial_worker_processes.get("stale_workers", 0) or 0),
                "field_job_worker_recoveries_started": field_job_worker_recoveries_started,
                "field_job_worker_alert_open": bool(
                    int(initial_queue_counts.get("actionable_jobs", 0) or 0) > 0
                    and int(initial_queue_counts.get("running_jobs", 0) or 0) == 0
                ),
            }
            aggregate.update(_field_job_batch_delta_payload(before_metrics, before_metrics, processed=0))
            aggregate["stale_jobs_recovered"] = stale_jobs_recovered
            aggregate["stale_graph_runs_recovered"] = stale_graph_runs_recovered
            log_event(
                LOGGER,
                "field_job_batch_finished",
                limit=limit,
                source_slug=source_slug,
                field_name=field_name,
                workers=0,
                degraded_mode=degraded_mode,
                runtime_mode=effective_runtime_mode,
                graph_durability=effective_graph_durability,
                preflight=preflight_snapshot,
                **aggregate,
            )
            return aggregate

        supervisor = FieldJobSupervisorGraphRuntime(
            worker_limits=worker_limits,
            runtime_mode=effective_runtime_mode,
            graph_durability=effective_graph_durability,
            source_slug=source_slug,
            field_name=field_name,
            degraded_mode=degraded_mode,
            chunk_processor=lambda limit, source_slug, field_name, worker_index, total_workers, degraded_mode, runtime_mode, graph_durability: self._process_field_job_chunk(
                limit,
                source_slug,
                field_name,
                worker_index,
                total_workers,
                degraded_mode=degraded_mode,
                runtime_mode=runtime_mode,
                graph_durability=graph_durability,
                preflight_snapshot=preflight_snapshot,
            ),
        )
        aggregate = supervisor.run()

        with get_connection(self._settings) as connection:
            after_metrics = CrawlerRepository(connection).get_accuracy_recovery_metrics()

        aggregate["preflight_probe_queries"] = _merge_unique_texts(
            _probe_queries_from_preflight(preflight_snapshot),
            aggregate.get("preflight_probe_queries") or [],
        )
        aggregate.update(_field_job_batch_delta_payload(before_metrics, after_metrics, processed=int(aggregate.get("processed", 0) or 0)))
        aggregate["stale_jobs_recovered"] = stale_jobs_recovered
        aggregate["stale_graph_runs_recovered"] = stale_graph_runs_recovered
        aggregate["queue_triage"] = triage_summary
        aggregate["chapter_repair"] = repair_summary
        aggregate["policy_pack"] = str(policy_pack.get("name") or "default")
        aggregate["field_job_worker_recoveries_started"] = field_job_worker_recoveries_started
        aggregate["field_job_workers_active"] = max(len(worker_limits), int(initial_worker_processes.get("active_workers", 0) or 0))
        aggregate["field_job_workers_stale"] = int(initial_worker_processes.get("stale_workers", 0) or 0)
        aggregate["field_job_worker_alert_open"] = False
        aggregate["provider_window_state"] = aggregate.get("provider_window_state") or _provider_window_state_from_preflight(
            preflight_snapshot,
            degraded_mode=degraded_mode,
        )

        log_event(
            LOGGER,
            "field_job_batch_finished",
            limit=limit,
            source_slug=source_slug,
            field_name=field_name,
            workers=len(worker_limits),
            degraded_mode=degraded_mode,
            runtime_mode=effective_runtime_mode,
            graph_durability=effective_graph_durability,
            preflight=preflight_snapshot,
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
        degraded_mode: bool = False,
        runtime_mode: str = "legacy",
        graph_durability: str = "sync",
        preflight_snapshot: dict[str, object] | None = None,
    ) -> dict[str, object]:
        policy_pack = self._resolve_field_job_policy_pack(source_slug)
        current_search_settings = _search_settings_from_preflight(self._settings, preflight_snapshot)
        current_degraded_mode = degraded_mode
        current_provider_window_state = _provider_window_state_from_preflight(preflight_snapshot, degraded_mode=current_degraded_mode)
        aggregate: dict[str, object] = {
            "processed": 0,
            "requeued": 0,
            "failed_terminal": 0,
            "runtime_fallback_count": 0,
            "runtime_mode_used": runtime_mode,
            "provider_degraded_deferred": 0,
            "dependency_wait_deferred": 0,
            "supporting_page_resolved": 0,
            "supporting_page_contact_resolved": 0,
            "external_search_contact_resolved": 0,
            "enrichment_observations_logged": 0,
            "mid_batch_provider_rechecks": 0,
            "mid_batch_provider_reorders": 0,
            "degraded_authoritative_claimed": 0,
            "verify_school_cache_hit": 0,
            "verify_school_official_url_reused": 0,
            "verify_school_provider_search_attempted": 0,
            "preflight_probe_queries": [],
            "chapter_search_queries": [],
            "provider_window_state": current_provider_window_state,
        }
        remaining = max(0, int(limit))
        if remaining <= 0:
            return aggregate

        recheck_enabled = bool(
            self._settings.crawler_search_enabled
            and self._settings.crawler_search_mid_batch_recheck_enabled
            and remaining > 0
        )
        jobs_per_recheck = max(1, int(self._settings.crawler_search_mid_batch_recheck_every_jobs or 25))
        seconds_per_recheck = max(1, int(self._settings.crawler_search_mid_batch_recheck_every_seconds or 90))
        min_success_rate = float(self._settings.crawler_search_mid_batch_min_success_rate or 0.25)
        handled_since_recheck = 0
        window_started = time.monotonic()

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            request_repository = RequestGraphRepository(connection)
            worker_id = _worker_id(self._settings.crawler_field_job_worker_id, worker_index, total_workers)
            lease_seconds = max(30, int(self._settings.crawler_field_job_worker_lease_seconds))
            request_repository.upsert_worker_process(
                worker_id=worker_id,
                workload_lane="contact_resolution",
                runtime_owner=f"python_field_job_{runtime_mode}",
                status="active",
                lease_seconds=lease_seconds,
                metadata={
                    "sourceSlug": source_slug,
                    "fieldName": field_name,
                    "runtimeMode": runtime_mode,
                    "graphDurability": graph_durability,
                    "workerIndex": worker_index,
                    "totalWorkers": total_workers,
                    "degradedMode": current_degraded_mode,
                },
            )
            try:
                while remaining > 0:
                    segment_limit = min(remaining, jobs_per_recheck) if recheck_enabled and not current_degraded_mode else remaining
                    max_search_pages = self._settings.crawler_search_max_pages_per_job
                    dependency_wait_seconds = self._settings.crawler_search_dependency_wait_seconds
                    email_max_queries = min(self._settings.crawler_search_email_max_queries, 3)
                    instagram_max_queries = min(self._settings.crawler_search_instagram_max_queries, 3)
                    if policy_pack.get("max_search_pages") is not None:
                        max_search_pages = min(max_search_pages, int(policy_pack["max_search_pages"]))
                    if policy_pack.get("email_max_queries") is not None:
                        email_max_queries = min(email_max_queries, int(policy_pack["email_max_queries"]))
                    if policy_pack.get("instagram_max_queries") is not None:
                        instagram_max_queries = min(instagram_max_queries, int(policy_pack["instagram_max_queries"]))
                    if current_degraded_mode:
                        current_search_settings = current_search_settings.model_copy(
                            update={"crawler_search_max_results": current_search_settings.crawler_search_degraded_max_results}
                        )
                        max_search_pages = max(1, self._settings.crawler_search_degraded_max_pages_per_job)
                        dependency_wait_seconds = max(
                            self._settings.crawler_search_degraded_dependency_wait_seconds,
                            self._settings.crawler_search_dependency_wait_seconds,
                        )
                        email_max_queries = min(max(1, self._settings.crawler_search_degraded_email_max_queries), 3)
                        instagram_max_queries = min(max(1, self._settings.crawler_search_degraded_instagram_max_queries), 3)

                    adaptive_policy: AdaptivePolicy | None = None
                    if self._settings.crawler_adaptive_enrichment_observations_enabled:
                        adaptive_policy = AdaptivePolicy(
                            epsilon=self._settings.crawler_adaptive_epsilon,
                            policy_version=self._settings.crawler_policy_version,
                            live_epsilon=self._settings.crawler_adaptive_live_epsilon,
                            train_epsilon=self._settings.crawler_adaptive_train_epsilon,
                            risk_timeout_weight=self._settings.crawler_adaptive_risk_timeout_weight,
                            risk_requeue_weight=self._settings.crawler_adaptive_risk_requeue_weight,
                        )
                        snapshot = repository.load_latest_policy_snapshot(
                            policy_version=adaptive_policy.policy_version,
                            runtime_mode=self._settings.crawler_adaptive_train_default_runtime_mode or "adaptive_assisted",
                        )
                        payload = snapshot.get("model_payload") if isinstance(snapshot, dict) else None
                        if isinstance(payload, dict):
                            adaptive_policy.load_snapshot(payload)

                    engine = FieldJobEngine(
                        repository=repository,
                        logger=LOGGER,
                        worker_id=worker_id,
                        base_backoff_seconds=self._settings.crawler_field_job_base_backoff_seconds,
                        source_slug=source_slug,
                        field_name=field_name,
                        search_client=SearchClient(current_search_settings),
                        search_provider=self._settings.crawler_search_provider,
                        max_search_pages=max_search_pages,
                        negative_result_cooldown_days=self._settings.crawler_search_negative_cooldown_days,
                        dependency_wait_seconds=dependency_wait_seconds,
                        require_confident_website_for_email=self._settings.crawler_search_require_confident_website_for_email,
                        email_escape_on_provider_block=self._settings.crawler_search_email_escape_on_provider_block,
                        email_escape_min_website_failures=self._settings.crawler_search_email_escape_min_website_failures,
                        transient_short_retries=self._settings.crawler_search_transient_short_retries,
                        transient_long_cooldown_seconds=self._settings.crawler_search_transient_long_cooldown_seconds,
                        min_no_candidate_backoff_seconds=self._settings.crawler_search_min_no_candidate_backoff_seconds,
                        email_max_queries=email_max_queries,
                        instagram_max_queries=instagram_max_queries,
                        enable_school_initials=self._settings.crawler_search_enable_school_initials,
                        min_school_initial_length=self._settings.crawler_search_min_school_initial_length,
                        enable_compact_fraternity=self._settings.crawler_search_enable_compact_fraternity,
                        instagram_enable_handle_queries=self._settings.crawler_search_instagram_enable_handle_queries,
                        instagram_direct_probe_enabled=self._settings.crawler_search_instagram_direct_probe_enabled,
                        greedy_collect_mode=self._settings.crawler_greedy_collect,
                        search_degraded_mode=current_degraded_mode,
                        adaptive_policy=adaptive_policy,
                        adaptive_runtime_mode=runtime_mode,
                        adaptive_policy_mode="shadow",
                        adaptive_policy_version=self._settings.crawler_policy_version,
                        provider_window_state=current_provider_window_state,
                        enrichment_observations_enabled=self._settings.crawler_adaptive_enrichment_observations_enabled,
                    )
                    result = self._run_field_job_runtime(
                        repository=repository,
                        engine=engine,
                        worker_id=worker_id,
                        runtime_mode=runtime_mode,
                        graph_durability=graph_durability,
                        source_slug=source_slug,
                        field_name=field_name,
                        limit=segment_limit,
                    )
                    result.update(engine.consume_last_batch_metrics())
                    aggregate = _merge_field_job_chunk_results(aggregate, result)
                    request_repository.heartbeat_worker_process(worker_id, lease_seconds=lease_seconds)
                    handled = int(result.get("processed", 0) or 0) + int(result.get("requeued", 0) or 0) + int(result.get("failed_terminal", 0) or 0)
                    if handled <= 0:
                        break
                    remaining -= handled
                    handled_since_recheck += handled
                    if current_degraded_mode or not recheck_enabled or remaining <= 0:
                        continue
                    if handled_since_recheck < jobs_per_recheck and (time.monotonic() - window_started) < seconds_per_recheck:
                        continue

                    recheck_snapshot = self.search_preflight()
                    aggregate["mid_batch_provider_rechecks"] = int(aggregate.get("mid_batch_provider_rechecks", 0) or 0) + 1
                    aggregate["preflight_probe_queries"] = _merge_unique_texts(
                        aggregate.get("preflight_probe_queries") or [],
                        _probe_queries_from_preflight(recheck_snapshot),
                    )
                    reordered_settings = _reorder_search_settings_from_window(
                        current_search_settings,
                        recheck_snapshot,
                        min_success_rate=min_success_rate,
                    )
                    previous_order = _provider_order_from_settings(current_search_settings)
                    next_order = _provider_order_from_settings(reordered_settings)
                    if previous_order != next_order:
                        aggregate["mid_batch_provider_reorders"] = int(aggregate.get("mid_batch_provider_reorders", 0) or 0) + 1
                    current_search_settings = reordered_settings
                    if _all_attempted_providers_below_threshold(
                        recheck_snapshot,
                        current_search_settings,
                        min_success_rate=min_success_rate,
                    ):
                        current_degraded_mode = True
                    current_provider_window_state = _provider_window_state_from_preflight(
                        recheck_snapshot,
                        degraded_mode=current_degraded_mode,
                    )
                    aggregate["provider_window_state"] = current_provider_window_state
                    handled_since_recheck = 0
                    window_started = time.monotonic()
            finally:
                request_repository.stop_worker_process(worker_id, status="stopped")

        return aggregate

    def _run_field_job_runtime(
        self,
        *,
        repository: CrawlerRepository,
        engine: FieldJobEngine,
        worker_id: str,
        runtime_mode: str,
        graph_durability: str,
        source_slug: str | None,
        field_name: str | None,
        limit: int,
    ) -> dict[str, object]:
        if runtime_mode == "legacy":
            result = engine.process(limit=limit)
            result["runtime_fallback_count"] = 0
            result["runtime_mode_used"] = "legacy"
            return result
        if not repository.field_job_graph_tables_ready():
            log_event(
                LOGGER,
                "field_job_graph_runtime_fallback_missing_tables",
                level=logging.WARNING,
                runtime_mode=runtime_mode,
                worker_id=worker_id,
                source_slug=source_slug,
                field_name=field_name,
                error="field-job graph tables are unavailable",
            )
            result = engine.process(limit=limit)
            result["runtime_fallback_count"] = 1
            result["runtime_mode_used"] = "legacy"
            return result
        graph_runtime = FieldJobGraphRuntime(
            repository=repository,
            engine=engine,
            worker_id=worker_id,
            runtime_mode=runtime_mode,
            graph_durability=graph_durability,
            source_slug=source_slug,
            field_name=field_name,
        )
        try:
            result = graph_runtime.process(limit=limit)
            result["runtime_fallback_count"] = 0
            result["runtime_mode_used"] = runtime_mode
            return result
        except Exception as exc:  # pragma: no cover - runtime guardrail
            log_event(
                LOGGER,
                "field_job_graph_runtime_fallback_exception",
                level=logging.WARNING,
                runtime_mode=runtime_mode,
                worker_id=worker_id,
                source_slug=source_slug,
                field_name=field_name,
                error=str(exc),
            )
            result = engine.process(limit=limit)
            result["runtime_fallback_count"] = 1
            result["runtime_mode_used"] = "legacy"
            return result

    def search_preflight(self, probes: int | None = None) -> dict[str, object]:
        if not self._settings.crawler_search_enabled:
            return {
                "healthy": True,
                "success_rate": 1.0,
                "successes": 0,
                "probes": 0,
                "probe_outcomes": [],
                "reason": "search_disabled",
            }

        query_pool = _SEARCH_PREFLIGHT_QUERIES
        probe_count = max(1, min(len(query_pool), probes or self._settings.crawler_search_preflight_probe_count))
        selected_queries = query_pool[:probe_count]
        search_client = SearchClient(self._settings)

        successes = 0
        probe_outcomes: list[dict[str, object]] = []
        provider_health: dict[str, dict[str, object]] = {}
        for query in selected_queries:
            try:
                results = search_client.search(query, max_results=min(3, self._settings.crawler_search_max_results))
                provider_attempts = search_client.consume_last_provider_attempts()
                success = len(results) > 0
                if success:
                    successes += 1
                probe_outcomes.append(
                    {
                        "query": query,
                        "success": success,
                        "result_count": len(results),
                        "provider_attempts": provider_attempts,
                    }
                )
                for attempt in provider_attempts:
                    provider = str(attempt.get("provider") or "unknown")
                    bucket = provider_health.setdefault(
                        provider,
                        {"attempts": 0, "successes": 0, "unavailable": 0, "request_error": 0, "skipped": 0},
                    )
                    bucket["attempts"] = int(bucket["attempts"]) + 1
                    status = str(attempt.get("status") or "")
                    if status == "success":
                        bucket["successes"] = int(bucket["successes"]) + 1
                    elif status == "unavailable":
                        bucket["unavailable"] = int(bucket["unavailable"]) + 1
                    elif status == "request_error":
                        bucket["request_error"] = int(bucket["request_error"]) + 1
                    elif status == "skipped":
                        bucket["skipped"] = int(bucket["skipped"]) + 1
            except (SearchUnavailableError, requests.RequestException) as exc:
                provider_attempts = search_client.consume_last_provider_attempts()
                probe_outcomes.append(
                    {
                        "query": query,
                        "success": False,
                        "result_count": 0,
                        "error": str(exc),
                        "provider_attempts": provider_attempts,
                    }
                )
                for attempt in provider_attempts:
                    provider = str(attempt.get("provider") or "unknown")
                    bucket = provider_health.setdefault(
                        provider,
                        {"attempts": 0, "successes": 0, "unavailable": 0, "request_error": 0, "skipped": 0},
                    )
                    bucket["attempts"] = int(bucket["attempts"]) + 1
                    status = str(attempt.get("status") or "")
                    if status == "success":
                        bucket["successes"] = int(bucket["successes"]) + 1
                    elif status == "unavailable":
                        bucket["unavailable"] = int(bucket["unavailable"]) + 1
                    elif status == "request_error":
                        bucket["request_error"] = int(bucket["request_error"]) + 1
                    elif status == "skipped":
                        bucket["skipped"] = int(bucket["skipped"]) + 1

        success_rate = successes / probe_count if probe_count else 0.0
        for provider, bucket in provider_health.items():
            attempts = max(1, int(bucket["attempts"]))
            bucket["success_rate"] = round(float(bucket["successes"]) / attempts, 4)

        provider_mode = self._settings.crawler_search_provider.lower()
        provider_window_success_rate = _provider_window_success_rate(provider_health)
        primary_provider_success = any(
            int(provider_health.get(provider, {}).get("successes", 0)) > 0
            for provider in ("searxng_json", "tavily_api", "serper_api")
        )
        healthy = success_rate >= self._settings.crawler_search_preflight_min_success_rate
        if provider_mode in {"auto_free", "searxng_json", "tavily_api", "serper_api"}:
            if any(provider in provider_health for provider in ("searxng_json", "tavily_api", "serper_api")):
                healthy = healthy and primary_provider_success
        provider_window_min_success_rate = min(0.25, float(self._settings.crawler_search_preflight_min_success_rate))
        if provider_health:
            healthy = healthy and provider_window_success_rate >= provider_window_min_success_rate
        captured_at = _utc_now_iso()
        snapshot = {
            "captured_at": captured_at,
            "healthy": healthy,
            "success_rate": round(success_rate, 4),
            "provider_window_success_rate": provider_window_success_rate,
            "successes": successes,
            "probes": probe_count,
            "min_success_rate": self._settings.crawler_search_preflight_min_success_rate,
            "provider_health": provider_health,
            "probe_outcomes": probe_outcomes,
        }
        if not healthy:
            if provider_health and provider_window_success_rate < provider_window_min_success_rate:
                snapshot["reason"] = "provider_window_success_below_threshold"
            elif provider_mode in {"auto_free", "searxng_json", "tavily_api", "serper_api"} and not primary_provider_success:
                snapshot["reason"] = "primary_provider_success_missing"
            else:
                snapshot["reason"] = "probe_success_below_threshold"
        snapshot["provider_window_state"] = _provider_window_state_from_preflight(snapshot)
        log_event(LOGGER, "search_preflight_completed", **snapshot)
        return snapshot

    def system_baseline(self, *, include_preflight: bool = True, probes: int | None = None) -> dict[str, object]:
        preflight_snapshot = self.search_preflight(probes=probes) if include_preflight else None
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            accuracy = _accuracy_recovery_metrics_payload(repository.get_accuracy_recovery_metrics())
            national_profiles = repository.list_national_profiles(limit=1000)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      COUNT(*) FILTER (WHERE status = 'queued')::int AS queued_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_provider')::int AS blocked_provider_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_dependency')::int AS blocked_dependency_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_repairable')::int AS blocked_repairable_jobs,
                      COUNT(*) FILTER (WHERE status = 'running')::int AS running_jobs,
                      COUNT(*) FILTER (WHERE status = 'done')::int AS done_jobs,
                      COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs,
                      COUNT(*) FILTER (WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'updated')::int AS updated_jobs,
                      COUNT(*) FILTER (WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'review_required')::int AS review_jobs,
                      COUNT(*) FILTER (WHERE status = 'done' AND COALESCE(terminal_outcome, '') = 'terminal_no_signal')::int AS terminal_no_signal_jobs
                    FROM field_jobs
                    """
                )
                queue = dict(cursor.fetchone() or {})
                cursor.execute(
                    """
                    SELECT
                      field_name,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_provider')::int AS blocked_provider_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_dependency')::int AS blocked_dependency_jobs,
                      COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_repairable')::int AS blocked_repairable_jobs,
                      COUNT(*) FILTER (WHERE status = 'done')::int AS done_jobs,
                      COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs
                    FROM field_jobs
                    WHERE field_name IN ('find_website', 'verify_website', 'find_instagram', 'find_email', 'verify_school_match')
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
                field_breakdown = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT
                      COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') AS blocked_reason,
                      COUNT(*)::int AS count,
                      CASE
                        WHEN COALESCE(queue_state, 'actionable') = 'blocked_repairable'
                          OR COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') IN ('queued_for_entity_repair', 'identity_semantically_incomplete', 'repair_exhausted')
                          THEN 'repair_backlog'
                        WHEN COALESCE(queue_state, 'actionable') = 'blocked_provider'
                          THEN 'provider_dependent_search'
                        WHEN COALESCE(queue_state, 'actionable') = 'blocked_dependency'
                          THEN 'dependency_blocked'
                        WHEN COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') IN ('provider_degraded', 'transient_network', 'provider_low_signal')
                          THEN 'provider_dependent_search'
                        WHEN COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') IN ('dependency_wait', 'website_required')
                          THEN 'dependency_blocked'
                        ELSE 'authoritative_resolution'
                      END AS queue_lane
                    FROM field_jobs
                    WHERE status = 'queued'
                      AND COALESCE(queue_state, 'actionable') IN ('deferred', 'blocked_repairable', 'blocked_provider', 'blocked_dependency')
                    GROUP BY 1, 3
                    ORDER BY 2 DESC, 1 ASC
                    LIMIT 20
                    """
                )
                deferred_reason_breakdown = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT
                      field_name,
                      COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') AS blocked_reason,
                      COUNT(*)::int AS count,
                      CASE
                        WHEN COALESCE(queue_state, 'actionable') = 'blocked_repairable'
                          OR COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') IN ('queued_for_entity_repair', 'identity_semantically_incomplete', 'repair_exhausted')
                          THEN 'repair_backlog'
                        WHEN COALESCE(queue_state, 'actionable') = 'blocked_provider'
                          THEN 'provider_dependent_search'
                        WHEN COALESCE(queue_state, 'actionable') = 'blocked_dependency'
                          THEN 'dependency_blocked'
                        WHEN COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') IN ('provider_degraded', 'transient_network', 'provider_low_signal')
                          THEN 'provider_dependent_search'
                        WHEN COALESCE(NULLIF(BTRIM(blocked_reason), ''), 'unknown') IN ('dependency_wait', 'website_required')
                          THEN 'dependency_blocked'
                        ELSE 'authoritative_resolution'
                      END AS queue_lane
                    FROM field_jobs
                    WHERE status = 'queued'
                      AND COALESCE(queue_state, 'actionable') IN ('deferred', 'blocked_repairable', 'blocked_provider', 'blocked_dependency')
                    GROUP BY 1, 2, 4
                    ORDER BY 3 DESC, 1 ASC, 2 ASC
                    LIMIT 20
                    """
                )
                deferred_field_breakdown = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT
                      COALESCE(s.slug, 'unknown') AS source_slug,
                      CASE
                        WHEN COALESCE(fj.queue_state, 'actionable') = 'blocked_repairable'
                          OR COALESCE(NULLIF(BTRIM(fj.blocked_reason), ''), 'unknown') IN ('queued_for_entity_repair', 'identity_semantically_incomplete', 'repair_exhausted')
                          THEN 'repair_backlog'
                        WHEN COALESCE(fj.queue_state, 'actionable') = 'blocked_provider'
                          THEN 'provider_dependent_search'
                        WHEN COALESCE(fj.queue_state, 'actionable') = 'blocked_dependency'
                          THEN 'dependency_blocked'
                        WHEN COALESCE(NULLIF(BTRIM(fj.blocked_reason), ''), 'unknown') IN ('provider_degraded', 'transient_network', 'provider_low_signal')
                          THEN 'provider_dependent_search'
                        WHEN COALESCE(NULLIF(BTRIM(fj.blocked_reason), ''), 'unknown') IN ('dependency_wait', 'website_required')
                          THEN 'dependency_blocked'
                        ELSE 'authoritative_resolution'
                      END AS queue_lane,
                      COUNT(*)::int AS count
                    FROM field_jobs fj
                    LEFT JOIN crawl_runs cr ON cr.id = fj.crawl_run_id
                    LEFT JOIN sources s ON s.id = cr.source_id
                    WHERE fj.status = 'queued'
                      AND COALESCE(fj.queue_state, 'actionable') IN ('deferred', 'blocked_repairable', 'blocked_provider', 'blocked_dependency')
                    GROUP BY 1, 2
                    ORDER BY 3 DESC, 1 ASC
                    LIMIT 20
                    """
                )
                deferred_source_breakdown = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT
                      CASE
                        WHEN NOW() - COALESCE(created_at, scheduled_at, NOW()) < INTERVAL '1 hour' THEN 'lt_1h'
                        WHEN NOW() - COALESCE(created_at, scheduled_at, NOW()) < INTERVAL '6 hours' THEN '1h_to_6h'
                        WHEN NOW() - COALESCE(created_at, scheduled_at, NOW()) < INTERVAL '24 hours' THEN '6h_to_24h'
                        WHEN NOW() - COALESCE(created_at, scheduled_at, NOW()) < INTERVAL '72 hours' THEN '1d_to_3d'
                        ELSE 'gte_3d'
                      END AS age_bucket,
                      COUNT(*)::int AS count
                    FROM field_jobs
                    WHERE status = 'queued'
                      AND COALESCE(queue_state, 'actionable') IN ('deferred', 'blocked_repairable', 'blocked_provider', 'blocked_dependency')
                    GROUP BY 1
                    ORDER BY count DESC, age_bucket ASC
                    """
                )
                deferred_age_buckets = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT
                      COUNT(*) FILTER (
                        WHERE status = 'active'
                          AND workload_lane = 'contact_resolution'
                          AND (lease_expires_at IS NULL OR lease_expires_at > NOW())
                      )::int AS active_workers,
                      COUNT(*) FILTER (
                        WHERE workload_lane = 'contact_resolution'
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at <= NOW()
                      )::int AS stale_workers
                    FROM worker_processes
                    """
                )
                field_worker_processes = dict(cursor.fetchone() or {})
                cursor.execute(
                    """
                    SELECT
                      COALESCE(metadata ->> 'reasonCode', 'unknown') AS reason_code,
                      COUNT(*)::int AS count
                    FROM chapter_evidence
                    GROUP BY 1
                    ORDER BY 2 DESC, 1 ASC
                    LIMIT 12
                    """
                )
                reason_rows = [dict(row) for row in cursor.fetchall()]

        baseline = {
            "captured_at": _utc_now_iso(),
            "accuracy": accuracy,
            "queue": queue,
            "queue_health": _queue_health_payload(
                queue,
                deferred_reason_breakdown=deferred_reason_breakdown,
                field_worker_processes=field_worker_processes,
                liveness_alert_poll_windows=self._settings.crawler_field_job_liveness_alert_poll_windows,
            ),
            "field_breakdown": field_breakdown,
            "deferred_reason_breakdown": deferred_reason_breakdown,
            "deferred_field_breakdown": deferred_field_breakdown,
            "deferred_source_breakdown": deferred_source_breakdown,
            "deferred_age_buckets": deferred_age_buckets,
            "field_worker_processes": field_worker_processes,
            "national_profiles": {
                "total_profiles": len(national_profiles),
                "with_email": sum(1 for item in national_profiles if item.contact_email),
                "with_instagram": sum(1 for item in national_profiles if item.instagram_url),
                "with_phone": sum(1 for item in national_profiles if item.phone),
            },
            "top_evidence_reason_codes": reason_rows,
            "provenance_audit": self.provenance_completeness_audit(limit=25),
            "enrichment_shadow": self.enrichment_shadow_policy_report(
                limit=25,
                include_preflight=False,
                preflight_snapshot=preflight_snapshot,
            ),
            "enrichment_replay_compare": self.enrichment_policy_compare_report(limit=25),
        }
        if preflight_snapshot is not None:
            baseline["search_preflight"] = preflight_snapshot
        return baseline

    def provenance_completeness_audit(self, *, limit: int = 50) -> dict[str, object]:
        with get_connection(self._settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH flagged AS (
                        SELECT
                            c.slug AS chapter_slug,
                            f.slug AS fraternity_slug,
                            'website_url'::text AS field_name,
                            c.website_url::text AS field_value,
                            COALESCE(c.contact_provenance -> 'website_url' ->> 'reasonCode', '') AS reason_code,
                            COALESCE(c.contact_provenance -> 'website_url' ->> 'supportingPageScope', '') AS supporting_page_scope,
                            NULL::text AS national_value,
                            FALSE AS national_profile_collision,
                            CASE
                                WHEN c.website_url IS NOT NULL AND BTRIM(c.website_url) <> '' AND COALESCE(c.contact_provenance -> 'website_url' ->> 'reasonCode', '') = ''
                                    THEN 'missing_reason_code'
                                WHEN c.website_url IS NOT NULL AND BTRIM(c.website_url) <> '' AND COALESCE(c.contact_provenance -> 'website_url' ->> 'supportingPageScope', '') = ''
                                    THEN 'missing_supporting_page_scope'
                                ELSE NULL
                            END AS issue
                        FROM chapters c
                        JOIN fraternities f ON f.id = c.fraternity_id
                        UNION ALL
                        SELECT
                            c.slug AS chapter_slug,
                            f.slug AS fraternity_slug,
                            'contact_email'::text AS field_name,
                            c.contact_email::text AS field_value,
                            COALESCE(c.contact_provenance -> 'contact_email' ->> 'reasonCode', '') AS reason_code,
                            COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') AS supporting_page_scope,
                            np.contact_email::text AS national_value,
                            (
                                np.contact_email IS NOT NULL
                                AND c.contact_email IS NOT NULL
                                AND LOWER(np.contact_email) = LOWER(c.contact_email)
                                AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') NOT IN ('chapter_site', 'school_affiliation_page', 'nationals_chapter_page')
                            ) AS national_profile_collision,
                            CASE
                                WHEN c.contact_email IS NOT NULL AND BTRIM(c.contact_email) <> '' AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'reasonCode', '') = ''
                                    THEN 'missing_reason_code'
                                WHEN c.contact_email IS NOT NULL AND BTRIM(c.contact_email) <> '' AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') = ''
                                    THEN 'missing_supporting_page_scope'
                                WHEN np.contact_email IS NOT NULL
                                    AND c.contact_email IS NOT NULL
                                    AND LOWER(np.contact_email) = LOWER(c.contact_email)
                                    AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') NOT IN ('chapter_site', 'school_affiliation_page', 'nationals_chapter_page')
                                    THEN 'matches_national_profile'
                                ELSE NULL
                            END AS issue
                        FROM chapters c
                        JOIN fraternities f ON f.id = c.fraternity_id
                        LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
                        UNION ALL
                        SELECT
                            c.slug AS chapter_slug,
                            f.slug AS fraternity_slug,
                            'instagram_url'::text AS field_name,
                            c.instagram_url::text AS field_value,
                            COALESCE(c.contact_provenance -> 'instagram_url' ->> 'reasonCode', '') AS reason_code,
                            COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') AS supporting_page_scope,
                            np.instagram_url::text AS national_value,
                            (
                                np.instagram_url IS NOT NULL
                                AND c.instagram_url IS NOT NULL
                                AND LOWER(np.instagram_url) = LOWER(c.instagram_url)
                                AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') NOT IN ('chapter_site', 'school_affiliation_page', 'nationals_chapter_page')
                            ) AS national_profile_collision,
                            CASE
                                WHEN c.instagram_url IS NOT NULL AND BTRIM(c.instagram_url) <> '' AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'reasonCode', '') = ''
                                    THEN 'missing_reason_code'
                                WHEN c.instagram_url IS NOT NULL AND BTRIM(c.instagram_url) <> '' AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') = ''
                                    THEN 'missing_supporting_page_scope'
                                WHEN np.instagram_url IS NOT NULL
                                    AND c.instagram_url IS NOT NULL
                                    AND LOWER(np.instagram_url) = LOWER(c.instagram_url)
                                    AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') NOT IN ('chapter_site', 'school_affiliation_page', 'nationals_chapter_page')
                                    THEN 'matches_national_profile'
                                ELSE NULL
                            END AS issue
                        FROM chapters c
                        JOIN fraternities f ON f.id = c.fraternity_id
                        LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
                    )
                    SELECT issue, COUNT(*)::int AS count
                    FROM flagged
                    WHERE issue IS NOT NULL
                    GROUP BY 1
                    ORDER BY 2 DESC, 1 ASC
                    """
                )
                counts = {str(row["issue"]): int(row["count"]) for row in cursor.fetchall()}
                cursor.execute(
                    """
                    WITH flagged AS (
                        SELECT
                            c.slug AS chapter_slug,
                            f.slug AS fraternity_slug,
                            'website_url'::text AS field_name,
                            c.website_url::text AS field_value,
                            COALESCE(c.contact_provenance -> 'website_url' ->> 'reasonCode', '') AS reason_code,
                            COALESCE(c.contact_provenance -> 'website_url' ->> 'supportingPageScope', '') AS supporting_page_scope,
                            NULL::text AS national_value,
                            CASE
                                WHEN c.website_url IS NOT NULL AND BTRIM(c.website_url) <> '' AND COALESCE(c.contact_provenance -> 'website_url' ->> 'reasonCode', '') = ''
                                    THEN 'missing_reason_code'
                                WHEN c.website_url IS NOT NULL AND BTRIM(c.website_url) <> '' AND COALESCE(c.contact_provenance -> 'website_url' ->> 'supportingPageScope', '') = ''
                                    THEN 'missing_supporting_page_scope'
                                ELSE NULL
                            END AS issue
                        FROM chapters c
                        JOIN fraternities f ON f.id = c.fraternity_id
                        UNION ALL
                        SELECT
                            c.slug AS chapter_slug,
                            f.slug AS fraternity_slug,
                            'contact_email'::text AS field_name,
                            c.contact_email::text AS field_value,
                            COALESCE(c.contact_provenance -> 'contact_email' ->> 'reasonCode', '') AS reason_code,
                            COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') AS supporting_page_scope,
                            np.contact_email::text AS national_value,
                            CASE
                                WHEN c.contact_email IS NOT NULL AND BTRIM(c.contact_email) <> '' AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'reasonCode', '') = ''
                                    THEN 'missing_reason_code'
                                WHEN c.contact_email IS NOT NULL AND BTRIM(c.contact_email) <> '' AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') = ''
                                    THEN 'missing_supporting_page_scope'
                                WHEN np.contact_email IS NOT NULL
                                    AND c.contact_email IS NOT NULL
                                    AND LOWER(np.contact_email) = LOWER(c.contact_email)
                                    AND COALESCE(c.contact_provenance -> 'contact_email' ->> 'supportingPageScope', '') NOT IN ('chapter_site', 'school_affiliation_page', 'nationals_chapter_page')
                                    THEN 'matches_national_profile'
                                ELSE NULL
                            END AS issue
                        FROM chapters c
                        JOIN fraternities f ON f.id = c.fraternity_id
                        LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
                        UNION ALL
                        SELECT
                            c.slug AS chapter_slug,
                            f.slug AS fraternity_slug,
                            'instagram_url'::text AS field_name,
                            c.instagram_url::text AS field_value,
                            COALESCE(c.contact_provenance -> 'instagram_url' ->> 'reasonCode', '') AS reason_code,
                            COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') AS supporting_page_scope,
                            np.instagram_url::text AS national_value,
                            CASE
                                WHEN c.instagram_url IS NOT NULL AND BTRIM(c.instagram_url) <> '' AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'reasonCode', '') = ''
                                    THEN 'missing_reason_code'
                                WHEN c.instagram_url IS NOT NULL AND BTRIM(c.instagram_url) <> '' AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') = ''
                                    THEN 'missing_supporting_page_scope'
                                WHEN np.instagram_url IS NOT NULL
                                    AND c.instagram_url IS NOT NULL
                                    AND LOWER(np.instagram_url) = LOWER(c.instagram_url)
                                    AND COALESCE(c.contact_provenance -> 'instagram_url' ->> 'supportingPageScope', '') NOT IN ('chapter_site', 'school_affiliation_page', 'nationals_chapter_page')
                                    THEN 'matches_national_profile'
                                ELSE NULL
                            END AS issue
                        FROM chapters c
                        JOIN fraternities f ON f.id = c.fraternity_id
                        LEFT JOIN national_profiles np ON np.fraternity_slug = f.slug
                    )
                    SELECT
                        issue,
                        chapter_slug,
                        fraternity_slug,
                        field_name,
                        field_value,
                        NULLIF(reason_code, '') AS reason_code,
                        NULLIF(supporting_page_scope, '') AS supporting_page_scope,
                        national_value
                    FROM flagged
                    WHERE issue IS NOT NULL
                    ORDER BY issue ASC, fraternity_slug ASC, chapter_slug ASC, field_name ASC
                    LIMIT %(limit)s
                    """,
                    {"limit": max(1, int(limit))},
                )
                samples = [dict(row) for row in cursor.fetchall()]

        return {
            "captured_at": _utc_now_iso(),
            "accepted_rows_missing_reason_code": int(counts.get("missing_reason_code", 0) or 0),
            "accepted_rows_missing_supporting_page_scope": int(counts.get("missing_supporting_page_scope", 0) or 0),
            "accepted_rows_matching_national_profile": int(counts.get("matches_national_profile", 0) or 0),
            "samples": samples,
        }

    def enrichment_shadow_policy_report(
        self,
        *,
        limit: int = 50,
        source_slug: str | None = None,
        field_name: str | None = None,
        include_preflight: bool = True,
        probes: int | None = None,
        preflight_snapshot: dict[str, object] | None = None,
    ) -> dict[str, object]:
        if preflight_snapshot is None and include_preflight:
            preflight_snapshot = self.search_preflight(probes=probes)

        provider_window_state = _provider_window_state_from_preflight(preflight_snapshot)
        effective_runtime_mode = self._settings.crawler_adaptive_train_default_runtime_mode or "adaptive_assisted"

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            policy = AdaptivePolicy(
                epsilon=self._settings.crawler_adaptive_epsilon,
                policy_version=self._settings.crawler_policy_version,
                live_epsilon=self._settings.crawler_adaptive_live_epsilon,
                train_epsilon=self._settings.crawler_adaptive_train_epsilon,
                risk_timeout_weight=self._settings.crawler_adaptive_risk_timeout_weight,
                risk_requeue_weight=self._settings.crawler_adaptive_risk_requeue_weight,
            )
            snapshot = repository.load_latest_policy_snapshot(
                policy_version=policy.policy_version,
                runtime_mode=effective_runtime_mode,
            )
            payload = snapshot.get("model_payload") if isinstance(snapshot, dict) else None
            if isinstance(payload, dict):
                policy.load_snapshot(payload)
            jobs = repository.list_queued_field_jobs_for_triage(
                limit=max(1, int(limit)),
                source_slug=source_slug,
                field_name=field_name,
            )

        recommended_actions: dict[str, int] = {}
        samples: list[dict[str, object]] = []
        authoritative_ready = 0
        provider_dependent = 0

        for job in jobs:
            context = _build_enrichment_shadow_context(job, provider_window_state)
            if bool(context.get("supporting_page_present")):
                authoritative_ready += 1
            else:
                provider_dependent += 1
            decisions = policy.choose_action(
                [
                    "parse_supporting_page",
                    "verify_school",
                    "verify_website",
                    "search_web",
                    "search_social",
                    "defer",
                    "stop_no_signal",
                    "review_required",
                ],
                context=context,
                template_profile=None,
                mode="adaptive_shadow",
            )
            if not decisions:
                continue
            selected = decisions[0]
            recommended_actions[selected.action_type] = int(recommended_actions.get(selected.action_type, 0) or 0) + 1
            if len(samples) < min(max(5, int(limit)), 15):
                samples.append(
                    {
                        "chapterSlug": job.chapter_slug,
                        "fieldName": job.field_name,
                        "queueState": job.queue_state,
                        "recommendedAction": selected.action_type,
                        "topActions": [
                            {
                                "actionType": decision.action_type,
                                "score": decision.score,
                            }
                            for decision in decisions[:3]
                        ],
                        "context": {
                            "supportingPagePresent": context.get("supporting_page_present"),
                            "supportingPageScope": context.get("supporting_page_scope"),
                            "providerWindowHealthy": context.get("provider_window_healthy"),
                            "websitePrerequisiteUnmet": context.get("website_prerequisite_unmet"),
                            "identityComplete": context.get("identity_complete"),
                            "priorQueryCount": context.get("prior_query_count"),
                        },
                    }
                )

        ranked_actions = [
            {"actionType": action_type, "count": count}
            for action_type, count in sorted(recommended_actions.items(), key=lambda item: (-item[1], item[0]))
        ]
        return {
            "captured_at": _utc_now_iso(),
            "jobs_considered": len(jobs),
            "authoritative_ready_jobs": authoritative_ready,
            "provider_dependent_jobs": provider_dependent,
            "provider_window_state": provider_window_state,
            "recommended_actions": ranked_actions,
            "samples": samples,
        }

    def discover_source(self, fraternity_name: str) -> dict[str, object]:
        search_client = SearchClient(self._settings)
        try:
            with get_connection(self._settings) as connection:
                repository = CrawlerRepository(connection)
                result = discover_source(
                    fraternity_name,
                    search_client,
                    repository=repository,
                    verified_min_confidence=self._settings.crawler_discovery_verified_min_confidence,
                )
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
                "source_provenance": None,
                "fallback_reason": "source_discovery_exception",
                "source_quality": {
                    "score": 0.0,
                    "is_weak": True,
                    "is_blocked": False,
                    "reasons": ["source_discovery_exception"],
                },
                "selected_candidate_rationale": None,
                "resolution_trace": [
                    {
                        "step": "source_discovery_exception",
                        "error": str(exc),
                    }
                ],
            }

    def bootstrap_verified_sources(self, input_path: str, dry_run: bool = False) -> dict[str, int]:
        payload = json.loads(Path(input_path).read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Bootstrap input must be a JSON array")

        inserted = 0
        skipped = 0

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            for row in payload:
                if not isinstance(row, dict):
                    skipped += 1
                    continue

                fraternity_name = str(row.get("name") or "").strip()
                if not fraternity_name:
                    skipped += 1
                    continue
                fraternity_slug = _slugify(fraternity_name)
                candidate_url, selected_reason = _select_registry_url(row)
                if not candidate_url:
                    skipped += 1
                    continue

                http_status = _coerce_int(row.get("status"))
                is_active = http_status is not None and 200 <= http_status < 400
                confidence = _bootstrap_confidence(http_status, selected_reason)
                metadata = {
                    "bootstrap_input": Path(input_path).name,
                    "selected_reason": selected_reason,
                    "base_url": row.get("base"),
                    "final_url": row.get("final_url"),
                    "error": row.get("error"),
                }

                if dry_run:
                    inserted += 1
                    continue

                repository.upsert_verified_source(
                    fraternity_slug=fraternity_slug,
                    fraternity_name=fraternity_name,
                    national_url=candidate_url,
                    origin="nic_bootstrap",
                    confidence=confidence,
                    http_status=http_status,
                    is_active=is_active,
                    metadata=metadata,
                )
                inserted += 1

        log_event(
            LOGGER,
            "verified_sources_bootstrap_finished",
            input_path=input_path,
            dry_run=dry_run,
            inserted=inserted,
            skipped=skipped,
        )
        return {"inserted": inserted, "skipped": skipped}

    def revalidate_verified_source(self, fraternity_slug: str) -> dict[str, object]:
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            record = repository.get_verified_source_by_slug(fraternity_slug)
            if record is None:
                raise ValueError(f"verified source not found for slug={fraternity_slug}")

            http_status, final_url, error = _probe_url(record.national_url, self._settings)
            is_active = http_status is not None and 200 <= http_status < 400
            confidence = record.confidence
            if http_status is not None and 200 <= http_status < 400:
                confidence = max(confidence, 0.75)
            elif http_status is not None and http_status >= 400:
                confidence = min(confidence, 0.49)

            metadata = dict(record.metadata or {})
            metadata["revalidated_at"] = _utc_now_iso()
            if error:
                metadata["revalidate_error"] = error

            updated = repository.upsert_verified_source(
                fraternity_slug=record.fraternity_slug,
                fraternity_name=record.fraternity_name,
                national_url=final_url or record.national_url,
                origin=record.origin,
                confidence=confidence,
                http_status=http_status if http_status is not None else record.http_status,
                is_active=is_active,
                metadata=metadata,
            )

        result = {
            "fraternity_slug": updated.fraternity_slug,
            "national_url": updated.national_url,
            "http_status": updated.http_status,
            "is_active": updated.is_active,
            "confidence": updated.confidence,
        }
        log_event(LOGGER, "verified_source_revalidated", **result)
        return result

    def revalidate_verified_sources(self, limit: int = 20) -> dict[str, int]:
        revalidated = 0
        failed = 0
        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            records = repository.list_verified_sources(limit=limit)
            slugs = [row.fraternity_slug for row in records]

        for slug in slugs:
            try:
                self.revalidate_verified_source(slug)
                revalidated += 1
            except Exception:
                failed += 1

        result = {"revalidated": revalidated, "failed": failed}
        log_event(LOGGER, "verified_sources_bulk_revalidated", **result, limit=limit)
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



def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _linear_slope(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    x_values = list(range(1, n + 1))
    x_mean = sum(x_values) / n
    y_mean = sum(values) / n
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, values, strict=False))
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _normalize_source_slugs(slugs: list[str]) -> list[str]:
    cleaned: list[str] = []
    for slug in slugs:
        value = str(slug).strip()
        if not value:
            continue
        if value not in cleaned:
            cleaned.append(value)
    return cleaned


def _resolve_repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pnpm-workspace.yaml").exists():
            return parent
    return Path.cwd()


def _default_epoch_report_path() -> str:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"docs/reports/ADAPTIVE_EPOCH_REPORT_{date_str}.md"


def _render_epoch_report(
    *,
    epochs: int,
    runtime_mode: str,
    train_sources: list[str],
    eval_sources: list[str],
    epoch_rows: list[dict[str, object]],
    slope: dict[str, float],
) -> str:
    lines = [
        f"# Adaptive Train/Eval Epoch Report ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')})",
        "",
        f"- Epochs: `{epochs}`",
        f"- Adaptive runtime: `{runtime_mode}`",
        f"- Train sources: `{', '.join(train_sources)}`",
        f"- Eval sources: `{', '.join(eval_sources)}`",
        "",
        "## KPI Delta Slope (Adaptive - Legacy)",
        f"- recordsPerPageDeltaSlope: `{slope.get('recordsPerPageDeltaSlope', 0)}`",
        f"- pagesPerRecordDeltaSlope: `{slope.get('pagesPerRecordDeltaSlope', 0)}`",
        f"- upsertRatioDeltaSlope: `{slope.get('upsertRatioDeltaSlope', 0)}`",
        f"- jobsPerMinuteDeltaSlope: `{slope.get('jobsPerMinuteDeltaSlope', 0)}`",
        f"- reviewRateDeltaSlope: `{slope.get('reviewRateDeltaSlope', 0)}`",
        f"- anyContactRateDeltaSlope: `{slope.get('anyContactRateDeltaSlope', 0)}`",
        f"- balancedScoreSlope: `{slope.get('balancedScoreSlope', 0)}`",
        "",
        "## Per-Epoch KPI Deltas",
        "| Epoch | Records/Page Delta | Pages/Record Delta | Upsert Ratio Delta | Jobs/Min Delta | Review Rate Delta | Any Contact Delta | Balanced Score |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in epoch_rows:
        kpis = row["kpis"]
        lines.append(
            f"| {row['epoch']} | {kpis.get('recordsPerPageDelta', 0)} | {kpis.get('pagesPerRecordDelta', 0)} | {kpis.get('upsertRatioDelta', 0)} | {kpis.get('jobsPerMinuteDelta', 0)} | {kpis.get('reviewRateDelta', 0)} | {kpis.get('anyContactRateDelta', 0)} | {kpis.get('balancedScore', 0)} |"
        )
    lines.append("")
    lines.append("## Raw Rows")
    lines.append("```json")
    lines.append(json.dumps(epoch_rows, indent=2))
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _balanced_kpi_weights(raw: str) -> dict[str, float]:
    defaults = {"coverage": 0.45, "throughput": 0.2, "queue": 0.2, "reliability": 0.15}
    try:
        parsed = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return defaults
    if not isinstance(parsed, dict):
        return defaults
    weights: dict[str, float] = {}
    for key, default in defaults.items():
        value = parsed.get(key, default)
        try:
            weights[key] = max(0.0, float(value))
        except (TypeError, ValueError):
            weights[key] = default
    total = sum(weights.values())
    if total <= 0:
        return defaults
    return {key: value / total for key, value in weights.items()}


def _compute_balanced_score(kpis: dict[str, float], weights: dict[str, float]) -> float:
    coverage_component = float(kpis.get("anyContactRateDelta", kpis.get("upsertRatioDelta", 0.0)))
    throughput_component = float(kpis.get("jobsPerMinuteDelta", 0.0)) / 10.0
    queue_component = -float(kpis.get("pagesPerRecordDelta", 0.0))
    reliability_component = -float(kpis.get("reviewRateDelta", 0.0))
    return (
        weights.get("coverage", 0.45) * coverage_component
        + weights.get("throughput", 0.2) * throughput_component
        + weights.get("queue", 0.2) * queue_component
        + weights.get("reliability", 0.15) * reliability_component
    )


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


_SEARCH_PREFLIGHT_QUERIES = (
    '"sigma chi" University of Virginia instagram',
    '"delta chi" Mississippi State chapter website',
    '"lambda chi alpha" Purdue contact email',
    '"phi gamma delta" chapter directory',
)


def _search_settings_from_preflight(settings: Settings, preflight_snapshot: dict[str, object] | None) -> Settings:
    provider = (settings.crawler_search_provider or "").strip().lower()
    if provider not in {"auto", "auto_free"}:
        return settings

    provider_health = (preflight_snapshot or {}).get("provider_health")
    if not isinstance(provider_health, dict) or not provider_health:
        return settings

    base_order: list[str] = []
    raw_order = (settings.crawler_search_provider_order_free or "").strip()
    if raw_order:
        for token in (part.strip().lower() for part in raw_order.split(",")):
            if token and token not in base_order:
                base_order.append(token)
    if not base_order:
        base_order = ["searxng_json", "serper_api", "tavily_api", "duckduckgo_html", "bing_html", "brave_html"]

    successful: list[tuple[str, float, int, int, int]] = []
    neutral: list[str] = []
    degraded: list[str] = []

    for index, provider_name in enumerate(base_order):
        payload = provider_health.get(provider_name)
        if not isinstance(payload, dict):
            neutral.append(provider_name)
            continue
        attempts = int(payload.get("attempts", 0) or 0)
        successes = int(payload.get("successes", 0) or 0)
        request_error = int(payload.get("request_error", 0) or 0)
        unavailable = int(payload.get("unavailable", 0) or 0)
        success_rate = float(payload.get("success_rate", 0.0) or 0.0)

        if successes > 0:
            successful.append((provider_name, success_rate, successes, attempts, index))
        elif attempts > 0 and (request_error + unavailable) >= attempts:
            degraded.append(provider_name)
        else:
            neutral.append(provider_name)

    if not successful:
        return settings

    ranked = [
        provider_name
        for provider_name, _, _, _, _ in sorted(
            successful,
            key=lambda item: (-item[1], -item[2], item[3], item[4]),
        )
    ]
    new_order = ",".join(ranked + neutral + degraded)
    if new_order == raw_order:
        return settings
    return settings.model_copy(update={"crawler_search_provider_order_free": new_order})




def _probe_queries_from_preflight(preflight_snapshot: dict[str, object] | None) -> list[str]:
    queries: list[str] = []
    for item in (preflight_snapshot or {}).get("probe_outcomes") or []:
        if not isinstance(item, dict):
            continue
        query = str(item.get("query") or "").strip()
        if query and query not in queries:
            queries.append(query)
    return queries


def _provider_window_success_rate(provider_health: dict[str, dict[str, object]] | None) -> float:
    if not isinstance(provider_health, dict) or not provider_health:
        return 1.0
    attempts = 0
    successes = 0
    for payload in provider_health.values():
        if not isinstance(payload, dict):
            continue
        attempts += int(payload.get("attempts", 0) or 0)
        successes += int(payload.get("successes", 0) or 0)
    if attempts <= 0:
        return 1.0
    return round(successes / attempts, 4)


def _merge_unique_texts(*groups: list[str] | tuple[str, ...] | None) -> list[str]:
    merged: list[str] = []
    for group in groups:
        for value in group or []:
            text = str(value or "").strip()
            if text and text not in merged:
                merged.append(text)
    return merged


def _accuracy_recovery_metrics_payload(metrics) -> dict[str, int]:
    if metrics is None:
        return {
            "complete_rows": 0,
            "chapter_specific_contact_rows": 0,
            "nationals_only_contact_rows": 0,
            "inactive_validated_rows": 0,
            "confirmed_absent_website_rows": 0,
            "active_rows_with_chapter_specific_email": 0,
            "active_rows_with_chapter_specific_instagram": 0,
            "active_rows_with_any_contact": 0,
            "total_chapters": 0,
        }
    return {
        "complete_rows": int(getattr(metrics, "complete_rows", 0) or 0),
        "chapter_specific_contact_rows": int(getattr(metrics, "chapter_specific_contact_rows", 0) or 0),
        "nationals_only_contact_rows": int(getattr(metrics, "nationals_only_contact_rows", 0) or 0),
        "inactive_validated_rows": int(getattr(metrics, "inactive_validated_rows", 0) or 0),
        "confirmed_absent_website_rows": int(getattr(metrics, "confirmed_absent_website_rows", 0) or 0),
        "active_rows_with_chapter_specific_email": int(getattr(metrics, "active_rows_with_chapter_specific_email", 0) or 0),
        "active_rows_with_chapter_specific_instagram": int(getattr(metrics, "active_rows_with_chapter_specific_instagram", 0) or 0),
        "active_rows_with_any_contact": int(getattr(metrics, "active_rows_with_any_contact", 0) or 0),
        "total_chapters": int(getattr(metrics, "total_chapters", 0) or 0),
    }


def _field_job_batch_delta_payload(before_metrics, after_metrics, *, processed: int) -> dict[str, object]:
    before_payload = _accuracy_recovery_metrics_payload(before_metrics)
    after_payload = _accuracy_recovery_metrics_payload(after_metrics)
    new_complete_rows = max(0, after_payload["complete_rows"] - before_payload["complete_rows"])
    new_inactive_validated_rows = max(0, after_payload["inactive_validated_rows"] - before_payload["inactive_validated_rows"])
    new_confirmed_absent_website_rows = max(0, after_payload["confirmed_absent_website_rows"] - before_payload["confirmed_absent_website_rows"])
    productive_numerator = new_complete_rows + new_inactive_validated_rows + new_confirmed_absent_website_rows
    return {
        "accuracy_recovery_before": before_payload,
        "accuracy_recovery_after": after_payload,
        "new_complete_rows": new_complete_rows,
        "new_inactive_validated_rows": new_inactive_validated_rows,
        "new_confirmed_absent_website_rows": new_confirmed_absent_website_rows,
        "productive_yield": round(productive_numerator / processed, 4) if processed > 0 else 0.0,
    }


def _provider_order_from_settings(settings: Settings) -> list[str]:
    raw_order = str(getattr(settings, "crawler_search_provider_order_free", "") or "").strip()
    base_order: list[str] = []
    if raw_order:
        for token in (part.strip().lower() for part in raw_order.split(",")):
            if token and token not in base_order:
                base_order.append(token)
    if not base_order:
        base_order = ["searxng_json", "serper_api", "tavily_api", "duckduckgo_html", "bing_html", "brave_html"]
    return base_order


def _all_attempted_providers_below_threshold(
    preflight_snapshot: dict[str, object] | None,
    settings: Settings,
    *,
    min_success_rate: float,
) -> bool:
    provider_health = (preflight_snapshot or {}).get("provider_health")
    if not isinstance(provider_health, dict) or not provider_health:
        return False
    attempted_rates: list[float] = []
    for provider_name in _provider_order_from_settings(settings):
        payload = provider_health.get(provider_name)
        if not isinstance(payload, dict):
            continue
        attempts = int(payload.get("attempts", 0) or 0)
        if attempts < 4:
            continue
        attempted_rates.append(float(payload.get("success_rate", 0.0) or 0.0))
    return bool(attempted_rates) and all(rate < min_success_rate for rate in attempted_rates)


def _reorder_search_settings_from_window(
    settings: Settings,
    preflight_snapshot: dict[str, object] | None,
    *,
    min_success_rate: float,
) -> Settings:
    provider = (settings.crawler_search_provider or "").strip().lower()
    if provider not in {"auto", "auto_free"}:
        return settings
    provider_health = (preflight_snapshot or {}).get("provider_health")
    if not isinstance(provider_health, dict) or not provider_health:
        return settings
    base_order = _provider_order_from_settings(settings)
    ranked: list[tuple[str, float, int]] = []
    stable: list[str] = []
    demoted: list[str] = []
    for index, provider_name in enumerate(base_order):
        payload = provider_health.get(provider_name)
        if not isinstance(payload, dict):
            stable.append(provider_name)
            continue
        attempts = int(payload.get("attempts", 0) or 0)
        success_rate = float(payload.get("success_rate", 0.0) or 0.0)
        if attempts >= 4 and success_rate >= min_success_rate:
            ranked.append((provider_name, success_rate, index))
            continue
        if index < 2 and attempts >= 4 and success_rate < min_success_rate:
            demoted.append(provider_name)
            continue
        stable.append(provider_name)
    ranked_names = [name for name, _, _ in sorted(ranked, key=lambda item: (-item[1], item[2]))]
    new_order = ranked_names + [name for name in stable if name not in ranked_names] + [name for name in demoted if name not in ranked_names]
    normalized = ",".join(dict.fromkeys(new_order or base_order))
    if normalized == str(getattr(settings, "crawler_search_provider_order_free", "") or ""):
        return settings
    return settings.model_copy(update={"crawler_search_provider_order_free": normalized})


def _merge_field_job_chunk_results(aggregate: dict[str, object], result: dict[str, object]) -> dict[str, object]:
    merged = dict(aggregate)
    for key in (
        "processed",
        "requeued",
        "failed_terminal",
        "runtime_fallback_count",
        "provider_degraded_deferred",
        "dependency_wait_deferred",
        "supporting_page_resolved",
        "supporting_page_contact_resolved",
        "external_search_contact_resolved",
        "enrichment_observations_logged",
        "mid_batch_provider_rechecks",
        "mid_batch_provider_reorders",
        "degraded_authoritative_claimed",
        "verify_school_cache_hit",
        "verify_school_official_url_reused",
        "verify_school_provider_search_attempted",
    ):
        merged[key] = int(merged.get(key, 0) or 0) + int(result.get(key, 0) or 0)
    merged["runtime_mode_used"] = str(result.get("runtime_mode_used") or merged.get("runtime_mode_used") or "legacy")
    merged["preflight_probe_queries"] = _merge_unique_texts(merged.get("preflight_probe_queries") or [], result.get("preflight_probe_queries") or [])
    merged["chapter_search_queries"] = _merge_unique_texts(merged.get("chapter_search_queries") or [], result.get("chapter_search_queries") or [])
    if isinstance(result.get("provider_window_state"), dict):
        merged["provider_window_state"] = dict(result["provider_window_state"])
    return merged


def _preflight_snapshot_is_healthy(preflight_snapshot: dict[str, object] | None) -> bool:
    if preflight_snapshot is None:
        return False
    return bool(preflight_snapshot.get("healthy", False))


def _provider_window_state_from_preflight(
    preflight_snapshot: dict[str, object] | None,
    *,
    degraded_mode: bool = False,
) -> dict[str, object]:
    provider_health = (preflight_snapshot or {}).get("provider_health")
    captured_at = str((preflight_snapshot or {}).get("captured_at") or _utc_now_iso())
    if not isinstance(provider_health, dict):
        healthy = not degraded_mode
        degraded_reason = "degraded_mode_enabled" if degraded_mode else "no_preflight_snapshot"
        general = {
            "lane": "general_web_search",
            "window_started_at": captured_at,
            "window_success_rate": 1.0 if healthy else 0.0,
            "attempt_count": 0,
            "request_error_count": 0,
            "challenge_or_anomaly_count": 0,
            "healthy": healthy,
            "degraded_reason": degraded_reason if not healthy else "",
            "providers": [],
        }
        return {
            "general_web_search": general,
            "social_search": {
                **general,
                "lane": "social_search",
                "healthy": healthy,
                "degraded_reason": degraded_reason if not healthy else "",
                "source_mode": "shares_general_provider_pool",
            },
            "authoritative_fetch": {
                "lane": "authoritative_fetch",
                "window_started_at": captured_at,
                "window_success_rate": 1.0,
                "attempt_count": 0,
                "request_error_count": 0,
                "challenge_or_anomaly_count": 0,
                "healthy": True,
                "degraded_reason": "",
                "source_mode": "independent_of_search_provider",
            },
        }

    attempt_count = 0
    success_count = 0
    request_error_count = 0
    challenge_or_anomaly_count = 0
    providers: list[dict[str, object]] = []
    for provider_name, payload in provider_health.items():
        if not isinstance(payload, dict):
            continue
        attempts = int(payload.get("attempts", 0) or 0)
        successes = int(payload.get("successes", 0) or 0)
        request_errors = int(payload.get("request_error", 0) or 0)
        unavailable = int(payload.get("unavailable", 0) or 0)
        challenge_guess = 0
        for key in payload.keys():
            if "challenge" in str(key).lower() or "anomaly" in str(key).lower():
                challenge_guess += int(payload.get(key, 0) or 0)
        attempt_count += attempts
        success_count += successes
        request_error_count += request_errors
        challenge_or_anomaly_count += challenge_guess
        providers.append(
            {
                "provider": str(provider_name),
                "attempt_count": attempts,
                "success_count": successes,
                "request_error_count": request_errors,
                "challenge_or_anomaly_count": challenge_guess,
                "unavailable_count": unavailable,
                "window_success_rate": round(float(payload.get("success_rate", 0.0) or 0.0), 4),
            }
        )
    healthy = bool((preflight_snapshot or {}).get("healthy", False)) and not degraded_mode
    window_success_rate = round(success_count / attempt_count, 4) if attempt_count > 0 else (1.0 if healthy else 0.0)
    degraded_reason = ""
    if not healthy:
        degraded_reason = "degraded_mode_enabled" if degraded_mode else str((preflight_snapshot or {}).get("reason") or "provider_success_below_threshold")
    general = {
        "lane": "general_web_search",
        "window_started_at": captured_at,
        "window_success_rate": window_success_rate,
        "attempt_count": attempt_count,
        "request_error_count": request_error_count,
        "challenge_or_anomaly_count": challenge_or_anomaly_count,
        "healthy": healthy,
        "degraded_reason": degraded_reason,
        "providers": providers,
    }
    return {
        "general_web_search": general,
        "social_search": {
            **general,
            "lane": "social_search",
            "source_mode": "shares_general_provider_pool",
        },
        "authoritative_fetch": {
            "lane": "authoritative_fetch",
            "window_started_at": captured_at,
            "window_success_rate": 1.0,
            "attempt_count": 0,
            "request_error_count": 0,
            "challenge_or_anomaly_count": 0,
            "healthy": True,
            "degraded_reason": "",
            "source_mode": "independent_of_search_provider",
        },
    }


def _cached_school_policy_is_decisive(record: object) -> bool:
    status = str(getattr(record, "greek_life_status", "") or "").strip().lower()
    source_type = str(getattr(record, "evidence_source_type", "") or "").strip().lower()
    if source_type != "official_school":
        return False
    return status in {"allowed", "banned"}


def _cached_chapter_activity_is_decisive(record: object) -> bool:
    status = str(getattr(record, "chapter_activity_status", "") or "").strip().lower()
    source_type = str(getattr(record, "evidence_source_type", "") or "").strip().lower()
    if source_type != "official_school":
        return False
    return status in {"confirmed_active", "confirmed_inactive"}


def _job_has_reusable_official_school_evidence(repository: CrawlerRepository, job: FieldJob) -> bool:
    url = repository.get_reusable_official_school_evidence_url(
        fraternity_slug=job.fraternity_slug,
        school_name=job.university_name,
    )
    return bool(str(url or "").strip())


def _job_is_degraded_authoritative_candidate(repository: CrawlerRepository, job: FieldJob) -> bool:
    if job.field_name == FIELD_JOB_VERIFY_SCHOOL:
        cached_school_policy = repository.get_school_policy(job.university_name)
        cached_chapter_activity = repository.get_chapter_activity(
            fraternity_slug=job.fraternity_slug,
            school_name=job.university_name,
        )
        return (
            _cached_school_policy_is_decisive(cached_school_policy)
            or _cached_chapter_activity_is_decisive(cached_chapter_activity)
            or _job_has_reusable_official_school_evidence(repository, job)
        )
    if job.field_name == FIELD_JOB_VERIFY_WEBSITE:
        return _job_supporting_page_ready(job)
    if job.field_name in {FIELD_JOB_FIND_INSTAGRAM, FIELD_JOB_FIND_EMAIL}:
        return _job_supporting_page_ready(job)
    return False


def _job_has_provider_retry_signature(job: FieldJob) -> bool:
    payload = dict(job.payload or {})
    last_reason = str(payload.get("transient_provider_last_reason") or "").strip().lower()
    if last_reason in {"provider_degraded", "transient_network", "provider_low_signal"}:
        return True
    attempts = payload.get("provider_attempts")
    if not isinstance(attempts, list) or not attempts:
        return False
    statuses = {
        str((item or {}).get("status") or "").strip().lower()
        for item in attempts
        if isinstance(item, dict)
    }
    if not statuses:
        return False
    return statuses.issubset({"unavailable", "request_error", "low_signal"})


def _field_job_queue_lane(*, queue_state: str | None, blocked_reason: str | None, field_name: str | None = None) -> str:
    normalized_state = str(queue_state or "actionable").strip().lower() or "actionable"
    normalized_reason = str(blocked_reason or "").strip().lower()
    normalized_field = str(field_name or "").strip().lower()
    if normalized_state == "blocked_provider":
        return "provider_dependent_search"
    if normalized_state == "blocked_dependency":
        return "dependency_blocked"
    if normalized_state == "blocked_repairable" or normalized_reason in {
        "queued_for_entity_repair",
        "identity_semantically_incomplete",
        "repair_exhausted",
    }:
        return "repair_backlog"
    if normalized_reason in {"provider_degraded", "transient_network", "provider_low_signal"}:
        return "provider_dependent_search"
    if normalized_reason in {"dependency_wait", "website_required"}:
        return "dependency_blocked"
    if normalized_field in {FIELD_JOB_VERIFY_SCHOOL, FIELD_JOB_VERIFY_WEBSITE}:
        return "authoritative_resolution"
    return "authoritative_resolution"


def _queue_health_payload(
    queue: dict[str, object],
    *,
    deferred_reason_breakdown: list[dict[str, object]],
    field_worker_processes: dict[str, int],
    liveness_alert_poll_windows: int,
) -> dict[str, object]:
    queued_jobs = max(0, int(queue.get("queued_jobs", 0) or 0))
    actionable_jobs = max(0, int(queue.get("actionable_jobs", 0) or 0))
    deferred_jobs = max(0, int(queue.get("deferred_jobs", 0) or 0))
    running_jobs = max(0, int(queue.get("running_jobs", 0) or 0))
    deferred_ratio = round(deferred_jobs / queued_jobs, 4) if queued_jobs else 0.0

    lane_totals = {
        "provider_dependent_search": 0,
        "dependency_blocked": 0,
        "repair_backlog": 0,
        "authoritative_resolution": 0,
    }
    for row in deferred_reason_breakdown:
        lane = str(row.get("queue_lane") or "authoritative_resolution")
        lane_totals[lane] = int(lane_totals.get(lane, 0) or 0) + int(row.get("count", 0) or 0)

    active_workers = max(0, int(field_worker_processes.get("active_workers", 0) or 0))
    stale_workers = max(0, int(field_worker_processes.get("stale_workers", 0) or 0))
    liveness_alert = bool(actionable_jobs > 0 and running_jobs == 0 and active_workers == 0)
    worker_liveness_ratio = 1.0 if actionable_jobs <= 0 else (1.0 if (running_jobs > 0 or active_workers > 0) else 0.0)

    return {
        "deferred_ratio": deferred_ratio,
        "provider_degraded_ratio": round(lane_totals["provider_dependent_search"] / queued_jobs, 4) if queued_jobs else 0.0,
        "dependency_blocked_ratio": round(lane_totals["dependency_blocked"] / queued_jobs, 4) if queued_jobs else 0.0,
        "repair_backlog_ratio": round(lane_totals["repair_backlog"] / queued_jobs, 4) if queued_jobs else 0.0,
        "authoritative_resolution_ratio": round(lane_totals["authoritative_resolution"] / queued_jobs, 4) if queued_jobs else 0.0,
        "worker_liveness_ratio": round(worker_liveness_ratio, 4),
        "worker_liveness_alert": {
            "open": liveness_alert,
            "message": (
                f"Actionable field jobs are present but no field-job workers are active across {liveness_alert_poll_windows} poll windows"
                if liveness_alert
                else ""
            ),
            "actionableJobs": actionable_jobs,
            "runningJobs": running_jobs,
            "activeWorkers": active_workers,
            "staleWorkers": stale_workers,
            "pollWindows": max(1, int(liveness_alert_poll_windows or 1)),
        },
    }


def _job_supporting_page_ready(job: FieldJob) -> bool:
    field_states = dict(job.field_states or {})
    website_state = str(field_states.get("website_url") or "").strip().lower()
    if job.website_url and website_state not in {"", "missing", "low_confidence"}:
        return True
    contact_resolution = job.payload.get("contactResolution") if isinstance(job.payload.get("contactResolution"), dict) else {}
    supporting_page_url = str(contact_resolution.get("supportingPageUrl") or "").strip()
    supporting_page_scope = str(contact_resolution.get("supportingPageScope") or contact_resolution.get("pageScope") or "").strip().lower()
    if supporting_page_url and supporting_page_scope in {
        "chapter_site",
        "school_affiliation_page",
        "nationals_chapter_page",
    }:
        return True
    if website_state == "confirmed_absent":
        if job.contact_email or job.instagram_url:
            return True
        if supporting_page_url and supporting_page_scope in {"school_affiliation_page", "nationals_chapter_page"}:
            return True
    return False


def _build_enrichment_shadow_context(job: FieldJob, provider_window_state: dict[str, object] | None) -> dict[str, object]:
    contact_resolution = job.payload.get("contactResolution") if isinstance(job.payload.get("contactResolution"), dict) else {}
    general_lane = (provider_window_state or {}).get("general_web_search")
    if not isinstance(general_lane, dict):
        general_lane = {}
    supporting_page_url = str(contact_resolution.get("supportingPageUrl") or "").strip()
    supporting_page_scope = str(contact_resolution.get("supportingPageScope") or contact_resolution.get("pageScope") or "").strip().lower()
    target_field_value = {
        FIELD_JOB_FIND_WEBSITE: job.website_url,
        FIELD_JOB_VERIFY_WEBSITE: job.website_url,
        FIELD_JOB_FIND_EMAIL: job.contact_email,
        FIELD_JOB_FIND_INSTAGRAM: job.instagram_url,
        FIELD_JOB_VERIFY_SCHOOL: job.university_name,
    }.get(job.field_name)
    prior_query_count = len(list(job.payload.get("provider_attempts") or [])) + int(job.payload.get("terminal_no_signal_count", 0) or 0)
    return {
        "field_type": job.field_name,
        "supporting_page_present": bool(_job_supporting_page_ready(job) or supporting_page_url),
        "supporting_page_scope": supporting_page_scope,
        "website_prerequisite_unmet": bool(job.field_name == FIELD_JOB_FIND_EMAIL and not _job_supporting_page_ready(job)),
        "school_validation_status": str(job.payload.get("schoolValidationStatus") or "").strip().lower(),
        "provider_window_healthy": bool(general_lane.get("healthy", False)),
        "provider_window_degraded": not bool(general_lane.get("healthy", False)),
        "prior_query_count": prior_query_count,
        "identity_complete": bool(job.chapter_name and job.university_name and job.fraternity_slug),
        "has_candidate_website": bool(job.website_url),
        "has_target_value": bool(target_field_value),
        "needs_authoritative_validation": bool(job.field_name in {FIELD_JOB_VERIFY_SCHOOL, FIELD_JOB_VERIFY_WEBSITE}),
        "timeout_risk": 1.0 if not bool(general_lane.get("healthy", False)) else 0.15,
        "requeue_risk": 0.8 if job.queue_state == "deferred" else 0.2,
    }


def _infer_repair_family(job: FieldJob, *, repaired_university: str | None = None) -> str:
    chapter_name = str(job.chapter_name or "").strip()
    chapter_slug = str(job.chapter_slug or "").strip().lower()
    university_name = str(repaired_university or job.university_name or "").strip()
    if not university_name:
        return "school_name_normalizer"
    if re.match(r"^(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)\b", chapter_name.lower()):
        return "state_prefix_resolver"
    if any(token in chapter_slug for token in ("unknown", "--", "chapter-", "-main-")) or re.search(r"\d{3,}", chapter_name):
        return "chapter_designation_repair"
    return "duplicate_identity_merge"


def _increment_repair_family_summary(summary: dict[str, int], repair_family: str, amount: int) -> None:
    mapping = {
        "state_prefix_resolver": "statePrefixResolver",
        "school_name_normalizer": "schoolNameNormalizer",
        "chapter_designation_repair": "chapterDesignationRepair",
        "duplicate_identity_merge": "duplicateIdentityMerge",
    }
    key = mapping.get(repair_family)
    if key is None:
        return
    summary[key] = int(summary.get(key, 0) or 0) + int(amount or 0)

def _slugify(value: str) -> str:
    return "-".join(token for token in "".join(ch if ch.isalnum() else " " for ch in value.lower()).split())


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bootstrap_confidence(http_status: int | None, selected_reason: str) -> float:
    confidence = 0.65
    if http_status is not None:
        if 200 <= http_status < 400:
            confidence += 0.2
        elif http_status >= 400:
            confidence -= 0.2
    if selected_reason == "chapterish_link":
        confidence += 0.1
    return max(0.0, min(0.99, confidence))


def _select_registry_url(payload: dict[str, object]) -> tuple[str | None, str]:
    chapterish_links = payload.get("chapterish_links")
    if isinstance(chapterish_links, list):
        best_score = -1.0
        best_url: str | None = None
        for entry in chapterish_links:
            if not isinstance(entry, dict):
                continue
            url = str(entry.get("url") or "").strip()
            if not url.startswith("http"):
                continue
            label = str(entry.get("text") or "").lower()
            score = 0.0
            if any(marker in label for marker in ("chapter directory", "find a chapter", "our chapters", "chapter roll", "chapter map", "chapters")):
                score += 1.0
            if any(marker in label for marker in ("toolkit", "news", "award", "staff", "resource")):
                score -= 0.7
            if score > best_score:
                best_score = score
                best_url = url
        if best_url is not None and best_score >= 0.6:
            return best_url, "chapterish_link"

    final_url = str(payload.get("final_url") or "").strip()
    if final_url.startswith("http"):
        return final_url, "final_url"
    base_url = str(payload.get("base") or "").strip()
    if base_url.startswith("http"):
        return base_url, "base_url"
    return None, "none"


def _probe_url(url: str, settings: Settings) -> tuple[int | None, str | None, str | None]:
    try:
        response = requests.get(
            url,
            timeout=settings.crawler_http_timeout_seconds,
            verify=settings.crawler_http_verify_ssl,
            headers={"User-Agent": settings.crawler_http_user_agent},
            allow_redirects=True,
        )
        return response.status_code, response.url, None
    except Exception as exc:
        parsed = urlparse(url)
        fallback_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.scheme and parsed.netloc else url
        return None, fallback_url, str(exc)


_CAPITALIZED_SCHOOL_WORD = r"[A-Z][A-Za-z&.'-]*"
_CAPITALIZED_SCHOOL_PHRASE = rf"{_CAPITALIZED_SCHOOL_WORD}(?: {_CAPITALIZED_SCHOOL_WORD}){{0,7}}"

_REPAIR_SCHOOL_PATTERNS = (
    re.compile(rf"\b(University of {_CAPITALIZED_SCHOOL_PHRASE})\b"),
    re.compile(rf"\b({_CAPITALIZED_SCHOOL_PHRASE} State University)\b"),
    re.compile(rf"\b({_CAPITALIZED_SCHOOL_PHRASE} University)\b"),
    re.compile(rf"\b({_CAPITALIZED_SCHOOL_PHRASE} College)\b"),
    re.compile(rf"\b({_CAPITALIZED_SCHOOL_PHRASE} Institute(?: of {_CAPITALIZED_SCHOOL_PHRASE})?)\b"),
)


def _classify_field_job_identity(job: FieldJob):
    repair_context_parts = [job.chapter_slug]
    candidate_school = job.payload.get("candidateSchoolName")
    if isinstance(candidate_school, str) and candidate_school.strip():
        repair_context_parts.append(candidate_school)
    return classify_chapter_validity(
        ExtractedChapter(
            name=job.chapter_name,
            university_name=job.university_name,
            website_url=job.website_url,
            instagram_url=job.instagram_url,
            contact_email=job.contact_email,
            source_snippet=" ".join(part for part in repair_context_parts if part),
            source_url=(job.payload.get("sourceListUrl") if isinstance(job.payload.get("sourceListUrl"), str) else job.source_base_url) or "",
            source_confidence=0.9,
        ),
        source_class="national",
        provenance="historical_queue",
    )


def _infer_university_name_for_job(job: FieldJob, snippets: list[str]) -> str | None:
    candidates: list[str] = []
    payload_candidate = job.payload.get("candidateSchoolName")
    if isinstance(payload_candidate, str) and payload_candidate.strip():
        candidates.append(payload_candidate.strip())
    for snippet in snippets[:20]:
        text = snippet.strip()
        if not text:
            continue
        for pattern in _REPAIR_SCHOOL_PATTERNS:
            for match in pattern.findall(text):
                candidate = " ".join(str(match).split())
                if 4 <= len(candidate) <= 96:
                    candidates.append(candidate)
    seen: set[str] = set()
    deduped: list[str] = []
    for candidate in candidates:
        normalized = candidate.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    for candidate in deduped:
        decision = classify_chapter_validity(
            ExtractedChapter(
                name=job.chapter_name,
                university_name=candidate,
                website_url=job.website_url,
                instagram_url=job.instagram_url,
                contact_email=job.contact_email,
                source_url=(job.payload.get("sourceListUrl") if isinstance(job.payload.get("sourceListUrl"), str) else job.source_base_url) or "",
                source_confidence=0.9,
            ),
            source_class="national",
            provenance="queue_repair_candidate",
        )
        if decision.validity_class == "canonical_valid":
            return candidate
    return None


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()




















