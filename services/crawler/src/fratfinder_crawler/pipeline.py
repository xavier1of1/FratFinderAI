from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import requests

from fratfinder_crawler.adapters import AdapterRegistry
from fratfinder_crawler.adaptive import AdaptivePolicy
from fratfinder_crawler.config import Settings
from fratfinder_crawler.db.connection import get_connection
from fratfinder_crawler.discovery import discover_source
from fratfinder_crawler.db.repository import CrawlerRepository
from fratfinder_crawler.field_jobs import FieldJobEngine
from fratfinder_crawler.http.client import HttpClient
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.search import SearchClient, SearchUnavailableError
from fratfinder_crawler.models import CrawlMetrics, EpochMetric
from fratfinder_crawler.orchestration import AdaptiveCrawlOrchestrator, CrawlOrchestrator

LOGGER = logging.getLogger(__name__)


class CrawlService:
    def __init__(self, settings: Settings):
        self._settings = settings

    def run(
        self,
        source_slug: str | None = None,
        runtime_mode: str | None = None,
        policy_mode: str = "live",
    ) -> dict[str, int]:
        aggregate = CrawlMetrics()
        effective_runtime_mode = self._resolve_runtime_mode(runtime_mode)

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            sources = repository.load_sources(source_slug=source_slug)
            orchestrator = self._build_orchestrator(repository, effective_runtime_mode, policy_mode=policy_mode)

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
            eval_legacy = self._run_sources_batch(eval_sources, "legacy", policy_mode="live")
            eval_adaptive = self._run_sources_batch(eval_sources, effective_runtime_mode, policy_mode="live")

            legacy_records_per_page = _safe_ratio(eval_legacy["records_seen"], eval_legacy["pages_processed"])
            adaptive_records_per_page = _safe_ratio(eval_adaptive["records_seen"], eval_adaptive["pages_processed"])
            legacy_pages_per_record = _safe_ratio(eval_legacy["pages_processed"], eval_legacy["records_seen"])
            adaptive_pages_per_record = _safe_ratio(eval_adaptive["pages_processed"], eval_adaptive["records_seen"])
            legacy_upsert_ratio = _safe_ratio(eval_legacy["records_upserted"], eval_legacy["records_seen"])
            adaptive_upsert_ratio = _safe_ratio(eval_adaptive["records_upserted"], eval_adaptive["records_seen"])
            legacy_jobs_per_min = float(eval_legacy.get("jobs_per_minute", 0.0))
            adaptive_jobs_per_min = float(eval_adaptive.get("jobs_per_minute", 0.0))
            legacy_review_rate = _safe_ratio(eval_legacy["review_items_created"], max(eval_legacy["records_seen"], 1.0))
            adaptive_review_rate = _safe_ratio(eval_adaptive["review_items_created"], max(eval_adaptive["records_seen"], 1.0))

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
                            "balancedScore": float(kpis["balancedScore"]),
                        },
                        slopes=slope_snapshot,
                        cohort_label=cohort_label,
                        metadata={
                            "weights": weights,
                            "replayWindowDays": replay_days,
                            "replayBatchSize": replay_size,
                        },
                    )
                )

        slope = {
            "recordsPerPageDeltaSlope": round(_linear_slope([float(row["kpis"]["recordsPerPageDelta"]) for row in epoch_rows]), 6),
            "pagesPerRecordDeltaSlope": round(_linear_slope([float(row["kpis"]["pagesPerRecordDelta"]) for row in epoch_rows]), 6),
            "upsertRatioDeltaSlope": round(_linear_slope([float(row["kpis"]["upsertRatioDelta"]) for row in epoch_rows]), 6),
            "jobsPerMinuteDeltaSlope": round(_linear_slope([float(row["kpis"]["jobsPerMinuteDelta"]) for row in epoch_rows]), 6),
            "reviewRateDeltaSlope": round(_linear_slope([float(row["kpis"]["reviewRateDelta"]) for row in epoch_rows]), 6),
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
    ) -> dict[str, float]:
        aggregate = CrawlMetrics()
        effective_runtime_mode = self._resolve_runtime_mode(runtime_mode)
        requested = [slug for slug in source_slugs if slug]
        selected_count = 0
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
                metrics = orchestrator.run_for_source(source)
                aggregate.pages_processed += metrics.pages_processed
                aggregate.records_seen += metrics.records_seen
                aggregate.records_upserted += metrics.records_upserted
                aggregate.review_items_created += metrics.review_items_created
                aggregate.field_jobs_created += metrics.field_jobs_created
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

        replay = []
        for action, values in sorted(action_buckets.items(), key=lambda entry: (-entry[1]["records"], -entry[1]["count"])):
            replay.append(
                {
                    "actionType": action,
                    "count": int(values["count"]),
                    "avgRecords": round(values["records"] / max(values["count"], 1.0), 4),
                    "avgSelectedScore": round(values["avgSelectedScore"] / max(values["count"], 1.0), 4),
                    "avgRisk": round(values["avgRisk"] / max(values["count"], 1.0), 4),
                }
            )

        reward_stage_summary: dict[str, dict[str, float]] = {}
        for event in reward_events:
            stage = str(event.get("reward_stage") or "unknown")
            bucket = reward_stage_summary.setdefault(stage, {"count": 0.0, "total": 0.0})
            bucket["count"] += 1.0
            bucket["total"] += float(event.get("reward_value") or 0.0)

        stage_rows = [
            {
                "stage": stage,
                "count": int(values["count"]),
                "avgReward": round(values["total"] / max(values["count"], 1.0), 4),
                "totalReward": round(values["total"], 4),
            }
            for stage, values in sorted(reward_stage_summary.items(), key=lambda item: -item[1]["count"])
        ]

        return {"count": len(replay), "actions": replay, "rewardStages": stage_rows}
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

    def _build_orchestrator(
        self,
        repository: CrawlerRepository,
        runtime_mode: str,
        *,
        policy_mode: str = "live",
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
        )

    def process_field_jobs(
        self,
        limit: int = 25,
        source_slug: str | None = None,
        field_name: str | None = None,
        workers: int | None = None,
        require_healthy_search: bool = False,
        run_preflight: bool | None = None,
    ) -> dict[str, int]:
        effective_workers = workers or self._settings.crawler_field_job_max_workers
        degraded_mode = False
        preflight_enabled = self._settings.crawler_search_preflight_enabled if run_preflight is None else run_preflight
        preflight_snapshot: dict[str, object] | None = None

        if preflight_enabled and self._settings.crawler_search_enabled:
            preflight_snapshot = self.search_preflight()
            healthy = bool(preflight_snapshot.get("healthy", False))
            if not healthy:
                if require_healthy_search:
                    result = {"processed": 0, "requeued": 0, "failed_terminal": 0}
                    log_event(
                        LOGGER,
                        "field_job_batch_skipped_provider_degraded",
                        limit=limit,
                        source_slug=source_slug,
                        field_name=field_name,
                        workers=effective_workers,
                        preflight=preflight_snapshot,
                    )
                    return result
                degraded_mode = True
                effective_workers = max(1, min(effective_workers, self._settings.crawler_search_degraded_worker_cap))

        worker_limits = _distribute_limit(limit, effective_workers)
        if not worker_limits:
            result = {"processed": 0, "requeued": 0, "failed_terminal": 0}
            log_event(
                LOGGER,
                "field_job_batch_finished",
                limit=limit,
                source_slug=source_slug,
                field_name=field_name,
                workers=0,
                degraded_mode=degraded_mode,
                preflight=preflight_snapshot,
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
                degraded_mode=degraded_mode,
            )
            log_event(
                LOGGER,
                "field_job_batch_finished",
                limit=limit,
                source_slug=source_slug,
                field_name=field_name,
                workers=1,
                degraded_mode=degraded_mode,
                preflight=preflight_snapshot,
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
                    degraded_mode,
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
            degraded_mode=degraded_mode,
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
    ) -> dict[str, int]:
        search_settings = self._settings
        max_search_pages = self._settings.crawler_search_max_pages_per_job
        dependency_wait_seconds = self._settings.crawler_search_dependency_wait_seconds
        email_max_queries = self._settings.crawler_search_email_max_queries
        instagram_max_queries = self._settings.crawler_search_instagram_max_queries
        if degraded_mode:
            search_settings = self._settings.model_copy(
                update={"crawler_search_max_results": self._settings.crawler_search_degraded_max_results}
            )
            max_search_pages = max(1, self._settings.crawler_search_degraded_max_pages_per_job)
            dependency_wait_seconds = max(
                self._settings.crawler_search_degraded_dependency_wait_seconds,
                self._settings.crawler_search_dependency_wait_seconds,
            )
            email_max_queries = max(1, self._settings.crawler_search_degraded_email_max_queries)
            instagram_max_queries = max(1, self._settings.crawler_search_degraded_instagram_max_queries)

        with get_connection(self._settings) as connection:
            repository = CrawlerRepository(connection)
            engine = FieldJobEngine(
                repository=repository,
                logger=LOGGER,
                worker_id=_worker_id(self._settings.crawler_field_job_worker_id, worker_index, total_workers),
                base_backoff_seconds=self._settings.crawler_field_job_base_backoff_seconds,
                source_slug=source_slug,
                field_name=field_name,
                search_client=SearchClient(search_settings),
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
            )
            return engine.process(limit=limit)

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
        primary_provider_success = any(
            int(provider_health.get(provider, {}).get("successes", 0)) > 0
            for provider in ("searxng_json", "tavily_api", "serper_api")
        )
        healthy = success_rate >= self._settings.crawler_search_preflight_min_success_rate
        if provider_mode in {"auto_free", "searxng_json", "tavily_api", "serper_api"}:
            if any(provider in provider_health for provider in ("searxng_json", "tavily_api", "serper_api")):
                healthy = healthy and primary_provider_success
        snapshot = {
            "healthy": healthy,
            "success_rate": round(success_rate, 4),
            "successes": successes,
            "probes": probe_count,
            "min_success_rate": self._settings.crawler_search_preflight_min_success_rate,
            "provider_health": provider_health,
            "probe_outcomes": probe_outcomes,
        }
        log_event(LOGGER, "search_preflight_completed", **snapshot)
        return snapshot

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
        f"- balancedScoreSlope: `{slope.get('balancedScoreSlope', 0)}`",
        "",
        "## Per-Epoch KPI Deltas",
        "| Epoch | Records/Page Delta | Pages/Record Delta | Upsert Ratio Delta | Jobs/Min Delta | Review Rate Delta | Balanced Score |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in epoch_rows:
        kpis = row["kpis"]
        lines.append(
            f"| {row['epoch']} | {kpis.get('recordsPerPageDelta', 0)} | {kpis.get('pagesPerRecordDelta', 0)} | {kpis.get('upsertRatioDelta', 0)} | {kpis.get('jobsPerMinuteDelta', 0)} | {kpis.get('reviewRateDelta', 0)} | {kpis.get('balancedScore', 0)} |"
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
    coverage_component = float(kpis.get("upsertRatioDelta", 0.0))
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


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()




















