
from __future__ import annotations

import argparse
import json

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.logging_utils import configure_logging
from fratfinder_crawler.models import FIELD_JOB_TYPES
from fratfinder_crawler.pipeline import CrawlService


CRAWL_RUNTIME_CHOICES = ["adaptive_shadow", "adaptive_assisted"]
LIVE_CRAWL_RUNTIME_CHOICES = ["adaptive_assisted"]
FIELD_JOB_RUNTIME_CHOICES = ["langgraph_primary"]
REQUEST_RUNTIME_CHOICES = ["v3_request_supervisor"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Frat Finder AI crawler")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run ingestion for active sources")
    run_parser.add_argument("--source-slug", help="Only crawl one source slug", default=None)
    run_parser.add_argument(
        "--runtime-mode",
        choices=CRAWL_RUNTIME_CHOICES,
        default=None,
        help="Override runtime mode for this batch",
    )
    run_parser.add_argument("--policy-mode", choices=["live", "train"], default="live")

    request_parser = subparsers.add_parser("run-request", help="Run one fraternity crawl request through the V3 request graph")
    request_parser.add_argument("--request-id", required=True)
    request_parser.add_argument("--runtime-mode", choices=REQUEST_RUNTIME_CHOICES, default="v3_request_supervisor")
    request_parser.add_argument("--crawl-runtime-mode", choices=LIVE_CRAWL_RUNTIME_CHOICES, default=None)
    request_parser.add_argument("--field-job-runtime-mode", choices=FIELD_JOB_RUNTIME_CHOICES, default=None)
    request_parser.add_argument("--graph-durability", choices=["exit", "async", "sync"], default=None)

    request_worker_parser = subparsers.add_parser("run-request-worker", help="Claim and execute queued fraternity crawl requests")
    request_worker_parser.add_argument("--once", action="store_true", help="Process up to the current batch limit and then exit")
    request_worker_parser.add_argument("--limit", type=int, default=None, help="Maximum requests to process in one batch")
    request_worker_parser.add_argument("--poll-seconds", type=int, default=None, help="Worker idle poll interval in seconds")
    request_worker_parser.add_argument("--runtime-mode", choices=REQUEST_RUNTIME_CHOICES, default="v3_request_supervisor")

    field_job_worker_parser = subparsers.add_parser("run-field-job-worker", help="Continuously claim and execute queued field jobs while actionable work exists")
    field_job_worker_parser.add_argument("--once", action="store_true", help="Process one bounded field-job batch and then exit")
    field_job_worker_parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process per batch")
    field_job_worker_parser.add_argument("--workers", type=int, default=None, help="Number of field-job workers per batch")
    field_job_worker_parser.add_argument("--poll-seconds", type=int, default=None, help="Worker idle poll interval in seconds")
    field_job_worker_parser.add_argument("--runtime-mode", choices=FIELD_JOB_RUNTIME_CHOICES, default=None, help="Field-job runtime mode")
    field_job_worker_parser.add_argument("--graph-durability", choices=["exit", "async", "sync"], default=None, help="LangGraph checkpoint durability mode")
    field_job_worker_parser.add_argument("--skip-preflight", action="store_true", help="Skip search preflight before each field-job batch")

    jobs_parser = subparsers.add_parser("process-field-jobs", help="Process queued field jobs")
    jobs_parser.add_argument("--limit", type=int, default=25)
    jobs_parser.add_argument("--source-slug", help="Only process field jobs for one source slug", default=None)
    jobs_parser.add_argument("--field-name", choices=FIELD_JOB_TYPES, help="Only process one field job type", default=None)
    jobs_parser.add_argument("--workers", type=int, default=None, help="Number of concurrent field-job workers to run")
    jobs_parser.add_argument("--runtime-mode", choices=FIELD_JOB_RUNTIME_CHOICES, default=None, help="Field-job runtime mode")
    jobs_parser.add_argument("--graph-durability", choices=["exit", "async", "sync"], default=None, help="LangGraph checkpoint durability mode")
    jobs_parser.add_argument(
        "--require-healthy-search",
        action="store_true",
        help="Run a preflight and skip the batch when provider health is degraded",
    )
    jobs_parser.add_argument(
        "--run-preflight",
        action="store_true",
        default=None,
        help="Run provider preflight before processing jobs (degraded mode may be applied)",
    )

    preflight_parser = subparsers.add_parser("search-preflight", help="Run search provider health probes")
    preflight_parser.add_argument("--probes", type=int, default=None, help="Number of probe queries to run")

    smoke_parser = subparsers.add_parser("search-provider-smoke", help="Run a fixed provider smoke cohort and emit JSON metrics")
    smoke_parser.add_argument("--provider", required=True, help="Provider to evaluate, for example serper_api or tavily_api")
    smoke_parser.add_argument("--query-set-file", default=None, help="Optional JSON or JSONL file with category/query records")
    smoke_parser.add_argument("--max-queries", type=int, default=None, help="Optional cap on the number of cohort queries")
    smoke_parser.add_argument("--delay-ms", type=int, default=None, help="Optional delay between cohort queries")
    smoke_parser.add_argument("--output-path", default=None, help="Optional JSON report output path")

    subparsers.add_parser("doctor", help="Report effective crawler settings, env resolution, provider reachability, and worker liveness")

    baseline_parser = subparsers.add_parser("system-baseline", help="Capture a live baseline snapshot for accuracy, queue state, and provider health")
    baseline_parser.add_argument("--skip-preflight", action="store_true", help="Skip search preflight in the baseline snapshot")
    baseline_parser.add_argument("--probes", type=int, default=None, help="Number of preflight probes when search health is included")

    provenance_parser = subparsers.add_parser("provenance-audit", help="Audit accepted contact provenance completeness and national-profile collisions")
    provenance_parser.add_argument("--limit", type=int, default=50, help="Maximum number of sample rows to return")

    enrichment_shadow_parser = subparsers.add_parser("enrichment-policy-shadow", help="Score queued field jobs with the enrichment shadow policy")
    enrichment_shadow_parser.add_argument("--limit", type=int, default=50)
    enrichment_shadow_parser.add_argument("--source-slug", default=None)
    enrichment_shadow_parser.add_argument("--field-name", choices=FIELD_JOB_TYPES, default=None)
    enrichment_shadow_parser.add_argument("--skip-preflight", action="store_true", help="Skip search preflight and use cached/default provider window assumptions")
    enrichment_shadow_parser.add_argument("--probes", type=int, default=None)

    enrichment_export_parser = subparsers.add_parser("enrichment-export-observations", help="Export observed enrichment shadow decisions and outcomes")
    enrichment_export_parser.add_argument("--source-slug", default=None)
    enrichment_export_parser.add_argument("--field-name", choices=FIELD_JOB_TYPES, default=None)
    enrichment_export_parser.add_argument("--window-days", type=int, default=None)
    enrichment_export_parser.add_argument("--limit", type=int, default=None)

    enrichment_replay_parser = subparsers.add_parser("enrichment-replay-policy", help="Summarize enrichment shadow recommendations versus deterministic outcomes")
    enrichment_replay_parser.add_argument("--source-slug", default=None)
    enrichment_replay_parser.add_argument("--field-name", choices=FIELD_JOB_TYPES, default=None)
    enrichment_replay_parser.add_argument("--window-days", type=int, default=None)
    enrichment_replay_parser.add_argument("--limit", type=int, default=None)

    enrichment_compare_parser = subparsers.add_parser("enrichment-compare-report", help="Break down RL-vs-deterministic enrichment disagreements and opportunity areas")
    enrichment_compare_parser.add_argument("--source-slug", default=None)
    enrichment_compare_parser.add_argument("--field-name", choices=FIELD_JOB_TYPES, default=None)
    enrichment_compare_parser.add_argument("--window-days", type=int, default=None)
    enrichment_compare_parser.add_argument("--limit", type=int, default=None)

    enrichment_promote_parser = subparsers.add_parser("enrichment-promote-verify-school", help="Identify or enqueue verify_school jobs for the safest RL-backed opportunity class")
    enrichment_promote_parser.add_argument("--source-slug", default=None)
    enrichment_promote_parser.add_argument("--field-name", choices=FIELD_JOB_TYPES, default=None)
    enrichment_promote_parser.add_argument("--limit", type=int, default=50)
    enrichment_promote_parser.add_argument("--apply", action="store_true", help="Actually enqueue verify_school jobs instead of reporting candidates only")
    enrichment_promote_parser.add_argument("--skip-preflight", action="store_true", help="Skip search preflight and use cached/default provider window assumptions")
    enrichment_promote_parser.add_argument("--probes", type=int, default=None)

    health_parser = subparsers.add_parser("health", help="Run crawler health probes")
    health_parser.add_argument("--probe", choices=["liveness", "readiness"], default="readiness")

    discover_parser = subparsers.add_parser("discover-source", help="Discover likely national source for a fraternity name")
    discover_parser.add_argument("--fraternity-name", required=True, help="Fraternity display name, e.g. Lambda Chi Alpha")

    bootstrap_parser = subparsers.add_parser(
        "bootstrap-nic-sources",
        help="Load verified fraternity source seeds from a research JSON file",
    )
    bootstrap_parser.add_argument("--input", required=True, help="Path to bootstrap JSON (for example research_nav_21.json)")
    bootstrap_parser.add_argument("--dry-run", action="store_true", help="Validate and score records without writing to DB")

    revalidate_one_parser = subparsers.add_parser(
        "revalidate-verified-source",
        help="Revalidate one verified source by fraternity slug",
    )
    revalidate_one_parser.add_argument("--fraternity-slug", required=True, help="Fraternity slug to revalidate")

    revalidate_many_parser = subparsers.add_parser(
        "revalidate-verified-sources",
        help="Revalidate up to N verified sources by newest checked_at",
    )
    revalidate_many_parser.add_argument("--limit", type=int, default=20)

    export_parser = subparsers.add_parser("crawl-export-observations", help="Export adaptive crawl observations")
    export_parser.add_argument("--source-slug", default=None)
    export_parser.add_argument("--crawl-session-id", default=None)
    export_parser.add_argument("--runtime-mode", choices=CRAWL_RUNTIME_CHOICES, default=None)
    export_parser.add_argument("--window-days", type=int, default=None)
    export_parser.add_argument("--limit", type=int, default=None)

    replay_parser = subparsers.add_parser("crawl-replay-policy", help="Summarize observed adaptive policy outcomes")
    replay_parser.add_argument("--source-slug", default=None)
    replay_parser.add_argument("--runtime-mode", choices=CRAWL_RUNTIME_CHOICES, default=None)
    replay_parser.add_argument("--window-days", type=int, default=None)
    replay_parser.add_argument("--limit", type=int, default=None)

    policy_report_parser = subparsers.add_parser("crawl-policy-report", help="Show adaptive template/profile policy report")
    policy_report_parser.add_argument("--limit", type=int, default=25)

    epoch_parser = subparsers.add_parser("adaptive-train-eval", help="Run repeated adaptive train/eval epochs and publish KPI slope report")
    epoch_parser.add_argument("--epochs", type=int, default=None)
    epoch_parser.add_argument("--train-sources", default=None, help="Comma-separated source slugs for train epochs")
    epoch_parser.add_argument("--eval-sources", default=None, help="Comma-separated source slugs for eval epochs")
    epoch_parser.add_argument("--runtime-mode", choices=CRAWL_RUNTIME_CHOICES, default=None)
    epoch_parser.add_argument("--cohort-label", default="target-cohort")
    epoch_parser.add_argument("--policy-version", default=None)
    epoch_parser.add_argument("--replay-window-days", type=int, default=None)
    epoch_parser.add_argument("--replay-batch-size", type=int, default=None)
    epoch_parser.add_argument("--report-path", default=None)
    epoch_parser.add_argument("--eval-enrichment-limit-per-source", type=int, default=None)
    epoch_parser.add_argument("--eval-enrichment-workers", type=int, default=None)

    loop_parser = subparsers.add_parser("adaptive-train-loop", help="Run multiple train/eval rounds")
    loop_parser.add_argument("--rounds", type=int, default=2)
    loop_parser.add_argument("--epochs-per-round", type=int, default=None)
    loop_parser.add_argument("--train-sources", default=None)
    loop_parser.add_argument("--eval-sources", default=None)
    loop_parser.add_argument("--runtime-mode", choices=CRAWL_RUNTIME_CHOICES, default=None)
    loop_parser.add_argument("--cohort-label", default="target-cohort")
    loop_parser.add_argument("--report-dir", default="docs/reports")
    loop_parser.add_argument("--eval-enrichment-limit-per-source", type=int, default=None)
    loop_parser.add_argument("--eval-enrichment-workers", type=int, default=None)

    replay_window_parser = subparsers.add_parser("adaptive-replay-window", help="Export adaptive replay window observations and rewards")
    replay_window_parser.add_argument("--source-slugs", required=True, help="Comma-separated source slugs")
    replay_window_parser.add_argument("--runtime-mode", choices=CRAWL_RUNTIME_CHOICES, default=None)
    replay_window_parser.add_argument("--window-days", type=int, default=None)
    replay_window_parser.add_argument("--limit", type=int, default=None)

    policy_diff_parser = subparsers.add_parser("adaptive-policy-diff", help="Compare two policy snapshots")
    policy_diff_parser.add_argument("--snapshot-a", required=True, type=int)
    policy_diff_parser.add_argument("--snapshot-b", required=True, type=int)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.crawler_log_level)

    service = CrawlService(settings)

    if args.command == "run":
        result = service.run(source_slug=args.source_slug, runtime_mode=args.runtime_mode, policy_mode=args.policy_mode)
        print(json.dumps(result, indent=2))
        return

    if args.command == "run-request":
        result = service.run_request(
            request_id=args.request_id,
            runtime_mode=args.runtime_mode,
            crawl_runtime_mode=args.crawl_runtime_mode,
            field_job_runtime_mode=args.field_job_runtime_mode,
            graph_durability=args.graph_durability,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "run-request-worker":
        result = service.run_request_worker(
            once=args.once,
            limit=args.limit,
            poll_seconds=args.poll_seconds,
            runtime_mode=args.runtime_mode,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "run-field-job-worker":
        result = service.run_field_job_worker(
            once=args.once,
            limit=args.limit,
            workers=args.workers,
            poll_seconds=args.poll_seconds,
            runtime_mode=args.runtime_mode,
            graph_durability=args.graph_durability,
            run_preflight=not args.skip_preflight,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "process-field-jobs":
        result = service.process_field_jobs(
            limit=args.limit,
            source_slug=args.source_slug,
            field_name=args.field_name,
            workers=args.workers,
            require_healthy_search=args.require_healthy_search,
            run_preflight=args.run_preflight,
            runtime_mode=args.runtime_mode,
            graph_durability=args.graph_durability,
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "search-preflight":
        result = service.search_preflight(probes=args.probes)
        print(json.dumps(result, indent=2))
        return

    if args.command == "search-provider-smoke":
        result = service.search_provider_smoke(
            provider=args.provider,
            query_set_file=args.query_set_file,
            max_queries=args.max_queries,
            output_path=args.output_path,
            delay_ms=args.delay_ms,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "doctor":
        result = service.doctor()
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "system-baseline":
        result = service.system_baseline(include_preflight=not args.skip_preflight, probes=args.probes)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "provenance-audit":
        result = service.provenance_completeness_audit(limit=args.limit)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "enrichment-policy-shadow":
        result = service.enrichment_shadow_policy_report(
            limit=args.limit,
            source_slug=args.source_slug,
            field_name=args.field_name,
            include_preflight=not args.skip_preflight,
            probes=args.probes,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "enrichment-export-observations":
        result = service.export_enrichment_observations(
            source_slug=args.source_slug,
            field_name=args.field_name,
            window_days=args.window_days,
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "enrichment-replay-policy":
        result = service.enrichment_replay_policy(
            source_slug=args.source_slug,
            field_name=args.field_name,
            window_days=args.window_days,
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "enrichment-compare-report":
        result = service.enrichment_policy_compare_report(
            source_slug=args.source_slug,
            field_name=args.field_name,
            window_days=args.window_days,
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "enrichment-promote-verify-school":
        result = service.enrichment_promote_verify_school_candidates(
            source_slug=args.source_slug,
            field_name=args.field_name,
            limit=args.limit,
            apply_changes=args.apply,
            include_preflight=not args.skip_preflight,
            probes=args.probes,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "health":
        result = service.liveness() if args.probe == "liveness" else service.readiness()
        print(json.dumps(result, indent=2))
        return

    if args.command == "discover-source":
        result = service.discover_source(fraternity_name=args.fraternity_name)
        print(json.dumps(result, indent=2))
        return

    if args.command == "bootstrap-nic-sources":
        result = service.bootstrap_verified_sources(input_path=args.input, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
        return

    if args.command == "revalidate-verified-source":
        result = service.revalidate_verified_source(fraternity_slug=args.fraternity_slug)
        print(json.dumps(result, indent=2))
        return

    if args.command == "revalidate-verified-sources":
        result = service.revalidate_verified_sources(limit=args.limit)
        print(json.dumps(result, indent=2))
        return

    if args.command == "crawl-export-observations":
        result = service.export_crawl_observations(
            source_slug=args.source_slug,
            crawl_session_id=args.crawl_session_id,
            runtime_mode=args.runtime_mode,
            window_days=args.window_days,
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "crawl-replay-policy":
        result = service.crawl_replay_policy(
            source_slug=args.source_slug,
            runtime_mode=args.runtime_mode,
            window_days=args.window_days,
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "crawl-policy-report":
        result = service.crawl_policy_report(limit=args.limit)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "adaptive-replay-window":
        runtime_mode = args.runtime_mode or settings.crawler_adaptive_train_default_runtime_mode
        source_slugs = [value.strip() for value in str(args.source_slugs).split(",") if value.strip()]
        result = service.adaptive_replay_window(
            source_slugs=source_slugs,
            runtime_mode=runtime_mode,
            window_days=args.window_days,
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "adaptive-policy-diff":
        result = service.adaptive_policy_diff(snapshot_id_a=args.snapshot_a, snapshot_id_b=args.snapshot_b)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "adaptive-train-eval":
        runtime_mode = args.runtime_mode or settings.crawler_adaptive_train_default_runtime_mode
        epochs = args.epochs if args.epochs is not None else settings.crawler_adaptive_train_default_epochs
        train_raw = args.train_sources if args.train_sources is not None else settings.crawler_adaptive_train_source_slugs
        eval_raw = args.eval_sources if args.eval_sources is not None else settings.crawler_adaptive_eval_source_slugs
        train_source_slugs = [value.strip() for value in str(train_raw).split(",") if value.strip()]
        eval_source_slugs = [value.strip() for value in str(eval_raw).split(",") if value.strip()]
        if not train_source_slugs:
            raise ValueError("adaptive-train-eval requires train sources via --train-sources or CRAWLER_ADAPTIVE_TRAIN_SOURCE_SLUGS")
        if not eval_source_slugs:
            raise ValueError("adaptive-train-eval requires eval sources via --eval-sources or CRAWLER_ADAPTIVE_EVAL_SOURCE_SLUGS")
        result = service.adaptive_train_eval(
            epochs=epochs,
            train_source_slugs=train_source_slugs,
            eval_source_slugs=eval_source_slugs,
            runtime_mode=runtime_mode,
            cohort_label=args.cohort_label,
            policy_version=args.policy_version,
            replay_window_days=args.replay_window_days,
            replay_batch_size=args.replay_batch_size,
            report_path=args.report_path,
            eval_enrichment_limit_per_source=args.eval_enrichment_limit_per_source,
            eval_enrichment_workers=args.eval_enrichment_workers,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "adaptive-train-loop":
        runtime_mode = args.runtime_mode or settings.crawler_adaptive_train_default_runtime_mode
        epochs_per_round = args.epochs_per_round if args.epochs_per_round is not None else settings.crawler_adaptive_train_default_epochs
        train_raw = args.train_sources if args.train_sources is not None else settings.crawler_adaptive_train_source_slugs
        eval_raw = args.eval_sources if args.eval_sources is not None else settings.crawler_adaptive_eval_source_slugs
        train_source_slugs = [value.strip() for value in str(train_raw).split(",") if value.strip()]
        eval_source_slugs = [value.strip() for value in str(eval_raw).split(",") if value.strip()]
        if not train_source_slugs:
            raise ValueError("adaptive-train-loop requires train sources via --train-sources or CRAWLER_ADAPTIVE_TRAIN_SOURCE_SLUGS")
        if not eval_source_slugs:
            raise ValueError("adaptive-train-loop requires eval sources via --eval-sources or CRAWLER_ADAPTIVE_EVAL_SOURCE_SLUGS")
        result = service.adaptive_train_loop(
            rounds=args.rounds,
            epochs_per_round=epochs_per_round,
            train_source_slugs=train_source_slugs,
            eval_source_slugs=eval_source_slugs,
            runtime_mode=runtime_mode,
            cohort_label=args.cohort_label,
            report_dir=args.report_dir,
            eval_enrichment_limit_per_source=args.eval_enrichment_limit_per_source,
            eval_enrichment_workers=args.eval_enrichment_workers,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
