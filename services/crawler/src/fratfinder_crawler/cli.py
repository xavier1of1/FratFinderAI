from __future__ import annotations

import argparse
import json

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.logging_utils import configure_logging
from fratfinder_crawler.models import FIELD_JOB_TYPES
from fratfinder_crawler.pipeline import CrawlService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Frat Finder AI crawler")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run ingestion for active sources")
    run_parser.add_argument("--source-slug", help="Only crawl one source slug", default=None)

    jobs_parser = subparsers.add_parser("process-field-jobs", help="Process queued field jobs")
    jobs_parser.add_argument("--limit", type=int, default=25)
    jobs_parser.add_argument("--source-slug", help="Only process field jobs for one source slug", default=None)
    jobs_parser.add_argument("--field-name", choices=FIELD_JOB_TYPES, help="Only process one field job type", default=None)
    jobs_parser.add_argument("--workers", type=int, default=None, help="Number of concurrent field-job workers to run")
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

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.crawler_log_level)

    service = CrawlService(settings)

    if args.command == "run":
        result = service.run(source_slug=args.source_slug)
        print(json.dumps(result, indent=2))
        return

    if args.command == "process-field-jobs":
        result = service.process_field_jobs(
            limit=args.limit,
            source_slug=args.source_slug,
            field_name=args.field_name,
            workers=args.workers,
            require_healthy_search=args.require_healthy_search,
            run_preflight=args.run_preflight,
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "search-preflight":
        result = service.search_preflight(probes=args.probes)
        print(json.dumps(result, indent=2))
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

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
