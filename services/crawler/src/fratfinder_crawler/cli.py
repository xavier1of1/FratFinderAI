from __future__ import annotations

import argparse
import json

from fratfinder_crawler.config import get_settings
from fratfinder_crawler.logging_utils import configure_logging
from fratfinder_crawler.pipeline import CrawlService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Frat Finder AI crawler")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run ingestion for active sources")
    run_parser.add_argument("--source-slug", help="Only crawl one source slug", default=None)

    jobs_parser = subparsers.add_parser("process-field-jobs", help="Process queued field jobs")
    jobs_parser.add_argument("--limit", type=int, default=25)
    jobs_parser.add_argument("--source-slug", help="Only process field jobs for one source slug", default=None)

    health_parser = subparsers.add_parser("health", help="Run crawler health probes")
    health_parser.add_argument("--probe", choices=["liveness", "readiness"], default="readiness")

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
        result = service.process_field_jobs(limit=args.limit, source_slug=args.source_slug)
        print(json.dumps(result, indent=2))
        return

    if args.command == "health":
        result = service.liveness() if args.probe == "liveness" else service.readiness()
        print(json.dumps(result, indent=2))
        return

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
