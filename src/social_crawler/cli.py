from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

from .config import LedgerConfig, QueryConfig, RedditCredentials, ScraperConfig, StorageConfig
from .scraper import RedditScraper


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reddit scraping utility")
    parser.add_argument("--query", action="append", default=[], help="Search query string. Repeatable.")
    parser.add_argument("--subreddit", action="append", default=[], help="Target subreddit. Repeatable.")
    parser.add_argument("--sort", default="new", choices=["relevance", "hot", "top", "new", "comments"], help="Sort order")
    parser.add_argument("--time-filter", default="all", choices=["hour", "day", "week", "month", "year", "all"], help="Time filter for searches")
    parser.add_argument("--max-posts", type=int, default=50, help="Max posts per query/subreddit")
    parser.add_argument("--media-only", action="store_true", help="Require posts to include media")
    parser.add_argument("--download-media", action="store_true", help="Download media files when available")

    parser.add_argument("--storage-backend", default="local", choices=["local", "gcs"], help="Storage backend for cached files")
    parser.add_argument("--storage-path", default="cache", help="Local directory for cached data")
    parser.add_argument("--gcs-bucket", default=None, help="GCS bucket for storage backend")
    parser.add_argument("--gcs-prefix", default="social_crawler", help="Base prefix for GCS uploads")

    parser.add_argument("--ledger-mode", default="csv", choices=["csv", "sqlite"], help="Ledger persistence mode")
    parser.add_argument("--ledger-path", default="ledger.csv", help="Path for CSV ledger or sqlite DB")

    return parser.parse_args(argv)


def build_config(ns: argparse.Namespace) -> ScraperConfig:
    query_config = QueryConfig(
        queries=ns.query,
        subreddits=ns.subreddit,
        sort=ns.sort,
        time_filter=ns.time_filter,
        max_posts=ns.max_posts,
        media_only=ns.media_only,
        download_media=ns.download_media,
    )

    storage_config = StorageConfig(
        backend=ns.storage_backend,
        local_path=ns.storage_path,
        gcs_bucket=ns.gcs_bucket,
        gcs_prefix=ns.gcs_prefix,
    )

    ledger_path = ns.ledger_path
    ledger_config = LedgerConfig(
        mode=ns.ledger_mode,
        csv_path=ledger_path if ns.ledger_mode == "csv" else "ledger.csv",
        sqlite_path=ledger_path if ns.ledger_mode == "sqlite" else "ledger.db",
    )

    return ScraperConfig(queries=query_config, storage=storage_config, ledger=ledger_config)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv or sys.argv[1:])
    creds = RedditCredentials()
    config = build_config(args)
    scraper = RedditScraper(creds, config)
    try:
        scraper.run()
    finally:
        scraper.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
