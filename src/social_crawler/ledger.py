from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Set

from .config import LedgerConfig


@dataclass
class LedgerEntry:
    post_id: str
    created_utc: float
    subreddit: str
    author: str
    title: str
    permalink: str
    url: str
    media_url: Optional[str]
    cached_json_path: Optional[str]
    cached_media_path: Optional[str]
    scraped_utc: Optional[float] = None
    score: Optional[int] = None
    num_comments: Optional[int] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "post_id": self.post_id,
            "created_utc": str(self.created_utc),
            "subreddit": self.subreddit,
            "author": self.author,
            "title": self.title,
            "permalink": self.permalink,
            "url": self.url,
            "media_url": self.media_url or "",
            "cached_json_path": self.cached_json_path or "",
            "cached_media_path": self.cached_media_path or "",
            "scraped_utc": "" if self.scraped_utc is None else str(self.scraped_utc),
            "score": "" if self.score is None else str(self.score),
            "num_comments": "" if self.num_comments is None else str(self.num_comments),
        }


class Ledger:
    # New columns (scraped_utc, score, num_comments) are appended at the end
    # so ledgers written under the older schema can be migrated by adding
    # empty trailing columns rather than rewriting the whole layout.
    FIELDNAMES = [
        "post_id",
        "created_utc",
        "subreddit",
        "author",
        "title",
        "permalink",
        "url",
        "media_url",
        "cached_json_path",
        "cached_media_path",
        "scraped_utc",
        "score",
        "num_comments",
    ]

    def __init__(self, config: LedgerConfig) -> None:
        self.config = config
        if config.mode == "csv":
            self._init_csv()
        elif config.mode == "sqlite":
            self._init_sqlite()
        else:
            raise ValueError(f"Unsupported ledger mode: {config.mode}")

    def _init_csv(self) -> None:
        path = self.config.csv_path
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.FIELDNAMES)
                writer.writeheader()
            return
        self._migrate_csv_if_needed(path)

    def _migrate_csv_if_needed(self, path: Path) -> None:
        """Rewrite an older ledger CSV to the current schema.

        Existing rows keep their original values; newly added columns are
        left empty so downstream code can treat them as "not known at
        scrape time."
        """
        with path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader, [])
        if header == self.FIELDNAMES:
            return
        with path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            existing_rows = list(reader)
        with path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.FIELDNAMES)
            writer.writeheader()
            for row in existing_rows:
                writer.writerow({field: row.get(field, "") for field in self.FIELDNAMES})

    def _init_sqlite(self) -> None:
        path = self.config.sqlite_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reddit_posts (
                    post_id TEXT PRIMARY KEY,
                    created_utc REAL,
                    subreddit TEXT,
                    author TEXT,
                    title TEXT,
                    permalink TEXT,
                    url TEXT,
                    media_url TEXT,
                    cached_json_path TEXT,
                    cached_media_path TEXT,
                    scraped_utc REAL,
                    score INTEGER,
                    num_comments INTEGER
                )
                """
            )
            existing = {row[1] for row in conn.execute("PRAGMA table_info(reddit_posts)")}
            for column, sql_type in (
                ("scraped_utc", "REAL"),
                ("score", "INTEGER"),
                ("num_comments", "INTEGER"),
            ):
                if column not in existing:
                    conn.execute(f"ALTER TABLE reddit_posts ADD COLUMN {column} {sql_type}")
            conn.commit()

    def load_known_post_ids(self) -> Set[str]:
        """Return every ``post_id`` already recorded in the ledger.

        Used by the scraper to decide whether to re-download content for a
        post or just append a metadata-refresh row.
        """
        if self.config.mode == "csv":
            return self._load_known_csv()
        return self._load_known_sqlite()

    def _load_known_csv(self) -> Set[str]:
        path = self.config.csv_path
        if not path.exists():
            return set()
        ids: Set[str] = set()
        with path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                pid = (row.get("post_id") or "").strip()
                if pid:
                    ids.add(pid)
        return ids

    def _load_known_sqlite(self) -> Set[str]:
        path = self.config.sqlite_path
        if not path.exists():
            return set()
        with sqlite3.connect(path) as conn:
            rows = conn.execute("SELECT post_id FROM reddit_posts").fetchall()
        return {row[0] for row in rows if row[0]}

    def record(self, entry: LedgerEntry) -> None:
        if self.config.mode == "csv":
            self._append_csv(entry)
        else:
            self._upsert_sqlite(entry)

    def _append_csv(self, entry: LedgerEntry) -> None:
        with self.config.csv_path.open("a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.FIELDNAMES)
            writer.writerow(entry.to_dict())

    def _upsert_sqlite(self, entry: LedgerEntry) -> None:
        with sqlite3.connect(self.config.sqlite_path) as conn:
            conn.execute(
                """
                INSERT INTO reddit_posts (
                    post_id, created_utc, subreddit, author, title,
                    permalink, url, media_url, cached_json_path, cached_media_path,
                    scraped_utc, score, num_comments
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id) DO UPDATE SET
                    created_utc=excluded.created_utc,
                    subreddit=excluded.subreddit,
                    author=excluded.author,
                    title=excluded.title,
                    permalink=excluded.permalink,
                    url=excluded.url,
                    media_url=excluded.media_url,
                    cached_json_path=excluded.cached_json_path,
                    cached_media_path=excluded.cached_media_path,
                    scraped_utc=excluded.scraped_utc,
                    score=excluded.score,
                    num_comments=excluded.num_comments
                """,
                (
                    entry.post_id,
                    entry.created_utc,
                    entry.subreddit,
                    entry.author,
                    entry.title,
                    entry.permalink,
                    entry.url,
                    entry.media_url,
                    entry.cached_json_path,
                    entry.cached_media_path,
                    entry.scraped_utc,
                    entry.score,
                    entry.num_comments,
                ),
            )
            conn.commit()


__all__ = ["Ledger", "LedgerEntry"]
