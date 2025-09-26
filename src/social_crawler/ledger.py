from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

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
        }


class Ledger:
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
                    cached_media_path TEXT
                )
                """
            )
            conn.commit()

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
                    permalink, url, media_url, cached_json_path, cached_media_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id) DO UPDATE SET
                    created_utc=excluded.created_utc,
                    subreddit=excluded.subreddit,
                    author=excluded.author,
                    title=excluded.title,
                    permalink=excluded.permalink,
                    url=excluded.url,
                    media_url=excluded.media_url,
                    cached_json_path=excluded.cached_json_path,
                    cached_media_path=excluded.cached_media_path
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
                ),
            )
            conn.commit()


__all__ = ["Ledger", "LedgerEntry"]
