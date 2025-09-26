from __future__ import annotations

import csv
import sqlite3

from social_crawler.config import LedgerConfig
from social_crawler.ledger import Ledger, LedgerEntry


def make_entry(post_id: str, title: str = "Title") -> LedgerEntry:
    return LedgerEntry(
        post_id=post_id,
        created_utc=1_650_000_000.0,
        subreddit="python",
        author="tester",
        title=title,
        permalink="https://reddit.com/abc",
        url="https://reddit.com/abc",
        media_url="https://cdn.example.com/video.mp4",
        cached_json_path=f"json/python/{post_id}.json",
        cached_media_path=f"media/python/{post_id}.mp4",
    )


def test_ledger_csv_records_and_appends(tmp_path) -> None:
    csv_path = tmp_path / "ledger.csv"
    config = LedgerConfig(mode="csv", csv_path=csv_path)
    ledger = Ledger(config)

    ledger.record(make_entry("abc"))
    ledger.record(make_entry("def"))

    with csv_path.open("r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    assert [row["post_id"] for row in rows] == ["abc", "def"]


def test_ledger_sqlite_upserts_on_conflict(tmp_path) -> None:
    db_path = tmp_path / "ledger.db"
    config = LedgerConfig(mode="sqlite", sqlite_path=db_path)
    ledger = Ledger(config)

    ledger.record(make_entry("abc", title="First"))
    ledger.record(make_entry("abc", title="Updated"))

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT post_id, title FROM reddit_posts").fetchall()

    assert rows == [("abc", "Updated")]
