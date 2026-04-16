from __future__ import annotations

import csv
import sqlite3

from social_crawler.config import LedgerConfig
from social_crawler.ledger import Ledger, LedgerEntry


def make_entry(
    post_id: str,
    title: str = "Title",
    *,
    scraped_utc: float | None = 1_650_001_000.0,
    score: int | None = 10,
    num_comments: int | None = 2,
) -> LedgerEntry:
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
        scraped_utc=scraped_utc,
        score=score,
        num_comments=num_comments,
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


def test_ledger_load_known_post_ids_csv(tmp_path) -> None:
    csv_path = tmp_path / "ledger.csv"
    config = LedgerConfig(mode="csv", csv_path=csv_path)
    ledger = Ledger(config)

    ledger.record(make_entry("abc"))
    ledger.record(make_entry("abc"))  # duplicate append — still one post_id
    ledger.record(make_entry("def"))

    assert ledger.load_known_post_ids() == {"abc", "def"}


def test_ledger_load_known_post_ids_empty_before_first_write(tmp_path) -> None:
    csv_path = tmp_path / "ledger.csv"
    config = LedgerConfig(mode="csv", csv_path=csv_path)
    ledger = Ledger(config)

    assert ledger.load_known_post_ids() == set()


def test_ledger_load_known_post_ids_sqlite(tmp_path) -> None:
    db_path = tmp_path / "ledger.db"
    config = LedgerConfig(mode="sqlite", sqlite_path=db_path)
    ledger = Ledger(config)

    ledger.record(make_entry("abc", title="First"))
    ledger.record(make_entry("abc", title="Updated"))  # upsert
    ledger.record(make_entry("def"))

    assert ledger.load_known_post_ids() == {"abc", "def"}


def test_ledger_csv_migrates_legacy_header(tmp_path) -> None:
    """A ledger written under the pre-scraped_utc schema should be migrated
    in place on next init, preserving existing rows and adding empty values
    for the new columns.
    """
    csv_path = tmp_path / "ledger.csv"
    legacy_header = [
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
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=legacy_header)
        writer.writeheader()
        writer.writerow({
            "post_id": "legacy_1",
            "created_utc": "1650000000.0",
            "subreddit": "python",
            "author": "tester",
            "title": "legacy",
            "permalink": "https://reddit.com/legacy",
            "url": "https://reddit.com/legacy",
            "media_url": "",
            "cached_json_path": "json/python/legacy_1.json",
            "cached_media_path": "",
        })

    Ledger(LedgerConfig(mode="csv", csv_path=csv_path))

    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == Ledger.FIELDNAMES
        rows = list(reader)

    assert len(rows) == 1
    assert rows[0]["post_id"] == "legacy_1"
    assert rows[0]["scraped_utc"] == ""
    assert rows[0]["score"] == ""
    assert rows[0]["num_comments"] == ""
