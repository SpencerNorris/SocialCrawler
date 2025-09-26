from __future__ import annotations

import csv
from dataclasses import dataclass

import httpx

from social_crawler.config import LedgerConfig, QueryConfig, RedditCredentials, ScraperConfig, StorageConfig
from social_crawler.reddit_client import RedditPost
from social_crawler.scraper import RedditScraper


@dataclass
class DummyClient:
    posts: list[RedditPost]

    def iter_posts(self, config: QueryConfig):  # noqa: D401 - test double
        return iter(self.posts)

    def close(self) -> None:  # pragma: no cover - tests don't rely on it
        pass


class DummyResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.status_code = 200

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=None, response=None)


class DummyHTTP:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[str] = []

    def get(self, url: str, follow_redirects: bool = True) -> DummyResponse:
        self.calls.append(url)
        return DummyResponse(self.payload)

    def close(self) -> None:  # pragma: no cover
        pass


def make_credentials() -> RedditCredentials:
    return RedditCredentials(
        client_id="id",
        client_secret="secret",
        username="user",
        password="pass",
        user_agent="social-crawler-tests",
    )


def make_post(post_id: str, media_url: str | None) -> RedditPost:
    return RedditPost(
        id=post_id,
        title=f"Post {post_id}",
        subreddit="python",
        author="tester",
        permalink=f"/r/python/{post_id}",
        url=f"https://reddit.com/{post_id}",
        created_utc=1_650_000_000.0,
        media_url=media_url,
        raw={"id": post_id, "media_url": media_url},
    )


def test_scraper_media_only_filters_and_records(tmp_path) -> None:
    creds = make_credentials()
    query_config = QueryConfig(queries=[], subreddits=["python"], media_only=True, download_media=False)
    storage_config = StorageConfig(backend="local", local_path=tmp_path / "cache")
    ledger_config = LedgerConfig(mode="csv", csv_path=tmp_path / "ledger.csv")
    config = ScraperConfig(queries=query_config, storage=storage_config, ledger=ledger_config)

    scraper = RedditScraper(creds, config, session=httpx.Client())
    scraper.client.close()
    scraper.client = DummyClient([
        make_post("no_media", None),
        make_post("with_media", "https://cdn.example.com/image.png"),
    ])

    scraper.run()

    ledger_path = tmp_path / "ledger.csv"
    with ledger_path.open("r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    assert [row["post_id"] for row in rows] == ["with_media"]
    assert (tmp_path / "cache" / "json" / "python" / "with_media.json").exists()

    scraper.close()


def test_scraper_downloads_media_when_requested(tmp_path) -> None:
    creds = make_credentials()
    query_config = QueryConfig(queries=[], subreddits=["python"], media_only=False, download_media=True)
    storage_config = StorageConfig(backend="local", local_path=tmp_path / "cache")
    ledger_config = LedgerConfig(mode="csv", csv_path=tmp_path / "ledger.csv")
    config = ScraperConfig(queries=query_config, storage=storage_config, ledger=ledger_config)

    scraper = RedditScraper(creds, config, session=httpx.Client())
    scraper.client.close()
    scraper.client = DummyClient([make_post("media", "https://cdn.example.com/file.mp4")])
    scraper.http = DummyHTTP(b"bytes")

    scraper.run()

    media_file = tmp_path / "cache" / "media" / "python" / "media.mp4"
    assert media_file.exists()
    assert scraper.http.calls == ["https://cdn.example.com/file.mp4"]

    scraper.close()
