from __future__ import annotations

import csv
from dataclasses import dataclass
from types import SimpleNamespace

import httpx
import pytest

from social_crawler.config import LedgerConfig, QueryConfig, RedditCredentials, ScraperConfig, StorageConfig
from social_crawler.reddit_client import RedditClient, RedditPost
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


@dataclass
class DummySubmission:
    id: str
    title: str
    subreddit_name: str
    author_name: str
    permalink: str
    url: str
    created_utc: float
    is_video: bool = False
    media: dict | None = None
    preview: dict | None = None

    @property
    def subreddit(self) -> SimpleNamespace:
        return SimpleNamespace(display_name=self.subreddit_name)

    @property
    def author(self) -> str:
        return self.author_name


class DummyListing:
    def __init__(self, items):
        self._items = list(items)

    def __iter__(self):
        return iter(self._items)


class DummySubreddit:
    def __init__(self, name: str) -> None:
        self.name = name
        self.search_calls: list[tuple[str, str, str, int]] = []
        self.new_calls: list[int] = []
        self._search_results: list[DummySubmission] = []
        self._new_results: list[DummySubmission] = []

    def set_search_results(self, items: list[DummySubmission]) -> None:
        self._search_results = items

    def set_new_results(self, items: list[DummySubmission]) -> None:
        self._new_results = items

    def search(self, query: str, sort: str, time_filter: str, limit: int):
        self.search_calls.append((query, sort, time_filter, limit))
        return DummyListing(self._search_results[:limit])

    def new(self, limit: int):
        self.new_calls.append(limit)
        return DummyListing(self._new_results[:limit])


class DummyReddit:
    def __init__(self, mapping: dict[str, DummySubreddit]) -> None:
        self._mapping = mapping
        self.read_only = False

    def subreddit(self, name: str) -> DummySubreddit:
        return self._mapping[name]


def make_credentials() -> RedditCredentials:
    return RedditCredentials(
        client_id="id",
        client_secret="secret",
        user_agent="social-crawler-tests",
        username=None,
        password=None,
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


def test_scraper_media_only_filters_and_records(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    creds = make_credentials()
    query_config = QueryConfig(queries=[], subreddits=["python"], media_only=True, download_media=False)
    storage_config = StorageConfig(backend="local", local_path=tmp_path / "cache")
    ledger_config = LedgerConfig(mode="csv", csv_path=tmp_path / "ledger.csv")
    config = ScraperConfig(queries=query_config, storage=storage_config, ledger=ledger_config)

    dummy_client = DummyClient([
        make_post("no_media", None),
        make_post("with_media", "https://cdn.example.com/image.png"),
    ])
    monkeypatch.setattr("social_crawler.scraper.RedditClient", lambda *args, **kwargs: dummy_client)

    scraper = RedditScraper(creds, config, session=httpx.Client())

    scraper.run()

    ledger_path = tmp_path / "ledger.csv"
    with ledger_path.open("r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    assert [row["post_id"] for row in rows] == ["with_media"]
    assert (tmp_path / "cache" / "json" / "python" / "with_media.json").exists()

    scraper.close()


def test_scraper_downloads_media_when_requested(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    creds = make_credentials()
    query_config = QueryConfig(queries=[], subreddits=["python"], media_only=False, download_media=True)
    storage_config = StorageConfig(backend="local", local_path=tmp_path / "cache")
    ledger_config = LedgerConfig(mode="csv", csv_path=tmp_path / "ledger.csv")
    config = ScraperConfig(queries=query_config, storage=storage_config, ledger=ledger_config)

    dummy_client = DummyClient([make_post("media", "https://cdn.example.com/file.mp4")])
    monkeypatch.setattr("social_crawler.scraper.RedditClient", lambda *args, **kwargs: dummy_client)

    scraper = RedditScraper(creds, config, session=httpx.Client())
    scraper.http = DummyHTTP(b"bytes")

    scraper.run()

    media_file = tmp_path / "cache" / "media" / "python" / "media.mp4"
    assert media_file.exists()
    assert scraper.http.calls == ["https://cdn.example.com/file.mp4"]

    scraper.close()


def test_reddit_client_searches_with_subreddit(monkeypatch: pytest.MonkeyPatch) -> None:
    subreddit = DummySubreddit("python")
    subreddit.set_search_results([
        DummySubmission(
            id="abc123",
            title="Post abc123",
            subreddit_name="python",
            author_name="tester",
            permalink="/r/python/abc123",
            url="https://img.example.com/pic.png",
            created_utc=1_650_000_000.0,
            preview={"images": [{"source": {"url": "https://img.example.com/pic.png"}}]},
        )
    ])
    reddit = DummyReddit({"python": subreddit, "all": DummySubreddit("all")})

    monkeypatch.setattr("social_crawler.reddit_client.praw", SimpleNamespace(Reddit=lambda **kwargs: reddit))

    client = RedditClient(make_credentials())
    config = QueryConfig(queries=["openai"], subreddits=["python"], max_posts=5)

    posts = list(client.iter_posts(config))

    assert subreddit.search_calls == [("openai", "new", "all", 5)]
    assert [post.id for post in posts] == ["abc123"]


def test_reddit_client_read_only_without_password(monkeypatch: pytest.MonkeyPatch) -> None:
    created_kwargs = {}

    class DummyPrawReddit:
        def __init__(self, **kwargs):
            nonlocal created_kwargs
            created_kwargs = kwargs
            self.read_only = False

        def subreddit(self, name: str) -> DummySubreddit:  # pragma: no cover - not used here
            raise AssertionError("not used")

    monkeypatch.setattr("social_crawler.reddit_client.praw", SimpleNamespace(Reddit=DummyPrawReddit))

    client = RedditClient(make_credentials())
    assert created_kwargs["client_id"] == "id"
    assert client._reddit.read_only is True
