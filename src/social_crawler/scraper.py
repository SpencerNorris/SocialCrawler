from __future__ import annotations

import mimetypes
import time
from pathlib import Path
from typing import Any, Optional, Set
from urllib.parse import urlparse

import httpx

from .config import QueryConfig, RedditCredentials, ScraperConfig
from .ledger import Ledger, LedgerEntry
from .reddit_client import RedditClient, RedditPost
from .storage import StorageBackend, build_storage_backend


class RedditScraper:
    def __init__(
        self,
        creds: RedditCredentials,
        config: ScraperConfig,
        *,
        session: Optional[httpx.Client] = None,
    ) -> None:
        config.ensure_paths()
        self.config = config
        self.client = RedditClient(creds)
        self.storage: StorageBackend = build_storage_backend(
            config.storage.backend,
            local_path=config.storage.local_path,
            gcs_bucket=config.storage.gcs_bucket,
            gcs_prefix=config.storage.gcs_prefix,
        )
        self.ledger = Ledger(config.ledger)
        self.http = session or httpx.Client(timeout=20.0)

    def run(self) -> None:
        """Drive one scrape pass.

        For each post yielded by the client:
          - skip silently if it already matched a different query this run
            (a single submission can match many queries in ``QueryConfig``)
          - if the ``post_id`` is already in the ledger, append a fresh
            metadata row (updated score, num_comments, scraped_utc) but
            skip re-downloading JSON and media
          - otherwise run the full pipeline and append a row
        """
        known_post_ids: Set[str] = self.ledger.load_known_post_ids()
        seen_this_run: Set[str] = set()

        for post in self.client.iter_posts(self.config.queries):
            print(post)
            if self.config.queries.media_only and not post.media_url:
                continue
            if post.id in seen_this_run:
                continue
            seen_this_run.add(post.id)

            download_media = bool(self.config.queries.download_media and post.media_url)

            if post.id in known_post_ids:
                json_path = self._make_json_path(post)
                media_path = self._make_media_path(post) if download_media else None
            else:
                json_path = self._cache_post_json(post)
                media_path = self._cache_media(post) if download_media else None

            entry = LedgerEntry(
                post_id=post.id,
                created_utc=post.created_utc,
                subreddit=post.subreddit,
                author=post.author,
                title=post.title,
                permalink=post.permalink,
                url=post.url,
                media_url=post.media_url,
                cached_json_path=json_path,
                cached_media_path=media_path,
                scraped_utc=time.time(),
                score=self._int_from_raw(post.raw, "score"),
                num_comments=self._int_from_raw(post.raw, "num_comments"),
            )
            self.ledger.record(entry)

    @staticmethod
    def _int_from_raw(raw: Any, key: str) -> Optional[int]:
        if not isinstance(raw, dict):
            return None
        value = raw.get(key)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _cache_post_json(self, post: RedditPost) -> str:
        relative = self._make_json_path(post)
        if not self.storage.exists(relative):
            self.storage.save_json(relative, post.raw)
        return relative

    def _cache_media(self, post: RedditPost) -> Optional[str]:
        if not post.media_url:
            return None
        relative = self._make_media_path(post)
        if self.storage.exists(relative):
            return relative
        response = self.http.get(post.media_url, follow_redirects=True)
        response.raise_for_status()
        self.storage.save_bytes(relative, response.content)
        return relative

    @staticmethod
    def _make_json_path(post: RedditPost) -> str:
        return f"json/{post.subreddit}/{post.id}.json"

    def _make_media_path(self, post: RedditPost) -> str:
        parsed = urlparse(post.media_url or "")
        extension = self._determine_extension(parsed.path, parsed.query)
        filename = f"{post.id}{extension}"
        safe_subreddit = post.subreddit.replace("/", "_")
        return f"media/{safe_subreddit}/{filename}"

    def _determine_extension(self, path: str, query: str) -> str:
        guess = Path(path).suffix
        if guess:
            return guess
        mime = None
        if "mimetype=" in query:
            mime = query.split("mimetype=")[-1].split("&", 1)[0]
        if not mime and path:
            mime, _ = mimetypes.guess_type(path)
        if mime:
            ext = mimetypes.guess_extension(mime)
            if ext:
                return ext
        return ".bin"

    def close(self) -> None:
        self.client.close()
        self.http.close()


def load_config(
    *,
    creds: Optional[RedditCredentials] = None,
    config: Optional[ScraperConfig] = None,
    queries: Optional[QueryConfig] = None,
) -> RedditScraper:
    credentials = creds or RedditCredentials()
    scraper_config = config or ScraperConfig()
    if queries:
        scraper_config.queries = queries
    return RedditScraper(credentials, scraper_config)


__all__ = ["RedditScraper", "load_config"]
