from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import httpx

from .config import QueryConfig, RedditCredentials


@dataclass
class RedditPost:
    id: str
    title: str
    subreddit: str
    author: str
    permalink: str
    url: str
    created_utc: float
    media_url: Optional[str]
    raw: Dict


class RedditClient:
    TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
    API_BASE = "https://oauth.reddit.com"

    def __init__(
        self,
        creds: RedditCredentials,
        session: Optional[httpx.Client] = None,
    ) -> None:
        self.creds = creds
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._session = session or httpx.Client(timeout=20.0)

    def _authenticate(self) -> None:
        now = time.time()
        if self._token and now < self._token_expiry - 30:
            return
        auth = (self.creds.client_id, self.creds.client_secret)
        data = {
            "grant_type": "password",
            "username": self.creds.username,
            "password": self.creds.password,
        }
        headers = {"User-Agent": self.creds.user_agent}
        response = self._session.post(self.TOKEN_URL, data=data, auth=auth, headers=headers)
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        self._token_expiry = now + payload.get("expires_in", 3600)

    def _request(self, method: str, path: str, params: Optional[Dict] = None) -> Dict:
        self._authenticate()
        assert self._token
        headers = {"Authorization": f"bearer {self._token}", "User-Agent": self.creds.user_agent}
        url = f"{self.API_BASE}{path}"
        response = self._session.request(method, url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    def iter_posts(self, config: QueryConfig) -> Iterable[RedditPost]:
        if config.queries:
            for subreddit in config.subreddits or [None]:
                for query in config.queries:
                    yield from self._search(subreddit=subreddit, query=query, config=config)
        else:
            for subreddit in config.subreddits:
                yield from self._listing(subreddit=subreddit, config=config)

    def _search(self, subreddit: Optional[str], query: str, config: QueryConfig) -> Iterable[RedditPost]:
        if subreddit:
            path = f"/r/{subreddit}/search"
        else:
            path = "/search"
        params = {
            "q": query,
            "sort": config.sort,
            "t": config.time_filter,
            "limit": config.max_posts,
            "restrict_sr": bool(subreddit),
            "include_over_18": True,
        }
        payload = self._request("GET", path, params=params)
        yield from self._parse_listing(payload)

    def _listing(self, subreddit: str, config: QueryConfig) -> Iterable[RedditPost]:
        path = f"/r/{subreddit}/{config.sort}"
        params = {"limit": config.max_posts, "t": config.time_filter}
        payload = self._request("GET", path, params=params)
        yield from self._parse_listing(payload)

    def _parse_listing(self, payload: Dict) -> Iterable[RedditPost]:
        for child in payload.get("data", {}).get("children", []):
            data = child.get("data", {})
            media_url = self._extract_media_url(data)
            yield RedditPost(
                id=data.get("id", ""),
                title=data.get("title", ""),
                subreddit=data.get("subreddit", ""),
                author=data.get("author", ""),
                permalink=f"https://www.reddit.com{data.get('permalink', '')}",
                url=data.get("url_overridden_by_dest") or data.get("url", ""),
                created_utc=float(data.get("created_utc", 0.0)),
                media_url=media_url,
                raw=data,
            )

    @staticmethod
    def _extract_media_url(data: Dict) -> Optional[str]:
        if data.get("is_video") and data.get("media"):
            reddit_video = data["media"].get("reddit_video")
            if reddit_video and reddit_video.get("fallback_url"):
                return reddit_video["fallback_url"]
        preview = data.get("preview")
        if preview and "images" in preview and preview["images"]:
            return preview["images"][0].get("source", {}).get("url")
        url = data.get("url_overridden_by_dest")
        if url and any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".mp4", ".mov")):
            return url
        return None

    def close(self) -> None:
        self._session.close()


__all__ = ["RedditClient", "RedditPost"]
