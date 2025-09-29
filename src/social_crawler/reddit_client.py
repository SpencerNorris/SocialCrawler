from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, Iterable, Iterator, Optional, TYPE_CHECKING

try:  # pragma: no cover - import guard for environments without praw
    import praw  # type: ignore
except ImportError:  # pragma: no cover
    praw = None  # type: ignore

if TYPE_CHECKING:  # pragma: no cover
    from praw import Reddit as PrawReddit  # type: ignore
else:  # runtime fallback used when praw is missing during type checking
    PrawReddit = Any  # type: ignore

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
    raw: Dict[str, Any]


class RedditClient:
    def __init__(
        self,
        creds: RedditCredentials,
        reddit: Optional[PrawReddit] = None,
    ) -> None:
        self.creds = creds
        self._reddit = reddit or self._build_reddit(creds)

    def _build_reddit(self, creds: RedditCredentials) -> PrawReddit:
        if praw is None:
            raise RuntimeError(
                "praw is required but not installed. Install it via 'pip install praw' to use the scraper."
            )
        kwargs: Dict[str, Any] = {
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "user_agent": creds.user_agent,
            "check_for_async": False,
        }
        if creds.username and creds.password:
            kwargs.update(username=creds.username, password=creds.password)
        reddit = praw.Reddit(**kwargs)  # type: ignore[call-arg]
        if not (creds.username and creds.password):
            reddit.read_only = True
        return reddit

    def iter_posts(self, config: QueryConfig) -> Iterable[RedditPost]:
        if config.queries:
            subreddits = config.subreddits or [None]
            for subreddit in subreddits:
                for query in config.queries:
                    yield from self._search(subreddit, query, config)
        else:
            for subreddit in config.subreddits:
                yield from self._listing(subreddit, config)

    def _search(self, subreddit: Optional[str], query: str, config: QueryConfig) -> Iterator[RedditPost]:
        target = self._reddit.subreddit(subreddit or "all")
        results = target.search(
            query,
            sort=config.sort,
            time_filter=config.time_filter,
            limit=config.max_posts,
        )
        for submission in results:
            yield self._to_post(submission)

    def _listing(self, subreddit: str, config: QueryConfig) -> Iterator[RedditPost]:
        target = self._reddit.subreddit(subreddit)
        limit = config.max_posts
        sort = config.sort
        if sort == "new":
            listings = target.new(limit=limit)
        elif sort == "top":
            listings = target.top(time_filter=config.time_filter, limit=limit)
        elif sort in {"hot", "relevance"}:
            listings = target.hot(limit=limit)
        elif sort == "comments":
            listings = target.top(time_filter=config.time_filter, limit=limit)
        else:
            listings = target.hot(limit=limit)
        for submission in listings:
            yield self._to_post(submission)

    def _to_post(self, submission: Any) -> RedditPost:
        subreddit_name = getattr(getattr(submission, "subreddit", None), "display_name", "")
        author = getattr(submission, "author", None)
        author_name = str(author) if author else "[deleted]"
        media_url = self._extract_media_url(submission)
        raw_payload = self._serialize_submission(submission)
        return RedditPost(
            id=getattr(submission, "id", ""),
            title=getattr(submission, "title", ""),
            subreddit=subreddit_name,
            author=author_name,
            permalink=f"https://www.reddit.com{getattr(submission, 'permalink', '')}",
            url=getattr(submission, "url", ""),
            created_utc=float(getattr(submission, "created_utc", 0.0) or 0.0),
            media_url=media_url,
            raw=raw_payload,
        )

    @staticmethod
    def _serialize_submission(submission: Any) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        fields = [
            "id",
            "title",
            "selftext",
            "subreddit_id",
            "url",
            "permalink",
            "created_utc",
            "over_18",
            "ups",
            "downs",
            "score",
            "num_comments",
            "author_fullname",
        ]
        for field in fields:
            if hasattr(submission, field):
                snapshot[field] = getattr(submission, field)
        author = getattr(submission, "author", None)
        snapshot["author"] = str(author) if author else None
        snapshot["subreddit"] = getattr(getattr(submission, "subreddit", None), "display_name", None)
        return snapshot

    @staticmethod
    def _extract_media_url(submission: Any) -> Optional[str]:
        if getattr(submission, "is_video", False) and getattr(submission, "media", None):
            media = submission.media or {}
            reddit_video = media.get("reddit_video") if isinstance(media, dict) else None
            if reddit_video and reddit_video.get("fallback_url"):
                return reddit_video["fallback_url"]
        preview = getattr(submission, "preview", None)
        if isinstance(preview, dict):
            images = preview.get("images") or []
            if images:
                source = images[0].get("source") or {}
                if source.get("url"):
                    return source["url"]
        url = getattr(submission, "url", "")
        if url and any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".mp4", ".mov")):
            return url
        return None

    def close(self) -> None:
        # PRAW does not expose an explicit close, but we keep the method for API parity.
        pass


__all__ = ["RedditClient", "RedditPost"]
