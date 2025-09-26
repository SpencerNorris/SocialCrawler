from __future__ import annotations

import httpx

from social_crawler.config import QueryConfig, RedditCredentials
from social_crawler.reddit_client import RedditClient


TOKEN_PAYLOAD = {"access_token": "token", "expires_in": 3600}


def make_credentials() -> RedditCredentials:
    return RedditCredentials(
        client_id="id",
        client_secret="secret",
        username="user",
        password="pass",
        user_agent="social-crawler-tests",
    )


def test_iter_posts_search_uses_query_and_subreddit() -> None:
    listing_payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "abc123",
                        "title": "Test title",
                        "subreddit": "python",
                        "author": "tester",
                        "permalink": "/r/python/comments/abc123/test_title/",
                        "url": "https://example.com/post",
                        "created_utc": 1_650_000_000.0,
                        "preview": {
                            "images": [
                                {"source": {"url": "https://images.example.com/pic.png"}}
                            ]
                        },
                    }
                }
            ]
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.reddit.com":
            assert request.url.path == "/api/v1/access_token"
            return httpx.Response(200, json=TOKEN_PAYLOAD)
        assert request.url.host == "oauth.reddit.com"
        assert request.url.path == "/r/python/search"
        params = request.url.params
        assert params["q"] == "test"
        assert params.get("restrict_sr") in {"true", "True"}
        return httpx.Response(200, json=listing_payload)

    session = httpx.Client(transport=httpx.MockTransport(handler))
    client = RedditClient(make_credentials(), session=session)
    config = QueryConfig(queries=["test"], subreddits=["python"], max_posts=25)

    posts = list(client.iter_posts(config))

    assert len(posts) == 1
    post = posts[0]
    assert post.id == "abc123"
    assert post.media_url == "https://images.example.com/pic.png"
    client.close()


def test_iter_posts_listing_without_queries() -> None:
    listing_payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "abc",
                        "title": "First",
                        "subreddit": "learnpython",
                        "author": "user1",
                        "permalink": "/r/learnpython/comments/abc/first/",
                        "url": "https://example.com/first",
                        "created_utc": 1.0,
                        "media": {"reddit_video": {"fallback_url": "https://vid.example.com/1.mp4"}},
                        "is_video": True,
                    }
                },
                {
                    "data": {
                        "id": "def",
                        "title": "Second",
                        "subreddit": "learnpython",
                        "author": "user2",
                        "permalink": "/r/learnpython/comments/def/second/",
                        "url": "https://example.com/second",
                        "created_utc": 2.0,
                        "url_overridden_by_dest": "https://cdn.example.com/image.jpeg",
                    }
                },
            ]
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.reddit.com":
            return httpx.Response(200, json=TOKEN_PAYLOAD)
        assert request.url.path == "/r/learnpython/new"
        params = request.url.params
        assert params["limit"] == "2"
        return httpx.Response(200, json=listing_payload)

    session = httpx.Client(transport=httpx.MockTransport(handler))
    client = RedditClient(make_credentials(), session=session)
    config = QueryConfig(queries=[], subreddits=["learnpython"], sort="new", max_posts=2)

    posts = list(client.iter_posts(config))

    assert len(posts) == 2
    assert posts[0].media_url == "https://vid.example.com/1.mp4"
    assert posts[1].media_url == "https://cdn.example.com/image.jpeg"
    client.close()
