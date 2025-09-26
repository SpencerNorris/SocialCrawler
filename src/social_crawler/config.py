from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, validator

try:  # Pydantic v2+
    from pydantic_settings import BaseSettings  # type: ignore
except ImportError:  # pragma: no cover - for backward compatibility
    from pydantic import BaseSettings  # type: ignore


class RedditCredentials(BaseSettings):
    client_id: str = Field(..., env="REDDIT_CLIENT_ID")
    client_secret: str = Field(..., env="REDDIT_CLIENT_SECRET")
    username: str = Field(..., env="REDDIT_USERNAME")
    password: str = Field(..., env="REDDIT_PASSWORD")
    user_agent: str = Field(..., env="REDDIT_USER_AGENT")


class QueryConfig(BaseModel):
    queries: List[str] = Field(default_factory=list)
    subreddits: List[str] = Field(default_factory=list)
    time_filter: str = Field("all")  # hour, day, week, month, year, all
    sort: str = Field("new")  # relevance, hot, top, new, comments
    max_posts: int = Field(50, ge=1)
    media_only: bool = Field(False)
    download_media: bool = Field(False)

    @validator("sort")
    def validate_sort(cls, value: str) -> str:
        allowed = {"relevance", "hot", "top", "new", "comments"}
        if value not in allowed:
            raise ValueError(f"sort must be one of {allowed}")
        return value

    @validator("time_filter")
    def validate_time_filter(cls, value: str) -> str:
        allowed = {"all", "year", "month", "week", "day", "hour"}
        if value not in allowed:
            raise ValueError(f"time_filter must be one of {allowed}")
        return value


class StorageConfig(BaseModel):
    backend: str = Field("local")  # local or gcs
    local_path: Path = Field(Path("cache"))
    gcs_bucket: Optional[str] = None
    gcs_prefix: str = Field("social_crawler")


class LedgerConfig(BaseModel):
    mode: str = Field("csv")  # csv or sqlite
    csv_path: Path = Field(Path("ledger.csv"))
    sqlite_path: Path = Field(Path("ledger.db"))


class ScraperConfig(BaseModel):
    queries: QueryConfig = Field(default_factory=QueryConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    ledger: LedgerConfig = Field(default_factory=LedgerConfig)

    def ensure_paths(self) -> None:
        if self.storage.backend == "local":
            self.storage.local_path.mkdir(parents=True, exist_ok=True)
        if self.ledger.mode == "csv":
            self.ledger.csv_path.parent.mkdir(parents=True, exist_ok=True)
        if self.ledger.mode == "sqlite":
            self.ledger.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
