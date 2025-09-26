from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Any

try:
    from google.cloud import storage as gcs  # type: ignore
except ImportError:  # pragma: no cover
    gcs = None


class StorageBackend(ABC):
    @abstractmethod
    def save_json(self, path: str, data: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def save_bytes(self, path: str, payload: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    def exists(self, path: str) -> bool:
        raise NotImplementedError


class LocalStorage(StorageBackend):
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, path: str) -> Path:
        return self.root / path

    def save_json(self, path: str, data: dict) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def save_bytes(self, path: str, payload: bytes) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()


class GCSStorage(StorageBackend):
    def __init__(self, bucket_name: str, prefix: str = "social_crawler", client: Optional[Any] = None) -> None:
        if not bucket_name:
            raise ValueError("bucket_name is required for GCS storage")
        if gcs is None:  # pragma: no cover
            raise RuntimeError("google-cloud-storage is required for the GCS backend")
        self.client = client or gcs.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.prefix = prefix.rstrip("/")

    def _blob_path(self, path: str) -> str:
        if path.startswith("/"):
            path = path[1:]
        return f"{self.prefix}/{path}" if self.prefix else path

    def save_json(self, path: str, data: dict) -> None:
        blob = self.bucket.blob(self._blob_path(path))
        blob.upload_from_string(json.dumps(data), content_type="application/json")

    def save_bytes(self, path: str, payload: bytes) -> None:
        blob = self.bucket.blob(self._blob_path(path))
        blob.upload_from_string(payload)

    def exists(self, path: str) -> bool:
        blob = self.bucket.blob(self._blob_path(path))
        return blob.exists()


def build_storage_backend(backend: str, *, local_path: Path, gcs_bucket: Optional[str], gcs_prefix: str) -> StorageBackend:
    if backend == "local":
        return LocalStorage(local_path)
    if backend == "gcs":
        return GCSStorage(bucket_name=gcs_bucket or "", prefix=gcs_prefix)
    raise ValueError(f"Unsupported storage backend: {backend}")


__all__ = ["StorageBackend", "LocalStorage", "GCSStorage", "build_storage_backend"]
