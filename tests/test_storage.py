from __future__ import annotations

import json
from types import SimpleNamespace

from social_crawler.storage import GCSStorage, LocalStorage


def test_local_storage_round_trip(tmp_path) -> None:
    storage = LocalStorage(tmp_path)
    storage.save_json("foo/data.json", {"value": 1})
    storage.save_bytes("foo/data.bin", b"payload")

    assert storage.exists("foo/data.json")
    assert storage.exists("foo/data.bin")

    with (tmp_path / "foo" / "data.json").open("r", encoding="utf-8") as infile:
        payload = json.load(infile)

    assert payload["value"] == 1


def test_gcs_storage_uses_blob_operations(monkeypatch) -> None:
    uploads: dict[str, dict] = {}

    class DummyBlob:
        def __init__(self, path: str, store: dict[str, dict]) -> None:
            self.path = path
            self.store = store

        def upload_from_string(self, data, content_type: str | None = None) -> None:
            self.store[self.path] = {"data": data, "content_type": content_type}

        def exists(self) -> bool:
            return self.path in self.store

    class DummyBucket:
        def __init__(self, name: str, store: dict[str, dict]) -> None:
            self.name = name
            self.store = store

        def blob(self, path: str) -> DummyBlob:
            return DummyBlob(path, self.store)

    class DummyClient:
        def __init__(self) -> None:
            self.last_bucket: str | None = None

        def bucket(self, name: str) -> DummyBucket:
            self.last_bucket = name
            return DummyBucket(name, uploads)

    # Ensure module-level gcs symbol is not None and provides a Client attr
    monkeypatch.setattr("social_crawler.storage.gcs", SimpleNamespace(Client=lambda: DummyClient()), raising=False)

    backend = GCSStorage("test-bucket", prefix="prefix", client=DummyClient())

    backend.save_json("foo.json", {"value": 2})
    backend.save_bytes("/bar.bin", b"bytes")

    assert uploads["prefix/foo.json"]["content_type"] == "application/json"
    assert json.loads(uploads["prefix/foo.json"]["data"]) == {"value": 2}
    assert uploads["prefix/bar.bin"]["data"] == b"bytes"
    assert backend.exists("bar.bin") is True
