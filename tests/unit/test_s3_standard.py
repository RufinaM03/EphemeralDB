from dataclasses import replace

import pytest
from test_s3_stabilization import ClientError, MemoryClient

from ephemeraldb.aws.s3_standard import S3StandardStore
from ephemeraldb.checkpoint.models import CheckpointError
from ephemeraldb.checkpoint.store import get_bytes, put_bytes
from ephemeraldb.config.settings import load_settings


class StreamingClient(MemoryClient):
    def __init__(self):
        super().__init__()
        self.lose_response = False
        self.denied = False
        self.storage_classes = []

    def put_object(self, **kwargs):
        if self.denied:
            raise ClientError("AccessDenied")
        self.storage_classes.append(kwargs["StorageClass"])
        kwargs["Body"] = kwargs["Body"].read()
        result = super().put_object(**kwargs)
        if self.lose_response:
            self.lose_response = False
            raise TimeoutError("stored then response lost")
        return result


def store_and_client():
    client = StreamingClient()
    store = S3StandardStore(
        replace(load_settings({}), s3_checkpoint_bucket="standard-bucket"), client=client
    )
    return store, client


def test_standard_streaming_idempotency_and_lost_response():
    store, client = store_and_client()
    for key in ("run/snapshot.db", "run/manifest.json"):
        client.lose_response = True
        put_bytes(store, key, b"content")
        put_bytes(store, key, b"content")
        assert get_bytes(store, key) == b"content"
        with pytest.raises(CheckpointError, match="conflicting"):
            put_bytes(store, key, b"different")
    assert set(client.storage_classes) == {"STANDARD"}
    assert store.accounting()["wire_attempts"] is None


def test_paginated_out_of_order_listing_and_authorization_failure():
    store, client = store_and_client()
    for key in ("run/c", "run/a", "run/b", "unrelated/key"):
        put_bytes(store, key, b"data")
    assert set(store.list_keys("run/")) == {"run/a", "run/b", "run/c"}
    assert store.calls["setup.LIST"] == 3
    gets = store.calls["setup.GET"]
    client.denied = True
    with pytest.raises(ClientError, match="AccessDenied"):
        put_bytes(store, "run/new", b"data")
    assert store.calls["setup.GET"] == gets


def test_standard_bucket_must_be_separate_general_purpose():
    with pytest.raises(CheckpointError, match="general-purpose"):
        S3StandardStore(
            replace(load_settings({}), s3_checkpoint_bucket="test--x-s3"), client=object()
        )


def test_path_escape_and_nonadvancing_pagination_rejected(monkeypatch):
    store, client = store_and_client()
    with pytest.raises(CheckpointError, match="unsafe"):
        put_bytes(store, "../outside", b"data")
    monkeypatch.setattr(
        client,
        "list_objects_v2",
        lambda **kw: {"IsTruncated": True, "NextContinuationToken": "same", "Contents": []},
    )
    with pytest.raises(CheckpointError, match="advance"):
        store.list_keys("run/")
