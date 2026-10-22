from dataclasses import replace
from io import BytesIO

import pytest
from test_wal_batching import _dataset, _entity_delta, _record, _tenant_ids

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.core.errors import RemoteWalConflictError, S3ExpressWriteError
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.wal.batching import build_batch


class ClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class MemoryClient:
    def __init__(self):
        self.objects = {}
        self.fail = False
        self.delete_hook = lambda: None
        self.page_size = 1

    def put_object(self, **kwargs):
        if self.fail:
            raise TimeoutError("lost response")
        key = kwargs["Key"]
        if key in self.objects:
            raise ClientError("PreconditionFailed")
        self.objects[key] = kwargs["Body"]
        return {"ETag": "test"}

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        if key not in self.objects:
            raise ClientError("NoSuchKey")
        return {"Body": BytesIO(self.objects[key])}

    def delete_object(self, **kwargs):
        self.delete_hook()
        self.objects.pop(kwargs["Key"], None)
        return {}

    def list_objects_v2(self, **kwargs):
        keys = sorted(
            (key for key in self.objects if key.startswith(kwargs["Prefix"])), reverse=True
        )
        offset = int(kwargs.get("ContinuationToken", "0"))
        next_offset = offset + self.page_size
        return {
            "Contents": [{"Key": key} for key in keys[offset:next_offset]],
            "IsTruncated": next_offset < len(keys),
            "NextContinuationToken": str(next_offset),
        }


def test_sdk_call_accounting_includes_failed_put_and_existing_verification():
    client = MemoryClient()
    store = S3ExpressWalStore(S3ExpressWalConfig("test"), client=client)
    store.request_phase = "workload"
    record = _record("a", 1)
    store.persist_record(record)
    assert store.persist_record(record).already_exists
    client.fail = True
    with pytest.raises(S3ExpressWriteError):
        store.persist_record(_record("b", 1))
    client.fail = False
    store.request_phase = "cleanup"
    store.delete_record("a", 1)
    accounting = store.request_accounting()
    assert accounting["sdk_calls"] == {
        "workload.put_object": 3,
        "workload.put_object.failed": 2,
        "workload.get_object": 1,
        "cleanup.delete_object": 1,
    }
    assert accounting["wire_attempts"] is None and accounting["billable_usage"] is None


def test_paginated_mixed_history_is_sorted_and_batch_content_filtered():
    store = S3ExpressWalStore(S3ExpressWalConfig("test"), client=MemoryClient())
    first = _record("a", 1)
    second = _record("a", 2, previous_record_checksum=first.record_checksum)
    store.persist_record(first)
    store.persist_batch(build_batch([second, _record("b", 1)]))
    store.persist_batch(build_batch([_record("c", 1)]))
    history = store.inspect_history("a")
    assert [item.record for item in history] == [first, second]
    assert store.request_accounting()["sdk_calls"]["setup.list_objects_v2"] == 3


def test_conflicting_batch_key_rejected():
    from ephemeraldb.wal.batching.codec import build_batch as make_batch

    store = S3ExpressWalStore(S3ExpressWalConfig("test"), client=MemoryClient())
    batch = build_batch([_record("a", 1)])
    store.persist_batch(batch)
    assert store.persist_batch(batch).already_exists
    conflict = make_batch([_record("b", 1)], batch_id=batch.batch_id, created_at=batch.created_at)
    with pytest.raises(RemoteWalConflictError):
        store.persist_batch(conflict)


def test_remote_bootstrap_is_explicit_and_restart_verifies_history(tmp_path):
    store = S3ExpressWalStore(S3ExpressWalConfig("test"), client=MemoryClient())
    dataset = _dataset(tmp_path, tenant_count=1)
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    try:
        assert engine.execute(operation).reconciliation_required
    finally:
        engine.close_all()
    engine = TenantDatabaseEngine.from_dataset_dir(
        dataset, wal_store=store, initialize_durability=True
    )
    assert engine.execute(operation).success
    engine.close_all()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    try:
        assert engine.execute(operation).duplicate_transaction
    finally:
        engine.close_all()
    wrong = S3ExpressWalStore(replace(store.config, prefix="other"), client=store._client)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=wrong)
    try:
        assert engine.execute(operation).reconciliation_required
    finally:
        engine.close_all()
