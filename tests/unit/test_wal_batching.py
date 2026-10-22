import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from ephemeraldb.core.errors import S3ExpressWriteError, WalChecksumError
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.executor import WalCommitFaultPoint
from ephemeraldb.engine.results import EngineErrorCode
from ephemeraldb.loadgen.models import (
    BenchmarkPhase,
    EntityDeltaPayload,
    OperationType,
    WorkloadOperation,
)
from ephemeraldb.schema.generator import deterministic_uuid, generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.wal.batching import (
    BatchedWalCommitter,
    WalBatch,
    WalBatchingConfig,
    build_batch,
    encode_batch,
    validate_batch,
)
from ephemeraldb.wal.batching.codec import build_record_index
from ephemeraldb.wal.checksum import attach_record_checksum
from ephemeraldb.wal.codec import encode_record
from ephemeraldb.wal.durability import PersistedBatchReceipt
from ephemeraldb.wal.models import WalMutation, WalMutationType, WalRecord, WalScalar


def test_records_within_one_window_form_one_batch() -> None:
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(batch_window_ms=50, max_records_per_batch=3),
    )
    records = (_record("tenant-a", 1), _record("tenant-b", 1), _record("tenant-c", 1))

    try:
        receipts = _commit_concurrently(committer, records)
    finally:
        committer.close()

    assert len(store.batches) == 1
    assert store.batches[0].records == records
    assert {receipt.wal_batch_id for receipt in receipts} == {store.batches[0].batch_id}
    assert all(receipt.wal_batch_record_count == 3 for receipt in receipts)


def test_timeout_flushes_single_record_without_long_sleep() -> None:
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(batch_window_ms=0, max_records_per_batch=100),
    )

    try:
        receipt = committer.commit(_record("tenant-a", 1))
    finally:
        committer.close()

    assert len(store.batches) == 1
    assert receipt.wal_batch_flush_reason == "time"


def test_count_flushes_immediately() -> None:
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(batch_window_ms=10_000, max_records_per_batch=3),
    )
    records = (_record("tenant-a", 1), _record("tenant-b", 1), _record("tenant-c", 1))

    try:
        receipts = _commit_concurrently(committer, records)
    finally:
        committer.close()

    assert len(store.batches) == 1
    assert {receipt.wal_batch_flush_reason for receipt in receipts} == {"count"}


def test_byte_flush_keeps_crossing_record_for_next_batch() -> None:
    first = _record("tenant-a", 1)
    second = _record("tenant-b", 1)
    max_batch_bytes = len(encode_record(first)) + 1
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(
            batch_window_ms=10_000,
            max_records_per_batch=100,
            max_batch_bytes=max_batch_bytes,
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(committer.commit, first)
        second_future = pool.submit(committer.commit, second)
        first_receipt = first_future.result(timeout=2)
        committer.flush()
        second_receipt = second_future.result(timeout=2)
    committer.close()

    assert [batch.records for batch in store.batches] == [(first,), (second,)]
    assert first_receipt.wal_batch_flush_reason == "bytes"
    assert second_receipt.wal_batch_flush_reason == "manual"


def test_cross_tenant_batch_preserves_records_order_and_checksums() -> None:
    a1 = _record("tenant-a", 1)
    b1 = _record("tenant-b", 1)
    a2 = _record("tenant-a", 2, previous_record_checksum=a1.record_checksum)
    c1 = _record("tenant-c", 1)

    batch = build_batch((a1, b1, a2, c1), batch_id="batch-cross", created_at=_BATCH_TIME)

    assert [item.tenant_id for item in batch.record_index] == [
        "tenant-a",
        "tenant-b",
        "tenant-a",
        "tenant-c",
    ]
    assert [item.sequence_no for item in batch.record_index] == [1, 1, 2, 1]
    assert batch.records[0].record_checksum == a1.record_checksum
    assert batch.records[2].previous_record_checksum == a1.record_checksum


def test_batch_checksum_detects_content_and_order_changes() -> None:
    a1 = _record("tenant-a", 1)
    b1 = _record("tenant-b", 1)
    batch = build_batch((a1, b1), batch_id="batch-checksum", created_at=_BATCH_TIME)
    reversed_records = tuple(reversed(batch.records))
    tampered = replace(
        batch,
        records=reversed_records,
        record_index=build_record_index(reversed_records),
    )
    changed_sequence = (
        _record("tenant-a", 3),
        b1,
    )
    changed = build_batch(
        changed_sequence,
        batch_id=batch.batch_id,
        created_at=batch.created_at,
    )

    with pytest.raises(WalChecksumError):
        validate_batch(tampered)
    assert changed.batch_checksum != batch.batch_checksum


def test_s3_store_persists_batch_body_metadata_and_roundtrip() -> None:
    from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore

    client = CapturingS3Client()
    store = S3ExpressWalStore(
        S3ExpressWalConfig(bucket="bench--aps1-az1--x-s3"),
        client=client,
    )
    batch = build_batch(
        (_record("tenant-a", 1), _record("tenant-b", 1)),
        batch_id="batch-roundtrip",
        created_at=_BATCH_TIME,
    )

    receipt = store.persist_batch(batch)
    fetched = store.fetch_batch(receipt.key)

    put = client.puts[0]
    assert receipt.remote_durable
    assert receipt.key == "wal/wal-batches/2026-01-01/batch-roundtrip.json"
    assert put["Body"] == encode_batch(batch)
    assert put["IfNoneMatch"] == "*"
    assert put["Metadata"]["batch-checksum"] == batch.batch_checksum
    assert fetched == batch


def test_batch_put_failure_fails_all_waiters_and_skips_local_apply(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, tenant_count=2)
    store = RecordingBatchStore(fail=True)
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(batch_window_ms=100, max_records_per_batch=2),
    )
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=committer)
    tenant_ids = _tenant_ids(dataset)
    before = {
        tenant_id: _entity_value_and_version(
            dataset,
            tenant_id,
            _first_entity_id(dataset, tenant_id),
        )
        for tenant_id in tenant_ids
    }

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda tenant_id: engine.execute(_entity_delta(dataset, tenant_id)),
                    tenant_ids,
                )
            )
    finally:
        engine.close_all()

    assert all(not result.success for result in results)
    assert {result.error_code for result in results} == {
        EngineErrorCode.REMOTE_DURABILITY_FAILED
    }
    assert all(not result.remote_durable for result in results)
    assert _transaction_count(dataset) == 0
    assert {
        tenant_id: _entity_value_and_version(
            dataset,
            tenant_id,
            _first_entity_id(dataset, tenant_id),
        )
        for tenant_id in tenant_ids
    } == before


def test_successful_batch_allows_each_waiter_to_apply_locally(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, tenant_count=3)
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(batch_window_ms=100, max_records_per_batch=3),
    )
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=committer)
    tenant_ids = _tenant_ids(dataset)

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(
                pool.map(
                    lambda tenant_id: engine.execute(_entity_delta(dataset, tenant_id)),
                    tenant_ids,
                )
            )
    finally:
        engine.close_all()

    assert all(result.success for result in results)
    assert all(result.remote_durable for result in results)
    assert len(store.batches) == 1
    assert {result.wal_batch_record_count for result in results} == {3}
    assert _transaction_count(dataset) == 3


def test_one_local_apply_failure_after_durable_batch_does_not_fail_other_tenants(
    tmp_path: Path,
) -> None:
    dataset = _dataset(tmp_path, tenant_count=3)
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(batch_window_ms=100, max_records_per_batch=3),
    )
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=committer)
    tenant_ids = _tenant_ids(dataset)
    failing_tenant = tenant_ids[1]
    barrier = threading.Barrier(3)

    def run(tenant_id: str):
        def prepared_barrier(
            point: WalCommitFaultPoint,
            _connection: sqlite3.Connection,
        ) -> None:
            if point != WalCommitFaultPoint.AFTER_PREPARE:
                return
            barrier.wait(timeout=2)

        def conflict(connection: sqlite3.Connection) -> None:
            if tenant_id != failing_tenant:
                return
            connection.execute(
                """
                INSERT INTO transactions (
                    transaction_id,
                    tenant_id,
                    status,
                    created_at,
                    completed_at
                )
                VALUES (?, ?, 'PENDING', '2026-01-01T00:00:00Z', NULL)
                """,
                (_tx_id(tenant_id), tenant_id),
            )

        return engine.execute(
            _entity_delta(dataset, tenant_id),
            before_transaction_insert_hook=conflict,
            wal_fault_hook=prepared_barrier,
        )

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(run, tenant_ids))
    finally:
        engine.close_all()

    by_tenant = {result.tenant_id: result for result in results}
    assert by_tenant[tenant_ids[0]].success
    assert not by_tenant[failing_tenant].success
    assert by_tenant[failing_tenant].error_code == (
        EngineErrorCode.LOCAL_APPLY_AFTER_DURABILITY_FAILED
    )
    assert by_tenant[failing_tenant].remote_durable
    assert by_tenant[failing_tenant].reconciliation_required
    assert by_tenant[tenant_ids[2]].success
    assert len(store.batches) == 1


def test_shutdown_flushes_waiting_records() -> None:
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(batch_window_ms=10_000, max_records_per_batch=100),
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(committer.commit, _record("tenant-a", 1))
        _wait_for_queue_depth(committer, expected=1)
        committer.close()
        receipt = future.result(timeout=2)

    assert len(store.batches) == 1
    assert receipt.wal_batch_flush_reason == "shutdown"


def test_queue_backpressure_blocks_producer_until_capacity_returns() -> None:
    store = RecordingBatchStore(block=True)
    committer = BatchedWalCommitter(
        store,
        config=WalBatchingConfig(
            batch_window_ms=10_000,
            max_records_per_batch=1,
            max_queue_records=1,
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(committer.commit, _record("tenant-a", 1))
        assert store.started.wait(timeout=2)
        second = pool.submit(committer.commit, _record("tenant-b", 1))
        time.sleep(0.05)
        assert not second.done()
        store.release.set()
        assert first.result(timeout=2).remote_durable
        assert second.result(timeout=2).remote_durable
    committer.close()


class RecordingBatchStore:
    def __init__(self, *, fail: bool = False, block: bool = False) -> None:
        self.fail = fail
        self.block = block
        self.batches: list[WalBatch] = []
        self.started = threading.Event()
        self.release = threading.Event()
        self._lock = threading.Lock()

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        self.started.set()
        if self.block:
            assert self.release.wait(timeout=2)
        if self.fail:
            msg = "injected batch PUT failure"
            raise S3ExpressWriteError(msg)
        body = encode_batch(batch)
        with self._lock:
            self.batches.append(batch)
        assert batch.batch_checksum is not None
        return PersistedBatchReceipt(
            bucket="fake-s3-express",
            key=f"wal/wal-batches/{batch.created_at[:10]}/{batch.batch_id}.json",
            etag="batch-etag",
            version_id=None,
            batch_id=batch.batch_id,
            batch_checksum=batch.batch_checksum,
            record_count=batch.record_count,
            bytes_written=len(body),
            duration_ns=4321,
            remote_durable=True,
        )

    def fetch_batch(self, key: str) -> WalBatch:
        for batch in self.batches:
            if key.endswith(f"{batch.batch_id}.json"):
                return batch
        raise KeyError(key)


class CapturingS3Client:
    def __init__(self) -> None:
        self.puts: list[dict[str, object]] = []
        self.objects: dict[str, bytes] = {}

    def put_object(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Key"])
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        self.puts.append(kwargs)
        self.objects[key] = body
        return {"ETag": '"etag-1"', "VersionId": "version-1"}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Key"])
        return {"Body": _Body(self.objects[key])}


class _Body:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body


_BATCH_TIME = "2026-01-01T00:00:00Z"


def _commit_concurrently(
    committer: BatchedWalCommitter,
    records: tuple[WalRecord, ...],
):
    with ThreadPoolExecutor(max_workers=len(records)) as pool:
        return list(pool.map(committer.commit, records))


def _record(
    tenant_id: str,
    sequence_no: int,
    *,
    previous_record_checksum: str | None = None,
) -> WalRecord:
    transaction_id = f"{tenant_id}-tx-{sequence_no}"
    values: dict[str, WalScalar] = {
        "transaction_id": transaction_id,
        "tenant_id": tenant_id,
        "status": "COMMITTED",
        "created_at": _BATCH_TIME,
        "completed_at": _BATCH_TIME,
    }
    mutation = WalMutation.from_mappings(
        WalMutationType.INSERT_TRANSACTION,
        "transactions",
        key={"tenant_id": tenant_id, "transaction_id": transaction_id},
        values=values,
    )
    return attach_record_checksum(
        WalRecord(
            wal_format_version=1,
            tenant_id=tenant_id,
            transaction_id=transaction_id,
            sequence_no=sequence_no,
            operation_type=OperationType.ENTITY_DELTA,
            operations=(mutation,),
            created_at=_BATCH_TIME,
            previous_record_checksum=previous_record_checksum,
        )
    )


def _dataset(tmp_path: Path, *, tenant_count: int) -> Path:
    return generate_dataset(
        tmp_path / f"dataset-{tenant_count}",
        DatasetConfig(
            tenant_count=tenant_count,
            entities_per_tenant=5,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
            seed=42,
        ),
    ).output_dir


def _entity_delta(dataset_dir: Path, tenant_id: str) -> WorkloadOperation:
    entity_id = _first_entity_id(dataset_dir, tenant_id)
    return WorkloadOperation(
        operation_id=deterministic_uuid("phase6-test-operation", tenant_id),
        sequence_no=0,
        tenant_id=tenant_id,
        operation_type=OperationType.ENTITY_DELTA,
        scheduled_offset_ms=0,
        benchmark_phase=BenchmarkPhase.MEASUREMENT,
        virtual_client_id="client-0001",
        payload=EntityDeltaPayload(_tx_id(tenant_id), entity_id, 1),
    )


def _tx_id(tenant_id: str) -> str:
    return f"tx-{tenant_id}"


def _tenant_ids(dataset_dir: Path) -> tuple[str, ...]:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    return tuple(tenant["tenant_id"] for tenant in manifest["tenant_databases"])


def _tenant_db_path(dataset_dir: Path, tenant_id: str) -> Path:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    for tenant in manifest["tenant_databases"]:
        if tenant["tenant_id"] == tenant_id:
            return dataset_dir / tenant["filename"]
    raise AssertionError(f"tenant not found: {tenant_id}")


def _first_entity_id(dataset_dir: Path, tenant_id: str) -> str:
    connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
    try:
        row = connection.execute(
            "SELECT entity_id FROM entities WHERE tenant_id = ? ORDER BY entity_id LIMIT 1",
            (tenant_id,),
        ).fetchone()
        return str(row[0])
    finally:
        connection.close()


def _entity_value_and_version(
    dataset_dir: Path,
    tenant_id: str,
    entity_id: str,
) -> tuple[int, int]:
    connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
    try:
        row = connection.execute(
            "SELECT numeric_value, version FROM entities WHERE tenant_id = ? AND entity_id = ?",
            (tenant_id, entity_id),
        ).fetchone()
        return int(row[0]), int(row[1])
    finally:
        connection.close()


def _transaction_count(dataset_dir: Path) -> int:
    total = 0
    for tenant_id in _tenant_ids(dataset_dir):
        connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
        try:
            total += int(connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0])
        finally:
            connection.close()
    return total


def _wait_for_queue_depth(committer: BatchedWalCommitter, *, expected: int) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if committer.snapshot_metrics()["queue_depth"] == expected:
            return
        time.sleep(0.001)
    raise AssertionError(f"queue depth did not reach {expected}")
