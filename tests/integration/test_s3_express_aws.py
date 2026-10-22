import json
import os
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.config.settings import load_settings
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.loadgen.models import (
    BenchmarkPhase,
    EntityDeltaPayload,
    OperationType,
    WorkloadOperation,
)
from ephemeraldb.schema.generator import deterministic_uuid, generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatch, WalBatchingConfig, build_batch
from ephemeraldb.wal.batching.codec import decode_batch, validate_batch
from ephemeraldb.wal.codec import encode_record
from ephemeraldb.wal.durability import PersistedBatchReceipt, PersistedWalReceipt
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.validation import validate_record

pytestmark = pytest.mark.aws


@pytest.fixture()
def s3_store() -> S3ExpressWalStore:
    if os.environ.get("RUN_AWS_TESTS") != "1":
        pytest.skip("set RUN_AWS_TESTS=1 to run real S3 Express tests")
    settings = load_settings()
    if not settings.s3_express_bucket:
        pytest.skip("S3_EXPRESS_BUCKET is required for real S3 Express tests")
    run_prefix = f"{settings.s3_express_prefix}/integration-tests/{uuid.uuid4().hex}"
    config = S3ExpressWalConfig(
        bucket=settings.s3_express_bucket,
        region=settings.aws_region,
        prefix=run_prefix,
        zone_id=settings.s3_express_zone_id,
        profile=settings.aws_profile,
        connect_timeout_seconds=settings.aws_connect_timeout_seconds,
        read_timeout_seconds=settings.aws_read_timeout_seconds,
        max_attempts=settings.aws_max_attempts,
        retry_mode=settings.aws_retry_mode,
    )
    print(
        {
            "bucket": config.bucket,
            "region": config.region,
            "prefix": config.prefix,
            "expected_puts": 1,
            "expected_gets": 1,
            "expected_deletes": 1,
        }
    )
    return S3ExpressWalStore(config)


def test_s3_express_put_get_exact_wal_roundtrip(
    tmp_path: Path,
    s3_store: S3ExpressWalStore,
) -> None:
    record = _engine_record(tmp_path)
    receipt = s3_store.persist_record(record)
    try:
        fetched_bytes = s3_store.fetch_record_bytes(record.tenant_id, record.sequence_no)
        fetched_record = s3_store.fetch_record(record.tenant_id, record.sequence_no)
    finally:
        s3_store.delete_record(record.tenant_id, record.sequence_no)

    assert receipt.remote_durable
    assert fetched_bytes == encode_record(record)
    validate_record(fetched_record, expected_tenant_id=record.tenant_id)
    assert fetched_record.record_checksum == record.record_checksum


def test_s3_express_engine_write_reports_remote_durability(
    tmp_path: Path,
    s3_store: S3ExpressWalStore,
) -> None:
    dataset = _dataset(tmp_path / "source")
    engine = TenantDatabaseEngine.from_dataset_dir(
        dataset, wal_store=s3_store, initialize_durability=True
    )
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_id(dataset, tenant_id)
    operation = _operation(tenant_id, EntityDeltaPayload("tx-real-s3", entity_id, 1))

    result = engine.execute(operation)
    try:
        assert result.success
        assert result.remote_durable
        assert result.wal_sequence_no == 1
        assert result.wal_object_key is not None
        assert result.wal_record_checksum is not None
        fetched = s3_store.fetch_record(tenant_id, 1)
        assert fetched.record_checksum == result.wal_record_checksum
    finally:
        if result.wal_sequence_no is not None:
            s3_store.delete_record(tenant_id, result.wal_sequence_no)
        engine.close_all()


def test_s3_express_batch_roundtrip(
    tmp_path: Path,
    s3_store: S3ExpressWalStore,
) -> None:
    records = _engine_records(tmp_path / "batch-records", tenant_count=4)
    batch = build_batch(records[:4])
    receipt = s3_store.persist_batch(batch)
    try:
        fetched_bytes = s3_store.fetch_batch_bytes(receipt.key)
        fetched_batch = decode_batch(fetched_bytes)
    finally:
        s3_store.delete_batch(receipt.key)

    assert receipt.remote_durable
    validate_batch(fetched_batch)
    assert fetched_batch.batch_checksum == batch.batch_checksum
    assert [item.record_position for item in fetched_batch.record_index] == [0, 1, 2, 3]
    assert [record.record_checksum for record in fetched_batch.records] == [
        record.record_checksum for record in records[:4]
    ]


def test_s3_express_batched_commits_wait_for_batch_put(
    tmp_path: Path,
    s3_store: S3ExpressWalStore,
) -> None:
    dataset = _dataset(tmp_path / "waiting", tenant_count=2)
    blocking_store = _BlockingBatchStore(s3_store)
    committer = BatchedWalCommitter(
        blocking_store,
        config=WalBatchingConfig(batch_window_ms=100, max_records_per_batch=2),
    )
    engine = TenantDatabaseEngine.from_dataset_dir(
        dataset, wal_committer=committer, initialize_durability=True
    )
    tenant_ids = _tenant_ids(dataset)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    engine.execute,
                    _operation(
                        tenant_id,
                        EntityDeltaPayload(
                            f"tx-wait-{index}",
                            _first_entity_id(dataset, tenant_id),
                            1,
                        ),
                    ),
                )
                for index, tenant_id in enumerate(tenant_ids)
            ]
            assert blocking_store.started.wait(timeout=10)
            assert all(not future.done() for future in futures)
            blocking_store.release.set()
            results = [future.result(timeout=30) for future in futures]
        assert all(result.success for result in results)
        assert all(result.remote_durable for result in results)
        assert len({result.wal_batch_id for result in results}) == 1
    finally:
        engine.close_all()
        for key in blocking_store.keys:
            s3_store.delete_batch(key)


def test_s3_express_batch_reduces_put_count_for_concurrent_tenants(
    tmp_path: Path,
    s3_store: S3ExpressWalStore,
) -> None:
    tenant_count = 10
    single_dataset = _dataset(tmp_path / "single", tenant_count=tenant_count)
    batch_dataset = _dataset(tmp_path / "batch", tenant_count=tenant_count)
    single_store = _CountingStore(
        S3ExpressWalStore(replace(s3_store.config, prefix=f"{s3_store.config.prefix}/single"))
    )
    batch_store = _CountingStore(
        S3ExpressWalStore(replace(s3_store.config, prefix=f"{s3_store.config.prefix}/batch"))
    )
    single_engine = TenantDatabaseEngine.from_dataset_dir(
        single_dataset,
        wal_store=single_store,
        initialize_durability=True,
        max_open_connections=tenant_count,
    )
    batch_committer = BatchedWalCommitter(
        batch_store,
        config=WalBatchingConfig(batch_window_ms=100, max_records_per_batch=tenant_count),
    )
    batch_engine = TenantDatabaseEngine.from_dataset_dir(
        batch_dataset,
        wal_committer=batch_committer,
        initialize_durability=True,
        max_open_connections=tenant_count,
    )

    try:
        single_results = _run_one_write_per_tenant(single_engine, single_dataset)
        batch_results = _run_one_write_per_tenant(batch_engine, batch_dataset)
        assert all(result.success for result in single_results)
        assert all(result.success for result in batch_results)
        assert single_store.record_puts == tenant_count
        assert batch_store.batch_puts < single_store.record_puts
    finally:
        single_engine.close_all()
        batch_engine.close_all()
        single_store.cleanup()
        batch_store.cleanup()


def _engine_record(tmp_path: Path):
    return _engine_records(tmp_path / "record", tenant_count=1)[0]


def _engine_records(tmp_path: Path, *, tenant_count: int) -> tuple[WalRecord, ...]:
    dataset = _dataset(tmp_path, tenant_count=tenant_count)
    store = _MemoryStore()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    try:
        for index, tenant in enumerate(engine.registry.tenant_ids):
            tenant_id = tenant.value
            entity_id = _first_entity_id(dataset, tenant_id)
            result = engine.execute(
                _operation(tenant_id, EntityDeltaPayload(f"tx-roundtrip-{index}", entity_id, 1))
            )
            assert result.success
    finally:
        engine.close_all()
    return tuple(store.records)


class _MemoryStore:
    def __init__(self) -> None:
        self.records: list[WalRecord] = []

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        self.records.append(record)
        assert record.record_checksum is not None
        return PersistedWalReceipt(
            bucket="memory",
            key="memory-key",
            etag=None,
            version_id=None,
            checksum=record.record_checksum,
            bytes_written=len(encode_record(record)),
            duration_ns=1,
            remote_durable=True,
        )

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        for record in self.records:
            if record.tenant_id == tenant_id and record.sequence_no == sequence_no:
                return record
        raise KeyError(sequence_no)


class _BlockingBatchStore:
    requires_bootstrap = True

    @property
    def namespace_id(self):
        return self.store.namespace_id

    def inspect_history(self, tenant):
        return self.store.inspect_history(tenant)

    def __init__(self, store: S3ExpressWalStore) -> None:
        self.store = store
        self.started = threading.Event()
        self.release = threading.Event()
        self.keys: list[str] = []

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        self.started.set()
        assert self.release.wait(timeout=30)
        receipt = self.store.persist_batch(batch)
        self.keys.append(receipt.key)
        return receipt

    def fetch_batch(self, key: str) -> WalBatch:
        return self.store.fetch_batch(key)


class _CountingStore:
    requires_bootstrap = True

    @property
    def namespace_id(self):
        return self.store.namespace_id

    def inspect_history(self, tenant):
        return self.store.inspect_history(tenant)

    def __init__(self, store: S3ExpressWalStore) -> None:
        self.store = store
        self.record_puts = 0
        self.batch_puts = 0
        self.record_keys: list[tuple[str, int]] = []
        self.batch_keys: list[str] = []

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        receipt = self.store.persist_record(record)
        self.record_puts += 1
        self.record_keys.append((record.tenant_id, record.sequence_no))
        return receipt

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        return self.store.fetch_record(tenant_id, sequence_no)

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        receipt = self.store.persist_batch(batch)
        self.batch_puts += 1
        self.batch_keys.append(receipt.key)
        return receipt

    def fetch_batch(self, key: str) -> WalBatch:
        return self.store.fetch_batch(key)

    def cleanup(self) -> None:
        for tenant_id, sequence_no in self.record_keys:
            self.store.delete_record(tenant_id, sequence_no)
        for key in self.batch_keys:
            self.store.delete_batch(key)


def _dataset(output_dir: Path, *, tenant_count: int = 1) -> Path:
    return generate_dataset(
        output_dir,
        DatasetConfig(
            tenant_count=tenant_count,
            entities_per_tenant=5,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
            seed=42,
        ),
    ).output_dir


def _operation(tenant_id: str, payload: EntityDeltaPayload) -> WorkloadOperation:
    return WorkloadOperation(
        operation_id=deterministic_uuid("aws-test-operation", tenant_id, payload.transaction_id),
        sequence_no=0,
        tenant_id=tenant_id,
        operation_type=OperationType.ENTITY_DELTA,
        scheduled_offset_ms=0,
        benchmark_phase=BenchmarkPhase.MEASUREMENT,
        virtual_client_id="client-0001",
        payload=payload,
    )


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


def _tenant_ids(dataset_dir: Path) -> tuple[str, ...]:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    return tuple(tenant["tenant_id"] for tenant in manifest["tenant_databases"])


def _tenant_db_path(dataset_dir: Path, tenant_id: str) -> Path:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    for tenant in manifest["tenant_databases"]:
        if tenant["tenant_id"] == tenant_id:
            return dataset_dir / tenant["filename"]
    raise AssertionError(f"tenant not found: {tenant_id}")


def _run_one_write_per_tenant(
    engine: TenantDatabaseEngine,
    dataset_dir: Path,
):
    tenant_ids = _tenant_ids(dataset_dir)
    with ThreadPoolExecutor(max_workers=len(tenant_ids)) as pool:
        return list(
            pool.map(
                lambda tenant_id: engine.execute(
                    _operation(
                        tenant_id,
                        EntityDeltaPayload(
                            f"tx-reduction-{tenant_id}",
                            _first_entity_id(dataset_dir, tenant_id),
                            1,
                        ),
                    )
                ),
                tenant_ids,
            )
        )
