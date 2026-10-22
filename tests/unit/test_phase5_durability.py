import json
import sqlite3
import threading
from pathlib import Path

import pytest

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.config.settings import load_settings
from ephemeraldb.core.errors import S3ExpressConfigurationError, S3ExpressWriteError
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
from ephemeraldb.wal.codec import encode_record
from ephemeraldb.wal.durability import PersistedWalReceipt
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.sequence import current_sequence_state
from ephemeraldb.wal.writer import sequence_filename


class RecordingDurabilityStore:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.records: list[WalRecord] = []
        self.bodies: dict[tuple[str, int], bytes] = {}

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        self.records.append(record)
        if self.fail:
            msg = "injected S3 PUT failure"
            raise S3ExpressWriteError(msg)
        body = encode_record(record)
        self.bodies[(record.tenant_id, record.sequence_no)] = body
        assert record.record_checksum is not None
        return PersistedWalReceipt(
            bucket="fake-s3-express",
            key=f"wal/{record.tenant_id}/{sequence_filename(record.sequence_no)}",
            etag="fake-etag",
            version_id=None,
            checksum=record.record_checksum,
            bytes_written=len(body),
            duration_ns=1234,
            remote_durable=True,
        )

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        for record in self.records:
            if record.tenant_id == tenant_id and record.sequence_no == sequence_no:
                return record
        raise KeyError(sequence_no)


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


def test_s3_store_configuration_and_object_key() -> None:
    config = S3ExpressWalConfig(
        bucket="bench--aps1-az1--x-s3",
        region="ap-south-1",
        prefix="wal",
        zone_id="aps1-az1",
    )
    store = S3ExpressWalStore(config, client=CapturingS3Client())

    assert store.object_key("tenant-000001", 7) == (
        "wal/tenant-000001/00000000000000000007.json"
    )
    with pytest.raises(S3ExpressConfigurationError):
        S3ExpressWalConfig(bucket="", region="ap-south-1")


def test_s3_store_preserves_canonical_body_and_metadata(tmp_path: Path) -> None:
    client = CapturingS3Client()
    store = S3ExpressWalStore(
        S3ExpressWalConfig(bucket="bench--aps1-az1--x-s3"),
        client=client,
    )
    record = _record_from_engine(tmp_path)

    receipt = store.persist_record(record)

    put = client.puts[0]
    assert put["Body"] == encode_record(record)
    assert put["IfNoneMatch"] == "*"
    assert put["StorageClass"] == "EXPRESS_ONEZONE"
    assert put["Metadata"]["record-checksum"] == record.record_checksum
    assert receipt.remote_durable
    assert receipt.key.endswith(sequence_filename(record.sequence_no))


def test_s3_mode_without_bucket_does_not_fallback_to_local(tmp_path: Path) -> None:
    settings = load_settings({"WAL_STORE": "s3-express", "AWS_REGION": "ap-south-1"})

    with pytest.raises(S3ExpressConfigurationError):
        TenantDatabaseEngine.from_settings(_dataset(tmp_path), settings, wal_root=tmp_path / "wal")


def test_durability_failure_leaves_local_db_unchanged(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    store = RecordingDurabilityStore(fail=True)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id)[0]
    before = _entity_value_and_version(dataset, tenant_id, entity_id)

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            payload=EntityDeltaPayload("tx-remote-fails", entity_id, 10, before[1]),
        )
    )

    assert not result.success
    assert result.error_code == EngineErrorCode.REMOTE_DURABILITY_FAILED
    assert not result.remote_durable
    assert len(store.records) == 1
    assert _entity_value_and_version(dataset, tenant_id, entity_id) == before
    assert _transaction_count(dataset, "tx-remote-fails") == 0


def test_successful_remote_durability_then_local_apply_ack(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    store = RecordingDurabilityStore()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id)[0]
    before = _entity_value_and_version(dataset, tenant_id, entity_id)

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            payload=EntityDeltaPayload("tx-remote-ok", entity_id, 5, before[1]),
        )
    )

    assert result.success
    assert result.remote_durable
    assert result.wal_sequence_no == 1
    assert result.wal_object_key is not None
    assert result.wal_record_checksum == store.records[0].record_checksum
    assert result.s3_put_latency_ns == 1234
    assert _entity_value_and_version(dataset, tenant_id, entity_id) == (
        before[0] + 5,
        before[1] + 1,
    )


def test_remote_success_local_apply_failure_requires_reconciliation(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    store = RecordingDurabilityStore()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id)[0]
    before = _entity_value_and_version(dataset, tenant_id, entity_id)

    def insert_conflicting_transaction(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            INSERT INTO transactions (
                transaction_id,
                tenant_id,
                status,
                created_at,
                completed_at
            )
            VALUES ('tx-apply-fails', ?, 'PENDING', '2026-01-01T00:00:00Z', NULL)
            """,
            (tenant_id,),
        )

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            payload=EntityDeltaPayload("tx-apply-fails", entity_id, 5, before[1]),
        ),
        before_transaction_insert_hook=insert_conflicting_transaction,
    )

    assert not result.success
    assert result.error_code == EngineErrorCode.LOCAL_APPLY_AFTER_DURABILITY_FAILED
    assert result.remote_durable
    assert result.reconciliation_required
    assert len(store.records) == 1
    assert _entity_value_and_version(dataset, tenant_id, entity_id) == before
    assert _transaction_count(dataset, "tx-apply-fails") == 0


def test_pre_s3_conflict_does_not_persist_or_advance_sequence(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    store = RecordingDurabilityStore()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id)[0]
    _numeric_value, version = _entity_value_and_version(dataset, tenant_id, entity_id)

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            payload=EntityDeltaPayload("tx-conflict", entity_id, 1, version - 1),
        )
    )

    assert not result.success
    assert result.conflict
    assert store.records == []
    assert _wal_state(dataset, tenant_id) == (0, None)


def test_duplicate_transaction_does_not_create_another_remote_put(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    store = RecordingDurabilityStore()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id)[0]
    operation = _operation(
        tenant_id=tenant_id,
        payload=EntityDeltaPayload("tx-duplicate-remote", entity_id, 1),
    )

    first = engine.execute(operation)
    second = engine.execute(operation)

    assert first.success
    assert second.success
    assert second.duplicate_transaction
    assert second.remote_durable
    assert second.wal_sequence_no == 1
    assert len(store.records) == 1


def test_per_tenant_lock_is_held_across_prepare_persist_apply(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    store = RecordingDurabilityStore()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id)[0]
    probe_results: list[bool] = []

    def probe_lock(point: WalCommitFaultPoint, _connection: sqlite3.Connection) -> None:
        if point != WalCommitFaultPoint.AFTER_PREPARE:
            return

        def try_lock() -> None:
            lock = engine.executor._tenant_locks[tenant_id]
            acquired = lock.acquire(blocking=False)
            probe_results.append(acquired)
            if acquired:
                lock.release()

        thread = threading.Thread(target=try_lock)
        thread.start()
        thread.join()

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            payload=EntityDeltaPayload("tx-lock", entity_id, 1),
        ),
        wal_fault_hook=probe_lock,
    )

    assert result.success
    assert probe_results == [False]


def _record_from_engine(tmp_path: Path) -> WalRecord:
    dataset = _dataset(tmp_path / "s3-record")
    store = RecordingDurabilityStore()
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id)[0]
    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            payload=EntityDeltaPayload("tx-record", entity_id, 1),
        )
    )
    engine.close_all()
    assert result.success
    return store.records[0]


def _dataset(tmp_path: Path) -> Path:
    return generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(
            tenant_count=1,
            entities_per_tenant=5,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
            seed=42,
        ),
    ).output_dir


def _operation(
    *,
    tenant_id: str,
    payload: EntityDeltaPayload,
    sequence_no: int = 0,
) -> WorkloadOperation:
    return WorkloadOperation(
        operation_id=deterministic_uuid("phase5-test-operation", tenant_id, sequence_no),
        sequence_no=sequence_no,
        tenant_id=tenant_id,
        operation_type=OperationType.ENTITY_DELTA,
        scheduled_offset_ms=0,
        benchmark_phase=BenchmarkPhase.MEASUREMENT,
        virtual_client_id="client-0001",
        payload=payload,
    )


def _tenant_db_path(dataset_dir: Path, tenant_id: str) -> Path:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    for tenant in manifest["tenant_databases"]:
        if tenant["tenant_id"] == tenant_id:
            return dataset_dir / tenant["filename"]
    raise AssertionError(f"tenant not found: {tenant_id}")


def _first_entity_ids(dataset_dir: Path, tenant_id: str) -> list[str]:
    connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
    try:
        return [
            str(row[0])
            for row in connection.execute(
                "SELECT entity_id FROM entities ORDER BY entity_id LIMIT 1"
            ).fetchall()
        ]
    finally:
        connection.close()


def _entity_value_and_version(dataset_dir: Path, tenant_id: str, entity_id: str) -> tuple[int, int]:
    connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
    try:
        row = connection.execute(
            "SELECT numeric_value, version FROM entities WHERE tenant_id = ? AND entity_id = ?",
            (tenant_id, entity_id),
        ).fetchone()
        return int(row[0]), int(row[1])
    finally:
        connection.close()


def _transaction_count(dataset_dir: Path, transaction_id: str) -> int:
    tenant_id = _tenant_ids(dataset_dir)[0]
    connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
    try:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM transactions WHERE transaction_id = ?",
                (transaction_id,),
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _wal_state(dataset_dir: Path, tenant_id: str) -> tuple[int, str | None]:
    connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
    try:
        return current_sequence_state(connection)
    finally:
        connection.close()


def _tenant_ids(dataset_dir: Path) -> tuple[str, ...]:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    return tuple(tenant["tenant_id"] for tenant in manifest["tenant_databases"])
