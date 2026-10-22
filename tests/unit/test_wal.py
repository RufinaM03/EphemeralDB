import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from ephemeraldb.core.errors import (
    UnsupportedWalVersionError,
    WalChainError,
    WalChecksumError,
    WalReplayError,
    WalSequenceError,
)
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.loadgen.models import (
    BenchmarkPhase,
    EntityDeltaPayload,
    MultiEntityTransactionItem,
    MultiEntityTransactionPayload,
    OperationType,
    WorkloadOperation,
)
from ephemeraldb.schema.checksum import calculate_logical_checksum
from ephemeraldb.schema.generator import deterministic_uuid, generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.wal.checksum import attach_record_checksum, calculate_record_checksum
from ephemeraldb.wal.codec import decode_record, encode_record, record_to_dict
from ephemeraldb.wal.models import WAL_FORMAT_VERSION, WalMutation, WalMutationType, WalRecord
from ephemeraldb.wal.reader import iter_record_paths, iter_records
from ephemeraldb.wal.replay import replay_wal
from ephemeraldb.wal.sequence import (
    advance_sequence,
    allocate_next_sequence,
    current_sequence_state,
    ensure_wal_state_table,
)
from ephemeraldb.wal.validation import validate_chain, wal_summary
from ephemeraldb.wal.writer import LocalWalStore, sequence_filename


def test_canonical_serialization_and_record_checksum() -> None:
    record = _sample_record()

    encoded = encode_record(record)
    decoded = decode_record(encoded)

    assert encode_record(decoded) == encoded
    assert decoded.record_checksum == calculate_record_checksum(decoded)
    assert len(decoded.record_checksum or "") == 64


def test_sequence_allocation_uses_db_state() -> None:
    connection = sqlite3.connect(":memory:", isolation_level=None)

    ensure_wal_state_table(connection)
    assert allocate_next_sequence(connection) == (1, None)
    advance_sequence(connection, 1, "a" * 64)
    assert current_sequence_state(connection) == (1, "a" * 64)
    assert allocate_next_sequence(connection) == (2, "a" * 64)


def test_local_wal_store_reader_and_summary(tmp_path: Path) -> None:
    store = LocalWalStore(tmp_path / "wal")
    first = _sample_record(sequence_no=1, transaction_id="tx-1")
    second = _sample_record(
        sequence_no=2,
        previous_record_checksum=first.record_checksum,
        transaction_id="tx-2",
    )

    first_bytes = store.write_record(first)
    second_bytes = store.write_record(second)
    records = list(iter_records(store.wal_root, first.tenant_id))
    summary = wal_summary(store.wal_root, first.tenant_id)

    assert [record.sequence_no for record in records] == [1, 2]
    assert summary.record_count == 2
    assert summary.first_sequence == 1
    assert summary.last_sequence == 2
    assert summary.total_bytes == first_bytes + second_bytes
    assert summary.unique_transactions == 2
    assert summary.checksum_chain_valid


def test_entity_delta_wal_contents_and_metrics(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, "dataset")
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id, limit=1)[0]
    _numeric_value, version = _entity_value_and_version(dataset, tenant_id, entity_id)

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload("tx-delta", entity_id, 7, version),
        )
    )
    records = list(iter_records(wal_root, tenant_id))
    record = records[0]

    assert result.success
    assert result.wal_sequence_no == 1
    assert result.wal_bytes > 0
    assert engine.metrics.snapshot()["wal_records_created"] == 1
    assert record.operation_type == OperationType.ENTITY_DELTA
    assert [mutation.mutation_type for mutation in record.operations] == [
        WalMutationType.INSERT_TRANSACTION,
        WalMutationType.UPDATE_ENTITY,
        WalMutationType.INSERT_ENTITY_VERSION,
        WalMutationType.INSERT_TRANSACTION_ITEM,
        WalMutationType.INSERT_AUDIT_LOG,
    ]


def test_multi_entity_transaction_wal_contents(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, "dataset", entities_per_tenant=6)
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_ids = _first_entity_ids(dataset, tenant_id, limit=3)
    versions = [
        _entity_value_and_version(dataset, tenant_id, entity_id)[1]
        for entity_id in entity_ids
    ]

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.MULTI_ENTITY_TRANSACTION,
            payload=MultiEntityTransactionPayload(
                "tx-multi",
                tuple(
                    MultiEntityTransactionItem(entity_id, index + 1, versions[index])
                    for index, entity_id in enumerate(entity_ids)
                ),
            ),
        )
    )
    record = next(iter(iter_records(wal_root, tenant_id)))

    assert result.success
    assert record.operation_type == OperationType.MULTI_ENTITY_TRANSACTION
    assert len(record.operations) == 13
    assert _mutation_count(record, WalMutationType.UPDATE_ENTITY) == 3
    assert _mutation_count(record, WalMutationType.INSERT_ENTITY_VERSION) == 3


def test_conflict_produces_no_wal_record(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, "dataset")
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id, limit=1)[0]
    _numeric_value, version = _entity_value_and_version(dataset, tenant_id, entity_id)

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload("tx-conflict", entity_id, 1, version - 1),
        )
    )

    assert not result.success
    assert result.wal_sequence_no is None
    assert list(iter_record_paths(wal_root, tenant_id)) == []


def test_duplicate_transaction_produces_no_additional_wal_record(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, "dataset")
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id, limit=1)[0]
    operation = _operation(
        tenant_id=tenant_id,
        operation_type=OperationType.ENTITY_DELTA,
        payload=EntityDeltaPayload("tx-duplicate", entity_id, 1),
    )

    first = engine.execute(operation)
    second = engine.execute(operation)

    assert first.success
    assert second.success
    assert second.duplicate_transaction
    assert second.wal_sequence_no == 1
    assert [record.sequence_no for record in iter_records(wal_root, tenant_id)] == [1]


@pytest.mark.parametrize("scenario", ["entity-delta", "multi-entity", "mixed"])
def test_replay_rebuild_checksum_matches_source(tmp_path: Path, scenario: str) -> None:
    source = _dataset(tmp_path, f"source-{scenario}", entities_per_tenant=8)
    target = _dataset(tmp_path, f"target-{scenario}", entities_per_tenant=8)
    wal_root = tmp_path / f"wal-{scenario}"
    engine = TenantDatabaseEngine.from_dataset_dir(source, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value

    operations = _scenario_operations(source, tenant_id, scenario)
    for operation in operations:
        result = engine.execute(operation)
        assert result.success
    source_checksum = engine.calculate_tenant_checksum(tenant_id)
    engine.close_all()

    connection = sqlite3.connect(_tenant_db_path(target, tenant_id), isolation_level=None)
    try:
        summary = replay_wal(connection, iter_records(wal_root, tenant_id), tenant_id=tenant_id)
        target_checksum = calculate_logical_checksum(connection)
    finally:
        connection.close()

    assert summary.records_applied == len(operations)
    assert summary.final_sequence == len(operations)
    assert source_checksum == target_checksum


def test_replay_is_idempotent_for_already_applied_records(tmp_path: Path) -> None:
    source = _dataset(tmp_path, "source", entities_per_tenant=8)
    target = _dataset(tmp_path, "target", entities_per_tenant=8)
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(source, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value

    for operation in _scenario_operations(source, tenant_id, "mixed"):
        assert engine.execute(operation).success
    engine.close_all()

    connection = sqlite3.connect(_tenant_db_path(target, tenant_id), isolation_level=None)
    try:
        first = replay_wal(connection, iter_records(wal_root, tenant_id), tenant_id=tenant_id)
        checksum = calculate_logical_checksum(connection)
        second = replay_wal(connection, iter_records(wal_root, tenant_id), tenant_id=tenant_id)
        assert calculate_logical_checksum(connection) == checksum
    finally:
        connection.close()

    assert first.records_applied == 2
    assert second.records_applied == 0
    assert second.duplicates_skipped == 2


def test_deleted_middle_record_is_detected_as_gap(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, "dataset", entities_per_tenant=8)
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value

    for index in range(3):
        entity_id = _first_entity_ids(dataset, tenant_id, limit=1)[0]
        assert engine.execute(
            _operation(
                tenant_id=tenant_id,
                operation_type=OperationType.ENTITY_DELTA,
                payload=EntityDeltaPayload(f"tx-{index}", entity_id, 1),
                sequence_no=index,
            )
        ).success
    (wal_root / tenant_id / sequence_filename(2)).unlink()

    with pytest.raises(WalSequenceError):
        list(iter_records(wal_root, tenant_id))


def test_mutation_tampering_without_checksum_recalculation_fails(tmp_path: Path) -> None:
    record, path = _engine_record_and_path(tmp_path)
    payload = record_to_dict(record, include_checksum=True)
    payload["operations"][1]["values"]["numeric_value"] = 999
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    with pytest.raises(WalChecksumError):
        list(iter_records(path.parent.parent, record.tenant_id))


def test_changed_previous_checksum_fails_chain_validation(tmp_path: Path) -> None:
    store = LocalWalStore(tmp_path / "wal")
    first = _sample_record(sequence_no=1, transaction_id="tx-1")
    second = _sample_record(
        sequence_no=2,
        previous_record_checksum=first.record_checksum,
        transaction_id="tx-2",
    )
    store.write_record(first)
    store.write_record(second)
    changed = attach_record_checksum(
        replace(second, previous_record_checksum="b" * 64, record_checksum=None)
    )
    _write_record(store.record_path(changed.tenant_id, changed.sequence_no), changed)

    with pytest.raises(WalChainError):
        validate_chain(
            iter_records(store.wal_root, first.tenant_id),
            expected_tenant_id=first.tenant_id,
        )


def test_sequence_metadata_tampering_fails_filename_validation(tmp_path: Path) -> None:
    record, path = _single_record_and_path(tmp_path)
    changed = attach_record_checksum(replace(record, sequence_no=2, record_checksum=None))
    _write_record(path, changed)

    with pytest.raises(WalSequenceError):
        list(iter_records(path.parent.parent, record.tenant_id))


def test_unsupported_wal_format_version_fails(tmp_path: Path) -> None:
    record, path = _single_record_and_path(tmp_path)
    changed = attach_record_checksum(
        replace(record, wal_format_version=WAL_FORMAT_VERSION + 1, record_checksum=None)
    )
    _write_record(path, changed)

    with pytest.raises(UnsupportedWalVersionError):
        list(iter_records(path.parent.parent, record.tenant_id))


def test_corrupt_replay_rolls_back_entire_record(tmp_path: Path) -> None:
    source = _dataset(tmp_path, "source")
    target = _dataset(tmp_path, "target")
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(source, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(source, tenant_id, limit=1)[0]
    assert engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload("tx-bad-replay", entity_id, 1),
        )
    ).success
    record = next(iter(iter_records(wal_root, tenant_id)))
    bad_record = _replace_first_mutation(
        record,
        WalMutationType.UPDATE_ENTITY,
        key_updates={"entity_id": "missing-entity"},
        value_updates={"entity_id": "missing-entity"},
    )

    connection = sqlite3.connect(_tenant_db_path(target, tenant_id), isolation_level=None)
    try:
        with pytest.raises(WalReplayError):
            replay_wal(connection, (bad_record,), tenant_id=tenant_id)
        assert _transaction_count(connection, "tx-bad-replay") == 0
        assert current_sequence_state(connection) == (0, None)
    finally:
        connection.close()


def test_replay_rejects_invalid_version_progression(tmp_path: Path) -> None:
    source = _dataset(tmp_path, "source")
    target = _dataset(tmp_path, "target")
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(source, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(source, tenant_id, limit=1)[0]
    _numeric_value, version = _entity_value_and_version(source, tenant_id, entity_id)
    assert engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload("tx-version-gap", entity_id, 1, version),
        )
    ).success
    record = next(iter(iter_records(wal_root, tenant_id)))
    bad_record = _replace_first_mutation(
        record,
        WalMutationType.UPDATE_ENTITY,
        value_updates={"version": version + 3},
    )

    connection = sqlite3.connect(_tenant_db_path(target, tenant_id), isolation_level=None)
    try:
        with pytest.raises(WalReplayError):
            replay_wal(connection, (bad_record,), tenant_id=tenant_id)
        assert _transaction_count(connection, "tx-version-gap") == 0
    finally:
        connection.close()


def test_multi_tenant_wal_sequences_are_independent(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, "dataset", tenant_count=3)
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    tenant_a, tenant_b, tenant_c = [tenant.value for tenant in engine.registry.tenant_ids]

    for tenant_id, count in ((tenant_a, 3), (tenant_b, 1), (tenant_c, 2)):
        entity_id = _first_entity_ids(dataset, tenant_id, limit=1)[0]
        for index in range(count):
            assert engine.execute(
                _operation(
                    tenant_id=tenant_id,
                    operation_type=OperationType.ENTITY_DELTA,
                    payload=EntityDeltaPayload(f"tx-{tenant_id}-{index}", entity_id, 1),
                    sequence_no=index,
                )
            ).success

    assert [record.sequence_no for record in iter_records(wal_root, tenant_a)] == [1, 2, 3]
    assert [record.sequence_no for record in iter_records(wal_root, tenant_b)] == [1]
    assert [record.sequence_no for record in iter_records(wal_root, tenant_c)] == [1, 2]


def _sample_record(
    *,
    sequence_no: int = 1,
    previous_record_checksum: str | None = None,
    tenant_id: str = "tenant-000001",
    transaction_id: str = "tx-1",
) -> WalRecord:
    return attach_record_checksum(
        WalRecord(
            wal_format_version=1,
            tenant_id=tenant_id,
            transaction_id=transaction_id,
            sequence_no=sequence_no,
            operation_type=OperationType.ENTITY_DELTA,
            operations=(
                WalMutation.from_mappings(
                    WalMutationType.INSERT_TRANSACTION,
                    "transactions",
                    key={"tenant_id": tenant_id, "transaction_id": transaction_id},
                    values={
                        "transaction_id": transaction_id,
                        "tenant_id": tenant_id,
                        "status": "COMMITTED",
                        "created_at": "2026-01-01T00:00:00Z",
                        "completed_at": "2026-01-01T00:00:00Z",
                    },
                ),
            ),
            created_at="2026-01-01T00:00:00Z",
            previous_record_checksum=previous_record_checksum,
        )
    )


def _single_record_and_path(tmp_path: Path) -> tuple[WalRecord, Path]:
    store = LocalWalStore(tmp_path / "wal")
    record = _sample_record()
    store.write_record(record)
    return record, store.record_path(record.tenant_id, record.sequence_no)


def _engine_record_and_path(tmp_path: Path) -> tuple[WalRecord, Path]:
    dataset = _dataset(tmp_path, "engine-record")
    wal_root = tmp_path / "engine-wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id = _first_entity_ids(dataset, tenant_id, limit=1)[0]
    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload("tx-engine-record", entity_id, 1),
        )
    )
    engine.close_all()
    assert result.success
    record = next(iter(iter_records(wal_root, tenant_id)))
    return record, wal_root / tenant_id / sequence_filename(record.sequence_no)


def _write_record(path: Path, record: WalRecord) -> None:
    path.write_bytes(encode_record(record))


def _dataset(
    tmp_path: Path,
    name: str,
    *,
    tenant_count: int = 1,
    entities_per_tenant: int = 10,
) -> Path:
    return generate_dataset(
        tmp_path / name,
        DatasetConfig(
            tenant_count=tenant_count,
            entities_per_tenant=entities_per_tenant,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
            seed=42,
        ),
    ).output_dir


def _scenario_operations(
    dataset_dir: Path,
    tenant_id: str,
    scenario: str,
) -> tuple[WorkloadOperation, ...]:
    entity_ids = _first_entity_ids(dataset_dir, tenant_id, limit=4)
    versions = [
        _entity_value_and_version(dataset_dir, tenant_id, entity_id)[1]
        for entity_id in entity_ids
    ]
    delta = _operation(
        tenant_id=tenant_id,
        operation_type=OperationType.ENTITY_DELTA,
        payload=EntityDeltaPayload("tx-replay-delta", entity_ids[0], 5, versions[0]),
        sequence_no=1,
    )
    multi = _operation(
        tenant_id=tenant_id,
        operation_type=OperationType.MULTI_ENTITY_TRANSACTION,
        payload=MultiEntityTransactionPayload(
            "tx-replay-multi",
            (
                MultiEntityTransactionItem(entity_ids[1], 2, versions[1]),
                MultiEntityTransactionItem(entity_ids[2], 3, versions[2]),
            ),
        ),
        sequence_no=2,
    )
    if scenario == "entity-delta":
        return (delta,)
    if scenario == "multi-entity":
        return (multi,)
    return (delta, multi)


def _operation(
    *,
    tenant_id: str,
    operation_type: OperationType,
    payload,
    sequence_no: int = 0,
) -> WorkloadOperation:
    return WorkloadOperation(
        operation_id=deterministic_uuid(
            "wal-test-operation",
            tenant_id,
            operation_type,
            sequence_no,
        ),
        sequence_no=sequence_no,
        tenant_id=tenant_id,
        operation_type=operation_type,
        scheduled_offset_ms=0,
        benchmark_phase=BenchmarkPhase.MEASUREMENT,
        virtual_client_id="client-0001",
        payload=payload,
    )


def _tenant_ids(dataset_dir: Path) -> tuple[str, ...]:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    return tuple(tenant["tenant_id"] for tenant in manifest["tenant_databases"])


def _tenant_db_path(dataset_dir: Path, tenant_id: str) -> Path:
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    for tenant in manifest["tenant_databases"]:
        if tenant["tenant_id"] == tenant_id:
            return dataset_dir / tenant["filename"]
    raise AssertionError(f"tenant not found: {tenant_id}")


def _first_entity_ids(dataset_dir: Path, tenant_id: str, *, limit: int) -> list[str]:
    connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
    try:
        return [
            str(row[0])
            for row in connection.execute(
                "SELECT entity_id FROM entities ORDER BY entity_id LIMIT ?",
                (limit,),
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


def _mutation_count(record: WalRecord, mutation_type: WalMutationType) -> int:
    return sum(mutation.mutation_type == mutation_type for mutation in record.operations)


def _replace_first_mutation(
    record: WalRecord,
    mutation_type: WalMutationType,
    *,
    key_updates: dict[str, object] | None = None,
    value_updates: dict[str, object] | None = None,
) -> WalRecord:
    operations: list[WalMutation] = []
    replaced = False
    for mutation in record.operations:
        if mutation.mutation_type == mutation_type and not replaced:
            key = dict((field.name, field.value) for field in mutation.key)
            values = dict((field.name, field.value) for field in mutation.values)
            key.update(key_updates or {})
            values.update(value_updates or {})
            operations.append(
                WalMutation.from_mappings(
                    mutation.mutation_type,
                    mutation.table,
                    key=key,
                    values=values,
                )
            )
            replaced = True
        else:
            operations.append(mutation)
    assert replaced
    return attach_record_checksum(
        replace(record, operations=tuple(operations), record_checksum=None)
    )


def _transaction_count(connection: sqlite3.Connection, transaction_id: str) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) FROM transactions WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()[0]
    )
