import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.registry import TenantDatabaseRegistry, TenantRegistryError
from ephemeraldb.engine.results import EngineErrorCode
from ephemeraldb.engine.runner import run_workload
from ephemeraldb.loadgen.generator import generate_workload
from ephemeraldb.loadgen.models import (
    AggregatePayload,
    BenchmarkPhase,
    EntityDeltaPayload,
    EntityHistoryPayload,
    EntityLookupPayload,
    MetadataLookupPayload,
    MultiEntityTransactionItem,
    MultiEntityTransactionPayload,
    OperationType,
    RelationshipExpansionPayload,
    StateScanPayload,
    WorkloadOperation,
)
from ephemeraldb.loadgen.profiles import get_builtin_profile
from ephemeraldb.loadgen.schedule import constant_schedule
from ephemeraldb.loadgen.serialization import iter_workload
from ephemeraldb.schema.generator import deterministic_uuid, generate_dataset
from ephemeraldb.schema.models import DatasetConfig


def test_registry_rejects_path_escape(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    (dataset_dir / "manifest.json").write_text(
        json.dumps(
            {
                "tenant_databases": [
                    {
                        "tenant_id": "tenant-a",
                        "filename": "../escape.db",
                        "entity_count": 1,
                        "relationship_count": 0,
                        "metadata_count": 0,
                        "state_checksum": "checksum",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    try:
        TenantDatabaseRegistry.from_dataset_dir(dataset_dir)
    except TenantRegistryError as exc:
        assert "escapes" in str(exc)
    else:
        raise AssertionError("expected TenantRegistryError")


def test_connection_configuration_and_cache(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, tenant_count=2)
    engine = TenantDatabaseEngine.from_dataset_dir(
        dataset,
        max_open_connections=1,
        busy_timeout_ms=1234,
    )
    first_tenant, second_tenant = [tenant.value for tenant in engine.registry.tenant_ids[:2]]

    with engine.connection_manager.connection_for(engine.registry.tenant_ids[0]) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].upper() == "WAL"
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 1234
    assert engine.connection_manager.open_connection_count == 1

    with engine.connection_manager.connection_for(engine.registry.tenant_ids[1]):
        assert engine.connection_manager.open_connection_count == 1
    assert first_tenant != second_tenant
    engine.close_all()
    assert engine.connection_manager.open_connection_count == 0


def test_unknown_tenant_returns_structured_error(tmp_path: Path) -> None:
    engine = TenantDatabaseEngine.from_dataset_dir(_dataset(tmp_path))
    operation = _operation(
        tenant_id="unknown-tenant",
        operation_type=OperationType.ENTITY_LOOKUP,
        payload=EntityLookupPayload(entity_id="entity"),
    )

    result = engine.execute(operation)

    assert not result.success
    assert result.error_code == EngineErrorCode.TENANT_NOT_FOUND


def test_read_operations_execute_against_query_catalog(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id, state = _first_entity(dataset, tenant_id)
    operations = [
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_LOOKUP,
            payload=EntityLookupPayload(entity_id=entity_id),
        ),
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.STATE_SCAN,
            payload=StateScanPayload(state=state, limit=5),
        ),
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.RELATIONSHIP_EXPANSION,
            payload=RelationshipExpansionPayload(source_entity_id=entity_id, limit=5),
        ),
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.METADATA_LOOKUP,
            payload=MetadataLookupPayload(entity_id=entity_id, metadata_key="attribute_00"),
        ),
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_HISTORY,
            payload=EntityHistoryPayload(entity_id=entity_id, limit=5),
        ),
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.AGGREGATE_BY_STATE,
            payload=AggregatePayload(),
        ),
    ]

    results = [engine.execute(operation) for operation in operations]

    assert all(result.success for result in results)
    assert results[0].records[0]["entity_id"] == entity_id
    assert all(result.rows_read > 0 for result in results)


def test_entity_delta_success_and_duplicate_transaction(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id, _state = _first_entity(dataset, tenant_id)
    numeric_value, version = _entity_value_and_version(dataset, tenant_id, entity_id)
    operation = _operation(
        tenant_id=tenant_id,
        operation_type=OperationType.ENTITY_DELTA,
        payload=EntityDeltaPayload(
            transaction_id="tx-duplicate",
            entity_id=entity_id,
            delta=10,
            expected_version=version,
        ),
    )

    first = engine.execute(operation)
    second = engine.execute(operation)
    updated_numeric_value, updated_version = _entity_value_and_version(
        dataset,
        tenant_id,
        entity_id,
    )

    assert first.success
    assert not first.duplicate_transaction
    assert second.success
    assert second.duplicate_transaction
    assert updated_numeric_value == numeric_value + 10
    assert updated_version == version + 1


def test_entity_delta_conflict_rolls_back(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id, _state = _first_entity(dataset, tenant_id)
    before = _entity_value_and_version(dataset, tenant_id, entity_id)

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload(
                transaction_id="tx-conflict",
                entity_id=entity_id,
                delta=10,
                expected_version=before[1] - 1,
            ),
        )
    )

    assert not result.success
    assert result.conflict
    assert result.error_code == EngineErrorCode.TRANSACTION_CONFLICT
    assert _entity_value_and_version(dataset, tenant_id, entity_id) == before
    assert _transaction_item_count(dataset, "tx-conflict") == 0


def test_multi_entity_conflict_rolls_back_everything(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, entities_per_tenant=5)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_ids = _first_entity_ids(dataset, tenant_id, limit=3)
    before = [_entity_value_and_version(dataset, tenant_id, entity_id) for entity_id in entity_ids]
    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.MULTI_ENTITY_TRANSACTION,
            payload=MultiEntityTransactionPayload(
                transaction_id="tx-multi-conflict",
                items=(
                    MultiEntityTransactionItem(entity_ids[0], 10, before[0][1]),
                    MultiEntityTransactionItem(entity_ids[1], 10, before[1][1] - 1),
                    MultiEntityTransactionItem(entity_ids[2], 10, before[2][1]),
                ),
            ),
        )
    )

    assert not result.success
    assert result.conflict
    assert [
        _entity_value_and_version(dataset, tenant_id, entity_id)
        for entity_id in entity_ids
    ] == before
    assert _transaction_item_count(dataset, "tx-multi-conflict") == 0
    assert _audit_count(dataset, "tx-multi-conflict") == 0


def test_multi_entity_transaction_commits_atomically(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, entities_per_tenant=5)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_ids = _first_entity_ids(dataset, tenant_id, limit=3)
    before = [_entity_value_and_version(dataset, tenant_id, entity_id) for entity_id in entity_ids]

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.MULTI_ENTITY_TRANSACTION,
            payload=MultiEntityTransactionPayload(
                transaction_id="tx-multi",
                items=(
                    MultiEntityTransactionItem(entity_ids[0], 1, before[0][1]),
                    MultiEntityTransactionItem(entity_ids[1], 2, before[1][1]),
                    MultiEntityTransactionItem(entity_ids[2], 3, before[2][1]),
                ),
            ),
        )
    )

    assert result.success
    assert _transaction_item_count(dataset, "tx-multi") == 3
    after = [_entity_value_and_version(dataset, tenant_id, entity_id) for entity_id in entity_ids]
    assert after == [
        (before[0][0] + 1, before[0][1] + 1),
        (before[1][0] + 2, before[1][1] + 1),
        (before[2][0] + 3, before[2][1] + 1),
    ]


def test_integrity_error_rolls_back_partial_state(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id, _state = _first_entity(dataset, tenant_id)
    before = _entity_value_and_version(dataset, tenant_id, entity_id)
    _insert_pending_transaction(dataset, tenant_id, "tx-integrity")

    result = engine.execute(
        _operation(
            tenant_id=tenant_id,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload(
                transaction_id="tx-integrity",
                entity_id=entity_id,
                delta=5,
                expected_version=before[1],
            ),
        )
    )

    assert not result.success
    assert result.error_code == EngineErrorCode.INTEGRITY_ERROR
    assert _entity_value_and_version(dataset, tenant_id, entity_id) == before
    assert _transaction_item_count(dataset, "tx-integrity") == 0


def test_tenant_isolation_across_physical_databases(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, tenant_count=3)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_a, tenant_b, tenant_c = [tenant.value for tenant in engine.registry.tenant_ids]
    entity_a, _state_a = _first_entity(dataset, tenant_a)
    entity_b, _state_b = _first_entity(dataset, tenant_b)
    entity_c, _state_c = _first_entity(dataset, tenant_c)
    before_b = _entity_value_and_version(dataset, tenant_b, entity_b)
    before_c = _entity_value_and_version(dataset, tenant_c, entity_c)

    result_a = engine.execute(
        _operation(
            tenant_id=tenant_a,
            operation_type=OperationType.ENTITY_DELTA,
            payload=EntityDeltaPayload("tx-a", entity_a, 7),
        )
    )
    result_c = engine.execute(
        _operation(
            tenant_id=tenant_c,
            operation_type=OperationType.ENTITY_LOOKUP,
            payload=EntityLookupPayload(entity_c),
        )
    )

    assert result_a.success
    assert result_c.success
    assert _entity_value_and_version(dataset, tenant_b, entity_b) == before_b
    assert _entity_value_and_version(dataset, tenant_c, entity_c) == before_c


def test_separate_tenant_files_can_be_written_from_separate_engine_contexts(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, tenant_count=2)
    tenant_ids = _tenant_ids(dataset)

    def write_one(tenant_id: str, tx_id: str) -> bool:
        engine = TenantDatabaseEngine.from_dataset_dir(dataset)
        try:
            entity_id, _state = _first_entity(dataset, tenant_id)
            return engine.execute(
                _operation(
                    tenant_id=tenant_id,
                    operation_type=OperationType.ENTITY_DELTA,
                    payload=EntityDeltaPayload(tx_id, entity_id, 1),
                )
            ).success
        finally:
            engine.close_all()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda args: write_one(*args),
                zip(tenant_ids, ("tx-a", "tx-b"), strict=True),
            )
        )

    assert results == [True, True]


def test_checksum_and_validation_integration(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset)
    tenant_id = engine.registry.tenant_ids[0].value

    checksum = engine.calculate_tenant_checksum(tenant_id)
    validation = engine.validate_tenant(tenant_id)

    assert len(checksum) == 64
    assert validation.ok


def test_sequential_workload_runner_and_deterministic_final_state(tmp_path: Path) -> None:
    first_dataset = generate_dataset(
        tmp_path / "dataset-a",
        DatasetConfig(tenant_count=2, entities_per_tenant=20, seed=42),
    )
    workload = generate_workload(
        dataset_manifest_path=first_dataset.manifest_path,
        profile=get_builtin_profile("write-heavy", seed=42).with_overrides(virtual_client_count=5),
        schedule=constant_schedule(name="local", duration_seconds=2, rate=20),
        output_path=tmp_path / "workload.jsonl",
    )
    first_engine = TenantDatabaseEngine.from_dataset_dir(first_dataset.output_dir)
    first_summary = run_workload(
        first_engine,
        iter_workload(workload.workload_path),
        output_path=tmp_path / "first-results.jsonl",
    )
    first_engine.close_all()

    second_dataset = generate_dataset(
        tmp_path / "dataset-b",
        DatasetConfig(tenant_count=2, entities_per_tenant=20, seed=42),
    )
    second_engine = TenantDatabaseEngine.from_dataset_dir(second_dataset.output_dir)
    second_summary = run_workload(second_engine, iter_workload(workload.workload_path))
    second_engine.close_all()

    assert first_summary.operations_attempted == 40
    assert first_summary.final_tenant_checksums == second_summary.final_tenant_checksums
    assert (tmp_path / "first-results.jsonl").exists()


def test_sqlite_busy_maps_to_stable_error_code(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, busy_timeout_ms=1)
    tenant_id = engine.registry.tenant_ids[0].value
    entity_id, _state = _first_entity(dataset, tenant_id)
    locker = sqlite3.connect(_tenant_db_path(dataset, tenant_id), isolation_level=None)
    try:
        locker.execute("BEGIN IMMEDIATE")
        result = engine.execute(
            _operation(
                tenant_id=tenant_id,
                operation_type=OperationType.ENTITY_DELTA,
                payload=EntityDeltaPayload("tx-busy", entity_id, 1),
            )
        )
    finally:
        locker.rollback()
        locker.close()

    assert not result.success
    assert result.error_code == EngineErrorCode.SQLITE_BUSY


def _dataset(
    tmp_path: Path,
    *,
    tenant_count: int = 1,
    entities_per_tenant: int = 10,
) -> Path:
    return generate_dataset(
        tmp_path / f"dataset-{tenant_count}-{entities_per_tenant}",
        DatasetConfig(
            tenant_count=tenant_count,
            entities_per_tenant=entities_per_tenant,
            relationships_per_entity=2,
            metadata_entries_per_entity=3,
            seed=42,
        ),
    ).output_dir


def _operation(
    *,
    tenant_id: str,
    operation_type: OperationType,
    payload,
    sequence_no: int = 0,
) -> WorkloadOperation:
    return WorkloadOperation(
        operation_id=deterministic_uuid("test-operation", tenant_id, operation_type, sequence_no),
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


def _first_entity(dataset_dir: Path, tenant_id: str) -> tuple[str, str]:
    database_path = _tenant_db_path(dataset_dir, tenant_id)
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute(
            "SELECT entity_id, state FROM entities ORDER BY entity_id LIMIT 1"
        ).fetchone()
        return str(row[0]), str(row[1])
    finally:
        connection.close()


def _first_entity_ids(dataset_dir: Path, tenant_id: str, *, limit: int) -> list[str]:
    database_path = _tenant_db_path(dataset_dir, tenant_id)
    connection = sqlite3.connect(database_path)
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
    database_path = _tenant_db_path(dataset_dir, tenant_id)
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute(
            "SELECT numeric_value, version FROM entities WHERE entity_id = ?",
            (entity_id,),
        ).fetchone()
        return int(row[0]), int(row[1])
    finally:
        connection.close()


def _transaction_item_count(dataset_dir: Path, transaction_id: str) -> int:
    return _count_across_tenants(
        dataset_dir,
        "SELECT COUNT(*) FROM transaction_items WHERE transaction_id = ?",
        transaction_id,
    )


def _audit_count(dataset_dir: Path, transaction_id: str) -> int:
    return _count_across_tenants(
        dataset_dir,
        "SELECT COUNT(*) FROM audit_log WHERE transaction_id = ?",
        transaction_id,
    )


def _count_across_tenants(dataset_dir: Path, sql: str, value: str) -> int:
    total = 0
    for tenant_id in _tenant_ids(dataset_dir):
        connection = sqlite3.connect(_tenant_db_path(dataset_dir, tenant_id))
        try:
            total += int(connection.execute(sql, (value,)).fetchone()[0])
        finally:
            connection.close()
    return total


def _insert_pending_transaction(dataset_dir: Path, tenant_id: str, transaction_id: str) -> None:
    database_path = _tenant_db_path(dataset_dir, tenant_id)
    connection = sqlite3.connect(database_path)
    try:
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
            (transaction_id, tenant_id),
        )
        connection.commit()
    finally:
        connection.close()
