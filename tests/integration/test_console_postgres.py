"""Real PostgreSQL parity checks, independent from any Aurora performance claim."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from ephemeraldb.console.backends import AuroraBackend
from ephemeraldb.console.models import RunRequest
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.loadgen.catalog import DatasetIdentityCatalog
from ephemeraldb.loadgen.generator import WorkloadGenerator
from ephemeraldb.loadgen.models import BenchmarkPhase, EntityDeltaPayload, OperationType
from ephemeraldb.loadgen.profiles import get_builtin_profile
from ephemeraldb.loadgen.schedule import WorkloadSchedule, WorkloadSegment
from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig

pytestmark = pytest.mark.postgres


def test_postgres_adapter_matches_sqlite_logical_operations(tmp_path: Path) -> None:
    if os.environ.get("RUN_POSTGRES_TESTS") != "1" or not os.environ.get("AURORA_DSN"):
        pytest.skip("set RUN_POSTGRES_TESTS=1 and AURORA_DSN for real baseline checks")
    dataset = generate_dataset(
        tmp_path / "dataset", DatasetConfig(tenant_count=2, entities_per_tenant=12)
    )
    catalog = DatasetIdentityCatalog.from_path(dataset.manifest_path)
    profile = get_builtin_profile("balanced", seed=42)
    schedule = WorkloadSchedule(
        "parity", (WorkloadSegment("measure", BenchmarkPhase.MEASUREMENT, 1, 50, 50),)
    )
    operations = list(WorkloadGenerator(catalog, profile, schedule).iter_operations())
    postgres = AuroraBackend(tmp_path, RunRequest(concurrency=4), uuid4().hex)
    sqlite = TenantDatabaseEngine.from_dataset_dir(tmp_path / "dataset")
    try:
        for operation in operations:
            expected = sqlite.execute(operation)
            observed = postgres.execute(operation)
            assert observed["success"] == expected.success, observed
            assert observed["records"] == list(expected.records)
            assert observed["rows_written"] == expected.rows_written
        # Retry identity must be stable; conflicting reuse cannot be acknowledged.
        write = next(op for op in operations if op.operation_type == OperationType.ENTITY_DELTA)
        assert postgres.execute(write)["duplicate_transaction"] is True
        assert isinstance(write.payload, EntityDeltaPayload)
        changed = replace(write, payload=replace(write.payload, delta=write.payload.delta + 1))
        assert postgres.execute(changed)["success"] is False
        # Compare every logical table, including transaction and audit rows.
        from ephemeraldb.schema.checksum import CHECKSUM_TABLES

        with postgres.pool.connection() as connection:
            for tenant in sqlite.registry.tenant_ids:
                with sqlite.connection_manager.connection_for(tenant) as local:
                    for table, ordering, columns in CHECKSUM_TABLES:
                        query = (
                            f'SELECT {",".join(columns)} FROM "{table}" '
                            f"WHERE tenant_id=%s ORDER BY {','.join(ordering)}"
                        )
                        remote = connection.execute(query, (tenant.value,)).fetchall()
                        local_rows = local.execute(
                            query.replace("%s", "?"), (tenant.value,)
                        ).fetchall()
                        assert remote == [dict(row) for row in local_rows], table
    finally:
        sqlite.close_all()
        # Only this test's UUID schema is dropped. The database and other schemas remain intact.
        with postgres.pool.connection() as connection:
            connection.execute(f'DROP SCHEMA "{postgres.schema}" CASCADE')
        postgres.close()
