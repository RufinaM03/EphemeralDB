import json
import shutil
import sqlite3

import pytest

from ephemeraldb.schema.checksum import calculate_logical_checksum
from ephemeraldb.schema.generator import deterministic_uuid, generate_dataset, timestamp_for
from ephemeraldb.schema.models import DatasetConfig


def test_deterministic_ids_and_timestamps() -> None:
    assert deterministic_uuid(42, "tenant", 0) == deterministic_uuid(42, "tenant", 0)
    assert deterministic_uuid(42, "tenant", 0) != deterministic_uuid(43, "tenant", 0)
    assert timestamp_for(0, 0) == "2026-01-01T00:00:00Z"
    assert timestamp_for(1, 0) == "2026-01-12T13:46:40Z"


def test_same_seed_generates_same_manifest_and_logical_rows(tmp_path) -> None:  # type: ignore[no-untyped-def]
    config = DatasetConfig(
        tenant_count=2,
        entities_per_tenant=5,
        relationships_per_entity=2,
        metadata_entries_per_entity=2,
        seed=42,
    )
    first = generate_dataset(tmp_path / "first", config)
    second = generate_dataset(tmp_path / "second", config)

    first_manifest = first.manifest_path.read_text(encoding="utf-8")
    second_manifest = second.manifest_path.read_text(encoding="utf-8")
    assert first_manifest == second_manifest

    first_rows = _sample_rows(tmp_path / "first" / "tenant-000001.db")
    second_rows = _sample_rows(tmp_path / "second" / "tenant-000001.db")
    assert first_rows == second_rows


def test_different_seed_changes_dataset_checksum(tmp_path) -> None:  # type: ignore[no-untyped-def]
    config_a = DatasetConfig(tenant_count=1, entities_per_tenant=5, seed=42)
    config_b = DatasetConfig(tenant_count=1, entities_per_tenant=5, seed=43)

    first = generate_dataset(tmp_path / "seed-42", config_a)
    second = generate_dataset(tmp_path / "seed-43", config_b)

    assert first.manifest.tenant_ids != second.manifest.tenant_ids
    assert first.manifest.state_checksum != second.manifest.state_checksum


def test_logical_checksum_is_stable_after_file_copy(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(tenant_count=1, entities_per_tenant=5),
    )
    source = dataset.output_dir / "tenant-000001.db"
    copied = tmp_path / "copy.db"
    shutil.copyfile(source, copied)

    assert _checksum(source) == _checksum(copied)


def test_regeneration_from_same_config_has_matching_checksum(tmp_path) -> None:  # type: ignore[no-untyped-def]
    config = DatasetConfig(tenant_count=1, entities_per_tenant=5)
    first = generate_dataset(tmp_path / "dataset", config)
    first_checksum = first.manifest.state_checksum

    shutil.rmtree(tmp_path / "dataset")
    second = generate_dataset(tmp_path / "dataset", config)

    assert second.manifest.state_checksum == first_checksum


def test_manifest_counts(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(
            tenant_count=2,
            entities_per_tenant=5,
            relationships_per_entity=2,
            metadata_entries_per_entity=3,
        ),
    )
    manifest = json.loads(dataset.manifest_path.read_text(encoding="utf-8"))

    assert manifest["schema_version"] == 1
    assert manifest["total_entity_count"] == 10
    assert manifest["total_relationship_count"] == 20
    assert manifest["total_metadata_count"] == 30
    assert len(manifest["tenant_ids"]) == 2


@pytest.mark.scale
@pytest.mark.skipif(
    not bool(int(__import__("os").environ.get("EPHEMERALDB_RUN_SCALE_TESTS", "0"))),
    reason="set EPHEMERALDB_RUN_SCALE_TESTS=1 to run scale sanity generation",
)
def test_scale_sanity_generation(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "scale",
        DatasetConfig(tenant_count=10, entities_per_tenant=1000),
    )

    assert dataset.manifest.total_entity_count == 10_000


def _sample_rows(database_path) -> dict[str, list[tuple[object, ...]]]:  # type: ignore[no-untyped-def]
    connection = sqlite3.connect(database_path)
    try:
        return {
            "tenants": connection.execute("SELECT * FROM tenants ORDER BY tenant_id").fetchall(),
            "entities": connection.execute("SELECT * FROM entities ORDER BY entity_id").fetchall(),
            "relationships": connection.execute(
                """
                SELECT *
                FROM relationships
                ORDER BY source_entity_id, target_entity_id, relationship_type
                """
            ).fetchall(),
            "metadata": connection.execute(
                "SELECT * FROM metadata ORDER BY entity_id, metadata_key"
            ).fetchall(),
        }
    finally:
        connection.close()


def _checksum(database_path) -> str:  # type: ignore[no-untyped-def]
    connection = sqlite3.connect(database_path)
    try:
        return calculate_logical_checksum(connection)
    finally:
        connection.close()
