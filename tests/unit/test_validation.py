import sqlite3

from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.schema.validation import validate_dataset_dir, validate_tenant_database


def test_validate_generated_tenant_database(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(tenant_count=1, entities_per_tenant=5),
    )
    tenant_manifest = dataset.manifest.tenant_databases[0]

    result = validate_tenant_database(
        dataset.output_dir / tenant_manifest.filename,
        expected_tenant_id=tenant_manifest.tenant_id,
        expected_entity_count=tenant_manifest.entity_count,
        expected_relationship_count=tenant_manifest.relationship_count,
        expected_metadata_count=tenant_manifest.metadata_count,
        expected_state_checksum=tenant_manifest.state_checksum,
    )

    assert result.ok
    assert result.errors == ()


def test_validate_dataset_dir_uses_manifest(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(tenant_count=2, entities_per_tenant=5),
    )

    result = validate_dataset_dir(dataset.output_dir)

    assert result["ok"] is True
    assert result["database_count"] == 2


def test_validation_reports_count_and_checksum_mismatch(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(tenant_count=1, entities_per_tenant=5),
    )
    tenant_manifest = dataset.manifest.tenant_databases[0]

    result = validate_tenant_database(
        dataset.output_dir / tenant_manifest.filename,
        expected_tenant_id=tenant_manifest.tenant_id,
        expected_entity_count=999,
        expected_state_checksum="not-the-checksum",
    )

    assert not result.ok
    assert any("expected 999 entities" in error for error in result.errors)
    assert any("expected checksum" in error for error in result.errors)


def test_validation_reports_version_inconsistency(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(tenant_count=1, entities_per_tenant=5),
    )
    database_path = dataset.output_dir / "tenant-000001.db"
    connection = sqlite3.connect(database_path)
    try:
        entity_id = connection.execute("SELECT entity_id FROM entities LIMIT 1").fetchone()[0]
        connection.execute(
            "UPDATE entities SET version = version + 1 WHERE entity_id = ?",
            (entity_id,),
        )
        connection.commit()
    finally:
        connection.close()

    result = validate_tenant_database(database_path)

    assert not result.ok
    assert any("version consistency failures" in error for error in result.errors)
