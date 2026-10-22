"""Deterministic SQLite tenant database generator."""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from ephemeraldb.schema.checksum import calculate_logical_checksum, combine_tenant_checksums
from ephemeraldb.schema.ddl import create_schema
from ephemeraldb.schema.models import (
    ENTITY_STATES,
    GENERATION_TIMESTAMP_BASIS,
    RELATIONSHIP_TYPES,
    SCHEMA_VERSION,
    DatasetConfig,
    DatasetManifest,
    EntityRecord,
    EntityVersionRecord,
    GeneratedDataset,
    MetadataRecord,
    RelationshipRecord,
    TenantManifest,
    TenantRecord,
)

UUID_NAMESPACE = uuid.UUID("1d9d8b75-fc02-47cc-99ff-8567f5d5e1f2")
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def generate_dataset(
    output_dir: Path,
    config: DatasetConfig,
    *,
    replace: bool = False,
) -> GeneratedDataset:
    """Generate deterministic tenant SQLite databases and a manifest."""

    output_dir = Path(output_dir)
    if output_dir.exists() and replace:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tenant_manifests: list[TenantManifest] = []
    tenant_ids: list[str] = []
    tenant_checksums: list[tuple[str, str]] = []

    for tenant_index in range(config.tenant_count):
        tenant_id = deterministic_uuid(config.seed, "tenant", tenant_index)
        tenant_ids.append(tenant_id)
        filename = tenant_database_filename(tenant_index)
        database_path = output_dir / filename
        if database_path.exists() and not replace:
            msg = f"database already exists: {database_path}"
            raise FileExistsError(msg)
        if database_path.exists():
            database_path.unlink()

        checksum = generate_tenant_database(database_path, config, tenant_index, tenant_id)
        tenant_checksums.append((tenant_id, checksum))
        tenant_manifests.append(
            TenantManifest(
                tenant_id=tenant_id,
                filename=filename,
                entity_count=config.entities_per_tenant,
                relationship_count=config.entities_per_tenant * config.relationships_per_entity,
                metadata_count=config.entities_per_tenant * config.metadata_entries_per_entity,
                state_checksum=checksum,
            )
        )

    manifest = DatasetManifest(
        schema_version=SCHEMA_VERSION,
        seed=config.seed,
        tenant_count=config.tenant_count,
        entities_per_tenant=config.entities_per_tenant,
        relationships_per_entity=config.relationships_per_entity,
        metadata_entries_per_entity=config.metadata_entries_per_entity,
        initial_versions_per_entity=config.initial_versions_per_entity,
        generation_timestamp_basis=GENERATION_TIMESTAMP_BASIS,
        tenant_ids=tuple(tenant_ids),
        total_entity_count=config.tenant_count * config.entities_per_tenant,
        total_relationship_count=(
            config.tenant_count * config.entities_per_tenant * config.relationships_per_entity
        ),
        total_metadata_count=(
            config.tenant_count * config.entities_per_tenant * config.metadata_entries_per_entity
        ),
        state_checksum=combine_tenant_checksums(tenant_checksums),
        tenant_databases=tuple(tenant_manifests),
    )
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(_manifest_to_json_dict(manifest), ensure_ascii=True, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return GeneratedDataset(output_dir=output_dir, manifest_path=manifest_path, manifest=manifest)


def generate_tenant_database(
    database_path: Path,
    config: DatasetConfig,
    tenant_index: int,
    tenant_id: str,
) -> str:
    """Generate one tenant-local SQLite database and return its logical checksum."""

    connection = sqlite3.connect(database_path)
    try:
        create_schema(connection)
        populate_tenant_database(connection, config, tenant_index, tenant_id)
        return calculate_logical_checksum(connection)
    finally:
        connection.close()


def populate_tenant_database(
    connection: sqlite3.Connection,
    config: DatasetConfig,
    tenant_index: int,
    tenant_id: str,
) -> None:
    """Populate one tenant database using streaming, deterministic row generation."""

    tenant = TenantRecord(
        tenant_id=tenant_id,
        created_at=timestamp_for(tenant_index, 0),
        schema_version=SCHEMA_VERSION,
    )
    connection.execute(
        "INSERT INTO tenants (tenant_id, created_at, schema_version) VALUES (?, ?, ?)",
        (tenant.tenant_id, tenant.created_at, tenant.schema_version),
    )

    entity_ids: list[str] = []
    entity_rows: list[EntityRecord] = []
    for entity_index in range(config.entities_per_tenant):
        entity_id = deterministic_uuid(config.seed, "tenant", tenant_index, "entity", entity_index)
        entity_ids.append(entity_id)
        entity = _build_entity_record(config, tenant_index, tenant_id, entity_index, entity_id)
        entity_rows.append(entity)
        connection.execute(
            """
            INSERT INTO entities (
                entity_id,
                tenant_id,
                state,
                numeric_value,
                version,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity.entity_id,
                entity.tenant_id,
                entity.state,
                entity.numeric_value,
                entity.version,
                entity.created_at,
                entity.updated_at,
            ),
        )
        for version_record in _iter_initial_versions(
            config,
            tenant_index,
            tenant_id,
            entity_index,
            entity_id,
        ):
            connection.execute(
                """
                INSERT INTO entity_versions (
                    tenant_id,
                    entity_id,
                    version,
                    state,
                    numeric_value,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    version_record.tenant_id,
                    version_record.entity_id,
                    version_record.version,
                    version_record.state,
                    version_record.numeric_value,
                    version_record.created_at,
                ),
            )
        for metadata in _iter_metadata_records(
            config,
            tenant_index,
            tenant_id,
            entity_index,
            entity_id,
        ):
            connection.execute(
                """
                INSERT INTO metadata (
                    tenant_id,
                    entity_id,
                    metadata_key,
                    metadata_value,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    metadata.tenant_id,
                    metadata.entity_id,
                    metadata.metadata_key,
                    metadata.metadata_value,
                    metadata.updated_at,
                ),
            )

    for relationship in _iter_relationships(config, tenant_index, tenant_id, entity_ids):
        connection.execute(
            """
            INSERT INTO relationships (
                tenant_id,
                source_entity_id,
                target_entity_id,
                relationship_type,
                weight,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                relationship.tenant_id,
                relationship.source_entity_id,
                relationship.target_entity_id,
                relationship.relationship_type,
                relationship.weight,
                relationship.created_at,
            ),
        )
    connection.commit()


def deterministic_uuid(seed: object, *parts: object) -> str:
    """Return a deterministic UUID string for generated benchmark identifiers."""

    name = ":".join(("ephemeraldb", str(seed), *(str(part) for part in parts)))
    return str(uuid.uuid5(UUID_NAMESPACE, name))


def timestamp_for(tenant_index: int, ordinal: int) -> str:
    """Return a deterministic UTC timestamp for a tenant-local ordinal."""

    offset_seconds = (tenant_index * 1_000_000) + ordinal
    value = BASE_TIME + timedelta(seconds=offset_seconds)
    return value.isoformat().replace("+00:00", "Z")


def mutation_timestamp(sequence_no: int) -> str:
    """Return deterministic timestamps for Phase 1 mutation helper records."""

    value = BASE_TIME + timedelta(seconds=50_000_000 + sequence_no)
    return value.isoformat().replace("+00:00", "Z")


def tenant_database_filename(tenant_index: int) -> str:
    return f"tenant-{tenant_index + 1:06d}.db"


def _build_entity_record(
    config: DatasetConfig,
    tenant_index: int,
    tenant_id: str,
    entity_index: int,
    entity_id: str,
) -> EntityRecord:
    current_version = config.initial_versions_per_entity
    return EntityRecord(
        entity_id=entity_id,
        tenant_id=tenant_id,
        state=_state_for(config.seed, tenant_index, entity_index, current_version),
        numeric_value=_numeric_value_for(config.seed, tenant_index, entity_index, current_version),
        version=current_version,
        created_at=timestamp_for(tenant_index, _entity_ordinal(entity_index, 0)),
        updated_at=timestamp_for(tenant_index, _entity_ordinal(entity_index, current_version)),
    )


def _iter_initial_versions(
    config: DatasetConfig,
    tenant_index: int,
    tenant_id: str,
    entity_index: int,
    entity_id: str,
) -> list[EntityVersionRecord]:
    records: list[EntityVersionRecord] = []
    for version in range(1, config.initial_versions_per_entity + 1):
        records.append(
            EntityVersionRecord(
                tenant_id=tenant_id,
                entity_id=entity_id,
                version=version,
                state=_state_for(config.seed, tenant_index, entity_index, version),
                numeric_value=_numeric_value_for(config.seed, tenant_index, entity_index, version),
                created_at=timestamp_for(tenant_index, _entity_ordinal(entity_index, version)),
            )
        )
    return records


def _iter_metadata_records(
    config: DatasetConfig,
    tenant_index: int,
    tenant_id: str,
    entity_index: int,
    entity_id: str,
) -> list[MetadataRecord]:
    records: list[MetadataRecord] = []
    for metadata_index in range(config.metadata_entries_per_entity):
        records.append(
            MetadataRecord(
                tenant_id=tenant_id,
                entity_id=entity_id,
                metadata_key=f"attribute_{metadata_index:02d}",
                metadata_value=deterministic_uuid(
                    config.seed,
                    "tenant",
                    tenant_index,
                    "entity",
                    entity_index,
                    "metadata",
                    metadata_index,
                ),
                updated_at=timestamp_for(
                    tenant_index,
                    _metadata_ordinal(entity_index, metadata_index),
                ),
            )
        )
    return records


def _iter_relationships(
    config: DatasetConfig,
    tenant_index: int,
    tenant_id: str,
    entity_ids: list[str],
) -> list[RelationshipRecord]:
    records: list[RelationshipRecord] = []
    entity_count = len(entity_ids)
    for source_index, source_entity_id in enumerate(entity_ids):
        for relationship_index in range(config.relationships_per_entity):
            target_index = (source_index + relationship_index + 1) % entity_count
            records.append(
                RelationshipRecord(
                    tenant_id=tenant_id,
                    source_entity_id=source_entity_id,
                    target_entity_id=entity_ids[target_index],
                    relationship_type=RELATIONSHIP_TYPES[
                        (config.seed + source_index + relationship_index) % len(RELATIONSHIP_TYPES)
                    ],
                    weight=_relationship_weight(source_index, relationship_index),
                    created_at=timestamp_for(
                        tenant_index,
                        _relationship_ordinal(source_index, relationship_index),
                    ),
                )
            )
    return records


def _state_for(seed: int, tenant_index: int, entity_index: int, version: int) -> str:
    return ENTITY_STATES[(seed + tenant_index + entity_index + version) % len(ENTITY_STATES)]


def _numeric_value_for(seed: int, tenant_index: int, entity_index: int, version: int) -> int:
    return ((seed + 1) * (tenant_index + 1) * (entity_index + 3) * (version + 5)) % 100_000


def _relationship_weight(source_index: int, relationship_index: int) -> float:
    raw_value = ((source_index + 1) * (relationship_index + 3)) % 100
    return raw_value / 100


def _entity_ordinal(entity_index: int, version: int) -> int:
    return 10_000 + (entity_index * 100) + version


def _metadata_ordinal(entity_index: int, metadata_index: int) -> int:
    return 300_000 + (entity_index * 100) + metadata_index


def _relationship_ordinal(source_index: int, relationship_index: int) -> int:
    return 600_000 + (source_index * 100) + relationship_index


def _manifest_to_json_dict(manifest: DatasetManifest) -> dict[str, Any]:
    value = asdict(manifest)
    value["tenant_ids"] = list(manifest.tenant_ids)
    return value


def load_manifest(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(Path(path).read_text(encoding="utf-8")))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate deterministic EphemeralDB tenant databases."
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--tenants", dest="tenant_count", type=int, default=3)
    parser.add_argument("--entities-per-tenant", type=int, default=100)
    parser.add_argument("--relationships-per-entity", type=int, default=2)
    parser.add_argument("--metadata-entries-per-entity", type=int, default=3)
    parser.add_argument("--initial-versions-per-entity", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args(argv)

    config = DatasetConfig(
        tenant_count=args.tenant_count,
        entities_per_tenant=args.entities_per_tenant,
        relationships_per_entity=args.relationships_per_entity,
        metadata_entries_per_entity=args.metadata_entries_per_entity,
        initial_versions_per_entity=args.initial_versions_per_entity,
        seed=args.seed,
    )
    dataset = generate_dataset(args.output_dir, config, replace=args.replace)
    print(
        json.dumps(
            {
                "manifest": str(dataset.manifest_path),
                "tenant_count": dataset.manifest.tenant_count,
                "total_entity_count": dataset.manifest.total_entity_count,
                "state_checksum": dataset.manifest.state_checksum,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
