"""Validation helpers for generated tenant SQLite databases."""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ephemeraldb.schema.checksum import calculate_logical_checksum
from ephemeraldb.schema.generator import load_manifest
from ephemeraldb.schema.models import ValidationResult


def validate_tenant_database(
    database_path: Path,
    *,
    expected_tenant_id: str | None = None,
    expected_entity_count: int | None = None,
    expected_relationship_count: int | None = None,
    expected_metadata_count: int | None = None,
    expected_state_checksum: str | None = None,
) -> ValidationResult:
    """Validate one generated tenant-local SQLite database."""

    errors: list[str] = []
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        tenant_ids = _fetch_single_column(
            connection,
            "SELECT tenant_id FROM tenants ORDER BY tenant_id",
        )
        tenant_id = tenant_ids[0] if len(tenant_ids) == 1 else None
        if len(tenant_ids) != 1:
            errors.append(f"expected exactly one tenant row, found {len(tenant_ids)}")
        if expected_tenant_id is not None and tenant_id != expected_tenant_id:
            errors.append(f"expected tenant {expected_tenant_id}, found {tenant_id}")

        foreign_key_violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_violations:
            errors.append(f"foreign key violations: {foreign_key_violations!r}")

        entity_count = _count(connection, "entities")
        relationship_count = _count(connection, "relationships")
        metadata_count = _count(connection, "metadata")
        if expected_entity_count is not None and entity_count != expected_entity_count:
            errors.append(f"expected {expected_entity_count} entities, found {entity_count}")
        if (
            expected_relationship_count is not None
            and relationship_count != expected_relationship_count
        ):
            errors.append(
                f"expected {expected_relationship_count} relationships, found {relationship_count}"
            )
        if expected_metadata_count is not None and metadata_count != expected_metadata_count:
            errors.append(
                f"expected {expected_metadata_count} metadata rows, found {metadata_count}"
            )

        _append_referential_errors(connection, errors)
        _append_version_consistency_errors(connection, errors)

        checksum = calculate_logical_checksum(connection)
        if expected_state_checksum is not None and checksum != expected_state_checksum:
            errors.append(
                f"expected checksum {expected_state_checksum}, found {checksum}"
            )
        return ValidationResult(
            tenant_id=tenant_id,
            entity_count=entity_count,
            relationship_count=relationship_count,
            metadata_count=metadata_count,
            state_checksum=checksum,
            errors=tuple(errors),
        )
    finally:
        connection.close()


def validate_dataset_dir(dataset_dir: Path) -> dict[str, Any]:
    """Validate all tenant databases referenced by a generated manifest."""

    dataset_dir = Path(dataset_dir)
    manifest = load_manifest(dataset_dir / "manifest.json")
    results: list[ValidationResult] = []
    for tenant_database in manifest["tenant_databases"]:
        result = validate_tenant_database(
            dataset_dir / tenant_database["filename"],
            expected_tenant_id=tenant_database["tenant_id"],
            expected_entity_count=tenant_database["entity_count"],
            expected_relationship_count=tenant_database["relationship_count"],
            expected_metadata_count=tenant_database["metadata_count"],
            expected_state_checksum=tenant_database["state_checksum"],
        )
        results.append(result)
    return {
        "ok": all(result.ok for result in results),
        "database_count": len(results),
        "results": [asdict(result) for result in results],
    }


def _fetch_single_column(connection: sqlite3.Connection, sql: str) -> list[str]:
    return [row[0] for row in connection.execute(sql).fetchall()]


def _count(connection: sqlite3.Connection, table_name: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _append_referential_errors(connection: sqlite3.Connection, errors: list[str]) -> None:
    relationship_orphans = connection.execute(
        """
        SELECT COUNT(*)
        FROM relationships AS r
        LEFT JOIN entities AS source
          ON source.tenant_id = r.tenant_id
         AND source.entity_id = r.source_entity_id
        LEFT JOIN entities AS target
          ON target.tenant_id = r.tenant_id
         AND target.entity_id = r.target_entity_id
        WHERE source.entity_id IS NULL
           OR target.entity_id IS NULL
        """
    ).fetchone()[0]
    if relationship_orphans:
        errors.append(f"relationship referential integrity failures: {relationship_orphans}")

    metadata_orphans = connection.execute(
        """
        SELECT COUNT(*)
        FROM metadata AS m
        LEFT JOIN entities AS e
          ON e.tenant_id = m.tenant_id
         AND e.entity_id = m.entity_id
        WHERE e.entity_id IS NULL
        """
    ).fetchone()[0]
    if metadata_orphans:
        errors.append(f"metadata referential integrity failures: {metadata_orphans}")


def _append_version_consistency_errors(connection: sqlite3.Connection, errors: list[str]) -> None:
    inconsistent_versions = connection.execute(
        """
        SELECT COUNT(*)
        FROM entities AS e
        LEFT JOIN (
            SELECT entity_id, MAX(version) AS max_version
            FROM entity_versions
            GROUP BY entity_id
        ) AS v
          ON v.entity_id = e.entity_id
        WHERE v.max_version IS NULL
           OR v.max_version <> e.version
        """
    ).fetchone()[0]
    if inconsistent_versions:
        errors.append(f"version consistency failures: {inconsistent_versions}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate generated EphemeralDB SQLite databases.")
    parser.add_argument("--dataset-dir", type=Path)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--tenant-id")
    parser.add_argument("--entities", type=int)
    parser.add_argument("--relationships", type=int)
    parser.add_argument("--metadata", type=int)
    parser.add_argument("--checksum")
    args = parser.parse_args(argv)

    if args.dataset_dir is None and args.database is None:
        parser.error("one of --dataset-dir or --database is required")
    if args.dataset_dir is not None and args.database is not None:
        parser.error("--dataset-dir and --database are mutually exclusive")

    if args.dataset_dir is not None:
        payload = validate_dataset_dir(args.dataset_dir)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if payload["ok"] else 1

    result = validate_tenant_database(
        args.database,
        expected_tenant_id=args.tenant_id,
        expected_entity_count=args.entities,
        expected_relationship_count=args.relationships,
        expected_metadata_count=args.metadata,
        expected_state_checksum=args.checksum,
    )
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
