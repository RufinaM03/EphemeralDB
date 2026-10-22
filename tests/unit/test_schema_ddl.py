import sqlite3

import pytest

from ephemeraldb.schema.ddl import create_schema
from ephemeraldb.schema.models import SCHEMA_VERSION


def test_create_schema_is_idempotent() -> None:
    connection = sqlite3.connect(":memory:")

    create_schema(connection)
    create_schema(connection)

    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
    }
    assert {
        "tenants",
        "entities",
        "entity_versions",
        "relationships",
        "transactions",
        "transaction_items",
        "audit_log",
        "metadata",
    }.issubset(tables)


def test_foreign_keys_are_enforced() -> None:
    connection = sqlite3.connect(":memory:")
    create_schema(connection)

    with pytest.raises(sqlite3.IntegrityError):
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
            VALUES ('entity-1', 'missing-tenant', 'ACTIVE', 1, 1, '2026-01-01T00:00:00Z',
                    '2026-01-01T00:00:00Z')
            """
        )


def test_schema_version_can_be_stored_in_tenant_metadata() -> None:
    connection = sqlite3.connect(":memory:")
    create_schema(connection)

    connection.execute(
        "INSERT INTO tenants (tenant_id, created_at, schema_version) VALUES (?, ?, ?)",
        ("tenant-a", "2026-01-01T00:00:00Z", SCHEMA_VERSION),
    )

    stored_version = connection.execute("SELECT schema_version FROM tenants").fetchone()[0]
    assert stored_version == SCHEMA_VERSION
