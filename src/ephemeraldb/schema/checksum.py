"""Deterministic logical checksums for SQLite tenant databases."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable

CHECKSUM_TABLES: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    ("tenants", ("tenant_id",), ("tenant_id", "created_at", "schema_version")),
    (
        "entities",
        ("entity_id",),
        ("entity_id", "tenant_id", "state", "numeric_value", "version", "created_at", "updated_at"),
    ),
    (
        "entity_versions",
        ("entity_id", "version"),
        ("tenant_id", "entity_id", "version", "state", "numeric_value", "created_at"),
    ),
    (
        "relationships",
        ("source_entity_id", "target_entity_id", "relationship_type"),
        (
            "tenant_id",
            "source_entity_id",
            "target_entity_id",
            "relationship_type",
            "weight",
            "created_at",
        ),
    ),
    (
        "transactions",
        ("transaction_id",),
        ("transaction_id", "tenant_id", "status", "created_at", "completed_at"),
    ),
    (
        "transaction_items",
        ("transaction_id", "item_no"),
        (
            "transaction_id",
            "item_no",
            "tenant_id",
            "entity_id",
            "delta",
            "previous_version",
            "new_version",
        ),
    ),
    (
        "audit_log",
        ("sequence_no", "audit_id"),
        (
            "audit_id",
            "tenant_id",
            "transaction_id",
            "entity_id",
            "operation",
            "sequence_no",
            "created_at",
        ),
    ),
    (
        "metadata",
        ("entity_id", "metadata_key"),
        ("tenant_id", "entity_id", "metadata_key", "metadata_value", "updated_at"),
    ),
)


def calculate_logical_checksum(connection: sqlite3.Connection) -> str:
    """Hash all logical table rows in deterministic order.

    Phase 1 includes every table in the checksum, including operational tables
    such as transactions and audit_log. The checksum intentionally does not hash
    raw SQLite bytes because page layout is not a stable logical representation.
    """

    digest = hashlib.sha256()
    for table_name, order_columns, selected_columns in CHECKSUM_TABLES:
        digest.update(f"table:{table_name}\n".encode())
        for row in _iter_table_rows(connection, table_name, order_columns, selected_columns):
            encoded = json.dumps(row, ensure_ascii=True, separators=(",", ":"))
            digest.update(encoded.encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


def combine_tenant_checksums(tenant_checksums: Iterable[tuple[str, str]]) -> str:
    """Combine per-tenant checksums into one deterministic dataset checksum."""

    digest = hashlib.sha256()
    for tenant_id, checksum in sorted(tenant_checksums):
        digest.update(tenant_id.encode("utf-8"))
        digest.update(b":")
        digest.update(checksum.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _iter_table_rows(
    connection: sqlite3.Connection,
    table_name: str,
    order_columns: tuple[str, ...],
    selected_columns: tuple[str, ...],
) -> Iterable[tuple[object, ...]]:
    select_clause = ", ".join(selected_columns)
    order_clause = ", ".join(order_columns)
    cursor = connection.execute(f"SELECT {select_clause} FROM {table_name} ORDER BY {order_clause}")
    for row in cursor.fetchall():
        yield tuple(row)
