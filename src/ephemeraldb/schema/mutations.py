"""Minimal transaction helper proving the schema supports atomic mutations."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

from ephemeraldb.core.errors import TransactionConflictError
from ephemeraldb.schema.generator import deterministic_uuid, mutation_timestamp


def apply_entity_delta(
    connection: sqlite3.Connection,
    *,
    transaction_id: str,
    entity_id: str,
    delta: int,
    expected_version: int | None = None,
    after_entity_update_hook: Callable[[], None] | None = None,
) -> None:
    """Apply one neutral entity mutation inside a single SQLite transaction."""

    try:
        connection.execute("BEGIN")
        row = connection.execute(
            """
            SELECT tenant_id, state, numeric_value, version
            FROM entities
            WHERE entity_id = ?
            """,
            (entity_id,),
        ).fetchone()
        if row is None:
            msg = f"entity does not exist: {entity_id}"
            raise KeyError(msg)

        tenant_id = str(row[0])
        state = str(row[1])
        previous_numeric_value = int(row[2])
        previous_version = int(row[3])
        if expected_version is not None and previous_version != expected_version:
            msg = f"expected version {expected_version}, found {previous_version}"
            raise TransactionConflictError(msg)

        new_numeric_value = previous_numeric_value + delta
        new_version = previous_version + 1
        sequence_no = _next_audit_sequence(connection, tenant_id)
        timestamp = mutation_timestamp(sequence_no)

        connection.execute(
            """
            UPDATE entities
            SET numeric_value = ?,
                version = ?,
                updated_at = ?
            WHERE entity_id = ?
            """,
            (new_numeric_value, new_version, timestamp, entity_id),
        )

        if after_entity_update_hook is not None:
            after_entity_update_hook()

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
            (tenant_id, entity_id, new_version, state, new_numeric_value, timestamp),
        )
        connection.execute(
            """
            INSERT INTO transactions (
                transaction_id,
                tenant_id,
                status,
                created_at,
                completed_at
            )
            VALUES (?, ?, 'COMMITTED', ?, ?)
            """,
            (transaction_id, tenant_id, timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO transaction_items (
                transaction_id,
                item_no,
                tenant_id,
                entity_id,
                delta,
                previous_version,
                new_version
            )
            VALUES (?, 1, ?, ?, ?, ?, ?)
            """,
            (transaction_id, tenant_id, entity_id, delta, previous_version, new_version),
        )
        connection.execute(
            """
            INSERT INTO audit_log (
                audit_id,
                tenant_id,
                transaction_id,
                entity_id,
                operation,
                sequence_no,
                created_at
            )
            VALUES (?, ?, ?, ?, 'ENTITY_UPDATED', ?, ?)
            """,
            (
                deterministic_uuid("mutation", tenant_id, transaction_id, sequence_no),
                tenant_id,
                transaction_id,
                entity_id,
                sequence_no,
                timestamp,
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _next_audit_sequence(connection: sqlite3.Connection, tenant_id: str) -> int:
    row = connection.execute(
        "SELECT COALESCE(MAX(sequence_no), -1) + 1 FROM audit_log WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()
    return int(row[0])
