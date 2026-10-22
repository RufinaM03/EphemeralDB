"""Deterministic replay of explicit logical WAL records into SQLite."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass

from ephemeraldb.core.errors import WalChainError, WalReplayError, WalSequenceError
from ephemeraldb.wal.identity import record_identity
from ephemeraldb.wal.models import (
    WalMutation,
    WalMutationType,
    WalRecord,
    WalScalar,
    fields_to_dict,
)
from ephemeraldb.wal.sequence import (
    advance_sequence,
    current_sequence_state,
    ensure_wal_state_table,
    record_transaction_wal,
    transaction_wal_state,
)
from ephemeraldb.wal.validation import validate_record


@dataclass(frozen=True, slots=True)
class WalReplaySummary:
    records_applied: int
    duplicates_skipped: int
    final_sequence: int


def replay_wal(
    connection: sqlite3.Connection,
    records: Iterable[WalRecord],
    *,
    tenant_id: str | None = None,
    from_sequence: int | None = None,
    remote_durable: bool = False,
    object_keys: dict[int, str] | None = None,
) -> WalReplaySummary:
    connection.execute("PRAGMA foreign_keys = ON")
    ensure_wal_state_table(connection)
    if connection.in_transaction:
        connection.commit()

    applied = 0
    duplicates = 0
    expected_tenant_id = tenant_id
    for record in records:
        if from_sequence is not None and record.sequence_no < from_sequence:
            continue
        if expected_tenant_id is None:
            expected_tenant_id = record.tenant_id
        validate_record(record, expected_tenant_id=expected_tenant_id)
        current_sequence, current_checksum = current_sequence_state(connection)

        if record.sequence_no <= current_sequence:
            checksum_conflicts_with_db = (
                record.sequence_no == current_sequence
                and record.record_checksum != current_checksum
            )
            if checksum_conflicts_with_db:
                msg = f"WAL checksum for sequence {record.sequence_no} conflicts with DB state"
                raise WalReplayError(msg)
            if _record_already_applied(connection, record):
                _repair_duplicate(connection, record, remote_durable, object_keys, advance=False)
                duplicates += 1
                continue
            msg = f"WAL sequence {record.sequence_no} is older than DB state but not applied"
            raise WalReplayError(msg)

        expected_sequence = current_sequence + 1
        if record.sequence_no != expected_sequence:
            msg = f"expected WAL sequence {expected_sequence}, got {record.sequence_no}"
            raise WalSequenceError(msg)
        if record.previous_record_checksum != current_checksum:
            msg = f"WAL previous checksum mismatch at sequence {record.sequence_no}"
            raise WalChainError(msg)

        if _record_already_applied(connection, record):
            _repair_duplicate(connection, record, remote_durable, object_keys, advance=True)
            duplicates += 1
            continue

        try:
            connection.execute("BEGIN IMMEDIATE")
            for mutation in record.operations:
                _apply_mutation(connection, mutation)
            assert record.record_checksum is not None
            advance_sequence(connection, record.sequence_no, record.record_checksum)
            _restore_retry_state(connection, record, remote_durable, object_keys)
            connection.commit()
        except WalReplayError:
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            msg = f"failed to replay WAL sequence {record.sequence_no}: {exc}"
            raise WalReplayError(msg) from exc
        except Exception:
            connection.rollback()
            raise
        applied += 1

    final_sequence, _final_checksum = current_sequence_state(connection)
    return WalReplaySummary(
        records_applied=applied,
        duplicates_skipped=duplicates,
        final_sequence=final_sequence,
    )


def _restore_retry_state(
    connection: sqlite3.Connection,
    record: WalRecord,
    remote: bool,
    keys: dict[int, str] | None,
) -> None:
    if record.request_fingerprint is not None:
        record_identity(
            connection, record.tenant_id, record.transaction_id, record.request_fingerprint
        )
    existing = transaction_wal_state(
        connection, tenant_id=record.tenant_id, transaction_id=record.transaction_id
    )
    if existing is not None:
        if existing[:2] != (record.sequence_no, record.record_checksum):
            raise WalReplayError("retry metadata conflicts with WAL")
        if remote:
            connection.execute(
                "UPDATE _ephemeraldb_transaction_wal SET remote_durable=1, "
                "wal_object_key=COALESCE(?,wal_object_key) WHERE tenant_id=? AND transaction_id=?",
                (
                    None if keys is None else keys.get(record.sequence_no),
                    record.tenant_id,
                    record.transaction_id,
                ),
            )
        return
    assert record.record_checksum is not None
    record_transaction_wal(
        connection,
        tenant_id=record.tenant_id,
        transaction_id=record.transaction_id,
        wal_sequence=record.sequence_no,
        record_checksum=record.record_checksum,
        wal_object_key=None if keys is None else keys.get(record.sequence_no),
        remote_durable=remote,
    )


def _repair_duplicate(
    connection: sqlite3.Connection,
    record: WalRecord,
    remote: bool,
    keys: dict[int, str] | None,
    *,
    advance: bool,
) -> None:
    try:
        connection.execute("BEGIN IMMEDIATE")
        assert record.record_checksum is not None
        if advance:
            advance_sequence(connection, record.sequence_no, record.record_checksum)
        _restore_retry_state(connection, record, remote, keys)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _apply_mutation(connection: sqlite3.Connection, mutation: WalMutation) -> None:
    values = fields_to_dict(mutation.values)
    if mutation.mutation_type == WalMutationType.UPDATE_ENTITY:
        _apply_update_entity(connection, values)
        return
    if mutation.mutation_type == WalMutationType.INSERT_ENTITY_VERSION:
        _insert_row(
            connection,
            "entity_versions",
            ("tenant_id", "entity_id", "version", "state", "numeric_value", "created_at"),
            values,
        )
        return
    if mutation.mutation_type == WalMutationType.INSERT_TRANSACTION:
        _insert_row(
            connection,
            "transactions",
            ("transaction_id", "tenant_id", "status", "created_at", "completed_at"),
            values,
        )
        return
    if mutation.mutation_type == WalMutationType.INSERT_TRANSACTION_ITEM:
        _insert_row(
            connection,
            "transaction_items",
            (
                "transaction_id",
                "item_no",
                "tenant_id",
                "entity_id",
                "delta",
                "previous_version",
                "new_version",
            ),
            values,
        )
        return
    if mutation.mutation_type == WalMutationType.INSERT_AUDIT_LOG:
        _insert_row(
            connection,
            "audit_log",
            (
                "audit_id",
                "tenant_id",
                "transaction_id",
                "entity_id",
                "operation",
                "sequence_no",
                "created_at",
            ),
            values,
        )


def _apply_update_entity(
    connection: sqlite3.Connection,
    values: dict[str, WalScalar],
) -> None:
    tenant_id = _str_value(values, "tenant_id")
    entity_id = _str_value(values, "entity_id")
    new_version = _int_value(values, "version")
    row = connection.execute(
        """
        SELECT version
        FROM entities
        WHERE tenant_id = ?
          AND entity_id = ?
        """,
        (tenant_id, entity_id),
    ).fetchone()
    if row is None:
        msg = f"WAL update references missing entity: {entity_id}"
        raise WalReplayError(msg)
    current_version = int(row[0])
    if new_version != current_version + 1:
        msg = (
            f"invalid WAL version progression for {entity_id}: "
            f"current {current_version}, record {new_version}"
        )
        raise WalReplayError(msg)
    cursor = connection.execute(
        """
        UPDATE entities
        SET state = ?,
            numeric_value = ?,
            version = ?,
            updated_at = ?
        WHERE tenant_id = ?
          AND entity_id = ?
        """,
        (
            _str_value(values, "state"),
            _int_value(values, "numeric_value"),
            new_version,
            _str_value(values, "updated_at"),
            tenant_id,
            entity_id,
        ),
    )
    if cursor.rowcount != 1:
        msg = f"WAL update affected {cursor.rowcount} rows for entity {entity_id}"
        raise WalReplayError(msg)


def _insert_row(
    connection: sqlite3.Connection,
    table_name: str,
    columns: tuple[str, ...],
    values: dict[str, WalScalar],
) -> None:
    column_sql = ", ".join(columns)
    placeholder_sql = ", ".join("?" for _column in columns)
    connection.execute(
        f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholder_sql})",
        tuple(values[column] for column in columns),
    )


def _record_already_applied(connection: sqlite3.Connection, record: WalRecord) -> bool:
    if not _transaction_row_matches(connection, record):
        return False
    return all(
        _mutation_matches_committed_state(connection, mutation) for mutation in record.operations
    )


def _transaction_row_matches(connection: sqlite3.Connection, record: WalRecord) -> bool:
    row = connection.execute(
        """
        SELECT tenant_id, status
        FROM transactions
        WHERE transaction_id = ?
        """,
        (record.transaction_id,),
    ).fetchone()
    return row is not None and str(row[0]) == record.tenant_id and str(row[1]) == "COMMITTED"


def _mutation_matches_committed_state(
    connection: sqlite3.Connection,
    mutation: WalMutation,
) -> bool:
    values = fields_to_dict(mutation.values)
    if mutation.mutation_type == WalMutationType.UPDATE_ENTITY:
        return _update_matches_entity_history(connection, values)
    if mutation.mutation_type == WalMutationType.INSERT_ENTITY_VERSION:
        return _row_matches(
            connection,
            "entity_versions",
            ("tenant_id", "entity_id", "version", "state", "numeric_value", "created_at"),
            "tenant_id = ? AND entity_id = ? AND version = ?",
            (
                values["tenant_id"],
                values["entity_id"],
                values["version"],
            ),
            values,
        )
    if mutation.mutation_type == WalMutationType.INSERT_TRANSACTION:
        return _row_matches(
            connection,
            "transactions",
            ("transaction_id", "tenant_id", "status", "created_at", "completed_at"),
            "tenant_id = ? AND transaction_id = ?",
            (values["tenant_id"], values["transaction_id"]),
            values,
        )
    if mutation.mutation_type == WalMutationType.INSERT_TRANSACTION_ITEM:
        return _row_matches(
            connection,
            "transaction_items",
            (
                "transaction_id",
                "item_no",
                "tenant_id",
                "entity_id",
                "delta",
                "previous_version",
                "new_version",
            ),
            "transaction_id = ? AND item_no = ?",
            (values["transaction_id"], values["item_no"]),
            values,
        )
    if mutation.mutation_type == WalMutationType.INSERT_AUDIT_LOG:
        return _row_matches(
            connection,
            "audit_log",
            (
                "audit_id",
                "tenant_id",
                "transaction_id",
                "entity_id",
                "operation",
                "sequence_no",
                "created_at",
            ),
            "audit_id = ?",
            (values["audit_id"],),
            values,
        )


def _update_matches_entity_history(
    connection: sqlite3.Connection,
    values: dict[str, WalScalar],
) -> bool:
    row = connection.execute(
        """
        SELECT state, numeric_value, created_at
        FROM entity_versions
        WHERE tenant_id = ?
          AND entity_id = ?
          AND version = ?
        """,
        (values["tenant_id"], values["entity_id"], values["version"]),
    ).fetchone()
    if row is None:
        return False
    return (
        str(row[0]) == values["state"]
        and int(row[1]) == values["numeric_value"]
        and str(row[2]) == values["updated_at"]
    )


def _row_matches(
    connection: sqlite3.Connection,
    table_name: str,
    columns: tuple[str, ...],
    where_clause: str,
    parameters: tuple[WalScalar, ...],
    values: dict[str, WalScalar],
) -> bool:
    row = connection.execute(
        f"SELECT {', '.join(columns)} FROM {table_name} WHERE {where_clause}",
        parameters,
    ).fetchone()
    if row is None:
        return False
    return all(row[index] == values[column] for index, column in enumerate(columns))


def _str_value(values: dict[str, WalScalar], name: str) -> str:
    value = values[name]
    if not isinstance(value, str):
        msg = f"WAL field {name} must be a string"
        raise WalReplayError(msg)
    return value


def _int_value(values: dict[str, WalScalar], name: str) -> int:
    value = values[name]
    if type(value) is not int:
        msg = f"WAL field {name} must be an integer"
        raise WalReplayError(msg)
    return value
