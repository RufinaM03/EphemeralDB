"""Tenant-local explicit WAL sequence state."""

from __future__ import annotations

import sqlite3

from ephemeraldb.core.errors import WalSequenceError
from ephemeraldb.schema.models import SCHEMA_VERSION

WAL_STATE_TABLE = "_ephemeraldb_state"


def ensure_wal_state_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS _ephemeraldb_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            schema_version INTEGER NOT NULL CHECK (schema_version > 0),
            last_wal_sequence INTEGER NOT NULL CHECK (last_wal_sequence >= 0),
            last_record_checksum TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS _ephemeraldb_transaction_wal (
            tenant_id TEXT NOT NULL,
            transaction_id TEXT NOT NULL,
            wal_sequence INTEGER NOT NULL CHECK (wal_sequence > 0),
            record_checksum TEXT NOT NULL,
            wal_object_key TEXT,
            remote_durable INTEGER NOT NULL CHECK (remote_durable IN (0, 1)),
            PRIMARY KEY (tenant_id, transaction_id)
        )
        """
    )
    if connection.execute("SELECT 1 FROM _ephemeraldb_state WHERE id=1").fetchone() is not None:
        return
    connection.execute(
        """
        INSERT OR IGNORE INTO _ephemeraldb_state (
            id,
            schema_version,
            last_wal_sequence,
            last_record_checksum
        )
        VALUES (1, ?, 0, NULL)
        """,
        (SCHEMA_VERSION,),
    )


def current_sequence_state(connection: sqlite3.Connection) -> tuple[int, str | None]:
    ensure_wal_state_table(connection)
    row = connection.execute(
        """
        SELECT last_wal_sequence, last_record_checksum
        FROM _ephemeraldb_state
        WHERE id = 1
        """
    ).fetchone()
    if row is None:
        msg = "missing WAL state row"
        raise WalSequenceError(msg)
    return int(row[0]), None if row[1] is None else str(row[1])


def allocate_next_sequence(connection: sqlite3.Connection) -> tuple[int, str | None]:
    last_sequence, last_checksum = current_sequence_state(connection)
    if last_sequence > 0 and last_checksum is None:
        msg = "WAL state is missing the previous record checksum"
        raise WalSequenceError(msg)
    return last_sequence + 1, last_checksum


def advance_sequence(
    connection: sqlite3.Connection,
    sequence_no: int,
    record_checksum: str,
) -> None:
    if sequence_no <= 0:
        msg = f"WAL sequence must be positive: {sequence_no}"
        raise WalSequenceError(msg)
    if not record_checksum:
        msg = "record checksum is required when advancing WAL state"
        raise WalSequenceError(msg)
    last_sequence, _last_checksum = current_sequence_state(connection)
    expected_sequence = last_sequence + 1
    if sequence_no != expected_sequence:
        msg = f"expected WAL sequence {expected_sequence}, got {sequence_no}"
        raise WalSequenceError(msg)
    connection.execute(
        """
        UPDATE _ephemeraldb_state
        SET last_wal_sequence = ?,
            last_record_checksum = ?,
            schema_version = ?
        WHERE id = 1
        """,
        (sequence_no, record_checksum, SCHEMA_VERSION),
    )


def record_transaction_wal(
    connection: sqlite3.Connection,
    *,
    tenant_id: str,
    transaction_id: str,
    wal_sequence: int,
    record_checksum: str,
    wal_object_key: str | None,
    remote_durable: bool,
) -> None:
    ensure_wal_state_table(connection)
    connection.execute(
        """
        INSERT INTO _ephemeraldb_transaction_wal (
            tenant_id,
            transaction_id,
            wal_sequence,
            record_checksum,
            wal_object_key,
            remote_durable
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            tenant_id,
            transaction_id,
            wal_sequence,
            record_checksum,
            wal_object_key,
            int(remote_durable),
        ),
    )


def transaction_wal_state(
    connection: sqlite3.Connection,
    *,
    tenant_id: str,
    transaction_id: str,
) -> tuple[int, str, str | None, bool] | None:
    ensure_wal_state_table(connection)
    row = connection.execute(
        """
        SELECT wal_sequence, record_checksum, wal_object_key, remote_durable
        FROM _ephemeraldb_transaction_wal
        WHERE tenant_id = ?
          AND transaction_id = ?
        """,
        (tenant_id, transaction_id),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), str(row[1]), None if row[2] is None else str(row[2]), bool(row[3])
