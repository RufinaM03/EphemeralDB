"""Versioned request identity, independent of runtime scheduling and WAL layout."""

from __future__ import annotations

import hashlib
import sqlite3

from ephemeraldb.core.errors import TransactionConflictError
from ephemeraldb.loadgen.models import (
    EntityDeltaPayload,
    MultiEntityTransactionPayload,
    WorkloadOperation,
)
from ephemeraldb.wal.codec import canonical_json_bytes


def request_fingerprint(operation: WorkloadOperation) -> str:
    payload = operation.payload
    if isinstance(payload, EntityDeltaPayload):
        items = [(payload.entity_id, payload.delta, payload.expected_version)]
    elif isinstance(payload, MultiEntityTransactionPayload):
        items = [(item.entity_id, item.delta, item.expected_version) for item in payload.items]
    else:
        raise ValueError("request fingerprint requires a write")
    body = {
        "version": 1,
        "tenant_id": operation.tenant_id,
        "operation_type": operation.operation_type.value,
        "ordered_items": items,
    }
    return "v1:" + hashlib.sha256(canonical_json_bytes(body)).hexdigest()


def ensure_identity_table(connection: sqlite3.Connection) -> None:
    connection.execute("""CREATE TABLE IF NOT EXISTS _ephemeraldb_internal_versions (
        component TEXT PRIMARY KEY, version INTEGER NOT NULL)""")
    row = connection.execute(
        "SELECT version FROM _ephemeraldb_internal_versions WHERE component='request_identity'"
    ).fetchone()
    if row is not None and row[0] != 1:
        raise TransactionConflictError("unsupported request identity schema")
    if row is None:
        connection.execute(
            "INSERT INTO _ephemeraldb_internal_versions VALUES ('request_identity',1)"
        )
    connection.execute("""CREATE TABLE IF NOT EXISTS _ephemeraldb_request_identity (
        tenant_id TEXT NOT NULL, transaction_id TEXT NOT NULL,
        fingerprint TEXT NOT NULL, PRIMARY KEY (tenant_id, transaction_id))""")


def check_identity(connection: sqlite3.Connection, tenant: str, tx: str, fingerprint: str) -> None:
    ensure_identity_table(connection)
    row = connection.execute(
        "SELECT fingerprint FROM _ephemeraldb_request_identity "
        "WHERE tenant_id=? AND transaction_id=?",
        (tenant, tx),
    ).fetchone()
    if row is None or row[0] != fingerprint:
        raise TransactionConflictError(
            "transaction ID has different or unverifiable request intent"
        )


def record_identity(connection: sqlite3.Connection, tenant: str, tx: str, fingerprint: str) -> None:
    ensure_identity_table(connection)
    row = connection.execute(
        "SELECT fingerprint FROM _ephemeraldb_request_identity "
        "WHERE tenant_id=? AND transaction_id=?",
        (tenant, tx),
    ).fetchone()
    if row is not None:
        check_identity(connection, tenant, tx, fingerprint)
    else:
        connection.execute(
            "INSERT INTO _ephemeraldb_request_identity VALUES (?, ?, ?)", (tenant, tx, fingerprint)
        )
