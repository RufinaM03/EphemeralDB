"""Standalone snapshots and separate user/internal integrity contracts."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from ephemeraldb.checkpoint.models import CheckpointError, CheckpointManifest, digest
from ephemeraldb.checkpoint.store import file_hash
from ephemeraldb.schema.checksum import CHECKSUM_TABLES, calculate_logical_checksum
from ephemeraldb.schema.ddl import create_schema
from ephemeraldb.schema.validation import validate_tenant_database
from ephemeraldb.wal.identity import ensure_identity_table
from ephemeraldb.wal.sequence import ensure_wal_state_table

INTERNAL_TABLES = (
    "_ephemeraldb_state",
    "_ephemeraldb_internal_versions",
    "_ephemeraldb_request_identity",
    "_ephemeraldb_transaction_wal",
)


def initialize_internal(connection: sqlite3.Connection) -> None:
    ensure_wal_state_table(connection)
    ensure_identity_table(connection)


def _schema(connection: sqlite3.Connection) -> list[tuple[str, str]]:
    return connection.execute(
        "SELECT type, sql FROM sqlite_master WHERE sql IS NOT NULL "
        "AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ).fetchall()


def inspect_snapshot(path: Path, tenant: str) -> dict[str, object]:
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)) as connection:
        if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise CheckpointError("SQLite integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise CheckpointError("foreign key integrity failed")
        with closing(sqlite3.connect(":memory:")) as expected:
            create_schema(expected)
            initialize_internal(expected)
            if _schema(connection) != _schema(expected):
                raise CheckpointError("unsupported or altered SQLite schema")
        if connection.execute("SELECT tenant_id,schema_version FROM tenants").fetchall() != [
            (tenant, 1)
        ]:
            raise CheckpointError("tenant/schema binding mismatch")
        for table, _, _ in CHECKSUM_TABLES:
            if connection.execute(
                f"SELECT 1 FROM {table} WHERE tenant_id<>? LIMIT 1", (tenant,)
            ).fetchone():
                raise CheckpointError("cross-tenant snapshot row")
        state = connection.execute("SELECT * FROM _ephemeraldb_state").fetchall()
        if len(state) != 1 or state[0][:2] != (1, 1):
            raise CheckpointError("invalid internal WAL state")
        sequence, head = state[0][2:]
        if (sequence == 0) != (head is None):
            raise CheckpointError("invalid sequence/head")
        if connection.execute("SELECT * FROM _ephemeraldb_internal_versions").fetchall() != [
            ("request_identity", 1)
        ]:
            raise CheckpointError("unsupported internal schema")
        rows = connection.execute(
            "SELECT w.tenant_id,w.transaction_id,w.wal_sequence,w.record_checksum,i.fingerprint "
            "FROM _ephemeraldb_transaction_wal w LEFT JOIN _ephemeraldb_request_identity i "
            "ON w.tenant_id=i.tenant_id AND w.transaction_id=i.transaction_id "
            "ORDER BY w.wal_sequence"
        ).fetchall()
        import re

        for n, row in enumerate(rows, 1):
            if row[0] != tenant or row[2] != n or not re.fullmatch(r"[0-9a-f]{64}", row[3]):
                raise CheckpointError("invalid retry WAL metadata")
            if not isinstance(row[4], str) or not re.fullmatch(r"v1:[0-9a-f]{64}", row[4]):
                raise CheckpointError("missing/unverifiable retry fingerprint")
        count = connection.execute(
            "SELECT count(*) FROM transactions WHERE status='COMMITTED'"
        ).fetchone()[0]
        identities = connection.execute(
            "SELECT count(*) FROM _ephemeraldb_request_identity"
        ).fetchone()[0]
        if len(rows) != sequence or count != sequence or identities != sequence:
            raise CheckpointError("incomplete transaction/retry coverage")
        if rows and rows[-1][3] != head:
            raise CheckpointError("retry metadata head mismatch")
        if connection.execute(
            "SELECT 1 FROM _ephemeraldb_transaction_wal w LEFT JOIN transactions t "
            "ON t.transaction_id=w.transaction_id AND t.tenant_id=w.tenant_id "
            "WHERE t.transaction_id IS NULL LIMIT 1"
        ).fetchone():
            raise CheckpointError("retry metadata references absent transaction")
        internal = {
            table: sorted(connection.execute(f"SELECT * FROM {table}").fetchall())
            for table in INTERNAL_TABLES
        }
        # A verified identical WAL copy can have another physical key after discovery.
        # Snapshot SHA covers the locator; semantic retry integrity excludes that locator.
        internal["_ephemeraldb_transaction_wal"] = sorted(
            connection.execute(
                "SELECT tenant_id,transaction_id,wal_sequence,record_checksum,remote_durable "
                "FROM _ephemeraldb_transaction_wal"
            ).fetchall()
        )
        facts: dict[str, object] = {
            "sequence": sequence,
            "head": head,
            "logical_checksum": calculate_logical_checksum(connection),
            "internal_checksum": digest(internal),
        }
    validation = validate_tenant_database(path, expected_tenant_id=tenant)
    if not validation.ok:
        raise CheckpointError(str(validation.errors))
    return facts


def validate_snapshot(path: Path, manifest: CheckpointManifest) -> dict[str, object]:
    if path.stat().st_size != manifest.size_bytes or file_hash(path) != manifest.snapshot_sha256:
        raise CheckpointError("snapshot size/hash mismatch")
    facts = inspect_snapshot(path, manifest.tenant_id)
    for field, actual in facts.items():
        if actual != getattr(manifest, field):
            raise CheckpointError(f"snapshot {field} binding mismatch")
    return facts


def backup(source: sqlite3.Connection, target: Path) -> None:
    initialize_internal(source)
    if source.in_transaction:
        raise CheckpointError("backup cannot run inside an active transaction")
    with closing(sqlite3.connect(target)) as output:
        source.backup(output)
        output.execute("PRAGMA journal_mode=DELETE")
        output.commit()
