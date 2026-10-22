"""SQLite-compatible DDL for the neutral benchmark schema."""

from __future__ import annotations

import sqlite3

from ephemeraldb.schema.models import (
    AUDIT_OPERATIONS,
    ENTITY_STATES,
    RELATIONSHIP_TYPES,
    TRANSACTION_STATUSES,
)


def _quoted_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


TABLE_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS tenants (
        tenant_id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        schema_version INTEGER NOT NULL CHECK (schema_version > 0)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS entities (
        entity_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ({_quoted_values(ENTITY_STATES)})),
        numeric_value INTEGER NOT NULL,
        version INTEGER NOT NULL CHECK (version >= 0),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (tenant_id)
            REFERENCES tenants(tenant_id)
            ON DELETE CASCADE,
        UNIQUE (tenant_id, entity_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS entity_versions (
        tenant_id TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version >= 0),
        state TEXT NOT NULL CHECK (state IN ({_quoted_values(ENTITY_STATES)})),
        numeric_value INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (entity_id, version),
        FOREIGN KEY (tenant_id, entity_id)
            REFERENCES entities(tenant_id, entity_id)
            ON DELETE CASCADE
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS relationships (
        tenant_id TEXT NOT NULL,
        source_entity_id TEXT NOT NULL,
        target_entity_id TEXT NOT NULL,
        relationship_type TEXT NOT NULL
            CHECK (relationship_type IN ({_quoted_values(RELATIONSHIP_TYPES)})),
        weight REAL NOT NULL CHECK (weight >= 0.0 AND weight <= 1.0),
        created_at TEXT NOT NULL,
        PRIMARY KEY (source_entity_id, target_entity_id, relationship_type),
        FOREIGN KEY (tenant_id, source_entity_id)
            REFERENCES entities(tenant_id, entity_id)
            ON DELETE CASCADE,
        FOREIGN KEY (tenant_id, target_entity_id)
            REFERENCES entities(tenant_id, entity_id)
            ON DELETE CASCADE,
        CHECK (source_entity_id <> target_entity_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ({_quoted_values(TRANSACTION_STATUSES)})),
        created_at TEXT NOT NULL,
        completed_at TEXT,
        FOREIGN KEY (tenant_id)
            REFERENCES tenants(tenant_id)
            ON DELETE CASCADE,
        UNIQUE (tenant_id, transaction_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transaction_items (
        transaction_id TEXT NOT NULL,
        item_no INTEGER NOT NULL CHECK (item_no >= 1),
        tenant_id TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        delta INTEGER NOT NULL,
        previous_version INTEGER NOT NULL CHECK (previous_version >= 0),
        new_version INTEGER NOT NULL CHECK (new_version >= 0),
        PRIMARY KEY (transaction_id, item_no),
        FOREIGN KEY (tenant_id, transaction_id)
            REFERENCES transactions(tenant_id, transaction_id)
            ON DELETE CASCADE,
        FOREIGN KEY (tenant_id, entity_id)
            REFERENCES entities(tenant_id, entity_id)
            ON DELETE CASCADE,
        CHECK (new_version > previous_version)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS audit_log (
        audit_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        transaction_id TEXT,
        entity_id TEXT,
        operation TEXT NOT NULL CHECK (operation IN ({_quoted_values(AUDIT_OPERATIONS)})),
        sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
        created_at TEXT NOT NULL,
        FOREIGN KEY (tenant_id)
            REFERENCES tenants(tenant_id)
            ON DELETE CASCADE,
        FOREIGN KEY (tenant_id, transaction_id)
            REFERENCES transactions(tenant_id, transaction_id)
            ON DELETE CASCADE,
        FOREIGN KEY (tenant_id, entity_id)
            REFERENCES entities(tenant_id, entity_id)
            ON DELETE CASCADE,
        UNIQUE (tenant_id, sequence_no)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS metadata (
        tenant_id TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        metadata_key TEXT NOT NULL,
        metadata_value TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (entity_id, metadata_key),
        FOREIGN KEY (tenant_id, entity_id)
            REFERENCES entities(tenant_id, entity_id)
            ON DELETE CASCADE
    )
    """,
)


INDEX_DDL: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_entities_state ON entities(state)",
    "CREATE INDEX IF NOT EXISTS idx_entities_updated_at ON entities(updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_entities_tenant_state ON entities(tenant_id, state)",
    "CREATE INDEX IF NOT EXISTS idx_entities_tenant_updated_at ON entities(tenant_id, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_entity_versions_entity_version "
    "ON entity_versions(entity_id, version)",
    "CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_relationships_type ON relationships(relationship_type)",
    "CREATE INDEX IF NOT EXISTS idx_relationships_tenant_source "
    "ON relationships(tenant_id, source_entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_relationships_tenant_type "
    "ON relationships(tenant_id, relationship_type)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_tenant_status ON transactions(tenant_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_tenant_created_at "
    "ON transactions(tenant_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_audit_log_sequence_no ON audit_log(sequence_no)",
    "CREATE INDEX IF NOT EXISTS idx_audit_log_transaction_id ON audit_log(transaction_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_log_tenant_sequence_no "
    "ON audit_log(tenant_id, sequence_no)",
    "CREATE INDEX IF NOT EXISTS idx_metadata_key ON metadata(metadata_key)",
    "CREATE INDEX IF NOT EXISTS idx_metadata_tenant_key ON metadata(tenant_id, metadata_key)",
)


def create_schema(connection: sqlite3.Connection) -> None:
    """Create the SQLite schema and indexes on an existing connection."""

    connection.execute("PRAGMA foreign_keys = ON")
    for statement in TABLE_DDL:
        connection.execute(statement)
    for statement in INDEX_DDL:
        connection.execute(statement)
    connection.commit()
