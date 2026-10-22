# Neutral Synthetic Schema

Phase 1 defines a business-neutral relational model that can later be represented as many SQLite tenant databases or as a shared Aurora PostgreSQL schema. It does not implement WAL, S3, Aurora, Kubernetes, recovery, checkpointing, ownership, or load generation.

## Tables

`tenants`

- Purpose: logical tenant metadata.
- Primary key: `tenant_id`.
- Key fields: `tenant_id`, `created_at`, `schema_version`.

`entities`

- Purpose: primary mutable benchmark records.
- Primary key: `entity_id`.
- Key fields: `tenant_id`, `state`, `numeric_value`, `version`, `created_at`, `updated_at`.
- Constraint: `state` must be one of `ACTIVE`, `INACTIVE`, `PENDING`, `ARCHIVED`.

`entity_versions`

- Purpose: append-style history for entity state.
- Primary key: `(entity_id, version)`.
- Foreign key: `(tenant_id, entity_id)` references `entities`.

`relationships`

- Purpose: directed graph-style links between entities.
- Primary key: `(source_entity_id, target_entity_id, relationship_type)`.
- Foreign keys: source and target both reference `entities`.
- Constraint: `relationship_type` is neutral and `weight` is between 0 and 1.

`transactions`

- Purpose: benchmark-level logical transactions.
- Primary key: `transaction_id`.
- Status values: `PENDING`, `COMMITTED`, `ABORTED`.

`transaction_items`

- Purpose: individual entity mutations within a logical transaction.
- Primary key: `(transaction_id, item_no)`.
- Foreign keys: transaction and entity references.

`audit_log`

- Purpose: append-only operational events.
- Primary key: `audit_id`.
- Unique ordering key: `(tenant_id, sequence_no)`.
- Operations are neutral events such as `ENTITY_UPDATED` and `TRANSACTION_COMMITTED`.

`metadata`

- Purpose: key-value attributes attached to entities.
- Primary key: `(entity_id, metadata_key)`.
- Foreign key: `(tenant_id, entity_id)` references `entities`.

## Indexes

The SQLite schema creates indexes for future read paths:

- `entities(state)`
- `entities(updated_at)`
- `entities(tenant_id, state)`
- `entities(tenant_id, updated_at)`
- `entity_versions(entity_id, version)`
- `relationships(source_entity_id)`
- `relationships(target_entity_id)`
- `relationships(relationship_type)`
- `relationships(tenant_id, source_entity_id)`
- `relationships(tenant_id, relationship_type)`
- `transactions(created_at)`
- `transactions(status)`
- `transactions(tenant_id, status)`
- `transactions(tenant_id, created_at)`
- `audit_log(sequence_no)`
- `audit_log(transaction_id)`
- `audit_log(tenant_id, sequence_no)`
- `metadata(metadata_key)`
- `metadata(tenant_id, metadata_key)`

Tenant-aware composite indexes are included even though each Phase 1 SQLite file contains one tenant. They document the future Aurora shared-schema access pattern.

## SQLite-Per-Tenant Representation

The generator writes one SQLite file per tenant:

```text
tenant-000001.db
tenant-000002.db
tenant-000003.db
```

Each database contains exactly one row in `tenants`. Tenant identity is stored inside the database and validated independently of the filename.

## Future Aurora Representation

Aurora will eventually store many tenants in one PostgreSQL cluster. The Phase 1 schema carries `tenant_id` on tenant-owned rows so the logical mapping remains equivalent. PostgreSQL-specific DDL, migrations, partitioning, and Aurora connection code are intentionally not implemented in Phase 1.

## Deterministic Generation

Dataset generation is driven by `DatasetConfig`:

```text
tenant_count = 3
entities_per_tenant = 100
relationships_per_entity = 2
metadata_entries_per_entity = 3
initial_versions_per_entity = 1
seed = 42
```

Identifiers use UUIDv5 with a fixed namespace and names derived from the seed plus logical indexes. Wall-clock time is never used. Timestamps start from:

```text
2026-01-01T00:00:00Z
```

and deterministic offsets produce later timestamps.

## Logical Checksum

`calculate_logical_checksum(connection)` computes a SHA-256 checksum from canonical table rows:

1. Tables are processed in a fixed order.
2. Rows are selected in deterministic primary-key order.
3. Values are serialized as compact JSON tuples.
4. The raw SQLite database file is never hashed.

Phase 1 includes all current table contents in the checksum, including `transactions`, `transaction_items`, and `audit_log`.

## Manifest

Every generated dataset includes `manifest.json` with schema version, seed, dataset dimensions, timestamp basis, tenant IDs, row totals, per-tenant checksums, and the combined dataset checksum.

## Minimal Mutation Helper

`apply_entity_delta(...)` proves the schema can support a real atomic multi-table update. It updates one entity, appends an entity version, records a committed transaction, writes a transaction item, and inserts an audit event inside one SQLite transaction.

This helper is not the final database engine and does not implement WAL durability or acknowledgment rules.
