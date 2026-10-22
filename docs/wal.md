# Explicit Logical WAL

## Fenced Meaning

In `legacy-sealed-v1`, validated WAL persistence is the durable commit record. In a fresh
`fenced-v1` namespace, the WAL object is a candidate until an immutable descriptor referencing
its exact checksum/locator becomes reachable through accepted tenant control. Single-record
candidate keys include epoch and record checksum so stale and successor sequence numbers do
not collide.

Phase 4 introduced the EphemeralDB explicit write-ahead log. This is an
application-level logical WAL, not SQLite's internal `-wal` file.

```text
SQLite internal WAL != EphemeralDB explicit logical WAL
```

SQLite's WAL is tied to SQLite page storage and a local filesystem. The project
needs a portable record that can later be batched, persisted to S3 Express One
Zone, replayed after checkpoints, and validated independently of a dead pod's
local disk.

## Record Structure

`WAL_FORMAT_VERSION = 2`. Version 1 remains readable with unchanged canonical
bytes/checksum semantics. Unsupported versions fail validation.

Each record contains:

- `wal_format_version`: explicit format version; replay rejects unsupported
  versions.
- `tenant_id`: tenant database whose logical state is mutated.
- `transaction_id`: client transaction ID used for replay idempotency.
- `sequence_no`: positive per-tenant WAL sequence.
- `owner_epoch`: reserved ownership epoch; Phase 4 local records use `0`.
- `operation_type`: source workload write operation, currently `ENTITY_DELTA`
  or `MULTI_ENTITY_TRANSACTION`.
- `operations`: ordered logical mutations that describe the committed result.
- `created_at`: deterministic logical transaction timestamp.
- `previous_record_checksum`: SHA-256 checksum of the prior tenant WAL record,
  or `null` for sequence `1`.
- `record_checksum`: SHA-256 checksum for this record.
- `request_fingerprint`: v2-only `v1:<sha256>` canonical logical request identity.

The record checksum is computed over canonical JSON for every field except
`record_checksum`.

## Logical Mutations

WAL records store logical mutations rather than SQL strings or SQLite page
bytes. Current mutation types are:

- `UPDATE_ENTITY`
- `INSERT_ENTITY_VERSION`
- `INSERT_TRANSACTION`
- `INSERT_TRANSACTION_ITEM`
- `INSERT_AUDIT_LOG`

Each mutation carries a target table, primary-key identity, and values. Entity
updates store the resulting committed state, such as `numeric_value`, `version`,
`state`, and `updated_at`, rather than relying only on a delta. This makes
replay safer: transaction-ID deduplication remains required, but a replayed
record can validate the intended resulting version instead of applying `ADD 10`
twice by accident.

One logical write transaction creates one WAL record. A multi-entity transaction
therefore has one record containing all entity updates, version rows,
transaction items, and audit rows.

## Sequence Model

Sequences are per tenant database, not global. Tenant A can have `1, 2, 3` while
tenant B independently has `1`.

The local allocator stores committed sequence state in the tenant database's
internal `_ephemeraldb_state` table:

```text
last_wal_sequence
schema_version
last_record_checksum
```

This table is system metadata, not benchmark/business data, and is excluded from
the logical state checksum.

## Hash Chain

Sequence `1` uses `previous_record_checksum = null`. Every later record stores
the checksum of the preceding tenant record. Validation rejects gaps, reordered
records, checksum tampering, and previous-checksum mismatches.

## Local Storage

`LocalWalStore` writes development WAL files as:

```text
wal-root/
  <tenant-id>/
    00000000000000000001.json
    00000000000000000002.json
```

Filenames are fixed-width decimal sequence numbers so lexicographic order
matches replay order. Local writes use a temporary file, file fsync, and atomic
rename to avoid intentionally publishing partial JSON files.

This is not an S3 mock and is not used as a fallback when `WAL_STORE=s3-express`.
The S3 Express implementation lives in the AWS durability store.

## Phase 5 S3 Write Protocol

When an S3-backed durability store is configured, the engine uses this ordering:

```text
BEGIN IMMEDIATE
validate entities and expected versions
allocate next tenant WAL sequence from _ephemeraldb_state
build and checksum explicit WAL record
persist canonical WAL bytes to S3 Express
apply logical SQLite mutations
advance _ephemeraldb_state
COMMIT SQLite
ACK
```

The SQLite write lock spans prepare, remote persist, and local apply to prevent
two local writers in the same process from preparing the same tenant/version
concurrently. This is local coordination only, not distributed ownership.

If S3 persistence fails, the local transaction rolls back and the result is not
successful. If S3 persistence succeeds but local apply fails, the remote WAL is
retained and the result reports reconciliation is required.

## Local Development Protocol

`LocalWalStore` remains available for tests and offline replay. It implements the
same one-record durability interface but returns `remote_durable = false`.
Local mode must not be interpreted as S3-backed durability.

When local WAL mode is enabled:

```text
BEGIN IMMEDIATE
validate entities and expected versions
allocate next tenant WAL sequence from _ephemeraldb_state
build and checksum explicit WAL record
write local WAL file
apply logical SQLite mutations
advance _ephemeraldb_state
COMMIT SQLite
```

Local files can exist even if local apply later fails. That is acceptable only
for local development because local WAL is not the authoritative remote store.

## Replay

Replay validates each record, checks tenant and sequence position, verifies the
hash chain against `_ephemeraldb_state`, then applies all mutations inside one
SQLite transaction. Any failed mutation rolls back the whole WAL record.

Replay is idempotent. If a record's transaction ID is already committed and the
stable committed rows match the WAL content, replay skips it. For older records,
the current `entities` row may have advanced, so replay validates historical
`entity_versions`, transaction rows, transaction items, and audit rows rather
than requiring an old entity version to still be current.

Replay rejects missing intermediate sequences, unsupported versions, checksum
corruption, invalid foreign keys, and invalid entity version progression.

## CLI

Inspect a tenant WAL:

```bash
python -m ephemeraldb.wal.cli inspect --wal-root ./generated-wal --tenant <tenant-id>
```

Validate a tenant WAL:

```bash
python -m ephemeraldb.wal.cli validate --wal-root ./generated-wal --tenant <tenant-id>
```

Replay a tenant WAL into a compatible SQLite tenant database:

```bash
python -m ephemeraldb.wal.cli replay --wal-root ./generated-wal --tenant <tenant-id> --database ./recovered/tenant-000001.db
```

## Batch Envelope

Phase 6 uses a version-1 batch envelope containing versioned WAL records. The
record-level checksum chain remains per tenant. A batch may contain records from
many tenants, but it does not introduce a global database sequence.

See `docs/wal-batching.md` for the batch format, queue semantics, and benchmark
trade-off.

## Phase 6 Boundary

This section describes the stabilized WAL foundation. Phase 7 now adds checkpoint
restore and suffix replay; see `docs/checkpointing.md`. Replay retains original
sequences and reconstructs retry identity. Recovery deduplicates identical copies,
rejects conflicting sequence/transaction mappings, and requires an explicit sealed
final sequence/head so missing trailing records cannot be mistaken for completion.
Full retained history is still scanned; no WAL garbage collection is implemented.

Transaction IDs are tenant-scoped. Fingerprints include tenant, operation type,
and ordered entity/delta/expected-version tuples. Item order is semantic;
scheduling, timing, batch IDs, and diagnostic stale-version annotations are not.
Same ID/different fingerprint is a conflict without a new PUT. The internal
request-identity schema is version 1; unsupported versions reject. Fingerprint
and outcome metadata commit atomically with local application and replay restores
them. Legacy records replay, but unverifiable client retries reject.

Phase 6 can persist either one WAL object per transaction or one cross-tenant
batch object per group of transactions to S3 Express One Zone. It still does not
provide automatic unsealed remote recovery, distributed writer
ownership, Kubernetes deployment, Aurora integration, Terraform, or chaos
testing.
