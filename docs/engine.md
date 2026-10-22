# Local Tenant Execution Engine

Phase 3 implements the local tenant database execution engine. Phase 4 adds explicit logical WAL records. Phase 5 adds optional S3 Express One Zone WAL durability. Phase 6 adds cross-tenant WAL batching.

## What Exists

```text
deterministic workload
    |
    v
local tenant execution engine
    |
    +--> SQLite WAL/internal commit
    |
    +--> explicit logical WAL file when --wal-root is enabled
    |
    +--> S3 Express WAL object or batch when WAL_STORE=s3-express
```

The engine can:

- Resolve tenant databases from a Phase 1 manifest.
- Validate paths stay inside the configured database root.
- Open and cache SQLite connections per execution thread and tenant.
- Execute all Phase 2 read operations.
- Execute `ENTITY_DELTA` writes atomically.
- Execute `MULTI_ENTITY_TRANSACTION` writes atomically within one tenant DB.
- Detect already committed duplicate transaction IDs locally.
- Return structured `ExecutionResult` values.
- Collect lightweight in-process metrics.
- Calculate Phase 1 logical checksums.
- Validate tenant databases using Phase 1 validation.
- Stream a workload artifact sequentially and write JSONL execution results.
- Emit, validate, and replay explicit logical WAL records when configured.
- Persist one WAL object per write or one cross-tenant batch object to S3 Express before ACK in S3 mode.

## What Does Not Exist Yet

```text
automatic unsealed/distributed recovery
ownership/epochs
Kubernetes
Aurora
open-loop rate-controlled traffic runner
```

SQLite's internal WAL is an implementation detail of the local engine and must not be confused with the explicit logical WAL introduced in Phase 4 or persisted to S3 Express in Phase 5/6.

## Configuration

Defaults:

```text
journal_mode = WAL
synchronous_mode = FULL
busy_timeout_ms = 5000
enable_foreign_keys = true
max_open_connections = 32
```

These are local SQLite settings. They do not provide remote durability across pod, node, or local disk loss; S3 Express WAL persistence provides the remote commit record.

## Tenant Registry

The registry is manifest-driven. It maps `tenant_id` to the expected tenant database file, such as:

```text
tenant-000001.db
tenant-000002.db
tenant-000003.db
```

Resolved paths must remain inside the configured dataset directory. Unknown tenant IDs are rejected with a structured `TENANT_NOT_FOUND` result.

## Connection Management

The connection manager owns an LRU cache of SQLite connections keyed by
execution thread and tenant database. `max_open_connections` is an idle eviction
target, not a hard concurrent bound: active connections are never evicted and
may temporarily exceed it. Benchmark worker admission is separately bounded.
It enables foreign keys, applies busy
timeout, configures SQLite WAL journal mode, and sets synchronous mode on every
opened connection.

Threading boundary: SQLite connections are not shared across active execution
threads. The cache is thread-aware so independent tenant databases can prepare
WAL records concurrently and wait on one shared Phase 6 batcher.

## Reads

Supported read operations:

- `ENTITY_LOOKUP`
- `STATE_SCAN`
- `RELATIONSHIP_EXPANSION`
- `METADATA_LOOKUP`
- `ENTITY_HISTORY`
- `COMPLEX_JOIN`
- `AGGREGATE_BY_STATE`

Read SQL reuses the Phase 1 query catalog where practical.

## Writes

`ENTITY_DELTA` without explicit WAL:

```text
BEGIN IMMEDIATE
read entity
validate expected_version if supplied
insert transaction row
update entity
append entity_versions row
insert transaction_items row
append audit_log row
COMMIT
```

`MULTI_ENTITY_TRANSACTION` validates all target entities and expected versions before applying any mutation. If one item conflicts, everything rolls back.

Successful writes insert `transactions.status = COMMITTED`. Conflicts and rolled-back failures do not persist transaction items or audit rows from the attempted mutation.

With a WAL committer configured, successful `ENTITY_DELTA` and
`MULTI_ENTITY_TRANSACTION` writes allocate a per-tenant WAL sequence, build one
explicit logical WAL record for the whole transaction, persist that record alone
or inside a durable batch, apply local SQLite state, advance
`_ephemeraldb_state`, and then acknowledge success. Conflicts and duplicate
transaction IDs do not allocate a new WAL sequence.

In `WAL_STORE=s3-express`, the S3 Express object is the authoritative durable
commit record. If remote persistence succeeds but local apply fails, the result
is not a clean success and `reconciliation_required = true`.

`WAL_PERSISTENCE_MODE=single` keeps the Phase 5 one-record-per-PUT behavior.
`WAL_PERSISTENCE_MODE=batch` uses the Phase 6 cross-tenant batcher. Same-tenant
writes remain serialized by the tenant lock; different tenants can wait in the
same batch.

## Duplicate Transactions

Before executing a write, the engine checks whether the transaction ID already
exists as COMMITTED and verifies its versioned request fingerprint. Equivalent
retries return success with `duplicate_transaction = true` without application
or a new PUT. Different or unverifiable intent returns TRANSACTION_CONFLICT.

This is local transaction-ID deduplication only. It is not the final distributed idempotency behavior.

## Error Codes

Stable local engine error codes:

- `TENANT_NOT_FOUND`
- `TENANT_NOT_READY`
- `ENTITY_NOT_FOUND`
- `TRANSACTION_CONFLICT`
- `DUPLICATE_TRANSACTION`
- `SQLITE_BUSY`
- `INTEGRITY_ERROR`
- `INVALID_OPERATION`
- `REMOTE_DURABILITY_FAILED`
- `LOCAL_APPLY_AFTER_DURABILITY_FAILED`
- `INJECTED_WAL_FAULT`
- `INTERNAL_DATABASE_ERROR`

Execution results do not expose Python tracebacks.

## Metrics

The in-process collector tracks:

- `operations_total`
- `operations_success`
- `operations_failed`
- `reads_total`
- `writes_total`
- `conflicts_total`
- `duplicates_total`
- `rows_read_total`
- `rows_written_total`
- `wal_records_created`
- `wal_bytes_created`
- `wal_validation_failures`
- `wal_replay_records`
- `wal_replay_duplicates`
- `wal_replay_failures`
- `remote_wal_puts_total`
- `remote_wal_put_failures_total`
- `remote_wal_bytes_total`
- `remote_wal_put_latency_ns`
- `remote_wal_conflicts_total`
- `wal_batches_total`
- `wal_batches_failed_total`
- `wal_batch_records_total`
- `wal_batch_bytes_total`
- `wal_batch_wait_latency_ns`
- `wal_batch_put_latency_ns`
- `queue_depth`
- `queue_wait_latency_ns`
- `local_apply_after_remote_failure_total`
- `reconciliation_required_total`

Latency uses monotonic `perf_counter_ns()` samples with count, total, min, max, and a bounded recent sample deque.

The collector is thread-safe and observational: metric failures do not rewrite
durability outcomes. Legacy engine remote counters describe operation/receipt
events, not SDK requests. Use benchmark request accounting for actual SDK calls.
Local-apply latency includes SQLite COMMIT; tenant-lock and executor-queue waits
are separately exposed. Reads share the tenant lock and serving-state gate.

## Restart And Reconciliation

S3 first use requires `--initialize-durability` for a new exclusive namespace.
Retain the dataset's `.ephemeraldb-attempts.sqlite` journal; it is not remote
authority. Restart checks complete remote sequence/checksum history before
serving. Missing journal with existing history, unresolved attempts, gaps, and
conflicting records fail closed. Local development adapters without history
inspection have only a limited receipt-verification contract, not S3 recovery.

After stopping previous writers:

```bash
python -m ephemeraldb.engine.reconcile --dataset-dir ./generated --tenant <tenant-id> --wal-store s3-express --wal-persistence-mode batch
```

For local development use `--wal-store local --wal-root ./generated-wal`.
Reconciliation issues no new WAL PUT and only clears uncertainty on positive
validated evidence. It is not distributed fencing. Phase 7 adds separate sealed-run
complete cache recovery and a permanently non-serving SEALED journal state;
Phase 6 reconciliation cannot reopen that source. See `docs/checkpointing.md`.

## Local Runner

Run:

```bash
python -m ephemeraldb.engine.runner --dataset-dir ./generated --workload ./benchmark/workloads/hot-contention.jsonl --output ./benchmark/results/local-hot-contention.jsonl
```

Run with explicit local WAL:

```bash
python -m ephemeraldb.engine.runner --dataset-dir ./generated --workload ./benchmark/workloads/hot-contention.jsonl --wal-root ./generated-wal --output ./benchmark/results/local-hot-contention.jsonl
```

Run with S3 Express WAL durability:

```bash
WAL_STORE=s3-express python -m ephemeraldb.engine.runner --dataset-dir ./generated --workload ./benchmark/workloads/hot-contention.jsonl --output ./benchmark/results/s3-write-run.jsonl
```

The runner streams workload operations sequentially, does not sleep for scheduled offsets, does not emulate concurrency, and writes one `ExecutionResult` JSON object per line.
