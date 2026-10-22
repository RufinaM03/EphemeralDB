# WAL Batching

## Per-Tenant Fencing

A Phase 8 physical batch may span tenants, but it is not a distributed transaction. Each
record receives an independent descriptor and tenant-control CAS after upload. The descriptor
binds batch ID, checksum, count, and index. Accepted records keep a shared batch live even when
other embedded records are orphan candidates. Batching does not amortize control decisions.

Phase 6 adds cross-tenant WAL batching for S3 Express One Zone.

Batching is an I/O and request-cost optimization. It does not provide
transaction isolation, same-database write concurrency, distributed ownership,
split-brain prevention, or recovery orchestration.

## Why Batching Exists

One S3 object PUT per transaction is simple and correct, but every PUT carries
request latency and request cost. A database-per-tenant architecture can have
many independent tenant databases producing WAL at the same time. Combining
those records into one S3 Express object amortizes that PUT across multiple
logical transactions.

```text
tenant A WAL record --+
tenant B WAL record --+--> one batch object --> S3 Express PUT
tenant C WAL record --+
tenant D WAL record --+
```

The durable acknowledgment invariant remains:

```text
ACK(transaction)
=>
the batch containing that transaction is durably stored in S3 Express
```

No transaction returns success while its batch is only queued, waiting for the
window, uploading, or retrying at the SDK request level.

## Why This Fits Many Tenant DBs

Each tenant database still serializes its own writes under the tenant lock:

```text
tenant lock
prepare WAL record
enqueue in batcher
wait for batch durability
apply local SQLite mutation
ACK
```

Different tenant databases can do this concurrently. While tenant A holds its
own lock and waits, tenant B and tenant C can independently prepare their own WAL
records and join the same batch. A single hot tenant has much less batching
opportunity because the tenant lock deliberately keeps that database's writes
serial.

## Batch Model

`WAL_BATCH_FORMAT_VERSION = 1`.

Each `WalBatch` contains:

- `batch_format_version`
- `batch_id`
- `created_at`
- `records`
- `record_index`
- `record_count`
- `total_payload_bytes`
- `batch_checksum`

Every embedded WAL record remains a full Phase 4 record with its tenant ID,
transaction ID, per-tenant sequence number, owner epoch, logical mutations,
record checksum, and previous-record checksum. Batch integrity supplements the
record hash chain; it does not replace it.

The `record_index` section lists:

- `tenant_id`
- `sequence_no`
- `transaction_id`
- `record_position`
- `record_checksum`

This is the Phase 6 recovery/index decision: no remote per-record index objects
are written, because that would recreate one remote write per transaction.
Stabilization reconciliation lists batch objects, validates embedded records,
sorts per tenant, and verifies the complete chain from sequence 1.
Checkpoint-based recovery remains future work.

## Serialization And Checksums

Batches use canonical JSON:

- UTF-8
- stable key ordering
- compact separators
- no NaN
- canonical embedded WAL record dictionaries

`batch_checksum` is SHA-256 over the canonical batch payload excluding the
`batch_checksum` field. Embedded WAL record checksums are not recomputed with a
different rule.

Unsupported batch format versions are rejected during decoding.

## Flush Triggers

The batcher seals and flushes an open batch when any trigger fires:

- time window expires
- max records reached
- max byte size reached
- explicit flush
- clean shutdown

Default configuration:

```text
WAL_BATCH_WINDOW_MS=5
WAL_BATCH_MAX_RECORDS=100
WAL_BATCH_MAX_BYTES=1048576
WAL_BATCH_MAX_QUEUE_RECORDS=10000
WAL_BATCH_FLUSH_ON_SHUTDOWN=true
```

The byte limit covers encoded record payloads, not envelope/index overhead.
A record exceeding a non-empty batch starts the next batch; an oversized single
record is rejected before persistence.

## Stabilized Lifecycle

Admission tracks queued, sealed, uploading, and resolved handles. Fatal worker
failure stops admission and wakes producers, flush waiters, and pending callers.
Pre-invocation failures are rejected; failures after invoking persistence may be
UNKNOWN. Receipt-processing failure after confirmed storage preserves durability.
Optional metrics failures cannot alter the outcome.

Programmatic `wait_timeout_seconds` and `shutdown_timeout_seconds` default to 30.
A wait timeout does not cancel an upload; the tenant remains blocked. Close is
bounded and repeatable, with fatal errors repeated on subsequent close calls.
Shutdown without flushing rejects queued work; an in-flight upload may finish.
One upload worker remains intentional. SDK transport timeouts are separately
configured and a timed-out shutdown does not prove the upload has stopped.

## Queue And Backpressure

`BatchedWalCommitter` uses one background worker thread and a bounded
condition-protected queue. Producers block when `max_queue_records` in-flight
records already exist. Records are never silently dropped.

Each enqueued transaction owns a pending durability handle. The caller waits on
that handle until the batch PUT succeeds or fails.

## Failure Semantics

S3 object PUT is treated as all-or-not-confirmed. If a batch PUT fails, every
waiting transaction in that batch receives remote durability failure and none of
them proceeds to local SQLite apply.

If a batch PUT succeeds, all records in the batch are durably stored. Each
transaction then proceeds independently to local apply under its own tenant
lock. If one tenant's local apply fails after durability, that transaction
reports `reconciliation_required = true`; other tenants in the same durable
batch are not marked failed because of that local cache failure.

Batch object keys use stable batch IDs. If an expected batch object already
exists, identical bytes are treated as an idempotent retry and different bytes
raise a remote WAL conflict.

## Trade-Off

```text
larger batch window
  -> more records per PUT
  -> fewer S3 requests
  -> higher commit waiting latency
```

Phase 6 includes a benchmark CLI to compare:

```text
single record per PUT
batch --window 0
batch --window 1
batch --window 2
batch --window 5
batch --window 10
```

It also supports two important patterns:

- `many-tenants`: transactions are spread across many tenant databases.
- `single-hot-tenant`: all transactions target one tenant database.

The expected architectural result is that many independent tenants produce
better batching than one hot tenant, because same-tenant writes remain safely
serialized.
