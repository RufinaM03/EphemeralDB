# S3 Express WAL Durability

Phase 5 introduces Amazon S3 Express One Zone as the remote durability layer for
explicit EphemeralDB WAL records.

## Why S3 Express Is Used

The Turso-inspired architecture treats local database files and NVMe/cache state
as disposable. A transaction that has been acknowledged must survive process,
pod, worker-node, and local-disk loss. S3 Express One Zone is the planned
low-latency remote WAL store for that commit history.

AWS documents S3 Express One Zone as directory-bucket storage located in a single
Availability Zone, with object APIs such as `PutObject`, `GetObject`,
`HeadObject`, and `ListObjectsV2` supported for directory buckets. The SDK
handles S3 Express session authentication for directory-bucket names.

## Commit Rule

```text
remote S3 Express PUT success
before
client ACK
```

The engine must not acknowledge a successful write until the WAL store has
returned a successful receipt for the canonical WAL object.

## Authority Model

```text
S3 Express WAL = authoritative durable commit history
Local SQLite DB = compute-local materialized state/cache
```

This is not a distributed atomic transaction between S3 and SQLite. If the S3
PUT succeeds and local SQLite apply later fails, the remote WAL remains the
durable source of truth. The engine returns a failed result with
`reconciliation_required = true` and does not delete the WAL object as
compensation.

## Object Layout

Single-record mode stores one logical WAL record per S3 object:

```text
<S3_EXPRESS_PREFIX>/
  <tenant-id>/
    00000000000000000001.json
    00000000000000000002.json
```

The object body is exactly the canonical Phase 4 WAL serialization. Object
metadata includes tenant ID, transaction ID, sequence number, WAL checksum,
format version, and owner epoch for diagnostics only. The body remains
authoritative.

Batched mode stores cross-tenant batch objects:

```text
<S3_EXPRESS_PREFIX>/
  wal-batches/
    YYYY-MM-DD/
      <batch-id>.json
```

The batch body is canonical JSON containing the full embedded WAL records plus a
record index. There is intentionally no remote per-record index object in Phase
6, because that would add one PUT per transaction and erase the batching
benefit.

## Overwrite Safety

The S3 store uses `IfNoneMatch="*"` on `PutObject` so an existing object key is
not silently overwritten. If the key already exists, the store fetches the
existing object:

- identical bytes are treated as an idempotent retry;
- different bytes raise `RemoteWalConflictError`.

This protects tenant/sequence object keys from accidental conflicting reuse.

Different batch keys do not enforce per-tenant sequence uniqueness. The local
admission journal and verified tenant chain prevent unsafe continuation under
the single-runtime assumption; neither conditional PUT nor listing is fencing.

## Retry And Timeout Settings

Botocore standard retry behavior is configured explicitly:

```text
AWS_RETRY_MODE=standard
AWS_MAX_ATTEMPTS=3
AWS_CONNECT_TIMEOUT_SECONDS=3
AWS_READ_TIMEOUT_SECONDS=10
```

SDK transport retry is not logical transaction retry. A retried PUT is still the
same WAL record and must not create a second transaction.

## Manual Prerequisites

Phase 5 assumes the directory bucket already exists.

1. AWS account credentials configured through the normal AWS provider chain.
2. Region configured, default `ap-south-1`.
3. Existing S3 Express One Zone directory bucket, for example a name ending in
   `--aps1-az1--x-s3` for Mumbai AZ ID `aps1-az1`.
4. IAM permissions for `PutObject`, `GetObject`, `HeadObject`, `ListObjectsV2`,
   `DeleteObject` for test cleanup, and any required S3 Express session
   authorization.
5. Environment variables configured from `.env.example`.

No Terraform, EKS, Aurora, Docker, or checkpoint infrastructure is provisioned
in Phase 5.

## Commands

Check configuration without creating resources:

```bash
python -m ephemeraldb.aws.check_config
```

Run the small raw PUT latency probe:

```bash
python -m ephemeraldb.aws.s3_express_probe --payload-bytes 4096 --requests 10
```

Run real AWS integration tests explicitly:

```bash
RUN_AWS_TESTS=1 pytest -m aws
```

Run the engine with S3 Express WAL durability:

```bash
export WAL_STORE=s3-express
export AWS_REGION=ap-south-1
export S3_EXPRESS_BUCKET=<directory-bucket>
export S3_EXPRESS_PREFIX=wal

python -m ephemeraldb.engine.runner \
  --dataset-dir ./generated \
  --workload ./benchmark/workloads/hot-contention.jsonl \
  --output ./benchmark/results/s3-write-run.jsonl
```

## WAL Persistence Modes

Use `WAL_PERSISTENCE_MODE=single` for the Phase 5 baseline and
`WAL_PERSISTENCE_MODE=batch` for Phase 6 batching. The engine does not silently
switch modes.

Batch tuning:

```text
WAL_BATCH_WINDOW_MS=5
WAL_BATCH_MAX_RECORDS=100
WAL_BATCH_MAX_BYTES=1048576
WAL_BATCH_MAX_QUEUE_RECORDS=10000
WAL_BATCH_FLUSH_ON_SHUTDOWN=true
```

Run the conservative real AWS comparison benchmark:

```bash
python -m ephemeraldb.benchmark.wal_batching \
  --mode single \
  --transactions 100 \
  --tenants 20

python -m ephemeraldb.benchmark.wal_batching \
  --mode batch \
  --batch-window-ms 5 \
  --transactions 100 \
  --tenants 20
```

## Phase 6 Limitations

First use of a new exclusive namespace requires `--initialize-durability` on the
runner. Retained-state restart inspects paginated single-record and batch
prefixes, validates batch/record checksums, explicitly sorts tenant sequences,
and rejects gaps or conflicting records. Phase 7 tightens inspection to 1,000
objects, 10,000 total records, 64 MiB/object and 128 MiB/scan; it is not a production
recovery index. Stop old
writers before inspection/reconciliation. Empty history cannot resolve a prior
timed-out attempt. See the reconciliation command in `docs/engine.md`.

SDK-call counters include inspection, conditional-write verification, and
cleanup. SDK-internal retries and billable usage are not measured. One Zone
durability must not be presented as multi-AZ disaster resilience.

API references: [conditional PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
and [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).
Directory-bucket listing order is not lexicographic, hence explicit sorting.

Phase 7 adds separate S3 Standard checkpoints and sealed-run suffix recovery.
Neither phase implements automatic unsealed remote recovery,
distributed writer ownership, Kubernetes, Aurora, Terraform, or load testing.
The batcher runs locally while talking to an existing S3 Express directory
bucket.
