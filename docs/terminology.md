# Terminology

## Tenant

A logical benchmark participant with its own isolated tenant database. A tenant is not a human user and does not receive dedicated infrastructure by default.

## Tenant Database

The logical database assigned to one tenant. Future phases will use a database-per-tenant model so ownership, WAL ordering, checkpointing, and recovery can be reasoned about per tenant database.

## Database Compute Pod

A Kubernetes pod that hosts database execution for one or more tenant databases. A pod is disposable compute, not durable state.

## Local Cache

Pod-local or node-local files used to accelerate database access. Local cache can be deleted at any time without violating correctness.

## Local NVMe

Ephemeral high-performance storage attached to an EC2 worker node. It is useful for cache and local working files, but it is not authoritative durable storage.

## Durable WAL

Write-ahead log records that have been persisted to the remote durability store. Phase 5 uses Amazon S3 Express One Zone for one WAL object per logical transaction.

## WAL Batch

A group of WAL records for one or more independent tenant databases persisted together to reduce remote write overhead. Batching must not break per-tenant ordering or acknowledgment rules.

## Transaction Acknowledgment

The success response returned to a client after a transaction commits. An acknowledgment is only valid after required WAL durability has been confirmed.

## Checkpoint

A full-database snapshot stored remotely so recovery does not need to replay WAL from the beginning of time.

## Checkpoint Sequence

The highest WAL sequence included in a checkpoint. Recovery from checkpoint sequence `N` must only apply WAL records with sequence greater than `N`.

## Recovery

The process of reconstructing a tenant database from the latest valid checkpoint plus durable WAL records after that checkpoint.

## RPO

Recovery Point Objective. The amount of committed data that can be lost after a failure. For acknowledged transactions in this project, the target RPO is zero.

## RTO

Recovery Time Objective. The amount of time required to restore service after a failure.

## Owner

The pod or node identity currently allowed to write for a tenant database.

## Owner Epoch

A monotonically increasing ownership generation for a tenant database. Epochs prevent stale writers from being accepted after ownership transfer.

## Split Brain

A failure mode where more than one writer believes it can safely write the same tenant database at the same ownership generation.

## Stale Writer

A previous owner that continues issuing writes after ownership has transferred to a newer epoch.

## Transaction ID

A globally unique identifier for a logical transaction. It supports idempotent retries and duplicate replay detection.

## Idempotency

The property that repeating the same logical transaction or replaying the same WAL record does not apply its effects more than once.

## Concurrency

The ability to execute multiple operations at the same time. In this project, independent tenant databases may write concurrently, while one tenant database still requires coordinated write ownership.

## Durability

The guarantee that committed data survives process, pod, node, and local-storage loss.

## Workload

A deterministic mix of benchmark operations, tenant distribution, read/write ratio, entity access pattern, and concurrency level.

## Benchmark Run

A single measured execution of a workload under a recorded configuration manifest, with warm-up, measurement, cooldown, metrics export, and cost accounting.
