# Planned Architecture

## Phase 8 Fenced Mode

Fenced namespaces add S3 Standard tenant control plus immutable commit descriptors above S3
Express payloads. Owner transfer and accepted-head movement serialize on one conditional
control object. SQLite remains a disposable serving cache; accepted history is reconstructed
from the authority-bound checkpoint and descriptor-directed payload reads. Legacy sealed mode
retains payload-as-commit semantics and is not silently mixed with fenced namespaces.

This document describes the architecture the project plans to benchmark. It distinguishes implemented phases from future infrastructure.

## Turso-Inspired Architecture

```text
Load Generator
      |
      v
Kubernetes Service
      |
      v
Tenant Router
      |
 +----+----+
 |    |    |
 v    v    v
P1   P2   P3
 |    |    |
 local disposable NVMe/cache
      |
      v
WAL batcher
      |
      v
S3 Express One Zone
      |
 checkpoint
      |
      v
S3 Standard
```

The planned Turso-inspired path uses Kubernetes database compute pods to host many tenant databases. Users do not receive one pod each. Tenant placement is an operational concern, while correctness is defined per tenant database.

Phase 1 represents tenant databases as separate SQLite files generated from the neutral schema. Each file contains a single logical tenant row plus tenant-local entities, versions, relationships, metadata, transactions, transaction items, and audit rows.

Phase 3 adds a local tenant database execution engine:

```text
deterministic workload
      |
      v
local tenant execution engine
      |
      v
SQLite transaction commit
```

This local engine is a correctness and development layer. SQLite's internal WAL mode is configured for local SQLite behavior only. It must not be confused with the explicit EphemeralDB logical WAL introduced in Phase 4.

Phase 4 adds the explicit logical WAL format and a local filesystem store:

```text
deterministic workload
      |
      v
local tenant execution engine
      |
      +--> SQLite transaction commit
      |
      +--> explicit logical WAL record
```

The explicit WAL stores logical committed mutations, per-tenant sequence numbers,
and a SHA-256 hash chain. It is designed for later batching and remote
persistence, but Phase 4 stores records locally only. It does not upload or parse
SQLite's binary `-wal` files.

Phase 5 adds real S3 Express One Zone WAL durability:

```text
deterministic workload
      |
      v
local tenant execution engine
      |
      v
prepare explicit logical WAL record
      |
      v
S3 Express One Zone PUT
      |
      v
local SQLite apply/commit
      |
      v
ACK
```

The S3 Express WAL object is the authoritative durable commit history. Local
SQLite remains a compute-local materialized state that can lag durable WAL if
local apply fails after a successful remote PUT.

Phase 6 adds cross-tenant WAL batching:

```text
tenant A prepare WAL --+
tenant B prepare WAL --+--> canonical batch --> S3 Express PUT
tenant C prepare WAL --+
      |
      v
each tenant waits for batch durability before local apply/ACK
```

Batching is a request-latency and request-cost optimization. It does not create
one global WAL sequence and does not relax per-tenant serialization. Each tenant
keeps its own sequence and checksum chain.

Ownership is per tenant database. Multiple tenant databases can execute writes concurrently when their ownership and WAL ordering are independent. One logical tenant database must still have coordinated write ownership so two writers do not accept conflicting writes for the same owner epoch.

Local NVMe is a cache and working area, not authoritative durable state. The architecture must assume that the database process, pod, worker node, and all local cache files can disappear.

Amazon S3 Express One Zone is the implemented remote durability target for
single-record WAL objects and Phase 6 cross-tenant batch objects. A transaction
can be acknowledged only after the required single record or containing batch
has been durably persisted.

Phase 7 implements S3 Standard full-database checkpoints and bounded sealed-run
recovery. SQLite backup captures a consistent tenant prefix; immutable snapshot
and manifest objects publish in that order. A quiescent boundary establishes the
required final sequence independently of the demo ACK ledger. Recovery installs
a validated staged database only after suffix replay and final integrity checks.
Checkpoints reduce replay work; current scan-based recovery still validates all
retained WAL, including shared batches. See ADR 0002.

## Aurora Baseline

```text
Load Generator
      |
      v
Kubernetes application/benchmark pods
      |
      v
Aurora PostgreSQL Serverless v2
      |
      v
Aurora distributed storage
```

Aurora PostgreSQL Serverless v2 represents the practical managed relational baseline. It should use the same logical schema, workload definitions, benchmark harness, and reporting conventions as the Turso-inspired path.

Aurora must not be intentionally handicapped. Its managed storage, scaling model, transaction engine, and operational behavior are part of the baseline being measured.

The Phase 1 schema deliberately carries `tenant_id` on tenant-owned rows even in tenant-local SQLite files. This makes a future shared Aurora schema a direct logical mapping rather than a separate benchmark model.

## Current Boundary

The current project can generate deterministic local SQLite tenant databases,
generate backend-independent workload artifacts, execute workloads against local
tenant DB files, emit explicit local logical WAL records for development,
persist one WAL object per transaction or one cross-tenant batch object to S3
Express One Zone, validate WAL chains, and replay single-record WAL files into
compatible regenerated tenant databases. It does not create Kubernetes
resources, Terraform modules, Aurora connections, Docker images,
routing, distributed recovery, ownership coordination, or production traffic
execution.

Complete cache loss is demonstrated only for exclusive sealed runs with available
remote data. It is not distributed recovery orchestration or permanent AZ-loss
protection. The offline proof uses external filesystem authorities and never
claims AWS durability/performance from those results.
