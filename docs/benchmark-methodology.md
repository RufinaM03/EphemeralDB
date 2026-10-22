# Benchmark Methodology

## Fenced Accounting

Report payload, descriptor, control, recovery, and verification requests separately. A
fenced batch comparison includes every per-tenant decision request; only payload PUTs are
amortized. Offline SQLite calls are never labeled AWS requests. SDK wire retries and billable
usage remain null unless measured externally.

This document records the planned benchmark dimensions. Phase 2 provides deterministic workload intent generation, but it does not execute traffic or measure backend performance.

## Logical Dataset

The benchmark data model is business-neutral and uses:

- tenants
- entities
- entity_versions
- relationships
- transactions
- transaction_items
- audit_log
- metadata

Generated datasets must be reproducible from a fixed seed and configuration. Logical state checksums are computed from canonical ordered table rows, not raw SQLite file bytes.

## Workload Mixes

Read-heavy:

```text
90% reads / 10% writes
```

Balanced:

```text
60% reads / 40% writes
```

Write-heavy:

```text
30% reads / 70% writes
```

Hot contention:

```text
Many transactions touching a small set of entities.
```

Multi-tenant skew:

```text
1% tenants: very active
9% tenants: moderately active
90% tenants: mostly idle
```

The Phase 2 workload generator implements these as backend-independent operation streams. Operation selection is profile-driven and weighted; it is not tied to Locust, SQLite, Aurora, Kubernetes, or any real network client.

## Concurrency Levels

Future runs should evaluate:

```text
10
50
100
250
500
1000 virtual clients
```

In Phase 2, virtual clients are identifiers assigned to scheduled operations. They describe future replay ordering but do not create threads, processes, asyncio tasks, or network calls.

## Planned Metrics

- TPS
- Successful transactions
- Failed transactions
- p50 latency
- p95 latency
- p99 latency
- Transaction retries
- Lock and conflict rates
- WAL PUT latency
- WAL batch waiting time
- Transactions per batch
- S3 PUT count
- WAL bytes
- Checkpoint duration
- Recovery duration
- WAL records replayed
- Cache hit and miss rates
- Pod count
- CPU
- Memory
- RPO
- RTO
- AWS cost per benchmark run
- Estimated cost per million transactions

## Run Discipline

Benchmarks must include:

- Deterministic dataset seed
- Warm-up period
- Measurement period
- Cooldown
- Repeated runs
- Configuration manifest
- No cherry-picking of the best run

Each benchmark run should export enough information for later comparison, including workload mix, tenant distribution, concurrency level, software version, infrastructure configuration, region, duration, and observed cost.

Phase 2 workload manifests include deterministic checksums so the exact logical traffic can be reproduced and validated before any backend execution exists.

## Phase 6 WAL Batching Experiment

Phase 6 adds a small real S3 Express comparison benchmark:

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

The benchmark reports logical transactions, success/failure counts, S3 PUT
count, transactions per PUT, bytes per PUT, runtime, TPS, commit latency, batch
wait latency, S3 PUT latency, and batch-utilization statistics.

Required batching windows are supported as command-line values:

```text
0 ms
1 ms
2 ms
5 ms
10 ms
```

The CLI supports:

- `--pattern many-tenants`
- `--pattern single-hot-tenant`

This comparison demonstrates whether batching comes from many independent tenant
databases rather than from unsafe same-tenant write concurrency.

## Stabilized Artifact Contract

Schema version 2 retains legacy field names but changes their interpretation:

- `total_runtime_ns` ends after measured outcomes, before drain and cleanup.
  Setup, drain, cleanup, and export have separate stage durations. Export times
  the first artifact serialization/write, excluding its timing-metadata rewrite.
- `tps` is successful new writes per measured second; duplicates are excluded.
  `attempted_tps` uses submitted workload count, including missing/error outcomes.
- `transactions_per_put` uses unique confirmed durable writes divided by logical
  persist calls, not all attempted transactions. Zero denominators are null.
- `s3_put_count` is the legacy name for logical persist calls. It is NOT wire
  attempts or AWS billing. Request accounting separately exposes newly created
  objects, existing-object confirmations, failed calls, and phase-scoped SDK
  PUT/GET/LIST/DELETE/HEAD invocations. Failed invocations count too.
- SDK internal retry attempts and billable usage are null/unsupported. Stored
  bytes describe newly created object bodies, not all bytes sent on the wire.
- Savings are null unless comparable, complete, successful, nonduplicate remote
  work exists. An all-failure run never earns apparent useful savings.
- Success, rejection/conflict, unknown, and other-failure latency groups are
  separate. Batch wait includes PUT time; do not add overlapping durations.
  End-to-end latency includes executor queue delay plus engine execution.

This is a submit-all thread pool with bounded workers, NOT an open-loop
rate-controlled load generator. Same-tenant reads/writes serialize and there is
one batch-upload worker. No warmup is currently performed. Many-tenant and
single-hot-tenant results must be reported separately with repetitions; local
recording stores are correctness evidence, never cloud-performance evidence.

Artifacts contain seed, workload/dataset identity, versions, revision/placement
where available (null otherwise), raw outcomes, configuration, primary and
teardown errors, cleanup state, and retained prefix. Cleanup errors do not
replace the primary failure. Failed/ambiguous runs retain WAL evidence because
an upload may still complete. Partial artifacts are written when execution and
teardown fail; setup failure before artifact context exists can still prevent
export. Benchmark dataset generation refuses destructive replacement.
