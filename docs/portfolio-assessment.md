# Portfolio Assessment and Recommended Direction

Reviewed: 2026-09-18. This is an assessment and proposed roadmap, not a claim
that the recommended capabilities already exist.

## Verdict

Keep this project. Its strongest potential is an evidence-backed systems case
study: when does a database-per-tenant design with disposable compute make sense,
and what correctness, latency, recovery, and operating costs does it introduce?

The current implementation is a useful research prototype. It has substantially
more technical substance than a deployment-only portfolio, but it is not yet an
end-to-end demonstration of a recoverable database service. Finishing that proof
is more valuable than adding infrastructure services immediately.

For a fresh graduate, the appropriate goal is to demonstrate architectural
judgment through working software, measured tradeoffs, and clearly bounded
claims. A project cannot substitute for production operating experience, but it
can demonstrate unusually strong preparation for backend, platform, cloud, and
distributed-systems roles.

## Evidence Reviewed

- Original project brief, Phase 1-5 requirements, Phase 6 brief, and available
  conversation context about their implementation.
- README, architecture, invariants, failure model, benchmark methodology,
  configuration, and project tooling.
- Engine execution and connection management, WAL sequence allocation and
  replay, S3 persistence, batching lifecycle, benchmark runner, and relevant tests.
- Current verification: 102 tests passed, 7 skipped; Ruff passed; strict mypy
  passed across 59 source files. Five skipped tests require explicit AWS access;
  the other two are optional scale tests.
- Two targeted local reproductions using the existing recording batch store.
  These demonstrate engine behavior; they are not AWS performance evidence.

No AWS experiment was run during this assessment. No benchmark result files were
found in benchmark/results. This checkout has no Git metadata, and no CI workflow
was found. That does not establish whether a separate remote repository exists.

## What Is Already Strong

- Deterministic data and backend-independent operation definitions support
  reproducible experiments and eventual backend comparison.
- Logical state checksums make recovery validation stronger than row counts.
- Explicit logical WAL, canonical serialization, record hash chains, and batch
  checksums make the persistence model explainable and inspectable.
- The prepare, persist, apply, acknowledge boundary is visible in code.
- Per-tenant serialization and cross-tenant batching expose a useful systems
  tradeoff: many independent tenants can share persistence costs, while a single
  hot tenant has little batching opportunity under the current locking model.
- Documentation distinguishes implemented work from planned infrastructure.
- Offline tests, strict typing, and small explicit interfaces are good foundations.

## Findings to Address First

### P1: Durable local-apply failure permits conflicting subsequent history

Location: src/ephemeraldb/engine/executor.py:475 and
src/ephemeraldb/wal/sequence.py:70.

The executor rolls back local state and returns reconciliation_required after a
durable record fails to apply. It does not prevent the next operation on that
tenant. Sequence allocation still reads the old local sequence. In batch mode,
the next record has a different batch object key, so conditional object creation
does not enforce uniqueness of the tenant sequence.

Reproduction: persist transaction A, inject AFTER_REMOTE_PERSIST failure, then
execute a different transaction B on the same tenant. Observed:

```text
A: success=false, remote_durable=true, reconciliation_required=true
B: success=true, wal_sequence=1
Remote batches: A at sequence 1, B at sequence 1, different record checksums
```

Introduce a tenant serving state and block new writes until reconciliation has
completed. Define read behavior during this interval explicitly. Startup must
also reconcile remote history before serving; an in-memory flag alone cannot
protect a restarted process. Test both single-record and batch modes and test
continuation after faults, not only the immediate error result.

A timed-out PUT can also have succeeded remotely. Treat an unresolved outcome as
unknown and reconcile it before reusing its sequence or preparing another write.
This timeout case is an architectural risk inferred from the error path, not an
AWS timeout reproduced in this review.

### P2: Transaction identity does not validate request identity

Location: src/ephemeraldb/engine/executor.py:316 and :950.

The duplicate path checks for a committed transaction ID without comparing the
new request contents. A targeted reproduction reused an acknowledged ID with a
different delta and received success with no error. This does not apply the new
delta, but silently reporting a conflicting request as successful is unsafe API
behavior. Persist a canonical request fingerprint and reject mismatched reuse.
Ensure recovery restores the information needed to give equivalent retry results.

### P1 for published performance claims: Measurement includes object cleanup

Location: src/ephemeraldb/benchmark/wal_batching.py:189-208.

The runtime ends after deleting remote objects. Single-record mode usually has
more objects to delete, so its reported TPS includes more cleanup work. This
confounds the intended batching comparison. End execution timing before teardown
and report teardown duration separately.

### P2: Metrics do not yet support cost or successful-throughput conclusions

Location: src/ephemeraldb/benchmark/wal_batching.py:61-77 and :316-340.

PUT counters count successful store calls, not all SDK request attempts. Failed
requests and retry traffic are omitted; GET count is hardcoded to zero despite
the persistence layer having an existing-object verification GET path. TPS and
transactions-per-PUT use all returned operations, including failures. An all-fail
run can misleadingly report a 100% PUT reduction.

Distinguish attempted throughput, successful goodput, durable records, new objects,
SDK calls, wire attempts, and billable usage where measurable. Count actual
requests with appropriate instrumentation and state limitations. Separate success,
conflict, unknown-outcome, and failure latency distributions.

### Other follow-up review targets

- All operations, including reads, acquire the same tenant lock. This is an
  implementation constraint to disclose and measure, not a general SQLite limit.
- Batcher fatal-exception handling clears the current batch before persistence;
  an exception outside the store-call catch can strand its pending handles. The
  outer failure handler also leaves admission open. Add worker-death tests and
  terminal-state admission rejection. This is a static finding, not reproduced here.
- max_open_connections is an eviction target, not a hard active-connection bound;
  all-active connections can exceed it. Define resource limits before density tests.
- Batch wait currently includes persistence time. Rename it or split admission,
  batch formation, upload, and local apply to avoid double-counting latency.
- One batch worker serializes uploads. Measure its saturation point before adding
  parallel upload workers, which would introduce ordering considerations.

## Reframe the Research Question

Recommended project description:

> An experimental multi-tenant database runtime evaluating the cost, latency,
> and recovery tradeoffs of disposable compute with remotely persisted logical WAL.

Use Turso as an attributed architectural inspiration, not as a claim of internal
implementation equivalence. Present Aurora as a managed alternative with different
capabilities and guarantees, not a target the experiment must outperform.

Specify a hypothetical user profile: many small, independently accessed tenant
databases, uneven activity, and predominantly tenant-local transactions. Keep the
schema business-neutral. State where the design is unsuitable: cross-tenant
transactions, a small number of intensely written databases, or requirements the
prototype cannot meet. Distinguish logical tenant separation from security isolation.

Choose explicit target latency, recovery time, dataset size, tenant density, and
budget before performance work. These are proposed acceptance targets until tested,
not service guarantees.

## Failure-Domain Boundary

S3 Express One Zone keeps data in one Availability Zone and is not designed to
survive that zone's physical loss. Aurora storage spans three Availability Zones.
Consequently, equal latency and cost numbers do not imply equal durability.

Sources checked for this assessment:

- [AWS S3 storage class durability](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html)
- [AWS Aurora storage reliability](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.StorageReliability.html)
- [AWS S3 Express placement guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-high-performance.html)

Define separate claims for process failure, complete compute/cache loss, temporary
AZ unavailability, and permanent AZ loss. S3 Standard checkpoints would protect
only the history included in successfully published checkpoints if later WAL is
lost with its AZ. Zero acknowledged-data loss across AZ destruction would require
another durability design and a separately measured cost/latency envelope.

Run representative S3 Express measurements from compute in the bucket's AZ;
laptop-to-AWS runs can validate integration but cannot establish that deployment's
latency profile. Fix compute and storage placement for comparable runs.

## Recommended Delivery Order

### 1. Correctness and measurement stabilization

Fix the findings above. Add regressions for repeated requests, conflicting IDs,
durable-but-unapplied writes, unknown remote outcomes, restart, and batch-worker
failure. Establish READY, RECONCILING, and FAILED serving states with explicit
transitions. Reconcile before admitting writes after restart.

Exit evidence: a fault matrix with immediate and next-operation outcomes, plus
benchmark tests proving cleanup exclusion and failure-aware metrics.

### 2. Checkpoint and recovery vertical slice

Implement a consistent SQLite snapshot with a bound sequence/checksum, immutable
S3 Standard checkpoint object, validated metadata, and safe publication. Do not
copy a live SQLite main file and assume its WAL-backed state is included.

Recover into a clean directory: discover a valid checkpoint, extract the tenant's
records from batch objects, validate sequence and hash chain, replay, verify, then
serve. Detect gaps and conflicting records rather than guessing a winner.

Maintain an external client acknowledgment ledger for the demonstration. Kill the
process, discard its local working directory, recover, and verify that every
acknowledged transaction exists exactly once. Resolve unknown outcomes separately.
Check logical state against an independent reference execution of resolved history.

Exit evidence: a repeatable cache-loss demo with measured recovery time, recovered
transactions, checksums, and raw results. This is the minimum compelling release.

### 3. Explain recovery cost and retention

Initially scan batch objects and report LIST/GET counts and bytes. Measure how
recovery grows with total batch history, including unrelated tenant activity.
Then justify a coarse index if the measurements require one.

Shared batches complicate garbage collection: a batch cannot be removed simply
because one tenant checkpointed it. Every contained record must be covered by the
retention policy and valid recovery state. Keep conservative retention in the
first release and state that operational limitation explicitly.

Exit evidence: checkpoint interval versus recovery-time and storage/request-cost
curves, with a documented retention rule.

### 4. Single-writer ownership with real fencing

Add two runtime processes and an authoritative ownership protocol. An epoch field
or a registry lookup before PUT is insufficient: an old owner may pause after
checking, resume after transfer, and still publish or acknowledge a write.

Write an ADR defining the linearization point, how the durability/commit path
enforces fencing, how ownership transfer accounts for in-flight commits, and how
recovery chooses authoritative history. A conditional commit manifest or a
coordinated commit service are options to evaluate, not an already chosen design.
Any added coordination requests belong in the benchmark cost and latency model.

Exit evidence: old owner pauses, new owner takes over, old owner resumes, and no
stale write can become an acknowledged authoritative commit. Lease expiry or
Kubernetes routing alone does not satisfy this test.

### 5. Controlled AWS experiments and managed baseline

First collect a small real S3 integration run. Then measure windows of 0, 1, 2, 5,
and 10 ms against true single-record mode, with many active tenants, one hot
tenant, and skew. Preserve dataset/workload hashes, seed, runtime/dependency
versions, Git revision, hardware, AZ, connection settings, and raw observations.

Use warmup and repeated measurement runs. Capture scheduled-arrival, admission,
and completion times so executor queue delays are visible. Distinguish offered
load from completed throughput; disclose closed-loop backpressure. A 100-operation
smoke test is not strong p99 evidence. Size later samples and repetitions around
variability, statistical usefulness, and the agreed cost cap.

Add a local PostgreSQL adapter to validate semantic parity inexpensively, then
Aurora. Keep transaction boundaries, conflict/retry semantics, data, query results,
and workload intent comparable. Tune indexes and pooling fairly. Separate warm
steady-state measurements from startup/scaling experiments. Local PostgreSQL
results are not Aurora performance results.

Compare absolute cost/run and estimated cost per million successful transactions
under stated utilization, including idle compute, storage, WAL, checkpointing,
recovery requests, coordination, observability, and infrastructure overhead.
Use dated regional prices or measured billing data; do not equate PUT savings with
total-cost savings. Document managed operational benefits that the prototype lacks.

Exit evidence: raw reproducible runs and a decision table showing where each
architecture is appropriate, including experiments unfavorable to EphemeralDB.

### 6. Disposable infrastructure and operating proof

Package and test on one disposable Linux compute instance before EKS. Then add
Terraform and EKS if demonstrating node replacement, placement, and operational
automation contributes to the target role. Add Spot and autoscaling only after
recovery and fencing pass their tests.

Use bounded create, run, export, destroy workflows, cleanup on failed runs, and a
post-destroy resource audit. Include least-privilege IAM, private service exposure,
temporary credentials, and an explicit tenant trust model. Avoid per-tenant metrics
labels that grow without bound. Surface queue depth, durable/applied lag, unknown
outcomes, ownership changes, recovery stages, saturation, and failed requests.

Exit evidence: a reproducible deployment, teardown record, and operator runbook
for cache loss, corruption, stalled persistence, and ownership transfer.

## Portfolio Presentation

Create a real Git repository or recover the existing remote history; preserve
meaningful incremental work from this point. Add offline CI, a reproducible
dependency environment, and a clear license decision. Benchmark results are
currently ignored: publish sanitized curated evidence in a dedicated tracked
location or release artifacts with stable links.

Build three entry points for reviewers:

- A short README: problem, implemented capabilities, architecture, one measured
  result, one failure-recovery result, limitations, and reproduction commands.
- A 5-8 minute demo: normal writes, cross-tenant batching, compute/cache loss,
  recovery verification, and a chart explaining one cost/latency tradeoff.
- A technical case study: requirements, alternatives, ADRs, experiment design,
  failure evidence, cost model, and conclusions about when not to use the design.

Useful ADRs: logical versus physical WAL; single-AZ durability contract; remote
commit versus local application; unknown-outcome reconciliation; batching versus
recovery lookup cost; checkpoint publication and shared-batch retention; fencing;
managed database versus custom operational ownership.

Do not add a business application or frontend just to make the project look larger.
Charts, terminal demos, diagrams, and reproducible artifacts are enough. Do not
claim production readiness, Aurora equivalence, or measured savings before the
evidence exists. Record real failed experiments and the decisions they changed.

## Immediate Recommendation

Next work should be a Phase 6 stabilization release covering durable-state
reconciliation, request identity, and trustworthy benchmark metrics. Then build
checkpoint-to-recovery end to end. A smaller finished system that survives cache
loss and explains its limitations is a stronger portfolio than a wider unfinished
stack. Keep distributed ownership, Aurora, and EKS as successive evidence-driven
milestones rather than simultaneous workstreams.
