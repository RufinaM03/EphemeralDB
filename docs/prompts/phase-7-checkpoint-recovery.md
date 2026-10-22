# Implementation Prompt: Phase 7 Checkpoints and a Bounded Recovery Proof

You are continuing **EphemeralDB Benchmark** after Phase 6 stabilization.

Implement this prompt end to end in the current workspace. Inspect actual code
and prior reports first; do not assume an earlier prompt was successfully completed.
Preserve user changes, existing workload semantics, and tested durability contracts.

## 1. Purpose and Deliberate Phase Boundary

Build consistent full-database checkpoints stored in **S3 Standard** and a bounded
single-runtime restore-and-WAL-replay path. Demonstrate that complete loss of a
tenant's local working state does not lose acknowledged transactions within the
declared failure model.

This deliberately extends checkpoint-only Phase 7 with the smallest complete
recovery demonstration. Full distributed recovery orchestration, ownership
transfer/fencing, routing, multi-pod operation, and node failover remain Phase 8
or later. Do not claim that this phase makes concurrent owners safe.

Keep S3 Express One Zone as the WAL store, in both single-record and batch modes.
Use a separate configured general-purpose S3 bucket with STANDARD storage for
checkpoints. Buckets are prerequisites; do not create infrastructure in this task.

Out of scope: EKS, Terraform, Aurora, autoscaling, Spot deployment, production
backup management, automatic WAL deletion, frontend, and cross-tenant transactions.

## 2. Prerequisite Audit

Read docs/prompts/phase-6-stabilization.md, its implementation report if present,
docs/portfolio-assessment.md, README, WAL/batching docs, invariants, failure model,
and current engine, schema, replay, durability, and S3 code.

Verify these gates before relying on them:

- Durable-but-unapplied and unknown outcomes block unsafe tenant continuation.
- Restart admission does not blindly trust a stale local sequence.
- Request fingerprints/idempotency metadata survive WAL replay.
- Worker shutdown/failure cannot strand callers.
- Benchmark execution timing and outcome accounting are trustworthy.

If a gate is missing, fix narrowly scoped prerequisite defects with regressions
and identify them in the report. Do not silently redefine safety to bypass the
gate. If stabilization is substantially absent, report the exact blocker instead
of layering checkpoints on known unsafe behavior.

## 3. State the Correctness Contract

For a published checkpoint at sequence N:

- It contains the complete tenant state after exactly the applied prefix through N.
- Its embedded WAL state identifies N and the corresponding record hash-chain head.
- It includes internal state required for safe client retries and future writes.
- Metadata and bytes identify the same tenant, schema, format, and sequence.
- Recovery applies only validated records with sequence greater than N.
- READY is reached only after snapshot, WAL chain, and retry state are validated.

Sequences are per tenant. A checkpoint cannot include an arbitrary subset of a
transaction, nor can its metadata name a newer sequence than its snapshot.
Unknown or unapplied durable transactions must not be hidden by a checkpoint.

Define process crash, local cache loss, temporary remote unavailability, and
permanent AZ loss separately. S3 Standard checkpoint durability does not upgrade
later S3 Express WAL to multi-AZ durability. Zero acknowledged loss in the demo
applies to compute/cache destruction while required remote data remains available.

## 4. Small Architecture and ADR

Prefer a small checkpoint package separating snapshot creation, metadata/codec,
publication, validation, and restore, plus an S3 Standard adapter. Reuse existing
WAL decoding/replay, logical checksums, tenant registry, and serving-state gates.
Use a bounded recovery orchestrator rather than embedding all logic in a CLI.

Evolve the existing CheckpointMetadata/CheckpointStore interfaces where useful.
Do not force large files through a bytes-only API merely because an old placeholder
protocol uses bytes. Stream files and hash incrementally where practical.

Write an ADR explaining snapshot consistency, publication commit point, checkpoint
selection, replay boundary, retry-state preservation, exclusive-run assumptions,
and why shared-batch garbage collection is deferred. Then implement that decision.

## 5. Consistent SQLite Snapshot

Use the supported SQLite backup API or another justified consistent snapshot
mechanism. Do not copy only a live database's main file and ignore its SQLite WAL.
Check current official SQLite/Python documentation when choosing API behavior.

For the initial implementation, coordinating with the existing per-tenant lock
during snapshot capture is acceptable. Do not hold an active write transaction in
a way that deadlocks backup. Capture a standalone immutable local snapshot, then
release the tenant lock before remote upload. Other tenants must remain usable.

Derive sequence N, chain head, schema version, tenant identity, and logical
checksum from the captured snapshot itself. Do not sample live metadata after the
snapshot and assume it still describes the same state.

Validate SQLite integrity, expected schema, foreign keys, tenant identity, and
internal WAL/retry state. The snapshot must open correctly without external
-wal/-shm sidecars. Temporary artifacts need deterministic cleanup on failure.

Checkpoint only tenants with a verified consistent applied prefix. Reject or
reconcile RECONCILING/FAILED tenants according to the Phase 6 contract.

## 6. Versioned Checkpoint Metadata

Define a strongly typed, versioned manifest with at least:

- Checkpoint ID, checkpoint format version, and schema/internal-state versions.
- Tenant ID and dataset/run namespace identity.
- Applied checkpoint sequence and last WAL record checksum (null at sequence zero).
- Immutable snapshot object key, stored byte count, and snapshot SHA-256.
- Logical user-state checksum, with a defined checksum schema/version.
- Integrity coverage for internal sequence and retry metadata.
- Creation timestamp, timezone-aware, for audit only rather than sequence ordering.
- WAL namespace/persistence format context needed to discover subsequent records.

Use canonical serialization and a manifest checksum excluding its own checksum
field. Validate every required field and reject unsupported future versions.
Do not treat ETag as the SHA-256 of file content. Do not treat SQLite raw-byte
hash equality as logical database equivalence.

Do not extend the user-state checksum with internal tables silently. Define a
separate internal-state digest if needed and document compatibility explicitly.
Handle sequence zero without inventing a preceding record checksum.

## 7. Immutable Publication Protocol

Use an isolated layout such as:

```text
<prefix>/<run-or-dataset-id>/checkpoints/<tenant>/<sequence>/<checkpoint-id>/snapshot.db
<prefix>/<run-or-dataset-id>/checkpoints/<tenant>/<sequence>/<checkpoint-id>/manifest.json
```

Adapt the layout to existing safe tenant-ID encoding. Never accept arbitrary paths
from untrusted metadata outside the configured namespace.

Recommended publication:

1. Produce and validate the immutable local snapshot and manifest.
2. Upload snapshot bytes and confirm storage, with checksum/size verification.
3. Publish the immutable manifest only after snapshot durability is confirmed.
4. Treat the valid published manifest plus validated referenced snapshot as the
   recovery candidate. An uploaded snapshot alone is an orphan, not a checkpoint.

S3 offers no atomic two-object transaction; make incomplete publication safe.
Retries of the same attempt use identical keys and contents. Identical existing
content can succeed idempotently; conflicting content must fail explicitly.
Unknown upload outcomes require verification, not blind overwrites.

Prefer discovering immutable manifests over a mutable latest pointer in this
phase. If adding a latest pointer, make it an optimization with safe monotonic
conditional updates and a documented recovery fallback. Never select latest by
lexicographic listing order, upload completion time, or wall clock alone.

Verify currently supported S3 APIs and conditional-write semantics against official
AWS docs. Reuse SDK credential handling and bounded retry/timeouts. Do not add
credentials to source or build a shell-based AWS adapter.

## 8. Selecting and Restoring a Checkpoint

Implement candidate listing with pagination. Choose the highest valid applicable
sequence, with deterministic handling of equivalent candidates at the same
sequence. Conflicting candidates at one sequence need an integrity error; do not
resolve conflicting history by timestamp.

Validate the manifest, object size/hash, database integrity, tenant, schema,
embedded sequence/hash head, logical checksum, and internal retry state before
using a checkpoint. Distinguish corruption/not-found from authorization failure
or service outage. Do not silently fallback on access denial as though it were
corruption.

Default to explicit failure on a corrupted newest candidate. An explicit fallback
policy may use an older valid checkpoint only when the complete necessary WAL is
available and validates; record the skipped candidate and reason. Never silently
serve an older database without replaying the required suffix.

Restore into a staging directory, not the live file. No normal engine connection
may access an incomplete restore. Close handles before final installation; use a
same-filesystem replacement strategy compatible with Windows and Linux. Do not
overwrite an active or unrelated dataset. Cleanup staging on failure while
preserving diagnostic information and any previously valid live state.

## 9. Bounded WAL Discovery and Replay

Provide the minimum discovery needed for single-record and cross-tenant batch
objects in an explicitly configured dataset/run namespace.

- List all required pages; listing order is not WAL order.
- Verify each relevant object's codec/integrity and extract records for the tenant.
- Validate embedded record indexes rather than trusting them blindly.
- Sort by per-tenant sequence and validate the chain anchored at checkpoint N.
- Apply sequences strictly greater than N with no silent gaps.
- Deduplicate identical record copies; reject conflicting tenant/sequence or
  transaction identity mappings.
- Preserve/restablish request fingerprints, transaction outcome metadata, and
  local sequence state so retries and the first new write behave correctly.
- Never allocate fresh sequence numbers during replay.

Do not accept a complete-looking prefix as proof that a required trailing record
is absent. Define how the expected durable recovery boundary is established.
An external acknowledgment ledger can serve as the demo's independent oracle,
but must not be the only source of information used by the recovery algorithm.

Bounded assumption: one runtime has exclusive control of the namespace and all
previous writers are stopped. A multi-page scan is not a distributed snapshot
while writers are publishing. A crashed process can leave remote requests whose
outcome is still unresolved. Carry Phase 6 uncertainty rules into recovery; if a
stable boundary cannot be established, remain non-serving and explain why.
Do not claim that a local PID check fences a writer on another machine.

No per-record remote index writes. Start with scan-based discovery and expose its
request/byte cost. Limit concurrency and local resource use. If retaining a
tenant's records in memory for this bounded demo, document and enforce a budget;
do not claim unbounded-scale recovery.

## 10. Complete Cache-Loss Demonstration

Implement a small CLI/demo with separate parent verifier and child runtime:

1. Create an isolated run namespace and tiny deterministic tenant dataset.
2. Publish a sequence-zero checkpoint or another complete remote bootstrap so
   restore does not depend on surviving source database files.
3. Execute writes and capture acknowledged transaction IDs and request fingerprints
   in a parent-owned ledger outside the child's disposable working directory.
4. Publish a later checkpoint at N, then commit additional WAL after N.
5. Pause at a controlled post-ACK boundary with no unfinished request for the first
   deterministic scenario, forcibly terminate the child, and discard only that
   run's local database/cache state using validated paths.
6. Restore into a fresh directory using remote checkpoint plus remote WAL.
7. Verify every acknowledged transaction is present exactly once, validate full
   logical state against an independent reference execution, and check sequence,
   hash-chain head, and retry metadata.
8. Retry an acknowledged transaction and show that it creates no extra logical
   write; submit a new transaction and verify the next valid sequence.

Use real subprocess termination in at least one offline integration test/demo,
not only an exception hook in the same process. Avoid deleting a directory used
by active SQLite connections, the verifier, or the authoritative test store.

Add a separate durable-before-ACK scenario: the parent has no successful ACK but
remote history contains the transaction. Recovery must preserve it and resolve
the retry. The restored state is not necessarily exactly the ACK ledger's set;
all ACKed writes must survive, and additional durable unacknowledged writes must
be accounted for explicitly. Never assert that all unacknowledged writes vanish.

The independent oracle must derive expected state from logical operation intent
and resolved outcomes rather than calling the same WAL replay code under test.
Validate more than counts: values, versions, transactions/items, audit history,
logical checksum, and internal integrity should agree.

## 11. Triggering and Retention

Provide explicit checkpoint create/list/validate/restore commands. Manual
checkpointing is sufficient for this phase; design the coordinator so a future
scheduler can call it safely. Optional scheduling must serialize per-tenant
checkpoint attempts and expose failures rather than hide them in a daemon.

Retain all WAL and previous published checkpoints by default. Do not implement
automatic garbage collection. A shared batch is not deletable merely because
one contained tenant checkpointed its records; all covered tenants and retained
recovery requirements matter. Document this future constraint.

Explicit demo teardown may remove only artifacts belonging to its isolated run
prefix after verification/export. Never delete a whole user bucket or arbitrary
prefix. Report leftovers if cleanup is incomplete. Keep evidence before cleanup.

## 12. Metrics and Evidence Artifacts

Report snapshot lock/capture time, snapshot validation/hash time, upload time,
publication time, bytes, and request counters with Phase 6 accounting semantics.

Break recovery time into discovery, download, validation, WAL fetch/decode,
replay, final verification, and installation/readiness. Report WAL objects scanned,
irrelevant records encountered, records applied/deduplicated, bytes read, checkpoint
sequence, final sequence, and LIST/GET/PUT/DELETE counts by stage.

Define observed recovery time separately from a claimed production RTO. Report
acknowledged transaction loss as a measured count and observed RPO only for the
tested failure class. Do not infer permanent AZ-loss behavior from a process-kill test.

Write versioned JSON results with configuration, dataset/workload identity,
revision/dependency information when available, store type, namespace, phase
timestamps, outcomes, verification results, and cleanup status. Clearly distinguish
offline adapter evidence from real AWS evidence. No fabricated performance values.

Expose an optional bounded checkpoint-interval experiment using explicit intervals
and a small request/size cap. Do not run a large matrix by default. The initial
required deliverable is correctness and useful stage metrics, not a production
scaling study or statistically strong p99 claim.

## 13. Required Offline Tests

Use realistic local/test adapters behind store interfaces and controlled failure
hooks. The offline authoritative store must be outside the discarded cache path.
Normal pytest must never contact AWS. Cover:

1. Snapshot consistency and sequence/hash binding while writes are coordinated.
2. Standalone snapshot opens without source SQLite WAL/SHM files.
3. Sequence-zero, nonzero, idle-tenant, and multiple-tenant checkpoints.
4. Checkpoint requests for unresolved tenants reject safely.
5. Snapshot success plus manifest failure leaves no published partial candidate.
6. Stored-object/manifest response loss supports safe identical retry.
7. Same immutable key with different bytes is rejected.
8. Truncated/corrupt bytes, wrong size, tenant, sequence, chain head, schema, format,
   or internal retry state are detected before readiness.
9. Candidate listing handles pagination, out-of-order entries, orphan snapshots,
   and duplicate/equivalent or conflicting candidates.
10. Fallback, if supported, requires a valid complete WAL suffix and is observable.
11. Restore from both single-record and cross-tenant batch WAL; unrelated tenants
    never modify the target DB.
12. Missing middle and required trailing records, conflicts, checksum corruption,
    and unresolved history prevent normal serving.
13. Retry identity survives checkpoint/restore and post-checkpoint replay.
14. Duplicate replay is harmless; first new write uses the correct sequence.
15. Actual child process termination plus complete local-cache loss recovers all
    acknowledged writes, checked independently.
16. Durable-before-ACK crash preserves and reconciles the ambiguous transaction.
17. Interrupted restore is restartable and does not expose partial live state.
18. Failures export actionable results and namespace-scoped cleanup does not touch
    unrelated local files or remote keys.

Use deterministic synchronization instead of race-sensitive sleeps. Choose small
fixtures and make subprocess tests bounded with cleanup in finally blocks.

## 14. Real AWS Verification and Cost Discipline

Add explicitly gated AWS tests requiring RUN_AWS_TESTS=1 and both checkpoint/WAL
configuration. Missing configuration must skip with an actionable message.

Required cloud tests when configured:

- Tiny checkpoint PUT/GET/validate roundtrip in S3 Standard.
- Checkpoint publication and listing under an isolated prefix.
- Restore a tiny tenant from S3 Standard plus S3 Express WAL, including batch mode.
- Verify final logical state, retry behavior, and cleanup.

Reuse normal SDK credential resolution. No embedded credentials, account-specific
bucket names, public ACLs, or automatic bucket provisioning. Check current official
AWS documentation for used APIs and bucket distinctions. Print the run envelope,
bound object counts/bytes, and require explicit large-run opt-in. Leave AWS tests
skipped if credentials/configuration are absent; finish offline implementation.

For representative performance measurements, document same-AZ placement for compute
and S3 Express. Laptop cloud integration tests are functional evidence only.

## 15. Documentation, Gates, and Final Response

Update architecture, invariants, failure model, cost safety, WAL recovery notes,
configuration example, and README. Add docs/checkpointing.md,
docs/recovery-demo.md, and docs/phase-7-report.md plus the ADR.

Provide exact offline demo and opt-in AWS commands. Explain snapshot/publication
state transitions, serving readiness, corruption handling, retry behavior, retained
history, scan costs, and the exclusive-writer assumption. Clearly label implemented
local recovery versus future distributed orchestration.

Run pytest, ruff check ., and mypy src. Verify CLI help and the full offline
cache-loss demonstration. Report actual results and AWS run-or-skip status.

Final response should include:

1. Files and prerequisite corrections.
2. Snapshot and metadata consistency model.
3. Immutable publication and candidate-selection protocol.
4. Restore/replay/readiness behavior and uncertainty limits.
5. Actual cache-loss proof: checkpoints, recovered sequence, acknowledged writes,
   unknown outcomes, checksums, and measured stage durations.
6. Tests/lint/types, offline demo, and AWS evidence status.
7. Reproduction commands and evidence artifact paths.
8. Explicit limitations and remaining Phase 8 work, especially real fencing,
   distributed recovery, scan scalability, and shared-batch retention.

Do not automatically begin distributed ownership, deploy infrastructure, or claim
production readiness. Finish the bounded checkpoint-to-recovery proof completely.
