# Correctness Invariants

## Fenced Authority Invariants

- Successful fenced write implies exact payload persistence, accepted descriptor reachability,
  and completed local apply before clean success.
- After takeover to epoch E+1, no later control CAS with epoch E can be accepted.
- Payload or descriptor existence without reachability from control is not commitment.
- READY requires current token plus exact local/authority sequence and checksum head.
- Recovery installs only after a final authority recheck; required missing data fails closed.

These invariants define the minimum correctness contract for later implementation phases. They are architecture constraints, not optional optimizations.

Phase 7 boundary: remote ACK ordering, checkpoints, and sealed-run complete
cache-loss recovery are implemented. Ownership epochs and stale-writer fencing
remain architectural requirements, not delivered capabilities. S3 Express is
single-AZ; later WAL is not upgraded by a multi-AZ checkpoint store.

## Durable Acknowledgment Invariant

If the client has received a successful transaction acknowledgment, destruction of the following must not cause that acknowledged transaction to disappear:

- The database process
- Its Kubernetes pod
- Its Kubernetes worker node
- All local NVMe and cache contents

Formally:

```text
ACK(transaction)
=>
transaction exists in remotely durable state
```

A transaction must never be acknowledged before its required WAL has been durably persisted to the remote durability store.

Phase 5 implements this invariant for `WAL_STORE=s3-express` in single-record
mode. Phase 6 preserves it for batched mode: a successful clean write result is
returned only after the S3 Express object containing that WAL record succeeds
and local SQLite apply commits. Local WAL mode remains a development mode and
must not be interpreted as remote durability.

```text
SQLite internal WAL
!=
EphemeralDB explicit remotely durable WAL
```

The Phase 4 local explicit WAL is still available as a development and
correctness artifact for format, validation, and replay. It is not a substitute
for S3 Express durability.

## Unacknowledged Transaction Rule

If a pod dies before remote WAL persistence succeeds, the transaction is allowed to disappear provided the client was never told it successfully committed.

## WAL Ordering Invariant

Within each tenant database:

```text
sequence N + 1 > sequence N
```

No accepted WAL sequence may be silently skipped or reordered during recovery.

Phase 5/6 reuse the tenant DB's internal `_ephemeraldb_state` table for the
latest locally applied per-tenant WAL sequence. Sequence numbers are prepared
under the local tenant write lock and are not advanced on validation conflicts or
S3 PUT failures. If S3 succeeds and local apply fails, the remote WAL record or
batch remains authoritative and local state may lag.

Stabilization journals the attempted canonical record before submission. UNKNOWN
and durable-but-not-applied outcomes block all normal tenant reads/writes under
the tenant lock. Only validated positive history plus atomic local reconciliation
restores READY. Empty scans cannot clear ambiguity. Restart verifies retained
state against the full remote chain before admission; missing identity or
conflicting history fails closed. This assumes one runtime and stopped old writers.

## WAL Idempotency Invariant

Replaying the same transaction or WAL record more than once must not apply the logical transaction more than once.

Phase 4/5 replay treats an already committed transaction ID as applied only when
the stable committed rows match the WAL content. Conflicting content is treated
as WAL corruption.

## Checkpoint Invariant

Checkpoint sequence `N` represents all committed WAL operations up to and including `N`.

Recovery from that checkpoint applies only:

```text
WAL sequence > N
```

Published metadata must match the captured snapshot's sequence/head, tenant,
schema, logical checksum, and internal retry state. A snapshot without a valid
manifest is not a checkpoint. The Phase 7 restore path also requires an immutable
quiescent boundary: matching a complete-looking prefix is insufficient to detect
missing trailing WAL. Unknown/unsealed histories must remain non-serving.

## Ownership Invariant

At a given ownership epoch, there may be only one valid writer for a tenant database.

## Stale-Writer Invariant

If ownership has progressed:

```text
epoch 7 -> epoch 8
```

writes produced by epoch 7 must not be accepted as valid current writes.

## Recovery Invariant

Given:

- The latest valid checkpoint
- All durable WAL records after that checkpoint

the system must reconstruct exactly the same logical committed database state.

## Cache Invariant

Local database and cache files are disposable. Correctness must never depend on their survival.

## Isolation Of Concerns

The following concerns are related but distinct:

```text
concurrency control
!=
durability
!=
recovery
!=
autoscaling
```

S3 durability must not be treated as a concurrency-control mechanism. Remote WAL persistence can prove that data survived, but it does not by itself decide which writer is current or whether concurrent transactions conflict.
