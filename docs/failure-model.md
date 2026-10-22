# Failure Model

## Phase 8 Extension

The fenced slice covers a still-alive stale process, delayed payload/descriptor/control
requests, response loss, process termination without seal, complete disposable-cache loss,
and a successor losing ownership during recovery. Timeouts after submission are UNKNOWN.
It does not cover Byzantine credentials, automatic failure detection, region-wide control
loss, or permanent loss of an S3 Express availability-zone payload referenced by authority.

This document defines failures that the benchmark architecture must handle. Phase 5 includes focused fault hooks around WAL durability boundaries; it does not implement the full later chaos framework.

Phase 6 stabilization implements retained-state restart verification and explicit
reconciliation for F3-F5. Phase 7 additionally demonstrates process termination
and complete cache loss after an explicit quiescent seal, including a suppressed
ACK for a durable write. No actual Kubernetes/node-failover or stale-writer test
is claimed. A returned result is not proof a client observed ACK.

Temporary remote outage prevents validation/READY, rather than selecting stale
state. Permanent S3 Express AZ loss can lose WAL after the S3 Standard checkpoint.
An unsealed crash with unresolved in-flight requests is not covered by the sealed
demo boundary and cannot be resumed based on an empty scan. Snapshot corruption
and missing required WAL reject without exposing partially restored files.

## Serving And Outcome States

READY admits normal operations. PENDING journal attempts produce RECONCILING;
verification/corruption errors quarantine as FAILED. Both block normal reads and
writes. Successful reconciliation validates the chain and attempted record,
replays atomically, restores retry metadata, and returns READY. Independent
tenants remain usable unless the shared committer fails.

`durability_outcome` distinguishes NOT_ATTEMPTED, REJECTED, UNKNOWN,
DURABLE_NOT_APPLIED, DURABLE_APPLIED, and LOCAL_APPLIED. A blocked subsequent
request is NOT_ATTEMPTED; its tenant state conveys the unresolved prior attempt.
`remote_durable=false` alone is not evidence that no object exists.

Stored-then-timeout and not-stored-timeout are indistinguishable without positive
evidence. Neither a 404 nor an empty listing authorizes a new write. Retain the
journal, stop old writers, and use the reconciliation CLI. If the attempted
record is absent, remain blocked; no blind resubmission or regenerated batch ID
is supported. Complete local-cache loss is recoverable only through Phase 7's
explicit sealed-boundary path; arbitrary unknown in-flight histories remain blocked.

## F1 - Process Crashes Before WAL Generation

Expected result: the transaction is not committed.

No WAL record exists, so recovery has nothing to replay. The client must not receive a successful commit acknowledgment.

## F2 - Process Crashes After WAL Generation But Before Remote WAL Persistence

Expected result: the transaction may disappear. The client must not have received an acknowledgment.

Local WAL or cache files are not authoritative durable state. If the process or pod dies before remote WAL persistence succeeds, the system is allowed to lose that transaction.

## F3 - Remote WAL Persistence Succeeds And Process Crashes Before Local Apply

Expected result: the WAL record or containing batch remains in S3 Express, local
SQLite may remain old, and the client must not have received a successful
acknowledgment.

The remote WAL is retained because it is authoritative durable history. Later
recovery/reconciliation must apply or account for it.

## F4 - Remote WAL Persistence Succeeds And Local Apply Fails

Expected result: the operation is not reported as a clean success, the result
indicates reconciliation is required, and the remote WAL is retained.

The system must not delete the remote WAL as compensation for local apply
failure.

## F5 - Remote WAL Persistence And Local Apply Succeed But Process Crashes Before ACK

Expected result: the transaction is committed, but the client outcome is ambiguous.

The durable WAL record, whether stored alone or inside a batch, must be replayed
during recovery. A later idempotent retry by the client must not duplicate the
logical transaction.

## F6 - Process Crashes After ACK

Expected result: the transaction survives.

The ACK implies the transaction already exists in remotely durable state.

## F7 - Entire Kubernetes Pod Is Deleted

Expected result: all acknowledged transactions survive.

The new pod must recover from the latest valid checkpoint plus durable WAL. Pod-local cache contents are disposable.

## F8 - Entire Kubernetes Worker Node Is Terminated Including Local NVMe

Expected result: the tenant database can recover elsewhere.

The authoritative state must come from remote checkpoint and WAL storage, not the lost node-local storage.

## F9 - Stale Previous Owner Continues Attempting Writes After Ownership Transfer

Expected result: stale writes are rejected.

The ownership registry must distinguish current owner epochs from stale epochs and prevent split-brain writes.

## F10 - Checkpoint Object Is Invalid Or Corrupted

Expected result: the system detects the invalid checkpoint rather than silently recovering incorrect state.

Recovery must validate checkpoint metadata and contents before trusting them.
