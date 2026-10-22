# ADR 0001: Durability outcomes, retry identity, and bounded restart

Status: accepted for Phase 6 stabilization.

Remote persistence is the durable commit point; SQLite is its materialized state.
A timeout is UNKNOWN, not proof of absence. Reads and writes are blocked for a
tenant whose attempt is unresolved or whose durable record is not yet applied.
Independent tenants remain available while the shared committer is healthy.

A separate FULL-synchronous local SQLite attempt journal records canonical WAL
before calling the committer and records its receipt before returning to local
apply. It is diagnostic/admission state, not a replacement for remote history.
Reconciliation validates remote records, sequence uniqueness and the hash chain
before replaying a known attempted record. Empty listings/404s never clear an
unresolved attempt. Missing attempt identity fails closed after an unclean restart.
No new batch is submitted as a retry of an unresolved logical attempt. SDK retries
use the original key and bytes; explicit reconciliation only reads and applies.

Remote startup requires a retained journal or explicit fresh-namespace bootstrap.
Fresh bootstrap asserts exclusive access to a namespace never used by another
runtime and checks that remote history is empty. It is not an override for a
missing journal in an existing namespace. Remote scans require old writers to be
stopped; they do not implement distributed fencing or an atomic listing snapshot.
Complete local-cache loss is not recovered in this phase.

WAL v2 adds a versioned canonical request fingerprint, including ordered mutation
intent and expected versions. WAL v1 canonical serialization remains unchanged;
legacy records can replay but cannot prove equivalent retry intent. Transaction
IDs are enforced per tenant. Retry metadata is committed with local state and
restored during replay. Conflicting or unverifiable reuse is rejected.

The existing connection limit is an idle-cache target, not a hard concurrent
resource bound. Benchmark workers are bounded. Batch byte limits cover encoded
record payloads, excluding the envelope; a single oversized record is rejected.

Metrics must not alter transaction outcomes. Benchmark measurement ends before
drain/cleanup. SDK calls, successful creations, and attempts are distinct; wire
attempts and billable usage remain unavailable without transport instrumentation.

Operational consequences: the root attempt journal uses schema version 1 and
FULL synchronous commits, adding local write cost before and after remote I/O.
Remote startup/reconciliation scans complete tenant history from sequence 1,
including shared batch objects. This favors correctness over startup speed and
is bounded by inspection caps; a checkpoint/index design belongs to Phase 7.
The journal is not a fencing token and changing namespaces does not reset history.

Batch state is queued -> sealed -> uploading -> resolved. Fatal worker failure
wakes all waiters and stops admission. Deadlines bound caller waiting, not remote
commit time. A late upload is possible after a timeout, so unsuccessful benchmark
runs retain their remote prefix. Only definite pre-persistence rejection restores
the previous READY journal state without reconciliation.

AWS API constraints were checked against the official
[PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) and
[ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
references. Conditional writes preserve an object's key/body identity; directory
bucket listing order does not establish tenant sequence order. No SDK transport
event hooks or inferred billing counters are used.
