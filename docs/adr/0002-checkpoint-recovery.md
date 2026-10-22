# ADR 0002: Checkpoint publication and bounded recovery

Status: accepted for Phase 7.

Use SQLite Connection.backup under the existing tenant lock after READY admission,
without an active write transaction. Inspect the standalone snapshot itself for
sequence/head, schema, logical state, and internal retry-state integrity. Release
the lock before uploads. Publish immutable snapshot then immutable checksummed
manifest to a separate S3 Standard general-purpose bucket. Snapshot-only objects
are orphans. Conditional writes and read-back content verification make identical
publication retries safe. No mutable latest pointer or automatic deletion.

Choose the highest sequence manifest, not newest timestamp. Equivalent logical
and internal state at one sequence is acceptable even if SQLite page bytes differ;
conflicts or a corrupt newest candidate fail explicitly, with no implicit fallback.
Restore only into a new sibling staging directory, verify, then rename to a
previously absent destination. Never replace an active/existing dataset.

A scan alone cannot prove a missing trailing record. A separately published,
immutable quiescent boundary identifies tenant, WAL namespace, final sequence,
head, and logical/internal digests. The runtime publishes it under its tenant
lock only after READY verification, then stops accepting writes for that tenant.
Recovery requires an explicitly selected boundary and an assertion that all old
writers are stopped. It verifies remote WAL independently of the parent's ACK
ledger. No boundary, unresolved request, unexpected suffix, or missing required
record means no READY. This deliberately supports sealed demonstration runs, not
arbitrary unclean distributed failover. After recovery, new writes invalidate
that old boundary for any later recovery; seal again before the next demo kill.

Retain all WAL, including pre-checkpoint history, for current Phase 6 restart
verification. Replay only the suffix after checkpoint N. A shared batch cannot
be garbage-collected just because one tenant checkpointed. Bound bytes, objects,
and records during discovery. Offline filesystem stores live outside disposable
cache and are explicitly not AWS evidence.

No fencing or multi-owner safety is added. Process/cache loss with available
remote data is distinct from permanent AZ loss: S3 Standard checkpoints do not
make subsequent S3 Express One Zone WAL multi-AZ.

API basis: Python's supported [SQLite backup API](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.backup),
[S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html),
and [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
