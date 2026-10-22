# Checkpointing

## Fenced Checkpoints

Phase 8 registers a checkpoint through current-owner control CAS and binds it to an accepted
descriptor ancestor. A snapshot captured at N remains a valid prefix while later commits
advance the head. Uploaded but unregistered snapshots are candidates; stale owners cannot
change the trusted checkpoint. Recovery validates the anchor before skipping its prefix.

Phase 7 implements full tenant SQLite checkpoints and sealed-run recovery, not
distributed failover. See ADR 0002 and `docs/recovery-demo.md` for the failure proof.

## Capture And Publication

`CheckpointCoordinator.create(engine, tenant)` uses the same tenant lock as normal
operations. READY admission is mandatory. The supported SQLite backup API captures
committed SQLite-WAL content without copying a live main file or holding an active
write transaction. The resulting standalone snapshot uses DELETE journal mode;
it opens without source WAL/SHM sidecars. Other tenants can execute concurrently.
The tenant lock is released before hashing, validation, and remote upload.

Metadata comes from the snapshot, never from later live reads. The immutable
version-1 manifest binds namespace, tenant, schema/internal versions, sequence N,
chain head, byte count/SHA-256, logical checksum, internal checksum, timestamp,
WAL namespace, and supported WAL record versions. Sequence zero has a null head.
Timestamps are audit data, not ordering authority. Canonical JSON is checksummed
excluding its own checksum field; missing/unknown fields and future versions reject.

User logical checksum version 1 remains unchanged. Internal digest version 1
covers sequence, component versions, transaction fingerprints, record hashes,
sequence mappings, and durability flags. It excludes physical WAL object locators:
identical WAL copies may legitimately have different keys. Snapshot SHA covers
those locators; recovery cross-checks checkpoint locators against discovered WAL.
Snapshot validation checks the exact supported DDL, SQLite integrity, foreign keys,
tenant identity, user-row tenant boundaries, version consistency, and complete
transaction/retry coverage. Legacy WAL can decode/replay, but checkpoints with
unverifiable legacy retry identity are rejected rather than upgraded by guessing.

Publication order:

```text
READY -> captured standalone file -> validated snapshot
      -> immutable snapshot upload + content read-back
      -> immutable manifest publication + read-back -> published candidate
```

S3 Standard uses streaming file handles, `StorageClass=STANDARD`, conditional
`IfNoneMatch=*`, and bounded SDK timeouts/retries. ETags are not content hashes.
Identical stored bytes resolve a lost response or a conditional conflict; different
bytes reject. An upload failure with no positive read-back remains unresolved.
`publish(path, manifest)` can retry the same attempt using its original keys/bytes.
`create` is a new capture attempt with a new ID, not a retry of an old ID. Temporary
captures clean up on failure; snapshot-only orphan objects remain unreferenced.

Layout inside the configured checkpoint prefix:

```text
<namespace>/checkpoints/<tenant>/<sequence>/<checkpoint-id>/snapshot.db
<namespace>/checkpoints/<tenant>/<sequence>/<checkpoint-id>/manifest.json
<namespace>/boundaries/<tenant>/<boundary-id>.json
```

The object-store interface streams files rather than using the old bytes-only
Phase 0 placeholder. A separate existing general-purpose bucket is required;
directory buckets and the configured WAL bucket are rejected for checkpoints.

## Recovery Boundary

A complete-looking scan cannot establish that the last acknowledged WAL is present.
`seal(engine, tenant)` validates READY state, captures its final digests, permanently
marks that source tenant SEALED, then publishes an immutable boundary. Normal reads,
writes, and Phase 6 reconciliation cannot reopen a sealed source. Restore uses a new
directory. Publication failure leaves the source non-serving; never force-clear it.

Restore requires the explicit boundary key and `--old-writers-stopped`. This is an
operator assertion, NOT a lease or distributed fencing mechanism. It is valid only
for exclusive sealed runs with no unfinished requests and no writes after sealing.
A recovered runtime can write, but then the previous boundary cannot cover another
failure: publish a new boundary before the next controlled crash. An arbitrary
unclean crash without an established boundary remains outside this phase's recovery
claim. No 404/empty scan can resolve an unknown in-flight PUT.

## Selection And Readiness

List all manifest pages, ignore orphan snapshots, and select the highest sequence.
Equivalent candidates at that sequence must agree on semantic state, and every
peer snapshot validates. Different bytes alone need not imply different SQLite
logical state. Conflicting candidates or a corrupt newest candidate fail explicitly;
there is no fallback policy. Authorization/service errors propagate, not fallback.
Current discovery conservatively rejects malformed older manifests too.

Restore downloads into a same-filesystem staging directory and verifies all snapshot
bindings. Full retained WAL history is bounded, decoded, explicitly sorted, and
validated for tenant, batch indexes, checksums, contiguous sequences, and transaction
identity. Identical copies deduplicate. Only records greater than checkpoint N replay.
The required final sequence/head must exactly equal the remote sealed boundary:
missing trailing records and unexpected suffixes both reject. Final logical/internal
digests must match before a new admission journal and tenant registry are installed.

No normal engine opens an incomplete restore. All handles close before staging is
renamed into a previously absent destination. Existing/active datasets are never
overwritten. Interrupted restore cleans staging and can restart from remote data.
Previous valid directories and unrelated files remain untouched. Windows and Linux
use the same sibling-directory installation approach.

## Bounds And Retention

- Maximum snapshot/object download: 64 MiB; no multipart upload in this bounded phase.
- WAL scan: 128 MiB encoded bytes, 1,000 objects, 10,000 total decoded records,
  including irrelevant tenant records. Checkpoint listing caps 1,000 objects and
  4 MiB total manifest bodies. Validation/replay is single-threaded.
- Full pre-checkpoint WAL is retained and validated because Phase 6 startup still
  expects history from sequence 1. Checkpoints reduce replay, not scan cost yet.
- Manual triggering only; the coordinator can be called by a future scheduler.
  CLI commands require exclusive use of the dataset, not another live runtime.
- No automatic checkpoint or WAL deletion. Shared batches need a future all-tenant
  retention proof before garbage collection is safe.

Metrics report capture including lock/admission, validation/hash, upload/read-back,
publication, and recovery stages. Capture publication counters are store-lifetime
snapshots; recovery counters are deltas for that restore interval. Offline calls
are filesystem operations, not AWS requests. AWS counters are SDK invocations;
internal transport retries and billable usage remain null. Measured recovery time
is not a production RTO, and these tiny runs do not support p99 claims.

S3 Standard does not make later S3 Express WAL multi-AZ. Temporary remote outage
prevents READY; permanent loss of the WAL AZ can lose post-checkpoint history.
