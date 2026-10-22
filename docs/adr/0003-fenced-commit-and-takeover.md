# ADR 0003: Fenced Commit And Takeover

Status: accepted for Phase 8

## Context

The legacy protocol makes a validated WAL payload the durable commit record. That is safe
only after a cooperative seal and assurance that previous writers stopped. A stale writer
can otherwise upload another valid record after replacement. Merely checking an epoch
before upload leaves a pause-after-check race.

## Decision

Use one conditional S3 Standard control object per namespace and tenant. The same object
contains authority format/version, namespace and tenant identity, a unique owner session,
monotonic epoch and revision, accepted tenant sequence and WAL checksum head, immutable
descriptor head/checksum, and bootstrap checkpoint identity. Its ETag is an opaque compare-
and-swap validator. Offline tests use an external SQLite store exposing the same primitive.

Each immutable commit descriptor binds the namespace, tenant, owner session/epoch, stable
attempt ID, transaction/fingerprint, exact WAL checksum and locator (single key or batch
key/checksum/index), previous accepted descriptor and WAL heads, and sequence. Canonical
JSON plus SHA-256 protects model integrity. Descriptors form a traversable accepted chain;
unreachable descriptors and payloads are candidates only.

Fresh bootstrap uses conditional create and rejects existing authority. Phase 8 never
deletes or resets authority. Existing Phase 5-7 namespaces remain `legacy-sealed-v1`; new
namespaces are explicitly `fenced-v1`. In-place adoption is rejected.

## Linearization Points

- Commit: successful CAS replacing the tenant control object from the exact expected
  session, epoch, revision, sequence, WAL head, and descriptor head to the new head.
- Takeover: successful CAS of that same control object, preserving all accepted-history
  fields and incrementing epoch and revision while installing a fresh session ID.
- Read: authoritative control read followed by verification that session/epoch and local
  sequence/head match. A transfer overlapping the read may linearize before or after this
  control read; no stale-read lease is promised.

Payload persistence no longer proves commit in fenced mode. It proves only that bytes are
available for a possible decision. Descriptor persistence also does not prove commit.

## Protocol

Under the existing per-tenant execution lock:

1. Read/verify authority and prepare sequence `head + 1` under its epoch.
2. Persist the immutable WAL candidate (single or shared batch).
3. Persist the immutable commit descriptor.
4. CAS control from the captured state to the descriptor/head.
5. Apply SQLite locally, persist retry metadata, and acknowledge.

A takeover between steps 1 and 4 changes revision/session/epoch, so the old CAS cannot
succeed. A commit CAS that wins first is preserved by takeover and recovered by the new
owner. Physical late uploads remain unreachable and cannot consume accepted sequence.

## Unknown Outcomes

Every write timeout after submission is UNKNOWN. Stable keys, attempt IDs, and canonical
bytes make retries content-identical. A caller resolves a commit CAS by reading control and
traversing accepted descriptors for its attempt. If found, it committed. If a later control
revision no longer has the expected predecessor and the attempt is absent from the chain,
the old expected-revision CAS is permanently ineligible and the attempt is rejected. If the
expected revision is still current, uncertainty remains and normal serving is blocked.
Late service completion is therefore included rather than assumed cancelled.

Acquisition uses a stable operation/session identity. A lost successful response resolves
by reading the installed session. A different later session proves that activation is stale.

## Recovery And Checkpoints

Takeover atomically returns the accepted head. Recovery starts from the authority-bound
bootstrap checkpoint, traverses the descriptor chain with count/byte limits, verifies every
locator and WAL hash edge, replays only reachable records, validates the resulting local
head and retry identity, then re-reads authority before installation and READY activation.
Missing/corrupt required objects fail closed. Unreferenced candidates are ignored.

Checkpoint bytes are candidates until their metadata is registered by control CAS. The
registration must name an accepted descriptor ancestor and may remain behind the current
head. Writes after snapshot capture are valid. A stale owner cannot register a checkpoint.

## Batching

One immutable payload may contain records for several tenants. Each record receives an
independent descriptor and per-tenant control CAS. Acceptance of tenant A says nothing
about tenant B. A descriptor binds batch key, checksum, record count, and index; recovery
validates the full envelope. Shared payload retention follows any accepted reference.

## Alternatives

- DynamoDB conditional updates/transactions offer a mature coordination store but add a
  separately provisioned service and IAM/cost surface for this bounded portfolio phase.
- A central coordinator can serialize commits but introduces an availability bottleneck,
  deployment work, and a new service whose own durable log would need proof.
- Per-tenant S3 conditional objects use the existing general-purpose checkpoint bucket and
  official conditional PUT semantics. The cost is at least one descriptor PUT and control
  GET/PUT per accepted record and no multi-key transaction. This is the selected tradeoff.

## Failure And Trust Boundaries

This handles crash/stale-process faults for two runtimes and explicit takeover, not Byzantine
writers, automatic failure detection, authorization policy, or consensus across several
objects. Control unavailability blocks reads and writes. A committed descriptor cannot make
an unavailable S3 Express AZ payload recoverable. IAM must prevent unconditional control
overwrite; the adapter always issues conditional writes, and production should enforce the
headers with bucket policy.

S3 references: [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html),
[PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html), and
[consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel).
