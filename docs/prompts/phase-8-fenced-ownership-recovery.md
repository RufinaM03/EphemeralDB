# Implementation Prompt: Phase 8 Fenced Ownership and Unsealed Recovery

## 0. Handoff And Execution Contract

You are continuing **EphemeralDB Benchmark** in the existing workspace and the
same conversation. The user is switching to **GPT-5.6 Sol** for implementation.
Treat this file as the implementation specification, not as a request to rewrite
the prompt or produce only a plan. Do not switch models, create another task,
deploy infrastructure, or begin Phase 9 automatically.

Read current files before making changes. Conversation summaries and old reports
are orientation, not proof of current behavior. Preserve user changes and existing
artifacts. This workspace has historically lacked Git metadata; verify rather
than assuming Git commands will work. Do not initialize/rebuild the repository
or erase prior evidence as a prerequisite.

### Depth And Effort

The user explicitly wants a substantial, high-effort implementation. Use a large
share of the available reasoning/tool budget on understanding, implementation,
adversarial testing, and verification. Do not optimize for a short turn or stop at
the first happy-path demo. This is a systems-correctness phase, not scaffolding.

Do not manufacture token consumption with repetition, giant terminal dumps,
unrelated features, or inflated documentation. Spend the effort on real proof
obligations and executable evidence. No minimum token count is a correctness
criterion, and this prompt cannot change the runtime's context/usage limits.
Provide concise decision rationales, invariants, and counterexample traces, not
private chain-of-thought transcripts.

Persist a working checklist in `docs/phase-8-work-log.md`. Record milestones,
decisions, changed files, failed/passing commands, and next actions. If context
compacts, read that log and continue rather than starting again. Keep the user
informed while working. Carry the task through code, tests, demos, docs, and an
evidence report. If a genuine external prerequisite prevents part of completion,
finish everything possible offline and state the exact unfulfilled gate; do not
claim completion of unverified safety properties.

Use at least these distinct review passes before finishing:

1. Protocol review: identify linearization points and attack each race boundary.
2. Implementation review: inspect actual call paths, failure classification,
   persistence, schema compatibility, resource bounds, and shutdown behavior.
3. Evidence review: verify that tests and artifacts establish the claimed property,
   rather than assuming it or deriving the oracle from the implementation under test.

These are work requirements, not a request for verbose conversational narration.

## 1. Objective And Scope

Implement a **bounded two-runtime ownership, fencing, and recovery protocol**.
Prove that a stale writer cannot create a new authoritative commit after another
runtime has taken ownership, and that a replacement can recover acknowledged
state after an **unsealed crash and complete loss of the old local cache**.

Phase 7 required a cooperative seal and an operator assertion that previous
writers had stopped. Phase 8 must remove that dependency for its new fenced mode:
the old runtime may remain alive, paused, disconnected, or holding a delayed PUT.
The successor must not rely on the old runtime's cooperation, local files, PID,
clock, graceful shutdown, or parent-owned acknowledgment ledger for correctness.

Keep the project bounded:

- Python tenant SQLite engine and existing deterministic schema/workload semantics.
- Single-record and cross-tenant batched S3 Express WAL payloads.
- S3 Standard checkpoints and a small durable control-plane primitive.
- Two actual runtime processes for takeover/stale-owner proofs.
- Explicit ownership transfer; a manual trigger is acceptable. Automatic lease
  expiry, leader election scheduling, and a production failure detector are not required.
- An offline persistent multi-process adapter plus a real AWS adapter for the
  selected coordination primitive, with cloud tests explicitly gated.

Out of scope: EKS, Kubernetes controllers, Terraform, Aurora, Docker deployment,
Spot/autoscaling, frontend, global routing, cross-tenant distributed transactions,
production consensus implementation, automatic garbage collection, and a large
performance matrix. Do not add a distributed system library merely for appearance.

Completion is a working correctness slice, not a claim of production readiness.

## 2. Ground Truth And Mandatory Audit

Read these first, in manageable groups:

- `README.md`, `docs/architecture.md`, `docs/invariants.md`, `docs/failure-model.md`.
- `docs/portfolio-assessment.md`, `docs/phase-6-stabilization-report.md`.
- `docs/phase-7-report.md`, `docs/checkpointing.md`, `docs/recovery-demo.md`.
- Both existing ADRs and the Phase 6/7 implementation prompts.
- `docs/wal.md`, `docs/wal-batching.md`, `docs/benchmark-methodology.md`,
  `docs/s3-express.md`, `docs/cost-safety.md`, `.env.example`, `pyproject.toml`.

Inspect the implementation, especially:

- `engine/executor.py`, `engine/engine.py`, `engine/durability_guard.py`,
  `engine/connection.py`, `engine/results.py`, `engine/metrics.py`.
- `wal/durability.py`, `wal/models.py`, `wal/codec.py`, `wal/validation.py`,
  `wal/sequence.py`, `wal/identity.py`, `wal/replay.py`, and `wal/batching/`.
- `checkpoint/models.py`, `checkpoint/snapshot.py`, `checkpoint/coordinator.py`,
  `checkpoint/store.py`, and `checkpoint/cli.py`.
- `recovery/coordinator.py`, `recovery/offline.py`, `recovery/demo.py`.
- `aws/s3_express.py`, `aws/s3_standard.py`, and configuration plumbing.
- Actual Phase 6/7 tests and retained JSON evidence, not just their summaries.
- Phase 0 ownership/domain protocols: useful vocabulary, not implemented fencing.

Historical orientation, to be reverified:

- Last reported full suite: 180 passed, 9 skipped; Ruff passed; mypy covered 71 files.
- Final accounting changes additionally passed 39 focused tests.
- Phase 7 demonstrated checkpoint 2 -> recovered 4 -> next write 5 for two tenants.
- Offline post-ACK demo preserved eight ACKs. The ACK-suppressed variant preserved
  seven ACKs plus one durable unacknowledged operation.
- Both variants sealed before the real kill. They did NOT prove arbitrary
  in-flight crash recovery, concurrent ownership, or remote fencing.
- SDK metadata was unavailable in the prior interpreter. No real AWS run occurred.

Establish the current baseline with pytest, Ruff, mypy, and a small Phase 7 demo.
Record any pre-existing failures. Fix narrow prerequisite defects with regressions;
do not silently relax safety or delete tests to get a green baseline.

Audit likely integration hazards rather than copying assumptions:

- Current `owner_epoch` fields do not by themselves enforce ownership.
- A `WalCommitReceipt` currently lets the engine apply after payload persistence.
- Phase 7 treats discovered WAL as authoritative within a sealed namespace.
- Recovery currently expects complete retained history from sequence 1.
- Checkpoint validation expects exact supported DDL/internal metadata.
- Recovery reconstructs an attempt journal; do not duplicate its schema again
  if a focused shared initializer is needed for versioned evolution.
- Some metrics use mutable store-stage labels. Concurrent requests must not
  contaminate per-operation/per-stage accounting.
- Transaction identity is tenant-scoped despite any older globally-unique wording.

## 3. Write The Protocol ADR Before Substantial Code

Create `docs/adr/0003-fenced-commit-and-takeover.md`. It must specify:

1. The one authority that serializes ownership changes and commit acceptance.
2. The exact commit and takeover linearization points.
3. Durable control state, immutable history references, and format versions.
4. What payload persistence proves, and what it no longer proves in fenced mode.
5. Unknown write/CAS outcome resolution, including late completion.
6. What a new owner knows atomically when it acquires ownership.
7. How missing trailing data is detected without a pre-crash seal or ACK ledger.
8. Read semantics, delayed acknowledgment semantics, and control-plane partitions.
9. Batching behavior when one physical object spans several tenant authorities.
10. Checkpoint authority, schema/migration boundaries, cost, and failure domains.

Compare a small set of real options: one conditional control object per tenant,
a transactional coordination store, and a central commit coordinator. Choose one,
explain the tradeoffs, and implement it. Do not build three competing backends.

### Preferred Starting Candidate, Not An Unverified API Assumption

Start by evaluating a **per-tenant conditional control object in general-purpose
S3 Standard**, using the existing checkpoint bucket under a separate control prefix.
This avoids adding an infrastructure service solely for the demonstration.

Ownership/session/epoch and the committed-history head must live in the SAME
compare-and-swap domain. A small immutable commit descriptor references the WAL
payload and previous accepted descriptor. The mutable control object points to
the accepted descriptor. Ownership acquisition changes the same control object,
preserving its committed head and advancing epoch/revision.

Before selecting this candidate, verify current official AWS documentation for
general-purpose S3 conditional create/update, consistency, concurrent conflicts,
SDK support, and response-loss behavior. Use the ETag only as an opaque CAS
validator, not as a content SHA-256. Never assume a multi-object S3 transaction.
Use unique monotonic revisions/session identities to prevent ABA-style reuse.

If the chosen APIs cannot satisfy the proof obligations, adopt one supported
transactional alternative, such as an explicitly configured existing DynamoDB
table, and explain why. Keep the same acceptance tests and declare every added
dependency/request. Do not provision that table automatically. Prefer decisive
implementation after a reasoned choice over an open-ended architecture debate.

Do not hand-roll Raft/Paxos or claim consensus from a file lock.

## 4. Non-Negotiable Safety Properties

Use precise operational definitions and executable assertions:

```text
successful client write
  => immutable WAL payload exists
  AND authoritative fenced commit decision accepted that exact content
  AND required local application completed before clean success

takeover accepted at epoch E+1
  => no subsequent commit decision can be accepted with epoch E's token

recovered READY at committed head H
  => bootstrap/checkpoint is valid
  AND every required accepted record through H is present and validated
  AND local sequence, head, retry identity, and authority token agree
```

The control plane may reject a stale writer while the data store still accepts
its bytes. Those bytes are then an orphan candidate, not an authoritative commit.
Do not conflate prevention of accepted commits with prevention of all physical PUTs.

Fencing must protect the authoritative commit boundary, not just ownership lookup,
SQLite apply, application routing, or a local lock. The protocol must defeat:

```text
A checks it is owner at epoch 7
A pauses after that check
B acquires epoch 8 and recovers
A resumes using cached permission
A uploads or completes a previously started upload
A attempts to commit
```

A's new authoritative commit must fail. Its late payload must not poison B's
history, consume B's next sequence irreversibly, or become replayable just because
it has a valid WAL checksum and owner-epoch field.

Distinguish this from an operation whose commit linearized BEFORE takeover but
whose response arrives AFTER takeover. That old operation can legitimately be
committed and must survive. Do not write an impossible test asserting that no
old response may ever arrive after transfer in an asynchronous system.

## 5. Durable Control And Commit History

Design small typed models/interfaces for the chosen protocol. Require at least:

- Namespace/dataset identity, tenant, authority-format version.
- Owner/session identity unique per runtime incarnation; monotonically increasing epoch.
- Unique revision/CAS token; no recycled identity after release, crash, or restart.
- Committed tenant sequence and WAL hash-chain head.
- Immutable committed-history descriptor identity/key/checksum.
- Bootstrap/checkpoint identity sufficient to reconstruct a tenant with no local files.
- Stable operation/attempt IDs for acquisition and commit retries.
- Explicit state needed for recovery/admission, without relying on wall-clock ordering.

Use canonical serialization, strict field/type/version validation, namespace-safe
keys, explicit budgets, and integrity checks. No pickle or executable metadata.
Control identity and WAL identity must bind to the same tenant/run.

Do not overwrite or delete authority to reset an existing namespace. Fresh
bootstrap is a conditional, explicit operation that rejects existing history.
Releasing ownership must preserve sequence/history and prevent epoch reuse.
If no release API is needed, omit it rather than creating an unsafe reset button.

For the conditional-object design, reason about this candidate ordering:

```text
prepare next tenant record under local serialization
persist immutable WAL candidate, using collision-safe attempt identity
persist immutable commit descriptor referencing verified candidate and prior head
CAS tenant control: expected owner/epoch/revision/head -> new accepted head
apply locally and preserve retry metadata
return clean success
```

The descriptor PUT alone is NOT a commit. Reachability from the accepted control
head determines commitment. Unreachable descriptors are candidates/orphans too.
An epoch/head check followed by an independent unconditional commit write fails
the requirement even if both checks are fast.

Commit references must remain traversable after later commits and owner transfers.
Keeping only the newest request ID in a mutable row is insufficient to resolve
lost responses for earlier requests. No unbounded list in a single control object.

## 6. Authority Transition And Format Compatibility

This changes the meaning of durability in the new mode. State it explicitly:

```text
legacy sealed mode: validated WAL persistence is the durable commit record
new fenced mode: payload persistence + accepted control decision establish commit
```

Do not silently reinterpret all Phase 5-7 WAL as uncommitted candidates, or replay
all physically present Phase 8 WAL as committed history. Introduce explicit
configuration/namespace metadata identifying the protocol. Mismatches fail closed.

Version WAL, checkpoint, internal SQLite, receipt, and result formats when their
meaning changes. Preserve legacy decoding/checksum semantics where supportable.
Existing sealed-mode tests/demos must continue to work or receive an explicit,
tested compatibility path. Do not destructively regenerate user datasets.

It is acceptable to require fresh Phase 8 namespaces initially and reject
in-place legacy adoption. Document that boundary clearly. If migration is added,
it must anchor a fully verified sealed legacy prefix exactly once; no inference
from an empty listing or an arbitrary existing DB. Do not make migration a
second large project unless it is required for the chosen integration.

Same tenant sequence in two physical candidate objects is not automatically
corruption. Exactly one may be accepted. Two DIFFERENT accepted records at one
sequence are a fatal invariant violation. Update recovery validation accordingly.

## 7. Engine Integration And Outcomes

Integrate the protocol into ordinary execution, not only a special demo wrapper.
Both delta and multi-entity writes, duplicates, reads, checkpoints, restart, and
reconciliation must use the correct authority/admission path for their mode.

Suggested distinctions, with names adapted to existing result enums:

- Rejected before any persistence.
- Payload persistence unknown.
- Payload stored but commit not yet accepted.
- Commit decision unknown due to response loss.
- Commit accepted, local apply incomplete.
- Commit accepted and locally applied.
- Fenced/not current owner.
- Unavailable/corrupt authority or committed history.

Preserve known facts when later receipt/metrics/logging handling fails. A stored
candidate must not be labeled an accepted transaction. `remote_durable` alone
must not hide the distinction. Include epoch/attempt/decision identifiers where
useful, but never credentials or opaque security tokens in logs.

Possible serving lifecycle:

```text
UNOWNED -> ACQUIRING -> RECOVERING -> READY
READY -> RECONCILING | FENCED | FAILED | CLOSED
```

Document and test actual transitions. Missing control-plane evidence means no
normal serving. A stale local READY flag cannot authorize another operation.
One tenant's failure should not unnecessarily disable independent tenants.
Do not release the existing tenant synchronization boundary early for throughput.

Identical committed retries must return equivalent results without a new logical
commit/WAL sequence, even after takeover. Conflicting fingerprint reuse rejects.
A retry with unknown commit status first resolves authoritative history; it must
not create a second accepted incarnation under a new transaction/attempt ID.

Read behavior must be explicit. A conservative default is an authoritative
ownership/head check for each read plus a requirement that local state matches
the accepted head. Explain the read's linearization point and overlapping transfer
semantics. Reject on authority outage or stale cache. No cached lease/read-only
stale-serving optimization without a separate tested consistency contract.

## 8. Ownership Acquisition And Takeover

Provide explicit bootstrap/acquire/takeover/status operations with typed results.
The successor obtains a fresh session and higher epoch through the chosen atomic
primitive. Acquisition atomically preserves the committed head used for recovery.
Competing acquisitions must not both return the same valid ownership generation.

Takeover is an explicit authenticated operator/demo action in this phase. It does
not prove failure detection or authorization against a malicious owner. State the
crash/stale-process, non-Byzantine trust model. A stale runtime must not silently
reacquire itself through an internal retry loop when it receives FENCED.

Do not require A to release a lock, acknowledge revocation, stop its process, or
publish Phase 7's seal before B can fence it. B stays non-serving while restoring,
but A's already-issued old-token requests must be unable to add accepted history.
Do not mistake recovery duration for a lease that makes old requests harmless.

If B crashes during acquisition or recovery, C must safely take over again while
preserving the same accepted prefix. A timed-out acquisition might have succeeded;
resolve by operation/session/revision evidence, not repeated epoch increments
until one response looks successful.

## 9. Unknown Outcomes And Delayed Requests

For every network operation, state whether a timeout means REJECTED or UNKNOWN.
Never use absence at one GET/LIST as proof an outstanding write cannot arrive.

Required cases:

1. WAL PUT stored bytes then response was lost.
2. WAL PUT is delayed until after ownership transfer.
3. Descriptor PUT stored bytes then response was lost.
4. Commit CAS succeeded but its response was lost.
5. Commit CAS did not execute and the client timed out.
6. Commit CAS races ownership CAS; either may win, but history must be consistent.
7. Authority advances further before the first caller resolves its response.
8. An old SDK retry reaches the service after a new epoch is installed.
9. A delayed acquisition/activation response arrives after another takeover.

Use stable keys, canonical bytes, attempt IDs, and expected revisions across retries.
For a CAS protocol, explain when a revision-changing takeover makes a previously
pending old-revision CAS permanently ineligible. If the expected revision is still
current and a delayed CAS could succeed, uncertainty remains. Do not assume a
failed local wait cancels the service-side operation.

Resolving UNKNOWN requires positive authoritative evidence or a proved exclusion
rule derived from the atomic protocol. Publish that reasoning as a concise state
table, and test both outcomes. Do not hide uncertainty by relabeling every failure
as definitively rejected or every stored object as committed.

## 10. Batching Under Per-Tenant Fencing

Preserve one physical WAL batch containing records from multiple tenants. Commit
acceptance may be per tenant; do not invent a cross-tenant atomic transaction.

For a batch containing A and B, demonstrate:

- Payload upload can succeed for both.
- A can be accepted while B is fenced, rejected, or decision-unknown.
- A can apply/ACK after its own accepted decision without falsely committing B.
- B's record stays a non-authoritative candidate unless its own decision accepts it.
- Recovery filters according to accepted references, not batch membership alone.

An accepted descriptor must bind tenant, sequence, record checksum, batch key,
batch checksum/index or equivalent validated locator, and previous committed head.
Validate the full envelope/index before trusting an embedded record.

Do not resolve durability handles as successful merely when the batch PUT returns.
Audit whether commit-decision work belongs inside or above the batcher, and keep
worker error/shutdown semantics correct across queued, uploading, deciding, and
resolved states. Bound admission, waits, threads, and memory. Optional metrics
must remain observational. No unbounded decision thread pool to mask latency.

Fencing adds coordination requests that can erase some batching cost benefit.
Measure and disclose them. Do not preserve the old PUT-savings narrative by
omitting descriptor/control writes from the new mode's accounting.

## 11. Recovery Without A Pre-Crash Seal

Implement a new fenced recovery path, not `old_writers_stopped=True` under a new name.
The old cache and local attempt journal may be completely gone. The new runtime:

1. Acquires/fences ownership through authoritative control.
2. Captures the accepted committed head atomically with that acquisition.
3. Validates the remote bootstrap/checkpoint for that history.
4. Traverses bounded accepted descriptors/references to establish the exact suffix.
5. Fetches/verifies required single or batch payloads.
6. Replays only accepted records in tenant sequence order.
7. Validates logical/internal state, retry identity, and current authority again.
8. Installs into a fresh staging/destination and admits work only with valid ownership.

If takeover happens while recovery is running, the losing recovery must not become
an actively serving owner. A recovered cache may remain a diagnostic artifact,
but a stale READY flag or returned startup success cannot bypass future authority
checks. Test loss of ownership just before and just after local installation.

Required committed object missing/corrupt: fail closed. Unreferenced candidate
missing/corrupt: cannot be mistaken for a committed gap or application state.
Do not scan all orphan candidates and then reject a perfectly valid accepted
history because a stale writer produced another record at the same sequence.

The committed head identifies required trailing data, replacing the old seal's
role. The parent ACK ledger is not an input to recovery. Wall-clock timestamps,
object listing order, largest observed sequence, and multiple matching scans are
not substitutes for this authority.

Keep explicit budgets for descriptor traversal, WAL payload bytes, batch records,
checkpoint files, retries, pages, and total wait time. Detect loops and wrong
namespace references. No silent partial recovery when limits are reached.

## 12. Checkpoints And Safe Publication

Phase 8 checkpoints may include only accepted, locally applied commits. Never
checkpoint candidate-only mutations or unresolved decisions. Continue using SQLite
backup under tenant serialization and derive metadata from the snapshot itself.

Bind the checkpoint to its accepted-history position and protocol version, not
merely numeric sequence. A checkpoint at N must be an ancestor/prefix of the
captured accepted head used for recovery. A higher-sequence stale/orphan snapshot
must not win selection just because it is present in the bucket.

Specify whether checkpoint authority is registered through control CAS or proven
through authenticated/validated ancestry in the chosen trust model. If registered,
stale-owner registration must fail; uploaded bytes alone remain a candidate.
Allow writes after snapshot capture without incorrectly requiring that N still
equal the latest head at publication. A valid older prefix is not corruption.

Preserve identity/outcome metadata and the exact supported schema contract. Use
explicit version evolution rather than relaxing validation to accept arbitrary
tables or stripping internal rows so checksums pass.

Retain all payloads, descriptors, control history needed by the protocol, and
checkpoints. No garbage collection in Phase 8. Document why a shared batch can
contain both accepted and orphan records and still be required for other tenants.

## 13. Offline Authority And Multi-Process Demonstrations

Build the offline authority outside all disposable runtime directories. It must
implement real cross-process atomic compare-and-swap semantics and survive runtime
death. An external SQLite transactional control store is acceptable for this test
adapter; its successful serialization is local evidence, not a production remote
consensus claim. Do not use a copied Python dictionary, per-process mutex, or a
parent helper that always grants the desired winner.

Use typed store interfaces and the same protocol above both offline and real
adapters. Test the adapter's atomic primitive independently, including competing
processes and response loss. Avoid encoding richer offline semantics than the
AWS primitive actually supplies.

Required reproducible demos with bounded subprocesses and deterministic IPC:

### A. Stale Owner Resumes

A owns and commits a prefix. Pause A after its last ownership observation but
before commit acceptance. B takes over, restores, and commits. Resume A, including
its delayed upload/decision retry. Verify A's candidate cannot become accepted,
B's state stays correct, and both request outcomes are classified accurately.

### B. Unsealed Cache-Loss Recovery

A commits and returns real ACK messages to a parent verifier. Kill A without
`seal`, clean shutdown, release, or a cooperative final-boundary message. Delete
only A's validated disposable directory. B acquires authority and restores using
remote bootstrap/checkpoint plus committed history. Every ACK survives exactly
once; retries are idempotent; the next new write uses the next committed sequence.

### C. Commit Accepted But ACK Lost

Pause or kill A after commit acceptance and before the parent receives success.
B must recover the accepted unacknowledged transaction and resolve its retry.
Distinguish this from payload-only persistence, which must NOT be replayed as
committed. Test local-apply failure after accepted commit separately.

### D. Successor Interrupted

B fences A, then dies during recovery. C acquires a later epoch and finishes.
No history rollback, epoch reuse, lost ACK, or stale activation is allowed.

Run essential scenarios for both single and batched WAL. Include two tenants so
the suite demonstrates per-tenant isolation and real shared physical batches,
not just a batch wrapper around one record.

Use Events/Pipes/barriers and bounded joins, not sleep-based races. A parent
delayed-store helper may control request delivery, but must exercise the actual
authoritative operation after resumption rather than synthesizing an expected
answer. Clean children in finally blocks and leave actionable failure artifacts.

## 14. Independent Oracle And Protocol Model

Keep parent-owned ACK/request-intent/fingerprint records outside runtime caches.
Resolve intended committed operations from verified authority, then execute those
logical intents through an independent reference path that does not call the
recovery/replay implementation. Compare complete logical state, not only counts.

Check values, versions, transaction rows/items, audit history, relationships,
metadata, sequence/head, fingerprints, and post-recovery retry results. All ACKed
operations must be present; additional accepted unacknowledged operations are
valid and must be accounted for. Orphan payloads must not affect user state.

Add a small executable protocol state model or exhaustive bounded interleaving
test for two owners, two writes, takeover, and response loss. The model should
assert the invariants independently, and implementation traces should be checked
against allowed model transitions where feasible. Avoid a model that simply
calls the production commit method and asserts its own output is correct.

Use existing test tools or standard Python where sufficient; a new formal-methods
dependency is not required. A small honest model is preferable to a ceremonial
TLA+ file never executed or a claim of formal verification without a checked model.

## 15. Required Fault Matrix

Maintain a mapping from every item below to named tests and evidence. Parameterize
where that clarifies coverage, not to inflate test counts.

1. Conditional fresh bootstrap cannot overwrite existing authority/history.
2. Competing owners cannot share one valid generation; revisions/epochs never regress.
3. Reused runtime names do not reuse session authority.
4. Pause-after-owner-check race rejects stale commit acceptance.
5. Old WAL upload arrives after takeover without poisoning current history.
6. Old descriptor arrives after takeover but remains unreachable/non-authoritative.
7. Commit wins immediately before takeover; successor includes it.
8. Takeover wins immediately before commit; old commit cannot later succeed.
9. Stored payload/descriptor response loss preserves same key/content retry semantics.
10. Successful CAS response loss resolves even after later head advancement.
11. Nonexecuted/indeterminate CAS is not guessed committed or absent.
12. Delayed SDK retries cannot revive an old revision/session.
13. Accepted/local-failed write blocks local continuation until verified repair.
14. Duplicate and conflicting retries work across ownership changes for delta/multi writes.
15. Shared batch: one tenant accepted and another fenced/unknown.
16. Orphan candidate with conflicting sequence is ignored by authoritative replay.
17. Two conflicting accepted mappings are corruption, not arbitrarily deduplicated.
18. Missing middle and required trailing committed objects prevent READY.
19. Corrupt batch/index/descriptor, wrong namespace/tenant/epoch, cycles, and oversized
    metadata fail safely with bounded work.
20. Authority timeout/access denial/outage blocks serving; it is not an empty namespace.
21. Stale normal reads cannot bypass authority checks; overlapping read/transfer
    behavior matches the documented linearization contract.
22. Recovery loses ownership before activation/installation and cannot serve stale state.
23. Successor crashes during recovery; a later successor completes safely.
24. Stale checkpoint publication/registration cannot become the trusted latest prefix.
25. Checkpoint capture followed by normal commits still permits a valid older prefix.
26. Interrupted restore preserves existing directories and cleans only its own staging.
27. Worker exception/queue pressure/shutdown resolves waiters without false success.
28. Metrics/logging failure cannot change an accepted decision or unaccept a commit.
29. Deadline expiry does not claim cancellation of an in-flight service request.
30. Real process kill plus deletion of local DB/journal recovers without any pre-crash seal.
31. First post-takeover write uses the correct next sequence, not the epoch as a sequence.
32. Legacy and new protocol namespaces cannot be mixed silently; supported old tests pass.
33. Request counters include rejected/failed/retried logical control calls and separate
    payload, descriptor, control, recovery, verification, and cleanup traffic.
34. Partial/failure artifacts preserve primary errors, uncertain outcomes, and leftovers.

## 16. Metrics, Request Cost, And Experimental Honesty

Retain Phase 6 goodput/attempt separation and Phase 7 stage metrics. Add:

- Ownership acquire/takeover/recovery/activation durations.
- WAL payload, descriptor persistence, commit-decision, local-apply, and caller latency.
- CAS attempts/conflicts/unknowns, fences, epoch changes, orphan candidates, and
  accepted-but-unacknowledged operations.
- Recovery descriptor/object/byte counts and accepted/rejected/ignored record counts.
- Request counts by backend, operation, phase, and outcome using thread-safe attribution.

Document cumulative versus interval/delta counters and overlapping latency stages.
Do not use a globally mutable phase label to attribute concurrently active work
without isolation. Unknown wire retries/billable usage remain null. Never count
offline filesystem calls as actual AWS requests.

Report at least a small same-workload comparison between legacy payload-only
batching and fenced batching. Include EVERY descriptor/control read/write in the
new path, successful new-write goodput, bytes, and latency. No default large matrix,
fabricated AWS pricing, or p99 claims from a tiny sample. If overhead removes the
original request-savings benefit, report that plainly: this is valuable architecture
evidence, not a defect to conceal.

The control plane's own availability/durability is now part of the architecture.
S3 Standard or a multi-AZ control store does not make an S3 Express payload survive
permanent loss of its AZ. A committed reference to unavailable data means recovery
cannot proceed; do not label that as zero-RPO multi-AZ durability.

## 17. AWS Adapter Verification And Cost Controls

Implement the real adapter for the chosen authority with normal SDK credential
resolution, bounded connect/read/retry settings, and official API semantics.
Browse primary AWS/Python documentation for newly used concurrency primitives.
Record supporting links and semantic assumptions in the ADR.

Add `aws` tests gated by RUN_AWS_TESTS=1 plus all required configuration. Cover
conditional bootstrap, competing CAS/takeover, stale commit rejection, lost-response
verification where controllably injectable, and tiny checkpoint/WAL recovery in
both persistence modes. Missing SDK/configuration/credentials must skip clearly.
Offline suite execution must never contact AWS, including credential metadata APIs.

Cloud tests use a unique, isolated run prefix or tenant partition and print a
request/size envelope before operations. Do not create buckets/tables, alter IAM,
enable public access, or deploy services. Add any new env settings to `.env.example`.
Use an existing configured general-purpose bucket/control table as appropriate.

No deletion of authoritative objects while delayed requests can still complete.
Retain ambiguous/failed evidence. Any successful test cleanup must be scoped to
exact run-owned keys/items, report leftovers, and never reset another run's owner.
Do not run chargeable cloud tests merely because credentials happen to exist;
require explicit enablement. Missing live AWS access is not a reason to abandon
the offline implementation, but final claims must distinguish adapter tests from
real remote fencing evidence.

## 18. Milestones And Stop/Repair Gates

Maintain this progression in the work log and continue automatically within scope:

1. **Audit:** baseline, current contract map, narrow prerequisite regressions.
2. **Protocol:** ADR, authority model, race traces, and bounded executable model.
3. **Primitive:** typed control/descriptor formats and persistent multi-process CAS tests.
4. **Commit path:** engine integration, outcomes, single/batch acceptance and idempotency.
5. **Takeover/recovery:** unsealed boundary acquisition, validated replay, safe activation.
6. **Proof:** real subprocess stale-owner and cache-loss scenarios plus independent oracle.
7. **Adapters/accounting:** real gated AWS adapter/tests and complete cost attribution.
8. **Hardening:** full fault matrix, compatibility, budgets, teardown/error artifacts.
9. **Evidence:** fresh final quality gates, final demos, docs/report, concise handoff.

If a milestone fails, repair or revise its design before building on it. Do not
mark a milestone complete because classes exist or a mock returns success.
Revisit the ADR when a counterexample changes the protocol. Preserve useful failing
tests and record what they exposed. Do not repeatedly run the full suite without
purpose; use focused checks during iteration and a fresh full suite after final
functional changes. Never finish with relevant test processes still running.

Prefer existing code patterns, typed explicit interfaces, small modules, and
versioned migrations. Avoid broad Any/getattr-based protocol dispatch in new core
logic when a Protocol or tagged model can state the contract. Reuse parsers and
serialization primitives. Refactor existing internals only where the new safety
boundary makes it necessary, with regression coverage.

## 19. Required Documentation And Evidence

Create/update:

- `docs/adr/0003-fenced-commit-and-takeover.md`.
- `docs/ownership-fencing.md`: authority, commit semantics, state transitions,
  trust/failure model, read behavior, and unknown-outcome resolution table.
- `docs/phase-8-fault-matrix.md`: requirement -> named test -> observed outcome.
- `docs/phase-8-work-log.md`: resumable implementation state and command evidence.
- `docs/phase-8-report.md`: actual results, limitations, architecture/cost tradeoffs.
- README, architecture, invariants, failure model, WAL/batching, checkpoint/recovery,
  benchmark methodology, cost safety, and configuration examples as affected.

Versioned JSON artifacts must include configuration, namespace/backend/protocol,
software versions/revision when available, deterministic workload identity, epochs,
attempt/commit IDs, synchronization/crash events, raw outcomes, ACK ledger identity,
state checksums, observed acknowledged loss, request/byte counts, stage timings,
cleanup status, primary/teardown errors, and paths to retained evidence.

Unknown/unavailable metadata is null with a reason, not an invented value. Separate
observed recovery delay and transaction-loss counts from production RTO/RPO claims.
Published claims must survive someone reading the code and replaying the artifacts.

Provide exact runnable commands for bootstrap/status, normal execution, explicit
takeover, recovery, stale-owner demo, unsealed cache-loss demo, fault tests, and
opt-in AWS verification. Names may follow existing CLI conventions; document the
actual commands implemented, not hypothetical interfaces.

## 20. Definition Of Done And Final Response

Run the actual available equivalents of:

```powershell
python -m pytest -q
ruff check .
mypy src
python -m ephemeraldb.checkpoint.cli --help
python -m ephemeraldb.recovery.demo --help
# New Phase 8 CLI help, stale-owner demo, and unsealed-recovery demo commands.
# Explicitly gated AWS selection; skip without required enablement/configuration.
```

Also rerun the legacy sealed proof and verify all final artifacts against their
claims. Add the exact Phase 8 commands to the report. Do not use historical test
counts as this run's verification result.

Phase 8 is complete only when the integrated protocol and executable evidence
establish all of the following:

- A still-alive stale runtime cannot add a new accepted commit after takeover.
- A delayed payload/decision request cannot violate the accepted history.
- Accepted-but-unacknowledged writes survive; candidate-only writes are excluded.
- Unsealed process/cache loss recovers from remote authority without the old journal,
  cooperative stop/seal, or parent ledger as algorithm input.
- Same logical retry remains idempotent; conflicts reject; next sequence is correct.
- Single and shared-batch modes work with per-tenant decision outcomes.
- Ownership lost during recovery cannot activate a serving stale runtime.
- Missing/corrupt required data fails closed, and uncertainty is not fabricated away.
- Coordination overhead is fully accounted for and existing compatibility is explicit.
- Full tests/lint/types and deterministic demos pass, with AWS evidence honestly labeled.

The final answer should briefly give implemented behavior, the chosen linearization
point, concrete stale-owner/cache-loss results, actual verification counts, evidence
paths, reproduction commands, and limitations. Link the detailed report instead of
dumping all implementation notes into chat. Identify remaining distributed operations,
failure detection, scan/metadata scalability, IAM hardening, and shared-batch retention
work without implementing them automatically.

Do not say "fencing implemented" if the actual mechanism is only a preflight owner
check. Do not say "unsealed recovery" if the demo secretly seals before killing.
Do not say "AWS verified" for a local adapter. Finish the real bounded proof.
