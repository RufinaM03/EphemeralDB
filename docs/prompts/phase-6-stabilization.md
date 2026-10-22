# Implementation Prompt: Phase 6 Stabilization

You are continuing **EphemeralDB Benchmark** in the current workspace.

Implement this prompt end to end. This is an implementation task, not a request
for another plan. Inspect the current repository before editing, preserve user
changes, and adapt names and interfaces to the code that actually exists.

The project is a Python database/infrastructure experiment exploring independent
tenant SQLite databases, disposable compute, explicit logical WAL persisted to
S3 Express One Zone, and cross-tenant batching. Its portfolio goal is to provide
reproducible evidence of correctness and architectural tradeoffs.

## 1. Scope and Completion Contract

This task is **Phase 6 stabilization**. Correct unsafe continuation and retry
semantics, make batching lifecycle behavior reliable, and repair benchmark
measurement. Do not proceed to Phase 7 automatically.

Keep both single-record and batch modes. Normal development and tests must make
zero AWS calls. Test doubles are appropriate for deterministic fault injection;
they are not evidence of real AWS durability or performance.

Out of scope: checkpoint implementation, complete cache-loss recovery,
distributed ownership/fencing, parallel batch upload workers, Aurora, EKS,
Terraform, autoscaling, frontend, and business-domain features. A small remote
history inspection/reconciliation facility is in scope when needed for safe
restart. Do not build a distributed service to solve this local phase.

Completion requires:

- No subsequent tenant write can reuse a sequence whose remote outcome remains
  unresolved or whose durable content has not been reconciled locally.
- Identical retries are idempotent; conflicting transaction-ID reuse is rejected.
- Worker failure and shutdown cannot silently strand pending callers.
- Benchmark output separates successful work from attempts and execution from
  cleanup, with accurate metric definitions and request-accounting limitations.
- Focused regressions, offline quality gates, and an evidence report pass.

## 2. Read the Existing System

Read README.md, docs/portfolio-assessment.md, docs/invariants.md,
docs/failure-model.md, docs/wal.md, docs/wal-batching.md,
docs/benchmark-methodology.md, and relevant implementation/tests.

Pay particular attention to:

- engine/executor.py, engine/engine.py, engine/results.py, engine/connection.py.
- wal/sequence.py, wal/replay.py, wal/models.py, wal/codec.py, wal/durability.py.
- wal/batching/batcher.py, queue.py, models.py, metrics.py.
- aws/s3_express.py and benchmark/wal_batching.py.
- Tests for Phase 5 durability, WAL replay, and Phase 6 batching.

Verify findings against current code. Establish a baseline with pytest, Ruff, and
mypy. Record failures that predate your changes; do not erase or bypass them.

## 3. Reproduce Before Repair

Add focused regressions for these observed behaviors:

1. Persist A remotely, inject failure before local commit, then submit a different
   B for the same tenant. Existing batch mode can acknowledge B at the same
   sequence as A, with both records stored in distinct batches.
2. Commit an operation, then reuse its transaction ID with a different mutation.
   The existing duplicate path can return success without checking request identity.
3. Benchmark elapsed time includes S3 cleanup; single mode deletes more objects.
4. Successful store-call counters omit failure/retry traffic and hardcode GET=0.
5. All failed operations can still produce positive TPS and apparent PUT savings.

Use small datasets and controlled test hooks. Do not depend on AWS or timing
races to reproduce these defects.

## 4. Define Transaction Outcomes Precisely

Represent or clearly distinguish:

- Rejected before persistence was attempted.
- Persistence definitively rejected, where evidence supports that conclusion.
- Remote outcome unknown, including response loss after the object was stored.
- Remote durability confirmed, local application incomplete.
- Remote durability confirmed and local application complete.
- Acknowledgment observed by a caller versus merely prepared for delivery.

Keep backward-compatible fields where feasible, but do not use
remote_durable=false to imply definitely absent when the actual outcome is
unknown. Introduce a typed status or equivalent unambiguous representation.
An unknown outcome must not look like a safe-to-retry-new-write failure.

Document the commit point and read visibility. Remote persistence and local
SQLite commit are not one atomic transaction. Never delete remote WAL to undo a
local apply failure. Never reinterpret a lost response as proof of failed storage.

## 5. Tenant Serving State and Reconciliation

Introduce a small serving-state model such as READY, RECONCILING, and FAILED,
with explicit startup behavior. Transition under the tenant synchronization
boundary. Use the existing per-tenant lock across prepare, durability wait, and
local apply; do not release it early to improve throughput.

After a confirmed durable/local failure or unresolved remote attempt:

- Stop subsequent writes for that tenant, including different transaction IDs.
- Return structured status explaining whether reconciliation is required.
- Keep independent tenants usable unless the shared durability worker has failed.
- Default to rejecting normal reads while the tenant is unresolved; another read
  policy requires an explicit consistency contract and tests.
- Do not reset the state merely because another request arrived or time elapsed.

Implement reconciliation for a known durable record where safe: validate the
record and chain, apply it atomically, restore sequence/checksum and idempotency
metadata, and only then resume serving. Identical reconciliation must be safe.
Conflicting history must quarantine the tenant, not choose a record arbitrarily.

## 6. Restart and Remote Ambiguity

An in-memory reconciliation flag alone is insufficient. Define how a restarted
engine establishes that local materialized state agrees with authoritative
history before accepting writes. Scope this phase to the existing single-writer,
single-runtime assumption and state its limits explicitly.

Acceptable behavior is verified reconciliation or an actionable fail-closed state.
Do not claim transparent restart recovery if the implementation only blocks.
Healthy, verifiable history should remain usable; do not make remote mode
permanently unusable as a shortcut.

Specific requirements:

- Same attempted record/batch retries retain the exact object key and canonical
  bytes; batch IDs must not regenerate during retry of an unresolved attempt.
- Preserve enough attempt identity for reconciliation, or explicitly refuse
  automatic resumption when that identity cannot be established after restart.
- A local journal can assist diagnosis but cannot become the sole source of
  truth for remote state or survive an assumed complete cache loss by definition.
- A single 404 or an empty scan is not proof that an in-flight timed-out request
  cannot later complete. Explain the safe resolution rule. Remain blocked if the
  protocol cannot establish a definitive outcome.
- Reject sequence gaps, mismatched previous checksums, and different records for
  the same tenant/sequence. Identical duplicate records may be deduplicated after
  validation. Validate batch and embedded record integrity.
- Use pagination and explicit per-tenant sorting for any remote discovery. Do
  not assume object-list order or wall-clock timestamps define tenant WAL order.
- Do not add a remote index write per transaction and erase batching's benefit.

Write a short ADR before substantial implementation describing this bounded
restart protocol, uncertainty handling, and its prerequisites. Continue with the
implementation after recording the decision; do not stop at the ADR.

## 7. Request Identity and Durable Idempotency

Define a versioned canonical request fingerprint using the logical write intent:
tenant, operation type, target entities, deltas, expected versions, and any other
field affecting execution. Decide whether item order is semantic and test that
decision. Exclude scheduling offsets, runtime IDs, measured timing, and batch IDs.

Persist the fingerprint and required outcome metadata atomically with local
application, and carry sufficient information in durable WAL to reconstruct it.
Ensure replay restores retry semantics as well as user-visible rows.

Required behavior:

- Same tenant + transaction ID + fingerprint: no new logical application or WAL
  sequence; return an equivalent committed outcome when known.
- Same transaction ID + different fingerprint: explicit conflict, no new PUT.
- A retry during an unresolved attempt follows reconciliation rules; it does not
  bypass the serving-state gate or create another batch identity.
- A write committed before ACK loss remains idempotent after reconciliation and
  restart.
- Define whether transaction IDs are tenant-scoped or globally enforced. Do not
  imply global enforcement if uniqueness is only checked within tenant files.

Do not silently change an existing WAL version's checksum semantics. If durable
formats or internal SQLite schema must evolve, version them, retain explicit
legacy decoding where supportable, and test migration/rejection. Legacy records
may lack request fields such as expected_version; do not invent that information.
Document restricted retry behavior when safe equivalence cannot be established.
No destructive regeneration of existing user datasets as a migration strategy.

## 8. Batcher Lifecycle and Resource Limits

Track queued, sealed/in-flight, and resolved entries so every admitted request
reaches a terminal outcome. On a fatal worker exception:

- Stop admission and wake blocked producers, flush callers, and shutdown callers.
- Resolve/fail all unresolved handles, including entries removed from the queue.
- Preserve unknown versus confirmed-durable outcomes if persistence may have
  completed before a later serialization/metrics/receipt failure.
- Make close idempotent with documented error behavior; prevent self-join and
  worker-death deadlocks.

Test failures in batch construction, store invocation, and post-persistence
processing. Optional metrics/logging failures must not rewrite durability history.
Use bounded waits/deadlines where needed; timeout must not cancel a remotely
committed operation or falsely imply it did not commit. SDK I/O must be bounded.

Preserve bounded queue behavior, manual flush, count/byte/window triggers, and
shutdown policy. Define a maximum encoded record/batch size or explicitly name
payload-only limits; oversized records cannot silently bypass an advertised hard
limit. Include serialization envelope/index overhead if limiting object bytes.

Either implement a tested hard connection admission bound or rename/document the
existing idle-cache eviction target and bound benchmark worker admission. Avoid
closing active connections. Keep read serialization and one-upload-worker behavior
unless a correctness repair requires change; disclose these performance limits.

## 9. Benchmark Measurement Repairs

Use monotonic timestamps and define independent intervals:

- Preparation/setup and optional warmup.
- Submitted/scheduled arrival to execution start.
- Tenant/admission waiting, batch queue/formation, remote persistence, local apply.
- Measurement completion after all measured outcomes resolve.
- Shutdown/drain, remote cleanup, artifact export.

Stop primary workload timing before cleanup. Preserve the original exception when
cleanup also fails. Write a partial/failure artifact when feasible, including
cleanup status and run prefix. Do not silently lose results on teardown errors.

Report attempted throughput separately from successful goodput and unique durable
writes. Exclude duplicate retries from new-write throughput. Show latency
distributions separately for successes, rejections/conflicts, failures, and unknown
outcomes. Clearly label inclusive durations; do not add overlapping intervals.

Capture executor-queue delay. Do not describe the current submit-all thread pool
as an open-loop rate-controlled traffic generator. Preserve the many-tenants and
single-hot-tenant comparisons and disclose concurrency/arrival behavior.

## 10. Request Accounting and Experimental Honesty

Use thread-safe counters with precise names. Distinguish logical persist calls,
successful object creations, existing-object confirmations, failed SDK calls,
request attempts where instrumentable, and bytes sent/stored.

Count GET/LIST/DELETE paths actually used, including reconciliation, verification,
and cleanup. Split workload traffic from setup/recovery/cleanup traffic. If using
SDK event hooks, verify their supported behavior against current official docs
and test retry counting. Do not label application counters as AWS billing truth.
Unavailable measurements must be null/explicitly unsupported, not invented zero.

Savings calculations require valid comparable successful work. An all-failure run
must not report 100% useful request savings. Report zero-denominator ratios as
undefined and explain why. Do not hardcode current AWS prices.

Artifacts should include schema version, seed, dataset/workload identity,
configuration, Python/SQLite/SDK versions, revision when available, placement
when known, worker count, raw outcomes, timestamp definitions, and cleanup status.
Mark unavailable Git/AZ metadata honestly. Preserve old consumers where practical
or document artifact version changes.

Keep AWS defaults small: approximately 100 transactions and the existing explicit
large-run opt-in. Print the run envelope and isolate objects under a unique prefix.
Do not claim an exact request upper bound that excludes retries or reconciliation.
No automatic bucket creation or infrastructure provisioning.

## 11. Required Verification

Use Events/Conditions/barriers and injected clocks where appropriate, not long
sleeps or tight timing assertions. In addition to existing tests, cover:

1. Durable A/local failure followed by B cannot produce conflicting sequence N.
2. Other tenants proceed when only A's tenant needs reconciliation.
3. Correct reconciliation permits B at N+1; repeated reconciliation is idempotent.
4. Store saves bytes then raises a timeout: result is unknown and new writes block.
5. Restart after durable/local failure and after unknown outcome does not reset
   the tenant to READY without evidence.
6. Lost ACK after local commit followed by identical retry creates no new write.
7. Conflicting ID reuse for delta and multi-entity writes rejects without PUT.
8. Replay restores fingerprint/outcome state; legacy behavior is explicit.
9. Worker exceptions at each lifecycle stage resolve all waiters and reject new work.
10. Shutdown with queued/in-flight operations has bounded documented behavior.
11. Queue and connection resource contracts hold under concurrent admission.
12. Artificially expensive cleanup does not change workload runtime/goodput.
13. Failed and duplicate operations do not inflate new-write goodput or savings.
14. Retry/verification/cleanup request counts match controlled client behavior.
15. Partial artifacts survive failures and report cleanup errors accurately.

Run pytest, ruff check ., and mypy src. Inspect the actual commands/interpreter
available in this workspace. Do not install unrelated dependencies. Run real AWS
tests only with explicit test enablement and valid configuration; otherwise report
them skipped. Never turn local recording-store results into cloud evidence.

## 12. Documentation and Final Deliverables

Update affected contracts and README. Add an ADR covering durability outcomes,
restart/reconciliation, and retry identity. Document the batcher state machine,
metric definitions, compatibility changes, and remaining boundaries. State that
S3 Express durability is single-AZ and distributed ownership is not implemented.

Add docs/phase-6-stabilization-report.md with reproduction evidence, repairs,
test results, unresolved limitations, and exact offline verification/demo commands.
Separate current facts from proposed capabilities and historical test counts.

Final response should summarize:

1. Verified defects and repairs with file references.
2. Transaction/tenant state transitions and restart behavior.
3. Retry fingerprint and format/schema compatibility decisions.
4. Worker lifecycle and resource-limit behavior.
5. Corrected benchmark metrics and counting limitations.
6. Actual tests/lint/type results and AWS run-or-skip status.
7. Known limitations and readiness for the Phase 7 prompt.

Do not claim all unknown outcomes can be resolved if the implementation safely
blocks some of them. Do not automatically begin checkpointing or deploy AWS.
