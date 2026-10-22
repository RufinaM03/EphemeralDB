# Phase 7 Report

Implemented: consistent full-database checkpoints, immutable S3 Standard publication,
bounded sealed-run restore/WAL replay, and a real subprocess cache-loss proof.
Distributed ownership, deployment, automatic retention, and Phase 8 are not started.

## Prerequisite Audit

Phase 6 stabilization was present: unresolved writes/read admission gates, retained
attempt identity, restart verification, replayed fingerprints, bounded batch-worker
lifecycle, and cleanup-independent goodput accounting. Existing regressions remain
passing. No prerequisite safety bypass or automatic reset was introduced.

Two narrow extensions support Phase 7: SEALED source tenants cannot resume through
normal admission or reconciliation, and S3 Express discovery now bounds downloaded
bytes as well as object/record counts. Limits are 64 MiB/object, 128 MiB/scan,
1,000 objects, and 10,000 total records including irrelevant tenant records.

## Implementation Map

- `checkpoint/models.py`: strict versioned canonical manifests and quiescent boundaries.
- `checkpoint/snapshot.py`: SQLite backup, exact schema, standalone integrity,
  tenant/sequence/hash binding, logical and separate internal checksums.
- `checkpoint/store.py`: streaming object interface and explicitly offline authority.
- `checkpoint/coordinator.py`: capture/publication, candidate discovery, source sealing.
- `aws/s3_standard.py`: separate general-purpose bucket, STANDARD conditional PUT,
  streaming GET, positive read-back resolution, pagination and SDK-call accounting.
- `recovery/coordinator.py`: staged restore, checkpoint/remote-WAL cross-checks,
  suffix replay, retry restoration, boundary verification, atomic new-directory install.
- `recovery/offline.py`: filesystem WAL authority supporting single and shared batches.
- `recovery/demo.py`: parent ledger, real child kill/cache deletion, independent oracle.
- `checkpoint/cli.py`: create/list/validate/seal/restore and failure artifacts.
- Configuration, README, architecture, invariants, failure model, cost safety, WAL,
  engine and S3 notes updated. ADR 0002 records the protocol and rejected shortcuts.

The unused Phase 0 bytes-only checkpoint placeholder remains compatible; the new
file-streaming interface handles real snapshots rather than coercing them into bytes.

## Consistency And Publication

Snapshots use SQLite's supported backup API under the tenant lock without an active
write transaction. Metadata derives from the snapshot; remote upload does not hold
the tenant lock. Validation includes supported DDL, SQLite integrity, foreign keys,
tenant identity, version history, and complete fingerprint/transaction metadata.
Sequence-zero snapshots are valid and include initialized internal schema.

Immutable snapshot upload/read-back precedes manifest publication/read-back. Orphan
snapshots are not candidates. Same-attempt retries retain keys/content; conflicting
objects fail. ETag is not used as SHA-256. Candidate order is numeric tenant sequence,
not time/list order. Equivalent newest candidates validate; conflicting or corrupt
newest candidates fail without fallback. Service/access errors do not select stale
state. Version-1 logical checksum remains unchanged; separate internal digest covers
retry semantics, excluding physical keys that can differ between verified WAL copies.

## Recovery Contract

The source publishes an immutable quiescent boundary after marking itself SEALED.
This supplies an independent remote required final sequence/head and state digests;
the parent's ACK ledger is an oracle, not the recovery algorithm's authority.
No boundary, missing required trailing record, unexpected suffix, corruption, or
unresolved history permits READY. Old writers must be stopped by the operator.

Recovery validates retained history from sequence 1, anchors at checkpoint N, and
replays only N+1 onward. Fingerprints/outcome metadata are restored without allocating
new sequences. A new registry/journal is constructed only after final logical/internal
checks pass. Staging stays invisible to serving connections and is renamed into a
previously absent directory. Existing datasets are never replaced. Failed restores
remove staging and preserve authoritative artifacts for another attempt.

## Actual Cache-Loss Evidence

Final retained artifacts:

- `benchmark/results/recovery/phase7-proof-batch/result.json`
- `benchmark/results/recovery/phase7-proof-single-beforeack/result.json`

Both use Python 3.12.7 / SQLite 3.45.3, seed 42, two tenants, four writes per tenant,
200,704-byte snapshots, sequence-zero bootstrap, and a later checkpoint at 2.
The parent forcibly terminated the child (exit code -15), confirmed termination,
deleted its entire cache, then restored from external authorities into fresh paths.
Git revision and absent boto3 metadata are null, not invented.

| Scenario | ACKed | Durable Without ACK | ACK Loss | Checkpoint -> Recovered -> New Write |
| --- | ---: | ---: | ---: | --- |
| Batch, post-ACK kill | 8 | 0 | 0 | 2 -> 4 -> 5 per tenant |
| Single, ACK suppressed then sealed kill | 7 | 1 | 0 | 2 -> 4 -> 5 per tenant |

All retries were duplicates without additional WAL. Independent execution uses
logical operation intent through the ordinary engine without replay. Full logical
checksums match, covering values, versions, transaction items, audit history,
relationships, and metadata; IDs occur exactly once and retry metadata validates.
Recovered tenant checksums in both scenarios:

```text
24f483cedd15c52da97036d0630abbc20b0d3c7d07bbddf9877c2c4e11772039
77e3d5fc5fec414d16793619b82b0b7dd241c307eef2a4376b5eba6402d61e58
```

Observed per-tenant recovery times were 80.54/91.53 ms (batch) and 84.48/72.07 ms
(single ACK-loss). For the first batch tenant, milliseconds by stage were:

| Discovery | Download | Snapshot Validation | WAL Fetch/Decode/Validate | Replay | Final Verification | Install |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 15.12 | 2.72 | 9.62 | 11.67 | 14.39 | 25.23 | 1.40 |

The first batch restore scanned four shared objects and four irrelevant records.
The second scanned five objects because the first restored tenant had already
successfully added its sequence-5 record. Per-restore counters exclude earlier
traffic: two WAL LISTs and four/five GETs respectively, zero recovery PUTs. This
exposes shared-batch scan amplification; it is not an AWS request-cost measurement.
Full nanosecond stages, head hashes, identities, bytes, and raw outcomes are in JSON.

Observed RPO is zero acknowledged transactions for this sealed process/cache-loss
class only. These are tiny offline correctness runs, not cloud performance, a
production RTO, or permanent AZ-loss evidence. The durable-before-ACK scenario
suppresses ACK before sealing known applied state; it does not resolve an arbitrary
in-flight timeout. Unresolved runs without a stable boundary remain non-serving.

## Verification

Full regression suite: **180 passed, 9 skipped in 93.62 seconds**. Following the
final per-restore accounting adjustment, **39 focused tests passed in 32.50 seconds**,
including the four single/batch x post-ACK/durable-before-ACK real process-kill cases.
Ruff passed. Strict mypy passed across **71 source files**. Both CLI help commands
and both retained final CLI demonstrations passed.

Seven explicitly selected AWS tests skipped because RUN_AWS_TESTS was not enabled;
two other skipped tests are optional dataset/workload scale runs. No AWS call,
dependency installation, bucket creation, or infrastructure deployment occurred.
The SDK is absent in this local interpreter; cloud behavior has adapter-level
tests but is not live-AWS-verified.

Offline tests cover coordinated capture, sequence zero/nonzero/idle tenants,
standalone snapshots, unresolved checkpoint rejection, orphan publication,
stored-then-response-loss/identical retries, immutable conflicts, invalid manifests,
snapshot corruption/schema/tenant/sequence/head/retry tampering, paginated unordered
listing, equivalent/conflicting candidates, shared batches, gaps/trailing loss,
duplicate copies, interrupted restore, preserved destinations, and failure artifacts.
During development the first demo exposed missing boto3 metadata; dependency
reporting now handles absence explicitly. An invalid-key regression also caught
local validation being mislabeled as remote ambiguity; key validation now precedes
SDK accounting and invocation. Both were fixed rather than bypassed.

## Reproduce

```powershell
python -m pytest -q
ruff check .
mypy src
python -m ephemeraldb.checkpoint.cli --help
python -m ephemeraldb.recovery.demo --help
python -m ephemeraldb.recovery.demo --mode batch
python -m ephemeraldb.recovery.demo --mode single --scenario durable-before-ack
python -m pytest -m aws -q
```

The demo defaults to new UUID output directories. For opt-in AWS setup/commands
and manual create/list/validate/seal/restore, see `docs/recovery-demo.md`.

## Remaining Work

No distributed fencing, ownership transfer, routing, arbitrary unsealed crash
recovery, or multi-pod orchestration. Sealing adds one boundary publication per
controlled run/tenant, not an index write per transaction. Full-history scans and
in-memory bounded decoding do not scale indefinitely. Legacy retry-incomplete
snapshots reject. All WAL/checkpoints/orphans are retained; safe shared-batch garbage
collection needs a future retention proof. S3 Standard durability does not upgrade
later S3 Express One Zone WAL to multi-AZ. Phase 8 must address ownership and
unquiesced recovery explicitly rather than treating this bounded proof as fencing.
