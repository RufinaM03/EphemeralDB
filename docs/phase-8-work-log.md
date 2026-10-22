# Phase 8 Work Log

## Objective

Implement and prove fenced per-tenant ownership and unsealed cache-loss recovery. This
file is the resumable execution record for `phase-8-fenced-ownership-recovery.md`.

## Milestones

- [x] Audit current execution, WAL, checkpoint, recovery, and evidence contracts.
- [x] Verify the proposed S3 conditional-write primitive against official documentation.
- [x] Write ADR 0003 before implementation.
- [x] Implement typed authority/control/descriptor formats and persistent CAS adapters.
- [x] Integrate fenced commit and read admission into the ordinary engine.
- [x] Implement bounded accepted-history traversal and unsealed recovery.
- [x] Add single/shared-batch adversarial and subprocess evidence.
- [x] Add gated AWS tests, accounting, CLI, and documentation.
- [x] Run the complete fault matrix and final quality gates.

## Audit And Decisions

- Baseline contract: Phase 7 treats a persisted WAL payload as committed and requires a
  seal plus an operator assertion that old writers stopped. `owner_epoch` is informational.
- Phase 8 uses a fresh, explicitly fenced namespace. Legacy namespaces retain their old
  meaning; there is no implicit migration.
- Authority is one mutable per-tenant control object. Owner session/epoch, revision,
  committed sequence/hash head, descriptor head, and bootstrap identity move atomically.
- A commit linearizes only at successful control-object CAS. Payload and descriptor PUTs
  are immutable candidates until the descriptor becomes reachable from control.
- Takeover linearizes at CAS of the same object, preserving the accepted head while
  incrementing epoch and revision. This defeats a cached preflight ownership check.
- Offline evidence uses an external SQLite database and `BEGIN IMMEDIATE` transactions;
  it is a multiprocess adapter, not a claim of distributed consensus.
- S3 Standard is the remote adapter. ETags are opaque CAS validators, not content hashes.
- No authority object deletion/reset and no garbage collection are provided in Phase 8.

## External Semantic Evidence

- AWS conditional writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- S3 PutObject `If-Match`: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
- S3 consistency model: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel

The documentation states that `If-None-Match: *` conditionally creates, `If-Match`
updates only the matching ETag, conflicts can return 409/412, and successful single-key
updates are atomic with strong read-after-write consistency.

## Command Evidence

- `ruff check .`: passed before changes.
- `mypy src`: passed before changes (71 source files).
- `python -m pytest -q` baseline: 180 passed, 9 skipped in 88.13s.
- Phase 8 focused gate: 16 passed, 1 AWS skipped before final hardening; the final full
  run includes all settled Phase 8 tests.
- Single and batch stale-owner demos: passed; accepted history contains only successor.
- Single and batch unsealed cache-loss demos: passed; zero ACK loss, retry duplicate,
  next sequence 2.
- Single and batch ACK-loss demos: passed; one accepted-unacknowledged write recovered.
- Legacy Phase 7 batch sealed proof: passed with zero acknowledged loss.
- Same-workload accounting (two writes/one shared batch): legacy 1 immutable call; fenced
  2 creates + 3 immutable calls + 2 control GET + 2 control CAS, equal 1,987 payload bytes.
- A focused shared-batch run exposed a scheduling-dependent test split into two physical
  batches. The proof now uses a two-party barrier before bounded batch admission; focused
  rerun passed. An attempted `pytest --count=5` was unavailable because pytest-repeat is not
  installed; no dependency was added, and the deterministic focused rerun passed.

## Required Review Passes

- Protocol review: attacked bootstrap/create ambiguity, commit-before/after takeover,
  delayed old CAS, response loss after service apply, later-head resolution, checkpoint
  publication races, read/transfer overlap, and recovery activation loss. Commit and takeover
  share one control CAS lineage; candidate existence never authorizes replay.
- Implementation review: traced engine prepare/commit/apply, duplicate and conflicting retry,
  single/batch locators, strict canonical decoding, byte/traversal bounds, SQLite multiprocess
  transactions, S3 conditional status mapping, worker close behavior, checkpoint registration,
  staging cleanup, result facts, and request counters.
- Evidence review: the independent 30-ordering model does not call production protocol code;
  subprocess demos perform actual CAS/process termination; recovery does not consume the ACK
  ledger; complete state/retry/head checks supplement outcome counts. Live AWS remains gated.

## Next Action

Final settled-tree suite: 199 passed, 10 skipped in 90.32s. Ruff and strict mypy pass across
82 source files. Live AWS remains an explicit opt-in gate and is not claimed by offline
results. Phase 8 evidence is complete.
