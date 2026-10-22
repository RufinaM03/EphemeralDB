# Phase 6 Stabilization Report

Scope: correctness and measurement stabilization only. Phase 7 has not started.
This report describes implemented local behavior, not a deployed AWS result.

## Reproduction Evidence

The baseline passed 102 tests with 7 skips; Ruff and mypy passed. New regressions
failed against the prior behavior before repair:

- Durable A/local-apply failure allowed a different B to reuse A's tenant
  sequence in another batch. Admission now blocks B and normal reads until A is
  positively reconciled; independent tenants can continue.
- Committed transaction-ID reuse with changed mutation returned duplicate
  success. Fingerprint comparison now rejects changed or unverifiable intent.
- All-failure and duplicate-only summaries inflated useful throughput/savings.
  Goodput now counts successful new writes, with invalid ratios/savings null.

Code inspection also identified cleanup inside workload timing and successful
store-call counters omitting failed/verification traffic. Controlled tests now
inject ten seconds of cleanup without changing runtime/goodput, and check failed
PUTs, conditional-write GETs, paginated LISTs, and DELETE accounting.

## Implemented Protocol

`engine/durability_guard.py` journals canonical attempt identity before submission
and gates tenant reads/writes under the existing tenant lock. READY admits work;
unresolved PENDING attempts expose RECONCILING; validation failures quarantine as
FAILED. Remote durability and SQLite COMMIT remain separate events.

Results distinguish NOT_ATTEMPTED, REJECTED, UNKNOWN, DURABLE_NOT_APPLIED,
DURABLE_APPLIED, and LOCAL_APPLIED. Remote errors do not prove absence. A returned
success is not evidence that the client observed its acknowledgment.

Startup verifies retained local sequence/checksum against complete remote history.
New S3 namespaces require an explicit exclusive-bootstrap assertion. Missing
journal identity, missing history, chain gaps, conflicting records, or namespace
changes cannot silently reset a tenant. Explicit reconciliation reads and validates
history, applies records atomically, restores retry metadata, then enables READY.
It never issues a new WAL PUT or regenerates an unresolved batch identity.
An absent attempted record remains blocked even after a 404/empty scan.

`wal/identity.py` defines tenant-scoped request identity from tenant, operation
type, and ordered entity/delta/expected-version intent. Timing and scheduling
are excluded. WAL v2 carries that fingerprint; WAL v1 bytes/checksums remain
unchanged. Legacy records replay, but unverifiable client retries reject. Internal
request-identity and attempt-journal schema versions are 1. Replay restores both
identity and outcome metadata atomically with each record's application.

## Lifecycle And Resources

`wal/batching/batcher.py` tracks queued, sealed, uploading, and resolved handles.
Fatal construction, worker, or receipt failures wake waiters and stop admission.
Known persistence remains known after receipt-processing failure; optional metrics
failures do not alter transaction outcomes. Close and pending waits have bounded
deadlines; a timed-out upload may still finish. Repeated fatal close repeats its
error. Shutdown without flush rejects queued work, not a possibly committed upload.

Limits cover encoded record payloads, excluding envelope/index overhead. Oversized
records reject. The queue is bounded and one upload worker is retained. SQLite's
connection setting is an idle-cache eviction target, not a hard concurrent limit;
active connections are never evicted. Benchmark workers are separately bounded.

## Benchmark Contract

`benchmark/wal_batching.py` exports schema-version-2 artifacts with separate setup,
measurement, drain, cleanup, and first-export durations. Executor queue and tenant
waits are exposed; local apply includes COMMIT. Inclusive batch wait overlaps PUT
time and must not be summed with it. The submit-all thread pool is not open-loop
rate-controlled traffic, and no warmup is implemented.

Successful new-write goodput excludes duplicates. Attempted throughput and unique
confirmed durable writes are separate. Outcome latency groups distinguish success,
rejection/conflict, unknown, and other failure. Invalid savings/ratios are null.
Logical persist calls, creations, existing confirmations, failures, and phase-scoped
SDK requests are separate. SDK-internal wire retries and billable usage are unknown,
not zero. No prices or cloud performance claims were fabricated.

Artifacts preserve primary/teardown errors, raw results, run prefix, cleanup state,
seed, dataset/workload identity, dependency versions, and available placement/Git
metadata. This workspace has no Git repository, so revision is null. Failed or
ambiguous runs retain WAL evidence. Cleanup cannot overwrite the primary exception.
Setup failure before artifact context exists or filesystem export failure can still
prevent artifact creation. Dataset generation refuses destructive replacement.

## Verification

Final full suite: **141 passed, 7 skipped in 63.54 seconds**. Commands ran in the
workspace's existing Python environment, without installing dependencies:

```powershell
python -m pytest -q
ruff check .
mypy src
python -m pytest -m aws -q
python -m ephemeraldb.engine.runner --help
python -m ephemeraldb.engine.reconcile --help
python -m ephemeraldb.benchmark.wal_batching --help
```

Ruff passed; mypy passed across 62 source files. All three CLI help checks passed.
Explicit AWS selection skipped all five tests because RUN_AWS_TESTS was not enabled.
No AWS calls, infrastructure provisioning, or cloud performance runs were performed.
Two optional dataset/workload scale tests are also gated in the full suite.

Focused offline failure demonstrations:

```powershell
python -m pytest tests/unit/test_stabilization.py -q
python -m pytest tests/unit/test_benchmark_stabilization.py -q
python -m pytest tests/unit/test_s3_stabilization.py -q
```

These cover both persistence modes, durable/local failure, stored-then-timeout,
absent-timeout, restart, missing journal/history, ACK loss, conflicting delta and
ordered multi-entity retries, replay compatibility, worker lifecycle, bounded
shutdown/admission, connection eviction, request accounting, cleanup-independent
timing, and partial artifacts. Event-controlled concurrency and injected clocks
avoid depending on cloud timing.

For an existing local development attempt, the tested read/validate/replay CLI is:

```powershell
python -m ephemeraldb.engine.reconcile --dataset-dir ./generated --tenant <tenant-id> --wal-store local --wal-root ./generated-wal
```

Do not delete the journal or use bootstrap as an administrative bypass.

## Remaining Boundaries

- One exclusive runtime/namespace; old writers must stop before reconciliation.
  No distributed fencing, ownership transfer, or atomic remote listing snapshot.
- Complete cache-loss recovery, checkpoints, and WAL retention/garbage collection
  remain Phase 7 work. The local journal is not a replacement for remote authority.
- Some unknown outcomes intentionally remain blocked indefinitely without positive
  evidence. No automatic logical resubmission or operator force-clear exists.
- Startup scans full history, capped at 10,000 objects/100,000 tenant records.
  This is bounded correctness tooling, not a scalable production recovery index.
- S3 Express is single-AZ; durability does not imply multi-AZ disaster resilience.
- Real AWS compatibility/performance still requires an explicitly enabled run.
  Test doubles establish protocol behavior only.

The stabilized contracts provide a foundation for the separate Phase 7 prompt;
they are not evidence that checkpoint recovery is already implemented.
