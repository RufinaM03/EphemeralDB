# Phase 8 Fenced Ownership And Recovery Report

## Result

Phase 8 introduces fresh-namespace fenced commits. A commit becomes authoritative only when
its immutable descriptor is installed as the tenant control head by conditional CAS. Explicit
takeover modifies that same object, preserving history and advancing epoch/revision. This is
not a preflight lease check: a paused old runtime's later CAS uses an obsolete validator and
is rejected even when its payload arrived after takeover.

The ordinary `TenantDatabaseEngine` accepts `FencedCommitter`. Writes prepare WAL with the
current epoch, persist collision-safe candidates, decide through authority, apply locally,
and expose decision metadata in `ExecutionResult`. Reads verify current token and local head.
Accepted-but-not-applied tenants block. Duplicate request identity survives recovery.

Unsealed recovery starts from the authority checkpoint, traverses only reachable descriptors,
validates exact single/batch locators and WAL chains, replays into unique staging, and rechecks
ownership before installation. It does not read the old cache, attempt journal, seal, or parent
ACK ledger. Offline authority is an external SQLite WAL database with transactional CAS; the
production adapter uses S3 Standard conditional PUT. The S3 Express fenced candidate adapter
uses checksum/epoch keys so stale and successor records at one logical sequence cannot collide.

## Executed Evidence

Fresh retained artifacts:

- `.artifacts/phase8-evidence-stale-single/evidence.json`
- `.artifacts/phase8-evidence-stale-batch/evidence.json`
- `.artifacts/phase8-evidence-cache-single/evidence.json`
- `.artifacts/phase8-evidence-cache-batch/evidence.json`
- `.artifacts/phase8-evidence-ack-single/evidence.json`
- `.artifacts/phase8-evidence-ack-batch/evidence.json`
- `.artifacts/phase8-evidence-accounting/accounting.json`
- `.artifacts/phase8-legacy-sealed-proof/result.json`

Observed stale-owner result in both modes: epochs `[1, 2]`, successor decision `ACCEPTED`,
accepted history contains only `successor-tx`, and resumed A reports `takeover won before
commit acceptance`. The late A candidate remains retained but unreachable.

Observed unsealed process/cache-loss result in both modes: parent received one real ACK,
terminated the live child without seal/release/close, deleted `cache-a`, took epoch 2,
recovered committed sequence 1, resolved the retry as duplicate, accepted the next operation
at sequence 2, and observed zero acknowledged loss. The ACK-withheld run recovered one
accepted-unacknowledged operation and resolved its retry without a second commit.

Final settled-tree quality gates: `199 passed, 10 skipped` in 90.32 seconds; Ruff passed; strict
mypy passed across 82 source files. The ten skips are explicitly gated AWS/scale tests. No live
AWS run occurred, so remote AWS fencing is not claimed.

## Commands

```powershell
python -m ephemeraldb.ownership.cli --help
python -m ephemeraldb.ownership.demo --help
python -m ephemeraldb.ownership.demo stale-owner --root .artifacts/stale --mode single
python -m ephemeraldb.ownership.demo cache-loss --root .artifacts/cache --mode batch
python -m ephemeraldb.ownership.demo ack-loss --root .artifacts/ack-loss --mode single
python -m pytest -q tests/unit/test_phase8_ownership.py tests/unit/test_phase8_protocol_model.py
$env:RUN_AWS_TESTS='1'; python -m pytest -q tests/integration/test_phase8_aws.py
```

Bootstrap/status/takeover use `python -m ephemeraldb.ownership.cli --authority-db <path>
--namespace <name> <command> ...`; each subcommand has exact argument help.

## Cost And Limitations

Fenced single writes require one payload PUT, one descriptor PUT, control reads, and one
conditional control PUT. Shared batching amortizes payload PUTs only; descriptor/control work
remains per tenant. The artifacts report logical adapter requests, not unknown SDK wire retries
or fabricated prices.

The tiny same-workload accounting run submitted two tenant records in one shared batch. Both
paths wrote 1,987 payload bytes. Legacy payload-only mode made one immutable payload call.
Fenced mode made two conditional control creates, one batch PUT plus two descriptor PUTs, two
control GETs, and two control CAS calls. One observed offline run took 27.5 ms versus 37.6 ms;
that single sample is request-accounting evidence, not a latency distribution or p99 claim.

This is a bounded two-runtime crash/stale-process proof, not production leader election.
Remaining work includes automatic failure detection, distributed operator authorization,
IAM policy hardening, large descriptor-chain indexing/compaction, garbage collection with
shared-batch retention, broader checkpoint migration, and multi-region/AZ payload durability.
The real S3 adapter is implemented and gated, but no live AWS evidence is claimed without an
explicit `RUN_AWS_TESTS=1` run and configured existing buckets.
