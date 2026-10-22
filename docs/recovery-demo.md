# Recovery Demo

## Unsealed Phase 8 Proof

`python -m ephemeraldb.ownership.demo cache-loss --root <new-dir> --mode single` terminates a
live writer without seal/release/close, deletes its cache, takes ownership, restores accepted
history, resolves the retry, and writes the next sequence. Use `stale-owner` for the delayed
old-CAS proof and `ack-loss` for accepted-before-ACK recovery. Both single and batch modes are
supported and produce retained `evidence.json`.

The default demo uses external filesystem authorities, not AWS. It starts a real
child process, publishes sequence-zero checkpoints, executes eight writes across
two tenants, checkpoints at sequence 2, and seals at sequence 4. The parent stores
an ACK/fingerprint ledger outside the disposable cache, kills the child, waits for
termination, and deletes only the validated run/cache path. Remote-equivalent test
stores, ledger, and verifier remain outside that directory.

Each tenant restores into a fresh directory. An independent reference execution
regenerates the same initial dataset and executes operation intent without WAL
replay. Full logical checksums cover values, versions, transactions/items, audit
history, metadata, and relationships, not just counts. Every transaction is checked,
every retry must be duplicate without extra WAL, and the first new write uses 5.

## Offline Commands

From the project root, with the existing package installed:

```powershell
python -m ephemeraldb.recovery.demo --mode batch
python -m ephemeraldb.recovery.demo --mode single --scenario durable-before-ack
python -m ephemeraldb.recovery.demo --mode batch --checkpoint-interval 1
python -m pytest tests/integration/test_recovery_demo.py -q
```

Omitting `--output-dir` creates a fresh UUID directory under
`benchmark/results/recovery/`. An explicit output directory must not exist.
`--checkpoint-interval` accepts 1 through 4, keeping the experiment capped at eight
writes, two tenants, and tiny snapshots. Run explicit separate intervals to compare
checkpoint overhead and replay cost; no large matrix runs automatically.

The durable-before-ACK scenario suppresses one acknowledgment with the BEFORE_ACK
fault hook, then seals the known applied state before the actual process kill.
Seven acknowledged plus one durable unacknowledged write must survive. It is not
a claim that an arbitrary timed-out in-flight request can be resolved. The report
separates the ACK ledger from durable unacknowledged outcomes.

`result.json` is a versioned artifact with configuration, identities, versions,
outcomes, checkpoint/recovery stages, scan costs, checksums, process exit code,
verification results, and cleanup status. `ack-ledger.json` is parent-owned.
Authoritative objects remain for inspection. No whole-bucket or arbitrary-prefix
deletion command is provided. Demo cleanup deletes only its disposable cache;
failed runs preserve evidence. Initial argument/setup failures can precede export.

## Explicit Operations

Use a single inactive/exclusive dataset; these commands do not coordinate with a
separate running engine. For offline commands supply both external authority roots.
The `seal` command intentionally stops the source tenant permanently.

```powershell
python -m ephemeraldb.checkpoint.cli create --store offline --namespace demo --tenant <tenant> --dataset-dir <dataset> --checkpoint-root <checkpoint-store> --wal-root <wal-store> --output create.json
python -m ephemeraldb.checkpoint.cli list --store offline --namespace demo --tenant <tenant> --checkpoint-root <checkpoint-store> --wal-root <wal-store> --output list.json
python -m ephemeraldb.checkpoint.cli validate --store offline --namespace demo --tenant <tenant> --manifest-key <manifest-key> --checkpoint-root <checkpoint-store> --wal-root <wal-store> --output validate.json
python -m ephemeraldb.checkpoint.cli seal --store offline --namespace demo --tenant <tenant> --dataset-dir <dataset> --checkpoint-root <checkpoint-store> --wal-root <wal-store> --output seal.json
python -m ephemeraldb.checkpoint.cli restore --store offline --namespace demo --tenant <tenant> --boundary-key <boundary-key> --checkpoint-root <checkpoint-store> --wal-root <wal-store> --destination <new-directory> --old-writers-stopped --output restore.json
```

These offline commands use the Phase 7 external-filesystem WAL adapter, not the
older local WAL adapter's namespace identity. Do not point a different adapter at
an existing dataset to bypass namespace checks. The demo initializes the correct
adapter and creates inspectable example artifacts automatically.

## Opt-In AWS

Create neither buckets nor credentials with this project. Configure existing,
separate S3 Standard and S3 Express buckets through `.env.example` variables and
the ordinary SDK credential chain. Boto3/botocore are declared project dependencies,
but imported lazily; offline proof does not require them at runtime. Install the
declared project dependencies before an AWS run. No SDK was installed for this task.

```powershell
$env:AWS_REGION = 'ap-south-1'
$env:S3_CHECKPOINT_BUCKET = '<existing-general-purpose-bucket>'
$env:S3_EXPRESS_BUCKET = '<existing-directory-bucket>'
$env:RUN_AWS_TESTS = '1'
python -m pytest tests/integration/test_checkpoint_aws.py -q -s
```

The two gated tests cover single/batch WAL, sequence-zero/nonzero checkpoint
publication/listing/download/validation, suffix recovery, retries, and next write.
They use unique prefixes, print a tiny run envelope, and clean only their own keys
on success. Failure retains its named run prefix. No infrastructure is provisioned.
Missing enablement, bucket configuration, SDK, or credentials skips explicitly.

For manual operations use `--store aws`, omit filesystem roots, and configure
`WAL_PERSISTENCE_MODE=single` or `batch`. Add `--initialize-durability` only for
first use of a NEW empty exclusive WAL namespace; it cannot repair lost history.
Compute/S3 Express same-AZ placement matters for representative performance.
Laptop cloud tests would be functional evidence only, not a latency benchmark.
