# EphemeralDB Systems Lab

The Flutter Web console drives real experiments through a Python control API. Local
execution uses the existing tenant engine and external filesystem WAL. AWS execution
uses S3 Express payloads and S3 Standard fenced authority; PostgreSQL execution uses
real pooled database connections. Backend failures never fall back to local execution.

## Start locally

```powershell
python -m pip install -e '.[console,dev]'
./scripts/start-console.ps1
```

Open http://127.0.0.1:8765. The script builds Flutter and serves it from the API's
origin. For subsequent launches use `./scripts/start-console.ps1 -SkipBuild`.
Flutter 3.38.3 / Dart 3.10.1 were used during implementation.

No `.env` file is loaded automatically. `.env.example` documents environment variables;
set them in your shell or deployment Secret. No AWS credentials are needed locally.
For frontend development, rebuild with `flutter build web --release --no-wasm-dry-run`
and refresh the browser. The frontend makes relative same-origin API requests by default.

## Screens and execution contracts

* Overview: measured throughput, successful-operation percentiles, error/drop counts,
  protocol explanation, configuration readiness and experiment history.
* Database explorer: paginated allowlisted tables from the local serving cache. This
  inspection is not an authoritative application read. Remote-run seed files must
  never be presented as current remote state. Individual outcomes show records,
  payload locators, authority epochs/decisions and durability outcomes when available.
* Traffic lab: independent fresh datasets/namespaces, deterministic profiles/seeds,
  bounded concurrency, target arrival rate, warm-up, batch windows and AWS checkpoint
  intervals. Zero disables periodic checkpoints. Checkpoints are a fenced-S3 feature;
  Aurora manages its own durability. The mode field does not change PostgreSQL behavior.
* Failure lab: actual multiprocess cache-loss, ACK-loss and stale-owner proofs using
  the existing offline authority implementation. These are explicitly local evidence.
  Dedicated allowlisted AWS FIS templates can also be launched through the console.
* Comparisons: completed run selection, raw metrics and dataset/workload identity
  matching. A match does not prove equivalent durability or equal infrastructure.

The runner is open-loop in its scheduled arrivals, with at most twice the worker count
admitted at once. Saturated arrivals are recorded as `OVERLOAD_DROPPED`, not silently
queued without bound. Warm-up drains before measurement. Percentiles use successful
nonduplicate measurement operations and include scheduling/queue delay. Goodput uses
the greater of configured measurement duration and measured drain completion, excluding
setup/teardown. Arrival offsets are deterministic; concurrent completion order is not.
Hot contention deliberately generates conflicts; these remain visible rather than
being silently rewritten into successful operations.

## Persistent jobs and artifacts

One API process is supported. Each experiment runs in a separate Python subprocess;
closing the browser does not stop it. One experiment at a time avoids accidental
resource competition. Evidence is retained under `.artifacts/console/<run-id>/`:

* `run.json`: atomic status snapshots, configuration, identities, metrics and accounting.
* `workload.jsonl`: exact generated operation intents.
* `outcomes.jsonl`: every outcome, including warm-up, conflicts and admission drops.
* `dataset/`: tenant databases for local inspection.
* `worker.log`: local operator diagnostics, not available through public export routes.
* `proof/`: fault evidence and authority state for failure jobs.

Cancellation stops traffic admission and drains accepted work. Bounded multiprocess
proofs complete independently. Raw JSON/JSONL exports are available from the UI and
API. S3 runs also upload raw outcomes, workload and a final manifest to their isolated
authority `reports/` prefix. Failed/ambiguous uploads retain local evidence.

The subprocess runner is not a distributed durable queue. API restarts do not kill
running workers, but a host crash can leave a queued/running snapshot. Resolve the
underlying worker and retained evidence before archiving that run. Do not run multiple
API processes or enable a reloader against the same result directory.

## AWS validation and execution

First reauthenticate the intended AWS profile, then set existing bucket names:

```powershell
$env:AWS_PROFILE = 'ephemeraldb-dev'
$env:AWS_REGION = 'ap-south-1'
$env:S3_EXPRESS_BUCKET = '<directory-bucket>'
$env:S3_CHECKPOINT_BUCKET = '<general-purpose-bucket>'
./scripts/cloud-check.ps1 -Profile ephemeraldb-dev
$env:ENABLE_AWS_EXECUTION = '1'
```

Restart the console with these variables. Capability labels mean configured, not
verified. The first AWS session checked during implementation had expired, so no
live AWS durability, performance, FIS or deployment result is claimed.

Every S3 run gets a UUID namespace. Fenced writes include payload persistence,
descriptor persistence, authority reads and conditional control updates. Batching
amortizes payload PUTs only. SDK invocation counts are not billable-request counts;
internal retries are not independently instrumented. Cost and acknowledged-write-loss
metrics remain null when the experiment has no corresponding evidence.

S3 Express stores data in one Availability Zone. A compute/cache-loss proof is not a
permanent-AZ-loss guarantee. Aurora's managed durability and different failure domains
must be disclosed alongside performance and cost comparisons.

## PostgreSQL / Aurora baseline

Set `AURORA_DSN` through the shell or a Kubernetes Secret. Use `sslmode=verify-full`
and a trusted AWS CA bundle for Aurora. The adapter creates a fresh `run_<uuid>` schema,
imports the deterministic dataset before timing, creates equivalent indexes, runs
`ANALYZE`, and executes reads/writes through pooled PostgreSQL connections. It uses row
locks, request identity and atomic transactions; it does not impose SQLite's tenant lock.
The shared audit schema still needs an atomic per-tenant sequence allocation.

Schemas are retained for inspection. Drop only a known run's schema after exports and
after all clients stop. Pool shutdown releases connections; open pools would prevent
Aurora from auto-pausing. Choose a currently available engine version supporting 0 ACU.

```powershell
$env:RUN_POSTGRES_TESTS = '1'
$env:AURORA_DSN = '<test PostgreSQL DSN>'
python -m pytest -q tests/integration/test_console_postgres.py
```

The parity test compares operation records and every logical table against SQLite,
and verifies duplicate/conflicting retry identity. Its test-only UUID schema is removed
afterward. Local PostgreSQL validation is not an Aurora scaling/performance result.

## Infrastructure and runtime routing

`terraform/` defines a tagged VPC, public Spot NVMe workers without a NAT gateway,
private database subnets, EKS, IAM/IRSA, ECR, a directory bucket, a checkpoint bucket,
and optional Aurora Serverless v2. No infrastructure is created by tests or startup.
Inventory tooling was added before the infrastructure definitions.

Copy `terraform.tfvars.example` to `terraform.tfvars` and replace every placeholder.
Select supported EKS/Aurora versions and an S3 Express AZ in the target account.
Then use `cloud-lifecycle.ps1 -Action plan`, inspect the saved plan, and explicitly
invoke `-Action apply`. Never leave EKS running after a benchmark session.

Follow `kubernetes/README.md` for deployment, scoped runtime credentials and access.
The initial configuration uses one StatefulSet replica. For fixed multiple runtimes,
use distinct per-pod DNS endpoints in `RUNTIME_URLS`; tenant placement is SHA-256 modulo
that fixed endpoint count. Do not change placement during an experiment.

After a runtime process restarts, a request reaching its endpoint can trigger bounded
recovery: take a fresh ownership epoch, restore the S3 authority checkpoint, replay
accepted descriptors only, and retry the original operation. A live old owner is
fenced through the same authority. This path is implemented and transport-tested;
it has not been validated on a live EKS failure. Endpoint replacement across shards,
background failure detection, elastic rebalancing and autoscaling are still separate
work. Persistent network outages remain explicit failures/unknown outcomes.

## AWS FIS

Set `FIS_TEMPLATE_IDS` to a comma-separated allowlist of dedicated experiment templates.
The API checks for a CloudWatch alarm stop condition before launching a template.
IAM must scope start/get/stop permissions to the intended experiments and dedicated
targets; the application allowlist is not a substitute for IAM target restrictions.
Use EKS pod deletion/network latency or EC2 interruption actions as appropriate.
`POST /api/fis/<experiment-id>/stop` stops experiments launched by this console.
Stopping a fault does not undo an instance termination; validate replacement and recovery.

## Cost and teardown

Run `cloud-lifecycle.ps1 -Action inventory` before and after sessions. Its read-only
inventory is regional/tag-based, not a complete bill audit. Export evidence before
teardown. Preview isolated object cleanup with `python scripts/cleanup-run.py <run-id>`.
Deletion additionally requires `--execute --writers-stopped`; never remove authority
while delayed writers might still complete. No whole-bucket purge is provided.

Buckets refuse forced deletion, and Aurora preserves a final snapshot. Nonempty ECR
repositories, retained snapshots, S3 data, logs and public IPv4 can remain chargeable.
An existing final snapshot name can also block repeated teardown; use a distinct
reviewed snapshot identifier when repeating infrastructure cycles.
Run `cloud-lifecycle.ps1 -Action destroy-plan`, review it, then run `-Action destroy`. Check the
inventory/Billing afterward. Auto-pause does not eliminate EKS, storage or snapshot costs.

## References

* [Flutter web deployment](https://docs.flutter.dev/deployment/web)
* [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/)
* [Aurora auto-pause](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2-auto-pause.html)
* [EKS launch templates](https://docs.aws.amazon.com/eks/latest/userguide/launch-templates.html)
* [AWS FIS actions](https://docs.aws.amazon.com/fis/latest/userguide/fis-actions-reference.html)
