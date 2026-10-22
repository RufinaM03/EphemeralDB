# Cost Safety

## Phase 8 Cloud Gate

Real authority tests require `RUN_AWS_TESTS=1` and an existing general-purpose
`S3_CHECKPOINT_BUCKET`. They use a unique prefix, print a bounded request envelope, create no
infrastructure, and retain ambiguous authority objects rather than deleting while delayed
requests may complete. Conditional failures are still chargeable S3 requests.

This project has a strict AWS cost target. The benchmark architecture must be designed around explicit, temporary infrastructure.

> Do not leave EKS running after benchmark sessions.

## Rules

1. EKS must never be considered permanent infrastructure.
2. Expensive infrastructure lifecycle must eventually be:

```text
terraform apply
benchmark
export results
terraform destroy
```

3. No NAT Gateway unless explicitly justified later.
4. No Amazon Managed Grafana.
5. No Amazon Managed Service for Prometheus.
6. No always-running Aurora compute.
7. Aurora Serverless v2 should eventually use supported auto-pause and 0 ACU configuration where possible.
8. EC2 workers will eventually use Spot where appropriate.
9. S3 benchmark datasets should remain deliberately small.
10. ECR lifecycle policies must eventually remove old images.
11. Before future Terraform work, create scripts capable of checking whether chargeable resources remain deployed.
12. Never make `terraform apply` run automatically from tests.
13. Never provision AWS resources during unit tests.
14. Future benchmark commands must make potentially chargeable operations explicit.
15. Normal `pytest` must not make AWS calls.
16. Real AWS tests must be marked `aws` and gated by `RUN_AWS_TESTS=1`.
17. S3 Express probes must print bucket, region, expected object count, and expected request counts before running.
18. Phase 5 S3 Express probes and integration tests must use conservative request counts and dedicated test prefixes.
19. Phase 6 WAL batching benchmarks default to 100 logical transactions.
20. WAL batching benchmarks must print mode, transaction count, tenant count, batch window, planned logical PUT-call bound, bucket, and region before AWS calls. This bound excludes SDK retries, verification, and startup inspection; it is not a billable-request bound.
21. WAL batching benchmark runs above 1000 logical transactions require `--yes`.

## Phase 5 Boundary

Phase 5 is allowed to call an existing S3 Express One Zone directory bucket only when the user explicitly runs S3 commands or `RUN_AWS_TESTS=1 pytest -m aws`. It must not create buckets, Terraform stacks, EKS clusters, Aurora clusters, or other infrastructure.

The default S3 PUT latency probe uses 10 requests. It is a storage/network probe,
not a transaction benchmark.

## Phase 6 Boundary

The WAL batching benchmark talks only to an existing S3 Express One Zone
directory bucket. It does not create buckets or infrastructure. Successful runs
clean up their S3 WAL objects unless `--keep-objects` is set. Failed/ambiguous
runs retain evidence, including when an upload might finish after a timeout.
Artifacts identify the retained prefix and cleanup errors; operators must account
for retained storage and only remove it after resolving uncertainty and stopping
writers. The benchmark writes a machine-readable result
artifact under:

```text
benchmark/results/wal-batching/
```

## Phase 7 Boundary

Checkpoint commands use a separate existing general-purpose S3 bucket and STANDARD
storage. They never provision a bucket. Conditional uploads/read-back verification
incur PUT/GET requests; manifest discovery incurs paginated LIST/GET traffic.
Checkpoints and all WAL are retained by default, including orphan snapshots.
Shared-batch deletion requires a future all-tenant retention protocol.

The offline demo is capped at eight writes/two tenants and intervals 1..4. Remote
adapters cap objects at 64 MiB and WAL discovery at 128 MiB/1,000 objects/10,000
records. These are workload/memory safeguards, not exact billable-request bounds:
SDK retries may add traffic. Large-run overrides are intentionally not provided.
Gated AWS tests use isolated unique prefixes and clean only their own objects on
success; failed runs print retained prefixes. Never delete a whole bucket to clean
one demonstration. Observed process-recovery time is not a production RTO claim.
