# Experiment console implementation

Implemented on 2026-10-22. This tracks code and validation, not deployed AWS capabilities.

| Phase | Implementation | Validation |
|---|---|---|
| Control API | Authenticated local/remote access, isolated subprocess jobs, atomic snapshots, cancellation/drain, worker-exit monitoring | API and subprocess tests; local runs retained |
| Flutter UI | Overview, database explorer, transaction inspector, traffic lab, failure lab, comparisons and exports | Flutter analysis and release build; browser inspection |
| Traffic | Deterministic profiles, rate-controlled bounded admission, warm-up separation, overload records and latency groups | Real local single/batch runs and accounting tests |
| Cloud adapters | Fenced S3 commits/checkpoints/restart recovery; pooled PostgreSQL adapter | Both WAL modes tested over fake S3 transports; real PostgreSQL parity |
| Deployment | Dockerfile, EKS StatefulSet, NVMe initialization, scoped IRSA, Terraform network/storage/EKS/optional Aurora | Terraform formatting passes; provider download blocked full validation; Docker daemon unavailable |
| Failures | Isolated crash/cache-loss/ACK-loss/stale-owner jobs; allowlisted FIS start/status/stop | Local cache-loss job completed with zero observed acknowledged loss; no live AWS FIS run |

Settled local quality checks: **209 tests passed, 10 skipped**, Ruff passed, strict mypy
passed across 88 source files, Flutter analysis passed and release web build succeeded.
The full run explicitly enabled PostgreSQL integration against a separate temporary
PostgreSQL 17 instance. Eight AWS tests and two scale tests remained gated.

No EKS/Aurora infrastructure was provisioned. The intended `ephemeraldb-dev` AWS
session was expired, and cloud bucket names were not configured. Terraform AWS
provider installation was interrupted by the host network; full provider validation
is included in GitHub Actions. Docker is installed but its daemon was not running.

Not yet established: live S3 recovery evidence, EKS interruption/recovery behavior,
Aurora scaling/performance, background distributed failure detection, elastic tenant
rebalancing, automated repeated-run statistical analysis and measured AWS billing.
These are not represented as completed experiments in the UI.

Cloud infrastructure must be explicitly provisioned with a reviewed Terraform plan.
Missing credentials, bucket configuration or endpoints are reported as unavailable, never
replaced with synthetic cloud results. Local execution remains available without AWS.

See [the console runbook](console.md) for startup, exact contracts, deployment and teardown.
