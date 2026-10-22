# EKS benchmark runtime

Build Flutter first, then build and push the Docker image with an immutable tag.
Terraform outputs the ECR URL, runtime role ARN and bucket names. Replace all
`REPLACE_` values in runtime.yaml. Never commit secrets.

Create `runtime-secrets` in namespace `ephemeraldb` containing independently generated
`CONSOLE_TOKEN` and `RUNTIME_TOKEN`. Optional `AURORA_DSN` belongs here too. Use TLS
certificate verification for PostgreSQL and retrieve the password from Secrets Manager
through a secure operator workflow; Terraform never exports the password.

Apply the namespace before creating its Secret, then apply runtime.yaml. Access the
console with `kubectl -n ephemeraldb port-forward pod/runtime-0 8765:8765`.
Do not create a public LoadBalancer just for the demonstration.

HTTP traffic uses `RUNTIME_URLS` with explicit per-pod DNS names, for example
`http://runtime-0.runtime.ephemeraldb.svc.cluster.local:8765`. The traffic runner
must execute inside the cluster (or use distinct local port forwards), and use the
same `RUNTIME_TOKEN`. Do not route sharded requests through a load-balanced service.
An HTTP run prepares fresh S3 namespaces on each runtime, then routes tenants by
SHA-256 modulo the fixed number of endpoints. Do not change that list during a run.

The current router has fixed placement and fails closed after a runtime restart.
Automatic failure detection, takeover orchestration and rebalancing are not enabled.
Use the isolated failure proof screen for validated process recovery; do not claim
an EKS failover result from that proof. Cloud FIS experiments and their metrics must
be validated separately on the deployed stack.

NVMe setup formats only an EC2 instance-store disk whose model matches exactly.
The runtime uses a Directory hostPath, never a root-disk DirectoryOrCreate fallback.
Data is a disposable cache; S3 authority and WAL evidence are retained.

NetworkPolicy enforcement requires compatible VPC CNI network policy configuration.
The provided policy allows runtime ingress from this namespace only. Verify its
interaction with your port-forward/observability setup before enabling it.
