# Workload Generation

Phase 2 defines deterministic workload intent. It does not execute traffic, create concurrency, call AWS, connect to Aurora, write WAL, checkpoint, recover, or benchmark performance.

## Operation Model

Each workload line is one JSON object with:

- `schema_version`
- `operation_id`
- `sequence_no`
- `tenant_id`
- `operation_type`
- `scheduled_offset_ms`
- `benchmark_phase`
- `virtual_client_id`
- typed `payload`

Operation IDs are deterministic UUIDv5 values derived from workload seed, sequence number, tenant ID, and operation type. Write transaction IDs are separate deterministic UUIDv5 values so future idempotency experiments can distinguish request identity from logical transaction identity.

## Operation Types

Read operations:

- `ENTITY_LOOKUP`
- `STATE_SCAN`
- `RELATIONSHIP_EXPANSION`
- `METADATA_LOOKUP`
- `ENTITY_HISTORY`
- `COMPLEX_JOIN`
- `AGGREGATE_BY_STATE`

Write-intent operations:

- `ENTITY_DELTA`
- `MULTI_ENTITY_TRANSACTION`

Payloads are typed dataclasses in code and deterministic JSON objects on disk.

## Built-In Profiles

`READ_HEAVY`: 90% reads and 10% writes.

Read composition:

```text
ENTITY_LOOKUP                35%
STATE_SCAN                   15%
RELATIONSHIP_EXPANSION       15%
METADATA_LOOKUP              10%
ENTITY_HISTORY               10%
COMPLEX_JOIN                 10%
AGGREGATE_BY_STATE            5%
```

`BALANCED`: 60% reads and 40% writes.

`WRITE_HEAVY`: 30% reads and 70% writes.

`HOT_CONTENTION`: 30% reads and 70% writes, with hot tenants, hot entities, frequent expected-version usage, and deliberate stale-version conflicts.

`MULTI_TENANT_SKEW`: balanced read/write mix with weighted tenant buckets.

## Tenant Selection

Uniform selection gives each tenant equal probability.

Skewed selection uses deterministic weighted buckets:

```text
1% tenants: weight 100
9% tenants: weight 10
remaining tenants: weight 1
```

The buckets are selected using a dedicated seeded `random.Random` instance. The global random module is never used.

## Hot Sets

Hot-contention profiles precompute deterministic hot tenants and hot entities:

```text
hot tenant count = max(1, round(tenant_count * hot_tenant_fraction))
hot entity count = max(1, round(entities_per_tenant * hot_entity_fraction))
```

For the built-in hot-contention profile, hot tenant fraction is 10% and hot entity fraction is 1%. Write operations preferentially target those hot entities while remaining valid against the Phase 1 dataset identities.

## Shadow Versions

The workload generator tracks projected versions for entities touched by write-intent operations. This is not database execution. It is a deterministic shadow model used to make expected-version payloads coherent.

Coherent expected version:

```text
expected_version = projected current version
projected version increments after generated successful intent
```

Deliberate conflict:

```text
expected_version = stale projected version
projected version does not advance
```

Payloads mark deliberate stale expectations explicitly.

## Schedules

Schedules are virtual time, not sleeping or real traffic execution. Every operation receives `scheduled_offset_ms` and a benchmark phase:

- `WARMUP`
- `MEASUREMENT`
- `COOLDOWN`

Supported segment shapes:

- `CONSTANT`
- `RAMP`
- `SPIKE`
- `STEP`
- `COOLDOWN`

Built-in presets:

`SMALL_DEV`

```text
10 sec warmup at 10 ops/sec
30 sec measurement at 50 ops/sec
10 sec cooldown at 10 ops/sec
```

`RAMP_1000`

```text
30 sec at 50 ops/sec
60 sec ramp 50 -> 250
60 sec ramp 250 -> 500
60 sec ramp 500 -> 1000
30 sec cooldown at 50 ops/sec
```

## Virtual Clients

Each operation gets a deterministic `virtual_client_id` such as `client-0001`. Operations are distributed round-robin across configured virtual clients. Per-client scheduled offsets never move backward.

## Serialization

Workloads are JSON Lines:

```text
benchmark/workloads/hot-contention.jsonl
```

The adjacent manifest is:

```text
benchmark/workloads/hot-contention.manifest.json
```

The manifest records profile, schedule, dataset summary, dataset manifest checksum, operation counts, read/write counts, tenant distribution, hot set, and workload checksum.

## Workload Checksum

The workload checksum is SHA-256 over:

1. canonical manifest basis
2. canonical JSON representation of each operation in sequence

Changing dataset manifest checksum, workload seed, profile mix, tenant distribution, hot set, schedule, or operation content changes the workload checksum.

## Validation And Statistics

Validate:

```bash
python -m ephemeraldb.loadgen.validation ./benchmark/workloads/hot-contention.jsonl
```

Inspect:

```bash
python -m ephemeraldb.loadgen.stats ./benchmark/workloads/hot-contention.jsonl
```

Statistics describe the workload definition only: operation counts, read/write counts, tenant activity, top tenants, unique entities, multi-entity transactions, deliberate conflicts, and scheduled peak operations per second.
