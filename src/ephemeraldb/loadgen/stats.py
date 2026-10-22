"""Workload-definition statistics."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

from ephemeraldb.loadgen.models import (
    EntityDeltaPayload,
    EntityHistoryPayload,
    EntityLookupPayload,
    MetadataLookupPayload,
    MultiEntityTransactionPayload,
    OperationType,
    RelationshipExpansionPayload,
    WorkloadOperation,
    is_read_operation,
    is_write_operation,
)
from ephemeraldb.loadgen.serialization import iter_workload


@dataclass(frozen=True, slots=True)
class WorkloadStats:
    total_operations: int
    reads: int
    writes: int
    operation_counts: dict[str, int]
    operations_by_tenant: dict[str, int]
    top_10_busiest_tenants: tuple[tuple[str, int], ...]
    unique_tenants_touched: int
    unique_entities_touched: int
    multi_entity_transaction_count: int
    deliberate_conflict_count: int
    scheduled_peak_ops_per_sec: int


def calculate_workload_stats(operations: Iterable[WorkloadOperation]) -> WorkloadStats:
    operation_counts: Counter[str] = Counter()
    operations_by_tenant: Counter[str] = Counter()
    operations_by_second: Counter[int] = Counter()
    unique_entities: set[str] = set()
    reads = 0
    writes = 0
    multi_count = 0
    deliberate_conflict_count = 0
    total_operations = 0

    for operation in operations:
        total_operations += 1
        operation_counts[operation.operation_type.value] += 1
        operations_by_tenant[operation.tenant_id] += 1
        operations_by_second[operation.scheduled_offset_ms // 1000] += 1
        reads += int(is_read_operation(operation.operation_type))
        writes += int(is_write_operation(operation.operation_type))
        unique_entities.update(_entities_touched(operation))
        if operation.operation_type == OperationType.MULTI_ENTITY_TRANSACTION:
            multi_count += 1
        deliberate_conflict_count += _deliberate_conflict_count(operation)

    peak = max(operations_by_second.values(), default=0)
    return WorkloadStats(
        total_operations=total_operations,
        reads=reads,
        writes=writes,
        operation_counts=dict(sorted(operation_counts.items())),
        operations_by_tenant=dict(sorted(operations_by_tenant.items())),
        top_10_busiest_tenants=tuple(operations_by_tenant.most_common(10)),
        unique_tenants_touched=len(operations_by_tenant),
        unique_entities_touched=len(unique_entities),
        multi_entity_transaction_count=multi_count,
        deliberate_conflict_count=deliberate_conflict_count,
        scheduled_peak_ops_per_sec=peak,
    )


def calculate_workload_file_stats(path: Path) -> WorkloadStats:
    return calculate_workload_stats(iter_workload(path))


def _entities_touched(operation: WorkloadOperation) -> tuple[str, ...]:
    payload = operation.payload
    if isinstance(payload, EntityLookupPayload | MetadataLookupPayload | EntityHistoryPayload):
        return (payload.entity_id,)
    if isinstance(payload, RelationshipExpansionPayload):
        return (payload.source_entity_id,)
    if isinstance(payload, EntityDeltaPayload):
        return (payload.entity_id,)
    if isinstance(payload, MultiEntityTransactionPayload):
        return tuple(item.entity_id for item in payload.items)
    return ()


def _deliberate_conflict_count(operation: WorkloadOperation) -> int:
    payload = operation.payload
    if isinstance(payload, EntityDeltaPayload):
        return int(payload.expected_version_is_deliberately_stale)
    if isinstance(payload, MultiEntityTransactionPayload):
        return int(any(item.expected_version_is_deliberately_stale for item in payload.items))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect workload-definition statistics.")
    parser.add_argument("workload", type=Path)
    args = parser.parse_args(argv)
    print(
        json.dumps(
            asdict(calculate_workload_file_stats(args.workload)),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
