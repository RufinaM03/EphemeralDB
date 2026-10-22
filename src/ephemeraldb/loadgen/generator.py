"""Deterministic backend-independent workload generator."""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from ephemeraldb.loadgen.catalog import DatasetIdentityCatalog, HotSetCatalog
from ephemeraldb.loadgen.models import (
    WORKLOAD_SCHEMA_VERSION,
    AggregatePayload,
    ComplexJoinPayload,
    EntityDeltaPayload,
    EntityHistoryPayload,
    EntityLookupPayload,
    MetadataLookupPayload,
    MultiEntityTransactionItem,
    MultiEntityTransactionPayload,
    OperationType,
    RelationshipExpansionPayload,
    StateScanPayload,
    WorkloadOperation,
    is_read_operation,
    is_write_operation,
)
from ephemeraldb.loadgen.profiles import (
    OperationWeight,
    TenantDistribution,
    WorkloadProfile,
    builtin_profile_names,
    get_builtin_profile,
)
from ephemeraldb.loadgen.schedule import (
    WorkloadSchedule,
    get_schedule_preset,
    iter_schedule_slots,
    schedule_preset_names,
)
from ephemeraldb.loadgen.serialization import (
    WorkloadChecksum,
    canonical_json,
    manifest_path_for_workload,
    operation_to_dict,
    write_workload_manifest,
)
from ephemeraldb.schema.generator import deterministic_uuid
from ephemeraldb.schema.models import ENTITY_STATES, RELATIONSHIP_TYPES


@dataclass(frozen=True, slots=True)
class GeneratedWorkload:
    workload_path: Path
    manifest_path: Path
    manifest: dict[str, object]


class ShadowVersionTracker:
    """Projected per-entity version state for generated write intent."""

    def __init__(self, initial_version: int) -> None:
        self._initial_version = initial_version
        self._versions: dict[tuple[str, str], int] = {}

    def current_version(self, tenant_id: str, entity_id: str) -> int:
        return self._versions.get((tenant_id, entity_id), self._initial_version)

    def advance(self, tenant_id: str, entity_id: str) -> None:
        key = (tenant_id, entity_id)
        self._versions[key] = self.current_version(tenant_id, entity_id) + 1


class WorkloadGenerator:
    """Generates deterministic workload operations from a dataset manifest."""

    def __init__(
        self,
        catalog: DatasetIdentityCatalog,
        profile: WorkloadProfile,
        schedule: WorkloadSchedule,
    ) -> None:
        self.catalog = catalog
        self.profile = profile
        self.schedule = schedule
        self.rng = random.Random(f"{profile.seed}:workload")
        self.hot_set = HotSetCatalog(
            catalog,
            seed=profile.seed,
            hot_tenant_fraction=profile.hot_tenant_fraction,
            hot_entity_fraction=profile.hot_entity_fraction,
        )
        self.tenant_selector = TenantSelector(catalog, profile, self.hot_set)
        self.version_tracker = ShadowVersionTracker(catalog.initial_versions_per_entity)

    def iter_operations(self) -> Iterable[WorkloadOperation]:
        for sequence_no, slot in enumerate(iter_schedule_slots(self.schedule)):
            operation_type = _weighted_choice(self.profile.operation_weights, self.rng)
            tenant_id = self.tenant_selector.choose(self.rng)
            virtual_client_id = _virtual_client_id(
                sequence_no,
                self.profile.virtual_client_count,
            )
            yield WorkloadOperation(
                operation_id=_operation_id(
                    self.profile.seed,
                    sequence_no,
                    tenant_id,
                    operation_type,
                ),
                sequence_no=sequence_no,
                tenant_id=tenant_id,
                operation_type=operation_type,
                scheduled_offset_ms=slot.scheduled_offset_ms,
                benchmark_phase=slot.benchmark_phase,
                virtual_client_id=virtual_client_id,
                payload=self._payload_for(sequence_no, tenant_id, operation_type),
            )

    def _payload_for(
        self,
        sequence_no: int,
        tenant_id: str,
        operation_type: OperationType,
    ) -> (
        EntityLookupPayload
        | StateScanPayload
        | RelationshipExpansionPayload
        | MetadataLookupPayload
        | EntityHistoryPayload
        | ComplexJoinPayload
        | AggregatePayload
        | EntityDeltaPayload
        | MultiEntityTransactionPayload
    ):
        if operation_type == OperationType.ENTITY_LOOKUP:
            return EntityLookupPayload(
                entity_id=self._choose_entity_id(tenant_id, for_write=False)
            )
        if operation_type == OperationType.STATE_SCAN:
            return StateScanPayload(
                state=self.rng.choice(ENTITY_STATES),
                limit=self.rng.randint(10, 100),
            )
        if operation_type == OperationType.RELATIONSHIP_EXPANSION:
            return RelationshipExpansionPayload(
                source_entity_id=self._choose_entity_id(tenant_id, for_write=False),
                limit=self.rng.randint(5, 50),
            )
        if operation_type == OperationType.METADATA_LOOKUP:
            metadata_key = None
            if self.catalog.metadata_entries_per_entity > 0 and self.rng.random() < 0.50:
                metadata_key = self.catalog.metadata_key(
                    self.rng.randrange(self.catalog.metadata_entries_per_entity)
                )
            return MetadataLookupPayload(
                entity_id=self._choose_entity_id(tenant_id, for_write=False),
                metadata_key=metadata_key,
            )
        if operation_type == OperationType.ENTITY_HISTORY:
            return EntityHistoryPayload(
                entity_id=self._choose_entity_id(tenant_id, for_write=False),
                limit=self.rng.randint(5, 50),
            )
        if operation_type == OperationType.COMPLEX_JOIN:
            metadata_index = 0
            if self.catalog.metadata_entries_per_entity > 0:
                metadata_index = self.rng.randrange(self.catalog.metadata_entries_per_entity)
            return ComplexJoinPayload(
                source_state=self.rng.choice(ENTITY_STATES),
                relationship_type=self.rng.choice(RELATIONSHIP_TYPES),
                metadata_key=self.catalog.metadata_key(metadata_index),
                limit=self.rng.randint(10, 100),
            )
        if operation_type == OperationType.AGGREGATE_BY_STATE:
            state_filter = self.rng.choice(ENTITY_STATES) if self.rng.random() < 0.35 else None
            return AggregatePayload(state_filter=state_filter)
        if operation_type == OperationType.ENTITY_DELTA:
            return self._entity_delta_payload(sequence_no, tenant_id, operation_type)
        if operation_type == OperationType.MULTI_ENTITY_TRANSACTION:
            return self._multi_entity_transaction_payload(sequence_no, tenant_id, operation_type)

    def _entity_delta_payload(
        self,
        sequence_no: int,
        tenant_id: str,
        operation_type: OperationType,
    ) -> EntityDeltaPayload:
        entity_id = self._choose_entity_id(tenant_id, for_write=True)
        expected_version, deliberate_conflict = self._expected_version(tenant_id, entity_id)
        if not deliberate_conflict:
            self.version_tracker.advance(tenant_id, entity_id)
        return EntityDeltaPayload(
            transaction_id=_transaction_id(
                self.profile.seed,
                sequence_no,
                tenant_id,
                operation_type,
            ),
            entity_id=entity_id,
            delta=_nonzero_delta(self.rng),
            expected_version=expected_version,
            expected_version_is_deliberately_stale=deliberate_conflict,
        )

    def _multi_entity_transaction_payload(
        self,
        sequence_no: int,
        tenant_id: str,
        operation_type: OperationType,
    ) -> MultiEntityTransactionPayload:
        max_items = min(self.profile.multi_entity_max_items, self.catalog.entities_per_tenant)
        min_items = min(self.profile.multi_entity_min_items, max_items)
        item_count = self.rng.randint(min_items, max_items)
        entity_ids = self._choose_unique_entity_ids(tenant_id, item_count, for_write=True)
        force_conflict = self.rng.random() < self.profile.conflict_probability
        items: list[MultiEntityTransactionItem] = []
        has_conflict = False
        for item_index, entity_id in enumerate(entity_ids):
            expected_version, deliberate_conflict = self._expected_version(
                tenant_id,
                entity_id,
                force_conflict=force_conflict and item_index == 0,
            )
            has_conflict = has_conflict or deliberate_conflict
            items.append(
                MultiEntityTransactionItem(
                    entity_id=entity_id,
                    delta=_nonzero_delta(self.rng),
                    expected_version=expected_version,
                    expected_version_is_deliberately_stale=deliberate_conflict,
                )
            )
        if not has_conflict:
            for entity_id in entity_ids:
                self.version_tracker.advance(tenant_id, entity_id)
        return MultiEntityTransactionPayload(
            transaction_id=_transaction_id(
                self.profile.seed,
                sequence_no,
                tenant_id,
                operation_type,
            ),
            items=tuple(items),
        )

    def _expected_version(
        self,
        tenant_id: str,
        entity_id: str,
        *,
        force_conflict: bool = False,
    ) -> tuple[int | None, bool]:
        current_version = self.version_tracker.current_version(tenant_id, entity_id)
        deliberate_conflict = (
            force_conflict or self.rng.random() < self.profile.conflict_probability
        )
        if deliberate_conflict:
            return max(0, current_version - 1), True
        if self.rng.random() < self.profile.expected_version_probability:
            return current_version, False
        return None, False

    def _choose_entity_id(self, tenant_id: str, *, for_write: bool) -> str:
        if for_write and self.rng.random() < self.profile.hot_entity_write_probability:
            hot_entities = self.hot_set.hot_entities_for_tenant(tenant_id)
            if hot_entities:
                return self.rng.choice(hot_entities)
        entity_index = self.rng.randrange(self.catalog.entities_per_tenant)
        return self.catalog.entity_id_for_tenant(tenant_id, entity_index)

    def _choose_unique_entity_ids(
        self,
        tenant_id: str,
        count: int,
        *,
        for_write: bool,
    ) -> tuple[str, ...]:
        selected: list[str] = []
        seen: set[str] = set()
        while len(selected) < count:
            entity_id = self._choose_entity_id(tenant_id, for_write=for_write)
            if entity_id not in seen:
                selected.append(entity_id)
                seen.add(entity_id)
        return tuple(selected)


class TenantSelector:
    def __init__(
        self,
        catalog: DatasetIdentityCatalog,
        profile: WorkloadProfile,
        hot_set: HotSetCatalog,
    ) -> None:
        self.catalog = catalog
        self.profile = profile
        self.hot_set = hot_set
        self.weighted_tenants = self._build_weighted_tenants()

    def choose(self, rng: random.Random) -> str:
        if self.profile.tenant_distribution == TenantDistribution.HOT_SET:
            if self.hot_set.hot_tenant_ids and rng.random() < 0.90:
                return rng.choice(self.hot_set.hot_tenant_ids)
            return rng.choice(self.catalog.tenant_ids)
        if self.profile.tenant_distribution == TenantDistribution.UNIFORM:
            return rng.choice(self.catalog.tenant_ids)
        return _weighted_choice(self.weighted_tenants, rng)

    def _build_weighted_tenants(self) -> tuple[tuple[str, float], ...]:
        if self.profile.tenant_distribution != TenantDistribution.SKEWED:
            return tuple((tenant_id, 1.0) for tenant_id in self.catalog.tenant_ids)
        tenant_indexes = list(range(self.catalog.tenant_count))
        bucket_rng = random.Random(f"{self.profile.seed}:tenant-skew")
        bucket_rng.shuffle(tenant_indexes)
        very_active_count = max(1, round(self.catalog.tenant_count * 0.01))
        moderate_count = max(1, round(self.catalog.tenant_count * 0.09))
        moderate_count = min(moderate_count, self.catalog.tenant_count - very_active_count)
        very_active = set(tenant_indexes[:very_active_count])
        moderate = set(tenant_indexes[very_active_count : very_active_count + moderate_count])
        weighted: list[tuple[str, float]] = []
        for index, tenant_id in enumerate(self.catalog.tenant_ids):
            if index in very_active:
                weight = 100.0
            elif index in moderate:
                weight = 10.0
            else:
                weight = 1.0
            weighted.append((tenant_id, weight))
        return tuple(weighted)


def generate_workload(
    *,
    dataset_manifest_path: Path,
    profile: WorkloadProfile,
    schedule: WorkloadSchedule,
    output_path: Path,
) -> GeneratedWorkload:
    catalog = DatasetIdentityCatalog.from_path(dataset_manifest_path)
    generator = WorkloadGenerator(catalog, profile, schedule)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_path_for_workload(output_path)

    manifest_basis = _manifest_basis(catalog, profile, schedule, generator.hot_set)
    checksum = WorkloadChecksum(manifest_basis)
    operation_type_counts: Counter[str] = Counter()
    read_count = 0
    write_count = 0
    operation_count = 0

    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for operation in generator.iter_operations():
            handle.write(canonical_json(operation_to_dict(operation)))
            handle.write("\n")
            checksum.update(operation)
            operation_type_counts[operation.operation_type.value] += 1
            read_count += int(is_read_operation(operation.operation_type))
            write_count += int(is_write_operation(operation.operation_type))
            operation_count += 1

    manifest: dict[str, object] = {
        **manifest_basis,
        "operation_type_counts": dict(sorted(operation_type_counts.items())),
        "read_count": read_count,
        "write_count": write_count,
        "workload_checksum": checksum.hexdigest(),
    }
    if operation_count != schedule.operation_count:
        msg = f"generated {operation_count} operations, expected {schedule.operation_count}"
        raise RuntimeError(msg)
    write_workload_manifest(manifest_path, manifest)
    return GeneratedWorkload(
        workload_path=output_path,
        manifest_path=manifest_path,
        manifest=manifest,
    )


def _manifest_basis(
    catalog: DatasetIdentityCatalog,
    profile: WorkloadProfile,
    schedule: WorkloadSchedule,
    hot_set: HotSetCatalog,
) -> dict[str, object]:
    return {
        "workload_schema_version": WORKLOAD_SCHEMA_VERSION,
        "profile_name": profile.name,
        "seed": profile.seed,
        "dataset_seed": catalog.dataset_seed,
        "dataset_manifest_checksum": catalog.dataset_manifest_checksum,
        "operation_count": schedule.operation_count,
        "duration_seconds": schedule.duration_seconds,
        "virtual_client_count": profile.virtual_client_count,
        "tenant_distribution": profile.tenant_distribution.value,
        "schedule": _schedule_to_dict(schedule),
        "profile": _profile_to_dict(profile),
        "dataset": catalog.identity_summary(),
        "hot_set": hot_set.to_dict(),
    }


def _schedule_to_dict(schedule: WorkloadSchedule) -> dict[str, object]:
    return {
        "name": schedule.name,
        "duration_seconds": schedule.duration_seconds,
        "peak_rate": schedule.peak_rate,
        "segments": [
            {
                "name": segment.name,
                "phase": segment.phase.value,
                "duration_seconds": segment.duration_seconds,
                "start_rate": segment.start_rate,
                "end_rate": segment.end_rate,
                "shape": segment.shape.value,
                "operation_count": segment.operation_count,
            }
            for segment in schedule.segments
        ],
    }


def _profile_to_dict(profile: WorkloadProfile) -> dict[str, object]:
    return {
        "name": profile.name,
        "seed": profile.seed,
        "duration_seconds": profile.duration_seconds,
        "target_operations_per_second": profile.target_operations_per_second,
        "virtual_client_count": profile.virtual_client_count,
        "read_write_mix": {
            "read": profile.read_write_mix.read,
            "write": profile.read_write_mix.write,
        },
        "tenant_distribution": profile.tenant_distribution.value,
        "hot_tenant_fraction": profile.hot_tenant_fraction,
        "hot_entity_fraction": profile.hot_entity_fraction,
        "conflict_probability": profile.conflict_probability,
        "multi_entity_transaction_probability": profile.multi_entity_transaction_probability,
        "expected_version_probability": profile.expected_version_probability,
        "hot_entity_write_probability": profile.hot_entity_write_probability,
        "multi_entity_min_items": profile.multi_entity_min_items,
        "multi_entity_max_items": profile.multi_entity_max_items,
        "operation_weights": [
            {"operation_type": weight.operation_type.value, "weight": weight.weight}
            for weight in profile.operation_weights
        ],
    }


def _weighted_choice[T](
    weighted_items: Iterable[OperationWeight] | Iterable[tuple[T, float]],
    rng: random.Random,
) -> OperationType | T:
    items: list[tuple[OperationType | T, float]] = []
    total = 0.0
    for item in weighted_items:
        if isinstance(item, OperationWeight):
            value: OperationType | T = item.operation_type
            weight = item.weight
        else:
            value = item[0]
            weight = item[1]
        if weight <= 0.0:
            continue
        total += weight
        items.append((value, weight))
    if total <= 0.0:
        msg = "weighted choice requires positive total weight"
        raise ValueError(msg)
    threshold = rng.random() * total
    cumulative = 0.0
    for value, weight in items:
        cumulative += weight
        if threshold <= cumulative:
            return value
    return items[-1][0]


def _operation_id(
    benchmark_seed: int,
    sequence_no: int,
    tenant_id: str,
    operation_type: OperationType,
) -> str:
    return deterministic_uuid(
        "workload-operation",
        benchmark_seed,
        sequence_no,
        tenant_id,
        operation_type.value,
    )


def _transaction_id(
    benchmark_seed: int,
    sequence_no: int,
    tenant_id: str,
    operation_type: OperationType,
) -> str:
    return deterministic_uuid(
        "workload-transaction",
        benchmark_seed,
        sequence_no,
        tenant_id,
        operation_type.value,
    )


def _virtual_client_id(sequence_no: int, virtual_client_count: int) -> str:
    return f"client-{(sequence_no % virtual_client_count) + 1:04d}"


def _nonzero_delta(rng: random.Random) -> int:
    delta = rng.randint(-100, 100)
    return 1 if delta == 0 else delta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic workload JSONL.")
    parser.add_argument("--dataset-manifest", required=True, type=Path)
    parser.add_argument("--profile", required=True, choices=builtin_profile_names())
    parser.add_argument("--schedule", required=True, choices=schedule_preset_names())
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--virtual-clients", type=int)
    parser.add_argument("--target-rate", type=int)
    parser.add_argument("--duration", type=int)
    args = parser.parse_args(argv)

    profile = get_builtin_profile(args.profile, seed=args.seed).with_overrides(
        virtual_client_count=args.virtual_clients,
    )
    schedule = get_schedule_preset(args.schedule)
    if args.target_rate is not None:
        schedule = schedule.with_target_rate(args.target_rate)
        profile = profile.with_overrides(target_operations_per_second=args.target_rate)
    if args.duration is not None:
        schedule = schedule.with_measurement_duration(args.duration)
        profile = profile.with_overrides(duration_seconds=args.duration)

    workload = generate_workload(
        dataset_manifest_path=args.dataset_manifest,
        profile=profile,
        schedule=schedule,
        output_path=args.output,
    )
    print(
        json.dumps(
            {
                "workload": str(workload.workload_path),
                "manifest": str(workload.manifest_path),
                "operation_count": workload.manifest["operation_count"],
                "workload_checksum": workload.manifest["workload_checksum"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
