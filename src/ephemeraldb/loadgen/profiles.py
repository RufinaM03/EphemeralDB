"""Built-in backend-independent workload profiles."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from ephemeraldb.loadgen.models import OperationType, is_read_operation, is_write_operation


class TenantDistribution(StrEnum):
    UNIFORM = "UNIFORM"
    SKEWED = "SKEWED"
    HOT_SET = "HOT_SET"


@dataclass(frozen=True, slots=True)
class ReadWriteMix:
    read: float
    write: float

    def __post_init__(self) -> None:
        _require_probability("read", self.read)
        _require_probability("write", self.write)
        if abs((self.read + self.write) - 1.0) > 0.000001:
            msg = "read/write mix must sum to 1.0"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class OperationWeight:
    operation_type: OperationType
    weight: float

    def __post_init__(self) -> None:
        _require_probability("weight", self.weight, allow_zero=False)


@dataclass(frozen=True, slots=True)
class WorkloadProfile:
    name: str
    seed: int
    duration_seconds: int
    target_operations_per_second: int
    virtual_client_count: int
    read_write_mix: ReadWriteMix
    tenant_distribution: TenantDistribution
    hot_tenant_fraction: float
    hot_entity_fraction: float
    conflict_probability: float
    multi_entity_transaction_probability: float
    operation_weights: tuple[OperationWeight, ...]
    expected_version_probability: float = 0.0
    hot_entity_write_probability: float = 0.0
    multi_entity_min_items: int = 2
    multi_entity_max_items: int = 5

    def __post_init__(self) -> None:
        if not self.name.strip():
            msg = "profile name must not be empty"
            raise ValueError(msg)
        _require_at_least("duration_seconds", self.duration_seconds, 1)
        _require_at_least("target_operations_per_second", self.target_operations_per_second, 1)
        _require_at_least("virtual_client_count", self.virtual_client_count, 1)
        _require_probability("hot_tenant_fraction", self.hot_tenant_fraction)
        _require_probability("hot_entity_fraction", self.hot_entity_fraction)
        _require_probability("conflict_probability", self.conflict_probability)
        _require_probability(
            "multi_entity_transaction_probability",
            self.multi_entity_transaction_probability,
        )
        _require_probability("expected_version_probability", self.expected_version_probability)
        _require_probability("hot_entity_write_probability", self.hot_entity_write_probability)
        _require_at_least("multi_entity_min_items", self.multi_entity_min_items, 2)
        _require_at_least("multi_entity_max_items", self.multi_entity_max_items, 2)
        if self.multi_entity_min_items > self.multi_entity_max_items:
            msg = "multi_entity_min_items must be <= multi_entity_max_items"
            raise ValueError(msg)
        if not self.operation_weights:
            msg = "operation_weights must not be empty"
            raise ValueError(msg)

        total_weight = sum(weight.weight for weight in self.operation_weights)
        if abs(total_weight - 1.0) > 0.000001:
            msg = "operation weights must sum to 1.0"
            raise ValueError(msg)
        read_weight = sum(
            weight.weight
            for weight in self.operation_weights
            if is_read_operation(weight.operation_type)
        )
        write_weight = sum(
            weight.weight
            for weight in self.operation_weights
            if is_write_operation(weight.operation_type)
        )
        if abs(read_weight - self.read_write_mix.read) > 0.000001:
            msg = "read operation weights must match read_write_mix.read"
            raise ValueError(msg)
        if abs(write_weight - self.read_write_mix.write) > 0.000001:
            msg = "write operation weights must match read_write_mix.write"
            raise ValueError(msg)

    def with_overrides(
        self,
        *,
        seed: int | None = None,
        virtual_client_count: int | None = None,
        target_operations_per_second: int | None = None,
        duration_seconds: int | None = None,
    ) -> WorkloadProfile:
        return replace(
            self,
            seed=self.seed if seed is None else seed,
            virtual_client_count=(
                self.virtual_client_count
                if virtual_client_count is None
                else virtual_client_count
            ),
            target_operations_per_second=(
                self.target_operations_per_second
                if target_operations_per_second is None
                else target_operations_per_second
            ),
            duration_seconds=(
                self.duration_seconds if duration_seconds is None else duration_seconds
            ),
        )


READ_COMPOSITION: tuple[tuple[OperationType, float], ...] = (
    (OperationType.ENTITY_LOOKUP, 0.35),
    (OperationType.STATE_SCAN, 0.15),
    (OperationType.RELATIONSHIP_EXPANSION, 0.15),
    (OperationType.METADATA_LOOKUP, 0.10),
    (OperationType.ENTITY_HISTORY, 0.10),
    (OperationType.COMPLEX_JOIN, 0.10),
    (OperationType.AGGREGATE_BY_STATE, 0.05),
)


def get_builtin_profile(name: str, *, seed: int = 42) -> WorkloadProfile:
    normalized_name = name.strip().lower().replace("_", "-")
    if normalized_name == "read-heavy":
        return _profile(
            name="READ_HEAVY",
            seed=seed,
            read_probability=0.90,
            multi_entity_transaction_probability=0.10,
            tenant_distribution=TenantDistribution.UNIFORM,
        )
    if normalized_name == "balanced":
        return _profile(
            name="BALANCED",
            seed=seed,
            read_probability=0.60,
            multi_entity_transaction_probability=0.15,
            tenant_distribution=TenantDistribution.UNIFORM,
        )
    if normalized_name == "write-heavy":
        return _profile(
            name="WRITE_HEAVY",
            seed=seed,
            read_probability=0.30,
            multi_entity_transaction_probability=0.15,
            tenant_distribution=TenantDistribution.UNIFORM,
        )
    if normalized_name == "hot-contention":
        return _profile(
            name="HOT_CONTENTION",
            seed=seed,
            read_probability=0.30,
            multi_entity_transaction_probability=0.10,
            tenant_distribution=TenantDistribution.HOT_SET,
            hot_tenant_fraction=0.10,
            hot_entity_fraction=0.01,
            conflict_probability=0.35,
            expected_version_probability=0.90,
            hot_entity_write_probability=0.90,
        )
    if normalized_name == "multi-tenant-skew":
        return _profile(
            name="MULTI_TENANT_SKEW",
            seed=seed,
            read_probability=0.60,
            multi_entity_transaction_probability=0.15,
            tenant_distribution=TenantDistribution.SKEWED,
        )
    msg = f"unknown workload profile: {name}"
    raise ValueError(msg)


def builtin_profile_names() -> tuple[str, ...]:
    return ("read-heavy", "balanced", "write-heavy", "hot-contention", "multi-tenant-skew")


def _profile(
    *,
    name: str,
    seed: int,
    read_probability: float,
    multi_entity_transaction_probability: float,
    tenant_distribution: TenantDistribution,
    hot_tenant_fraction: float = 0.0,
    hot_entity_fraction: float = 0.0,
    conflict_probability: float = 0.0,
    expected_version_probability: float = 0.0,
    hot_entity_write_probability: float = 0.0,
) -> WorkloadProfile:
    write_probability = round(1.0 - read_probability, 10)
    entity_delta_probability = round(
        write_probability * (1.0 - multi_entity_transaction_probability),
        10,
    )
    multi_probability = round(write_probability * multi_entity_transaction_probability, 10)
    weights = [
        OperationWeight(operation_type, round(read_probability * share, 10))
        for operation_type, share in READ_COMPOSITION
    ]
    weights.append(OperationWeight(OperationType.ENTITY_DELTA, entity_delta_probability))
    weights.append(OperationWeight(OperationType.MULTI_ENTITY_TRANSACTION, multi_probability))
    weight_sum = sum(weight.weight for weight in weights)
    if weight_sum != 1.0:
        last = weights[-1]
        weights[-1] = OperationWeight(
            last.operation_type,
            round(last.weight + (1.0 - weight_sum), 10),
        )

    return WorkloadProfile(
        name=name,
        seed=seed,
        duration_seconds=60,
        target_operations_per_second=100,
        virtual_client_count=50,
        read_write_mix=ReadWriteMix(read=read_probability, write=write_probability),
        tenant_distribution=tenant_distribution,
        hot_tenant_fraction=hot_tenant_fraction,
        hot_entity_fraction=hot_entity_fraction,
        conflict_probability=conflict_probability,
        multi_entity_transaction_probability=multi_entity_transaction_probability,
        operation_weights=tuple(weights),
        expected_version_probability=expected_version_probability,
        hot_entity_write_probability=hot_entity_write_probability,
    )


def _require_at_least(name: str, value: int, minimum: int) -> None:
    if type(value) is not int:
        msg = f"{name} must be an integer"
        raise TypeError(msg)
    if value < minimum:
        msg = f"{name} must be >= {minimum}"
        raise ValueError(msg)


def _require_probability(name: str, value: object, *, allow_zero: bool = True) -> None:
    if not isinstance(value, int | float):
        msg = f"{name} must be numeric"
        raise TypeError(msg)
    if value < 0.0 or value > 1.0 or (not allow_zero and value == 0.0):
        msg = f"{name} must be in {'[0, 1]' if allow_zero else '(0, 1]'}"
        raise ValueError(msg)
