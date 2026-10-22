"""Typed workload operation models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

WORKLOAD_SCHEMA_VERSION = 1


class OperationType(StrEnum):
    ENTITY_LOOKUP = "ENTITY_LOOKUP"
    STATE_SCAN = "STATE_SCAN"
    RELATIONSHIP_EXPANSION = "RELATIONSHIP_EXPANSION"
    METADATA_LOOKUP = "METADATA_LOOKUP"
    ENTITY_HISTORY = "ENTITY_HISTORY"
    COMPLEX_JOIN = "COMPLEX_JOIN"
    AGGREGATE_BY_STATE = "AGGREGATE_BY_STATE"
    ENTITY_DELTA = "ENTITY_DELTA"
    MULTI_ENTITY_TRANSACTION = "MULTI_ENTITY_TRANSACTION"


class BenchmarkPhase(StrEnum):
    WARMUP = "WARMUP"
    MEASUREMENT = "MEASUREMENT"
    COOLDOWN = "COOLDOWN"


READ_OPERATION_TYPES = frozenset(
    {
        OperationType.ENTITY_LOOKUP,
        OperationType.STATE_SCAN,
        OperationType.RELATIONSHIP_EXPANSION,
        OperationType.METADATA_LOOKUP,
        OperationType.ENTITY_HISTORY,
        OperationType.COMPLEX_JOIN,
        OperationType.AGGREGATE_BY_STATE,
    }
)

WRITE_OPERATION_TYPES = frozenset(
    {
        OperationType.ENTITY_DELTA,
        OperationType.MULTI_ENTITY_TRANSACTION,
    }
)


@dataclass(frozen=True, slots=True)
class EntityLookupPayload:
    entity_id: str


@dataclass(frozen=True, slots=True)
class StateScanPayload:
    state: str
    limit: int


@dataclass(frozen=True, slots=True)
class RelationshipExpansionPayload:
    source_entity_id: str
    limit: int


@dataclass(frozen=True, slots=True)
class MetadataLookupPayload:
    entity_id: str
    metadata_key: str | None = None


@dataclass(frozen=True, slots=True)
class EntityHistoryPayload:
    entity_id: str
    limit: int


@dataclass(frozen=True, slots=True)
class ComplexJoinPayload:
    source_state: str
    relationship_type: str
    metadata_key: str
    limit: int


@dataclass(frozen=True, slots=True)
class AggregatePayload:
    state_filter: str | None = None


@dataclass(frozen=True, slots=True)
class EntityDeltaPayload:
    transaction_id: str
    entity_id: str
    delta: int
    expected_version: int | None = None
    expected_version_is_deliberately_stale: bool = False


@dataclass(frozen=True, slots=True)
class MultiEntityTransactionItem:
    entity_id: str
    delta: int
    expected_version: int | None = None
    expected_version_is_deliberately_stale: bool = False


@dataclass(frozen=True, slots=True)
class MultiEntityTransactionPayload:
    transaction_id: str
    items: tuple[MultiEntityTransactionItem, ...]


OperationPayload = (
    EntityLookupPayload
    | StateScanPayload
    | RelationshipExpansionPayload
    | MetadataLookupPayload
    | EntityHistoryPayload
    | ComplexJoinPayload
    | AggregatePayload
    | EntityDeltaPayload
    | MultiEntityTransactionPayload
)


@dataclass(frozen=True, slots=True)
class WorkloadOperation:
    operation_id: str
    sequence_no: int
    tenant_id: str
    operation_type: OperationType
    scheduled_offset_ms: int
    benchmark_phase: BenchmarkPhase
    virtual_client_id: str
    payload: OperationPayload

    def __post_init__(self) -> None:
        if self.sequence_no < 0:
            msg = "sequence_no must be non-negative"
            raise ValueError(msg)
        if self.scheduled_offset_ms < 0:
            msg = "scheduled_offset_ms must be non-negative"
            raise ValueError(msg)
        if not self.operation_id.strip():
            msg = "operation_id must not be empty"
            raise ValueError(msg)
        if not self.tenant_id.strip():
            msg = "tenant_id must not be empty"
            raise ValueError(msg)
        if not self.virtual_client_id.strip():
            msg = "virtual_client_id must not be empty"
            raise ValueError(msg)


def is_read_operation(operation_type: OperationType) -> bool:
    return operation_type in READ_OPERATION_TYPES


def is_write_operation(operation_type: OperationType) -> bool:
    return operation_type in WRITE_OPERATION_TYPES
