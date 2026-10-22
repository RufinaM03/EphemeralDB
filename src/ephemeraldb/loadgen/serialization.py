"""Deterministic workload JSONL serialization."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from ephemeraldb.loadgen.models import (
    WORKLOAD_SCHEMA_VERSION,
    AggregatePayload,
    BenchmarkPhase,
    ComplexJoinPayload,
    EntityDeltaPayload,
    EntityHistoryPayload,
    EntityLookupPayload,
    MetadataLookupPayload,
    MultiEntityTransactionItem,
    MultiEntityTransactionPayload,
    OperationPayload,
    OperationType,
    RelationshipExpansionPayload,
    StateScanPayload,
    WorkloadOperation,
)


def operation_to_dict(operation: WorkloadOperation) -> dict[str, object]:
    return {
        "schema_version": WORKLOAD_SCHEMA_VERSION,
        "operation_id": operation.operation_id,
        "sequence_no": operation.sequence_no,
        "tenant_id": operation.tenant_id,
        "operation_type": operation.operation_type.value,
        "scheduled_offset_ms": operation.scheduled_offset_ms,
        "benchmark_phase": operation.benchmark_phase.value,
        "virtual_client_id": operation.virtual_client_id,
        "payload": payload_to_dict(operation.payload),
    }


def operation_from_dict(data: Mapping[str, object]) -> WorkloadOperation:
    schema_version = _as_int(data["schema_version"])
    if schema_version != WORKLOAD_SCHEMA_VERSION:
        msg = f"unsupported workload schema version: {schema_version}"
        raise ValueError(msg)
    operation_type = OperationType(_as_str(data["operation_type"]))
    payload = payload_from_dict(operation_type, _as_mapping(data["payload"]))
    return WorkloadOperation(
        operation_id=_as_str(data["operation_id"]),
        sequence_no=_as_int(data["sequence_no"]),
        tenant_id=_as_str(data["tenant_id"]),
        operation_type=operation_type,
        scheduled_offset_ms=_as_int(data["scheduled_offset_ms"]),
        benchmark_phase=BenchmarkPhase(_as_str(data["benchmark_phase"])),
        virtual_client_id=_as_str(data["virtual_client_id"]),
        payload=payload,
    )


def payload_to_dict(payload: OperationPayload) -> dict[str, object]:
    if isinstance(payload, MultiEntityTransactionPayload):
        return {
            "transaction_id": payload.transaction_id,
            "items": [asdict(item) for item in payload.items],
        }
    return cast(dict[str, object], asdict(payload))


def payload_from_dict(
    operation_type: OperationType,
    payload: Mapping[str, object],
) -> OperationPayload:
    if operation_type == OperationType.ENTITY_LOOKUP:
        return EntityLookupPayload(entity_id=_as_str(payload["entity_id"]))
    if operation_type == OperationType.STATE_SCAN:
        return StateScanPayload(
            state=_as_str(payload["state"]),
            limit=_as_int(payload["limit"]),
        )
    if operation_type == OperationType.RELATIONSHIP_EXPANSION:
        return RelationshipExpansionPayload(
            source_entity_id=_as_str(payload["source_entity_id"]),
            limit=_as_int(payload["limit"]),
        )
    if operation_type == OperationType.METADATA_LOOKUP:
        metadata_key_value = payload.get("metadata_key")
        return MetadataLookupPayload(
            entity_id=_as_str(payload["entity_id"]),
            metadata_key=None if metadata_key_value is None else _as_str(metadata_key_value),
        )
    if operation_type == OperationType.ENTITY_HISTORY:
        return EntityHistoryPayload(
            entity_id=_as_str(payload["entity_id"]),
            limit=_as_int(payload["limit"]),
        )
    if operation_type == OperationType.COMPLEX_JOIN:
        return ComplexJoinPayload(
            source_state=_as_str(payload["source_state"]),
            relationship_type=_as_str(payload["relationship_type"]),
            metadata_key=_as_str(payload["metadata_key"]),
            limit=_as_int(payload["limit"]),
        )
    if operation_type == OperationType.AGGREGATE_BY_STATE:
        state_filter_value = payload.get("state_filter")
        return AggregatePayload(
            state_filter=None if state_filter_value is None else _as_str(state_filter_value)
        )
    if operation_type == OperationType.ENTITY_DELTA:
        expected_value = payload.get("expected_version")
        return EntityDeltaPayload(
            transaction_id=_as_str(payload["transaction_id"]),
            entity_id=_as_str(payload["entity_id"]),
            delta=_as_int(payload["delta"]),
            expected_version=None if expected_value is None else _as_int(expected_value),
            expected_version_is_deliberately_stale=_as_bool(
                payload["expected_version_is_deliberately_stale"]
            ),
        )
    if operation_type == OperationType.MULTI_ENTITY_TRANSACTION:
        return MultiEntityTransactionPayload(
            transaction_id=_as_str(payload["transaction_id"]),
            items=tuple(
                _multi_entity_item_from_dict(item)
                for item in _as_sequence(payload["items"])
            ),
        )


def iter_workload(path: Path) -> Iterator[WorkloadOperation]:
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield operation_from_dict(cast(dict[str, object], json.loads(line)))


def canonical_operation_json(operation: WorkloadOperation) -> str:
    return canonical_json(operation_to_dict(operation))


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def manifest_path_for_workload(workload_path: Path) -> Path:
    return workload_path.with_suffix(".manifest.json")


def load_workload_manifest(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(Path(path).read_text(encoding="utf-8")))


def write_workload_manifest(path: Path, manifest: dict[str, object]) -> None:
    Path(path).write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def checksum_basis_from_manifest(manifest: Mapping[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in manifest.items()
        if key not in {"workload_checksum", "operation_type_counts", "read_count", "write_count"}
    }


class WorkloadChecksum:
    def __init__(self, basis: Mapping[str, object]) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"workload-basis:")
        self._digest.update(canonical_json(dict(basis)).encode())
        self._digest.update(b"\n")

    def update(self, operation: WorkloadOperation) -> None:
        self._digest.update(canonical_operation_json(operation).encode())
        self._digest.update(b"\n")

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _multi_entity_item_from_dict(data: object) -> MultiEntityTransactionItem:
    mapping = _as_mapping(data)
    expected_value = mapping.get("expected_version")
    return MultiEntityTransactionItem(
        entity_id=_as_str(mapping["entity_id"]),
        delta=_as_int(mapping["delta"]),
        expected_version=None if expected_value is None else _as_int(expected_value),
        expected_version_is_deliberately_stale=_as_bool(
            mapping["expected_version_is_deliberately_stale"]
        ),
    )


def _as_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        msg = f"expected object, got {type(value).__name__}"
        raise TypeError(msg)
    return cast(Mapping[str, object], value)


def _as_sequence(value: object) -> tuple[object, ...]:
    if not isinstance(value, list):
        msg = f"expected list, got {type(value).__name__}"
        raise TypeError(msg)
    return tuple(value)


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        msg = f"expected string, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _as_int(value: object) -> int:
    if type(value) is not int:
        msg = f"expected integer, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _as_bool(value: object) -> bool:
    if type(value) is not bool:
        msg = f"expected boolean, got {type(value).__name__}"
        raise TypeError(msg)
    return value
