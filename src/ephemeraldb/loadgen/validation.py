"""Validation for deterministic workload artifacts."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from ephemeraldb.loadgen.catalog import DatasetIdentityCatalog
from ephemeraldb.loadgen.generator import _operation_id, _transaction_id
from ephemeraldb.loadgen.models import (
    WORKLOAD_SCHEMA_VERSION,
    AggregatePayload,
    ComplexJoinPayload,
    EntityDeltaPayload,
    EntityHistoryPayload,
    EntityLookupPayload,
    MetadataLookupPayload,
    MultiEntityTransactionPayload,
    RelationshipExpansionPayload,
    StateScanPayload,
    WorkloadOperation,
    is_read_operation,
    is_write_operation,
)
from ephemeraldb.loadgen.serialization import (
    WorkloadChecksum,
    checksum_basis_from_manifest,
    iter_workload,
    load_workload_manifest,
    manifest_path_for_workload,
)
from ephemeraldb.schema.models import ENTITY_STATES, RELATIONSHIP_TYPES


@dataclass(frozen=True, slots=True)
class WorkloadValidationResult:
    ok: bool
    operation_count: int
    workload_checksum: str | None
    errors: tuple[str, ...]


def validate_workload(
    workload_path: Path,
    manifest_path: Path | None = None,
) -> WorkloadValidationResult:
    workload_path = Path(workload_path)
    manifest_path = (
        manifest_path_for_workload(workload_path) if manifest_path is None else manifest_path
    )
    manifest = load_workload_manifest(manifest_path)
    errors: list[str] = []

    if _as_int(manifest["workload_schema_version"]) != WORKLOAD_SCHEMA_VERSION:
        errors.append("unsupported workload schema version")
    catalog = DatasetIdentityCatalog(cast(dict[str, Any], manifest["dataset"]))
    checksum = WorkloadChecksum(checksum_basis_from_manifest(manifest))
    previous_offset = -1
    previous_client_offset: dict[str, int] = {}
    operation_type_counts: Counter[str] = Counter()
    read_count = 0
    write_count = 0
    operation_count = 0

    for expected_sequence_no, operation in enumerate(iter_workload(workload_path)):
        operation_count += 1
        if operation.sequence_no != expected_sequence_no:
            errors.append(
                f"expected sequence {expected_sequence_no}, found {operation.sequence_no}"
            )
        if operation.scheduled_offset_ms < previous_offset:
            errors.append(f"global schedule moved backward at sequence {operation.sequence_no}")
        previous_offset = operation.scheduled_offset_ms
        previous_for_client = previous_client_offset.get(operation.virtual_client_id, -1)
        if operation.scheduled_offset_ms < previous_for_client:
            errors.append(
                f"client {operation.virtual_client_id} moved backward at sequence "
                f"{operation.sequence_no}"
            )
        previous_client_offset[operation.virtual_client_id] = operation.scheduled_offset_ms

        expected_operation_id = _operation_id(
            _as_int(manifest["seed"]),
            operation.sequence_no,
            operation.tenant_id,
            operation.operation_type,
        )
        if operation.operation_id != expected_operation_id:
            errors.append(f"operation_id mismatch at sequence {operation.sequence_no}")
        if operation.tenant_id not in catalog.tenant_ids:
            errors.append(f"unknown tenant_id at sequence {operation.sequence_no}")
        _validate_payload(operation, catalog, _as_int(manifest["seed"]), errors)
        checksum.update(operation)
        operation_type_counts[operation.operation_type.value] += 1
        read_count += int(is_read_operation(operation.operation_type))
        write_count += int(is_write_operation(operation.operation_type))

    if operation_count != _as_int(manifest["operation_count"]):
        errors.append(
            "operation_count mismatch: "
            f"manifest={manifest['operation_count']} actual={operation_count}"
        )
    if dict(sorted(operation_type_counts.items())) != _as_dict_int(
        manifest["operation_type_counts"]
    ):
        errors.append("operation_type_counts mismatch")
    if read_count != _as_int(manifest["read_count"]):
        errors.append(f"read_count mismatch: manifest={manifest['read_count']} actual={read_count}")
    if write_count != _as_int(manifest["write_count"]):
        errors.append(
            f"write_count mismatch: manifest={manifest['write_count']} actual={write_count}"
        )

    actual_checksum = checksum.hexdigest()
    if actual_checksum != _as_str(manifest["workload_checksum"]):
        errors.append("workload checksum mismatch")
    return WorkloadValidationResult(
        ok=not errors,
        operation_count=operation_count,
        workload_checksum=actual_checksum,
        errors=tuple(errors),
    )


def _validate_payload(
    operation: WorkloadOperation,
    catalog: DatasetIdentityCatalog,
    workload_seed: int,
    errors: list[str],
) -> None:
    payload = operation.payload
    if isinstance(payload, EntityLookupPayload | MetadataLookupPayload | EntityHistoryPayload):
        _validate_entity(operation, catalog, payload.entity_id, errors)
        if isinstance(payload, MetadataLookupPayload) and payload.metadata_key is not None:
            _validate_metadata_key(catalog, operation.sequence_no, payload.metadata_key, errors)
    elif isinstance(payload, StateScanPayload):
        _validate_state(operation.sequence_no, payload.state, errors)
        if payload.limit <= 0:
            errors.append(f"invalid state scan limit at sequence {operation.sequence_no}")
    elif isinstance(payload, RelationshipExpansionPayload):
        _validate_entity(operation, catalog, payload.source_entity_id, errors)
        if payload.limit <= 0:
            errors.append(
                f"invalid relationship expansion limit at sequence {operation.sequence_no}"
            )
    elif isinstance(payload, ComplexJoinPayload):
        _validate_state(operation.sequence_no, payload.source_state, errors)
        if payload.relationship_type not in RELATIONSHIP_TYPES:
            errors.append(f"invalid relationship type at sequence {operation.sequence_no}")
        _validate_metadata_key(catalog, operation.sequence_no, payload.metadata_key, errors)
        if payload.limit <= 0:
            errors.append(f"invalid complex join limit at sequence {operation.sequence_no}")
    elif isinstance(payload, AggregatePayload):
        if payload.state_filter is not None:
            _validate_state(operation.sequence_no, payload.state_filter, errors)
    elif isinstance(payload, EntityDeltaPayload):
        _validate_entity(operation, catalog, payload.entity_id, errors)
        _validate_transaction_id(operation, workload_seed, payload.transaction_id, errors)
        if payload.delta == 0:
            errors.append(f"zero delta at sequence {operation.sequence_no}")
    elif isinstance(payload, MultiEntityTransactionPayload):
        _validate_transaction_id(operation, workload_seed, payload.transaction_id, errors)
        if not 2 <= len(payload.items) <= 5:
            errors.append(f"invalid multi-entity item count at sequence {operation.sequence_no}")
        entity_ids = [item.entity_id for item in payload.items]
        if len(set(entity_ids)) != len(entity_ids):
            errors.append(f"duplicate multi-entity item at sequence {operation.sequence_no}")
        for item in payload.items:
            _validate_entity(operation, catalog, item.entity_id, errors)
            if item.delta == 0:
                errors.append(f"zero multi-entity delta at sequence {operation.sequence_no}")


def _validate_entity(
    operation: WorkloadOperation,
    catalog: DatasetIdentityCatalog,
    entity_id: str,
    errors: list[str],
) -> None:
    if not catalog.is_valid_entity_id(operation.tenant_id, entity_id):
        errors.append(f"unknown entity_id at sequence {operation.sequence_no}")


def _validate_metadata_key(
    catalog: DatasetIdentityCatalog,
    sequence_no: int,
    metadata_key: str,
    errors: list[str],
) -> None:
    valid_keys = {
        catalog.metadata_key(index) for index in range(catalog.metadata_entries_per_entity)
    }
    if metadata_key not in valid_keys:
        errors.append(f"invalid metadata key at sequence {sequence_no}")


def _validate_state(sequence_no: int, state: str, errors: list[str]) -> None:
    if state not in ENTITY_STATES:
        errors.append(f"invalid state at sequence {sequence_no}")


def _validate_transaction_id(
    operation: WorkloadOperation,
    workload_seed: int,
    transaction_id: str,
    errors: list[str],
) -> None:
    expected = _transaction_id(
        workload_seed,
        operation.sequence_no,
        operation.tenant_id,
        operation.operation_type,
    )
    if transaction_id != expected:
        errors.append(f"transaction_id mismatch at sequence {operation.sequence_no}")


def _as_int(value: object) -> int:
    if type(value) is not int:
        msg = f"expected integer, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        msg = f"expected string, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _as_dict_int(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        msg = f"expected object, got {type(value).__name__}"
        raise TypeError(msg)
    result: dict[str, int] = {}
    for key, item in value.items():
        result[_as_str(key)] = _as_int(item)
    return dict(sorted(result.items()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate deterministic workload JSONL.")
    parser.add_argument("workload", type=Path)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args(argv)
    result = validate_workload(args.workload, args.manifest)
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
