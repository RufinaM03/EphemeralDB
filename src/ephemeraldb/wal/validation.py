"""Validation helpers for explicit logical WAL records and chains."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

from ephemeraldb.core.errors import (
    UnsupportedWalVersionError,
    WalChainError,
    WalChecksumError,
    WalEncodingError,
    WalSequenceError,
)
from ephemeraldb.loadgen.models import OperationType
from ephemeraldb.schema.models import ENTITY_STATES, TRANSACTION_STATUSES
from ephemeraldb.wal.checksum import calculate_record_checksum
from ephemeraldb.wal.models import (
    WAL_FORMAT_VERSION,
    WalField,
    WalMutation,
    WalMutationType,
    WalRecord,
    WalScalar,
    WalSummary,
    fields_to_dict,
)

CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")

EXPECTED_TABLES: dict[WalMutationType, str] = {
    WalMutationType.UPDATE_ENTITY: "entities",
    WalMutationType.INSERT_ENTITY_VERSION: "entity_versions",
    WalMutationType.INSERT_TRANSACTION: "transactions",
    WalMutationType.INSERT_TRANSACTION_ITEM: "transaction_items",
    WalMutationType.INSERT_AUDIT_LOG: "audit_log",
}

REQUIRED_KEY_FIELDS: dict[WalMutationType, frozenset[str]] = {
    WalMutationType.UPDATE_ENTITY: frozenset({"tenant_id", "entity_id"}),
    WalMutationType.INSERT_ENTITY_VERSION: frozenset({"tenant_id", "entity_id", "version"}),
    WalMutationType.INSERT_TRANSACTION: frozenset({"tenant_id", "transaction_id"}),
    WalMutationType.INSERT_TRANSACTION_ITEM: frozenset({"transaction_id", "item_no"}),
    WalMutationType.INSERT_AUDIT_LOG: frozenset({"audit_id"}),
}

REQUIRED_VALUE_FIELDS: dict[WalMutationType, frozenset[str]] = {
    WalMutationType.UPDATE_ENTITY: frozenset(
        {"tenant_id", "entity_id", "state", "numeric_value", "version", "updated_at"}
    ),
    WalMutationType.INSERT_ENTITY_VERSION: frozenset(
        {"tenant_id", "entity_id", "version", "state", "numeric_value", "created_at"}
    ),
    WalMutationType.INSERT_TRANSACTION: frozenset(
        {"transaction_id", "tenant_id", "status", "created_at", "completed_at"}
    ),
    WalMutationType.INSERT_TRANSACTION_ITEM: frozenset(
        {
            "transaction_id",
            "item_no",
            "tenant_id",
            "entity_id",
            "delta",
            "previous_version",
            "new_version",
        }
    ),
    WalMutationType.INSERT_AUDIT_LOG: frozenset(
        {
            "audit_id",
            "tenant_id",
            "transaction_id",
            "entity_id",
            "operation",
            "sequence_no",
            "created_at",
        }
    ),
}


def validate_record(record: WalRecord, *, expected_tenant_id: str | None = None) -> None:
    if record.wal_format_version not in {1, WAL_FORMAT_VERSION}:
        msg = f"unsupported WAL format version: {record.wal_format_version}"
        raise UnsupportedWalVersionError(msg)
    if record.wal_format_version == 2 and (
        record.request_fingerprint is None
        or re.fullmatch(r"v1:[0-9a-f]{64}", record.request_fingerprint) is None
    ):
        raise WalEncodingError("WAL v2 requires a v1 request fingerprint")
    if record.wal_format_version == 1 and record.request_fingerprint is not None:
        raise WalEncodingError("WAL v1 cannot carry request identity")
    if expected_tenant_id is not None and record.tenant_id != expected_tenant_id:
        msg = f"WAL tenant mismatch: expected {expected_tenant_id}, got {record.tenant_id}"
        raise WalEncodingError(msg)
    if not record.tenant_id.strip():
        msg = "WAL tenant_id is required"
        raise WalEncodingError(msg)
    if not record.transaction_id.strip():
        msg = "WAL transaction_id is required"
        raise WalEncodingError(msg)
    if record.sequence_no <= 0:
        msg = f"WAL sequence must be positive: {record.sequence_no}"
        raise WalSequenceError(msg)
    if record.owner_epoch < 0:
        msg = f"WAL owner_epoch must be non-negative: {record.owner_epoch}"
        raise WalEncodingError(msg)
    if record.operation_type not in {
        OperationType.ENTITY_DELTA,
        OperationType.MULTI_ENTITY_TRANSACTION,
    }:
        msg = f"WAL records only support write operation types: {record.operation_type.value}"
        raise WalEncodingError(msg)
    if not record.operations:
        msg = "WAL record must contain at least one logical mutation"
        raise WalEncodingError(msg)
    if not record.created_at.strip():
        msg = "WAL created_at is required"
        raise WalEncodingError(msg)
    _validate_optional_checksum(record.previous_record_checksum, "previous_record_checksum")
    if record.record_checksum is None:
        msg = "WAL record_checksum is required"
        raise WalChecksumError(msg)
    _validate_optional_checksum(record.record_checksum, "record_checksum")
    expected_checksum = calculate_record_checksum(record)
    if record.record_checksum != expected_checksum:
        msg = "WAL record checksum does not match canonical payload"
        raise WalChecksumError(msg)
    for mutation in record.operations:
        _validate_mutation(record, mutation)


def validate_chain(
    records: Iterable[WalRecord],
    *,
    expected_tenant_id: str | None = None,
) -> None:
    expected_sequence = 1
    previous_checksum: str | None = None
    for record in records:
        validate_record(record, expected_tenant_id=expected_tenant_id)
        if record.sequence_no != expected_sequence:
            msg = f"expected WAL sequence {expected_sequence}, got {record.sequence_no}"
            raise WalSequenceError(msg)
        if record.previous_record_checksum != previous_checksum:
            msg = f"WAL chain mismatch at sequence {record.sequence_no}"
            raise WalChainError(msg)
        previous_checksum = record.record_checksum
        expected_sequence += 1


def wal_summary(wal_root: Path, tenant_id: str) -> WalSummary:
    from ephemeraldb.wal.reader import iter_record_paths, iter_records

    paths = list(iter_record_paths(wal_root, tenant_id))
    records = list(iter_records(wal_root, tenant_id))
    validate_chain(records, expected_tenant_id=tenant_id)
    return WalSummary(
        tenant_id=tenant_id,
        record_count=len(records),
        first_sequence=None if not records else records[0].sequence_no,
        last_sequence=None if not records else records[-1].sequence_no,
        total_bytes=sum(path.stat().st_size for _sequence, path in paths),
        unique_transactions=len({record.transaction_id for record in records}),
        checksum_chain_valid=True,
    )


def _validate_mutation(record: WalRecord, mutation: WalMutation) -> None:
    if mutation.table != EXPECTED_TABLES[mutation.mutation_type]:
        msg = (
            f"WAL mutation {mutation.mutation_type.value} targets {mutation.table}, "
            f"expected {EXPECTED_TABLES[mutation.mutation_type]}"
        )
        raise WalEncodingError(msg)
    key = _fields_to_dict(mutation.key)
    values = _fields_to_dict(mutation.values)
    _require_fields(key, REQUIRED_KEY_FIELDS[mutation.mutation_type], "key")
    _require_fields(values, REQUIRED_VALUE_FIELDS[mutation.mutation_type], "values")
    if values.get("tenant_id") != record.tenant_id:
        msg = f"WAL mutation tenant mismatch for {mutation.mutation_type.value}"
        raise WalEncodingError(msg)
    if "transaction_id" in values and values["transaction_id"] != record.transaction_id:
        msg = f"WAL mutation transaction mismatch for {mutation.mutation_type.value}"
        raise WalEncodingError(msg)
    _validate_key_values_match(key, values)
    _validate_known_value_domains(mutation.mutation_type, values)


def _fields_to_dict(fields: tuple[WalField, ...]) -> dict[str, WalScalar]:
    try:
        return fields_to_dict(fields)
    except ValueError as exc:
        raise WalEncodingError(str(exc)) from exc


def _require_fields(
    mapping: dict[str, WalScalar],
    required: frozenset[str],
    field_group: str,
) -> None:
    missing = required.difference(mapping)
    if missing:
        msg = f"WAL mutation {field_group} missing fields: {', '.join(sorted(missing))}"
        raise WalEncodingError(msg)


def _validate_key_values_match(
    key: dict[str, WalScalar],
    values: dict[str, WalScalar],
) -> None:
    for name, value in key.items():
        if name in values and values[name] != value:
            msg = f"WAL mutation key/value mismatch for {name}"
            raise WalEncodingError(msg)


def _validate_known_value_domains(
    mutation_type: WalMutationType,
    values: dict[str, WalScalar],
) -> None:
    if mutation_type in {WalMutationType.UPDATE_ENTITY, WalMutationType.INSERT_ENTITY_VERSION}:
        state = values["state"]
        if state not in ENTITY_STATES:
            msg = f"invalid entity state in WAL mutation: {state}"
            raise WalEncodingError(msg)
    if mutation_type == WalMutationType.INSERT_TRANSACTION:
        status = values["status"]
        if status not in TRANSACTION_STATUSES:
            msg = f"invalid transaction status in WAL mutation: {status}"
            raise WalEncodingError(msg)


def _validate_optional_checksum(checksum: str | None, field_name: str) -> None:
    if checksum is not None and CHECKSUM_RE.fullmatch(checksum) is None:
        msg = f"{field_name} must be a lowercase SHA-256 hex digest"
        raise WalChecksumError(msg)
