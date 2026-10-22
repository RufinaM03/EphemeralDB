"""Typed immutable models for the explicit logical WAL."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ephemeraldb.loadgen.models import OperationType

WAL_FORMAT_VERSION = 2
LOCAL_OWNER_EPOCH = 0
SEQUENCE_FILENAME_WIDTH = 20

WalScalar = str | int | float | bool | None


class WalMutationType(StrEnum):
    UPDATE_ENTITY = "UPDATE_ENTITY"
    INSERT_ENTITY_VERSION = "INSERT_ENTITY_VERSION"
    INSERT_TRANSACTION = "INSERT_TRANSACTION"
    INSERT_TRANSACTION_ITEM = "INSERT_TRANSACTION_ITEM"
    INSERT_AUDIT_LOG = "INSERT_AUDIT_LOG"


@dataclass(frozen=True, slots=True)
class WalField:
    name: str
    value: WalScalar


@dataclass(frozen=True, slots=True)
class WalMutation:
    mutation_type: WalMutationType
    table: str
    key: tuple[WalField, ...]
    values: tuple[WalField, ...]

    @classmethod
    def from_mappings(
        cls,
        mutation_type: WalMutationType,
        table: str,
        *,
        key: dict[str, WalScalar],
        values: dict[str, WalScalar],
    ) -> WalMutation:
        return cls(
            mutation_type=mutation_type,
            table=table,
            key=fields_from_mapping(key),
            values=fields_from_mapping(values),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class WalRecord:
    wal_format_version: int = WAL_FORMAT_VERSION
    tenant_id: str
    transaction_id: str
    sequence_no: int
    owner_epoch: int = LOCAL_OWNER_EPOCH
    operation_type: OperationType
    operations: tuple[WalMutation, ...]
    created_at: str
    previous_record_checksum: str | None
    record_checksum: str | None = None
    request_fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class WalSummary:
    tenant_id: str
    record_count: int
    first_sequence: int | None
    last_sequence: int | None
    total_bytes: int
    unique_transactions: int
    checksum_chain_valid: bool


def fields_from_mapping(mapping: dict[str, WalScalar]) -> tuple[WalField, ...]:
    return tuple(WalField(name, mapping[name]) for name in sorted(mapping))


def fields_to_dict(fields: tuple[WalField, ...]) -> dict[str, WalScalar]:
    output: dict[str, WalScalar] = {}
    for field in fields:
        if field.name in output:
            msg = f"duplicate WAL field: {field.name}"
            raise ValueError(msg)
        output[field.name] = field.value
    return output
