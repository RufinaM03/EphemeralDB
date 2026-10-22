"""Canonical JSON encoding for explicit logical WAL records."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import cast

from ephemeraldb.core.errors import WalEncodingError
from ephemeraldb.loadgen.models import OperationType
from ephemeraldb.wal.models import (
    WalMutation,
    WalMutationType,
    WalRecord,
    WalScalar,
    fields_from_mapping,
    fields_to_dict,
)


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def encode_record(record: WalRecord) -> bytes:
    return canonical_json_bytes(record_to_dict(record, include_checksum=True)) + b"\n"


def decode_record(data: bytes | str) -> WalRecord:
    if isinstance(data, bytes):
        text = data.decode("utf-8")
    else:
        text = data
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        msg = f"invalid WAL JSON: {exc.msg}"
        raise WalEncodingError(msg) from exc
    return record_from_dict(_as_mapping(payload))


def record_to_dict(record: WalRecord, *, include_checksum: bool) -> dict[str, object]:
    payload: dict[str, object] = {
        "wal_format_version": record.wal_format_version,
        "tenant_id": record.tenant_id,
        "transaction_id": record.transaction_id,
        "sequence_no": record.sequence_no,
        "owner_epoch": record.owner_epoch,
        "operation_type": record.operation_type.value,
        "operations": [mutation_to_dict(mutation) for mutation in record.operations],
        "created_at": record.created_at,
        "previous_record_checksum": record.previous_record_checksum,
    }
    if include_checksum:
        payload["record_checksum"] = record.record_checksum
    if record.wal_format_version >= 2:
        payload["request_fingerprint"] = record.request_fingerprint
    return payload


def record_from_dict(data: Mapping[str, object]) -> WalRecord:
    operation_type = OperationType(_as_str(data["operation_type"]))
    return WalRecord(
        wal_format_version=_as_int(data["wal_format_version"]),
        tenant_id=_as_str(data["tenant_id"]),
        transaction_id=_as_str(data["transaction_id"]),
        sequence_no=_as_int(data["sequence_no"]),
        owner_epoch=_as_int(data["owner_epoch"]),
        operation_type=operation_type,
        operations=tuple(
            mutation_from_dict(_as_mapping(item)) for item in _as_sequence(data["operations"])
        ),
        created_at=_as_str(data["created_at"]),
        previous_record_checksum=_as_optional_str(data["previous_record_checksum"]),
        record_checksum=_as_optional_str(data.get("record_checksum")),
        request_fingerprint=_as_optional_str(data.get("request_fingerprint")),
    )


def mutation_to_dict(mutation: WalMutation) -> dict[str, object]:
    return {
        "type": mutation.mutation_type.value,
        "table": mutation.table,
        "key": fields_to_dict(mutation.key),
        "values": fields_to_dict(mutation.values),
    }


def mutation_from_dict(data: Mapping[str, object]) -> WalMutation:
    return WalMutation(
        mutation_type=WalMutationType(_as_str(data["type"])),
        table=_as_str(data["table"]),
        key=fields_from_mapping(_scalar_mapping(_as_mapping(data["key"]))),
        values=fields_from_mapping(_scalar_mapping(_as_mapping(data["values"]))),
    )


def _scalar_mapping(data: Mapping[str, object]) -> dict[str, WalScalar]:
    return {str(key): _as_scalar(value) for key, value in data.items()}


def _as_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        msg = f"expected object, got {type(value).__name__}"
        raise WalEncodingError(msg)
    return cast(Mapping[str, object], value)


def _as_sequence(value: object) -> Sequence[object]:
    if not isinstance(value, list):
        msg = f"expected list, got {type(value).__name__}"
        raise WalEncodingError(msg)
    return cast(Sequence[object], value)


def _as_scalar(value: object) -> WalScalar:
    if value is None or type(value) in {str, int, float, bool}:
        return cast(WalScalar, value)
    msg = f"expected scalar WAL value, got {type(value).__name__}"
    raise WalEncodingError(msg)


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        msg = f"expected string, got {type(value).__name__}"
        raise WalEncodingError(msg)
    return value


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    return _as_str(value)


def _as_int(value: object) -> int:
    if type(value) is not int:
        msg = f"expected integer, got {type(value).__name__}"
        raise WalEncodingError(msg)
    return value


def load_record_file(path: str) -> WalRecord:
    with open(path, "rb") as handle:
        return decode_record(handle.read())
