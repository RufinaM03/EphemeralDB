"""Canonical JSON encoding and validation for WAL batches."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from typing import cast

from ephemeraldb.core.errors import (
    UnsupportedWalVersionError,
    WalChecksumError,
    WalEncodingError,
)
from ephemeraldb.wal.codec import (
    canonical_json_bytes,
    encode_record,
    record_from_dict,
    record_to_dict,
)
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.validation import validate_record

from .models import (
    WAL_BATCH_FORMAT_VERSION,
    WalBatch,
    WalBatchRecordIndex,
    new_batch_id,
    utc_now_iso,
)


def build_batch(
    records: Iterable[WalRecord],
    *,
    batch_id: str | None = None,
    created_at: str | None = None,
) -> WalBatch:
    record_tuple = tuple(records)
    if not record_tuple:
        msg = "WAL batch must contain at least one record"
        raise ValueError(msg)
    for record in record_tuple:
        validate_record(record, expected_tenant_id=record.tenant_id)
    batch = WalBatch(
        batch_id=batch_id or new_batch_id(),
        created_at=created_at or utc_now_iso(),
        records=record_tuple,
        record_index=build_record_index(record_tuple),
        record_count=len(record_tuple),
        total_payload_bytes=sum(len(encode_record(record)) for record in record_tuple),
    )
    return attach_batch_checksum(batch)


def attach_batch_checksum(batch: WalBatch) -> WalBatch:
    return WalBatch(
        batch_format_version=batch.batch_format_version,
        batch_id=batch.batch_id,
        created_at=batch.created_at,
        records=batch.records,
        record_index=batch.record_index,
        record_count=batch.record_count,
        total_payload_bytes=batch.total_payload_bytes,
        batch_checksum=calculate_batch_checksum(batch),
    )


def calculate_batch_checksum(batch: WalBatch) -> str:
    return hashlib.sha256(batch_canonical_bytes(batch, include_checksum=False)).hexdigest()


def encode_batch(batch: WalBatch) -> bytes:
    validate_batch(batch)
    return batch_canonical_bytes(batch, include_checksum=True) + b"\n"


def batch_canonical_bytes(batch: WalBatch, *, include_checksum: bool) -> bytes:
    return canonical_json_bytes(batch_to_dict(batch, include_checksum=include_checksum))


def decode_batch(data: bytes | str) -> WalBatch:
    text = data.decode("utf-8") if isinstance(data, bytes) else data
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        msg = f"invalid WAL batch JSON: {exc.msg}"
        raise WalEncodingError(msg) from exc
    batch = batch_from_dict(_as_mapping(payload))
    validate_batch(batch)
    return batch


def batch_to_dict(batch: WalBatch, *, include_checksum: bool) -> dict[str, object]:
    payload: dict[str, object] = {
        "batch_format_version": batch.batch_format_version,
        "batch_id": batch.batch_id,
        "created_at": batch.created_at,
        "record_count": batch.record_count,
        "total_payload_bytes": batch.total_payload_bytes,
        "record_index": [record_index_to_dict(item) for item in batch.record_index],
        "records": [record_to_dict(record, include_checksum=True) for record in batch.records],
    }
    if include_checksum:
        payload["batch_checksum"] = batch.batch_checksum
    return payload


def batch_from_dict(data: Mapping[str, object]) -> WalBatch:
    version = _as_int(data["batch_format_version"])
    if version != WAL_BATCH_FORMAT_VERSION:
        msg = f"unsupported WAL batch format version: {version}"
        raise UnsupportedWalVersionError(msg)
    return WalBatch(
        batch_format_version=version,
        batch_id=_as_str(data["batch_id"]),
        created_at=_as_str(data["created_at"]),
        record_count=_as_int(data["record_count"]),
        total_payload_bytes=_as_int(data["total_payload_bytes"]),
        record_index=tuple(
            record_index_from_dict(_as_mapping(item))
            for item in _as_sequence(data["record_index"])
        ),
        records=tuple(
            record_from_dict(_as_mapping(item)) for item in _as_sequence(data["records"])
        ),
        batch_checksum=_as_optional_str(data.get("batch_checksum")),
    )


def validate_batch(batch: WalBatch) -> None:
    if batch.batch_format_version != WAL_BATCH_FORMAT_VERSION:
        msg = f"unsupported WAL batch format version: {batch.batch_format_version}"
        raise UnsupportedWalVersionError(msg)
    if not batch.batch_id.strip():
        msg = "WAL batch_id must not be empty"
        raise WalEncodingError(msg)
    if not batch.created_at.strip():
        msg = "WAL batch created_at must not be empty"
        raise WalEncodingError(msg)
    if batch.record_count != len(batch.records):
        msg = "WAL batch record_count does not match embedded records"
        raise WalEncodingError(msg)
    expected_payload_bytes = sum(len(encode_record(record)) for record in batch.records)
    if batch.total_payload_bytes != expected_payload_bytes:
        msg = "WAL batch total_payload_bytes does not match embedded records"
        raise WalEncodingError(msg)
    for record in batch.records:
        validate_record(record, expected_tenant_id=record.tenant_id)
    expected_index = build_record_index(batch.records)
    if batch.record_index != expected_index:
        msg = "WAL batch record_index does not match embedded records"
        raise WalEncodingError(msg)
    if batch.batch_checksum is None:
        msg = "WAL batch checksum is missing"
        raise WalChecksumError(msg)
    expected_checksum = calculate_batch_checksum(batch)
    if batch.batch_checksum != expected_checksum:
        msg = "WAL batch checksum mismatch"
        raise WalChecksumError(msg)


def build_record_index(records: tuple[WalRecord, ...]) -> tuple[WalBatchRecordIndex, ...]:
    index: list[WalBatchRecordIndex] = []
    for position, record in enumerate(records):
        if record.record_checksum is None:
            msg = "embedded WAL record checksum is missing"
            raise WalChecksumError(msg)
        index.append(
            WalBatchRecordIndex(
                tenant_id=record.tenant_id,
                sequence_no=record.sequence_no,
                transaction_id=record.transaction_id,
                record_position=position,
                record_checksum=record.record_checksum,
            )
        )
    return tuple(index)


def record_index_to_dict(item: WalBatchRecordIndex) -> dict[str, object]:
    return {
        "tenant_id": item.tenant_id,
        "sequence_no": item.sequence_no,
        "transaction_id": item.transaction_id,
        "record_position": item.record_position,
        "record_checksum": item.record_checksum,
    }


def record_index_from_dict(data: Mapping[str, object]) -> WalBatchRecordIndex:
    return WalBatchRecordIndex(
        tenant_id=_as_str(data["tenant_id"]),
        sequence_no=_as_int(data["sequence_no"]),
        transaction_id=_as_str(data["transaction_id"]),
        record_position=_as_int(data["record_position"]),
        record_checksum=_as_str(data["record_checksum"]),
    )


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
