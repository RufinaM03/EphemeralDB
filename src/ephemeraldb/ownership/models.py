"""Canonical, strictly validated models for fenced ownership."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Self

AUTHORITY_FORMAT = "ephemeraldb-fenced-v1"
MAX_MODEL_BYTES = 64 * 1024


class AuthorityFormatError(ValueError):
    """Authority bytes are malformed, oversized, or from another protocol."""


class CommitDecision(StrEnum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    ALREADY_ACCEPTED = "ALREADY_ACCEPTED"


@dataclass(frozen=True, slots=True)
class OwnerToken:
    namespace: str
    tenant_id: str
    session_id: str
    epoch: int


@dataclass(frozen=True, slots=True)
class PayloadLocator:
    bucket: str
    key: str
    payload_checksum: str
    batch_id: str | None = None
    batch_checksum: str | None = None
    record_index: int | None = None
    record_count: int | None = None


@dataclass(frozen=True, slots=True)
class CommitDescriptor:
    format: str
    namespace: str
    tenant_id: str
    sequence_no: int
    owner_session: str
    owner_epoch: int
    attempt_id: str
    transaction_id: str
    request_fingerprint: str
    record_checksum: str
    previous_record_checksum: str | None
    previous_descriptor_key: str | None
    previous_descriptor_checksum: str | None
    locator: PayloadLocator

    def encode(self) -> bytes:
        return _encode(asdict(self))

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.encode()).hexdigest()

    @classmethod
    def decode(cls, raw: bytes) -> Self:
        data = _decode(raw)
        _exact_keys(
            data,
            {
                "format", "namespace", "tenant_id", "sequence_no", "owner_session",
                "owner_epoch", "attempt_id", "transaction_id", "request_fingerprint",
                "record_checksum", "previous_record_checksum", "previous_descriptor_key",
                "previous_descriptor_checksum", "locator",
            },
        )
        locator_data = data["locator"]
        if not isinstance(locator_data, dict):
            raise AuthorityFormatError("descriptor locator must be an object")
        _exact_keys(locator_data, set(PayloadLocator.__dataclass_fields__))
        result = cls(
            format=_string(data, "format"),
            namespace=_identity(data, "namespace"),
            tenant_id=_identity(data, "tenant_id"),
            sequence_no=_positive_int(data, "sequence_no"),
            owner_session=_identity(data, "owner_session"),
            owner_epoch=_positive_int(data, "owner_epoch"),
            attempt_id=_identity(data, "attempt_id"),
            transaction_id=_identity(data, "transaction_id"),
            request_fingerprint=_string(data, "request_fingerprint"),
            record_checksum=_hash(data, "record_checksum"),
            previous_record_checksum=_optional_hash(data, "previous_record_checksum"),
            previous_descriptor_key=_optional_string(data, "previous_descriptor_key"),
            previous_descriptor_checksum=_optional_hash(data, "previous_descriptor_checksum"),
            locator=PayloadLocator(
                bucket=_string(locator_data, "bucket"),
                key=_string(locator_data, "key"),
                payload_checksum=_hash(locator_data, "payload_checksum"),
                batch_id=_optional_string(locator_data, "batch_id"),
                batch_checksum=_optional_hash(locator_data, "batch_checksum"),
                record_index=_optional_nonnegative_int(locator_data, "record_index"),
                record_count=_optional_positive_int(locator_data, "record_count"),
            ),
        )
        result.validate()
        return result

    def validate(self) -> None:
        if self.format != AUTHORITY_FORMAT:
            raise AuthorityFormatError(f"unsupported authority format: {self.format}")
        batch_fields = (
            self.locator.batch_id,
            self.locator.batch_checksum,
            self.locator.record_index,
            self.locator.record_count,
        )
        if any(value is not None for value in batch_fields) and not all(
            value is not None for value in batch_fields
        ):
            raise AuthorityFormatError("batch locator fields must be all present or all absent")
        if self.locator.record_index is not None and self.locator.record_count is not None:
            if self.locator.record_index >= self.locator.record_count:
                raise AuthorityFormatError("batch record index is outside record count")


@dataclass(frozen=True, slots=True)
class ControlState:
    format: str
    namespace: str
    tenant_id: str
    revision: int
    owner_session: str
    owner_epoch: int
    committed_sequence: int
    record_head: str | None
    descriptor_head_key: str | None
    descriptor_head_checksum: str | None
    bootstrap_key: str
    bootstrap_checksum: str
    checkpoint_sequence: int
    checkpoint_descriptor_key: str | None
    checkpoint_descriptor_checksum: str | None
    last_transition_id: str

    def encode(self) -> bytes:
        return _encode(asdict(self))

    @classmethod
    def decode(cls, raw: bytes) -> Self:
        data = _decode(raw)
        _exact_keys(data, set(cls.__dataclass_fields__))
        result = cls(
            format=_string(data, "format"),
            namespace=_identity(data, "namespace"),
            tenant_id=_identity(data, "tenant_id"),
            revision=_nonnegative_int(data, "revision"),
            owner_session=_identity(data, "owner_session"),
            owner_epoch=_positive_int(data, "owner_epoch"),
            committed_sequence=_nonnegative_int(data, "committed_sequence"),
            record_head=_optional_hash(data, "record_head"),
            descriptor_head_key=_optional_string(data, "descriptor_head_key"),
            descriptor_head_checksum=_optional_hash(data, "descriptor_head_checksum"),
            bootstrap_key=_string(data, "bootstrap_key"),
            bootstrap_checksum=_hash(data, "bootstrap_checksum"),
            checkpoint_sequence=_nonnegative_int(data, "checkpoint_sequence"),
            checkpoint_descriptor_key=_optional_string(data, "checkpoint_descriptor_key"),
            checkpoint_descriptor_checksum=_optional_hash(
                data, "checkpoint_descriptor_checksum"
            ),
            last_transition_id=_identity(data, "last_transition_id"),
        )
        result.validate()
        return result

    def validate(self) -> None:
        if self.format != AUTHORITY_FORMAT:
            raise AuthorityFormatError(f"unsupported authority format: {self.format}")
        head_fields = (self.record_head, self.descriptor_head_key, self.descriptor_head_checksum)
        if self.committed_sequence == 0 and any(value is not None for value in head_fields):
            raise AuthorityFormatError("empty control cannot have committed heads")
        if self.committed_sequence > 0 and not all(value is not None for value in head_fields):
            raise AuthorityFormatError("non-empty control requires all committed heads")
        if self.checkpoint_sequence > self.committed_sequence:
            raise AuthorityFormatError("checkpoint is ahead of committed history")

    @property
    def token(self) -> OwnerToken:
        return OwnerToken(self.namespace, self.tenant_id, self.owner_session, self.owner_epoch)


def _encode(value: dict[str, Any]) -> bytes:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    if len(raw) > MAX_MODEL_BYTES:
        raise AuthorityFormatError("authority model exceeds byte budget")
    return raw


def _decode(raw: bytes) -> dict[str, Any]:
    if len(raw) > MAX_MODEL_BYTES:
        raise AuthorityFormatError("authority model exceeds byte budget")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityFormatError("invalid authority JSON") from exc
    if not isinstance(value, dict):
        raise AuthorityFormatError("authority model must be an object")
    return value


def _exact_keys(data: dict[str, Any], expected: set[str]) -> None:
    if set(data) != expected:
        raise AuthorityFormatError("authority model has missing or unknown fields")


def _string(data: dict[str, Any], key: str) -> str:
    value = data[key]
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise AuthorityFormatError(f"{key} must be a bounded non-empty string")
    return value


def _identity(data: dict[str, Any], key: str) -> str:
    value = _string(data, key)
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
    if len(value) > 200 or any(character not in allowed for character in value):
        raise AuthorityFormatError(f"{key} contains unsafe characters")
    return value


def _hash(data: dict[str, Any], key: str) -> str:
    value = _string(data, key)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise AuthorityFormatError(f"{key} must be a lowercase SHA-256")
    return value


def _optional_string(data: dict[str, Any], key: str) -> str | None:
    return None if data[key] is None else _string(data, key)


def _optional_hash(data: dict[str, Any], key: str) -> str | None:
    return None if data[key] is None else _hash(data, key)


def _nonnegative_int(data: dict[str, Any], key: str) -> int:
    value = data[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AuthorityFormatError(f"{key} must be a non-negative integer")
    return value


def _positive_int(data: dict[str, Any], key: str) -> int:
    value = _nonnegative_int(data, key)
    if value == 0:
        raise AuthorityFormatError(f"{key} must be positive")
    return value


def _optional_nonnegative_int(data: dict[str, Any], key: str) -> int | None:
    return None if data[key] is None else _nonnegative_int(data, key)


def _optional_positive_int(data: dict[str, Any], key: str) -> int | None:
    return None if data[key] is None else _positive_int(data, key)
