"""Small immutable domain models used across benchmark phases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TenantId:
    """Logical tenant database identifier."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _require_non_empty_text(self.value, "tenant id"))

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class TransactionId:
    """Globally unique transaction identifier."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "value",
            _require_non_empty_text(self.value, "transaction id"),
        )

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True, order=True)
class WalSequence:
    """Monotonically increasing sequence number within one tenant database."""

    value: int

    def __post_init__(self) -> None:
        _require_non_negative_int(self.value, "wal sequence")

    def next(self) -> WalSequence:
        """Return the next sequence value."""

        return WalSequence(self.value + 1)


@dataclass(frozen=True, slots=True, order=True)
class OwnerEpoch:
    """Monotonically increasing ownership generation for a tenant database."""

    value: int

    def __post_init__(self) -> None:
        _require_non_negative_int(self.value, "owner epoch")

    def next(self) -> OwnerEpoch:
        """Return the next owner epoch."""

        return OwnerEpoch(self.value + 1)


@dataclass(frozen=True, slots=True)
class DatabaseOwner:
    """Current writer ownership assignment for one tenant database."""

    tenant_id: TenantId
    owner_id: str
    epoch: OwnerEpoch

    def __post_init__(self) -> None:
        _require_instance("tenant id", self.tenant_id, TenantId)
        object.__setattr__(self, "owner_id", _require_non_empty_text(self.owner_id, "owner id"))
        _require_instance("owner epoch", self.epoch, OwnerEpoch)


@dataclass(frozen=True, slots=True)
class CheckpointMetadata:
    """Metadata for a remotely stored full-database checkpoint."""

    tenant_id: TenantId
    checkpoint_sequence: WalSequence
    checksum: str
    schema_version: int
    created_at: datetime
    object_key: str
    size_bytes: int

    def __post_init__(self) -> None:
        _require_instance("tenant id", self.tenant_id, TenantId)
        _require_instance("checkpoint sequence", self.checkpoint_sequence, WalSequence)
        object.__setattr__(self, "checksum", _require_non_empty_text(self.checksum, "checksum"))
        _require_positive_int(self.schema_version, "schema version")
        _require_datetime("created timestamp", self.created_at)
        object.__setattr__(
            self,
            "object_key",
            _require_non_empty_text(self.object_key, "object key"),
        )
        _require_non_negative_int(self.size_bytes, "size bytes")


def _require_non_empty_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        msg = f"{field_name} must be a string"
        raise TypeError(msg)
    normalized = value.strip()
    if not normalized:
        msg = f"{field_name} must not be empty"
        raise ValueError(msg)
    return normalized


def _require_non_negative_int(value: object, field_name: str) -> None:
    if type(value) is not int:
        msg = f"{field_name} must be an integer"
        raise TypeError(msg)
    if value < 0:
        msg = f"{field_name} must not be negative"
        raise ValueError(msg)


def _require_positive_int(value: object, field_name: str) -> None:
    if type(value) is not int:
        msg = f"{field_name} must be an integer"
        raise TypeError(msg)
    if value <= 0:
        msg = f"{field_name} must be positive"
        raise ValueError(msg)


def _require_instance(field_name: str, value: object, expected_type: type[object]) -> None:
    if not isinstance(value, expected_type):
        msg = f"{field_name} must be {expected_type.__name__}"
        raise TypeError(msg)


def _require_datetime(field_name: str, value: object) -> None:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
