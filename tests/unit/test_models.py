from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from ephemeraldb.core.models import (
    CheckpointMetadata,
    DatabaseOwner,
    OwnerEpoch,
    TenantId,
    TransactionId,
    WalSequence,
)


def test_tenant_id_requires_non_empty_value() -> None:
    with pytest.raises(ValueError, match="tenant id"):
        TenantId("  ")


def test_transaction_id_requires_non_empty_value() -> None:
    with pytest.raises(ValueError, match="transaction id"):
        TransactionId("")


def test_domain_identifiers_are_immutable() -> None:
    tenant_id = TenantId("tenant-a")
    transaction_id = TransactionId("transaction-a")

    with pytest.raises(FrozenInstanceError):
        tenant_id.value = "tenant-b"  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        transaction_id.value = "transaction-b"  # type: ignore[misc]


def test_wal_sequence_cannot_be_negative() -> None:
    with pytest.raises(ValueError, match="wal sequence"):
        WalSequence(-1)


def test_wal_sequence_next_is_monotonic() -> None:
    assert WalSequence(3).next() == WalSequence(4)


def test_owner_epoch_cannot_be_negative() -> None:
    with pytest.raises(ValueError, match="owner epoch"):
        OwnerEpoch(-1)


def test_owner_epoch_next_is_monotonic() -> None:
    assert OwnerEpoch(7).next() == OwnerEpoch(8)


def test_database_owner_validates_owner_id() -> None:
    with pytest.raises(ValueError, match="owner id"):
        DatabaseOwner(TenantId("tenant-a"), " ", OwnerEpoch(1))


def test_checkpoint_metadata_accepts_valid_values() -> None:
    metadata = _checkpoint_metadata()

    assert metadata.tenant_id == TenantId("tenant-a")
    assert metadata.checkpoint_sequence == WalSequence(12)
    assert metadata.schema_version == 1


def test_checkpoint_metadata_validates_checkpoint_sequence_type() -> None:
    with pytest.raises(TypeError, match="checkpoint sequence"):
        CheckpointMetadata(
            tenant_id=TenantId("tenant-a"),
            checkpoint_sequence=12,  # type: ignore[arg-type]
            checksum="sha256:abc",
            schema_version=1,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            object_key="checkpoints/tenant-a/12.db",
            size_bytes=2048,
        )


def test_checkpoint_metadata_requires_positive_schema_version() -> None:
    with pytest.raises(ValueError, match="schema version"):
        _checkpoint_metadata(schema_version=0)


def test_checkpoint_metadata_requires_timezone_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _checkpoint_metadata(created_at=datetime(2026, 1, 1))


def test_checkpoint_metadata_rejects_negative_size() -> None:
    with pytest.raises(ValueError, match="size bytes"):
        _checkpoint_metadata(size_bytes=-1)


def _checkpoint_metadata(
    *,
    schema_version: int = 1,
    created_at: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    size_bytes: int = 2048,
) -> CheckpointMetadata:
    return CheckpointMetadata(
        tenant_id=TenantId("tenant-a"),
        checkpoint_sequence=WalSequence(12),
        checksum="sha256:abc",
        schema_version=schema_version,
        created_at=created_at,
        object_key="checkpoints/tenant-a/12.db",
        size_bytes=size_bytes,
    )
