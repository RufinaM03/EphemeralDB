"""Core domain models, protocols, and exceptions."""

from ephemeraldb.core.errors import (
    CheckpointValidationError,
    DurabilityError,
    EphemeralDBError,
    OwnershipError,
    RecoveryError,
    StaleOwnerError,
    TransactionConflictError,
    WalSequenceError,
)
from ephemeraldb.core.models import (
    CheckpointMetadata,
    DatabaseOwner,
    OwnerEpoch,
    TenantId,
    TransactionId,
    WalSequence,
)

__all__ = [
    "CheckpointMetadata",
    "CheckpointValidationError",
    "DatabaseOwner",
    "DurabilityError",
    "EphemeralDBError",
    "OwnerEpoch",
    "OwnershipError",
    "RecoveryError",
    "StaleOwnerError",
    "TenantId",
    "TransactionConflictError",
    "TransactionId",
    "WalSequence",
    "WalSequenceError",
]
