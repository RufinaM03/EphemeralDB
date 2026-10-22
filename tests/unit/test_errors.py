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


def test_custom_exception_inheritance() -> None:
    assert issubclass(DurabilityError, EphemeralDBError)
    assert issubclass(WalSequenceError, EphemeralDBError)
    assert issubclass(CheckpointValidationError, EphemeralDBError)
    assert issubclass(OwnershipError, EphemeralDBError)
    assert issubclass(StaleOwnerError, OwnershipError)
    assert issubclass(RecoveryError, EphemeralDBError)
    assert issubclass(TransactionConflictError, EphemeralDBError)
