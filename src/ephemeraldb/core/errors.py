"""Project exception hierarchy."""


class EphemeralDBError(Exception):
    """Base class for all project-specific exceptions."""


class DurabilityError(EphemeralDBError):
    """Raised when remote durable persistence cannot be confirmed."""


class RemoteDurabilityError(DurabilityError):
    """Raised when a remote durability store cannot confirm persistence."""


class S3ExpressConfigurationError(RemoteDurabilityError):
    """Raised when S3 Express WAL configuration is incomplete or invalid."""


class S3ExpressWriteError(RemoteDurabilityError):
    """Raised when S3 Express WAL persistence fails."""


class S3ExpressReadError(RemoteDurabilityError):
    """Raised when an S3 Express WAL object cannot be read."""


class RemoteWalConflictError(RemoteDurabilityError):
    """Raised when a remote WAL object key already contains different content."""


class PersistenceRejectedError(RemoteDurabilityError):
    """No persistence was attempted; rejection is definitive for this request."""


class WalBatchingClosedError(PersistenceRejectedError):
    """Raised when a WAL commit is submitted after the batcher has closed."""


class LocalApplyAfterDurabilityError(DurabilityError):
    """Raised when local SQLite apply fails after remote WAL persistence."""


class InjectedWalFaultError(EphemeralDBError):
    """Raised by test hooks at explicit WAL commit fault points."""


class WalError(EphemeralDBError):
    """Base class for explicit logical WAL errors."""


class WalEncodingError(WalError):
    """Raised when a WAL record or WAL file cannot be decoded."""


class WalChecksumError(WalError):
    """Raised when a WAL record checksum is missing or invalid."""


class WalChainError(WalError):
    """Raised when WAL hash-chain validation fails."""


class WalSequenceError(WalError):
    """Raised when WAL sequence validation or ordering fails."""


class WalReplayError(WalError):
    """Raised when WAL replay cannot apply a valid record atomically."""


class UnsupportedWalVersionError(WalError):
    """Raised when WAL replay sees an unsupported format version."""


class CheckpointValidationError(EphemeralDBError):
    """Raised when checkpoint metadata or checkpoint contents are invalid."""


class OwnershipError(EphemeralDBError):
    """Raised when tenant database ownership cannot be established or verified."""


class StaleOwnerError(OwnershipError):
    """Raised when a write is attempted by a stale owner epoch."""


class RecoveryError(EphemeralDBError):
    """Raised when recovery cannot reconstruct a valid committed state."""


class TransactionConflictError(EphemeralDBError):
    """Raised when a benchmark transaction conflicts with concurrent work."""
