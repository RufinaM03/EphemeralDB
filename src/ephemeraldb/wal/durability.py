"""Durability-store protocol and receipts for explicit WAL records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from ephemeraldb.core.errors import RemoteDurabilityError
from ephemeraldb.wal.models import WalRecord

if TYPE_CHECKING:
    from ephemeraldb.wal.batching.models import WalBatch


@dataclass(frozen=True)
class StoredWalRecord:
    record: WalRecord
    key: str
    remote_durable: bool = True


@dataclass(frozen=True, slots=True)
class PersistedWalReceipt:
    bucket: str
    key: str
    etag: str | None
    version_id: str | None
    checksum: str
    bytes_written: int
    duration_ns: int
    remote_durable: bool
    already_exists: bool = False


@dataclass(frozen=True, slots=True)
class PersistedBatchReceipt:
    bucket: str
    key: str
    etag: str | None
    version_id: str | None
    batch_id: str
    batch_checksum: str
    record_count: int
    bytes_written: int
    duration_ns: int
    remote_durable: bool
    already_exists: bool = False

    @property
    def put_latency_ns(self) -> int:
        return self.duration_ns


@dataclass(frozen=True, slots=True)
class WalCommitReceipt:
    bucket: str
    key: str
    etag: str | None
    version_id: str | None
    checksum: str
    bytes_written: int
    duration_ns: int
    remote_durable: bool
    already_exists: bool = False
    wal_batch_id: str | None = None
    wal_batch_object_key: str | None = None
    wal_batch_checksum: str | None = None
    wal_batch_record_count: int | None = None
    wal_batch_record_index: int | None = None
    wal_batch_wait_latency_ns: int | None = None
    wal_batch_put_latency_ns: int | None = None
    wal_batch_flush_reason: str | None = None
    remote_wal_put_count: int = 1
    remote_wal_bytes_written: int | None = None
    authority_epoch: int | None = None
    authority_attempt_id: str | None = None
    authority_descriptor_key: str | None = None
    authority_revision: int | None = None
    authority_decision: str | None = None


class WalDurabilityStore(Protocol):
    """Synchronous one-record WAL durability boundary."""

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        """Persist one canonical WAL record before local apply/ACK."""

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        """Fetch and validate one known WAL record."""


class ConfirmedDurabilityError(RemoteDurabilityError):
    """Post-persistence processing failed, but the attached receipt remains valid."""

    def __init__(self, receipt: WalCommitReceipt) -> None:
        super().__init__("post-persistence processing failed; reconciliation required")
        self.receipt = receipt


class WalBatchDurabilityStore(Protocol):
    """Synchronous batch WAL durability boundary."""

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        """Persist one canonical WAL batch before local apply/ACK."""

    def fetch_batch(self, key: str) -> WalBatch:
        """Fetch and validate a durable WAL batch object."""


class WalCommitter(Protocol):
    """Higher-level durability boundary used by the transaction executor."""

    def commit(self, record: WalRecord) -> WalCommitReceipt:
        """Make one WAL record remotely durable before local apply/ACK."""

    def close(self) -> None:
        """Flush/close any committer resources."""


class SingleRecordWalCommitter:
    """WalCommitter adapter for Phase 5 one-record-per-PUT stores."""

    def __init__(self, store: WalDurabilityStore) -> None:
        self.store = store

    def commit(self, record: WalRecord) -> WalCommitReceipt:
        receipt = self.store.persist_record(record)
        return WalCommitReceipt(
            bucket=receipt.bucket,
            key=receipt.key,
            etag=receipt.etag,
            version_id=receipt.version_id,
            checksum=receipt.checksum,
            bytes_written=receipt.bytes_written,
            duration_ns=receipt.duration_ns,
            remote_durable=receipt.remote_durable,
            already_exists=receipt.already_exists,
            remote_wal_put_count=int(receipt.remote_durable),
            remote_wal_bytes_written=receipt.bytes_written if receipt.remote_durable else 0,
        )

    def close(self) -> None:
        close = getattr(self.store, "close", None)
        if callable(close):
            close()
