"""Architectural interfaces for Phase 0 boundaries.

These protocols define what future implementations must provide. They do not
include S3, Aurora, Kubernetes, or database engine behavior.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Protocol

from ephemeraldb.core.models import (
    CheckpointMetadata,
    DatabaseOwner,
    TenantId,
    TransactionId,
    WalSequence,
)
from ephemeraldb.wal.batching.models import WalBatch
from ephemeraldb.wal.durability import PersistedBatchReceipt, PersistedWalReceipt
from ephemeraldb.wal.models import WalRecord as LogicalWalRecord

type WalRecord = Mapping[str, object]
type TransactionPayload = Mapping[str, object]
type BenchmarkEntity = Mapping[str, object]
type MetricTags = Mapping[str, str]


class WalDurabilityStore(Protocol):
    """Boundary for remotely durable tenant WAL persistence."""

    def persist_record(self, record: LogicalWalRecord) -> PersistedWalReceipt:
        """Persist one WAL record and return a durability receipt."""

    def fetch_record(self, tenant_id: str, sequence_no: int) -> LogicalWalRecord:
        """Fetch a known durable WAL record."""

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        """Persist one cross-tenant WAL batch and return a durability receipt."""

    def records_after(self, tenant_id: TenantId, sequence: WalSequence) -> Iterable[WalRecord]:
        """Retrieve durable WAL records with sequence numbers greater than sequence."""


class CheckpointStore(Protocol):
    """Boundary for full-database checkpoint storage."""

    def store_checkpoint(self, metadata: CheckpointMetadata, payload: bytes) -> None:
        """Persist a checkpoint object and its metadata."""

    def latest_checkpoint(self, tenant_id: TenantId) -> CheckpointMetadata | None:
        """Return the latest valid checkpoint metadata for a tenant database."""

    def fetch_checkpoint(self, metadata: CheckpointMetadata) -> bytes:
        """Fetch checkpoint bytes for previously returned metadata."""


class OwnershipRegistry(Protocol):
    """Boundary for single-writer ownership coordination."""

    def current_owner(self, tenant_id: TenantId) -> DatabaseOwner | None:
        """Return the current owner for a tenant database, if one exists."""

    def acquire_or_transfer(self, tenant_id: TenantId, owner_id: str) -> DatabaseOwner:
        """Acquire ownership or transfer it to a new owner epoch."""

    def assert_current_owner(self, owner: DatabaseOwner) -> None:
        """Reject stale owner epochs or unknown owners."""


class DatabaseEngine(Protocol):
    """Boundary for tenant database execution and deterministic replay."""

    def execute_transaction(
        self,
        tenant_id: TenantId,
        transaction_id: TransactionId,
        payload: TransactionPayload,
    ) -> WalSequence:
        """Execute a benchmark transaction and return its WAL sequence."""

    def read_entity(self, tenant_id: TenantId, entity_id: str) -> BenchmarkEntity | None:
        """Read a benchmark entity from a tenant database."""

    def apply_replayed_record(self, tenant_id: TenantId, record: WalRecord) -> None:
        """Apply a WAL record during recovery or replica catch-up."""

    def state_checksum(self, tenant_id: TenantId) -> str:
        """Calculate a deterministic checksum of committed logical state."""


class MetricsSink(Protocol):
    """Boundary for benchmark metrics emission."""

    def record_latency(self, name: str, value_ms: float, tags: MetricTags | None = None) -> None:
        """Record a latency measurement in milliseconds."""

    def increment_counter(self, name: str, value: int = 1, tags: MetricTags | None = None) -> None:
        """Increment a named counter."""

    def record_gauge(self, name: str, value: float, tags: MetricTags | None = None) -> None:
        """Record a point-in-time gauge value."""
