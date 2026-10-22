"""Typed models for cross-tenant WAL batches."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from ephemeraldb.wal.models import WalRecord

WAL_BATCH_FORMAT_VERSION = 1


class BatchFlushReason(StrEnum):
    TIME = "time"
    COUNT = "count"
    BYTES = "bytes"
    MANUAL = "manual"
    SHUTDOWN = "shutdown"


class BatchState(StrEnum):
    OPEN = "OPEN"
    SEALED = "SEALED"
    UPLOADING = "UPLOADING"
    DURABLE = "DURABLE"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class WalBatchRecordIndex:
    tenant_id: str
    sequence_no: int
    transaction_id: str
    record_position: int
    record_checksum: str


@dataclass(frozen=True, slots=True, kw_only=True)
class WalBatch:
    batch_format_version: int = WAL_BATCH_FORMAT_VERSION
    batch_id: str
    created_at: str
    records: tuple[WalRecord, ...]
    record_index: tuple[WalBatchRecordIndex, ...]
    record_count: int
    total_payload_bytes: int
    batch_checksum: str | None = None


@dataclass(frozen=True, slots=True)
class WalBatchingConfig:
    batch_window_ms: int = 5
    max_records_per_batch: int = 100
    max_batch_bytes: int = 1024 * 1024
    max_queue_records: int = 10000
    flush_on_shutdown: bool = True
    wait_timeout_seconds: float = 30.0
    shutdown_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.wait_timeout_seconds <= 0 or self.shutdown_timeout_seconds <= 0:
            raise ValueError("batch wait and shutdown timeouts must be positive")
        if self.batch_window_ms < 0:
            msg = "batch_window_ms must be >= 0"
            raise ValueError(msg)
        if self.max_records_per_batch < 1:
            msg = "max_records_per_batch must be >= 1"
            raise ValueError(msg)
        if self.max_batch_bytes < 1:
            msg = "max_batch_bytes must be >= 1"
            raise ValueError(msg)
        if self.max_queue_records < 1:
            msg = "max_queue_records must be >= 1"
            raise ValueError(msg)


def new_batch_id() -> str:
    return str(uuid.uuid4())


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
