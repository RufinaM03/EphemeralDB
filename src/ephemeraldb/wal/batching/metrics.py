"""In-process metrics for the WAL batcher."""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field

from .models import BatchFlushReason


@dataclass(slots=True)
class BatchLatencyStats:
    count: int = 0
    total_ns: int = 0
    min_ns: int | None = None
    max_ns: int | None = None
    recent_samples_ns: deque[int] = field(default_factory=lambda: deque(maxlen=2048))

    def observe(self, latency_ns: int) -> None:
        self.count += 1
        self.total_ns += latency_ns
        self.min_ns = latency_ns if self.min_ns is None else min(self.min_ns, latency_ns)
        self.max_ns = latency_ns if self.max_ns is None else max(self.max_ns, latency_ns)
        self.recent_samples_ns.append(latency_ns)

    def snapshot(self) -> dict[str, object]:
        return {
            "count": self.count,
            "total_ns": self.total_ns,
            "min_ns": self.min_ns,
            "max_ns": self.max_ns,
            "recent_sample_count": len(self.recent_samples_ns),
        }


class BatcherMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.wal_batches_total = 0
        self.wal_batches_failed_total = 0
        self.wal_batch_records_total = 0
        self.wal_batch_bytes_total = 0
        self.wal_batch_size_records = BatchLatencyStats()
        self.wal_batch_size_bytes = BatchLatencyStats()
        self.wal_batch_wait_latency_ns = BatchLatencyStats()
        self.wal_batch_put_latency_ns = BatchLatencyStats()
        self.queue_wait_latency_ns = BatchLatencyStats()
        self.queue_depth = 0
        self.flush_reasons: dict[BatchFlushReason, int] = {
            reason: 0 for reason in BatchFlushReason
        }

    def observe_queue_depth(self, depth: int) -> None:
        with self._lock:
            self.queue_depth = depth

    def observe_queue_wait(self, latency_ns: int) -> None:
        with self._lock:
            self.queue_wait_latency_ns.observe(latency_ns)

    def observe_commit_wait(self, latency_ns: int) -> None:
        with self._lock:
            self.wal_batch_wait_latency_ns.observe(latency_ns)

    def observe_batch_success(
        self,
        *,
        record_count: int,
        bytes_written: int,
        flush_reason: BatchFlushReason,
        put_latency_ns: int,
    ) -> None:
        with self._lock:
            self.wal_batches_total += 1
            self.wal_batch_records_total += record_count
            self.wal_batch_bytes_total += bytes_written
            self.wal_batch_size_records.observe(record_count)
            self.wal_batch_size_bytes.observe(bytes_written)
            self.wal_batch_put_latency_ns.observe(put_latency_ns)
            self.flush_reasons[flush_reason] += 1

    def observe_batch_failure(
        self,
        *,
        record_count: int,
        bytes_written: int,
        flush_reason: BatchFlushReason,
    ) -> None:
        with self._lock:
            self.wal_batches_failed_total += 1
            self.wal_batch_records_total += record_count
            self.wal_batch_bytes_total += bytes_written
            self.wal_batch_size_records.observe(record_count)
            self.wal_batch_size_bytes.observe(bytes_written)
            self.flush_reasons[flush_reason] += 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "wal_batches_total": self.wal_batches_total,
                "wal_batches_failed_total": self.wal_batches_failed_total,
                "wal_batch_records_total": self.wal_batch_records_total,
                "wal_batch_bytes_total": self.wal_batch_bytes_total,
                "wal_batch_size_records": self.wal_batch_size_records.snapshot(),
                "wal_batch_size_bytes": self.wal_batch_size_bytes.snapshot(),
                "wal_batch_wait_latency_ns": self.wal_batch_wait_latency_ns.snapshot(),
                "wal_batch_put_latency_ns": self.wal_batch_put_latency_ns.snapshot(),
                "wal_batch_flush_reason_time": self.flush_reasons[BatchFlushReason.TIME],
                "wal_batch_flush_reason_count": self.flush_reasons[BatchFlushReason.COUNT],
                "wal_batch_flush_reason_bytes": self.flush_reasons[BatchFlushReason.BYTES],
                "wal_batch_flush_reason_manual": self.flush_reasons[BatchFlushReason.MANUAL],
                "wal_batch_flush_reason_shutdown": self.flush_reasons[
                    BatchFlushReason.SHUTDOWN
                ],
                "queue_depth": self.queue_depth,
                "queue_wait_latency_ns": self.queue_wait_latency_ns.snapshot(),
            }
