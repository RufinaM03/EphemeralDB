"""Small in-process metrics collector for local engine execution."""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field

from ephemeraldb.engine.results import ExecutionResult
from ephemeraldb.loadgen.models import is_read_operation, is_write_operation


@dataclass(slots=True)
class LatencyStats:
    count: int = 0
    total_ns: int = 0
    min_ns: int | None = None
    max_ns: int | None = None
    recent_samples_ns: deque[int] = field(default_factory=lambda: deque(maxlen=1024))

    def observe(self, latency_ns: int) -> None:
        self.count += 1
        self.total_ns += latency_ns
        self.min_ns = latency_ns if self.min_ns is None else min(self.min_ns, latency_ns)
        self.max_ns = latency_ns if self.max_ns is None else max(self.max_ns, latency_ns)
        self.recent_samples_ns.append(latency_ns)


@dataclass(slots=True)
class EngineMetrics:
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    operations_total: int = 0
    operations_success: int = 0
    operations_failed: int = 0
    reads_total: int = 0
    writes_total: int = 0
    conflicts_total: int = 0
    duplicates_total: int = 0
    rows_read_total: int = 0
    rows_written_total: int = 0
    wal_records_created: int = 0
    wal_bytes_created: int = 0
    wal_validation_failures: int = 0
    wal_replay_records: int = 0
    wal_replay_duplicates: int = 0
    wal_replay_failures: int = 0
    remote_wal_puts_total: int = 0
    remote_wal_put_failures_total: int = 0
    remote_wal_bytes_total: int = 0
    remote_wal_conflicts_total: int = 0
    wal_batches_total: int = 0
    wal_batches_failed_total: int = 0
    wal_batch_records_total: int = 0
    wal_batch_bytes_total: int = 0
    wal_batch_flush_reason_time: int = 0
    wal_batch_flush_reason_count: int = 0
    wal_batch_flush_reason_bytes: int = 0
    wal_batch_flush_reason_manual: int = 0
    queue_depth: int = 0
    local_apply_after_remote_failure_total: int = 0
    reconciliation_required_total: int = 0
    remote_wal_put_latency: LatencyStats = field(default_factory=LatencyStats)
    wal_batch_size_records: LatencyStats = field(default_factory=LatencyStats)
    wal_batch_size_bytes: LatencyStats = field(default_factory=LatencyStats)
    wal_batch_wait_latency: LatencyStats = field(default_factory=LatencyStats)
    wal_batch_put_latency: LatencyStats = field(default_factory=LatencyStats)
    queue_wait_latency: LatencyStats = field(default_factory=LatencyStats)
    latency: LatencyStats = field(default_factory=LatencyStats)

    def observe(self, result: ExecutionResult) -> None:
        with self._lock:
            self._observe_locked(result)

    def _observe_locked(self, result: ExecutionResult) -> None:
        self.operations_total += 1
        self.operations_success += int(result.success)
        self.operations_failed += int(not result.success)
        self.reads_total += int(is_read_operation(result.operation_type))
        self.writes_total += int(is_write_operation(result.operation_type))
        self.conflicts_total += int(result.conflict)
        self.duplicates_total += int(result.duplicate_transaction)
        self.rows_read_total += result.rows_read
        self.rows_written_total += result.rows_written
        self.wal_records_created += int(result.wal_bytes > 0 and not result.duplicate_transaction)
        self.wal_bytes_created += result.wal_bytes
        self.remote_wal_puts_total += result.remote_wal_put_count
        self.remote_wal_put_failures_total += int(
            result.error_code is not None and result.error_code.value == "REMOTE_DURABILITY_FAILED"
        )
        self.remote_wal_bytes_total += result.remote_wal_bytes_written
        if result.wal_batch_id is not None and result.wal_batch_record_index == 0:
            self.wal_batches_total += 1
            record_count = result.wal_batch_record_count or 0
            self.wal_batch_records_total += record_count
            self.wal_batch_bytes_total += result.remote_wal_bytes_written
            self.wal_batch_size_records.observe(record_count)
            self.wal_batch_size_bytes.observe(result.remote_wal_bytes_written)
            if result.wal_batch_flush_reason == "time":
                self.wal_batch_flush_reason_time += 1
            elif result.wal_batch_flush_reason == "count":
                self.wal_batch_flush_reason_count += 1
            elif result.wal_batch_flush_reason == "bytes":
                self.wal_batch_flush_reason_bytes += 1
            elif result.wal_batch_flush_reason in {"manual", "shutdown"}:
                self.wal_batch_flush_reason_manual += 1
        self.local_apply_after_remote_failure_total += int(
            result.error_code is not None
            and result.error_code.value == "LOCAL_APPLY_AFTER_DURABILITY_FAILED"
        )
        self.reconciliation_required_total += int(result.reconciliation_required)
        if result.s3_put_latency_ns is not None and (
            result.wal_batch_id is None or result.wal_batch_record_index == 0
        ):
            self.remote_wal_put_latency.observe(result.s3_put_latency_ns)
        if result.wal_batch_wait_latency_ns is not None:
            self.wal_batch_wait_latency.observe(result.wal_batch_wait_latency_ns)
        if result.wal_batch_put_latency_ns is not None and result.wal_batch_record_index == 0:
            self.wal_batch_put_latency.observe(result.wal_batch_put_latency_ns)
        self.latency.observe(result.latency_ns)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return self._snapshot_locked()

    def _snapshot_locked(self) -> dict[str, object]:
        return {
            "operations_total": self.operations_total,
            "operations_success": self.operations_success,
            "operations_failed": self.operations_failed,
            "reads_total": self.reads_total,
            "writes_total": self.writes_total,
            "conflicts_total": self.conflicts_total,
            "duplicates_total": self.duplicates_total,
            "rows_read_total": self.rows_read_total,
            "rows_written_total": self.rows_written_total,
            "wal_records_created": self.wal_records_created,
            "wal_bytes_created": self.wal_bytes_created,
            "wal_validation_failures": self.wal_validation_failures,
            "wal_replay_records": self.wal_replay_records,
            "wal_replay_duplicates": self.wal_replay_duplicates,
            "wal_replay_failures": self.wal_replay_failures,
            "remote_wal_puts_total": self.remote_wal_puts_total,
            "remote_wal_put_failures_total": self.remote_wal_put_failures_total,
            "remote_wal_bytes_total": self.remote_wal_bytes_total,
            "remote_wal_conflicts_total": self.remote_wal_conflicts_total,
            "wal_batches_total": self.wal_batches_total,
            "wal_batches_failed_total": self.wal_batches_failed_total,
            "wal_batch_records_total": self.wal_batch_records_total,
            "wal_batch_bytes_total": self.wal_batch_bytes_total,
            "wal_batch_flush_reason_time": self.wal_batch_flush_reason_time,
            "wal_batch_flush_reason_count": self.wal_batch_flush_reason_count,
            "wal_batch_flush_reason_bytes": self.wal_batch_flush_reason_bytes,
            "wal_batch_flush_reason_manual": self.wal_batch_flush_reason_manual,
            "queue_depth": self.queue_depth,
            "local_apply_after_remote_failure_total": (self.local_apply_after_remote_failure_total),
            "reconciliation_required_total": self.reconciliation_required_total,
            "remote_wal_put_latency_ns": {
                "count": self.remote_wal_put_latency.count,
                "total_ns": self.remote_wal_put_latency.total_ns,
                "min_ns": self.remote_wal_put_latency.min_ns,
                "max_ns": self.remote_wal_put_latency.max_ns,
                "recent_sample_count": len(self.remote_wal_put_latency.recent_samples_ns),
            },
            "wal_batch_size_records": {
                "count": self.wal_batch_size_records.count,
                "total_ns": self.wal_batch_size_records.total_ns,
                "min_ns": self.wal_batch_size_records.min_ns,
                "max_ns": self.wal_batch_size_records.max_ns,
                "recent_sample_count": len(self.wal_batch_size_records.recent_samples_ns),
            },
            "wal_batch_size_bytes": {
                "count": self.wal_batch_size_bytes.count,
                "total_ns": self.wal_batch_size_bytes.total_ns,
                "min_ns": self.wal_batch_size_bytes.min_ns,
                "max_ns": self.wal_batch_size_bytes.max_ns,
                "recent_sample_count": len(self.wal_batch_size_bytes.recent_samples_ns),
            },
            "wal_batch_wait_latency_ns": {
                "count": self.wal_batch_wait_latency.count,
                "total_ns": self.wal_batch_wait_latency.total_ns,
                "min_ns": self.wal_batch_wait_latency.min_ns,
                "max_ns": self.wal_batch_wait_latency.max_ns,
                "recent_sample_count": len(self.wal_batch_wait_latency.recent_samples_ns),
            },
            "wal_batch_put_latency_ns": {
                "count": self.wal_batch_put_latency.count,
                "total_ns": self.wal_batch_put_latency.total_ns,
                "min_ns": self.wal_batch_put_latency.min_ns,
                "max_ns": self.wal_batch_put_latency.max_ns,
                "recent_sample_count": len(self.wal_batch_put_latency.recent_samples_ns),
            },
            "queue_wait_latency_ns": {
                "count": self.queue_wait_latency.count,
                "total_ns": self.queue_wait_latency.total_ns,
                "min_ns": self.queue_wait_latency.min_ns,
                "max_ns": self.queue_wait_latency.max_ns,
                "recent_sample_count": len(self.queue_wait_latency.recent_samples_ns),
            },
            "latency": {
                "count": self.latency.count,
                "total_ns": self.latency.total_ns,
                "min_ns": self.latency.min_ns,
                "max_ns": self.latency.max_ns,
                "recent_sample_count": len(self.latency.recent_samples_ns),
            },
        }
