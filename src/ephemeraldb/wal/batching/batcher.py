"""One upload worker with bounded admission and terminal pending handles."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ephemeraldb.core.errors import (
    PersistenceRejectedError,
    RemoteDurabilityError,
    WalBatchingClosedError,
)
from ephemeraldb.wal.codec import encode_record
from ephemeraldb.wal.durability import (
    ConfirmedDurabilityError,
    PersistedBatchReceipt,
    WalBatchDurabilityStore,
    WalCommitReceipt,
)
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.validation import validate_record

from .codec import build_batch
from .metrics import BatcherMetrics
from .models import BatchFlushReason, WalBatchingConfig
from .queue import PendingWalCommit


@dataclass(frozen=True)
class _BatchEntry:
    record: WalRecord
    payload_bytes: int
    pending: PendingWalCommit


class BatchedWalCommitter:
    def __init__(
        self,
        store: WalBatchDurabilityStore,
        *,
        config: WalBatchingConfig | None = None,
        metrics: BatcherMetrics | None = None,
    ) -> None:
        self.store = store
        self.config = config or WalBatchingConfig()
        self.metrics = metrics or BatcherMetrics()
        self._condition = threading.Condition()
        self._queue: deque[_BatchEntry] = deque()
        self._outstanding: dict[PendingWalCommit, _BatchEntry] = {}
        self._accepting = True
        self._stopping = False
        self._closed = False
        self._manual_flush_requested = False
        self._worker_error: BaseException | None = None
        self._inflight: tuple[_BatchEntry, ...] = ()
        self._upload_started = False
        self._worker = threading.Thread(
            target=self._run_worker, name="ephemeraldb-wal-batcher", daemon=True
        )
        self._worker.start()

    @property
    def _pending_records(self) -> int:
        return len(self._outstanding)

    def commit(self, record: WalRecord) -> WalCommitReceipt:
        validate_record(record, expected_tenant_id=record.tenant_id)
        size = len(encode_record(record))
        if size > self.config.max_batch_bytes:
            raise PersistenceRejectedError("encoded record exceeds batch payload byte limit")
        pending = PendingWalCommit(enqueued_at_ns=time.perf_counter_ns())
        entry = _BatchEntry(record, size, pending)
        deadline = time.monotonic() + self.config.wait_timeout_seconds
        with self._condition:
            while self._pending_records >= self.config.max_queue_records and self._accepting:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PersistenceRejectedError(
                        "batch admission deadline exceeded; not submitted"
                    )
                self._condition.wait(remaining)
            if not self._accepting:
                raise WalBatchingClosedError("WAL batcher closed or worker failed")
            self._outstanding[pending] = entry
            self._queue.append(entry)
            self._observe(self.metrics.observe_queue_depth, self._pending_records)
            self._condition.notify_all()
        return pending.wait(max(0.001, deadline - time.monotonic()))

    def flush(self) -> None:
        deadline = time.monotonic() + self.config.wait_timeout_seconds
        with self._condition:
            targets = set(self._outstanding)
            self._manual_flush_requested = True
            self._condition.notify_all()
            while targets.intersection(self._outstanding) and self._worker_error is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RemoteDurabilityError("flush deadline exceeded; pending outcomes unknown")
                self._condition.wait(remaining)
            if self._worker_error is not None:
                raise RemoteDurabilityError(str(self._worker_error))

    def close(self) -> None:
        if threading.current_thread() is self._worker:
            raise RemoteDurabilityError("worker cannot join itself")
        with self._condition:
            self._accepting = False
            self._stopping = True
            self._condition.notify_all()
        self._worker.join(timeout=self.config.shutdown_timeout_seconds)
        if self._worker.is_alive():
            raise RemoteDurabilityError("shutdown deadline exceeded; upload may still complete")
        with self._condition:
            if not self._closed:
                self._closed = True
                close = getattr(self.store, "close", None)
                if callable(close):
                    close()
            if self._worker_error is not None:
                raise RemoteDurabilityError(str(self._worker_error))

    def snapshot_metrics(self) -> dict[str, object]:
        return self.metrics.snapshot()

    @staticmethod
    def _observe(method: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        try:
            method(*args, **kwargs)
        except Exception:
            pass  # Diagnostic failure cannot change a persistence outcome.

    def _finish(self, entries: tuple[_BatchEntry, ...], error: Exception | None = None) -> None:
        with self._condition:
            for entry in entries:
                if error is not None:
                    entry.pending.fail(error)
                self._outstanding.pop(entry.pending, None)
            self._observe(self.metrics.observe_queue_depth, self._pending_records)
            self._condition.notify_all()

    def _run_worker(self) -> None:
        try:
            while True:
                with self._condition:
                    while not self._queue and not self._stopping:
                        self._condition.wait()
                    if self._stopping and not self._queue:
                        return
                    if self._stopping and not self.config.flush_on_shutdown:
                        entries = tuple(self._queue)
                        self._queue.clear()
                        self._finish(
                            entries, WalBatchingClosedError("shutdown discarded queued work")
                        )
                        return
                    entries, reason = self._collect_locked()
                    self._inflight = entries
                    self._upload_started = False
                self._persist_entries(entries, reason)
                self._inflight = ()
        except BaseException as exc:
            with self._condition:
                self._accepting = False
                self._stopping = True
                self._worker_error = exc
                entries = tuple(self._outstanding.values())
                self._queue.clear()
                for entry in entries:
                    error: Exception = PersistenceRejectedError("worker failed before submission")
                    if self._upload_started and entry in self._inflight:
                        error = RemoteDurabilityError(f"worker failed; outcome unknown: {exc}")
                    self._finish((entry,), error)

    def _collect_locked(self) -> tuple[tuple[_BatchEntry, ...], BatchFlushReason]:
        entries = [self._queue.popleft()]
        size = entries[0].payload_bytes
        deadline = time.monotonic() + self.config.batch_window_ms / 1000
        while True:
            if len(entries) >= self.config.max_records_per_batch:
                return tuple(entries), BatchFlushReason.COUNT
            if size >= self.config.max_batch_bytes:
                return tuple(entries), BatchFlushReason.BYTES
            if self._queue:
                entry = self._queue[0]
                if size + entry.payload_bytes > self.config.max_batch_bytes:
                    return tuple(entries), BatchFlushReason.BYTES
                entries.append(self._queue.popleft())
                size += entry.payload_bytes
                continue
            if self._manual_flush_requested:
                self._manual_flush_requested = False
                return tuple(entries), BatchFlushReason.MANUAL
            if self._stopping:
                if not self.config.flush_on_shutdown:
                    self._finish(tuple(entries), WalBatchingClosedError("shutdown before upload"))
                    return (), BatchFlushReason.SHUTDOWN
                return tuple(entries), BatchFlushReason.SHUTDOWN
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return tuple(entries), BatchFlushReason.TIME
            self._condition.wait(remaining)

    def _persist_entries(self, entries: tuple[_BatchEntry, ...], reason: BatchFlushReason) -> None:
        if not entries:
            return
        batch = build_batch(entry.record for entry in entries)
        try:
            self._upload_started = True
            receipt = self.store.persist_batch(batch)
        except Exception as exc:
            error = (
                exc if isinstance(exc, RemoteDurabilityError) else RemoteDurabilityError(str(exc))
            )
            self._observe(
                self.metrics.observe_batch_failure,
                record_count=len(entries),
                bytes_written=batch.total_payload_bytes,
                flush_reason=reason,
            )
            if reason == BatchFlushReason.SHUTDOWN:
                self._worker_error = error
            self._finish(entries, error)
            return
        finished = time.perf_counter_ns()
        post_error: Exception | None = None
        for position, entry in enumerate(entries):
            wait = finished - entry.pending.enqueued_at_ns
            try:
                result = _receipt_for_record(
                    entry=entry,
                    receipt=receipt,
                    record_position=position,
                    wait_latency_ns=wait,
                    flush_reason=reason,
                )
                entry.pending.resolve(result)
            except Exception as exc:
                # Retain positive durability evidence even if receipt processing fails.
                fallback = WalCommitReceipt(
                    bucket=receipt.bucket,
                    key=receipt.key,
                    etag=receipt.etag,
                    version_id=receipt.version_id,
                    checksum=entry.record.record_checksum or "",
                    bytes_written=entry.payload_bytes,
                    duration_ns=receipt.duration_ns,
                    remote_durable=receipt.remote_durable,
                    wal_batch_id=receipt.batch_id,
                    wal_batch_object_key=receipt.key,
                    wal_batch_record_index=position,
                    wal_batch_record_count=receipt.record_count,
                    remote_wal_put_count=int(position == 0 and receipt.remote_durable),
                    remote_wal_bytes_written=receipt.bytes_written if position == 0 else 0,
                )
                entry.pending.fail(ConfirmedDurabilityError(fallback))
                post_error = exc
            self._observe(self.metrics.observe_commit_wait, wait)
        self._finish(entries)
        self._observe(
            self.metrics.observe_batch_success,
            record_count=receipt.record_count,
            bytes_written=receipt.bytes_written,
            flush_reason=reason,
            put_latency_ns=receipt.duration_ns,
        )
        if post_error is not None:
            raise post_error


def _receipt_for_record(
    *,
    entry: _BatchEntry,
    receipt: PersistedBatchReceipt,
    record_position: int,
    wait_latency_ns: int,
    flush_reason: BatchFlushReason,
) -> WalCommitReceipt:
    assert entry.record.record_checksum is not None
    first = int(record_position == 0)
    return WalCommitReceipt(
        bucket=receipt.bucket,
        key=receipt.key,
        etag=receipt.etag,
        version_id=receipt.version_id,
        checksum=entry.record.record_checksum,
        bytes_written=entry.payload_bytes,
        duration_ns=receipt.duration_ns,
        remote_durable=receipt.remote_durable,
        already_exists=receipt.already_exists,
        wal_batch_id=receipt.batch_id,
        wal_batch_object_key=receipt.key,
        wal_batch_checksum=receipt.batch_checksum,
        wal_batch_record_count=receipt.record_count,
        wal_batch_record_index=record_position,
        wal_batch_wait_latency_ns=wait_latency_ns,
        wal_batch_put_latency_ns=receipt.duration_ns,
        wal_batch_flush_reason=flush_reason.value,
        remote_wal_put_count=first,
        remote_wal_bytes_written=receipt.bytes_written if first else 0,
    )
