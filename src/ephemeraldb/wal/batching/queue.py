"""Thread-safe pending result handles for WAL batching."""

from __future__ import annotations

import threading

from ephemeraldb.core.errors import RemoteDurabilityError
from ephemeraldb.wal.durability import WalCommitReceipt


class PendingWalCommit:
    def __init__(self, *, enqueued_at_ns: int) -> None:
        self.enqueued_at_ns = enqueued_at_ns
        self._event = threading.Event()
        self._receipt: WalCommitReceipt | None = None
        self._error: Exception | None = None
        self._lock = threading.Lock()

    def resolve(self, receipt: WalCommitReceipt) -> None:
        with self._lock:
            if not self._event.is_set():
                self._receipt = receipt
                self._event.set()

    def fail(self, error: Exception) -> None:
        with self._lock:
            if not self._event.is_set():
                self._error = error
                self._event.set()

    def wait(self, timeout: float = 30.0) -> WalCommitReceipt:
        if not self._event.wait(timeout):
            raise RemoteDurabilityError("durability wait expired; outcome unknown, not cancelled")
        if self._error is not None:
            raise self._error
        assert self._receipt is not None
        return self._receipt
