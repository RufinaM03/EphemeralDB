"""SQLite connection management for local tenant databases."""

from __future__ import annotations

import sqlite3
import threading
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from ephemeraldb.core.models import TenantId
from ephemeraldb.engine.registry import TenantDatabaseRegistry


@dataclass(frozen=True, slots=True)
class EngineConfig:
    """Local SQLite engine configuration.

    SQLite's internal WAL mode here is a local database implementation detail.
    It is not the explicit EphemeralDB logical WAL.
    """

    database_root: Path
    max_open_connections: int = 32
    busy_timeout_ms: int = 5000
    enable_foreign_keys: bool = True
    journal_mode: str = "WAL"
    synchronous_mode: str = "FULL"

    def __post_init__(self) -> None:
        if self.max_open_connections < 1:
            msg = "max_open_connections must be >= 1"
            raise ValueError(msg)
        if self.busy_timeout_ms < 0:
            msg = "busy_timeout_ms must be >= 0"
            raise ValueError(msg)
        object.__setattr__(self, "database_root", Path(self.database_root).resolve())
        object.__setattr__(self, "journal_mode", self.journal_mode.upper())
        object.__setattr__(self, "synchronous_mode", self.synchronous_mode.upper())


class ConnectionManager:
    """Thread-aware LRU cache; max_open_connections is an idle eviction target.

    Active connections are never evicted. Bound concurrent callers separately.
    """

    def __init__(self, config: EngineConfig, registry: TenantDatabaseRegistry) -> None:
        self.config = config
        self.registry = registry
        self._lock = threading.RLock()
        self._connections: OrderedDict[tuple[int, str], sqlite3.Connection] = OrderedDict()
        self._active: set[tuple[int, str]] = set()

    @contextmanager
    def connection_for(self, tenant_id: TenantId) -> Iterator[sqlite3.Connection]:
        key = (threading.get_ident(), tenant_id.value)
        with self._lock:
            if key in self._connections:
                connection = self._connections[key]
                self._connections.move_to_end(key)
            else:
                connection = self._open_connection(self.registry.path_for(tenant_id))
                self._connections[key] = connection
            self._active.add(key)
            self._evict_if_needed()
        try:
            yield connection
        finally:
            with self._lock:
                self._active.discard(key)
                if key in self._connections:
                    self._connections.move_to_end(key)
                self._evict_if_needed()

    @property
    def open_connection_count(self) -> int:
        with self._lock:
            return len(self._connections)

    def close(self, tenant_id: TenantId) -> None:
        with self._lock:
            keys = [
                key
                for key in self._connections
                if key[1] == tenant_id.value and key not in self._active
            ]
            for key in keys:
                self._connections.pop(key).close()

    def close_all(self) -> None:
        with self._lock:
            for key in list(self._connections):
                if key in self._active:
                    continue
                self._connections.pop(key).close()

    def clear(self) -> None:
        self.close_all()

    def _open_connection(self, database_path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(
            database_path,
            timeout=self.config.busy_timeout_ms / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        if self.config.enable_foreign_keys:
            connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.config.busy_timeout_ms}")
        connection.execute(f"PRAGMA journal_mode = {self.config.journal_mode}")
        connection.execute(f"PRAGMA synchronous = {self.config.synchronous_mode}")
        return connection

    def _evict_if_needed(self) -> None:
        while len(self._connections) > self.config.max_open_connections:
            for key in list(self._connections):
                if key in self._active:
                    continue
                self._connections.pop(key).close()
                break
            else:
                return
