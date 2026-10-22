"""Persistent multiprocess CAS adapter used by offline proofs."""

from __future__ import annotations

import hashlib
import sqlite3
import threading
from contextlib import closing
from pathlib import Path

from ephemeraldb.core.errors import OwnershipError

from .store import CasResult, ControlSnapshot

MAX_AUTHORITY_OBJECT_BYTES = 8 * 1024 * 1024


class SQLiteAuthorityStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._counter_lock = threading.Lock()
        self._counters: dict[str, int] = {}
        if self.path.is_file():
            return
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS controls (
                    object_key TEXT PRIMARY KEY,
                    payload BLOB NOT NULL,
                    generation INTEGER NOT NULL CHECK(generation > 0)
                );
                CREATE TABLE IF NOT EXISTS immutable_objects (
                    object_key TEXT PRIMARY KEY,
                    payload BLOB NOT NULL,
                    sha256 TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS request_counts (
                    operation TEXT PRIMARY KEY,
                    count INTEGER NOT NULL CHECK(count >= 0)
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=30, isolation_level=None)

    def _count(self, operation: str) -> None:
        with self._counter_lock:
            try:
                with closing(self._connect()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "INSERT INTO request_counts VALUES(?,1) "
                        "ON CONFLICT(operation) DO UPDATE SET count=count+1",
                        (operation,),
                    )
                    connection.commit()
            except sqlite3.Error:
                self._counters[operation] = self._counters.get(operation, 0) + 1

    def counters(self) -> dict[str, int]:
        with self._counter_lock:
            try:
                with closing(self._connect()) as connection:
                    rows = connection.execute(
                        "SELECT operation,count FROM request_counts"
                    ).fetchall()
                return {str(operation): int(count) for operation, count in rows}
            except sqlite3.Error:
                return dict(self._counters)

    def create_control(self, key: str, payload: bytes) -> CasResult:
        _check(key, payload)
        self._count("CONTROL_CREATE")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "INSERT INTO controls(object_key,payload,generation) VALUES(?,?,1)",
                    (key, payload),
                )
            except sqlite3.IntegrityError:
                connection.rollback()
                return CasResult.CONFLICT
            connection.commit()
        return CasResult.APPLIED

    def get_control(self, key: str) -> ControlSnapshot:
        self._count("CONTROL_GET")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT payload,generation FROM controls WHERE object_key=?", (key,)
            ).fetchone()
        if row is None:
            raise OwnershipError(f"authority control does not exist: {key}")
        return ControlSnapshot(bytes(row[0]), str(row[1]))

    def compare_and_swap(
        self, key: str, expected_validator: str, payload: bytes
    ) -> CasResult:
        _check(key, payload)
        try:
            expected = int(expected_validator)
        except ValueError as exc:
            raise OwnershipError("invalid SQLite authority validator") from exc
        self._count("CONTROL_CAS")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "UPDATE controls SET payload=?,generation=generation+1 "
                "WHERE object_key=? AND generation=?",
                (payload, key, expected),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                return CasResult.CONFLICT
            connection.commit()
        return CasResult.APPLIED

    def put_immutable(self, key: str, payload: bytes) -> None:
        _check(key, payload)
        digest = hashlib.sha256(payload).hexdigest()
        self._count("IMMUTABLE_PUT")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT sha256,payload FROM immutable_objects WHERE object_key=?", (key,)
            ).fetchone()
            if row is not None:
                connection.rollback()
                if row[0] != digest or bytes(row[1]) != payload:
                    raise OwnershipError(f"immutable authority object conflict: {key}")
                return
            connection.execute(
                "INSERT INTO immutable_objects VALUES(?,?,?)", (key, payload, digest)
            )
            connection.commit()

    def get_immutable(self, key: str) -> bytes:
        self._count("IMMUTABLE_GET")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT payload,sha256 FROM immutable_objects WHERE object_key=?", (key,)
            ).fetchone()
        if row is None:
            raise OwnershipError(f"required immutable object does not exist: {key}")
        payload = bytes(row[0])
        if hashlib.sha256(payload).hexdigest() != row[1]:
            raise OwnershipError(f"immutable object checksum mismatch: {key}")
        return payload


def _check(key: str, payload: bytes) -> None:
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise OwnershipError("unsafe authority object key")
    if not payload or len(payload) > MAX_AUTHORITY_OBJECT_BYTES:
        raise OwnershipError("authority object violates byte budget")
