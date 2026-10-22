"""File-streaming immutable object stores; filesystem implementation is offline only."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Protocol

from ephemeraldb.checkpoint.models import CheckpointError
from ephemeraldb.wal.writer import _fsync_directory

MAX_OBJECT_BYTES = 64 * 1024 * 1024
MAX_SCAN_BYTES = 128 * 1024 * 1024
MAX_OBJECTS = 1000
MAX_RECORDS = 10000


def file_hash(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def safe_key(key: str) -> str:
    parts = key.split("/")
    if not key or any(p in {"", ".", ".."} for p in parts) or "\\" in key or ":" in key:
        raise CheckpointError("unsafe object key")
    return key


class ObjectStore(Protocol):
    stage: str

    def put_file(self, key: str, source: Path) -> None: ...
    def get_file(self, key: str, target: Path) -> None: ...
    def list_keys(self, prefix: str) -> tuple[str, ...]: ...
    def accounting(self) -> dict[str, object]: ...


class Accounting:
    def __init__(self) -> None:
        self.stage = "setup"
        self.calls: Counter[str] = Counter()
        self.bytes_read = 0
        self._lock = threading.Lock()

    def count(self, operation: str, *, size: int = 0) -> None:
        with self._lock:
            self.calls[f"{self.stage}.{operation}"] += 1
            self.bytes_read += size

    def accounting(self) -> dict[str, object]:
        with self._lock:
            return {
                "calls": dict(self.calls),
                "bytes_read": self.bytes_read,
                "wire_attempts": None,
                "billable_usage": None,
            }


class FileObjectStore(Accounting):
    """External-to-cache offline authority, not an AWS durability claim."""

    def __init__(self, root: Path) -> None:
        super().__init__()
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, key: str) -> Path:
        path = (self.root / PurePosixPath(safe_key(key))).resolve()
        if not path.is_relative_to(self.root):
            raise CheckpointError("object path escapes store")
        return path

    def put_file(self, key: str, source: Path) -> None:
        self.count("PUT")
        if source.stat().st_size > MAX_OBJECT_BYTES:
            raise CheckpointError("object exceeds byte budget")
        target = self.path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if file_hash(target) != file_hash(source):
                raise CheckpointError("immutable object conflict")
            return
        fd, name = tempfile.mkstemp(dir=target.parent, prefix=".upload-")
        temporary = Path(name)
        try:
            with os.fdopen(fd, "wb") as output, source.open("rb") as input_file:
                shutil.copyfileobj(input_file, output)
                output.flush()
                os.fsync(output.fileno())
            # Hard-link installation is atomic and cannot overwrite an existing key.
            try:
                os.link(temporary, target)
            except FileExistsError:
                if file_hash(target) != file_hash(source):
                    raise CheckpointError("immutable object conflict") from None
            _fsync_directory(target.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def get_file(self, key: str, target: Path) -> None:
        self.count("GET")
        source = self.path(key)
        size = source.stat().st_size
        if size > MAX_OBJECT_BYTES:
            raise CheckpointError("object exceeds byte budget")
        shutil.copyfile(source, target)
        with self._lock:
            self.bytes_read += size

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        safe_key(prefix.rstrip("/"))
        self.count("LIST")
        found = []
        for path in self.root.rglob("*"):
            key = path.relative_to(self.root).as_posix()
            if path.is_file() and key.startswith(prefix) and not path.name.startswith("."):
                found.append(key)
                if len(found) > MAX_OBJECTS:
                    raise CheckpointError("object scan budget exceeded")
        return tuple(sorted(found, reverse=True))


def put_bytes(store: ObjectStore, key: str, body: bytes) -> None:
    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory) / "body"
        source.write_bytes(body)
        store.put_file(key, source)


def get_bytes(store: ObjectStore, key: str) -> bytes:
    with tempfile.TemporaryDirectory() as directory:
        target = Path(directory) / "body"
        store.get_file(key, target)
        return target.read_bytes()
