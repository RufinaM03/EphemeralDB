"""Local filesystem persistence for explicit logical WAL records."""

from __future__ import annotations

import os
import re
import time
import uuid
from pathlib import Path

from ephemeraldb.core.errors import WalSequenceError
from ephemeraldb.wal.codec import decode_record, encode_record
from ephemeraldb.wal.durability import PersistedWalReceipt, StoredWalRecord
from ephemeraldb.wal.models import SEQUENCE_FILENAME_WIDTH, WalRecord
from ephemeraldb.wal.validation import validate_record

TENANT_DIR_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class LocalWalStore:
    """Development-only local WAL store.

    This is a deterministic filesystem format, not an S3 mock and not the final
    durability mechanism.
    """

    def __init__(self, wal_root: Path) -> None:
        self.wal_root = Path(wal_root)
        self.wal_root.mkdir(parents=True, exist_ok=True)

    def tenant_dir(self, tenant_id: str) -> Path:
        return self.wal_root / safe_tenant_dir_name(tenant_id)

    @property
    def namespace_id(self) -> str:
        return str(self.wal_root.resolve())

    def inspect_history(self, tenant_id: str) -> tuple[StoredWalRecord, ...]:
        return tuple(
            StoredWalRecord(decode_record(path.read_bytes()), str(path), False)
            for path in sorted(self.tenant_dir(tenant_id).glob("*.json"))
        )

    def record_path(self, tenant_id: str, sequence_no: int) -> Path:
        return self.tenant_dir(tenant_id) / sequence_filename(sequence_no)

    def write_record(self, record: WalRecord) -> int:
        validate_record(record, expected_tenant_id=record.tenant_id)
        final_path = self.record_path(record.tenant_id, record.sequence_no)
        if final_path.exists():
            msg = f"WAL record already exists: {final_path}"
            raise WalSequenceError(msg)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        encoded = encode_record(record)
        temp_path = final_path.with_name(f".{final_path.name}.{uuid.uuid4().hex}.tmp")
        with temp_path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(final_path)
        _fsync_directory(final_path.parent)
        return len(encoded)

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        started_at = time.perf_counter_ns()
        bytes_written = self.write_record(record)
        finished_at = time.perf_counter_ns()
        assert record.record_checksum is not None
        return PersistedWalReceipt(
            bucket="local",
            key=str(self.record_path(record.tenant_id, record.sequence_no)),
            etag=None,
            version_id=None,
            checksum=record.record_checksum,
            bytes_written=bytes_written,
            duration_ns=finished_at - started_at,
            remote_durable=False,
        )

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        path = self.record_path(tenant_id, sequence_no)
        record = decode_record(path.read_bytes())
        validate_record(record, expected_tenant_id=tenant_id)
        return record


def sequence_filename(sequence_no: int) -> str:
    if sequence_no <= 0:
        msg = f"WAL sequence must be positive: {sequence_no}"
        raise WalSequenceError(msg)
    return f"{sequence_no:0{SEQUENCE_FILENAME_WIDTH}d}.json"


def safe_tenant_dir_name(tenant_id: str) -> str:
    if not tenant_id or tenant_id in {".", ".."} or not TENANT_DIR_RE.fullmatch(tenant_id):
        msg = f"unsafe tenant WAL directory name: {tenant_id!r}"
        raise WalSequenceError(msg)
    return tenant_id


def _fsync_directory(directory: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    try:
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
