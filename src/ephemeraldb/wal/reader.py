"""Streaming readers for local explicit WAL files."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

from ephemeraldb.core.errors import WalChainError, WalEncodingError, WalSequenceError
from ephemeraldb.wal.codec import decode_record
from ephemeraldb.wal.models import SEQUENCE_FILENAME_WIDTH, WalRecord
from ephemeraldb.wal.validation import validate_record
from ephemeraldb.wal.writer import safe_tenant_dir_name, sequence_filename

WAL_FILENAME_RE = re.compile(rf"^[0-9]{{{SEQUENCE_FILENAME_WIDTH}}}\.json$")


def iter_record_paths(
    wal_root: Path,
    tenant_id: str,
    *,
    start_sequence: int | None = None,
    end_sequence: int | None = None,
) -> Iterator[tuple[int, Path]]:
    _validate_sequence_range(start_sequence, end_sequence)
    root = Path(wal_root)
    if not root.exists():
        msg = f"WAL root does not exist: {root}"
        raise WalEncodingError(msg)
    tenant_dir = root / safe_tenant_dir_name(tenant_id)
    if not tenant_dir.exists():
        return
    if not tenant_dir.is_dir():
        msg = f"WAL tenant path is not a directory: {tenant_dir}"
        raise WalEncodingError(msg)

    paths: list[tuple[int, Path]] = []
    seen_sequences: set[int] = set()
    for path in tenant_dir.iterdir():
        if not path.is_file():
            msg = f"unexpected non-file in WAL directory: {path}"
            raise WalEncodingError(msg)
        if WAL_FILENAME_RE.fullmatch(path.name) is None:
            msg = f"unexpected WAL filename: {path.name}"
            raise WalEncodingError(msg)
        sequence_no = int(path.stem)
        if sequence_no <= 0 or sequence_filename(sequence_no) != path.name:
            msg = f"invalid WAL sequence filename: {path.name}"
            raise WalSequenceError(msg)
        if sequence_no in seen_sequences:
            msg = f"duplicate WAL sequence file: {sequence_no}"
            raise WalSequenceError(msg)
        seen_sequences.add(sequence_no)
        if start_sequence is not None and sequence_no < start_sequence:
            continue
        if end_sequence is not None and sequence_no > end_sequence:
            continue
        paths.append((sequence_no, path))

    yield from sorted(paths, key=lambda pair: pair[0])


def iter_records(
    wal_root: Path,
    tenant_id: str,
    *,
    start_sequence: int | None = None,
    end_sequence: int | None = None,
) -> Iterator[WalRecord]:
    expected_sequence = 1 if start_sequence is None else start_sequence
    previous_checksum: str | None = None
    have_previous = False
    for filename_sequence, path in iter_record_paths(
        wal_root,
        tenant_id,
        start_sequence=start_sequence,
        end_sequence=end_sequence,
    ):
        if filename_sequence != expected_sequence:
            msg = f"expected WAL sequence {expected_sequence}, got {filename_sequence}"
            raise WalSequenceError(msg)
        record = decode_record(path.read_bytes())
        validate_record(record, expected_tenant_id=tenant_id)
        if record.sequence_no != filename_sequence:
            msg = (
                f"WAL filename sequence {filename_sequence} does not match "
                f"record sequence {record.sequence_no}"
            )
            raise WalSequenceError(msg)
        if record.sequence_no == 1 and record.previous_record_checksum is not None:
            msg = "first WAL record must not have a previous checksum"
            raise WalChainError(msg)
        if have_previous and record.previous_record_checksum != previous_checksum:
            msg = f"WAL chain mismatch at sequence {record.sequence_no}"
            raise WalChainError(msg)
        previous_checksum = record.record_checksum
        have_previous = True
        expected_sequence += 1
        yield record


def _validate_sequence_range(start_sequence: int | None, end_sequence: int | None) -> None:
    if start_sequence is not None and start_sequence <= 0:
        msg = f"start_sequence must be positive: {start_sequence}"
        raise WalSequenceError(msg)
    if end_sequence is not None and end_sequence <= 0:
        msg = f"end_sequence must be positive: {end_sequence}"
        raise WalSequenceError(msg)
    if start_sequence is not None and end_sequence is not None and start_sequence > end_sequence:
        msg = f"start_sequence {start_sequence} is after end_sequence {end_sequence}"
        raise WalSequenceError(msg)
