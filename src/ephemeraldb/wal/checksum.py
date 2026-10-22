"""SHA-256 checksums for canonical logical WAL records."""

from __future__ import annotations

import hashlib
from dataclasses import replace

from ephemeraldb.wal.codec import canonical_json_bytes, record_to_dict
from ephemeraldb.wal.models import WalRecord


def calculate_record_checksum(record: WalRecord) -> str:
    """Return the checksum over all record fields except ``record_checksum``."""

    digest = hashlib.sha256()
    digest.update(canonical_json_bytes(record_to_dict(record, include_checksum=False)))
    return digest.hexdigest()


def attach_record_checksum(record: WalRecord) -> WalRecord:
    return replace(record, record_checksum=calculate_record_checksum(record))


def record_checksum_matches(record: WalRecord) -> bool:
    return record.record_checksum == calculate_record_checksum(record)
