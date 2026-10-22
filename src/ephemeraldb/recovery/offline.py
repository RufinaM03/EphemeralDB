"""External-filesystem WAL adapter for the offline cache-loss proof only."""

from __future__ import annotations

from pathlib import Path

from ephemeraldb.checkpoint.models import CheckpointError
from ephemeraldb.checkpoint.store import (
    MAX_OBJECTS,
    MAX_RECORDS,
    MAX_SCAN_BYTES,
    FileObjectStore,
    get_bytes,
    put_bytes,
)
from ephemeraldb.wal.batching.codec import decode_batch, encode_batch
from ephemeraldb.wal.batching.models import WalBatch
from ephemeraldb.wal.codec import decode_record, encode_record
from ephemeraldb.wal.durability import PersistedBatchReceipt, PersistedWalReceipt, StoredWalRecord
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.writer import safe_tenant_dir_name


class OfflineWalStore:
    def __init__(self, root: Path) -> None:
        self.objects = FileObjectStore(root)
        self.namespace_id = "offline:" + str(root.resolve())
        self.scan_metrics: dict[str, int] = {}

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        self.objects.stage = "wal_workload"
        key = f"{safe_tenant_dir_name(record.tenant_id)}/{record.sequence_no:020d}.json"
        body = encode_record(record)
        put_bytes(self.objects, key, body)
        assert record.record_checksum is not None
        return PersistedWalReceipt(
            "offline", key, None, None, record.record_checksum, len(body), 0, False
        )

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        self.objects.stage = "wal_workload"
        key = f"wal-batches/{batch.batch_id}.json"
        body = encode_batch(batch)
        put_bytes(self.objects, key, body)
        assert batch.batch_checksum is not None
        return PersistedBatchReceipt(
            "offline",
            key,
            None,
            None,
            batch.batch_id,
            batch.batch_checksum,
            batch.record_count,
            len(body),
            0,
            False,
        )

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        return decode_record(
            get_bytes(self.objects, f"{safe_tenant_dir_name(tenant_id)}/{sequence_no:020d}.json")
        )

    def fetch_batch(self, key: str) -> WalBatch:
        return decode_batch(get_bytes(self.objects, key))

    def inspect_history(self, tenant_id: str) -> tuple[StoredWalRecord, ...]:
        found = []
        size = irrelevant = objects = total_records = 0
        self.objects.stage = "wal_discovery"
        for prefix in (safe_tenant_dir_name(tenant_id) + "/", "wal-batches/"):
            for key in self.objects.list_keys(prefix):
                body = get_bytes(self.objects, key)
                size += len(body)
                objects += 1
                if objects > MAX_OBJECTS:
                    raise CheckpointError("WAL scan object budget exceeded")
                if size > MAX_SCAN_BYTES:
                    raise CheckpointError("WAL scan byte budget exceeded")
                records = (
                    decode_batch(body).records
                    if prefix == "wal-batches/"
                    else (decode_record(body),)
                )
                total_records += len(records)
                if total_records > MAX_RECORDS:
                    raise CheckpointError("WAL scan record budget exceeded")
                for record in records:
                    if record.tenant_id == tenant_id:
                        found.append(StoredWalRecord(record, key, False))
                    else:
                        irrelevant += 1
        self.scan_metrics = {
            "objects_scanned": objects,
            "bytes_read": size,
            "irrelevant_records": irrelevant,
        }
        return tuple(found)
