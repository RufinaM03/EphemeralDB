"""Tiny same-workload legacy payload versus fenced batching comparison."""

from __future__ import annotations

import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from ephemeraldb.ownership.coordinator import OwnershipCoordinator
from ephemeraldb.ownership.demo import _demo_record
from ephemeraldb.ownership.recovery import AuthorityCandidateBatchStore
from ephemeraldb.ownership.sqlite_store import SQLiteAuthorityStore
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig
from ephemeraldb.wal.durability import WalCommitReceipt
from ephemeraldb.wal.models import WalRecord


def _upload_pair(
    store: SQLiteAuthorityStore,
    namespace: str,
    records: tuple[WalRecord, ...],
) -> tuple[WalCommitReceipt, WalCommitReceipt]:
    if len(records) != 2:
        raise ValueError("accounting comparison requires exactly two records")
    batcher = BatchedWalCommitter(
        AuthorityCandidateBatchStore(store, namespace),
        config=WalBatchingConfig(batch_window_ms=1000, max_records_per_batch=2),
    )
    try:
        barrier = threading.Barrier(2)

        def commit(record: WalRecord) -> WalCommitReceipt:
            barrier.wait()
            return batcher.commit(record)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(commit, record) for record in records]
            return futures[0].result(), futures[1].result()
    finally:
        batcher.close()


def run(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=False)
    tenants = ("tenant-a", "tenant-b")

    legacy_store = SQLiteAuthorityStore(root / "legacy.sqlite")
    legacy_records = tuple(_demo_record(tenant, 1, f"{tenant}-tx") for tenant in tenants)
    started = time.perf_counter_ns()
    legacy_receipts = _upload_pair(legacy_store, "legacy-batch", legacy_records)
    legacy_elapsed = time.perf_counter_ns() - started

    fenced_store = SQLiteAuthorityStore(root / "fenced.sqlite")
    coordinator = OwnershipCoordinator(fenced_store, "fenced-batch")
    states = tuple(
        coordinator.bootstrap(
            tenant,
            session_id=f"owner-{tenant}",
            bootstrap_key=f"{tenant}.db",
            bootstrap_checksum="0" * 64,
        )
        for tenant in tenants
    )
    fenced_records = tuple(
        _demo_record(tenant, state.owner_epoch, f"{tenant}-tx")
        for tenant, state in zip(tenants, states, strict=True)
    )
    started = time.perf_counter_ns()
    fenced_receipts = _upload_pair(fenced_store, "fenced-batch", fenced_records)
    for tenant, state, record, receipt in zip(
        tenants, states, fenced_records, fenced_receipts, strict=True
    ):
        coordinator.commit(
            state.token,
            record,
            receipt,
            attempt_id=f"attempt-{tenant}",
        )
    fenced_elapsed = time.perf_counter_ns() - started
    result = {
        "artifact_schema_version": 1,
        "workload": {"logical_writes": 2, "tenants": list(tenants), "shared_batch": True},
        "legacy_payload_only": {
            "elapsed_ns": legacy_elapsed,
            "payload_bytes": sum(
                receipt.remote_wal_bytes_written or 0 for receipt in legacy_receipts
            ),
            "requests": legacy_store.counters(),
        },
        "fenced": {
            "elapsed_ns": fenced_elapsed,
            "payload_bytes": sum(
                receipt.remote_wal_bytes_written or 0 for receipt in fenced_receipts
            ),
            "requests": fenced_store.counters(),
        },
        "scope": "offline adapter calls; not AWS wire attempts, prices, or a latency benchmark",
    }
    (root / "accounting.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(run(args.root), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
