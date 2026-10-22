"""Real child termination and complete cache-loss proof using offline authorities."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import platform
import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from ephemeraldb.checkpoint.coordinator import CheckpointCoordinator
from ephemeraldb.checkpoint.models import CheckpointError, boundary_key, digest
from ephemeraldb.checkpoint.store import FileObjectStore
from ephemeraldb.core.errors import InjectedWalFaultError
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.executor import WalCommitFaultPoint
from ephemeraldb.loadgen.models import (
    BenchmarkPhase,
    EntityDeltaPayload,
    OperationType,
    WorkloadOperation,
)
from ephemeraldb.recovery.coordinator import restore
from ephemeraldb.recovery.offline import OfflineWalStore
from ephemeraldb.schema.generator import deterministic_uuid, generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig
from ephemeraldb.wal.identity import request_fingerprint

SEED = 42


def operation(tenant_index: int, index: int) -> WorkloadOperation:
    return WorkloadOperation(
        operation_id=f"op-{tenant_index}-{index}",
        sequence_no=index,
        tenant_id=deterministic_uuid(SEED, "tenant", tenant_index),
        operation_type=OperationType.ENTITY_DELTA,
        scheduled_offset_ms=0,
        benchmark_phase=BenchmarkPhase.MEASUREMENT,
        virtual_client_id="demo",
        payload=EntityDeltaPayload(
            transaction_id=f"tx-{tenant_index}-{index}",
            entity_id=deterministic_uuid(SEED, "tenant", tenant_index, "entity", 0),
            delta=index + 1,
        ),
    )


def engine_for(dataset: Path, wal: OfflineWalStore, mode: str) -> TenantDatabaseEngine:
    if mode == "single":
        return TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=wal)
    return TenantDatabaseEngine.from_dataset_dir(
        dataset,
        wal_committer=BatchedWalCommitter(
            wal, config=WalBatchingConfig(batch_window_ms=10, max_records_per_batch=2)
        ),
    )


def _child(root: str, mode: str, scenario: str, interval: int, pipe: Connection) -> None:
    base = Path(root)
    try:
        dataset = generate_dataset(
            base / "cache",
            DatasetConfig(
                tenant_count=2,
                entities_per_tenant=4,
                relationships_per_entity=1,
                metadata_entries_per_entity=1,
                seed=SEED,
            ),
        )
        wal = OfflineWalStore(base / "authority-wal")
        checkpoints = CheckpointCoordinator(FileObjectStore(base / "authority-checkpoints"), "run")
        engine = engine_for(dataset.output_dir, wal, mode)
        evidence = [checkpoints.create(engine, operation(t, 0).tenant_id) for t in range(2)]

        def execute(t: int, n: int) -> dict[str, object]:
            def fault(point: WalCommitFaultPoint, connection: sqlite3.Connection) -> None:
                if (
                    scenario == "durable-before-ack"
                    and t == 0
                    and n == 3
                    and (point == WalCommitFaultPoint.BEFORE_ACK)
                ):
                    raise InjectedWalFaultError("controlled durable-before-ACK boundary")

            op = operation(t, n)
            assert isinstance(op.payload, EntityDeltaPayload)
            result = engine.execute(op, wal_fault_hook=fault)
            expected_loss = scenario == "durable-before-ack" and t == 0 and n == 3
            if not result.success and not expected_loss:
                raise CheckpointError(str(result))
            return {
                "tenant_index": t,
                "index": n,
                "ack": result.success,
                "transaction_id": op.payload.transaction_id,
                "fingerprint": request_fingerprint(op),
                "outcome": result.durability_outcome.value,
            }

        with ThreadPoolExecutor(max_workers=2) as pool:
            for n in range(4):
                futures = [pool.submit(execute, t, n) for t in range(2)]
                pipe.send({"outcomes": [f.result() for f in futures]})
                if pipe.recv() != "recorded":
                    raise CheckpointError("parent did not confirm ledger")
                if (n + 1) % interval == 0 and n < 3:
                    evidence.extend(
                        checkpoints.create(engine, operation(t, n).tenant_id) for t in range(2)
                    )
        boundaries = [
            boundary_key(checkpoints.seal(engine, operation(t, 0).tenant_id)) for t in range(2)
        ]
        pipe.send({"sealed": boundaries, "checkpoints": evidence})
        pipe.recv()  # Parent kills this process; it never exits cleanly on the proof path.
        engine.close_all()
    except BaseException as exc:
        pipe.send({"error": f"{type(exc).__name__}: {exc}"})
    finally:
        pipe.close()


def run_demo(
    root: Path, *, mode: str = "batch", scenario: str = "post-ack", interval: int = 2
) -> dict[str, Any]:
    if mode not in {"single", "batch"} or scenario not in {"post-ack", "durable-before-ack"}:
        raise ValueError("unsupported demonstration mode/scenario")
    if interval not in {1, 2, 3, 4}:
        raise ValueError("checkpoint interval must be 1..4; demo is capped at eight writes")
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter_ns()
    result: dict[str, Any] = {
        "artifact_schema_version": 1,
        "store_type": "offline-filesystem-not-AWS",
        "configuration": {"mode": mode, "scenario": scenario, "interval": interval, "seed": SEED},
        "namespace": str(root),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "dependencies": {"boto3": _dependency_version("boto3")},
        "revision": None,
        "workload_sha256": digest([asdict(operation(t, n)) for n in range(4) for t in range(2)]),
        "outcomes": [],
        "cleanup_status": "authoritative objects and evidence retained",
    }
    parent, child = multiprocessing.get_context("spawn").Pipe()
    process = multiprocessing.get_context("spawn").Process(
        target=_child, args=(str(root), mode, scenario, interval, child)
    )
    process.start()
    child.close()
    try:
        while True:
            if not parent.poll(45):
                raise TimeoutError("child did not reach a controlled boundary within 45 seconds")
            message = parent.recv()
            if "error" in message:
                raise CheckpointError(message["error"])
            if "sealed" in message:
                result["checkpoints"] = message["checkpoints"]
                boundaries = message["sealed"]
                break
            result["outcomes"].extend(message["outcomes"])
            # This ledger is parent-owned and outside the disposable cache directory.
            (root / "ack-ledger.json").write_text(
                json.dumps(result["outcomes"], indent=2), encoding="utf-8"
            )
            parent.send("recorded")
        process.kill()
        process.join(10)
        if process.is_alive():
            raise TimeoutError("child termination did not complete")
        result["child_exitcode"] = process.exitcode
        cache = (root / "cache").resolve()
        if cache.parent != root or cache.name != "cache" or cache.is_symlink():
            raise CheckpointError("refusing unsafe cache deletion")
        shutil.rmtree(cache)
        result["complete_cache_deleted"] = not cache.exists()
        reference = generate_dataset(
            root / "reference",
            DatasetConfig(
                tenant_count=2,
                entities_per_tenant=4,
                relationships_per_entity=1,
                metadata_entries_per_entity=1,
                seed=SEED,
            ),
        )
        result["dataset_identity"] = reference.manifest.state_checksum
        oracle = TenantDatabaseEngine.from_dataset_dir(reference.output_dir)
        wal = OfflineWalStore(root / "authority-wal")
        store = FileObjectStore(root / "authority-checkpoints")
        recoveries = []
        try:
            for n in range(4):
                for t in range(2):
                    if not oracle.execute(operation(t, n)).success:
                        raise CheckpointError("independent reference execution failed")
            for t in range(2):
                tenant = operation(t, 0).tenant_id
                destination = root / f"restored-{t}"
                recovered = restore(
                    store,
                    wal,
                    namespace="run",
                    tenant=tenant,
                    boundary_object_key=boundaries[t],
                    destination=destination,
                    old_writers_stopped=True,
                )
                expected = oracle.calculate_tenant_checksum(tenant)
                if recovered["logical_checksum"] != expected:
                    raise CheckpointError("independent logical state mismatch")
                runtime = engine_for(destination, wal, mode)
                try:
                    for n in range(4):
                        retry = runtime.execute(operation(t, n))
                        if not retry.success or not retry.duplicate_transaction:
                            raise CheckpointError("recovered retry was not idempotent")
                    if len(wal.inspect_history(tenant)) != 4:
                        raise CheckpointError("retry created additional WAL")
                    with closing(sqlite3.connect(destination / "tenant-000001.db")) as connection:
                        txs = connection.execute(
                            "SELECT transaction_id FROM transactions"
                        ).fetchall()
                        if sorted(row[0] for row in txs) != [f"tx-{t}-{n}" for n in range(4)]:
                            raise CheckpointError("missing/duplicate transaction identity")
                    new = runtime.execute(operation(t, 4))
                    if not new.success or new.wal_sequence_no != 5:
                        raise CheckpointError("first new write did not use sequence 5")
                    recovered["first_new_sequence"] = new.wal_sequence_no
                    recovered["independent_checksum_match"] = True
                    recoveries.append(recovered)
                finally:
                    runtime.close_all()
        finally:
            oracle.close_all()
        result.update(
            {
                "status": "passed",
                "recoveries": recoveries,
                "acknowledged_writes": sum(o["ack"] for o in result["outcomes"]),
                "durable_unacknowledged_writes": sum(not o["ack"] for o in result["outcomes"]),
                "acknowledged_loss": 0,
                "observed_rpo_transactions": 0,
                "failure_class": "sealed process kill plus complete local-cache loss",
            }
        )
        return result
    except BaseException as exc:
        result.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        raise
    finally:
        if process.is_alive():
            process.kill()
            process.join(10)
        parent.close()
        result["elapsed_ns"] = time.perf_counter_ns() - started
        (root / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--mode", choices=("single", "batch"), default="batch")
    parser.add_argument(
        "--scenario", choices=("post-ack", "durable-before-ack"), default="post-ack"
    )
    parser.add_argument("--checkpoint-interval", type=int, choices=range(1, 5), default=2)
    args = parser.parse_args(argv)
    root = args.output_dir or Path("benchmark/results/recovery") / uuid.uuid4().hex
    result = run_demo(
        root, mode=args.mode, scenario=args.scenario, interval=args.checkpoint_interval
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "acknowledged_loss": result["acknowledged_loss"],
                "artifact": str(root.resolve() / "result.json"),
            },
            indent=2,
        )
    )
    return 0


def _dependency_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
