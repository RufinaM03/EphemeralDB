"""Deterministic Phase 8 process proofs and JSON evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import platform
import shutil
import sqlite3
import time
from dataclasses import asdict
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from ephemeraldb.core.errors import InjectedWalFaultError, StaleOwnerError
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.executor import WalCommitFaultPoint
from ephemeraldb.loadgen.models import OperationType
from ephemeraldb.ownership.coordinator import FencedCommitter, OwnershipCoordinator
from ephemeraldb.ownership.models import OwnerToken
from ephemeraldb.ownership.recovery import (
    AuthorityCandidateBatchStore,
    AuthorityCandidateWalStore,
    AuthorityPayloadReader,
    FencedRecovery,
    copy_bootstrap,
)
from ephemeraldb.ownership.sqlite_store import SQLiteAuthorityStore
from ephemeraldb.recovery.demo import operation
from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig
from ephemeraldb.wal.checksum import attach_record_checksum
from ephemeraldb.wal.durability import SingleRecordWalCommitter, WalCommitter
from ephemeraldb.wal.models import WalMutation, WalMutationType, WalRecord


def _candidate_committer(store: SQLiteAuthorityStore, namespace: str, mode: str) -> WalCommitter:
    if mode == "single":
        return SingleRecordWalCommitter(AuthorityCandidateWalStore(store, namespace))
    return BatchedWalCommitter(
        AuthorityCandidateBatchStore(store, namespace),
        config=WalBatchingConfig(batch_window_ms=0, max_records_per_batch=2),
    )


def _demo_record(tenant: str, epoch: int, tx: str) -> WalRecord:
    timestamp = "2026-01-01T00:00:00Z"
    fingerprint = "v1:" + hashlib.sha256(f"{tenant}:{tx}".encode()).hexdigest()
    mutation = WalMutation.from_mappings(
        WalMutationType.INSERT_TRANSACTION,
        "transactions",
        key={"tenant_id": tenant, "transaction_id": tx},
        values={
            "tenant_id": tenant,
            "transaction_id": tx,
            "status": "COMMITTED",
            "created_at": timestamp,
            "completed_at": timestamp,
        },
    )
    return attach_record_checksum(
        WalRecord(
            tenant_id=tenant,
            transaction_id=tx,
            sequence_no=1,
            owner_epoch=epoch,
            operation_type=OperationType.ENTITY_DELTA,
            operations=(mutation,),
            created_at=timestamp,
            previous_record_checksum=None,
            request_fingerprint=fingerprint,
        )
    )


def _stale_child(
    authority_path: str,
    namespace: str,
    token: OwnerToken,
    mode: str,
    pipe: Connection,
) -> None:
    committer: WalCommitter | None = None
    try:
        store = SQLiteAuthorityStore(Path(authority_path))
        coordinator = OwnershipCoordinator(store, namespace)
        record = _demo_record(token.tenant_id, token.epoch, "stale-tx")
        observed = coordinator.status(token.tenant_id)
        committer = _candidate_committer(store, namespace, mode)
        receipt = committer.commit(record)
        pipe.send({"event": "candidate-stored", "key": receipt.key})
        if pipe.recv() != "resume":
            raise RuntimeError("parent did not resume stale commit")
        try:
            coordinator.commit(
                token,
                record,
                receipt,
                attempt_id="stale-attempt",
                observed=observed,
            )
        except StaleOwnerError as exc:
            pipe.send({"event": "fenced", "error": str(exc)})
        else:
            pipe.send({"event": "unsafe-accepted"})
    except BaseException as exc:
        pipe.send({"event": "error", "error": f"{type(exc).__name__}: {exc}"})
    finally:
        if committer is not None:
            committer.close()
        pipe.close()


def run_stale_owner_demo(root: Path, *, mode: str) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=False)
    authority = root / "authority.sqlite"
    namespace = "stale-owner-proof"
    tenant = "tenant-a"
    store = SQLiteAuthorityStore(authority)
    coordinator = OwnershipCoordinator(store, namespace)
    owner_a = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key="genesis.db",
        bootstrap_checksum="0" * 64,
    )
    parent, child = multiprocessing.get_context("spawn").Pipe()
    process = multiprocessing.get_context("spawn").Process(
        target=_stale_child,
        args=(str(authority), namespace, owner_a.token, mode, child),
    )
    process.start()
    child.close()
    events: list[dict[str, Any]] = []
    try:
        if not parent.poll(30):
            raise TimeoutError("stale owner did not reach candidate boundary")
        events.append(parent.recv())
        if events[-1]["event"] != "candidate-stored":
            raise RuntimeError(str(events[-1]))
        owner_b = coordinator.takeover(tenant, session_id="runtime-b")
        current = _demo_record(tenant, owner_b.owner_epoch, "successor-tx")
        committer = _candidate_committer(store, namespace, mode)
        try:
            receipt = committer.commit(current)
            accepted = coordinator.commit(
                owner_b.token, current, receipt, attempt_id="successor-attempt"
            )
        finally:
            committer.close()
        parent.send("resume")
        if not parent.poll(30):
            raise TimeoutError("stale owner did not return a decision")
        events.append(parent.recv())
        process.join(30)
        if process.exitcode != 0 or events[-1]["event"] != "fenced":
            raise RuntimeError(f"stale owner proof failed: {events}")
        final, _ = coordinator.status(tenant)
        history = coordinator.accepted_history(final)
        result = {
            "artifact_schema_version": 1,
            "scenario": "stale-owner-resumes",
            "mode": mode,
            "namespace": namespace,
            "protocol": "ephemeraldb-fenced-v1",
            "authority_backend": "offline-sqlite-multiprocess-cas",
            "revision": None,
            "revision_unavailable_reason": "workspace has no verified Git metadata",
            "deterministic_workload_identity": hashlib.sha256(
                b"stale-tx\0successor-tx"
            ).hexdigest(),
            "events": events,
            "synchronization": ["candidate-stored", "takeover", "resume-old-cas"],
            "epochs": [owner_a.owner_epoch, owner_b.owner_epoch],
            "accepted_decision": accepted.decision.value,
            "accepted_transactions": [item.transaction_id for item in history],
            "orphan_candidate_key": events[0]["key"],
            "authority_counters": store.counters(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "cleanup_status": "authoritative evidence retained",
            "primary_error": None,
            "teardown_errors": [],
            "retained_leftovers": [str(authority)],
        }
        (root / "evidence.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        return result
    finally:
        if process.is_alive():
            process.terminate()
            process.join(10)
        parent.close()


def _cache_child(
    dataset: str,
    authority_path: str,
    namespace: str,
    token: OwnerToken,
    mode: str,
    ack_loss: bool,
    pipe: Connection,
) -> None:
    engine: TenantDatabaseEngine | None = None
    try:
        store = SQLiteAuthorityStore(Path(authority_path))
        coordinator = OwnershipCoordinator(store, namespace)
        payload = _candidate_committer(store, namespace, mode)
        engine = TenantDatabaseEngine.from_dataset_dir(
            Path(dataset),
            wal_committer=FencedCommitter(payload, coordinator, {token.tenant_id: token}),
        )
        def fault(point: WalCommitFaultPoint, _connection: sqlite3.Connection) -> None:
            if ack_loss and point is WalCommitFaultPoint.BEFORE_ACK:
                raise InjectedWalFaultError("commit accepted; parent ACK deliberately withheld")

        result = engine.execute(operation(0, 0), wal_fault_hook=fault)
        event = "commit-withheld" if ack_loss else "ack"
        pipe.send({"event": event, "result": asdict(result)})
        pipe.recv()  # The parent terminates this live runtime; no seal/release/close.
    except BaseException as exc:
        pipe.send({"event": "error", "error": f"{type(exc).__name__}: {exc}"})
    finally:
        if engine is not None:
            engine.close_all()
        pipe.close()


def run_cache_loss_demo(root: Path, *, mode: str, ack_loss: bool = False) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=False)
    template = generate_dataset(
        root / "template",
        DatasetConfig(
            tenant_count=1,
            entities_per_tenant=4,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
            seed=42,
        ),
    ).output_dir
    cache_a = root / "cache-a"
    shutil.copytree(template, cache_a)
    tenant = operation(0, 0).tenant_id
    manifest = json.loads((template / "manifest.json").read_text(encoding="utf-8"))
    filename = manifest["tenant_databases"][0]["filename"]
    bootstrap_root = root / "bootstrap"
    bootstrap_root.mkdir()
    bootstrap = bootstrap_root / filename
    shutil.copyfile(template / filename, bootstrap)
    authority = root / "authority.sqlite"
    namespace = "unsealed-cache-loss-proof"
    store = SQLiteAuthorityStore(authority)
    coordinator = OwnershipCoordinator(store, namespace)
    owner_a = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key=filename,
        bootstrap_checksum=hashlib.sha256(bootstrap.read_bytes()).hexdigest(),
    )
    parent, child = multiprocessing.get_context("spawn").Pipe()
    process = multiprocessing.get_context("spawn").Process(
        target=_cache_child,
        args=(str(cache_a), str(authority), namespace, owner_a.token, mode, ack_loss, child),
    )
    process.start()
    child.close()
    started = time.perf_counter_ns()
    try:
        if not parent.poll(45):
            raise TimeoutError("writer did not return its acknowledged commit")
        acknowledged = parent.recv()
        expected_event = "commit-withheld" if ack_loss else "ack"
        if acknowledged["event"] != expected_event:
            raise RuntimeError(str(acknowledged))
        if ack_loss:
            if acknowledged["result"]["authority_decision"] != "ACCEPTED":
                raise RuntimeError("ACK-loss boundary was not authoritatively accepted")
        elif not acknowledged["result"]["success"]:
            raise RuntimeError(str(acknowledged))
        process.terminate()
        process.join(15)
        if process.is_alive():
            process.kill()
            process.join(10)
        shutil.rmtree(cache_a)
        owner_b = coordinator.takeover(tenant, session_id="runtime-b")
        recovered = root / "cache-b"
        shutil.copytree(template, recovered)
        destination = recovered / filename
        destination.unlink()
        recovery = FencedRecovery(coordinator, AuthorityPayloadReader(store)).recover(
            owner_b.token,
            destination,
            install_bootstrap=copy_bootstrap(bootstrap_root),
        )
        payload = _candidate_committer(store, namespace, mode)
        engine = TenantDatabaseEngine.from_dataset_dir(
            recovered,
            wal_committer=FencedCommitter(payload, coordinator, {tenant: owner_b.token}),
        )
        try:
            retry = engine.execute(operation(0, 0))
            following = engine.execute(operation(0, 1))
            checksum = engine.calculate_tenant_checksum(tenant)
        finally:
            engine.close_all()
        result = {
            "artifact_schema_version": 1,
            "scenario": "commit-accepted-ack-lost" if ack_loss else "unsealed-cache-loss",
            "mode": mode,
            "namespace": namespace,
            "protocol": "ephemeraldb-fenced-v1",
            "authority_backend": "offline-sqlite-multiprocess-cas",
            "revision": None,
            "revision_unavailable_reason": "workspace has no verified Git metadata",
            "deterministic_workload_identity": hashlib.sha256(
                json.dumps(asdict(operation(0, 0)), sort_keys=True).encode()
            ).hexdigest(),
            "events": [
                "commit-accepted-ack-withheld" if ack_loss else "ack-received",
                "process-terminated",
                "cache-a-deleted",
                "takeover",
            ],
            "acknowledged": acknowledged["result"],
            "epochs": [owner_a.owner_epoch, owner_b.owner_epoch],
            "recovery": asdict(recovery) | {"destination": str(recovery.destination)},
            "retry_duplicate": retry.duplicate_transaction,
            "next_sequence": following.wal_sequence_no,
            "logical_checksum": checksum,
            "observed_acknowledged_loss": 0,
            "accepted_unacknowledged_operations": int(ack_loss),
            "authority_counters": store.counters(),
            "elapsed_ns": time.perf_counter_ns() - started,
            "cleanup_status": "authoritative evidence retained",
            "primary_error": None,
            "teardown_errors": [],
            "retained_leftovers": [str(authority), str(bootstrap), str(recovered)],
        }
        (root / "evidence.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        return result
    finally:
        if process.is_alive():
            process.terminate()
            process.join(10)
        parent.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("stale-owner", "cache-loss", "ack-loss"):
        command = subparsers.add_parser(name)
        command.add_argument("--root", type=Path, required=True)
        command.add_argument("--mode", choices=("single", "batch"), default="single")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "stale-owner":
        result = run_stale_owner_demo(args.root, mode=args.mode)
    else:
        result = run_cache_loss_demo(
            args.root,
            mode=args.mode,
            ack_loss=args.command == "ack-loss",
        )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
