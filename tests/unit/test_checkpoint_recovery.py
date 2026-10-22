import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest
from test_wal_batching import _dataset, _entity_delta, _tenant_ids

from ephemeraldb.checkpoint.coordinator import CheckpointCoordinator
from ephemeraldb.checkpoint.models import (
    CheckpointError,
    boundary_key,
    checkpoint_prefix,
    encode,
    signed,
    validate,
)
from ephemeraldb.checkpoint.snapshot import validate_snapshot
from ephemeraldb.checkpoint.store import FileObjectStore, file_hash, put_bytes
from ephemeraldb.core.errors import InjectedWalFaultError
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.executor import WalCommitFaultPoint
from ephemeraldb.recovery.coordinator import restore
from ephemeraldb.recovery.offline import OfflineWalStore
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig


def open_engine(dataset, wal, mode):
    if mode == "single":
        return TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=wal)
    return TenantDatabaseEngine.from_dataset_dir(
        dataset, wal_committer=BatchedWalCommitter(wal, config=WalBatchingConfig(batch_window_ms=0))
    )


@pytest.mark.parametrize("mode", ["single", "batch"])
@pytest.mark.parametrize("checkpoint_sequence", [0, 1, 2])
def test_roundtrip_retry_and_first_new_write(tmp_path, mode, checkpoint_sequence):
    dataset = _dataset(tmp_path / "source", tenant_count=2)
    tenant = _tenant_ids(dataset)[0]
    wal = OfflineWalStore(tmp_path / "wal")
    store = FileObjectStore(tmp_path / "checkpoints")
    coordinator = CheckpointCoordinator(store, "run")
    engine = open_engine(dataset, wal, mode)
    first = _entity_delta(dataset, tenant)
    second = replace(first, payload=replace(first.payload, transaction_id="second"))
    try:
        for n in range(3):
            if n == checkpoint_sequence:
                coordinator.create(engine, tenant)
            if n < 2:
                assert engine.execute((first, second)[n]).success
        assert engine.execute(_entity_delta(dataset, _tenant_ids(dataset)[1])).success
        boundary = coordinator.seal(engine, tenant)
        assert not engine.execute(second).success
    finally:
        engine.close_all()
    output = tmp_path / "restored"
    result = restore(
        store,
        wal,
        namespace="run",
        tenant=tenant,
        boundary_object_key=boundary_key(boundary),
        destination=output,
        old_writers_stopped=True,
    )
    assert result["records_applied"] == 2 - checkpoint_sequence
    assert result["final_sequence"] == 2
    engine = open_engine(output, wal, mode)
    try:
        retry = engine.execute(first)
        assert retry.success and retry.duplicate_transaction
        assert engine.execute(replace(first, payload=replace(first.payload, delta=99))).conflict
        next_result = engine.execute(
            replace(first, payload=replace(first.payload, transaction_id="third"))
        )
        assert next_result.success and next_result.wal_sequence_no == 3
    finally:
        engine.close_all()


def prepared(tmp_path):
    dataset = _dataset(tmp_path / "source", tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    wal = OfflineWalStore(tmp_path / "wal")
    store = FileObjectStore(tmp_path / "checkpoints")
    coordinator = CheckpointCoordinator(store, "run")
    engine = open_engine(dataset, wal, "single")
    first = _entity_delta(dataset, tenant)
    coordinator.create(engine, tenant)
    assert engine.execute(first).success
    coordinator.create(engine, tenant)
    assert engine.execute(
        replace(first, payload=replace(first.payload, transaction_id="second"))
    ).success
    boundary = coordinator.seal(engine, tenant)
    engine.close_all()
    return store, wal, coordinator, tenant, boundary


def recover(tmp_path, store, wal, tenant, boundary):
    return restore(
        store,
        wal,
        namespace="run",
        tenant=tenant,
        boundary_object_key=boundary_key(boundary),
        destination=tmp_path / "restored",
        old_writers_stopped=True,
    )


@pytest.mark.parametrize("sequence", [1, 2])
def test_missing_middle_or_required_trailing_wal_blocks_install(tmp_path, sequence):
    store, wal, _, tenant, boundary = prepared(tmp_path)
    wal.objects.path(f"{tenant}/{sequence:020d}.json").unlink()
    with pytest.raises(CheckpointError, match="missing"):
        recover(tmp_path, store, wal, tenant, boundary)
    assert not (tmp_path / "restored").exists()
    assert not list(tmp_path.glob(".restore-*"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("format_version", 9),
        ("schema_version", 9),
        ("internal_version", 9),
        ("logical_checksum_version", 9),
        ("head", None),
        ("sequence", -1),
        ("created_at", "2026-01-01"),
        ("snapshot_key", "../escape"),
    ],
)
def test_manifest_rejects_invalid_contract(tmp_path, field, value):
    _, _, coordinator, tenant, _ = prepared(tmp_path)
    manifest = coordinator.list(tenant)[0]
    with pytest.raises(ValueError):
        validate(signed(replace(manifest, **{field: value})))


@pytest.mark.parametrize(
    "mutation", ["truncate", "tenant", "sequence", "head", "identity", "schema"]
)
def test_corrupt_newest_snapshot_never_falls_back_or_installs(tmp_path, mutation):
    store, wal, coordinator, tenant, boundary = prepared(tmp_path)
    manifest = coordinator.list(tenant)[0]
    path = store.path(manifest.snapshot_key)
    if mutation == "truncate":
        path.write_bytes(b"broken")
    else:
        with sqlite3.connect(path) as connection:
            sql = {
                "tenant": "UPDATE tenants SET tenant_id='wrong'",
                "sequence": "UPDATE _ephemeraldb_state SET last_wal_sequence=9",
                "head": "UPDATE _ephemeraldb_state SET last_record_checksum='wrong'",
                "identity": "DELETE FROM _ephemeraldb_request_identity",
                "schema": "CREATE TABLE surprise (value TEXT)",
            }[mutation]
            connection.execute(sql)
        # Re-sign the outer bytes to exercise structural/internal checks, not just SHA.
        manifest = signed(
            replace(manifest, snapshot_sha256=file_hash(path), size_bytes=path.stat().st_size)
        )
        store.path(checkpoint_prefix(manifest) + "manifest.json").write_bytes(encode(manifest))
    with pytest.raises((CheckpointError, sqlite3.DatabaseError)):
        recover(tmp_path, store, wal, tenant, boundary)
    assert not (tmp_path / "restored").exists()


def test_manifest_failure_leaves_only_orphan_and_cleans_capture(tmp_path, monkeypatch):
    dataset = _dataset(tmp_path / "source", tenant_count=1)
    wal = OfflineWalStore(tmp_path / "wal")
    store = FileObjectStore(tmp_path / "store")
    coordinator = CheckpointCoordinator(store, "run")
    original = store.put_file

    def fail(key, source):
        if key.endswith("manifest.json"):
            raise TimeoutError("manifest not uploaded")
        original(key, source)

    monkeypatch.setattr(store, "put_file", fail)
    engine = open_engine(dataset, wal, "single")
    try:
        with pytest.raises(TimeoutError):
            coordinator.create(engine, _tenant_ids(dataset)[0])
        assert coordinator.list(_tenant_ids(dataset)[0]) == ()
        assert list(store.root.rglob("snapshot.db"))
    finally:
        engine.close_all()


def test_snapshot_bound_to_capture_not_later_live_state(tmp_path, monkeypatch):
    dataset = _dataset(tmp_path / "source", tenant_count=2)
    wal = OfflineWalStore(tmp_path / "wal")
    store = FileObjectStore(tmp_path / "store")
    coordinator = CheckpointCoordinator(store, "run")
    engine = open_engine(dataset, wal, "single")
    tenant = _tenant_ids(dataset)[0]
    entered = threading.Event()
    release = threading.Event()
    original = store.put_file

    def blocked(key, source):
        if key.endswith("snapshot.db"):
            entered.set()
            assert release.wait(5)
        original(key, source)

    monkeypatch.setattr(store, "put_file", blocked)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(coordinator.create, engine, tenant)
            try:
                assert entered.wait(5)
                assert engine.execute(_entity_delta(dataset, tenant)).success
                assert engine.execute(_entity_delta(dataset, _tenant_ids(dataset)[1])).success
            finally:
                release.set()
            assert future.result()["sequence"] == 0
        manifest = coordinator.list(tenant)[0]
        assert validate_snapshot(store.path(manifest.snapshot_key), manifest)["sequence"] == 0
        assert not Path(str(store.path(manifest.snapshot_key)) + "-wal").exists()
    finally:
        engine.close_all()


def test_unresolved_tenant_cannot_checkpoint_or_seal(tmp_path):
    dataset = _dataset(tmp_path / "source", tenant_count=1)
    engine = open_engine(dataset, OfflineWalStore(tmp_path / "wal"), "single")
    coordinator = CheckpointCoordinator(FileObjectStore(tmp_path / "store"), "run")
    tenant = _tenant_ids(dataset)[0]

    def fault(point, connection):
        if point == WalCommitFaultPoint.AFTER_REMOTE_PERSIST:
            raise InjectedWalFaultError("local failure")

    try:
        assert not engine.execute(_entity_delta(dataset, tenant), wal_fault_hook=fault).success
        for command in (coordinator.create, coordinator.seal):
            with pytest.raises(Exception, match="READY"):
                command(engine, tenant)
    finally:
        engine.close_all()


def test_immutable_conflict_and_equivalent_candidates(tmp_path):
    store, wal, coordinator, tenant, boundary = prepared(tmp_path)
    manifest = coordinator.list(tenant)[0]
    put_bytes(store, "run/object", b"same")
    put_bytes(store, "run/object", b"same")
    with pytest.raises(CheckpointError, match="conflict"):
        put_bytes(store, "run/object", b"different")
    peer = replace(manifest, checkpoint_id="equivalent")
    peer = signed(replace(peer, snapshot_key=checkpoint_prefix(peer) + "snapshot.db"))
    coordinator.publish(store.path(manifest.snapshot_key), peer)
    assert recover(tmp_path, store, wal, tenant, boundary)["final_sequence"] == 2


def test_conflicting_candidate_and_existing_destination_fail_closed(tmp_path):
    store, wal, coordinator, tenant, boundary = prepared(tmp_path)
    manifest = coordinator.list(tenant)[0]
    peer = replace(manifest, checkpoint_id="conflict", logical_checksum="a" * 64)
    peer = signed(replace(peer, snapshot_key=checkpoint_prefix(peer) + "snapshot.db"))
    put_bytes(store, checkpoint_prefix(peer) + "manifest.json", encode(peer))
    with pytest.raises(CheckpointError, match="conflicting"):
        recover(tmp_path, store, wal, tenant, boundary)
    (tmp_path / "restored").mkdir()
    sentinel = tmp_path / "restored" / "unrelated.txt"
    sentinel.write_text("retain")
    with pytest.raises(CheckpointError, match="must not exist"):
        recover(tmp_path, store, wal, tenant, boundary)
    assert sentinel.read_text() == "retain"


def test_interrupted_restore_can_restart_without_exposing_partial_state(tmp_path, monkeypatch):
    from ephemeraldb.recovery import coordinator as module

    store, wal, _, tenant, boundary = prepared(tmp_path)
    replay = module.replay_wal

    def interrupted(*args, **kwargs):
        raise RuntimeError("interrupted replay")

    monkeypatch.setattr(module, "replay_wal", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        recover(tmp_path, store, wal, tenant, boundary)
    assert not (tmp_path / "restored").exists()
    assert not list(tmp_path.glob(".restore-*"))
    monkeypatch.setattr(module, "replay_wal", replay)
    assert recover(tmp_path, store, wal, tenant, boundary)["state"] == "READY"


def test_duplicate_wal_deduplicates_but_conflict_and_unresolved_boundary_block(
    tmp_path, monkeypatch
):
    from ephemeraldb.wal.checksum import attach_record_checksum

    store, wal, _, tenant, boundary = prepared(tmp_path)
    history = wal.inspect_history(tenant)
    monkeypatch.setattr(wal, "inspect_history", lambda tenant: history + history)
    result = recover(tmp_path, store, wal, tenant, boundary)
    assert result["records_deduplicated"] == 2
    conflict = replace(
        history[-1], record=attach_record_checksum(replace(history[-1].record, owner_epoch=99))
    )
    monkeypatch.setattr(wal, "inspect_history", lambda tenant: (*history, conflict))
    with pytest.raises(CheckpointError, match="conflicting"):
        restore(
            store,
            wal,
            namespace="run",
            tenant=tenant,
            boundary_object_key=boundary_key(boundary),
            destination=tmp_path / "conflict",
            old_writers_stopped=True,
        )
    with pytest.raises(CheckpointError, match="stopped-writer"):
        restore(
            store,
            wal,
            namespace="run",
            tenant=tenant,
            boundary_object_key=boundary_key(boundary),
            destination=tmp_path / "unknown",
            old_writers_stopped=False,
        )


def test_failed_cli_exports_actionable_artifact(tmp_path):
    import json

    from ephemeraldb.checkpoint.cli import main

    store, wal, _, tenant, _ = prepared(tmp_path)
    output = tmp_path / "failure.json"
    assert (
        main(
            [
                "restore",
                "--namespace",
                "run",
                "--tenant",
                tenant,
                "--checkpoint-root",
                str(store.root),
                "--wal-root",
                str(wal.objects.root),
                "--boundary-key",
                "run/missing.json",
                "--old-writers-stopped",
                "--destination",
                str(tmp_path / "restored"),
                "--output",
                str(output),
            ]
        )
        == 1
    )
    artifact = json.loads(output.read_text())
    assert artifact["status"] == "failed" and "FileNotFoundError" in artifact["error"]


def test_identical_suffix_record_in_multiple_batch_objects_is_recoverable(tmp_path):
    from ephemeraldb.wal.batching import build_batch

    store, wal, _, tenant, boundary = prepared(tmp_path)
    history = wal.inspect_history(tenant)
    wal.persist_batch(build_batch([history[-1].record]))
    result = recover(tmp_path, store, wal, tenant, boundary)
    assert result["records_deduplicated"] == 1
    assert result["final_sequence"] == 2
