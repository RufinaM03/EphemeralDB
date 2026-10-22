import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest
from test_phase5_durability import RecordingDurabilityStore
from test_wal_batching import (
    RecordingBatchStore,
    _dataset,
    _entity_delta,
    _record,
    _tenant_db_path,
    _tenant_ids,
)

from ephemeraldb.core.errors import (
    InjectedWalFaultError,
    PersistenceRejectedError,
    RemoteDurabilityError,
    UnsupportedWalVersionError,
    WalBatchingClosedError,
    WalReplayError,
)
from ephemeraldb.engine.durability_guard import TenantNotReadyError
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.executor import WalCommitFaultPoint
from ephemeraldb.engine.results import DurabilityOutcome
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig
from ephemeraldb.wal.batching import batcher as batcher_module
from ephemeraldb.wal.checksum import attach_record_checksum
from ephemeraldb.wal.codec import decode_record, encode_record
from ephemeraldb.wal.durability import StoredWalRecord
from ephemeraldb.wal.identity import request_fingerprint
from ephemeraldb.wal.replay import replay_wal
from ephemeraldb.wal.validation import validate_record


class HistoryBatchStore(RecordingBatchStore):
    def inspect_history(self, tenant):
        return tuple(
            StoredWalRecord(
                record, f"wal/wal-batches/{batch.created_at[:10]}/{batch.batch_id}.json"
            )
            for batch in self.batches
            for record in batch.records
            if record.tenant_id == tenant
        )


class HistorySingleStore(RecordingDurabilityStore):
    def inspect_history(self, tenant):
        return tuple(
            StoredWalRecord(decode_record(body), f"wal/{tid}/{seq:020d}.json")
            for (tid, seq), body in self.bodies.items()
            if tid == tenant
        )


def open_engine(dataset, store, mode):
    if mode == "single":
        return TenantDatabaseEngine.from_dataset_dir(dataset, wal_store=store)
    return TenantDatabaseEngine.from_dataset_dir(
        dataset,
        wal_committer=BatchedWalCommitter(store, config=WalBatchingConfig(batch_window_ms=0)),
    )


def fail_after_remote(point, connection):
    if point == WalCommitFaultPoint.AFTER_REMOTE_PERSIST:
        raise InjectedWalFaultError("after remote")


@pytest.mark.parametrize("mode", ["single", "batch"])
def test_restart_reconcile_retry_and_next_sequence(tmp_path, mode):
    dataset = _dataset(tmp_path, tenant_count=2)
    tenants = _tenant_ids(dataset)
    store = HistorySingleStore() if mode == "single" else HistoryBatchStore()
    operation = _entity_delta(dataset, tenants[0])
    engine = open_engine(dataset, store, mode)
    try:
        result = engine.execute(operation, wal_fault_hook=fail_after_remote)
        assert result.durability_outcome == DurabilityOutcome.DURABLE_NOT_APPLIED
        assert engine.execute(_entity_delta(dataset, tenants[1])).success
        assert not engine.execute(operation).success
    finally:
        engine.close_all()
    engine = open_engine(dataset, store, mode)
    try:
        assert not engine.execute(operation).success
        engine.reconcile_tenant(tenants[0])
        engine.reconcile_tenant(tenants[0])
        retry = engine.execute(operation)
        assert retry.success and retry.duplicate_transaction and retry.remote_durable
        assert retry.wal_sequence_no == 1
        second = engine.execute(
            replace(operation, payload=replace(operation.payload, transaction_id="next", delta=5))
        )
        assert second.success and second.wal_sequence_no == 2
    finally:
        engine.close_all()
    engine = open_engine(dataset, store, mode)
    try:
        assert engine.execute(operation).duplicate_transaction
    finally:
        engine.close_all()


@pytest.mark.parametrize("mode", ["single", "batch"])
@pytest.mark.parametrize("stored", [False, True])
def test_unknown_timeout_stays_blocked_until_positive_evidence(tmp_path, mode, stored):
    dataset = _dataset(tmp_path, tenant_count=1)
    store = HistorySingleStore() if mode == "single" else HistoryBatchStore()
    name = "persist_record" if mode == "single" else "persist_batch"
    original = getattr(store, name)

    def timeout(value):
        if stored:
            original(value)
        raise TimeoutError("response lost")

    setattr(store, name, timeout)
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    engine = open_engine(dataset, store, mode)
    try:
        result = engine.execute(operation)
        assert not result.success and result.durability_outcome == DurabilityOutcome.UNKNOWN
        assert not engine.execute(operation).success
    finally:
        engine.close_all()
    setattr(store, name, original)
    engine = open_engine(dataset, store, mode)
    try:
        assert engine.execute(operation).reconciliation_required
        if stored:
            engine.reconcile_tenant(operation.tenant_id)
            retry = engine.execute(operation)
            assert retry.success and retry.duplicate_transaction
            assert len(store.inspect_history(operation.tenant_id)) == 1
        else:
            with pytest.raises(TenantNotReadyError, match="absence"):
                engine.reconcile_tenant(operation.tenant_id)
            assert engine.execute(operation).reconciliation_required
    finally:
        engine.close_all()


@pytest.mark.parametrize("mode", ["single", "batch"])
def test_ack_loss_retry_after_restart(tmp_path, mode):
    dataset = _dataset(tmp_path, tenant_count=1)
    store = HistorySingleStore() if mode == "single" else HistoryBatchStore()
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    engine = open_engine(dataset, store, mode)

    def fault(point, connection):
        if point == WalCommitFaultPoint.BEFORE_ACK:
            raise InjectedWalFaultError("lost ACK")

    result = engine.execute(operation, wal_fault_hook=fault)
    engine.close_all()
    assert not result.success and result.durability_outcome == DurabilityOutcome.DURABLE_APPLIED
    engine = open_engine(dataset, store, mode)
    try:
        assert engine.execute(operation).duplicate_transaction
        assert len(store.inspect_history(operation.tenant_id)) == 1
    finally:
        engine.close_all()


def test_conflicting_remote_history_quarantines_tenant(tmp_path):
    from ephemeraldb.wal.batching import build_batch

    dataset = _dataset(tmp_path, tenant_count=1)
    store = HistoryBatchStore()
    engine = open_engine(dataset, store, "batch")
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    try:
        engine.execute(operation, wal_fault_hook=fail_after_remote)
        original = store.batches[0].records[0]
        conflict = attach_record_checksum(replace(original, owner_epoch=1))
        store.batches.append(build_batch([conflict]))
        with pytest.raises(WalReplayError, match="conflicting"):
            engine.reconcile_tenant(operation.tenant_id)
        assert not engine.execute(operation).success
    finally:
        engine.close_all()


@pytest.mark.parametrize("missing", ["first", "last", "journal"])
def test_restart_rejects_incomplete_history_or_missing_journal(tmp_path, missing):
    dataset = _dataset(tmp_path, tenant_count=1)
    store = HistorySingleStore()
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    engine = open_engine(dataset, store, "single")
    try:
        assert engine.execute(operation).success
        second = replace(operation, payload=replace(operation.payload, transaction_id="second"))
        assert engine.execute(second).success
    finally:
        engine.close_all()
    if missing == "journal":
        with sqlite3.connect(dataset / ".ephemeraldb-attempts.sqlite") as journal:
            journal.execute("DELETE FROM attempts WHERE tenant=?", (operation.tenant_id,))
    else:
        del store.bodies[(operation.tenant_id, 1 if missing == "first" else 2)]
    restarted = open_engine(dataset, store, "single")
    try:
        result = restarted.execute(operation)
        assert not result.success and result.reconciliation_required
        assert result.durability_outcome == DurabilityOutcome.NOT_ATTEMPTED
        with pytest.raises((TenantNotReadyError, WalReplayError)):
            restarted.reconcile_tenant(operation.tenant_id)
    finally:
        restarted.close_all()


def test_wal_v2_replay_restores_identity_and_legacy_bytes_stay_v1(tmp_path):
    dataset = _dataset(tmp_path / "source", tenant_count=1)
    target = _dataset(tmp_path / "target", tenant_count=1)
    store = HistorySingleStore()
    engine = open_engine(dataset, store, "single")
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    assert engine.execute(operation).success
    engine.close_all()
    record = store.records[0]
    assert record.wal_format_version == 2
    assert record.request_fingerprint == request_fingerprint(operation)
    with sqlite3.connect(_tenant_db_path(target, operation.tenant_id)) as connection:
        replay_wal(
            connection,
            [record],
            remote_durable=True,
            object_keys={1: store.inspect_history(operation.tenant_id)[0].key},
        )
        replay_wal(connection, [record])
    restored = TenantDatabaseEngine.from_dataset_dir(target)
    try:
        result = restored.execute(operation)
        assert result.success and result.duplicate_transaction and result.remote_durable
        changed = replace(operation, payload=replace(operation.payload, expected_version=17))
        assert restored.execute(changed).conflict
    finally:
        restored.close_all()
    legacy = attach_record_checksum(replace(record, wal_format_version=1, request_fingerprint=None))
    assert b"request_fingerprint" not in encode_record(legacy)
    assert encode_record(decode_record(encode_record(legacy))) == encode_record(legacy)
    validate_record(legacy)
    with pytest.raises(UnsupportedWalVersionError):
        validate_record(replace(record, wal_format_version=99))


@pytest.mark.parametrize("stage", ["construction", "store", "receipt"])
def test_worker_failure_releases_waiters_and_stops_admission(monkeypatch, stage):
    store = RecordingBatchStore()
    entered = threading.Event()
    release = threading.Event()
    original_build = batcher_module.build_batch

    def fatal(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        if stage == "store":
            raise SystemExit("worker died")
        raise RuntimeError("worker died")

    if stage == "construction":
        monkeypatch.setattr(batcher_module, "build_batch", fatal)
    elif stage == "store":
        monkeypatch.setattr(store, "persist_batch", fatal)
    else:
        monkeypatch.setattr(batcher_module, "_receipt_for_record", fatal)
    committer = BatchedWalCommitter(
        store, config=WalBatchingConfig(max_records_per_batch=1, wait_timeout_seconds=3)
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(committer.commit, _record("a", 1))
        assert entered.wait(2)
        second = pool.submit(committer.commit, _record("b", 1))
        release.set()
        for future in (first, second):
            with pytest.raises(RemoteDurabilityError):
                future.result(timeout=3)
    with pytest.raises(RemoteDurabilityError):
        committer.close()
    with pytest.raises(RemoteDurabilityError):
        committer.close()
    with pytest.raises(WalBatchingClosedError):
        committer.commit(_record("c", 1))
    assert original_build is not None


def test_metrics_failure_cannot_change_commit(monkeypatch):
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(store, config=WalBatchingConfig(batch_window_ms=0))

    def fail(*args, **kwargs):
        raise RuntimeError("metrics unavailable")

    monkeypatch.setattr(committer.metrics, "observe_batch_success", fail)
    monkeypatch.setattr(committer.metrics, "observe_queue_depth", fail)
    assert committer.commit(_record("a", 1)).remote_durable
    committer.close()


def test_oversized_record_rejected_without_upload():
    store = RecordingBatchStore()
    committer = BatchedWalCommitter(store, config=WalBatchingConfig(max_batch_bytes=1))
    try:
        with pytest.raises(PersistenceRejectedError):
            committer.commit(_record("a", 1))
        assert not store.batches
    finally:
        committer.close()


def test_multi_entity_identity_order_and_expected_version(tmp_path):
    from ephemeraldb.loadgen.models import (
        MultiEntityTransactionItem,
        MultiEntityTransactionPayload,
        OperationType,
    )

    dataset = _dataset(tmp_path, tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    with sqlite3.connect(_tenant_db_path(dataset, tenant)) as connection:
        ids = [
            row[0]
            for row in connection.execute("SELECT entity_id FROM entities ORDER BY entity_id")
        ]
    store = HistorySingleStore()
    engine = open_engine(dataset, store, "single")
    operation = replace(
        _entity_delta(dataset, tenant),
        operation_type=OperationType.MULTI_ENTITY_TRANSACTION,
        payload=MultiEntityTransactionPayload(
            "multi", (MultiEntityTransactionItem(ids[0], 1), MultiEntityTransactionItem(ids[1], 2))
        ),
    )
    try:
        assert engine.execute(operation).success
        scheduled_retry = replace(
            operation, operation_id="another-runtime-id", scheduled_offset_ms=9
        )
        assert engine.execute(scheduled_retry).duplicate_transaction
        for items in (
            tuple(reversed(operation.payload.items)),
            (replace(operation.payload.items[0], expected_version=99), operation.payload.items[1]),
        ):
            assert engine.execute(
                replace(operation, payload=replace(operation.payload, items=items))
            ).conflict
        assert len(store.records) == 1
    finally:
        engine.close_all()


def test_unresolved_tenant_rejects_reads_and_new_requests(tmp_path):
    from ephemeraldb.loadgen.models import EntityLookupPayload, OperationType

    dataset = _dataset(tmp_path, tenant_count=1)
    store = HistoryBatchStore()
    engine = open_engine(dataset, store, "batch")
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    try:
        engine.execute(operation, wal_fault_hook=fail_after_remote)
        read = replace(
            operation,
            operation_type=OperationType.ENTITY_LOOKUP,
            payload=EntityLookupPayload(operation.payload.entity_id),
        )
        result = engine.execute(read)
        assert not result.success and result.reconciliation_required
        assert result.durability_outcome == DurabilityOutcome.NOT_ATTEMPTED
        engine.reconcile_tenant(operation.tenant_id)
        assert engine.execute(read).success
    finally:
        engine.close_all()


def test_post_persistence_processing_error_retains_confirmed_outcome(tmp_path, monkeypatch):
    dataset = _dataset(tmp_path, tenant_count=1)
    store = HistoryBatchStore()
    engine = open_engine(dataset, store, "batch")
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])

    def fail(**kwargs):
        raise RuntimeError("receipt failed")

    monkeypatch.setattr(batcher_module, "_receipt_for_record", fail)
    result = engine.execute(operation)
    assert result.remote_durable and result.reconciliation_required
    assert result.durability_outcome == DurabilityOutcome.DURABLE_NOT_APPLIED
    with pytest.raises(RemoteDurabilityError):
        engine.close_all()
    engine = open_engine(dataset, store, "batch")
    try:
        engine.reconcile_tenant(operation.tenant_id)
        assert engine.execute(operation).duplicate_transaction
    finally:
        engine.close_all()


def test_rejected_oversize_preserves_previous_ready_head(tmp_path):
    dataset = _dataset(tmp_path, tenant_count=1)
    store = HistoryBatchStore()
    committer = BatchedWalCommitter(store, config=WalBatchingConfig(max_batch_bytes=1))
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=committer)
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    try:
        result = engine.execute(operation)
        assert result.durability_outcome == DurabilityOutcome.REJECTED
        assert result.tenant_state == "READY" and not result.reconciliation_required
        assert not store.batches
    finally:
        engine.close_all()


def test_shutdown_without_flush_explicitly_rejects_queued_work(monkeypatch):
    from test_wal_batching import _wait_for_queue_depth

    store = RecordingBatchStore()
    committer = BatchedWalCommitter(
        store, config=WalBatchingConfig(batch_window_ms=10000, flush_on_shutdown=False)
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(committer.commit, _record("a", 1))
        _wait_for_queue_depth(committer, expected=1)
        committer.close()
        with pytest.raises(PersistenceRejectedError):
            future.result(timeout=2)
    assert not store.batches


def test_shutdown_deadline_does_not_claim_upload_cancelled():
    store = RecordingBatchStore(block=True)
    committer = BatchedWalCommitter(
        store, config=WalBatchingConfig(batch_window_ms=0, shutdown_timeout_seconds=0.01)
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(committer.commit, _record("a", 1))
        assert store.started.wait(2)
        try:
            with pytest.raises(RemoteDurabilityError, match="may still complete"):
                committer.close()
        finally:
            store.release.set()
        assert future.result(timeout=2).remote_durable
    committer.close()


def test_connection_cache_target_never_evicts_active_connections(tmp_path):
    from ephemeraldb.core.models import TenantId

    dataset = _dataset(tmp_path, tenant_count=2)
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, max_open_connections=1)
    first, second = map(TenantId, _tenant_ids(dataset))
    try:
        with engine.connection_manager.connection_for(first) as left:
            with engine.connection_manager.connection_for(second) as right:
                assert left.execute("SELECT 1").fetchone()[0] == 1
                assert right.execute("SELECT 1").fetchone()[0] == 1
                assert engine.connection_manager.open_connection_count == 2
        assert engine.connection_manager.open_connection_count == 1
    finally:
        engine.close_all()


def test_legacy_replay_rejects_unverifiable_client_retry(tmp_path):
    dataset = _dataset(tmp_path / "source", tenant_count=1)
    target = _dataset(tmp_path / "target", tenant_count=1)
    store = HistorySingleStore()
    engine = open_engine(dataset, store, "single")
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    assert engine.execute(operation).success
    engine.close_all()
    legacy = attach_record_checksum(
        replace(store.records[0], wal_format_version=1, request_fingerprint=None)
    )
    connection = sqlite3.connect(_tenant_db_path(target, operation.tenant_id))
    try:
        replay_wal(connection, [legacy])
    finally:
        connection.close()
    engine = TenantDatabaseEngine.from_dataset_dir(target)
    try:
        result = engine.execute(operation)
        assert not result.success and result.conflict
    finally:
        engine.close_all()


def test_worker_death_wakes_capacity_blocked_producer(monkeypatch):
    store = RecordingBatchStore()
    entered = threading.Event()
    release = threading.Event()
    waiting = threading.Event()

    def die(batch):
        entered.set()
        assert release.wait(2)
        raise SystemExit("fatal upload worker")

    store.persist_batch = die
    committer = BatchedWalCommitter(
        store, config=WalBatchingConfig(max_records_per_batch=1, max_queue_records=1)
    )
    wait = committer._condition.wait

    def observed_wait(timeout=None):
        waiting.set()
        return wait(timeout)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(committer.commit, _record("a", 1))
        assert entered.wait(2)
        monkeypatch.setattr(committer._condition, "wait", observed_wait)
        second = pool.submit(committer.commit, _record("b", 1))
        assert waiting.wait(2)
        release.set()
        with pytest.raises(RemoteDurabilityError):
            first.result(timeout=2)
        with pytest.raises(WalBatchingClosedError):
            second.result(timeout=2)
    with pytest.raises(RemoteDurabilityError):
        committer.close()


def test_reconcile_cli_operates_on_retained_local_attempt(tmp_path, capsys):
    from ephemeraldb.engine.reconcile import main

    dataset = _dataset(tmp_path, tenant_count=1)
    wal_root = tmp_path / "wal"
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    assert not engine.execute(operation, wal_fault_hook=fail_after_remote).success
    engine.close_all()
    assert (
        main(
            [
                "--dataset-dir",
                str(dataset),
                "--tenant",
                operation.tenant_id,
                "--wal-store",
                "local",
                "--wal-root",
                str(wal_root),
            ]
        )
        == 0
    )
    assert '"state": "READY"' in capsys.readouterr().out
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_root=wal_root)
    try:
        assert engine.execute(operation).duplicate_transaction
    finally:
        engine.close_all()


def test_durable_local_failure_blocks_next_write(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, tenant_count=1)
    store = RecordingBatchStore()
    engine = TenantDatabaseEngine.from_dataset_dir(
        dataset,
        wal_committer=BatchedWalCommitter(store, config=WalBatchingConfig(batch_window_ms=0)),
    )
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])

    def fault(point, connection):
        if point == WalCommitFaultPoint.AFTER_REMOTE_PERSIST:
            raise InjectedWalFaultError("stored but not applied")

    try:
        first = engine.execute(operation, wal_fault_hook=fault)
        second = engine.execute(
            replace(operation, payload=replace(operation.payload, transaction_id="second", delta=2))
        )
        assert first.remote_durable and first.reconciliation_required
        assert not second.success
        assert second.reconciliation_required
        assert len(store.batches) == 1
    finally:
        engine.close_all()


def test_conflicting_retry_is_rejected(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, tenant_count=1)
    store = RecordingBatchStore()
    engine = TenantDatabaseEngine.from_dataset_dir(
        dataset,
        wal_committer=BatchedWalCommitter(store, config=WalBatchingConfig(batch_window_ms=0)),
    )
    operation = _entity_delta(dataset, _tenant_ids(dataset)[0])
    try:
        assert engine.execute(operation).success
        result = engine.execute(replace(operation, payload=replace(operation.payload, delta=99)))
        assert not result.success
        assert result.conflict
        assert len(store.batches) == 1
    finally:
        engine.close_all()
