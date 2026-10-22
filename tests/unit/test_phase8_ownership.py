from __future__ import annotations

import hashlib
import multiprocessing
import sqlite3
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from test_wal_batching import _dataset, _entity_delta, _record, _tenant_db_path, _tenant_ids

from ephemeraldb.checkpoint.coordinator import CheckpointCoordinator
from ephemeraldb.checkpoint.store import FileObjectStore
from ephemeraldb.core.errors import OwnershipError, RecoveryError, StaleOwnerError
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.executor import WalCommitFaultPoint
from ephemeraldb.engine.results import DurabilityOutcome, EngineErrorCode
from ephemeraldb.loadgen.models import (
    EntityLookupPayload,
    MultiEntityTransactionItem,
    MultiEntityTransactionPayload,
    OperationType,
)
from ephemeraldb.ownership.coordinator import FencedCommitter, OwnershipCoordinator
from ephemeraldb.ownership.models import CommitDecision
from ephemeraldb.ownership.recovery import (
    AuthorityCandidateBatchStore,
    AuthorityCandidateWalStore,
    AuthorityPayloadReader,
    FencedRecovery,
    copy_bootstrap,
)
from ephemeraldb.ownership.sqlite_store import SQLiteAuthorityStore
from ephemeraldb.ownership.store import CasResult
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig
from ephemeraldb.wal.checksum import attach_record_checksum
from ephemeraldb.wal.durability import SingleRecordWalCommitter, WalCommitReceipt
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.sequence import current_sequence_state


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _owned_record(
    tenant: str,
    sequence: int,
    epoch: int,
    previous: str | None = None,
    *,
    tx: str | None = None,
) -> WalRecord:
    base = _record(tenant, sequence)
    identity = f"{tenant}:{tx or base.transaction_id}".encode()
    fingerprint = "v1:" + hashlib.sha256(identity).hexdigest()
    return attach_record_checksum(
        replace(
            base,
            wal_format_version=2,
            transaction_id=tx or base.transaction_id,
            owner_epoch=epoch,
            previous_record_checksum=previous,
            request_fingerprint=fingerprint,
        )
    )


def _bootstrap(
    tmp_path: Path, tenant: str = "tenant-a", session: str = "runtime-a-session"
) -> tuple[SQLiteAuthorityStore, OwnershipCoordinator, object]:
    store = SQLiteAuthorityStore(tmp_path / "authority.sqlite")
    coordinator = OwnershipCoordinator(store, "run-phase8")
    state = coordinator.bootstrap(
        tenant,
        session_id=session,
        bootstrap_key="bootstrap/tenant.db",
        bootstrap_checksum="0" * 64,
        transition_id="bootstrap-op",
    )
    return store, coordinator, state


def _receipt(record: WalRecord, key: str = "candidate.json") -> WalCommitReceipt:
    assert record.record_checksum is not None
    return WalCommitReceipt(
        bucket="offline",
        key=key,
        etag=None,
        version_id=None,
        checksum=record.record_checksum,
        bytes_written=100,
        duration_ns=1,
        remote_durable=True,
    )


def test_bootstrap_is_conditional_and_runtime_names_do_not_reuse_sessions(tmp_path: Path) -> None:
    _store, coordinator, state = _bootstrap(tmp_path)
    with pytest.raises(OwnershipError, match="already exists"):
        coordinator.bootstrap(
            "tenant-a",
            session_id="other-session",
            bootstrap_key="other.db",
            bootstrap_checksum="1" * 64,
        )
    successor = coordinator.takeover("tenant-a", session_id="runtime-a-new-session")
    assert successor.owner_epoch == state.owner_epoch + 1
    assert successor.revision == state.revision + 1
    assert successor.owner_session != state.owner_session


def test_pause_after_owner_check_then_takeover_rejects_old_commit(tmp_path: Path) -> None:
    _store, coordinator, state = _bootstrap(tmp_path)
    observed = coordinator.status("tenant-a")
    record = _owned_record("tenant-a", 1, state.owner_epoch)
    coordinator.takeover("tenant-a", session_id="runtime-b-session")

    with pytest.raises(StaleOwnerError, match="takeover won"):
        coordinator.commit(
            state.token,
            record,
            _receipt(record),
            attempt_id="attempt-a",
            observed=observed,
        )

    latest, _ = coordinator.status("tenant-a")
    assert latest.committed_sequence == 0
    assert coordinator.accepted_history(latest) == ()


def test_commit_before_takeover_is_preserved_and_retry_resolves(tmp_path: Path) -> None:
    _store, coordinator, state = _bootstrap(tmp_path)
    record = _owned_record("tenant-a", 1, state.owner_epoch)
    first = coordinator.commit(state.token, record, _receipt(record), attempt_id="attempt-a")
    successor = coordinator.takeover("tenant-a", session_id="runtime-b-session")

    assert first.decision is CommitDecision.ACCEPTED
    assert successor.committed_sequence == 1
    assert coordinator.accepted_history(successor)[0].record_checksum == record.record_checksum
    retry = coordinator.commit(state.token, record, _receipt(record), attempt_id="attempt-a")
    assert retry.decision is CommitDecision.ALREADY_ACCEPTED


class _LostResponseStore(SQLiteAuthorityStore):
    lose_next_cas = False

    def compare_and_swap(self, key: str, expected_validator: str, payload: bytes) -> CasResult:
        result = super().compare_and_swap(key, expected_validator, payload)
        if self.lose_next_cas and result is CasResult.APPLIED:
            self.lose_next_cas = False
            return CasResult.UNKNOWN
        return result


def test_successful_cas_response_loss_resolves_from_history(tmp_path: Path) -> None:
    store = _LostResponseStore(tmp_path / "authority.sqlite")
    coordinator = OwnershipCoordinator(store, "lost-response")
    state = coordinator.bootstrap(
        "tenant-a",
        session_id="runtime-a",
        bootstrap_key="bootstrap.db",
        bootstrap_checksum="0" * 64,
    )
    record = _owned_record("tenant-a", 1, state.owner_epoch)
    store.lose_next_cas = True
    outcome = coordinator.commit(state.token, record, _receipt(record), attempt_id="attempt-a")
    assert outcome.decision is CommitDecision.ALREADY_ACCEPTED
    assert coordinator.status("tenant-a")[0].committed_sequence == 1


def test_missing_or_corrupt_required_descriptor_fails_closed(tmp_path: Path) -> None:
    store, coordinator, state = _bootstrap(tmp_path)
    record = _owned_record("tenant-a", 1, state.owner_epoch)
    outcome = coordinator.commit(state.token, record, _receipt(record), attempt_id="attempt-a")
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE immutable_objects SET payload=? WHERE object_key=?",
            (b"{}", outcome.descriptor_key),
        )
    with pytest.raises(OwnershipError, match="checksum mismatch"):
        coordinator.accepted_history(coordinator.status("tenant-a")[0])


def test_checkpoint_registration_accepts_older_prefix_and_fences_stale_owner(
    tmp_path: Path,
) -> None:
    _store, coordinator, state = _bootstrap(tmp_path)
    first = _owned_record("tenant-a", 1, state.owner_epoch)
    coordinator.commit(state.token, first, _receipt(first), attempt_id="attempt-a")
    after_first, _ = coordinator.status("tenant-a")
    second = _owned_record("tenant-a", 2, state.owner_epoch, first.record_checksum)
    coordinator.commit(state.token, second, _receipt(second), attempt_id="attempt-b")
    registered = coordinator.register_checkpoint(
        state.token,
        checkpoint_key="checkpoints/one.db",
        checkpoint_checksum="2" * 64,
        checkpoint_sequence=1,
        descriptor_key=after_first.descriptor_head_key,
        descriptor_checksum=after_first.descriptor_head_checksum,
    )
    assert registered.checkpoint_sequence == 1
    assert registered.committed_sequence == 2
    coordinator.takeover("tenant-a", session_id="runtime-b")
    with pytest.raises(StaleOwnerError):
        coordinator.register_checkpoint(
            state.token,
            checkpoint_key="stale.db",
            checkpoint_checksum="3" * 64,
            checkpoint_sequence=1,
            descriptor_key=after_first.descriptor_head_key,
            descriptor_checksum=after_first.descriptor_head_checksum,
        )


def test_shared_batch_allows_one_tenant_acceptance_and_one_fence(tmp_path: Path) -> None:
    store = SQLiteAuthorityStore(tmp_path / "authority.sqlite")
    coordinator = OwnershipCoordinator(store, "batch-run")
    states = {
        tenant: coordinator.bootstrap(
            tenant,
            session_id=f"owner-{tenant}",
            bootstrap_key=f"{tenant}.db",
            bootstrap_checksum="0" * 64,
        )
        for tenant in ("tenant-a", "tenant-b")
    }
    batcher = BatchedWalCommitter(
        AuthorityCandidateBatchStore(store, "batch-run"),
        config=WalBatchingConfig(batch_window_ms=1000, max_records_per_batch=2),
    )
    records = {
        tenant: _owned_record(tenant, 1, states[tenant].owner_epoch)
        for tenant in states
    }
    from concurrent.futures import ThreadPoolExecutor

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            barrier = threading.Barrier(2)

            def commit(record: WalRecord) -> WalCommitReceipt:
                barrier.wait()
                return batcher.commit(record)

            futures = {
                tenant: pool.submit(commit, record)
                for tenant, record in records.items()
            }
            receipts = {tenant: future.result() for tenant, future in futures.items()}
    finally:
        batcher.close()
    assert receipts["tenant-a"].key == receipts["tenant-b"].key
    coordinator.takeover("tenant-b", session_id="owner-b-new")
    accepted = coordinator.commit(
        states["tenant-a"].token,
        records["tenant-a"],
        receipts["tenant-a"],
        attempt_id="attempt-a",
    )
    with pytest.raises(StaleOwnerError):
        coordinator.commit(
            states["tenant-b"].token,
            records["tenant-b"],
            receipts["tenant-b"],
            attempt_id="attempt-b",
        )
    assert accepted.decision is CommitDecision.ACCEPTED
    assert coordinator.status("tenant-b")[0].committed_sequence == 0


def test_integrated_engine_fences_reads_and_recovers_without_seal(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path / "a", tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    database = _tenant_db_path(dataset, tenant)
    bootstrap_root = tmp_path / "remote-bootstrap"
    bootstrap_root.mkdir()
    bootstrap = bootstrap_root / "genesis.db"
    bootstrap.write_bytes(database.read_bytes())
    store = SQLiteAuthorityStore(tmp_path / "remote" / "authority.sqlite")
    coordinator = OwnershipCoordinator(store, "engine-run")
    state_a = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key="genesis.db",
        bootstrap_checksum=_sha(bootstrap),
    )
    payload_store = AuthorityCandidateWalStore(store, "engine-run")
    fenced_a = FencedCommitter(
        SingleRecordWalCommitter(payload_store), coordinator, {tenant: state_a.token}
    )
    engine_a = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=fenced_a)
    operation = _entity_delta(dataset, tenant)
    first = engine_a.execute(operation)
    assert first.success
    assert first.durability_outcome is DurabilityOutcome.COMMIT_ACCEPTED_APPLIED
    assert first.authority_decision == "ACCEPTED"

    state_b = coordinator.takeover(tenant, session_id="runtime-b")
    stale_read = engine_a.execute(
        replace(
            operation,
            operation_id="stale-read",
            operation_type=OperationType.ENTITY_LOOKUP,
            payload=EntityLookupPayload(entity_id=operation.payload.entity_id),
        )
    )
    assert not stale_read.success
    assert stale_read.error_code is EngineErrorCode.FENCED
    engine_a.close_all()

    recovered_dataset = tmp_path / "b" / "dataset"
    recovered_dataset.parent.mkdir(parents=True)
    # Preserve manifest and immutable generated support files, but not A's database cache.
    import shutil

    shutil.copytree(dataset, recovered_dataset)
    recovered_db = _tenant_db_path(recovered_dataset, tenant)
    recovered_db.unlink()
    result = FencedRecovery(coordinator, AuthorityPayloadReader(store)).recover(
        state_b.token,
        recovered_db,
        install_bootstrap=copy_bootstrap(bootstrap_root),
    )
    assert result.committed_sequence == 1
    fenced_b = FencedCommitter(
        SingleRecordWalCommitter(payload_store), coordinator, {tenant: state_b.token}
    )
    engine_b = TenantDatabaseEngine.from_dataset_dir(recovered_dataset, wal_committer=fenced_b)
    try:
        retry = engine_b.execute(operation)
        assert retry.success and retry.duplicate_transaction
        conflict = engine_b.execute(
            replace(operation, payload=replace(operation.payload, delta=999))
        )
        assert not conflict.success and conflict.error_code is EngineErrorCode.TRANSACTION_CONFLICT
        next_operation = replace(
            operation,
            operation_id="next-operation",
            payload=replace(operation.payload, transaction_id="next-transaction"),
        )
        following = engine_b.execute(next_operation)
        assert following.success and following.wal_sequence_no == 2
    finally:
        engine_b.close_all()


def test_recovery_losing_ownership_before_activation_preserves_destination(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path / "source", tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    source_db = _tenant_db_path(dataset, tenant)
    bootstrap_root = tmp_path / "bootstrap"
    bootstrap_root.mkdir()
    bootstrap = bootstrap_root / "genesis.db"
    bootstrap.write_bytes(source_db.read_bytes())
    store = SQLiteAuthorityStore(tmp_path / "authority.sqlite")
    coordinator = OwnershipCoordinator(store, "activation-run")
    state = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key="genesis.db",
        bootstrap_checksum=_sha(bootstrap),
    )
    destination = tmp_path / "existing.db"
    destination.write_bytes(b"preserve-me")

    def lose() -> None:
        coordinator.takeover(tenant, session_id="runtime-b")

    with pytest.raises(RecoveryError, match="changed before activation"):
        FencedRecovery(coordinator, AuthorityPayloadReader(store)).recover(
            state.token,
            destination,
            install_bootstrap=copy_bootstrap(bootstrap_root),
            before_activation=lose,
        )
    assert destination.read_bytes() == b"preserve-me"
    assert not tuple(tmp_path.glob(".existing.db.phase8-*.tmp"))

    owner_c = coordinator.takeover(tenant, session_id="runtime-c")
    completed = FencedRecovery(coordinator, AuthorityPayloadReader(store)).recover(
        owner_c.token,
        destination,
        install_bootstrap=copy_bootstrap(bootstrap_root),
    )
    assert completed.epoch == 3
    assert completed.committed_sequence == 0


def test_accepted_local_apply_failure_blocks_continuation(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path / "dataset", tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    database = _tenant_db_path(dataset, tenant)
    store = SQLiteAuthorityStore(tmp_path / "authority.sqlite")
    coordinator = OwnershipCoordinator(store, "local-failure")
    state = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key="genesis.db",
        bootstrap_checksum=_sha(database),
    )
    committer = FencedCommitter(
        SingleRecordWalCommitter(AuthorityCandidateWalStore(store, "local-failure")),
        coordinator,
        {tenant: state.token},
    )
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=committer)

    def fail(point: WalCommitFaultPoint, _connection: sqlite3.Connection) -> None:
        if point is WalCommitFaultPoint.AFTER_REMOTE_PERSIST:
            raise RuntimeError("local apply boundary failed")

    try:
        failed = engine.execute(_entity_delta(dataset, tenant), wal_fault_hook=fail)
        assert not failed.success
        assert failed.durability_outcome is DurabilityOutcome.COMMIT_ACCEPTED_NOT_APPLIED
        blocked = engine.execute(_entity_delta(dataset, tenant))
        assert not blocked.success
        assert blocked.error_code is EngineErrorCode.AUTHORITY_UNAVAILABLE
    finally:
        engine.close_all()


def test_fenced_multi_entity_write_and_conflicting_retry(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path / "dataset", tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    database = _tenant_db_path(dataset, tenant)
    with sqlite3.connect(database) as connection:
        entity_ids = [
            str(row[0])
            for row in connection.execute(
                "SELECT entity_id FROM entities ORDER BY entity_id LIMIT 2"
            )
        ]
    store = SQLiteAuthorityStore(tmp_path / "authority.sqlite")
    coordinator = OwnershipCoordinator(store, "multi-run")
    state = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key="genesis.db",
        bootstrap_checksum=_sha(database),
    )
    committer = FencedCommitter(
        SingleRecordWalCommitter(AuthorityCandidateWalStore(store, "multi-run")),
        coordinator,
        {tenant: state.token},
    )
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=committer)
    base = _entity_delta(dataset, tenant)
    operation = replace(
        base,
        operation_type=OperationType.MULTI_ENTITY_TRANSACTION,
        payload=MultiEntityTransactionPayload(
            transaction_id="multi-tx",
            items=tuple(
                MultiEntityTransactionItem(entity_id=entity_id, delta=index + 1)
                for index, entity_id in enumerate(entity_ids)
            ),
        ),
    )
    try:
        result = engine.execute(operation)
        assert result.success and result.wal_sequence_no == 1
        duplicate = engine.execute(operation)
        assert duplicate.success and duplicate.duplicate_transaction
        conflicting = engine.execute(
            replace(
                operation,
                payload=replace(
                    operation.payload,
                    items=(
                        replace(operation.payload.items[0], delta=99),
                        operation.payload.items[1],
                    ),
                ),
            )
        )
        assert not conflicting.success
        assert conflicting.error_code is EngineErrorCode.TRANSACTION_CONFLICT
    finally:
        engine.close_all()


def test_ordinary_checkpoint_registers_accepted_fenced_prefix(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path / "dataset", tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    database = _tenant_db_path(dataset, tenant)
    authority_store = SQLiteAuthorityStore(tmp_path / "authority.sqlite")
    coordinator = OwnershipCoordinator(authority_store, "checkpoint-run")
    state = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key="genesis.db",
        bootstrap_checksum=_sha(database),
    )
    committer = FencedCommitter(
        SingleRecordWalCommitter(
            AuthorityCandidateWalStore(authority_store, "checkpoint-run")
        ),
        coordinator,
        {tenant: state.token},
    )
    engine = TenantDatabaseEngine.from_dataset_dir(dataset, wal_committer=committer)
    checkpoints = CheckpointCoordinator(
        FileObjectStore(tmp_path / "checkpoint-objects"), "checkpoint-run"
    )
    try:
        assert engine.execute(_entity_delta(dataset, tenant)).success
        published = checkpoints.create(engine, tenant)
        control, _ = coordinator.status(tenant)
        assert published["authority_checkpoint_sequence"] == 1
        assert control.checkpoint_sequence == 1
        assert control.bootstrap_key.endswith("snapshot.db")
        assert len(control.bootstrap_checksum) == 64
    finally:
        engine.close_all()


def _takeover_worker(path: str, start: Any, output: Any) -> None:
    coordinator = OwnershipCoordinator(SQLiteAuthorityStore(Path(path)), "process-run")
    start.wait(10)
    try:
        session = f"child-{multiprocessing.current_process().name}"
        state = coordinator.takeover("tenant-a", session_id=session)
        output.put((state.owner_epoch, state.owner_session))
    except Exception as exc:  # pragma: no cover - child diagnostic path
        output.put(("error", str(exc)))


def _interrupted_recovery_worker(
    authority_path: str,
    bootstrap_root: str,
    token: object,
    destination: str,
    reached_activation: Any,
    release: Any,
) -> None:
    coordinator = OwnershipCoordinator(
        SQLiteAuthorityStore(Path(authority_path)), "interrupted-run"
    )

    def pause() -> None:
        reached_activation.set()
        release.wait(30)

    try:
        FencedRecovery(
            coordinator,
            AuthorityPayloadReader(SQLiteAuthorityStore(Path(authority_path))),
        ).recover(
            token,  # type: ignore[arg-type]
            Path(destination),
            install_bootstrap=copy_bootstrap(Path(bootstrap_root)),
            before_activation=pause,
        )
    except BaseException:
        return


def test_multiprocess_takeovers_serialize_distinct_epochs(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    path = tmp_path / "authority.sqlite"
    coordinator = OwnershipCoordinator(SQLiteAuthorityStore(path), "process-run")
    coordinator.bootstrap(
        "tenant-a",
        session_id="parent",
        bootstrap_key="bootstrap.db",
        bootstrap_checksum="0" * 64,
    )
    start = context.Event()
    output = context.Queue()
    children = [
        context.Process(target=_takeover_worker, args=(str(path), start, output))
        for _ in range(2)
    ]
    for child in children:
        child.start()
    start.set()
    results = [output.get(timeout=20) for _ in children]
    for child in children:
        child.join(20)
        assert child.exitcode == 0
    assert not [result for result in results if result[0] == "error"], results
    assert {result[0] for result in results} == {2, 3}
    assert len({result[1] for result in results}) == 2


def test_successor_process_killed_during_recovery_then_later_owner_completes(
    tmp_path: Path,
) -> None:
    dataset = _dataset(tmp_path / "dataset", tenant_count=1)
    tenant = _tenant_ids(dataset)[0]
    source = _tenant_db_path(dataset, tenant)
    bootstrap_root = tmp_path / "bootstrap"
    bootstrap_root.mkdir()
    bootstrap = bootstrap_root / "genesis.db"
    bootstrap.write_bytes(source.read_bytes())
    authority_path = tmp_path / "authority.sqlite"
    coordinator = OwnershipCoordinator(SQLiteAuthorityStore(authority_path), "interrupted-run")
    owner_a = coordinator.bootstrap(
        tenant,
        session_id="runtime-a",
        bootstrap_key="genesis.db",
        bootstrap_checksum=_sha(bootstrap),
    )
    owner_b = coordinator.takeover(tenant, session_id="runtime-b")
    assert owner_b.owner_epoch == owner_a.owner_epoch + 1
    destination = tmp_path / "destination.db"
    destination.write_bytes(b"existing-destination")
    context = multiprocessing.get_context("spawn")
    reached = context.Event()
    release = context.Event()
    process = context.Process(
        target=_interrupted_recovery_worker,
        args=(
            str(authority_path),
            str(bootstrap_root),
            owner_b.token,
            str(destination),
            reached,
            release,
        ),
    )
    process.start()
    try:
        assert reached.wait(20)
        process.terminate()
        process.join(20)
        assert not process.is_alive()
        assert destination.read_bytes() == b"existing-destination"
        owner_c = coordinator.takeover(tenant, session_id="runtime-c")
        result = FencedRecovery(
            coordinator, AuthorityPayloadReader(SQLiteAuthorityStore(authority_path))
        ).recover(
            owner_c.token,
            destination,
            install_bootstrap=copy_bootstrap(bootstrap_root),
        )
        assert result.epoch == 3
        with sqlite3.connect(destination) as connection:
            assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert current_sequence_state(connection) == (0, None)
        assert not tuple(tmp_path.glob(".destination.db.phase8-*.tmp"))
    finally:
        if process.is_alive():
            release.set()
            process.terminate()
            process.join(10)
