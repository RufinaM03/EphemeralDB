"""Execution of Phase 2 workload operations against local SQLite tenant DBs."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum

from ephemeraldb.core.errors import (
    InjectedWalFaultError,
    LocalApplyAfterDurabilityError,
    OwnershipError,
    PersistenceRejectedError,
    RemoteDurabilityError,
    RemoteWalConflictError,
    StaleOwnerError,
    TransactionConflictError,
)
from ephemeraldb.core.models import TenantId
from ephemeraldb.engine.connection import ConnectionManager
from ephemeraldb.engine.durability_guard import DurabilityGuard, TenantNotReadyError
from ephemeraldb.engine.registry import TenantRegistryError
from ephemeraldb.engine.results import DurabilityOutcome, EngineErrorCode, ExecutionResult
from ephemeraldb.loadgen.models import (
    AggregatePayload,
    ComplexJoinPayload,
    EntityDeltaPayload,
    EntityHistoryPayload,
    EntityLookupPayload,
    MetadataLookupPayload,
    MultiEntityTransactionPayload,
    OperationType,
    RelationshipExpansionPayload,
    StateScanPayload,
    WorkloadOperation,
)
from ephemeraldb.ownership.coordinator import FencedCommitter
from ephemeraldb.schema.generator import deterministic_uuid, mutation_timestamp
from ephemeraldb.schema.queries import QUERY_CATALOG
from ephemeraldb.wal.checksum import attach_record_checksum
from ephemeraldb.wal.durability import ConfirmedDurabilityError, WalCommitReceipt, WalCommitter
from ephemeraldb.wal.identity import check_identity, record_identity, request_fingerprint
from ephemeraldb.wal.models import (
    LOCAL_OWNER_EPOCH,
    WAL_FORMAT_VERSION,
    WalMutation,
    WalMutationType,
    WalRecord,
    WalScalar,
)
from ephemeraldb.wal.sequence import (
    advance_sequence,
    allocate_next_sequence,
    ensure_wal_state_table,
    record_transaction_wal,
    transaction_wal_state,
)


class WalCommitFaultPoint(StrEnum):
    AFTER_PREPARE = "AFTER_PREPARE"
    AFTER_REMOTE_PERSIST = "AFTER_REMOTE_PERSIST"
    AFTER_LOCAL_APPLY = "AFTER_LOCAL_APPLY"
    BEFORE_ACK = "BEFORE_ACK"


class OperationExecutor:
    def __init__(
        self,
        connection_manager: ConnectionManager,
        *,
        wal_committer: WalCommitter | None = None,
        initialize_durability: bool = False,
    ) -> None:
        self.connection_manager = connection_manager
        self.wal_committer = wal_committer
        self.guard: DurabilityGuard | FencedCommitter | None = (
            wal_committer
            if isinstance(wal_committer, FencedCommitter)
            else None
            if wal_committer is None
            else DurabilityGuard(
                connection_manager.config.database_root,
                wal_committer,
                initialize=initialize_durability,
            )
        )
        self._locks_guard = threading.Lock()
        self._tenant_locks: dict[str, threading.RLock] = {}

    def execute(
        self,
        operation: WorkloadOperation,
        *,
        before_transaction_insert_hook: Callable[[sqlite3.Connection], None] | None = None,
        wal_fault_hook: Callable[[WalCommitFaultPoint, sqlite3.Connection], None] | None = None,
    ) -> ExecutionResult:
        started_at = time.perf_counter_ns()
        try:
            tenant_id = TenantId(operation.tenant_id)
            with self._lock_for(tenant_id.value):
                tenant_wait = time.perf_counter_ns() - started_at
                with self.connection_manager.connection_for(tenant_id) as connection:
                    if self.guard is not None:
                        self.guard.require_ready(tenant_id.value, connection)
                    result = self._execute_with_connection(
                        operation,
                        connection,
                        started_at,
                        before_transaction_insert_hook=before_transaction_insert_hook,
                        wal_fault_hook=wal_fault_hook,
                    )
                    state = "READY" if self.guard is None else self.guard.state(tenant_id.value)
                    outcome = result.durability_outcome
                    if result.authority_decision in {"ACCEPTED", "ALREADY_ACCEPTED"}:
                        outcome = (
                            DurabilityOutcome.COMMIT_ACCEPTED_APPLIED
                            if not result.reconciliation_required
                            else DurabilityOutcome.COMMIT_ACCEPTED_NOT_APPLIED
                        )
                    elif result.remote_durable:
                        outcome = (
                            DurabilityOutcome.DURABLE_APPLIED
                            if not result.reconciliation_required
                            else DurabilityOutcome.DURABLE_NOT_APPLIED
                        )
                    elif state != "READY":
                        outcome = DurabilityOutcome.UNKNOWN
                    elif result.success and result.transaction_id is not None:
                        outcome = DurabilityOutcome.LOCAL_APPLIED
                    return replace(
                        result,
                        tenant_state=state,
                        durability_outcome=outcome,
                        reconciliation_required=result.reconciliation_required or state != "READY",
                        tenant_wait_latency_ns=tenant_wait,
                    )
        except TenantNotReadyError as exc:
            return replace(
                _finish_error(
                    operation,
                    started_at,
                    EngineErrorCode.TENANT_NOT_READY,
                    str(exc),
                    reconciliation_required=True,
                ),
                tenant_state="FAILED"
                if self.guard is not None and operation.tenant_id in self.guard._failed
                else "RECONCILING",
                durability_outcome=DurabilityOutcome.NOT_ATTEMPTED,
            )
        except StaleOwnerError as exc:
            return replace(
                _finish_error(
                    operation,
                    started_at,
                    EngineErrorCode.FENCED,
                    str(exc),
                    reconciliation_required=True,
                ),
                tenant_state="FENCED",
                durability_outcome=DurabilityOutcome.FENCED,
            )
        except OwnershipError as exc:
            return replace(
                _finish_error(
                    operation,
                    started_at,
                    EngineErrorCode.AUTHORITY_UNAVAILABLE,
                    str(exc),
                    reconciliation_required=True,
                ),
                tenant_state="FAILED",
                durability_outcome=DurabilityOutcome.UNKNOWN,
            )
        except TransactionConflictError as exc:
            return _finish_error(
                operation, started_at, EngineErrorCode.TRANSACTION_CONFLICT, str(exc), conflict=True
            )
        except TenantRegistryError as exc:
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.TENANT_NOT_FOUND,
                str(exc),
            )
        except sqlite3.OperationalError as exc:
            return _finish_error(
                operation,
                started_at,
                _operational_error_code(exc),
                _safe_error_message(exc),
            )
        except sqlite3.IntegrityError as exc:
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.INTEGRITY_ERROR,
                _safe_error_message(exc),
            )
        except Exception as exc:
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.INTERNAL_DATABASE_ERROR,
                _safe_error_message(exc),
            )

    def _execute_with_connection(
        self,
        operation: WorkloadOperation,
        connection: sqlite3.Connection,
        started_at: int,
        *,
        before_transaction_insert_hook: Callable[[sqlite3.Connection], None] | None,
        wal_fault_hook: Callable[[WalCommitFaultPoint, sqlite3.Connection], None] | None,
    ) -> ExecutionResult:
        payload = operation.payload
        if operation.operation_type == OperationType.ENTITY_LOOKUP:
            assert isinstance(payload, EntityLookupPayload)
            return self._read_query(
                operation,
                connection,
                started_at,
                "Q1_ENTITY_LOOKUP",
                {"tenant_id": operation.tenant_id, "entity_id": payload.entity_id},
            )
        if operation.operation_type == OperationType.STATE_SCAN:
            assert isinstance(payload, StateScanPayload)
            return self._read_query(
                operation,
                connection,
                started_at,
                "Q2_STATE_SCAN",
                {
                    "tenant_id": operation.tenant_id,
                    "state": payload.state,
                    "limit": payload.limit,
                },
            )
        if operation.operation_type == OperationType.RELATIONSHIP_EXPANSION:
            assert isinstance(payload, RelationshipExpansionPayload)
            return self._read_query(
                operation,
                connection,
                started_at,
                "Q3_RELATIONSHIP_EXPANSION",
                {
                    "tenant_id": operation.tenant_id,
                    "entity_id": payload.source_entity_id,
                    "limit": payload.limit,
                },
            )
        if operation.operation_type == OperationType.METADATA_LOOKUP:
            assert isinstance(payload, MetadataLookupPayload)
            return self._metadata_lookup(operation, connection, started_at, payload)
        if operation.operation_type == OperationType.ENTITY_HISTORY:
            assert isinstance(payload, EntityHistoryPayload)
            return self._read_query(
                operation,
                connection,
                started_at,
                "Q5_ENTITY_HISTORY",
                {
                    "tenant_id": operation.tenant_id,
                    "entity_id": payload.entity_id,
                    "limit": payload.limit,
                },
            )
        if operation.operation_type == OperationType.COMPLEX_JOIN:
            assert isinstance(payload, ComplexJoinPayload)
            return self._read_query(
                operation,
                connection,
                started_at,
                "Q6_COMPLEX_JOIN",
                {
                    "tenant_id": operation.tenant_id,
                    "source_state": payload.source_state,
                    "relationship_type": payload.relationship_type,
                    "metadata_key": payload.metadata_key,
                    "limit": payload.limit,
                },
            )
        if operation.operation_type == OperationType.AGGREGATE_BY_STATE:
            assert isinstance(payload, AggregatePayload)
            return self._aggregate(operation, connection, started_at, payload)
        if operation.operation_type == OperationType.ENTITY_DELTA:
            assert isinstance(payload, EntityDeltaPayload)
            return self._entity_delta(
                operation,
                connection,
                started_at,
                payload,
                before_transaction_insert_hook=before_transaction_insert_hook,
                wal_fault_hook=wal_fault_hook,
            )
        if operation.operation_type == OperationType.MULTI_ENTITY_TRANSACTION:
            assert isinstance(payload, MultiEntityTransactionPayload)
            return self._multi_entity_transaction(
                operation,
                connection,
                started_at,
                payload,
                before_transaction_insert_hook=before_transaction_insert_hook,
                wal_fault_hook=wal_fault_hook,
            )

    def _lock_for(self, tenant_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._tenant_locks.get(tenant_id)
            if lock is None:
                lock = threading.RLock()
                self._tenant_locks[tenant_id] = lock
            return lock

    def _read_query(
        self,
        operation: WorkloadOperation,
        connection: sqlite3.Connection,
        started_at: int,
        query_name: str,
        parameters: dict[str, object],
    ) -> ExecutionResult:
        rows = _rows_to_dicts(connection.execute(QUERY_CATALOG[query_name], parameters).fetchall())
        return _finish_success(operation, started_at, rows_read=len(rows), records=tuple(rows))

    def _metadata_lookup(
        self,
        operation: WorkloadOperation,
        connection: sqlite3.Connection,
        started_at: int,
        payload: MetadataLookupPayload,
    ) -> ExecutionResult:
        if payload.metadata_key is None:
            rows = _rows_to_dicts(
                connection.execute(
                    QUERY_CATALOG["Q4_METADATA_LOOKUP"],
                    {"tenant_id": operation.tenant_id, "entity_id": payload.entity_id},
                ).fetchall()
            )
        else:
            rows = _rows_to_dicts(
                connection.execute(
                    """
                    SELECT metadata_key, metadata_value, updated_at
                    FROM metadata
                    WHERE tenant_id = :tenant_id
                      AND entity_id = :entity_id
                      AND metadata_key = :metadata_key
                    ORDER BY metadata_key
                    """,
                    {
                        "tenant_id": operation.tenant_id,
                        "entity_id": payload.entity_id,
                        "metadata_key": payload.metadata_key,
                    },
                ).fetchall()
            )
        return _finish_success(operation, started_at, rows_read=len(rows), records=tuple(rows))

    def _aggregate(
        self,
        operation: WorkloadOperation,
        connection: sqlite3.Connection,
        started_at: int,
        payload: AggregatePayload,
    ) -> ExecutionResult:
        if payload.state_filter is None:
            rows = _rows_to_dicts(
                connection.execute(
                    QUERY_CATALOG["Q7_AGGREGATE"],
                    {"tenant_id": operation.tenant_id},
                ).fetchall()
            )
        else:
            rows = _rows_to_dicts(
                connection.execute(
                    """
                    SELECT state, COUNT(*) AS entity_count
                    FROM entities
                    WHERE tenant_id = :tenant_id
                      AND state = :state
                    GROUP BY state
                    ORDER BY state
                    """,
                    {"tenant_id": operation.tenant_id, "state": payload.state_filter},
                ).fetchall()
            )
        return _finish_success(operation, started_at, rows_read=len(rows), records=tuple(rows))

    def _entity_delta(
        self,
        operation: WorkloadOperation,
        connection: sqlite3.Connection,
        started_at: int,
        payload: EntityDeltaPayload,
        *,
        before_transaction_insert_hook: Callable[[sqlite3.Connection], None] | None,
        wal_fault_hook: Callable[[WalCommitFaultPoint, sqlite3.Connection], None] | None,
    ) -> ExecutionResult:
        duplicate = _committed_transaction_exists(connection, payload.transaction_id)
        if duplicate:
            check_identity(
                connection,
                operation.tenant_id,
                payload.transaction_id,
                request_fingerprint(operation),
            )
            wal_state = transaction_wal_state(
                connection,
                tenant_id=operation.tenant_id,
                transaction_id=payload.transaction_id,
            )
            return _finish_duplicate(
                operation,
                started_at,
                transaction_id=payload.transaction_id,
                wal_state=wal_state,
            )
        return self._execute_write_items(
            operation,
            connection,
            started_at,
            transaction_id=payload.transaction_id,
            rows_read=1,
            items=((payload.entity_id, payload.delta, payload.expected_version),),
            before_transaction_insert_hook=before_transaction_insert_hook,
            wal_fault_hook=wal_fault_hook,
        )

    def _execute_write_items(
        self,
        operation: WorkloadOperation,
        connection: sqlite3.Connection,
        started_at: int,
        *,
        transaction_id: str,
        rows_read: int,
        items: tuple[tuple[str, int, int | None], ...],
        before_transaction_insert_hook: Callable[[sqlite3.Connection], None] | None,
        wal_fault_hook: Callable[[WalCommitFaultPoint, sqlite3.Connection], None] | None,
    ) -> ExecutionResult:
        receipt: WalCommitReceipt | None = None
        prepared: _PreparedItems | None = None
        local_apply_latency_ns: int | None = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            prepared = _prepare_items(
                connection,
                operation.tenant_id,
                transaction_id,
                items,
                operation_type=operation.operation_type,
                wal_enabled=self.wal_committer is not None,
                fingerprint=request_fingerprint(operation),
                owner_epoch=(
                    LOCAL_OWNER_EPOCH
                    if not isinstance(self.wal_committer, FencedCommitter)
                    else self.wal_committer.owner_epoch(operation.tenant_id)
                ),
            )
            _run_fault_hook(wal_fault_hook, WalCommitFaultPoint.AFTER_PREPARE, connection)

            if prepared.wal_record is not None:
                assert self.wal_committer is not None
                assert self.guard is not None
                receipt = self.guard.commit(prepared.wal_record)
                _run_fault_hook(
                    wal_fault_hook,
                    WalCommitFaultPoint.AFTER_REMOTE_PERSIST,
                    connection,
                )

            local_apply_started_at = time.perf_counter_ns()
            _apply_prepared_items(
                connection,
                prepared,
                receipt=receipt,
                before_transaction_insert_hook=before_transaction_insert_hook,
            )
            local_apply_latency_ns = time.perf_counter_ns() - local_apply_started_at
            if receipt is not None:
                _run_fault_hook(
                    wal_fault_hook,
                    WalCommitFaultPoint.AFTER_LOCAL_APPLY,
                    connection,
                )

            connection.commit()
            local_apply_latency_ns = time.perf_counter_ns() - local_apply_started_at
            if prepared.wal_record is not None:
                assert self.guard is not None
                self.guard.applied(operation.tenant_id)
            commit_latency_ns = time.perf_counter_ns() - started_at
            if receipt is not None:
                try:
                    _run_fault_hook(
                        wal_fault_hook,
                        WalCommitFaultPoint.BEFORE_ACK,
                        connection,
                    )
                except InjectedWalFaultError as exc:
                    return _finish_error(
                        operation,
                        started_at,
                        EngineErrorCode.INJECTED_WAL_FAULT,
                        str(exc),
                        rows_read=rows_read,
                        transaction_id=transaction_id,
                        prepared=prepared,
                        receipt=receipt,
                        local_apply_latency_ns=local_apply_latency_ns,
                        commit_latency_ns=commit_latency_ns,
                    )
            return _finish_success(
                operation,
                started_at,
                rows_read=rows_read,
                rows_written=prepared.rows_written,
                transaction_id=transaction_id,
                wal_sequence_no=None
                if prepared.wal_record is None
                else prepared.wal_record.sequence_no,
                wal_bytes=0 if receipt is None else receipt.bytes_written,
                remote_durable=False if receipt is None else receipt.remote_durable,
                wal_object_key=None if receipt is None else receipt.key,
                wal_record_checksum=None
                if prepared.wal_record is None
                else prepared.wal_record.record_checksum,
                s3_put_latency_ns=None if receipt is None else receipt.duration_ns,
                local_apply_latency_ns=local_apply_latency_ns,
                commit_latency_ns=commit_latency_ns,
                wal_batch_id=None if receipt is None else receipt.wal_batch_id,
                wal_batch_object_key=None if receipt is None else receipt.wal_batch_object_key,
                wal_batch_record_count=None if receipt is None else receipt.wal_batch_record_count,
                wal_batch_record_index=None if receipt is None else receipt.wal_batch_record_index,
                wal_batch_wait_latency_ns=None
                if receipt is None
                else receipt.wal_batch_wait_latency_ns,
                wal_batch_put_latency_ns=None
                if receipt is None
                else receipt.wal_batch_put_latency_ns,
                wal_batch_flush_reason=None if receipt is None else receipt.wal_batch_flush_reason,
                remote_wal_put_count=0 if receipt is None else receipt.remote_wal_put_count,
                remote_wal_bytes_written=0
                if receipt is None
                else _remote_wal_bytes_written(receipt),
                authority_epoch=None if receipt is None else receipt.authority_epoch,
                authority_attempt_id=None
                if receipt is None
                else receipt.authority_attempt_id,
                authority_descriptor_key=None
                if receipt is None
                else receipt.authority_descriptor_key,
                authority_revision=None if receipt is None else receipt.authority_revision,
                authority_decision=None if receipt is None else receipt.authority_decision,
            )
        except ConfirmedDurabilityError as exc:
            connection.rollback()
            return _finish_local_apply_after_durability_error(
                operation,
                started_at,
                exc,
                rows_read=rows_read,
                transaction_id=transaction_id,
                prepared=prepared,
                receipt=exc.receipt,
                local_apply_latency_ns=None,
            )
        except PersistenceRejectedError as exc:
            connection.rollback()
            return replace(
                _finish_error(
                    operation,
                    started_at,
                    EngineErrorCode.REMOTE_DURABILITY_FAILED,
                    str(exc),
                    transaction_id=transaction_id,
                ),
                durability_outcome=DurabilityOutcome.REJECTED,
            )
        except StaleOwnerError as exc:
            connection.rollback()
            return replace(
                _finish_error(
                    operation,
                    started_at,
                    EngineErrorCode.FENCED,
                    str(exc),
                    transaction_id=transaction_id,
                    reconciliation_required=True,
                ),
                durability_outcome=DurabilityOutcome.FENCED,
                tenant_state="FENCED",
            )
        except OwnershipError as exc:
            connection.rollback()
            return replace(
                _finish_error(
                    operation,
                    started_at,
                    EngineErrorCode.AUTHORITY_UNAVAILABLE,
                    str(exc),
                    transaction_id=transaction_id,
                    reconciliation_required=True,
                ),
                durability_outcome=DurabilityOutcome.COMMIT_DECISION_UNKNOWN,
            )
        except RemoteWalConflictError as exc:
            connection.rollback()
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.REMOTE_DURABILITY_FAILED,
                str(exc),
                rows_read=rows_read,
                transaction_id=transaction_id,
                prepared=prepared,
            )
        except RemoteDurabilityError as exc:
            connection.rollback()
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.REMOTE_DURABILITY_FAILED,
                str(exc),
                rows_read=rows_read,
                transaction_id=transaction_id,
                prepared=prepared,
            )
        except InjectedWalFaultError as exc:
            connection.rollback()
            error_code = (
                EngineErrorCode.LOCAL_APPLY_AFTER_DURABILITY_FAILED
                if receipt is not None
                else EngineErrorCode.INJECTED_WAL_FAULT
            )
            return _finish_error(
                operation,
                started_at,
                error_code,
                str(exc),
                rows_read=rows_read,
                transaction_id=transaction_id,
                prepared=prepared,
                receipt=receipt,
                reconciliation_required=receipt is not None,
            )
        except TransactionConflictError as exc:
            connection.rollback()
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.TRANSACTION_CONFLICT,
                str(exc),
                rows_read=rows_read,
                transaction_id=transaction_id,
                conflict=True,
            )
        except KeyError as exc:
            connection.rollback()
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.ENTITY_NOT_FOUND,
                str(exc),
                transaction_id=transaction_id,
            )
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            if receipt is not None:
                return _finish_local_apply_after_durability_error(
                    operation,
                    started_at,
                    exc,
                    rows_read=rows_read,
                    transaction_id=transaction_id,
                    prepared=prepared,
                    receipt=receipt,
                    local_apply_latency_ns=local_apply_latency_ns,
                )
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.INTEGRITY_ERROR,
                _safe_error_message(exc),
                transaction_id=transaction_id,
            )
        except sqlite3.OperationalError as exc:
            connection.rollback()
            if receipt is not None:
                return _finish_local_apply_after_durability_error(
                    operation,
                    started_at,
                    exc,
                    rows_read=rows_read,
                    transaction_id=transaction_id,
                    prepared=prepared,
                    receipt=receipt,
                    local_apply_latency_ns=local_apply_latency_ns,
                )
            return _finish_error(
                operation,
                started_at,
                _operational_error_code(exc),
                _safe_error_message(exc),
                transaction_id=transaction_id,
            )

        except Exception as exc:
            connection.rollback()
            if receipt is not None:
                return _finish_local_apply_after_durability_error(
                    operation,
                    started_at,
                    exc,
                    rows_read=rows_read,
                    transaction_id=transaction_id,
                    prepared=prepared,
                    receipt=receipt,
                    local_apply_latency_ns=local_apply_latency_ns,
                )
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.INTERNAL_DATABASE_ERROR,
                str(exc),
                transaction_id=transaction_id,
            )

    def _multi_entity_transaction(
        self,
        operation: WorkloadOperation,
        connection: sqlite3.Connection,
        started_at: int,
        payload: MultiEntityTransactionPayload,
        *,
        before_transaction_insert_hook: Callable[[sqlite3.Connection], None] | None,
        wal_fault_hook: Callable[[WalCommitFaultPoint, sqlite3.Connection], None] | None,
    ) -> ExecutionResult:
        if not 2 <= len(payload.items) <= 5:
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.INVALID_OPERATION,
                "multi-entity transactions require 2 to 5 items",
                transaction_id=payload.transaction_id,
            )
        entity_ids = [item.entity_id for item in payload.items]
        if len(set(entity_ids)) != len(entity_ids):
            return _finish_error(
                operation,
                started_at,
                EngineErrorCode.INVALID_OPERATION,
                "multi-entity transaction contains duplicate entity ids",
                transaction_id=payload.transaction_id,
            )
        duplicate = _committed_transaction_exists(connection, payload.transaction_id)
        if duplicate:
            check_identity(
                connection,
                operation.tenant_id,
                payload.transaction_id,
                request_fingerprint(operation),
            )
            wal_state = transaction_wal_state(
                connection,
                tenant_id=operation.tenant_id,
                transaction_id=payload.transaction_id,
            )
            return _finish_duplicate(
                operation,
                started_at,
                transaction_id=payload.transaction_id,
                wal_state=wal_state,
            )
        return self._execute_write_items(
            operation,
            connection,
            started_at,
            transaction_id=payload.transaction_id,
            rows_read=len(payload.items),
            items=tuple(
                (item.entity_id, item.delta, item.expected_version) for item in payload.items
            ),
            before_transaction_insert_hook=before_transaction_insert_hook,
            wal_fault_hook=wal_fault_hook,
        )


@dataclass(frozen=True, slots=True)
class _PreparedItem:
    entity_values: dict[str, WalScalar]
    entity_version_values: dict[str, WalScalar]
    transaction_item_values: dict[str, WalScalar]
    audit_values: dict[str, WalScalar]


@dataclass(frozen=True, slots=True)
class _PreparedItems:
    tenant_id: str
    transaction_id: str
    rows_written: int
    transaction_values: dict[str, WalScalar]
    items: tuple[_PreparedItem, ...]
    wal_record: WalRecord | None
    fingerprint: str


def _prepare_items(
    connection: sqlite3.Connection,
    tenant_id: str,
    transaction_id: str,
    items: tuple[tuple[str, int, int | None], ...],
    *,
    operation_type: OperationType,
    wal_enabled: bool,
    fingerprint: str,
    owner_epoch: int = LOCAL_OWNER_EPOCH,
) -> _PreparedItems:
    entity_states: list[tuple[str, str, int, int, int]] = []
    for entity_id, delta, expected_version in items:
        row = connection.execute(
            """
            SELECT state, numeric_value, version
            FROM entities
            WHERE tenant_id = ?
              AND entity_id = ?
            """,
            (tenant_id, entity_id),
        ).fetchone()
        if row is None:
            msg = f"entity does not exist: {entity_id}"
            raise KeyError(msg)
        state = str(row["state"])
        numeric_value = int(row["numeric_value"])
        previous_version = int(row["version"])
        if expected_version is not None and expected_version != previous_version:
            msg = f"expected version {expected_version}, found {previous_version}"
            raise TransactionConflictError(msg)
        entity_states.append((entity_id, state, numeric_value, previous_version, delta))

    wal_sequence_no: int | None = None
    previous_record_checksum: str | None = None
    if wal_enabled:
        ensure_wal_state_table(connection)
        wal_sequence_no, previous_record_checksum = allocate_next_sequence(connection)

    audit_sequence_no = _next_audit_sequence(connection, tenant_id)
    timestamp = mutation_timestamp(audit_sequence_no)
    transaction_values: dict[str, WalScalar] = {
        "transaction_id": transaction_id,
        "tenant_id": tenant_id,
        "status": "COMMITTED",
        "created_at": timestamp,
        "completed_at": timestamp,
    }
    rows_written = 1
    wal_operations: list[WalMutation] = []
    if wal_enabled:
        wal_operations.append(
            _wal_mutation(
                WalMutationType.INSERT_TRANSACTION,
                "transactions",
                key={"tenant_id": tenant_id, "transaction_id": transaction_id},
                values=transaction_values,
            )
        )

    prepared_items: list[_PreparedItem] = []
    for item_no, (entity_id, state, numeric_value, previous_version, delta) in enumerate(
        entity_states,
        start=1,
    ):
        new_numeric_value = numeric_value + delta
        new_version = previous_version + 1
        item_sequence_no = audit_sequence_no + item_no - 1
        item_timestamp = mutation_timestamp(item_sequence_no)
        audit_id = deterministic_uuid("engine-audit", tenant_id, transaction_id, item_no)
        entity_values: dict[str, WalScalar] = {
            "tenant_id": tenant_id,
            "entity_id": entity_id,
            "state": state,
            "numeric_value": new_numeric_value,
            "version": new_version,
            "updated_at": item_timestamp,
        }
        entity_version_values: dict[str, WalScalar] = {
            "tenant_id": tenant_id,
            "entity_id": entity_id,
            "version": new_version,
            "state": state,
            "numeric_value": new_numeric_value,
            "created_at": item_timestamp,
        }
        transaction_item_values: dict[str, WalScalar] = {
            "transaction_id": transaction_id,
            "item_no": item_no,
            "tenant_id": tenant_id,
            "entity_id": entity_id,
            "delta": delta,
            "previous_version": previous_version,
            "new_version": new_version,
        }
        audit_values: dict[str, WalScalar] = {
            "audit_id": audit_id,
            "tenant_id": tenant_id,
            "transaction_id": transaction_id,
            "entity_id": entity_id,
            "operation": "ENTITY_UPDATED",
            "sequence_no": item_sequence_no,
            "created_at": item_timestamp,
        }
        rows_written += 4
        prepared_items.append(
            _PreparedItem(
                entity_values=entity_values,
                entity_version_values=entity_version_values,
                transaction_item_values=transaction_item_values,
                audit_values=audit_values,
            )
        )
        if wal_enabled:
            wal_operations.extend(
                (
                    _wal_mutation(
                        WalMutationType.UPDATE_ENTITY,
                        "entities",
                        key={"tenant_id": tenant_id, "entity_id": entity_id},
                        values=entity_values,
                    ),
                    _wal_mutation(
                        WalMutationType.INSERT_ENTITY_VERSION,
                        "entity_versions",
                        key={
                            "tenant_id": tenant_id,
                            "entity_id": entity_id,
                            "version": new_version,
                        },
                        values=entity_version_values,
                    ),
                    _wal_mutation(
                        WalMutationType.INSERT_TRANSACTION_ITEM,
                        "transaction_items",
                        key={"transaction_id": transaction_id, "item_no": item_no},
                        values=transaction_item_values,
                    ),
                    _wal_mutation(
                        WalMutationType.INSERT_AUDIT_LOG,
                        "audit_log",
                        key={"audit_id": audit_id},
                        values=audit_values,
                    ),
                )
            )

    wal_record: WalRecord | None = None
    if wal_enabled:
        assert wal_sequence_no is not None
        wal_record = attach_record_checksum(
            WalRecord(
                wal_format_version=WAL_FORMAT_VERSION,
                request_fingerprint=fingerprint,
                tenant_id=tenant_id,
                transaction_id=transaction_id,
                sequence_no=wal_sequence_no,
                owner_epoch=owner_epoch,
                operation_type=operation_type,
                operations=tuple(wal_operations),
                created_at=timestamp,
                previous_record_checksum=previous_record_checksum,
            )
        )
        assert wal_record.record_checksum is not None
    return _PreparedItems(
        tenant_id=tenant_id,
        transaction_id=transaction_id,
        rows_written=rows_written,
        transaction_values=transaction_values,
        items=tuple(prepared_items),
        wal_record=wal_record,
        fingerprint=fingerprint,
    )


def _apply_prepared_items(
    connection: sqlite3.Connection,
    prepared: _PreparedItems,
    *,
    receipt: WalCommitReceipt | None,
    before_transaction_insert_hook: Callable[[sqlite3.Connection], None] | None,
) -> None:
    if before_transaction_insert_hook is not None:
        before_transaction_insert_hook(connection)
    transaction_values = prepared.transaction_values
    record_identity(connection, prepared.tenant_id, prepared.transaction_id, prepared.fingerprint)
    connection.execute(
        """
        INSERT INTO transactions (
            transaction_id,
            tenant_id,
            status,
            created_at,
            completed_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            transaction_values["transaction_id"],
            transaction_values["tenant_id"],
            transaction_values["status"],
            transaction_values["created_at"],
            transaction_values["completed_at"],
        ),
    )
    for item in prepared.items:
        entity_values = item.entity_values
        connection.execute(
            """
            UPDATE entities
            SET state = ?,
                numeric_value = ?,
                version = ?,
                updated_at = ?
            WHERE tenant_id = ?
              AND entity_id = ?
            """,
            (
                entity_values["state"],
                entity_values["numeric_value"],
                entity_values["version"],
                entity_values["updated_at"],
                entity_values["tenant_id"],
                entity_values["entity_id"],
            ),
        )
        entity_version_values = item.entity_version_values
        connection.execute(
            """
            INSERT INTO entity_versions (
                tenant_id,
                entity_id,
                version,
                state,
                numeric_value,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                entity_version_values["tenant_id"],
                entity_version_values["entity_id"],
                entity_version_values["version"],
                entity_version_values["state"],
                entity_version_values["numeric_value"],
                entity_version_values["created_at"],
            ),
        )
        transaction_item_values = item.transaction_item_values
        connection.execute(
            """
            INSERT INTO transaction_items (
                transaction_id,
                item_no,
                tenant_id,
                entity_id,
                delta,
                previous_version,
                new_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transaction_item_values["transaction_id"],
                transaction_item_values["item_no"],
                transaction_item_values["tenant_id"],
                transaction_item_values["entity_id"],
                transaction_item_values["delta"],
                transaction_item_values["previous_version"],
                transaction_item_values["new_version"],
            ),
        )
        audit_values = item.audit_values
        connection.execute(
            """
            INSERT INTO audit_log (
                audit_id,
                tenant_id,
                transaction_id,
                entity_id,
                operation,
                sequence_no,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                audit_values["audit_id"],
                audit_values["tenant_id"],
                audit_values["transaction_id"],
                audit_values["entity_id"],
                audit_values["operation"],
                audit_values["sequence_no"],
                audit_values["created_at"],
            ),
        )

    if prepared.wal_record is not None:
        assert receipt is not None
        assert prepared.wal_record.record_checksum is not None
        advance_sequence(
            connection,
            prepared.wal_record.sequence_no,
            prepared.wal_record.record_checksum,
        )
        record_transaction_wal(
            connection,
            tenant_id=prepared.tenant_id,
            transaction_id=prepared.transaction_id,
            wal_sequence=prepared.wal_record.sequence_no,
            record_checksum=prepared.wal_record.record_checksum,
            wal_object_key=receipt.key,
            remote_durable=receipt.remote_durable,
        )


def _wal_mutation(
    mutation_type: WalMutationType,
    table: str,
    *,
    key: dict[str, WalScalar],
    values: dict[str, WalScalar],
) -> WalMutation:
    return WalMutation.from_mappings(mutation_type, table, key=key, values=values)


def _committed_transaction_exists(connection: sqlite3.Connection, transaction_id: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM transactions WHERE transaction_id = ? AND status = 'COMMITTED'",
        (transaction_id,),
    ).fetchone()
    return row is not None


def _next_audit_sequence(connection: sqlite3.Connection, tenant_id: str) -> int:
    row = connection.execute(
        "SELECT COALESCE(MAX(sequence_no), -1) + 1 FROM audit_log WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()
    return int(row[0])


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, object]]:
    return [dict(row) for row in rows]


def _finish_success(
    operation: WorkloadOperation,
    started_at: int,
    *,
    rows_read: int = 0,
    rows_written: int = 0,
    transaction_id: str | None = None,
    duplicate_transaction: bool = False,
    records: tuple[dict[str, object], ...] = (),
    wal_sequence_no: int | None = None,
    wal_bytes: int = 0,
    remote_durable: bool = False,
    wal_object_key: str | None = None,
    wal_record_checksum: str | None = None,
    s3_put_latency_ns: int | None = None,
    local_apply_latency_ns: int | None = None,
    commit_latency_ns: int | None = None,
    wal_batch_id: str | None = None,
    wal_batch_object_key: str | None = None,
    wal_batch_record_count: int | None = None,
    wal_batch_record_index: int | None = None,
    wal_batch_wait_latency_ns: int | None = None,
    wal_batch_put_latency_ns: int | None = None,
    wal_batch_flush_reason: str | None = None,
    remote_wal_put_count: int = 0,
    remote_wal_bytes_written: int = 0,
    authority_epoch: int | None = None,
    authority_attempt_id: str | None = None,
    authority_descriptor_key: str | None = None,
    authority_revision: int | None = None,
    authority_decision: str | None = None,
) -> ExecutionResult:
    finished_at = time.perf_counter_ns()
    return ExecutionResult(
        operation_id=operation.operation_id,
        tenant_id=operation.tenant_id,
        operation_type=operation.operation_type,
        success=True,
        started_at_monotonic_ns=started_at,
        finished_at_monotonic_ns=finished_at,
        latency_ns=finished_at - started_at,
        rows_read=rows_read,
        rows_written=rows_written,
        transaction_id=transaction_id,
        duplicate_transaction=duplicate_transaction,
        records=records,
        wal_sequence_no=wal_sequence_no,
        wal_bytes=wal_bytes,
        remote_durable=remote_durable,
        wal_object_key=wal_object_key,
        wal_record_checksum=wal_record_checksum,
        s3_put_latency_ns=s3_put_latency_ns,
        local_apply_latency_ns=local_apply_latency_ns,
        commit_latency_ns=commit_latency_ns,
        wal_batch_id=wal_batch_id,
        wal_batch_object_key=wal_batch_object_key,
        wal_batch_record_count=wal_batch_record_count,
        wal_batch_record_index=wal_batch_record_index,
        wal_batch_wait_latency_ns=wal_batch_wait_latency_ns,
        wal_batch_put_latency_ns=wal_batch_put_latency_ns,
        wal_batch_flush_reason=wal_batch_flush_reason,
        remote_wal_put_count=remote_wal_put_count,
        remote_wal_bytes_written=remote_wal_bytes_written,
        authority_epoch=authority_epoch,
        authority_attempt_id=authority_attempt_id,
        authority_descriptor_key=authority_descriptor_key,
        authority_revision=authority_revision,
        authority_decision=authority_decision,
    )


def _finish_error(
    operation: WorkloadOperation,
    started_at: int,
    error_code: EngineErrorCode,
    error_message: str,
    *,
    rows_read: int = 0,
    transaction_id: str | None = None,
    conflict: bool = False,
    prepared: _PreparedItems | None = None,
    receipt: WalCommitReceipt | None = None,
    local_apply_latency_ns: int | None = None,
    commit_latency_ns: int | None = None,
    reconciliation_required: bool = False,
) -> ExecutionResult:
    finished_at = time.perf_counter_ns()
    wal_record = None if prepared is None else prepared.wal_record
    return ExecutionResult(
        operation_id=operation.operation_id,
        tenant_id=operation.tenant_id,
        operation_type=operation.operation_type,
        success=False,
        started_at_monotonic_ns=started_at,
        finished_at_monotonic_ns=finished_at,
        latency_ns=finished_at - started_at,
        rows_read=rows_read,
        transaction_id=transaction_id,
        conflict=conflict,
        error_code=error_code,
        error_message=error_message,
        wal_sequence_no=None if wal_record is None else wal_record.sequence_no,
        wal_bytes=0 if receipt is None else receipt.bytes_written,
        remote_durable=False if receipt is None else receipt.remote_durable,
        wal_object_key=None if receipt is None else receipt.key,
        wal_record_checksum=None if wal_record is None else wal_record.record_checksum,
        s3_put_latency_ns=None if receipt is None else receipt.duration_ns,
        local_apply_latency_ns=local_apply_latency_ns,
        commit_latency_ns=commit_latency_ns,
        reconciliation_required=reconciliation_required,
        wal_batch_id=None if receipt is None else receipt.wal_batch_id,
        wal_batch_object_key=None if receipt is None else receipt.wal_batch_object_key,
        wal_batch_record_count=None if receipt is None else receipt.wal_batch_record_count,
        wal_batch_record_index=None if receipt is None else receipt.wal_batch_record_index,
        wal_batch_wait_latency_ns=None if receipt is None else receipt.wal_batch_wait_latency_ns,
        wal_batch_put_latency_ns=None if receipt is None else receipt.wal_batch_put_latency_ns,
        wal_batch_flush_reason=None if receipt is None else receipt.wal_batch_flush_reason,
        remote_wal_put_count=0 if receipt is None else receipt.remote_wal_put_count,
        remote_wal_bytes_written=0 if receipt is None else _remote_wal_bytes_written(receipt),
        authority_epoch=None if receipt is None else receipt.authority_epoch,
        authority_attempt_id=None if receipt is None else receipt.authority_attempt_id,
        authority_descriptor_key=None
        if receipt is None
        else receipt.authority_descriptor_key,
        authority_revision=None if receipt is None else receipt.authority_revision,
        authority_decision=None if receipt is None else receipt.authority_decision,
    )


def _finish_duplicate(
    operation: WorkloadOperation,
    started_at: int,
    *,
    transaction_id: str,
    wal_state: tuple[int, str, str | None, bool] | None,
) -> ExecutionResult:
    if wal_state is None:
        return _finish_success(
            operation,
            started_at,
            transaction_id=transaction_id,
            duplicate_transaction=True,
        )
    wal_sequence, checksum, object_key, remote_durable = wal_state
    return _finish_success(
        operation,
        started_at,
        transaction_id=transaction_id,
        duplicate_transaction=True,
        wal_sequence_no=wal_sequence,
        remote_durable=remote_durable,
        wal_object_key=object_key,
        wal_record_checksum=checksum,
    )


def _finish_local_apply_after_durability_error(
    operation: WorkloadOperation,
    started_at: int,
    error: Exception,
    *,
    rows_read: int,
    transaction_id: str,
    prepared: _PreparedItems | None,
    receipt: WalCommitReceipt,
    local_apply_latency_ns: int | None,
) -> ExecutionResult:
    failure = LocalApplyAfterDurabilityError(
        f"local SQLite apply failed after WAL durability: {_safe_error_message(error)}"
    )
    return _finish_error(
        operation,
        started_at,
        EngineErrorCode.LOCAL_APPLY_AFTER_DURABILITY_FAILED,
        str(failure),
        rows_read=rows_read,
        transaction_id=transaction_id,
        prepared=prepared,
        receipt=receipt,
        local_apply_latency_ns=local_apply_latency_ns,
        reconciliation_required=True,
    )


def _run_fault_hook(
    hook: Callable[[WalCommitFaultPoint, sqlite3.Connection], None] | None,
    point: WalCommitFaultPoint,
    connection: sqlite3.Connection,
) -> None:
    if hook is not None:
        hook(point, connection)


def _operational_error_code(error: sqlite3.OperationalError) -> EngineErrorCode:
    message = str(error).lower()
    if "locked" in message or "busy" in message:
        return EngineErrorCode.SQLITE_BUSY
    return EngineErrorCode.INTERNAL_DATABASE_ERROR


def _safe_error_message(error: Exception) -> str:
    return str(error)


def _remote_wal_bytes_written(receipt: WalCommitReceipt) -> int:
    if receipt.remote_wal_bytes_written is not None:
        return receipt.remote_wal_bytes_written
    return receipt.bytes_written if receipt.remote_durable else 0
