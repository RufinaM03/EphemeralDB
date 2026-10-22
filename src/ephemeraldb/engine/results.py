"""Structured local engine execution results."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ephemeraldb.loadgen.models import OperationType


class EngineErrorCode(StrEnum):
    TENANT_NOT_READY = "TENANT_NOT_READY"
    TENANT_NOT_FOUND = "TENANT_NOT_FOUND"
    ENTITY_NOT_FOUND = "ENTITY_NOT_FOUND"
    TRANSACTION_CONFLICT = "TRANSACTION_CONFLICT"
    DUPLICATE_TRANSACTION = "DUPLICATE_TRANSACTION"
    SQLITE_BUSY = "SQLITE_BUSY"
    INTEGRITY_ERROR = "INTEGRITY_ERROR"
    INVALID_OPERATION = "INVALID_OPERATION"
    REMOTE_DURABILITY_FAILED = "REMOTE_DURABILITY_FAILED"
    LOCAL_APPLY_AFTER_DURABILITY_FAILED = "LOCAL_APPLY_AFTER_DURABILITY_FAILED"
    INJECTED_WAL_FAULT = "INJECTED_WAL_FAULT"
    INTERNAL_DATABASE_ERROR = "INTERNAL_DATABASE_ERROR"
    FENCED = "FENCED"
    AUTHORITY_UNAVAILABLE = "AUTHORITY_UNAVAILABLE"


class DurabilityOutcome(StrEnum):
    NOT_ATTEMPTED = "NOT_ATTEMPTED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    DURABLE_NOT_APPLIED = "DURABLE_NOT_APPLIED"
    DURABLE_APPLIED = "DURABLE_APPLIED"
    LOCAL_APPLIED = "LOCAL_APPLIED"
    PAYLOAD_STORED = "PAYLOAD_STORED"
    COMMIT_DECISION_UNKNOWN = "COMMIT_DECISION_UNKNOWN"
    COMMIT_ACCEPTED_NOT_APPLIED = "COMMIT_ACCEPTED_NOT_APPLIED"
    COMMIT_ACCEPTED_APPLIED = "COMMIT_ACCEPTED_APPLIED"
    FENCED = "FENCED"


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    operation_id: str
    tenant_id: str
    operation_type: OperationType
    success: bool
    started_at_monotonic_ns: int
    finished_at_monotonic_ns: int
    latency_ns: int
    rows_read: int = 0
    rows_written: int = 0
    transaction_id: str | None = None
    conflict: bool = False
    duplicate_transaction: bool = False
    error_code: EngineErrorCode | None = None
    error_message: str | None = None
    records: tuple[dict[str, object], ...] = ()
    wal_sequence_no: int | None = None
    wal_bytes: int = 0
    remote_durable: bool = False
    wal_object_key: str | None = None
    wal_record_checksum: str | None = None
    s3_put_latency_ns: int | None = None
    local_apply_latency_ns: int | None = None
    commit_latency_ns: int | None = None
    reconciliation_required: bool = False
    wal_batch_id: str | None = None
    wal_batch_object_key: str | None = None
    wal_batch_record_count: int | None = None
    wal_batch_record_index: int | None = None
    wal_batch_wait_latency_ns: int | None = None
    wal_batch_put_latency_ns: int | None = None
    wal_batch_flush_reason: str | None = None
    remote_wal_put_count: int = 0
    remote_wal_bytes_written: int = 0
    durability_outcome: DurabilityOutcome = DurabilityOutcome.NOT_ATTEMPTED
    tenant_state: str = "READY"
    executor_queue_latency_ns: int = 0
    tenant_wait_latency_ns: int = 0
    authority_epoch: int | None = None
    authority_attempt_id: str | None = None
    authority_descriptor_key: str | None = None
    authority_revision: int | None = None
    authority_decision: str | None = None


@dataclass(frozen=True, slots=True)
class EngineStatus:
    database_root: str
    known_tenant_count: int
    open_connection_count: int
    max_open_connection_count: int
    journal_mode: str
    synchronous_mode: str
    busy_timeout_ms: int
    foreign_keys_enabled: bool


def execution_result_to_dict(result: ExecutionResult) -> dict[str, object]:
    return {
        "operation_id": result.operation_id,
        "tenant_id": result.tenant_id,
        "operation_type": result.operation_type.value,
        "success": result.success,
        "started_at_monotonic_ns": result.started_at_monotonic_ns,
        "finished_at_monotonic_ns": result.finished_at_monotonic_ns,
        "latency_ns": result.latency_ns,
        "rows_read": result.rows_read,
        "rows_written": result.rows_written,
        "transaction_id": result.transaction_id,
        "conflict": result.conflict,
        "duplicate_transaction": result.duplicate_transaction,
        "error_code": None if result.error_code is None else result.error_code.value,
        "error_message": result.error_message,
        "records": list(result.records),
        "wal_sequence_no": result.wal_sequence_no,
        "wal_sequence": result.wal_sequence_no,
        "wal_bytes": result.wal_bytes,
        "remote_durable": result.remote_durable,
        "wal_object_key": result.wal_object_key,
        "wal_record_checksum": result.wal_record_checksum,
        "s3_put_latency_ns": result.s3_put_latency_ns,
        "local_apply_latency_ns": result.local_apply_latency_ns,
        "commit_latency_ns": result.commit_latency_ns,
        "reconciliation_required": result.reconciliation_required,
        "wal_batch_id": result.wal_batch_id,
        "wal_batch_object_key": result.wal_batch_object_key,
        "wal_batch_record_count": result.wal_batch_record_count,
        "wal_batch_record_index": result.wal_batch_record_index,
        "wal_batch_wait_latency_ns": result.wal_batch_wait_latency_ns,
        "wal_batch_put_latency_ns": result.wal_batch_put_latency_ns,
        "wal_batch_flush_reason": result.wal_batch_flush_reason,
        "remote_wal_put_count": result.remote_wal_put_count,
        "remote_wal_bytes_written": result.remote_wal_bytes_written,
        "durability_outcome": result.durability_outcome.value,
        "tenant_state": result.tenant_state,
        "executor_queue_latency_ns": result.executor_queue_latency_ns,
        "tenant_wait_latency_ns": result.tenant_wait_latency_ns,
        "authority_epoch": result.authority_epoch,
        "authority_attempt_id": result.authority_attempt_id,
        "authority_descriptor_key": result.authority_descriptor_key,
        "authority_revision": result.authority_revision,
        "authority_decision": result.authority_decision,
    }
