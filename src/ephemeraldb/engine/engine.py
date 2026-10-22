"""Local tenant database execution engine."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.config.settings import Settings
from ephemeraldb.core.models import TenantId
from ephemeraldb.engine.connection import ConnectionManager, EngineConfig
from ephemeraldb.engine.executor import OperationExecutor, WalCommitFaultPoint
from ephemeraldb.engine.metrics import EngineMetrics
from ephemeraldb.engine.registry import TenantDatabaseRegistry
from ephemeraldb.engine.results import EngineStatus, ExecutionResult
from ephemeraldb.loadgen.models import WorkloadOperation
from ephemeraldb.schema.checksum import calculate_logical_checksum
from ephemeraldb.schema.models import ValidationResult
from ephemeraldb.schema.validation import validate_tenant_database
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig
from ephemeraldb.wal.durability import (
    SingleRecordWalCommitter,
    WalCommitter,
    WalDurabilityStore,
)
from ephemeraldb.wal.writer import LocalWalStore


class TenantDatabaseEngine:
    """Executes backend-independent workload operations against local SQLite DBs."""

    def __init__(
        self,
        config: EngineConfig,
        registry: TenantDatabaseRegistry,
        *,
        wal_store: WalDurabilityStore | None = None,
        wal_committer: WalCommitter | None = None,
        initialize_durability: bool = False,
    ) -> None:
        self.config = config
        self.registry = registry
        self.wal_store = wal_store
        self.wal_committer = wal_committer
        if self.wal_committer is None and wal_store is not None:
            self.wal_committer = SingleRecordWalCommitter(wal_store)
        self.connection_manager = ConnectionManager(config, registry)
        self.executor = OperationExecutor(
            self.connection_manager,
            wal_committer=self.wal_committer,
            initialize_durability=initialize_durability,
        )
        self.metrics = EngineMetrics()

    @classmethod
    def from_dataset_dir(
        cls,
        dataset_dir: Path,
        *,
        max_open_connections: int = 32,
        busy_timeout_ms: int = 5000,
        journal_mode: str = "WAL",
        synchronous_mode: str = "FULL",
        enable_foreign_keys: bool = True,
        wal_root: Path | None = None,
        wal_store: WalDurabilityStore | None = None,
        wal_committer: WalCommitter | None = None,
        initialize_durability: bool = False,
    ) -> TenantDatabaseEngine:
        registry = TenantDatabaseRegistry.from_dataset_dir(dataset_dir)
        config = EngineConfig(
            database_root=Path(dataset_dir),
            max_open_connections=max_open_connections,
            busy_timeout_ms=busy_timeout_ms,
            enable_foreign_keys=enable_foreign_keys,
            journal_mode=journal_mode,
            synchronous_mode=synchronous_mode,
        )
        resolved_wal_store = wal_store
        if resolved_wal_store is None and wal_committer is None and wal_root is not None:
            resolved_wal_store = LocalWalStore(wal_root)
        return cls(
            config,
            registry,
            wal_store=resolved_wal_store,
            wal_committer=wal_committer,
            initialize_durability=initialize_durability,
        )

    @classmethod
    def from_settings(
        cls,
        dataset_dir: Path,
        settings: Settings,
        *,
        max_open_connections: int = 32,
        busy_timeout_ms: int = 5000,
        wal_root: Path | None = None,
        initialize_durability: bool = False,
    ) -> TenantDatabaseEngine:
        wal_store: WalDurabilityStore | None = None
        wal_committer: WalCommitter | None = None
        persistence_mode = settings.wal_persistence_mode.lower()
        if settings.wal_store == "local":
            if persistence_mode != "single":
                msg = "WAL_PERSISTENCE_MODE=batch requires WAL_STORE=s3-express"
                raise ValueError(msg)
            if wal_root is not None:
                wal_store = LocalWalStore(wal_root)
        elif settings.wal_store == "s3-express":
            s3_store = S3ExpressWalStore(S3ExpressWalConfig.from_settings(settings))
            if persistence_mode == "single":
                wal_store = s3_store
            elif persistence_mode == "batch":
                wal_committer = BatchedWalCommitter(
                    s3_store,
                    config=WalBatchingConfig(
                        batch_window_ms=settings.wal_batch_window_ms,
                        max_records_per_batch=settings.wal_batch_max_records,
                        max_batch_bytes=settings.wal_batch_max_bytes,
                        max_queue_records=settings.wal_batch_max_queue_records,
                        flush_on_shutdown=settings.wal_batch_flush_on_shutdown,
                    ),
                )
            else:
                msg = f"unsupported WAL_PERSISTENCE_MODE: {settings.wal_persistence_mode}"
                raise ValueError(msg)
        elif settings.wal_store not in {"none", "off"}:
            msg = f"unsupported WAL_STORE: {settings.wal_store}"
            raise ValueError(msg)
        return cls.from_dataset_dir(
            dataset_dir,
            max_open_connections=max_open_connections,
            busy_timeout_ms=busy_timeout_ms,
            wal_store=wal_store,
            wal_committer=wal_committer,
            initialize_durability=initialize_durability,
        )

    def execute(
        self,
        operation: WorkloadOperation,
        *,
        before_transaction_insert_hook: Callable[[sqlite3.Connection], None] | None = None,
        wal_fault_hook: Callable[[WalCommitFaultPoint, sqlite3.Connection], None] | None = None,
    ) -> ExecutionResult:
        sqlite_hook = before_transaction_insert_hook
        result = self.executor.execute(
            operation,
            before_transaction_insert_hook=sqlite_hook,
            wal_fault_hook=wal_fault_hook,
        )
        try:
            self.metrics.observe(result)
        except Exception:
            pass  # Observability must not change a completed transaction outcome.
        return result

    def reconcile_tenant(self, tenant_id: TenantId | str) -> None:
        tenant = _tenant_id(tenant_id)
        with self.executor._lock_for(tenant.value):
            with self.connection_manager.connection_for(tenant) as connection:
                if self.executor.guard is not None:
                    self.executor.guard.reconcile(tenant.value, connection)

    def calculate_tenant_checksum(self, tenant_id: TenantId | str) -> str:
        tenant = _tenant_id(tenant_id)
        with self.connection_manager.connection_for(tenant) as connection:
            return calculate_logical_checksum(connection)

    def validate_tenant(self, tenant_id: TenantId | str) -> ValidationResult:
        tenant = _tenant_id(tenant_id)
        record = self.registry.record_for(tenant)
        return validate_tenant_database(
            record.database_path,
            expected_tenant_id=tenant.value,
            expected_entity_count=record.entity_count,
            expected_relationship_count=record.relationship_count,
            expected_metadata_count=record.metadata_count,
        )

    def status(self) -> EngineStatus:
        return EngineStatus(
            database_root=str(self.config.database_root),
            known_tenant_count=self.registry.tenant_count,
            open_connection_count=self.connection_manager.open_connection_count,
            max_open_connection_count=self.config.max_open_connections,
            journal_mode=self.config.journal_mode,
            synchronous_mode=self.config.synchronous_mode,
            busy_timeout_ms=self.config.busy_timeout_ms,
            foreign_keys_enabled=self.config.enable_foreign_keys,
        )

    def close_tenant(self, tenant_id: TenantId | str) -> None:
        self.connection_manager.close(_tenant_id(tenant_id))

    def close_all(self) -> None:
        try:
            if self.wal_committer is not None:
                self.wal_committer.close()
        finally:
            self.connection_manager.close_all()

    def clear_connection_cache(self) -> None:
        self.connection_manager.clear()


def _tenant_id(value: TenantId | str) -> TenantId:
    return value if isinstance(value, TenantId) else TenantId(value)
