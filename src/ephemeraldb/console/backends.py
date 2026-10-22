"""Real execution adapters. Backend selection never falls back to local execution."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import httpx

from ephemeraldb.aws.s3_express import S3ExpressWalConfig
from ephemeraldb.config.settings import load_settings
from ephemeraldb.console.models import RunRequest
from ephemeraldb.core.models import TenantId
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.results import execution_result_to_dict
from ephemeraldb.loadgen.models import WorkloadOperation
from ephemeraldb.loadgen.serialization import operation_to_dict
from ephemeraldb.ownership.coordinator import FencedCommitter, OwnershipCoordinator, new_session_id
from ephemeraldb.ownership.recovery import FencedRecovery
from ephemeraldb.ownership.s3_payload import FencedS3ExpressWalStore, S3ExpressAcceptedPayloadReader
from ephemeraldb.ownership.s3_store import S3AuthorityStore
from ephemeraldb.recovery.offline import OfflineWalStore
from ephemeraldb.schema.checksum import CHECKSUM_TABLES
from ephemeraldb.schema.ddl import INDEX_DDL, TABLE_DDL
from ephemeraldb.schema.generator import deterministic_uuid, mutation_timestamp
from ephemeraldb.schema.queries import QUERY_CATALOG
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig
from ephemeraldb.wal.durability import SingleRecordWalCommitter
from ephemeraldb.wal.identity import request_fingerprint

Json = dict[str, Any]
TABLES = tuple(row[0] for row in CHECKSUM_TABLES)


class SQLiteBackend:
    def __init__(
        self, root: Path, request: RunRequest, run_id: str, *, recover: bool = False
    ) -> None:
        self.root = root
        self.request = request
        self.checkpoints: list[Json] = []
        self.recoveries: list[Json] = []
        self.cloud = request.backend == "s3-express"
        if self.cloud:
            settings = load_settings()
            if not settings.s3_express_bucket or not settings.s3_checkpoint_bucket:
                raise ValueError("S3_EXPRESS_BUCKET and S3_CHECKPOINT_BUCKET are required")
            settings = replace(
                settings,
                s3_express_prefix=f"{settings.s3_express_prefix}/console/{run_id}",
                s3_checkpoint_prefix=f"{settings.s3_checkpoint_prefix}/console/{run_id}",
            )
            payload = FencedS3ExpressWalStore(S3ExpressWalConfig.from_settings(settings))
            self.payload = payload
            authority = S3AuthorityStore(settings)
            coordinator = OwnershipCoordinator(authority, run_id)
            self.coordinator = coordinator
            tokens = {}
            session_id = new_session_id(f"console-{run_id}")
            manifest = json.loads((root / "dataset/manifest.json").read_text())
            for tenant in manifest["tenant_databases"]:
                destination = root / "dataset" / tenant["filename"]
                if recover:
                    started = time.perf_counter()
                    state = coordinator.takeover(tenant["tenant_id"], session_id=session_id)

                    def install(key: str, target: Path) -> None:
                        target.write_bytes(authority.get_immutable(key))

                    for suffix in ("-wal", "-shm"):
                        sidecar = Path(str(destination) + suffix)
                        if sidecar.exists():
                            sidecar.rename(
                                sidecar.with_name(sidecar.name + ".stale-" + uuid.uuid4().hex)
                            )
                    result = FencedRecovery(
                        coordinator, S3ExpressAcceptedPayloadReader(payload)
                    ).recover(state.token, destination, install_bootstrap=install)
                    self.recoveries.append(
                        {
                            "tenant_id": tenant["tenant_id"],
                            "epoch": state.owner_epoch,
                            "sequence": result.committed_sequence,
                            "duration_ms": (time.perf_counter() - started) * 1000,
                        }
                    )
                else:
                    content = destination.read_bytes()
                    key = f"bootstrap/{tenant['tenant_id']}.db"
                    authority.put_immutable(key, content)
                    state = coordinator.bootstrap(
                        tenant["tenant_id"],
                        session_id=session_id,
                        bootstrap_key=key,
                        bootstrap_checksum=hashlib.sha256(content).hexdigest(),
                    )
                tokens[tenant["tenant_id"]] = state.token
            committer = (
                BatchedWalCommitter(
                    payload, config=WalBatchingConfig(batch_window_ms=request.batch_window_ms)
                )
                if request.mode == "batch"
                else SingleRecordWalCommitter(payload)
            )
            self.engine = TenantDatabaseEngine.from_dataset_dir(
                root / "dataset", wal_committer=FencedCommitter(committer, coordinator, tokens)
            )
            self.authority: S3AuthorityStore | None = authority
        else:
            offline = OfflineWalStore(root / "wal")
            committer = (
                BatchedWalCommitter(
                    offline, config=WalBatchingConfig(batch_window_ms=request.batch_window_ms)
                )
                if request.mode == "batch"
                else SingleRecordWalCommitter(offline)
            )
            self.engine = TenantDatabaseEngine.from_dataset_dir(
                root / "dataset", wal_committer=committer, initialize_durability=True
            )
            self.authority = None

    def execute(self, operation: WorkloadOperation) -> Json:
        result: Json = execution_result_to_dict(self.engine.execute(operation))
        result["evidence_scope"] = "aws-s3" if self.cloud else "local-filesystem"
        sequence = result.get("wal_sequence_no")
        if (
            self.cloud
            and result["success"]
            and not result.get("duplicate_transaction")
            and sequence
            and self.request.checkpoint_every
            and sequence % self.request.checkpoint_every == 0
        ):
            try:
                self.checkpoint(operation.tenant_id)
                result["checkpoint_published"] = True
            except Exception:
                result["checkpoint_error"] = "CHECKPOINT_PUBLICATION_FAILED"
        return result

    def checkpoint(self, tenant: str) -> Json:
        if not self.cloud or self.authority is None:
            raise ValueError("Authority checkpoints require the AWS fenced backend")
        started = time.perf_counter()
        with self.engine.executor._lock_for(tenant):
            state, _ = self.coordinator.status(tenant)
            path = self.root / f"checkpoint-{uuid.uuid4().hex}.db"
            try:
                with self.engine.connection_manager.connection_for(TenantId(tenant)) as source:
                    committer = self.engine.wal_committer
                    assert isinstance(committer, FencedCommitter)
                    committer.require_ready(tenant, source)
                    with closing(sqlite3.connect(path)) as target:
                        source.backup(target)
                body = path.read_bytes()
                checksum = hashlib.sha256(body).hexdigest()
                key = f"snapshots/{tenant}/{state.committed_sequence}-{checksum}.db"
                self.authority.put_immutable(key, body)
                self.coordinator.register_checkpoint(
                    state.token,
                    checkpoint_key=key,
                    checkpoint_checksum=checksum,
                    checkpoint_sequence=state.committed_sequence,
                    descriptor_key=state.descriptor_head_key,
                    descriptor_checksum=state.descriptor_head_checksum,
                )
                evidence = {
                    "tenant_id": tenant,
                    "sequence": state.committed_sequence,
                    "key": key,
                    "bytes": len(body),
                    "duration_ms": (time.perf_counter() - started) * 1000,
                }
                self.checkpoints.append(evidence)
                return evidence
            finally:
                if path.exists():
                    path.unlink()

    def close(self) -> None:
        self.engine.close_all()

    def accounting(self) -> Json:
        return {
            "authority_logical_requests": self.authority.counters() if self.authority else None,
            "payload_sdk_requests": self.payload.request_accounting() if self.cloud else None,
            "checkpoints": self.checkpoints,
            "recoveries": self.recoveries,
            "billable_requests": None,
            "aws_cost_usd": None,
        }


class HttpBackend:
    """Static tenant routing to explicitly configured runtime endpoints."""

    def __init__(self, root: Path, request: RunRequest, run_id: str) -> None:
        self.run_id = run_id
        urls = os.environ.get("RUNTIME_URLS", "").split(",")
        self.urls = [url.strip().rstrip("/") for url in urls if url.strip()]
        if not self.urls:
            raise ValueError("RUNTIME_URLS is required for HTTP execution")
        token = os.environ.get("RUNTIME_TOKEN", "")
        self.client = httpx.Client(timeout=60, headers={"Authorization": f"Bearer {token}"})
        self.request = request
        self.recovery_locks = [threading.Lock() for _ in self.urls]
        for index, url in enumerate(self.urls):
            body = {"request": request.model_dump(), "shard": index, "shards": len(self.urls)}
            response = self.client.post(f"{url}/api/runtime/{run_id}/prepare", json=body)
            response.raise_for_status()

    def execute(self, operation: WorkloadOperation) -> Json:
        index = int(hashlib.sha256(operation.tenant_id.encode()).hexdigest(), 16) % len(self.urls)
        response = self.client.post(
            f"{self.urls[index]}/api/runtime/{self.run_id}/execute",
            json=operation_to_dict(operation),
        )
        if response.status_code == 409:
            with self.recovery_locks[index]:
                response = self.client.post(
                    f"{self.urls[index]}/api/runtime/{self.run_id}/execute",
                    json=operation_to_dict(operation),
                )
                if response.status_code == 409:
                    body = {
                        "request": self.request.model_dump(),
                        "shard": index,
                        "shards": len(self.urls),
                    }
                    restored = self.client.post(
                        f"{self.urls[index]}/api/runtime/{self.run_id}/recover", json=body
                    )
                    restored.raise_for_status()
                    response = self.client.post(
                        f"{self.urls[index]}/api/runtime/{self.run_id}/execute",
                        json=operation_to_dict(operation),
                    )
        response.raise_for_status()
        value: Json = response.json()
        return value

    def close(self) -> None:
        try:
            for url in self.urls:
                self.client.post(f"{url}/api/runtime/{self.run_id}/close").raise_for_status()
        finally:
            self.client.close()

    def accounting(self) -> Json:
        runtimes = []
        for url in self.urls:
            try:
                response = self.client.get(f"{url}/api/runtime/{self.run_id}/status")
                response.raise_for_status()
                runtimes.append(response.json())
            except Exception:
                runtimes.append({"status": "unavailable"})
        return {
            "aws_cost_usd": None,
            "routing": "static-sha256-tenant",
            "runtime_count": len(self.urls),
            "runtimes": runtimes,
        }


def postgres_sql(query: str) -> str:
    return re.sub(r":([A-Za-z_][A-Za-z_0-9]*)", r"%(\1)s", query)


class AuroraBackend:
    """PostgreSQL adapter with isolated schemas, pooled connections and atomic mutations.

    The shared logical schema is retained, including its per-tenant audit sequence.
    No local mirror is used for execution. Credentials never enter result artifacts.
    """

    def __init__(self, root: Path, request: RunRequest, run_id: str) -> None:
        dsn = os.environ.get("AURORA_DSN")
        if not dsn:
            raise ValueError("AURORA_DSN is required")
        pool_module = importlib.import_module("psycopg_pool")
        rows = importlib.import_module("psycopg.rows")
        self.schema = f"run_{run_id}"
        self.pool = pool_module.ConnectionPool(
            dsn,
            min_size=1,
            max_size=request.concurrency,
            kwargs={
                "row_factory": rows.dict_row,
                "connect_timeout": 10,
                "options": f"-c search_path={self.schema} -c statement_timeout=30000",
            },
            open=True,
        )
        self.pool.wait(timeout=20)
        try:
            with self.pool.connection() as connection:
                connection.execute(f'CREATE SCHEMA "{self.schema}"')
                for ddl in (*TABLE_DDL, *INDEX_DDL):
                    connection.execute(ddl.replace("weight REAL", "weight DOUBLE PRECISION"))
                connection.execute(
                    "CREATE TABLE request_identity (tenant_id TEXT, transaction_id TEXT, "
                    "fingerprint TEXT NOT NULL, PRIMARY KEY (tenant_id, transaction_id))"
                )
                connection.execute(
                    "CREATE TABLE audit_counter (tenant_id TEXT PRIMARY KEY, value BIGINT NOT NULL)"
                )
                manifest = json.loads((root / "dataset/manifest.json").read_text())
                for tenant in manifest["tenant_databases"]:
                    with closing(sqlite3.connect(root / "dataset" / tenant["filename"])) as source:
                        for table in TABLES:
                            cursor = source.execute(f'SELECT * FROM "{table}"')
                            columns = [column[0] for column in cursor.description]
                            values = cursor.fetchall()
                            if values:
                                names = ",".join(columns)
                                placeholders = ",".join(["%s"] * len(columns))
                                with connection.cursor() as target:
                                    target.executemany(
                                        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})',
                                        values,
                                    )
                        maximum = source.execute(
                            "SELECT COALESCE(MAX(sequence_no),-1) FROM audit_log"
                        ).fetchone()[0]
                        connection.execute(
                            "INSERT INTO audit_counter VALUES (%s,%s)",
                            (tenant["tenant_id"], maximum),
                        )
                connection.execute("ANALYZE")
        except Exception:
            self.pool.close()
            raise

    def execute(self, operation: WorkloadOperation) -> Json:
        start = time.perf_counter_ns()
        payload = asdict(operation.payload)
        records: list[Json] = []
        duplicate = False
        written = 0
        try:
            with self.pool.connection() as connection:
                if operation.operation_type.value in {"ENTITY_DELTA", "MULTI_ENTITY_TRANSACTION"}:
                    duplicate, written = self._write(connection, operation, payload)
                else:
                    query, params = self._query(operation, payload)
                    records = connection.execute(postgres_sql(query), params).fetchall()
            success, error = True, None
        except Exception as exc:
            success = False
            # Never serialize DSNs or driver connection diagnostics.
            error = (
                "TRANSACTION_CONFLICT"
                if isinstance(exc, ValueError)
                else "POSTGRES_EXECUTION_ERROR"
            )
        return {
            "operation_id": operation.operation_id,
            "tenant_id": operation.tenant_id,
            "operation_type": operation.operation_type.value,
            "success": success,
            "latency_ns": time.perf_counter_ns() - start,
            "records": records,
            "rows_read": len(records),
            "rows_written": written if success else 0,
            "duplicate_transaction": duplicate,
            "error_code": error,
            "conflict": error == "TRANSACTION_CONFLICT",
            "evidence_scope": "postgresql",
            "durability_outcome": (
                "READ"
                if not payload.get("transaction_id")
                else "POSTGRES_COMMITTED"
                if success
                else "REJECTED"
                if error == "TRANSACTION_CONFLICT"
                else "UNKNOWN"
            ),
            "reconciliation_required": bool(payload.get("transaction_id"))
            and not success
            and error != "TRANSACTION_CONFLICT",
            "transaction_id": payload.get("transaction_id"),
        }

    def _query(self, operation: WorkloadOperation, payload: Json) -> tuple[str, Json]:
        mapping = {
            "ENTITY_LOOKUP": "Q1_ENTITY_LOOKUP",
            "STATE_SCAN": "Q2_STATE_SCAN",
            "RELATIONSHIP_EXPANSION": "Q3_RELATIONSHIP_EXPANSION",
            "METADATA_LOOKUP": "Q4_METADATA_LOOKUP",
            "ENTITY_HISTORY": "Q5_ENTITY_HISTORY",
            "COMPLEX_JOIN": "Q6_COMPLEX_JOIN",
            "AGGREGATE_BY_STATE": "Q7_AGGREGATE",
        }
        query = QUERY_CATALOG[mapping[operation.operation_type.value]]
        params = {**payload, "tenant_id": operation.tenant_id}
        if "source_entity_id" in params:
            params["entity_id"] = params["source_entity_id"]
        if (
            operation.operation_type.value == "METADATA_LOOKUP"
            and payload.get("metadata_key") is not None
        ):
            query = query.replace("ORDER BY", "AND metadata_key = :metadata_key ORDER BY")
        if (
            operation.operation_type.value == "AGGREGATE_BY_STATE"
            and payload.get("state_filter") is not None
        ):
            query = query.replace("GROUP BY", "AND state = :state_filter GROUP BY")
        return query, params

    def _write(
        self, connection: Any, operation: WorkloadOperation, payload: Json
    ) -> tuple[bool, int]:
        tenant, tx = operation.tenant_id, payload["transaction_id"]
        items = payload.get("items") or [payload]
        # Idempotency is checked under an identity-specific advisory lock, not a tenant-wide lock.
        connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (tenant + tx,))
        fingerprint = request_fingerprint(operation)
        existing = connection.execute(
            "SELECT fingerprint FROM request_identity WHERE tenant_id=%s AND transaction_id=%s",
            (tenant, tx),
        ).fetchone()
        if existing:
            if existing["fingerprint"] != fingerprint:
                raise ValueError("transaction identity mismatch")
            return True, 0
        states = {}
        for item in sorted(items, key=lambda item: item["entity_id"]):
            row = connection.execute(
                "SELECT * FROM entities WHERE tenant_id=%s AND entity_id=%s FOR UPDATE",
                (tenant, item["entity_id"]),
            ).fetchone()
            if row is None or (
                item.get("expected_version") is not None
                and item["expected_version"] != row["version"]
            ):
                raise ValueError("entity/version conflict")
            states[item["entity_id"]] = row
        last = connection.execute(
            "UPDATE audit_counter SET value=value+%s WHERE tenant_id=%s RETURNING value",
            (len(items), tenant),
        ).fetchone()["value"]
        first = last - len(items) + 1
        timestamp = mutation_timestamp(first)
        connection.execute(
            "INSERT INTO transactions VALUES (%s,%s,'COMMITTED',%s,%s)",
            (tx, tenant, timestamp, timestamp),
        )
        for index, item in enumerate(items):
            entity = item["entity_id"]
            row = states[entity]
            version, value = row["version"] + 1, row["numeric_value"] + item["delta"]
            timestamp = mutation_timestamp(first + index)
            connection.execute(
                "UPDATE entities SET numeric_value=%s,version=%s,updated_at=%s "
                "WHERE tenant_id=%s AND entity_id=%s",
                (value, version, timestamp, tenant, entity),
            )
            connection.execute(
                "INSERT INTO entity_versions VALUES (%s,%s,%s,%s,%s,%s)",
                (tenant, entity, version, row["state"], value, timestamp),
            )
            connection.execute(
                "INSERT INTO transaction_items VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (tx, index + 1, tenant, entity, item["delta"], row["version"], version),
            )
            audit = deterministic_uuid("engine-audit", tenant, tx, index + 1)
            connection.execute(
                "INSERT INTO audit_log VALUES (%s,%s,%s,%s,'ENTITY_UPDATED',%s,%s)",
                (audit, tenant, tx, entity, first + index, timestamp),
            )
        connection.execute(
            "INSERT INTO request_identity VALUES (%s,%s,%s)", (tenant, tx, fingerprint)
        )
        return False, 1 + 4 * len(items)

    def close(self) -> None:
        self.pool.close()

    def accounting(self) -> Json:
        return {
            "schema": self.schema,
            "retention": "schema retained for inspection; explicit cleanup required",
            "aws_cost_usd": None,
            "baseline_validation": "requires live PostgreSQL integration run",
        }
