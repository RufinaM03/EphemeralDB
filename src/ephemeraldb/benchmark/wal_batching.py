"""Real S3 Express single-vs-batched WAL benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import random
import sqlite3
import statistics
import subprocess
import threading
import time
import uuid
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, replace
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.config.settings import Settings, load_settings
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.results import DurabilityOutcome, ExecutionResult, execution_result_to_dict
from ephemeraldb.loadgen.models import (
    BenchmarkPhase,
    EntityDeltaPayload,
    OperationType,
    WorkloadOperation,
)
from ephemeraldb.schema.generator import deterministic_uuid, generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatch, WalBatchingConfig
from ephemeraldb.wal.codec import canonical_json_bytes
from ephemeraldb.wal.durability import PersistedBatchReceipt, PersistedWalReceipt, StoredWalRecord
from ephemeraldb.wal.models import WalRecord

CONFIRMATION_THRESHOLD_TRANSACTIONS = 1000


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    run_id: str
    mode: str
    pattern: str
    transaction_count: int
    tenant_count: int
    batch_window_ms: int
    max_records_per_batch: int
    max_batch_bytes: int
    max_queue_records: int
    worker_count: int
    aws_region: str
    bucket: str
    prefix: str
    keep_objects: bool


class CountingS3ExpressWalStore:
    def __init__(self, store: S3ExpressWalStore) -> None:
        self.store = store
        self.record_puts = 0
        self.batch_puts = 0
        self.bytes_written = 0
        self.record_objects: list[tuple[str, int]] = []
        self.batch_objects: set[str] = set()
        self._lock = threading.Lock()
        self.logical_persist_calls = 0
        self.failed_persist_calls = 0
        self.created_objects = 0
        self.existing_confirmations = 0

    @property
    def namespace_id(self) -> str:
        return self.store.namespace_id

    @property
    def requires_bootstrap(self) -> bool:
        return self.store.requires_bootstrap

    def inspect_history(self, tenant_id: str) -> tuple[StoredWalRecord, ...]:
        return self.store.inspect_history(tenant_id)

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        with self._lock:
            self.logical_persist_calls += 1
        try:
            receipt = self.store.persist_record(record)
        except Exception:
            with self._lock:
                self.failed_persist_calls += 1
            raise
        with self._lock:
            self.record_puts += 1
            self.created_objects += int(not receipt.already_exists)
            self.existing_confirmations += int(receipt.already_exists)
            self.bytes_written += 0 if receipt.already_exists else receipt.bytes_written
            self.record_objects.append((record.tenant_id, record.sequence_no))
        return receipt

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        return self.store.fetch_record(tenant_id, sequence_no)

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        with self._lock:
            self.logical_persist_calls += 1
        try:
            receipt = self.store.persist_batch(batch)
        except Exception:
            with self._lock:
                self.failed_persist_calls += 1
            raise
        with self._lock:
            self.batch_puts += 1
            self.created_objects += int(not receipt.already_exists)
            self.existing_confirmations += int(receipt.already_exists)
            self.bytes_written += 0 if receipt.already_exists else receipt.bytes_written
            self.batch_objects.add(receipt.key)
        return receipt

    def fetch_batch(self, key: str) -> WalBatch:
        return self.store.fetch_batch(key)

    def cleanup(self) -> dict[str, int]:
        deleted_records = 0
        deleted_batches = 0
        for tenant_id, sequence_no in self.record_objects:
            self.store.delete_record(tenant_id, sequence_no)
            deleted_records += 1
        for key in self.batch_objects:
            self.store.delete_batch(key)
            deleted_batches += 1
        return {
            "deleted_record_objects": deleted_records,
            "deleted_batch_objects": deleted_batches,
        }


def run_benchmark(args: argparse.Namespace) -> dict[str, object]:
    setup_started = time.perf_counter_ns()
    settings = load_settings()
    if args.transactions > CONFIRMATION_THRESHOLD_TRANSACTIONS and not args.yes:
        msg = (
            f"--transactions {args.transactions} exceeds "
            f"{CONFIRMATION_THRESHOLD_TRANSACTIONS}; pass --yes to confirm"
        )
        raise SystemExit(msg)
    if not settings.s3_express_bucket:
        msg = "S3_EXPRESS_BUCKET is required for the real S3 Express WAL benchmark"
        raise SystemExit(msg)

    run_id = uuid.uuid4().hex
    run_prefix = f"{settings.s3_express_prefix}/wal-batching/{run_id}"
    batch_window_ms = args.batch_window_ms
    worker_count = args.workers or min(32, args.transactions, max(args.tenants, 1))
    config = BenchmarkConfig(
        run_id=run_id,
        mode=args.mode,
        pattern=args.pattern,
        transaction_count=args.transactions,
        tenant_count=args.tenants,
        batch_window_ms=batch_window_ms,
        max_records_per_batch=args.max_records_per_batch,
        max_batch_bytes=args.max_batch_bytes,
        max_queue_records=args.max_queue_records,
        worker_count=worker_count,
        aws_region=settings.aws_region,
        bucket=settings.s3_express_bucket,
        prefix=run_prefix,
        keep_objects=args.keep_objects,
    )
    _print_run_envelope(config)

    dataset_dir = args.dataset_dir or (
        Path("benchmark") / "generated" / "wal-batching" / run_id / "dataset"
    )
    generated = generate_dataset(
        dataset_dir,
        DatasetConfig(
            tenant_count=args.tenants,
            entities_per_tenant=max(5, min(args.transactions, 100)),
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
            seed=args.seed if args.seed is not None else settings.benchmark_seed,
        ),
        replace=False,
    )
    s3_store = S3ExpressWalStore(
        S3ExpressWalConfig(
            bucket=settings.s3_express_bucket,
            region=settings.aws_region,
            prefix=run_prefix,
            zone_id=settings.s3_express_zone_id,
            profile=settings.aws_profile,
            connect_timeout_seconds=settings.aws_connect_timeout_seconds,
            read_timeout_seconds=settings.aws_read_timeout_seconds,
            max_attempts=settings.aws_max_attempts,
            retry_mode=settings.aws_retry_mode,
        )
    )
    counting_store = CountingS3ExpressWalStore(s3_store)
    if args.mode == "single":
        engine = TenantDatabaseEngine.from_dataset_dir(
            generated.output_dir,
            wal_store=counting_store,
            max_open_connections=max(worker_count, 1),
            initialize_durability=True,
        )
    else:
        committer = BatchedWalCommitter(
            counting_store,
            config=WalBatchingConfig(
                batch_window_ms=batch_window_ms,
                max_records_per_batch=args.max_records_per_batch,
                max_batch_bytes=args.max_batch_bytes,
                max_queue_records=args.max_queue_records,
                flush_on_shutdown=True,
            ),
        )
        engine = TenantDatabaseEngine.from_dataset_dir(
            generated.output_dir,
            wal_committer=committer,
            max_open_connections=max(worker_count, 1),
            initialize_durability=True,
        )

    operations = _build_operations(
        generated.manifest.tenant_ids,
        generated.output_dir,
        transaction_count=args.transactions,
        pattern=args.pattern,
        seed=args.seed if args.seed is not None else settings.benchmark_seed,
    )
    results: list[ExecutionResult] = []
    cleanup_result: dict[str, int] = {}
    teardown_errors: list[str] = []
    primary_error: Exception | None = None
    measurement_runtime = 0
    setup_finished = time.perf_counter_ns()
    measurement_started = setup_finished
    try:
        # Preflight does not become measured transaction latency or request traffic.
        if engine.executor.guard is not None:
            for tenant in engine.registry.tenant_ids:
                with engine.connection_manager.connection_for(tenant) as connection:
                    engine.executor.guard.require_ready(tenant.value, connection)
        setup_finished = time.perf_counter_ns()
        s3_store.request_phase = "workload"
        measurement_started = time.perf_counter_ns()
        results, measurement_runtime, primary_error = execute_measurement(
            engine, operations, worker_count
        )
    except Exception as exc:
        primary_error = exc
    finally:
        drain_started = time.perf_counter_ns()
        s3_store.request_phase = "drain"
        try:
            engine.close_all()
        except Exception as exc:
            teardown_errors.append(f"drain: {exc}")
        drain_finished = time.perf_counter_ns()
        s3_store.request_phase = "cleanup"
        # Do not erase evidence or race a possibly still-running remote request.
        cleanup_safe = (
            not teardown_errors
            and primary_error is None
            and all(result.success for result in results)
        )
        if not args.keep_objects and cleanup_safe:
            try:
                cleanup_result = counting_store.cleanup()
            except Exception as exc:
                teardown_errors.append(f"cleanup: {exc}")
        cleanup_finished = time.perf_counter_ns()

    summary = _summarize_results(
        config=config,
        settings=settings,
        results=results,
        runtime_ns=measurement_runtime,
        s3_put_count=counting_store.logical_persist_calls,
        bytes_written=counting_store.bytes_written,
        cleanup_result=cleanup_result,
    )
    summary.update(
        {
            "artifact_schema_version": 2,
            "run_status": "failed"
            if primary_error
            or teardown_errors
            or any(not result.success for result in results)
            or len(results) != len(operations)
            else "complete",
            "primary_error": None if primary_error is None else str(primary_error),
            "teardown_errors": teardown_errors,
            "cleanup_status": "retained"
            if args.keep_objects or not cleanup_safe
            else ("failed" if teardown_errors else "complete"),
            "retained_prefix": run_prefix
            if args.keep_objects or not cleanup_safe or teardown_errors
            else None,
            "phase_durations_ns": {
                "setup": setup_finished - setup_started,
                "measurement": measurement_runtime,
                "drain": drain_finished - drain_started,
                "cleanup": cleanup_finished - drain_finished,
            },
            "measurement_started_at_monotonic_ns": measurement_started,
            "request_accounting": s3_store.request_accounting(),
            "logical_persist_calls": counting_store.logical_persist_calls,
            "failed_persist_calls": counting_store.failed_persist_calls,
            "created_objects": counting_store.created_objects,
            "existing_object_confirmations": counting_store.existing_confirmations,
            "submitted_operations": len(operations) if measurement_runtime else 0,
            "operations_without_result": len(operations) - len(results)
            if measurement_runtime
            else 0,
            "reproducibility": _environment(args, settings, generated.output_dir, operations),
            "arrival_model": "submit-all thread pool; bounded workers; not open-loop rate control",
        }
    )
    accounting = s3_store.request_accounting()["sdk_calls"]
    assert isinstance(accounting, dict)
    cost = summary["cost_safety"]
    assert isinstance(cost, dict)
    for metric, operation in (
        ("get_count", "get_object"),
        ("list_count", "list_objects_v2"),
        ("delete_count", "delete_object"),
    ):
        cost[metric] = sum(
            value for key, value in accounting.items() if str(key).endswith("." + operation)
        )
    export_started = time.perf_counter_ns()
    _write_result(summary, args.results_dir)
    durations = summary["phase_durations_ns"]
    assert isinstance(durations, dict)
    durations["artifact_export"] = time.perf_counter_ns() - export_started
    summary["artifact_export_definition"] = (
        "first artifact serialization/write; excludes final timing-metadata rewrite"
    )
    _write_result(summary, args.results_dir)
    _print_human_summary(summary)
    if primary_error is not None:
        raise primary_error
    if teardown_errors:
        raise RuntimeError("; ".join(teardown_errors))
    return summary


def execute_measurement(
    engine: TenantDatabaseEngine,
    operations: list[WorkloadOperation],
    workers: int,
) -> tuple[list[ExecutionResult], int, Exception | None]:
    started = time.perf_counter_ns()
    results: list[ExecutionResult] = []
    error: Exception | None = None

    def execute(operation: WorkloadOperation, submitted: int) -> ExecutionResult:
        queue_latency = time.perf_counter_ns() - submitted
        return replace(engine.execute(operation), executor_queue_latency_ns=queue_latency)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(execute, operation, time.perf_counter_ns()) for operation in operations
        ]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                error = error or exc
    return results, time.perf_counter_ns() - started, error


def _environment(
    args: argparse.Namespace, settings: Settings, dataset: Path, operations: list[WorkloadOperation]
) -> dict[str, object]:
    dependencies: dict[str, str | None] = {}
    for package in ("boto3", "botocore"):
        try:
            dependencies[package] = version(package)
        except PackageNotFoundError:
            dependencies[package] = None
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=5
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        revision = None
    return {
        "seed": args.seed if args.seed is not None else settings.benchmark_seed,
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "dependencies": dependencies,
        "revision": revision,
        "configured_zone_id": settings.s3_express_zone_id,
        "compute_az": None,
        "dataset_manifest_sha256": hashlib.sha256(
            (dataset / "manifest.json").read_bytes()
        ).hexdigest(),
        "workload_sha256": hashlib.sha256(
            canonical_json_bytes([asdict(op) for op in operations])
        ).hexdigest(),
    }


def _build_operations(
    tenant_ids: tuple[str, ...],
    dataset_dir: Path,
    *,
    transaction_count: int,
    pattern: str,
    seed: int,
) -> list[WorkloadOperation]:
    rng = random.Random(seed)
    tenant_entities = {tenant_id: _entity_ids(dataset_dir, tenant_id) for tenant_id in tenant_ids}
    operations: list[WorkloadOperation] = []
    for sequence_no in range(transaction_count):
        if pattern == "single-hot-tenant":
            tenant_id = tenant_ids[0]
        else:
            tenant_id = rng.choice(tenant_ids)
        entity_ids = tenant_entities[tenant_id]
        entity_id = entity_ids[sequence_no % len(entity_ids)]
        transaction_id = f"bench-{sequence_no:06d}-{tenant_id}"
        operations.append(
            WorkloadOperation(
                operation_id=deterministic_uuid("wal-batching", seed, sequence_no, tenant_id),
                sequence_no=sequence_no,
                tenant_id=tenant_id,
                operation_type=OperationType.ENTITY_DELTA,
                scheduled_offset_ms=0,
                benchmark_phase=BenchmarkPhase.MEASUREMENT,
                virtual_client_id=f"client-{sequence_no % max(len(tenant_ids), 1):04d}",
                payload=EntityDeltaPayload(transaction_id, entity_id, 1),
            )
        )
    return operations


def _entity_ids(dataset_dir: Path, tenant_id: str) -> tuple[str, ...]:
    import sqlite3

    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    filename = next(
        tenant["filename"]
        for tenant in manifest["tenant_databases"]
        if tenant["tenant_id"] == tenant_id
    )
    connection = sqlite3.connect(dataset_dir / filename)
    try:
        rows = connection.execute(
            "SELECT entity_id FROM entities WHERE tenant_id = ? ORDER BY entity_id",
            (tenant_id,),
        ).fetchall()
        return tuple(str(row[0]) for row in rows)
    finally:
        connection.close()


def _summarize_results(
    *,
    config: BenchmarkConfig,
    settings: Settings,
    results: list[ExecutionResult],
    runtime_ns: int,
    s3_put_count: int,
    bytes_written: int,
    cleanup_result: dict[str, int],
) -> dict[str, object]:
    successes = [result for result in results if result.success]
    failures = [result for result in results if not result.success]
    new_writes = [
        result
        for result in successes
        if not result.duplicate_transaction and result.transaction_id is not None
    ]
    durable = {
        (result.tenant_id, result.transaction_id)
        for result in results
        if result.remote_durable and not result.duplicate_transaction
    }
    comparable = (
        len(new_writes) == len(results) == config.transaction_count
        and s3_put_count > 0
        and all(result.remote_durable for result in results)
    )
    commit_latencies = [
        result.commit_latency_ns or result.latency_ns
        for result in successes
        if result.commit_latency_ns is not None or result.latency_ns is not None
    ]
    batch_wait_latencies = [
        result.wal_batch_wait_latency_ns
        for result in results
        if result.wal_batch_wait_latency_ns is not None
    ]
    put_latencies = [
        result.s3_put_latency_ns
        for result in results
        if result.s3_put_latency_ns is not None
        and (result.wal_batch_id is None or result.wal_batch_record_index == 0)
    ]
    batch_sizes = [
        result.wal_batch_record_count
        for result in results
        if result.wal_batch_record_index == 0 and result.wal_batch_record_count is not None
    ]
    flush_reasons = [
        result.wal_batch_flush_reason
        for result in results
        if result.wal_batch_record_index == 0 and result.wal_batch_flush_reason is not None
    ]
    return {
        "configuration": asdict(config),
        "aws_region": settings.aws_region,
        "persistence_mode": config.mode,
        "batch_window_ms": config.batch_window_ms,
        "tenant_count": config.tenant_count,
        "transaction_count": config.transaction_count,
        "logical_transactions": len(results),
        "successful_transactions": len(successes),
        "failed_transactions": len(failures),
        "s3_put_count": s3_put_count,
        "transactions_per_put": None if s3_put_count == 0 else len(durable) / s3_put_count,
        "bytes_written": bytes_written,
        "bytes_per_put": None if s3_put_count == 0 else bytes_written / s3_put_count,
        "total_runtime_ns": runtime_ns,
        "tps": 0.0 if runtime_ns == 0 else len(new_writes) / (runtime_ns / 1_000_000_000),
        "attempted_tps": 0.0
        if runtime_ns == 0
        else config.transaction_count / (runtime_ns / 1_000_000_000),
        "successful_new_writes": len(new_writes),
        "unique_confirmed_durable_writes": len(durable),
        "latency_by_outcome_ns": {
            name: _stats([r.latency_ns for r in group])
            for name, group in {
                "success": successes,
                "rejected_or_conflict": [
                    r
                    for r in failures
                    if r.conflict or r.durability_outcome == DurabilityOutcome.REJECTED
                ],
                "unknown": [
                    r for r in failures if r.durability_outcome == DurabilityOutcome.UNKNOWN
                ],
                "other_failure": [
                    r
                    for r in failures
                    if not r.conflict
                    and r.durability_outcome
                    not in {DurabilityOutcome.UNKNOWN, DurabilityOutcome.REJECTED}
                ],
            }.items()
        },
        "executor_queue_latency_ns": _stats([r.executor_queue_latency_ns for r in results]),
        "tenant_wait_latency_ns": _stats([r.tenant_wait_latency_ns for r in results]),
        "commit_latency_ns": _stats(commit_latencies),
        "batch_wait_latency_ns": _stats(batch_wait_latencies),
        "batch_wait_definition": "inclusive admission-to-durability; contains PUT time",
        "end_to_end_latency_ns": _stats(
            [r.executor_queue_latency_ns + r.latency_ns for r in results]
        ),
        "s3_put_latency_ns": _stats(put_latencies),
        "batch_statistics": {
            "average_records_per_batch": _mean(batch_sizes),
            "median_records_per_batch": _median(batch_sizes),
            "maximum_records_per_batch": max(batch_sizes, default=0),
            "flush_reason_percentages": _flush_reason_percentages(flush_reasons),
        },
        "cost_safety": {
            "put_reduction_vs_one_put_per_transaction_percent": (
                None if not comparable else round((1 - (s3_put_count / len(results))) * 100, 2)
            ),
            "get_count": None,
            "comparison_valid": comparable,
            "put_count_definition": (
                "logical persist calls (not SDK retry attempts or billable requests)"
            ),
            "delete_count": sum(cleanup_result.values()),
        },
        "cleanup": cleanup_result,
        "results": [execution_result_to_dict(result) for result in results],
    }


def _stats(values: list[int]) -> dict[str, float | int | None]:
    if not values:
        return {
            "min": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "p99": None,
        }
    sorted_values = sorted(values)
    return {
        "min": sorted_values[0],
        "mean": statistics.fmean(sorted_values),
        "p50": _percentile(sorted_values, 50),
        "p95": _percentile(sorted_values, 95),
        "p99": _percentile(sorted_values, 99),
    }


def _percentile(sorted_values: list[int], percentile: int) -> int:
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = round((percentile / 100) * (len(sorted_values) - 1))
    return sorted_values[index]


def _mean(values: list[int]) -> float:
    return 0.0 if not values else statistics.fmean(values)


def _median(values: list[int]) -> float:
    return 0.0 if not values else float(statistics.median(values))


def _flush_reason_percentages(reasons: Sequence[str | None]) -> dict[str, float]:
    if not reasons:
        return {"time": 0.0, "count": 0.0, "bytes": 0.0, "manual": 0.0}
    total = len(reasons)
    return {
        reason: round((reasons.count(reason) / total) * 100, 2)
        for reason in ("time", "count", "bytes", "manual")
    }


def _write_result(summary: dict[str, object], results_dir: Path) -> Path:
    results_dir.mkdir(parents=True, exist_ok=True)
    configuration = summary["configuration"]
    assert isinstance(configuration, dict)
    run_id = str(configuration["run_id"])
    output_path = results_dir / f"run-{run_id}.json"
    summary["result_path"] = str(output_path)
    output_path.write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _print_run_envelope(config: BenchmarkConfig) -> None:
    estimated_max_puts = config.transaction_count
    print(
        json.dumps(
            {
                "mode": config.mode,
                "transaction_count": config.transaction_count,
                "tenant_count": config.tenant_count,
                "batch_window_ms": config.batch_window_ms,
                "planned_logical_put_calls_at_most": estimated_max_puts,
                "request_bound_note": "excludes SDK retries, startup inspection and verification",
                "bucket": config.bucket,
                "region": config.aws_region,
                "prefix": config.prefix,
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
    )


def _print_human_summary(summary: dict[str, object]) -> None:
    config = summary["configuration"]
    assert isinstance(config, dict)
    mode = str(config["mode"]).upper()
    if mode == "BATCH":
        mode = f"BATCH {config['batch_window_ms']}ms"
    print()
    print(mode)
    print(f"Transactions:       {summary['logical_transactions']}")
    print(f"Successful:         {summary['successful_transactions']}")
    print(f"Failed:             {summary['failed_transactions']}")
    print(f"Logical persist calls: {summary['s3_put_count']}")
    print(f"Durable writes/call:   {summary['transactions_per_put']}")
    print(f"Bytes/PUT:          {summary['bytes_per_put']}")
    print(f"TPS:                {_summary_float(summary, 'tps'):.2f}")
    commit_stats = summary["commit_latency_ns"]
    assert isinstance(commit_stats, dict)
    batch_wait_stats = summary["batch_wait_latency_ns"]
    assert isinstance(batch_wait_stats, dict)
    put_stats = summary["s3_put_latency_ns"]
    assert isinstance(put_stats, dict)
    print(f"p95 commit ns:      {commit_stats['p95']}")
    print(f"p95 batch wait ns:  {batch_wait_stats['p95']}")
    print(f"p95 S3 PUT ns:      {put_stats['p95']}")
    cost = summary["cost_safety"]
    assert isinstance(cost, dict)
    reduction = cost["put_reduction_vs_one_put_per_transaction_percent"]
    print(f"Logical PUT reduction: {'undefined' if reduction is None else str(reduction) + '%'}")
    print(f"Result artifact:    {summary['result_path']}")


def _summary_float(summary: dict[str, object], key: str) -> float:
    value = summary[key]
    assert isinstance(value, int | float)
    return float(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare single-record and batched WAL persistence on S3 Express."
    )
    parser.add_argument("--mode", choices=("single", "batch"), required=True)
    parser.add_argument("--transactions", type=int, default=100)
    parser.add_argument("--tenants", type=int, default=20)
    parser.add_argument(
        "--pattern",
        choices=("many-tenants", "single-hot-tenant"),
        default="many-tenants",
    )
    parser.add_argument("--batch-window-ms", type=int, default=5)
    parser.add_argument("--max-records-per-batch", type=int, default=100)
    parser.add_argument("--max-batch-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--max-queue-records", type=int, default=10000)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--dataset-dir", type=Path)
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("benchmark") / "results" / "wal-batching",
    )
    parser.add_argument("--keep-objects", action="store_true")
    parser.add_argument("--yes", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.transactions < 1:
        parser.error("--transactions must be >= 1")
    if args.tenants < 1:
        parser.error("--tenants must be >= 1")
    if args.batch_window_ms < 0:
        parser.error("--batch-window-ms must be >= 0")
    if args.workers is not None and args.workers < 1:
        parser.error("--workers must be >= 1")
    if args.max_records_per_batch < 1 or args.max_batch_bytes < 1 or args.max_queue_records < 1:
        parser.error("batch record, payload-byte and queue limits must be positive")
    summary = run_benchmark(args)
    return 0 if summary["run_status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
