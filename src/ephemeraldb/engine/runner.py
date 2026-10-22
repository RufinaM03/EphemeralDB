"""Sequential local workload runner for Phase 3 correctness checks."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

from ephemeraldb.config.settings import load_settings
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.results import ExecutionResult, execution_result_to_dict
from ephemeraldb.loadgen.models import WorkloadOperation, is_read_operation, is_write_operation
from ephemeraldb.loadgen.serialization import iter_workload


@dataclass(frozen=True, slots=True)
class LocalRunSummary:
    operations_attempted: int
    successful: int
    failed: int
    conflicts: int
    duplicates: int
    read_count: int
    write_count: int
    rows_read: int
    rows_written: int
    total_duration_ns: int
    final_tenant_checksums: dict[str, str]
    result_path: str | None


def run_workload(
    engine: TenantDatabaseEngine,
    workload: Iterable[WorkloadOperation],
    *,
    output_path: Path | None = None,
) -> LocalRunSummary:
    started_at = time.perf_counter_ns()
    operations_attempted = 0
    successful = 0
    failed = 0
    conflicts = 0
    duplicates = 0
    read_count = 0
    write_count = 0
    rows_read = 0
    rows_written = 0

    output_handle = None
    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_handle = output_path.open("w", encoding="utf-8", newline="\n")

    try:
        for operation in workload:
            result = engine.execute(operation)
            _update_counts(
                result,
                operation,
                counters := {
                    "operations_attempted": operations_attempted,
                    "successful": successful,
                    "failed": failed,
                    "conflicts": conflicts,
                    "duplicates": duplicates,
                    "read_count": read_count,
                    "write_count": write_count,
                    "rows_read": rows_read,
                    "rows_written": rows_written,
                },
            )
            operations_attempted = counters["operations_attempted"]
            successful = counters["successful"]
            failed = counters["failed"]
            conflicts = counters["conflicts"]
            duplicates = counters["duplicates"]
            read_count = counters["read_count"]
            write_count = counters["write_count"]
            rows_read = counters["rows_read"]
            rows_written = counters["rows_written"]
            if output_handle is not None:
                output_handle.write(
                    json.dumps(
                        execution_result_to_dict(result),
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )
                output_handle.write("\n")
    finally:
        if output_handle is not None:
            output_handle.close()

    final_checksums = {
        tenant_id.value: engine.calculate_tenant_checksum(tenant_id)
        for tenant_id in engine.registry.tenant_ids
    }
    finished_at = time.perf_counter_ns()
    return LocalRunSummary(
        operations_attempted=operations_attempted,
        successful=successful,
        failed=failed,
        conflicts=conflicts,
        duplicates=duplicates,
        read_count=read_count,
        write_count=write_count,
        rows_read=rows_read,
        rows_written=rows_written,
        total_duration_ns=finished_at - started_at,
        final_tenant_checksums=final_checksums,
        result_path=None if output_path is None else str(output_path),
    )


def _update_counts(
    result: ExecutionResult,
    operation: WorkloadOperation,
    counters: dict[str, int],
) -> None:
    counters["operations_attempted"] += 1
    counters["successful"] += int(result.success)
    counters["failed"] += int(not result.success)
    counters["conflicts"] += int(result.conflict)
    counters["duplicates"] += int(result.duplicate_transaction)
    counters["read_count"] += int(is_read_operation(operation.operation_type))
    counters["write_count"] += int(is_write_operation(operation.operation_type))
    counters["rows_read"] += result.rows_read
    counters["rows_written"] += result.rows_written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a workload sequentially against local SQLite DBs."
    )
    parser.add_argument("--dataset-dir", required=True, type=Path)
    parser.add_argument("--workload", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-open-connections", type=int, default=32)
    parser.add_argument("--busy-timeout-ms", type=int, default=5000)
    parser.add_argument("--wal-root", type=Path)
    parser.add_argument("--wal-store", choices=("none", "local", "s3-express"))
    parser.add_argument("--wal-persistence-mode", choices=("single", "batch"))
    parser.add_argument(
        "--initialize-durability",
        action="store_true",
        help="assert exclusive use of a NEW empty remote namespace",
    )
    args = parser.parse_args(argv)
    settings = load_settings()
    if args.wal_store is not None:
        settings = type(settings)(
            app_env=settings.app_env,
            aws_region=settings.aws_region,
            aws_profile=settings.aws_profile,
            wal_store=args.wal_store,
            wal_persistence_mode=(args.wal_persistence_mode or settings.wal_persistence_mode),
            s3_express_bucket=settings.s3_express_bucket,
            s3_express_zone_id=settings.s3_express_zone_id,
            s3_express_prefix=settings.s3_express_prefix,
            wal_batch_window_ms=settings.wal_batch_window_ms,
            wal_batch_max_records=settings.wal_batch_max_records,
            wal_batch_max_bytes=settings.wal_batch_max_bytes,
            wal_batch_max_queue_records=settings.wal_batch_max_queue_records,
            wal_batch_flush_on_shutdown=settings.wal_batch_flush_on_shutdown,
            aws_connect_timeout_seconds=settings.aws_connect_timeout_seconds,
            aws_read_timeout_seconds=settings.aws_read_timeout_seconds,
            aws_max_attempts=settings.aws_max_attempts,
            aws_retry_mode=settings.aws_retry_mode,
            cache_dir=settings.cache_dir,
            log_level=settings.log_level,
            benchmark_seed=settings.benchmark_seed,
        )
    elif args.wal_persistence_mode is not None:
        settings = type(settings)(
            app_env=settings.app_env,
            aws_region=settings.aws_region,
            aws_profile=settings.aws_profile,
            wal_store=settings.wal_store,
            wal_persistence_mode=args.wal_persistence_mode,
            s3_express_bucket=settings.s3_express_bucket,
            s3_express_zone_id=settings.s3_express_zone_id,
            s3_express_prefix=settings.s3_express_prefix,
            wal_batch_window_ms=settings.wal_batch_window_ms,
            wal_batch_max_records=settings.wal_batch_max_records,
            wal_batch_max_bytes=settings.wal_batch_max_bytes,
            wal_batch_max_queue_records=settings.wal_batch_max_queue_records,
            wal_batch_flush_on_shutdown=settings.wal_batch_flush_on_shutdown,
            aws_connect_timeout_seconds=settings.aws_connect_timeout_seconds,
            aws_read_timeout_seconds=settings.aws_read_timeout_seconds,
            aws_max_attempts=settings.aws_max_attempts,
            aws_retry_mode=settings.aws_retry_mode,
            cache_dir=settings.cache_dir,
            log_level=settings.log_level,
            benchmark_seed=settings.benchmark_seed,
        )

    engine = TenantDatabaseEngine.from_settings(
        args.dataset_dir,
        settings,
        max_open_connections=args.max_open_connections,
        busy_timeout_ms=args.busy_timeout_ms,
        wal_root=args.wal_root,
        initialize_durability=args.initialize_durability,
    )
    try:
        summary = run_workload(
            engine,
            iter_workload(args.workload),
            output_path=args.output,
        )
    finally:
        engine.close_all()

    print(json.dumps(asdict(summary), ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
