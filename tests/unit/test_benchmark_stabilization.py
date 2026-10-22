import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_s3_stabilization import MemoryClient

from ephemeraldb.aws.s3_express import S3ExpressWalStore
from ephemeraldb.benchmark import wal_batching as benchmark
from ephemeraldb.benchmark.wal_batching import BenchmarkConfig, _summarize_results
from ephemeraldb.config.settings import load_settings
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.engine.results import ExecutionResult
from ephemeraldb.loadgen.models import OperationType


def config() -> BenchmarkConfig:
    return BenchmarkConfig(
        "test",
        "batch",
        "many-tenants",
        1,
        1,
        5,
        100,
        1024,
        100,
        1,
        "ap-south-1",
        "test",
        "isolated",
        False,
    )


def result(success: bool) -> ExecutionResult:
    return ExecutionResult(
        "op", "tenant", OperationType.ENTITY_DELTA, success, 1, 101, 100, transaction_id="tx"
    )


def test_all_failures_cannot_report_goodput_or_savings() -> None:
    summary = _summarize_results(
        config=config(),
        settings=load_settings(),
        results=[result(False)],
        runtime_ns=1_000_000_000,
        s3_put_count=0,
        bytes_written=0,
        cleanup_result={},
    )
    assert summary["tps"] == 0
    assert summary["transactions_per_put"] is None
    assert summary["cost_safety"]["put_reduction_vs_one_put_per_transaction_percent"] is None


def test_duplicate_retry_is_not_new_write_goodput() -> None:
    summary = _summarize_results(
        config=config(),
        settings=load_settings(),
        results=[replace(result(True), duplicate_transaction=True)],
        runtime_ns=1_000_000_000,
        s3_put_count=0,
        bytes_written=0,
        cleanup_result={},
    )
    assert summary["tps"] == 0


class Clock:
    def __init__(self):
        self.value = 0

    def now(self):
        self.value += 100
        return self.value


def arrange_run(monkeypatch, tmp_path, cleanup_delay=0):
    clock = Clock()
    client = MemoryClient()

    def delete_hook():
        clock.value += cleanup_delay

    client.delete_hook = delete_hook
    settings = replace(load_settings(), s3_express_bucket="test")
    monkeypatch.setattr(benchmark, "load_settings", lambda: settings)
    monkeypatch.setattr(benchmark, "time", SimpleNamespace(perf_counter_ns=clock.now))
    monkeypatch.setattr(
        benchmark, "S3ExpressWalStore", lambda config: S3ExpressWalStore(config, client=client)
    )
    args = benchmark.build_parser().parse_args(
        [
            "--mode",
            "single",
            "--transactions",
            "1",
            "--tenants",
            "1",
            "--dataset-dir",
            str(tmp_path / "dataset"),
            "--results-dir",
            str(tmp_path / "results"),
        ]
    )
    return args, client


def test_cleanup_time_is_excluded_and_real_paths_counted(monkeypatch, tmp_path):
    args, _ = arrange_run(monkeypatch, tmp_path / "fast")
    fast = benchmark.run_benchmark(args)
    args, _ = arrange_run(monkeypatch, tmp_path / "slow", cleanup_delay=10_000_000_000)
    slow = benchmark.run_benchmark(args)
    assert slow["total_runtime_ns"] == fast["total_runtime_ns"]
    assert slow["tps"] == fast["tps"]
    assert slow["phase_durations_ns"]["cleanup"] > fast["phase_durations_ns"]["cleanup"]
    assert slow["cost_safety"]["delete_count"] == 1
    assert slow["cost_safety"]["list_count"] == 2
    assert slow["request_accounting"]["sdk_calls"]["workload.put_object"] == 1
    assert slow["reproducibility"]["workload_sha256"] == fast["reproducibility"]["workload_sha256"]


def test_cleanup_error_still_exports_artifact(monkeypatch, tmp_path):
    args, client = arrange_run(monkeypatch, tmp_path)

    def fail():
        raise RuntimeError("delete failed")

    client.delete_hook = fail
    with pytest.raises(RuntimeError, match="delete failed"):
        benchmark.run_benchmark(args)
    artifact = json.loads(next(args.results_dir.glob("run-*.json")).read_text())
    assert artifact["successful_transactions"] == 1
    assert artifact["cleanup_status"] == "failed"
    assert artifact["retained_prefix"]


def test_primary_exception_survives_drain_failure_and_partial_export(monkeypatch, tmp_path):
    args, _ = arrange_run(monkeypatch, tmp_path)
    close = TenantDatabaseEngine.close_all

    def fail_execute(self, operation):
        raise ValueError("primary execution error")

    def fail_close(self):
        close(self)
        raise RuntimeError("drain also failed")

    monkeypatch.setattr(TenantDatabaseEngine, "execute", fail_execute)
    monkeypatch.setattr(TenantDatabaseEngine, "close_all", fail_close)
    with pytest.raises(ValueError, match="primary execution error"):
        benchmark.run_benchmark(args)
    artifact = json.loads(next(args.results_dir.glob("run-*.json")).read_text())
    assert artifact["primary_error"] == "primary execution error"
    assert "drain also failed" in artifact["teardown_errors"][0]
    assert artifact["run_status"] == "failed"
