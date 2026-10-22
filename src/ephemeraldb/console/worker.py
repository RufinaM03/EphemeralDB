"""Isolated, rate-controlled experiment execution with bounded admission and partial evidence."""

from __future__ import annotations

import hashlib
import json
import math
import platform
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

from ephemeraldb.console.backends import AuroraBackend, HttpBackend, SQLiteBackend
from ephemeraldb.console.models import RunRequest
from ephemeraldb.console.storage import now, read, save
from ephemeraldb.loadgen.catalog import DatasetIdentityCatalog
from ephemeraldb.loadgen.generator import WorkloadGenerator
from ephemeraldb.loadgen.models import BenchmarkPhase, WorkloadOperation
from ephemeraldb.loadgen.profiles import get_builtin_profile
from ephemeraldb.loadgen.schedule import WorkloadSchedule, WorkloadSegment
from ephemeraldb.loadgen.serialization import canonical_operation_json
from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig

Json = dict[str, Any]


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 3)


def summarize(outcomes: list[Json], seconds: float) -> Json:
    measured = [row for row in outcomes if row["phase"] == "MEASUREMENT"]
    admitted = [row for row in measured if row.get("error_code") != "OVERLOAD_DROPPED"]
    successful = [
        row for row in admitted if row["success"] and not row.get("duplicate_transaction")
    ]
    latencies = [row["end_to_end_ms"] for row in successful]
    groups = {
        "success": successful,
        "conflict": [row for row in admitted if row.get("conflict")],
        "unknown": [
            row
            for row in admitted
            if row.get("durability_outcome") in {"UNKNOWN", "COMMIT_DECISION_UNKNOWN"}
        ],
        "other_failure": [
            row
            for row in admitted
            if not row["success"]
            and not row.get("conflict")
            and row.get("durability_outcome") not in {"UNKNOWN", "COMMIT_DECISION_UNKNOWN"}
        ],
    }
    return {
        "attempted": len(measured),
        "completed": len(admitted),
        "successful": len(successful),
        "failed": sum(not row["success"] for row in admitted),
        "dropped": len(measured) - len(admitted),
        "duplicates": sum(bool(row.get("duplicate_transaction")) for row in admitted),
        "conflicts": sum(bool(row.get("conflict")) for row in admitted),
        "goodput": round(len(successful) / max(seconds, 0.001), 2),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "measurement_seconds": round(seconds, 3),
        "wal_bytes": sum(row.get("wal_bytes", 0) for row in admitted),
        "aws_cost_usd": None,
        "latency_groups": {
            name: {
                "count": len(rows),
                "p95_ms": percentile(
                    [row["end_to_end_ms"] for row in rows if "end_to_end_ms" in row], 0.95
                ),
            }
            for name, rows in groups.items()
        },
        "acknowledged_write_loss": None,
        "latency_population": (
            "successful nonduplicate measurement operations; includes scheduler delay"
        ),
    }


def run_traffic(root: Path, state: Json) -> None:
    request = RunRequest.model_validate(state["config"])
    generated = generate_dataset(
        root / "dataset",
        DatasetConfig(
            tenant_count=request.tenants, entities_per_tenant=request.entities, seed=request.seed
        ),
    )
    catalog = DatasetIdentityCatalog.from_path(generated.manifest_path)
    profile = get_builtin_profile(request.profile, seed=request.seed).with_overrides(
        seed=request.seed,
        virtual_client_count=request.concurrency,
        target_operations_per_second=request.rate,
        duration_seconds=request.duration,
    )
    segments = []
    if request.warmup:
        segments.append(
            WorkloadSegment(
                "warmup", BenchmarkPhase.WARMUP, request.warmup, request.rate, request.rate
            )
        )
    segments.append(
        WorkloadSegment(
            "measurement", BenchmarkPhase.MEASUREMENT, request.duration, request.rate, request.rate
        )
    )
    schedule = WorkloadSchedule("console-constant", tuple(segments))
    operations = list(WorkloadGenerator(catalog, profile, schedule).iter_operations())
    workload = "\n".join(canonical_operation_json(operation) for operation in operations) + "\n"
    (root / "workload.jsonl").write_text(workload, encoding="utf-8")
    state.update(
        dataset_checksum=generated.manifest.state_checksum,
        workload_checksum=hashlib.sha256(workload.encode()).hexdigest(),
        evidence_scope={
            "local": "local-filesystem",
            "s3-express": "aws-s3",
            "aurora": "postgresql",
            "http": "remote-runtime",
        }[request.backend],
        total_operations=len(operations),
        python=platform.python_version(),
    )
    state["request_envelope"] = {
        "scheduled_operations": len(operations),
        "tenants": request.tenants,
        "cloud_calls_enabled": request.backend != "local",
        "note": "Fenced AWS writes include payload, descriptor, control GET/CAS; "
        "batching only amortizes payloads. SDK retries are not a billable-request bound.",
    }
    save(root / "run.json", state)
    backend: SQLiteBackend | AuroraBackend | HttpBackend
    if request.backend == "aurora":
        backend = AuroraBackend(root, request, state["id"])
    elif request.backend == "http":
        backend = HttpBackend(root, request, state["id"])
    else:
        backend = SQLiteBackend(root, request, state["id"])
    outcomes: list[Json] = []
    pending: set[Future[None]] = set()
    lock = threading.RLock()
    started = time.perf_counter()
    measurement_started: float | None = None
    last_save = 0.0
    bins: dict[int, Json] = {}
    with (root / "outcomes.jsonl").open("w", encoding="utf-8", buffering=1) as output:

        def snapshot() -> None:
            nonlocal last_save
            elapsed = time.perf_counter() - (measurement_started or started)
            state.update(
                updated_at=now(),
                phase="MEASUREMENT" if measurement_started else "WARMUP",
                processed=len(outcomes),
                metrics=summarize(outcomes, elapsed),
                series=[bins[key] for key in sorted(bins)],
            )
            save(root / "run.json", state)
            last_save = time.perf_counter()

        def record(row: Json) -> None:
            with lock:
                output.write(json.dumps(row, default=str) + "\n")
                outcomes.append(row)
                if row["phase"] == "MEASUREMENT":
                    bucket = int(row["completion_offset_ms"] / 1000)
                    point = bins.setdefault(
                        bucket,
                        {
                            "second": bucket,
                            "successful": 0,
                            "failed": 0,
                            "dropped": 0,
                            "latency_sum_ms": 0.0,
                            "count": 0,
                        },
                    )
                    point[
                        "successful"
                        if row["success"]
                        else "dropped"
                        if row.get("error_code") == "OVERLOAD_DROPPED"
                        else "failed"
                    ] += 1
                    point["count"] += 1
                    point["latency_sum_ms"] += row["end_to_end_ms"]
                if time.perf_counter() - last_save > 0.5:
                    snapshot()

        def execute(operation: WorkloadOperation, due: float, origin: float) -> None:
            try:
                row = backend.execute(operation)
            except Exception:
                row = {
                    "operation_id": operation.operation_id,
                    "tenant_id": operation.tenant_id,
                    "operation_type": operation.operation_type.value,
                    "success": False,
                    "error_code": "BACKEND_OUTCOME_UNKNOWN",
                    "durability_outcome": "UNKNOWN",
                    "reconciliation_required": True,
                }
            finished = time.perf_counter()
            row.update(
                phase=operation.benchmark_phase.value,
                end_to_end_ms=round((finished - due) * 1000, 3),
                completion_offset_ms=round((finished - origin) * 1000, 3),
                scheduled_offset_ms=operation.scheduled_offset_ms,
            )
            record(row)

        try:
            with ThreadPoolExecutor(max_workers=request.concurrency) as executor:
                for operation in operations:
                    if (root / "cancel").exists():
                        state["status"] = "cancelled"
                        break
                    if (
                        operation.benchmark_phase == BenchmarkPhase.MEASUREMENT
                        and measurement_started is None
                    ):
                        wait(pending)
                        pending.clear()
                        measurement_started = time.perf_counter()
                        state["measurement_started_at"] = now()
                    origin = measurement_started or started
                    offset = operation.scheduled_offset_ms / 1000
                    if measurement_started is not None:
                        offset -= request.warmup
                    due = origin + offset
                    while time.perf_counter() < due:
                        if (root / "cancel").exists():
                            break
                        time.sleep(min(0.02, max(0, due - time.perf_counter())))
                    if (root / "cancel").exists():
                        state["status"] = "cancelled"
                        break
                    pending = {future for future in pending if not future.done()}
                    if len(pending) >= request.concurrency * 2:
                        record(
                            {
                                "operation_id": operation.operation_id,
                                "tenant_id": operation.tenant_id,
                                "operation_type": operation.operation_type.value,
                                "success": False,
                                "error_code": "OVERLOAD_DROPPED",
                                "phase": operation.benchmark_phase.value,
                                "end_to_end_ms": 0,
                                "completion_offset_ms": (time.perf_counter() - origin) * 1000,
                            }
                        )
                    else:
                        pending.add(executor.submit(execute, operation, due, origin))
            with lock:
                snapshot()
                elapsed = time.perf_counter() - (measurement_started or started)
                state["metrics"] = summarize(
                    outcomes,
                    max(request.duration, elapsed) if state["status"] != "cancelled" else elapsed,
                )
                state["accounting"] = backend.accounting()
        finally:
            backend.close()


def main() -> None:
    root = Path(sys.argv[1]).resolve()
    state = read(root / "run.json")
    state.update(status="running", started_at=now(), updated_at=now())
    save(root / "run.json", state)
    try:
        if state["kind"] == "traffic":
            run_traffic(root, state)
        else:
            from ephemeraldb.ownership.demo import run_cache_loss_demo, run_stale_owner_demo

            config = state["config"]
            if config["scenario"] == "stale-owner":
                evidence = run_stale_owner_demo(root / "proof", mode=config["mode"])
            else:
                evidence = run_cache_loss_demo(
                    root / "proof", mode=config["mode"], ack_loss=config["scenario"] == "ack-loss"
                )
            state.update(evidence=evidence, evidence_scope="offline-process-proof")
        if state["status"] != "cancelled":
            state["status"] = "completed"
    except Exception as exc:
        # Keep details in local operator logs; public artifacts never expose secrets.
        print(f"Experiment failed: {type(exc).__name__}", file=sys.stderr)
        state.update(
            status="failed",
            error=f"{type(exc).__name__}: execution failed; check environment and worker log",
        )
    finally:
        state.update(finished_at=now(), updated_at=now())
        if state["kind"] == "traffic" and state["config"].get("backend") == "s3-express":
            try:
                from ephemeraldb.config.settings import load_settings
                from ephemeraldb.ownership.s3_store import S3AuthorityStore

                settings = load_settings()
                from dataclasses import replace

                settings = replace(
                    settings,
                    s3_checkpoint_prefix=(f"{settings.s3_checkpoint_prefix}/console/{state['id']}"),
                )
                authority = S3AuthorityStore(settings)
                for filename in ("outcomes.jsonl", "workload.jsonl"):
                    path = root / filename
                    if path.exists():
                        authority.put_immutable(f"reports/{filename}", path.read_bytes())
                state["artifact_export"] = {
                    "status": "exported",
                    "bucket": authority.bucket,
                    "prefix": authority.prefix + "/reports/",
                }
                authority.put_immutable("reports/run.json", json.dumps(state, default=str).encode())
            except Exception:
                state["artifact_export"] = {
                    "status": "unknown-or-failed",
                    "local_evidence_retained": True,
                }
        save(root / "run.json", state)


if __name__ == "__main__":
    main()
