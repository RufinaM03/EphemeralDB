from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from ephemeraldb.console.app import create_app
from ephemeraldb.console.models import RunRequest
from ephemeraldb.console.storage import RunStore, now, save
from ephemeraldb.console.worker import run_traffic, summarize


def test_console_authentication_cloud_gate_and_path_safety(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setenv("CONSOLE_TOKEN", "test-secret")
    monkeypatch.delenv("ENABLE_AWS_EXECUTION", raising=False)
    client = TestClient(create_app(tmp_path))
    assert client.get("/api/runs").status_code == 401
    headers = {"Authorization": "Bearer test-secret"}
    assert client.get("/api/runs", headers=headers).json() == []
    assert (
        client.post("/api/runs", headers=headers, json={"backend": "s3-express"}).status_code == 409
    )
    assert client.get("/api/runs/not-a-run", headers=headers).status_code == 400
    assert client.post("/api/runs", headers=headers, json={"tenants": 10000}).status_code == 422
    assert client.post("/api/runtime/" + "a" * 32 + "/close", json={}).status_code == 401
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("mode", ["single", "batch"])
def test_real_console_traffic_raw_evidence_and_table_inspection(
    tmp_path: Path, mode: str, monkeypatch: Any
) -> None:
    monkeypatch.delenv("CONSOLE_TOKEN", raising=False)
    root = tmp_path / ("a" * 32)
    root.mkdir()
    config = RunRequest(mode=mode, duration=1, warmup=1, rate=6, tenants=2, entities=8)
    state = {
        "id": "a" * 32,
        "name": "test",
        "config": config.model_dump(),
        "status": "running",
        "created_at": now(),
        "kind": "traffic",
    }
    run_traffic(root, state)
    state["status"] = "completed"
    save(root / "run.json", state)
    outcomes = [json.loads(line) for line in (root / "outcomes.jsonl").read_text().splitlines()]
    assert len(outcomes) == 12
    assert state["metrics"]["attempted"] == 6
    assert state["metrics"]["successful"] == 6
    assert state["metrics"]["aws_cost_usd"] is None
    assert all(row["evidence_scope"] == "local-filesystem" for row in outcomes)
    assert all(not row.get("remote_durable") for row in outcomes)
    client = TestClient(create_app(tmp_path))
    tenant = client.get(f"/api/runs/{'a' * 32}/tenants").json()["tenants"][0]["tenant_id"]
    result = client.get(
        f"/api/runs/{'a' * 32}/tables/entities", params={"tenant_id": tenant, "limit": 3}
    ).json()
    assert len(result["rows"]) == 3
    assert result["total"] == 8
    assert (
        client.get(
            f"/api/runs/{'a' * 32}/tables/sqlite_master", params={"tenant_id": tenant}
        ).status_code
        == 400
    )
    assert (
        client.get(f"/api/runs/{'a' * 32}/outcomes?offset=3&limit=2").json()["rows"]
        == outcomes[3:5]
    )
    assert client.get(f"/api/runs/{'a' * 32}/export?artifact=outcomes").status_code == 200
    assert client.get(f"/api/runs/{'a' * 32}/export?artifact=worker.log").status_code == 400
    assert "ephemeraldb_run_successful" in client.get("/metrics").text


def test_summary_excludes_warmup_failures_duplicates_and_drops_from_goodput() -> None:
    rows = [
        {"phase": "WARMUP", "success": True, "end_to_end_ms": 999},
        {"phase": "MEASUREMENT", "success": True, "end_to_end_ms": 4},
        {
            "phase": "MEASUREMENT",
            "success": True,
            "duplicate_transaction": True,
            "end_to_end_ms": 1,
        },
        {"phase": "MEASUREMENT", "success": False, "error_code": "OVERLOAD_DROPPED"},
        {"phase": "MEASUREMENT", "success": False, "conflict": True, "end_to_end_ms": 5},
    ]
    result = summarize(rows, 2)
    assert (result["successful"], result["failed"], result["dropped"], result["duplicates"]) == (
        1,
        1,
        1,
        1,
    )
    assert result["goodput"] == 0.5
    assert result["p99_ms"] == 4


def test_run_store_rejects_traversal_and_reads_partial_outcome_lines(tmp_path: Path) -> None:
    store = RunStore(tmp_path)
    with pytest.raises(ValueError):
        store.directory("../outside")
    directory = store.directory("f" * 32)
    directory.mkdir()
    (directory / "outcomes.jsonl").write_text('{"success": true}\n{"unfinished":', encoding="utf-8")
    assert store.outcomes("f" * 32, 0, 20)["rows"] == [{"success": True}]


def test_cancel_before_admission_retains_dataset_and_zero_outcomes(tmp_path: Path) -> None:
    (tmp_path / "cancel").touch()
    state = {
        "id": "b" * 32,
        "status": "running",
        "config": RunRequest(duration=1, warmup=0, rate=5).model_dump(),
    }
    run_traffic(tmp_path, state)
    assert state["status"] == "cancelled"
    assert state["metrics"]["attempted"] == 0
    assert (tmp_path / "dataset/manifest.json").exists()


@pytest.mark.parametrize("mode", ["single", "batch"])
def test_console_fenced_checkpoint_and_fresh_cache_recovery(
    tmp_path: Path, monkeypatch: Any, mode: str
) -> None:
    """Exercise real adapters over fake S3 transports; this is not live AWS evidence."""
    from test_phase8_s3_authority import _S3Client
    from test_phase8_s3_payload import _Client

    from ephemeraldb.console import backends
    from ephemeraldb.ownership.s3_payload import FencedS3ExpressWalStore
    from ephemeraldb.ownership.s3_store import S3AuthorityStore
    from ephemeraldb.recovery.demo import operation
    from ephemeraldb.schema.generator import generate_dataset
    from ephemeraldb.schema.models import DatasetConfig

    authority_client, payload_client = _S3Client(), _Client()
    monkeypatch.setenv("S3_EXPRESS_BUCKET", "example--zone--x-s3")
    monkeypatch.setenv("S3_CHECKPOINT_BUCKET", "checkpoints")
    monkeypatch.setattr(
        backends,
        "S3AuthorityStore",
        lambda settings: S3AuthorityStore(settings, client=authority_client),
    )
    monkeypatch.setattr(
        backends,
        "FencedS3ExpressWalStore",
        lambda config: FencedS3ExpressWalStore(config, client=payload_client),
    )
    config = RunRequest(backend="s3-express", mode=mode, checkpoint_every=1, tenants=2, entities=5)
    original = tmp_path / "original"
    generate_dataset(
        original / "dataset",
        DatasetConfig(
            tenant_count=2,
            entities_per_tenant=4,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
        ),
    )
    first = backends.SQLiteBackend(original, config, "c" * 32)
    write = operation(0, 1)
    result = first.execute(write)
    assert result["success"], result
    assert result.get("checkpoint_published"), result
    checksum = first.engine.calculate_tenant_checksum(write.tenant_id)
    first.close()
    fresh = tmp_path / "fresh-cache"
    generate_dataset(
        fresh / "dataset",
        DatasetConfig(
            tenant_count=2,
            entities_per_tenant=4,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
        ),
    )
    recovered = backends.SQLiteBackend(fresh, config, "c" * 32, recover=True)
    try:
        assert recovered.engine.calculate_tenant_checksum(write.tenant_id) == checksum
        assert recovered.execute(write)["duplicate_transaction"] is True
        assert recovered.execute(operation(0, 2))["success"] is True
        assert recovered.recoveries[0]["epoch"] == 2
    finally:
        recovered.close()


def test_console_subprocess_admission_cancel_and_persistence(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.delenv("CONSOLE_TOKEN", raising=False)
    client = TestClient(create_app(tmp_path))
    response = client.post("/api/runs", json={"duration": 2, "warmup": 0, "rate": 10})
    assert response.status_code == 202
    run_id = response.json()["id"]
    assert client.post("/api/runs", json={}).status_code == 409
    assert client.post(f"/api/runs/{run_id}/cancel").status_code == 200
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        state = client.get(f"/api/runs/{run_id}").json()
        if state["status"] not in {"queued", "running"}:
            break
        time.sleep(0.05)
    assert state["status"] == "cancelled", state
    reopened = TestClient(create_app(tmp_path))
    assert reopened.get("/api/runs").json()[0]["status"] == "cancelled"
