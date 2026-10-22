"""Authenticated experiment API and same-origin Flutter hosting.

Run one control-plane process. Experiments run in isolated child processes and keep
their evidence on disk independently of browser sessions.
"""

from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from ephemeraldb.console.backends import TABLES, SQLiteBackend
from ephemeraldb.console.models import FailureRequest, FisRequest, RunRequest
from ephemeraldb.console.storage import RunStore, now, save
from ephemeraldb.loadgen.serialization import operation_from_dict
from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig

Json = dict[str, Any]


def capabilities() -> Json:
    enabled = os.environ.get("ENABLE_AWS_EXECUTION") == "1"
    return {
        "local": {"configured": True, "label": "Local filesystem · real execution"},
        "s3-express": {
            "configured": enabled
            and bool(
                os.environ.get("S3_EXPRESS_BUCKET") and os.environ.get("S3_CHECKPOINT_BUCKET")
            ),
            "label": "AWS S3 · fenced commits",
        },
        "aurora": {
            "configured": enabled and bool(os.environ.get("AURORA_DSN")),
            "label": "Aurora / PostgreSQL",
        },
        "http": {
            "configured": enabled and bool(os.environ.get("RUNTIME_URLS")),
            "label": "EKS runtime · HTTP",
        },
        "fis": {
            "configured": enabled and bool(os.environ.get("FIS_TEMPLATE_IDS")),
            "label": "AWS FIS",
        },
        "region": os.environ.get("AWS_REGION", "ap-south-1"),
        "cloud_status": "configuration only; credentials and resources not verified",
        "fis_templates": [
            value for value in os.environ.get("FIS_TEMPLATE_IDS", "").split(",") if value
        ],
    }


def create_app(root: Path | None = None, web_dir: Path | None = None) -> FastAPI:
    store = RunStore(root or Path(os.environ.get("CONSOLE_DATA_DIR", ".artifacts/console")))
    runtime: dict[str, tuple[SQLiteBackend, int, int]] = {}
    runtime_lock = threading.Lock()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        for backend, _, _ in runtime.values():
            backend.close()

    application = FastAPI(
        title="EphemeralDB experiment console", version="0.2.0", lifespan=lifespan
    )
    application.state.store = store

    @application.exception_handler(FileNotFoundError)
    async def missing(_request: Request, _error: FileNotFoundError) -> PlainTextResponse:
        return PlainTextResponse("Not found", status_code=404)

    @application.exception_handler(ValueError)
    async def invalid(_request: Request, _error: ValueError) -> PlainTextResponse:
        return PlainTextResponse("Invalid identifier or request", status_code=400)

    def authorize(request: Request) -> None:
        token = os.environ.get("CONSOLE_TOKEN")
        if token:
            if not hmac.compare_digest(request.headers.get("authorization", ""), f"Bearer {token}"):
                raise HTTPException(401, "Console token required")
        elif request.client and request.client.host not in {"127.0.0.1", "::1", "testclient"}:
            raise HTTPException(403, "Set CONSOLE_TOKEN before exposing the control API")

    def runtime_auth(request: Request) -> None:
        token = os.environ.get("RUNTIME_TOKEN")
        if not token or not hmac.compare_digest(
            request.headers.get("authorization", ""), f"Bearer {token}"
        ):
            raise HTTPException(401, "Runtime token required")

    auth = [Depends(authorize)]

    @application.get("/healthz")
    def health() -> Json:
        return {"status": "ok"}

    @application.get("/api/capabilities", dependencies=auth)
    def get_capabilities() -> Json:
        return capabilities()

    @application.get("/api/runs", dependencies=auth)
    def list_runs() -> list[Json]:
        return store.list()

    def submit(config: Json, kind: str) -> Json:
        with store.lock:
            if any(run["status"] in {"queued", "running"} for run in store.list()):
                raise HTTPException(409, "An experiment is already active; wait or cancel it")
            run_id = uuid.uuid4().hex
            directory = store.directory(run_id)
            directory.mkdir()
            state: Json = {
                "id": run_id,
                "name": config.get("name", f"{config.get('scenario')} proof"),
                "kind": kind,
                "config": config,
                "status": "queued",
                "created_at": now(),
                "metrics": {},
                "series": [],
                "processed": 0,
            }
            save(directory / "run.json", state)
            with (directory / "worker.log").open("w", encoding="utf-8") as log:
                try:
                    process = subprocess.Popen(
                        [sys.executable, "-m", "ephemeraldb.console.worker", str(directory)],
                        stdout=log,
                        stderr=log,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )

                    def monitor() -> None:
                        exit_code = process.wait()
                        with store.lock:
                            latest = store.get(run_id)
                            if latest["status"] in {"queued", "running"}:
                                latest.update(
                                    status="interrupted",
                                    finished_at=now(),
                                    error=(
                                        f"Worker exited without a terminal snapshot ({exit_code}); "
                                        "partial evidence retained"
                                    ),
                                )
                                save(directory / "run.json", latest)

                    threading.Thread(target=monitor, daemon=True).start()
                except OSError:
                    state.update(status="failed", error="Could not start experiment process")
                    save(directory / "run.json", state)
                    raise HTTPException(500, "Could not start experiment process") from None
            return state

    @application.post("/api/runs", status_code=202, dependencies=auth)
    def start_run(config: RunRequest) -> Json:
        if not capabilities()[config.backend]["configured"]:
            raise HTTPException(409, f"{config.backend} is not configured; no run was started")
        return submit(config.model_dump(), "traffic")

    @application.post("/api/failures", status_code=202, dependencies=auth)
    def start_failure(config: FailureRequest) -> Json:
        return submit(config.model_dump(), "failure")

    @application.get("/api/runs/{run_id}", dependencies=auth)
    def get_run(run_id: str) -> Json:
        return store.get(run_id)

    @application.post("/api/runs/{run_id}/cancel", dependencies=auth)
    def cancel_run(run_id: str) -> Json:
        run = store.get(run_id)
        if run["kind"] != "traffic":
            raise HTTPException(
                409, "Bounded process proofs finish independently; cancellation is unavailable"
            )
        if run["status"] not in {"queued", "running"}:
            raise HTTPException(409, "Run is already terminal")
        (store.directory(run_id) / "cancel").touch()
        return {"status": "cancellation_requested"}

    @application.get("/api/runs/{run_id}/outcomes", dependencies=auth)
    def outcomes(
        run_id: str, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500)
    ) -> Json:
        store.get(run_id)
        return store.outcomes(run_id, offset, limit)

    @application.get("/api/runs/{run_id}/export", dependencies=auth)
    def export(run_id: str, artifact: str = "run") -> FileResponse:
        store.get(run_id)
        filenames = {"run": "run.json", "outcomes": "outcomes.jsonl", "workload": "workload.jsonl"}
        if artifact not in filenames:
            raise HTTPException(400, "Unknown artifact")
        path = store.directory(run_id) / filenames[artifact]
        if not path.exists():
            raise HTTPException(404, "Artifact not available")
        return FileResponse(path, filename=f"{run_id}-{filenames[artifact]}")

    @application.get("/api/runs/{run_id}/tenants", dependencies=auth)
    def tenants(run_id: str) -> Json:
        store.get(run_id)
        manifest = store.directory(run_id) / "dataset/manifest.json"
        return {
            "tenants": json.loads(manifest.read_text())["tenant_databases"],
            "tables": TABLES,
            "scope": "compute-local cache snapshot; not an authoritative serving read",
        }

    @application.get("/api/runs/{run_id}/tables/{table}", dependencies=auth)
    def table_rows(
        run_id: str,
        table: str,
        tenant_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(25, ge=1, le=100),
    ) -> Json:
        run = store.get(run_id)
        if run["config"].get("backend") in {"aurora", "http"}:
            raise HTTPException(
                409,
                "Inspect remote execution data at its runtime; "
                "the local dataset is only the initial seed",
            )
        if table not in TABLES:
            raise HTTPException(400, "Unknown table")
        directory = store.directory(run_id) / "dataset"
        manifest = json.loads((directory / "manifest.json").read_text())
        tenant = next(
            (row for row in manifest["tenant_databases"] if row["tenant_id"] == tenant_id), None
        )
        if tenant is None:
            raise HTTPException(404, "Unknown tenant")
        with closing(
            sqlite3.connect((directory / tenant["filename"]).as_uri() + "?mode=ro", uri=True)
        ) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f'SELECT * FROM "{table}" ORDER BY 1,2 LIMIT ? OFFSET ?', (limit, offset)
            ).fetchall()
            count = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        return {
            "rows": [dict(row) for row in rows],
            "total": count,
            "offset": offset,
            "scope": "local cache inspection",
        }

    @application.get("/metrics", response_class=PlainTextResponse, dependencies=auth)
    def prometheus() -> str:
        runs = store.list()
        return (
            "# HELP ephemeraldb_console_runs Stored experiment count\n"
            "# TYPE ephemeraldb_console_runs gauge\n"
            + f"ephemeraldb_console_runs {len(runs)}\n"
            + "\n".join(
                f'ephemeraldb_run_{metric}{{run_id="{run["id"]}"}} {run["metrics"][metric]}'
                for run in runs[:20]
                for metric in ("successful", "failed", "dropped", "goodput")
                if run.get("metrics", {}).get(metric) is not None
            )
            + "\n"
        )

    @application.post("/api/fis/start", dependencies=auth)
    def fis_start(config: FisRequest) -> Json:
        if (
            not capabilities()["fis"]["configured"]
            or config.template_id not in capabilities()["fis_templates"]
        ):
            raise HTTPException(403, "Template is not explicitly enabled for this console")
        boto3 = importlib.import_module("boto3")

        session = boto3.Session(
            profile_name=os.environ.get("AWS_PROFILE"), region_name=capabilities()["region"]
        )
        client = session.client("fis")
        template = client.get_experiment_template(id=config.template_id)["experimentTemplate"]
        if not any(
            condition.get("source") == "aws:cloudwatch:alarm"
            for condition in template.get("stopConditions", [])
        ):
            raise HTTPException(409, "Template must include a CloudWatch alarm stop condition")
        response = client.start_experiment(
            experimentTemplateId=config.template_id,
            clientToken=uuid.uuid4().hex,
            tags={"Project": "ephemeraldb", "Source": "console"},
        )
        experiment = response["experiment"]
        audit = {
            "id": experiment["id"],
            "template": config.template_id,
            "started_at": now(),
            "state": experiment["state"],
        }
        save(store.root / "fis" / f"{experiment['id']}.json", audit)
        return audit

    @application.get("/api/fis/{experiment_id}", dependencies=auth)
    def fis_status(experiment_id: str) -> Json:
        boto3 = importlib.import_module("boto3")

        if not capabilities()["fis"]["configured"]:
            raise HTTPException(409, "FIS is not configured")
        # Only inspect experiments launched by this console.
        if (
            not experiment_id.isalnum()
            or not (store.root / "fis" / f"{experiment_id}.json").exists()
        ):
            raise HTTPException(404, "Unknown experiment")
        session = boto3.Session(
            profile_name=os.environ.get("AWS_PROFILE"), region_name=capabilities()["region"]
        )
        value: Json = session.client("fis").get_experiment(id=experiment_id)["experiment"]
        return value

    @application.post("/api/runtime/{run_id}/prepare", dependencies=[Depends(runtime_auth)])
    def runtime_prepare(run_id: str, body: Json) -> Json:
        return prepare_runtime(run_id, body, recover=False)

    @application.post("/api/runtime/{run_id}/recover", dependencies=[Depends(runtime_auth)])
    def runtime_recover(run_id: str, body: Json) -> Json:
        return prepare_runtime(run_id, body, recover=True)

    def prepare_runtime(run_id: str, body: Json, *, recover: bool) -> Json:
        directory = store.directory(run_id)
        config = RunRequest.model_validate(body["request"])
        if not capabilities()["s3-express"]["configured"]:
            raise HTTPException(
                409, "Runtime needs configured S3 buckets and ENABLE_AWS_EXECUTION=1"
            )
        shards, shard = int(body["shards"]), int(body["shard"])
        if not 0 <= shard < shards <= 32:
            raise HTTPException(400, "Invalid shard")
        with runtime_lock:
            if run_id in runtime or (directory.exists() and not recover):
                raise HTTPException(409, "Run already prepared; fresh namespace required")
            if not (directory / "dataset/manifest.json").exists():
                generate_dataset(
                    directory / "dataset",
                    DatasetConfig(
                        tenant_count=config.tenants,
                        entities_per_tenant=config.entities,
                        seed=config.seed,
                    ),
                )
            manifest_path = directory / "dataset/manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["tenant_databases"] = [
                tenant
                for tenant in manifest["tenant_databases"]
                if int(hashlib.sha256(tenant["tenant_id"].encode()).hexdigest(), 16) % shards
                == shard
            ]
            manifest["tenant_ids"] = [
                tenant["tenant_id"] for tenant in manifest["tenant_databases"]
            ]
            manifest["tenant_count"] = len(manifest["tenant_ids"])
            save(manifest_path, manifest)
            backend = SQLiteBackend(
                directory,
                config.model_copy(update={"backend": "s3-express"}),
                run_id,
                recover=recover,
            )
            runtime[run_id] = (backend, shard, shards)
        return {"status": "ready", "tenants": len(manifest["tenant_ids"])}

    @application.post("/api/runtime/{run_id}/execute", dependencies=[Depends(runtime_auth)])
    def runtime_execute(run_id: str, body: Json) -> Json:
        store.directory(run_id)
        if run_id not in runtime:
            raise HTTPException(
                409, "Runtime not prepared; recovery requires explicit ownership takeover"
            )
        backend, shard, shards = runtime[run_id]
        operation = operation_from_dict(body)
        if int(hashlib.sha256(operation.tenant_id.encode()).hexdigest(), 16) % shards != shard:
            raise HTTPException(409, "Tenant belongs to another runtime")
        return backend.execute(operation)

    @application.post("/api/runtime/{run_id}/close", dependencies=[Depends(runtime_auth)])
    def runtime_close(run_id: str) -> Json:
        store.directory(run_id)
        with runtime_lock:
            item = runtime.pop(run_id, None)
            if item:
                item[0].close()
        return {"status": "closed", "retention": "S3 authority and payloads retained"}

    @application.get("/api/runtime/{run_id}/status", dependencies=[Depends(runtime_auth)])
    def runtime_status(run_id: str) -> Json:
        store.directory(run_id)
        if run_id not in runtime:
            raise HTTPException(409, "Runtime is not active")
        backend, shard, shards = runtime[run_id]
        return {
            "status": "ready",
            "shard": shard,
            "shards": shards,
            "accounting": backend.accounting(),
            "engine": {
                "tenants": backend.engine.registry.tenant_count,
                "connections": backend.engine.connection_manager.open_connection_count,
            },
        }

    @application.post("/api/fis/{experiment_id}/stop", dependencies=auth)
    def fis_stop(experiment_id: str) -> Json:
        if not capabilities()["fis"]["configured"]:
            raise HTTPException(409, "FIS is not configured")
        if (
            not experiment_id.isalnum()
            or not (store.root / "fis" / f"{experiment_id}.json").exists()
        ):
            raise HTTPException(404, "Unknown experiment")
        boto3 = importlib.import_module("boto3")
        session = boto3.Session(
            profile_name=os.environ.get("AWS_PROFILE"), region_name=capabilities()["region"]
        )
        value: Json = session.client("fis").stop_experiment(id=experiment_id)["experiment"]
        return value

    resolved_web = web_dir or Path(os.environ.get("CONSOLE_WEB_DIR", "frontend/build/web"))
    if resolved_web.is_dir():
        application.mount("/", StaticFiles(directory=resolved_web, html=True), name="flutter")
    return application


def main() -> None:
    import uvicorn

    host = os.environ.get("CONSOLE_HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "localhost", "::1"} and not os.environ.get("CONSOLE_TOKEN"):
        raise SystemExit("CONSOLE_TOKEN is required when binding beyond localhost")
    uvicorn.run(create_app(), host=host, port=int(os.environ.get("CONSOLE_PORT", "8765")))


if __name__ == "__main__":
    main()
