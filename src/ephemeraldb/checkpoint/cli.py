"""Explicit checkpoint create/list/validate/seal/restore commands."""

from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.aws.s3_standard import S3StandardStore
from ephemeraldb.checkpoint.coordinator import CheckpointCoordinator
from ephemeraldb.checkpoint.models import CheckpointManifest, boundary_key, decode
from ephemeraldb.checkpoint.snapshot import validate_snapshot
from ephemeraldb.checkpoint.store import FileObjectStore, get_bytes
from ephemeraldb.config.settings import load_settings
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.recovery.coordinator import restore
from ephemeraldb.recovery.demo import engine_for
from ephemeraldb.recovery.offline import OfflineWalStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("create", "list", "validate", "seal", "restore"))
    parser.add_argument("--store", choices=("offline", "aws"), default="offline")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--checkpoint-root", type=Path)
    parser.add_argument("--wal-root", type=Path)
    parser.add_argument("--dataset-dir", type=Path)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--manifest-key")
    parser.add_argument("--boundary-key")
    parser.add_argument("--old-writers-stopped", action="store_true")
    parser.add_argument(
        "--initialize-durability",
        action="store_true",
        help="assert exclusive first use of a NEW empty WAL namespace",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result: dict[str, Any] = {
        "artifact_schema_version": 1,
        "command": args.command,
        "namespace": args.namespace,
        "store_type": args.store,
    }
    try:
        settings = load_settings()
        store: Any
        wal: Any
        if args.store == "offline":
            if args.checkpoint_root is None or args.wal_root is None:
                parser.error("offline commands require --checkpoint-root and --wal-root")
            store = FileObjectStore(args.checkpoint_root)
            wal = OfflineWalStore(args.wal_root)
        else:
            settings = replace(settings, wal_store="s3-express")
            print(
                json.dumps(
                    {
                        "bucket": settings.s3_checkpoint_bucket,
                        "wal_bucket": settings.s3_express_bucket,
                        "namespace": args.namespace,
                        "max_object_bytes": 67108864,
                        "max_scan_bytes": 134217728,
                        "max_objects": 1000,
                    }
                )
            )
            store = S3StandardStore(settings)
            wal = S3ExpressWalStore(S3ExpressWalConfig.from_settings(settings))
        coordinator = CheckpointCoordinator(store, args.namespace)
        if args.command in {"create", "seal"}:
            if args.dataset_dir is None:
                parser.error("create/seal require --dataset-dir")
            engine = (
                engine_for(args.dataset_dir, wal, settings.wal_persistence_mode)
                if args.store == "offline"
                else TenantDatabaseEngine.from_settings(
                    args.dataset_dir, settings, initialize_durability=args.initialize_durability
                )
            )
            try:
                result["result"] = (
                    coordinator.create(engine, args.tenant)
                    if args.command == "create"
                    else {"boundary_key": boundary_key(coordinator.seal(engine, args.tenant))}
                )
            finally:
                engine.close_all()
        elif args.command == "list":
            result["candidates"] = [asdict(m) for m in coordinator.list(args.tenant)]
        elif args.command == "validate":
            if not args.manifest_key:
                parser.error("validate requires --manifest-key")
            manifest = decode(get_bytes(store, args.manifest_key), CheckpointManifest)
            if manifest.tenant_id != args.tenant or manifest.namespace != args.namespace:
                raise ValueError("manifest namespace/tenant mismatch")
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "snapshot.db"
                store.get_file(manifest.snapshot_key, path)
                result["validation"] = validate_snapshot(path, manifest)
        else:
            if args.destination is None or not args.boundary_key:
                parser.error("restore requires --destination and --boundary-key")
            result["recovery"] = restore(
                store,
                wal,
                namespace=args.namespace,
                tenant=args.tenant,
                boundary_object_key=args.boundary_key,
                destination=args.destination,
                old_writers_stopped=args.old_writers_stopped,
            )
        result["status"] = "passed"
        result["request_accounting"] = store.accounting()
        return 0
    except Exception as exc:
        result.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        print(result["error"])
        return 1
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
