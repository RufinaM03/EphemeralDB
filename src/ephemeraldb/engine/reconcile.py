"""Explicit, read/validate/replay reconciliation of a retained local attempt."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path

from ephemeraldb.config.settings import load_settings
from ephemeraldb.engine.engine import TenantDatabaseEngine


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", required=True, type=Path)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--wal-store", choices=("local", "s3-express"), required=True)
    parser.add_argument("--wal-persistence-mode", choices=("single", "batch"), default="single")
    parser.add_argument("--wal-root", type=Path)
    args = parser.parse_args(argv)
    if args.wal_store == "local" and args.wal_root is None:
        parser.error("local reconciliation requires --wal-root")
    settings = replace(
        load_settings(), wal_store=args.wal_store, wal_persistence_mode=args.wal_persistence_mode
    )
    engine = TenantDatabaseEngine.from_settings(args.dataset_dir, settings, wal_root=args.wal_root)
    try:
        engine.reconcile_tenant(args.tenant)
        print(json.dumps({"tenant": args.tenant, "state": "READY", "new_wal_puts": 0}))
    finally:
        engine.close_all()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
