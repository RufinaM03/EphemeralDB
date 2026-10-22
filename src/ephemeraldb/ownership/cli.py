"""Offline Phase 8 authority administration; cloud use is intentionally explicit."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from .coordinator import OwnershipCoordinator, new_session_id
from .sqlite_store import SQLiteAuthorityStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authority-db", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bootstrap = subparsers.add_parser("bootstrap")
    bootstrap.add_argument("--tenant", required=True)
    bootstrap.add_argument("--runtime", required=True)
    bootstrap.add_argument("--bootstrap-key", required=True)
    bootstrap.add_argument("--bootstrap-checksum", required=True)
    status = subparsers.add_parser("status")
    status.add_argument("--tenant", required=True)
    takeover = subparsers.add_parser("takeover")
    takeover.add_argument("--tenant", required=True)
    takeover.add_argument("--runtime", required=True)
    history = subparsers.add_parser("history")
    history.add_argument("--tenant", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    coordinator = OwnershipCoordinator(SQLiteAuthorityStore(args.authority_db), args.namespace)
    if args.command == "bootstrap":
        state = coordinator.bootstrap(
            args.tenant,
            session_id=new_session_id(args.runtime),
            bootstrap_key=args.bootstrap_key,
            bootstrap_checksum=args.bootstrap_checksum,
        )
        output: object = asdict(state)
    elif args.command == "takeover":
        state = coordinator.takeover(
            args.tenant,
            session_id=new_session_id(args.runtime),
        )
        output = asdict(state)
    elif args.command == "history":
        state, _ = coordinator.status(args.tenant)
        output = [asdict(item) for item in coordinator.accepted_history(state)]
    else:
        state, snapshot = coordinator.status(args.tenant)
        output = asdict(state) | {"validator": snapshot.validator}
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
