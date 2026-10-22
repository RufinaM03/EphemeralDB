"""Small local CLI for inspecting, validating, and replaying explicit WAL."""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict
from pathlib import Path

from ephemeraldb.wal.reader import iter_records
from ephemeraldb.wal.replay import replay_wal
from ephemeraldb.wal.validation import validate_chain, wal_summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect local EphemeralDB logical WAL files.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument("--wal-root", required=True, type=Path)
    inspect_parser.add_argument("--tenant", required=True)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--wal-root", required=True, type=Path)
    validate_parser.add_argument("--tenant", required=True)

    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("--wal-root", required=True, type=Path)
    replay_parser.add_argument("--tenant", required=True)
    replay_parser.add_argument("--database", required=True, type=Path)

    args = parser.parse_args(argv)
    if args.command == "inspect":
        summary = wal_summary(args.wal_root, args.tenant)
        print(json.dumps(asdict(summary), ensure_ascii=True, indent=2, sort_keys=True))
        return 0
    if args.command == "validate":
        records = list(iter_records(args.wal_root, args.tenant))
        validate_chain(records, expected_tenant_id=args.tenant)
        print(
            json.dumps(
                {
                    "tenant_id": args.tenant,
                    "record_count": len(records),
                    "checksum_chain_valid": True,
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    connection = sqlite3.connect(args.database, isolation_level=None)
    try:
        replay_summary = replay_wal(
            connection,
            iter_records(args.wal_root, args.tenant),
            tenant_id=args.tenant,
        )
    finally:
        connection.close()
    print(json.dumps(asdict(replay_summary), ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
