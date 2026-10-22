"""Atomic snapshots and immutable per-run directories; no database credentials in artifacts."""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

Json = dict[str, Any]


def now() -> str:
    return datetime.now(UTC).isoformat()


def save(path: Path, value: Json) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


def read(path: Path) -> Json:
    value: Json = json.loads(path.read_text(encoding="utf-8"))
    return value


class RunStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()

    def directory(self, run_id: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{32}", run_id):
            raise ValueError("invalid run identifier")
        return self.root / run_id

    def get(self, run_id: str) -> Json:
        return read(self.directory(run_id) / "run.json")

    def list(self) -> list[Json]:
        values = []
        for path in self.root.glob("*/run.json"):
            try:
                values.append(read(path))
            except (OSError, ValueError):
                continue
        return sorted(values, key=lambda run: run["created_at"], reverse=True)

    def outcomes(self, run_id: str, offset: int, limit: int) -> Json:
        path = self.directory(run_id) / "outcomes.jsonl"
        rows = []
        total = 0
        if path.exists():
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue  # Reader can overlap the final unflushed line.
                    if offset <= total < offset + limit:
                        rows.append(row)
                    total += 1
        return {"rows": rows, "total": total, "offset": offset}
