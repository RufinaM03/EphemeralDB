"""Bounded sealed-run recovery; no ownership transfer or distributed fencing."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path
from typing import Protocol

from ephemeraldb.checkpoint.coordinator import CheckpointCoordinator
from ephemeraldb.checkpoint.models import (
    CheckpointError,
    RecoveryBoundary,
    boundary_key,
    decode,
)
from ephemeraldb.checkpoint.snapshot import inspect_snapshot, validate_snapshot
from ephemeraldb.checkpoint.store import MAX_RECORDS, MAX_SCAN_BYTES, ObjectStore, get_bytes
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.wal.codec import encode_record
from ephemeraldb.wal.durability import StoredWalRecord
from ephemeraldb.wal.replay import replay_wal
from ephemeraldb.wal.validation import validate_record
from ephemeraldb.wal.writer import _fsync_directory


class RecoveryWalStore(Protocol):
    @property
    def namespace_id(self) -> str: ...
    def inspect_history(self, tenant_id: str) -> tuple[StoredWalRecord, ...]: ...


def _wal_accounting(wal: RecoveryWalStore) -> dict[str, object]:
    if hasattr(wal, "request_accounting"):
        return dict(wal.request_accounting())
    if hasattr(wal, "objects"):
        return dict(wal.objects.accounting())
    return {}


def _accounting_delta(before: dict[str, object], after: dict[str, object]) -> dict[str, object]:
    result = dict(after)
    for field in ("calls", "sdk_calls"):
        current, previous = after.get(field, {}), before.get(field, {})
        if isinstance(current, dict) and isinstance(previous, dict) and field in after:
            result[field] = {
                key: value - previous.get(key, 0)
                for key, value in current.items()
                if value != previous.get(key, 0)
            }
    current_bytes, previous_bytes = after.get("bytes_read"), before.get("bytes_read")
    if isinstance(current_bytes, int) and isinstance(previous_bytes, int):
        result["bytes_read"] = current_bytes - previous_bytes
    result["scope"] = "this restore interval; not wire attempts or billable usage"
    return result


def validate_history(
    history: tuple[StoredWalRecord, ...], boundary: RecoveryBoundary
) -> tuple[StoredWalRecord, ...]:
    unique: dict[int, StoredWalRecord] = {}
    size = 0
    if len(history) > MAX_RECORDS:
        raise CheckpointError("recovery record budget exceeded")
    for item in history:
        validate_record(item.record, expected_tenant_id=boundary.tenant_id)
        body = encode_record(item.record)
        size += len(body)
        if size > MAX_SCAN_BYTES:
            raise CheckpointError("recovery byte budget exceeded")
        prior = unique.get(item.record.sequence_no)
        if prior is not None and encode_record(prior.record) != body:
            raise CheckpointError("conflicting WAL sequence")
        unique.setdefault(item.record.sequence_no, item)
    ordered = tuple(unique[n] for n in sorted(unique))
    head = None
    transactions: set[str] = set()
    for n, item in enumerate(ordered, 1):
        record = item.record
        if record.sequence_no != n or record.previous_record_checksum != head:
            raise CheckpointError("missing WAL or invalid chain")
        if record.transaction_id in transactions:
            raise CheckpointError("transaction identity spans multiple sequences")
        transactions.add(record.transaction_id)
        head = record.record_checksum
    if len(ordered) != boundary.sequence or head != boundary.head:
        raise CheckpointError("required trailing WAL missing or boundary has unexpected suffix")
    return ordered


def restore(
    store: ObjectStore,
    wal: RecoveryWalStore,
    *,
    namespace: str,
    tenant: str,
    boundary_object_key: str,
    destination: Path,
    old_writers_stopped: bool,
) -> dict[str, object]:
    if not old_writers_stopped:
        raise CheckpointError("explicit stopped-writer assertion required; no fencing exists")
    destination = destination.resolve()
    if destination.exists():
        raise CheckpointError("restore destination must not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter_ns()
    checkpoint_counts_before = store.accounting()
    wal_counts_before = _wal_accounting(wal)
    phases: dict[str, int] = {}
    store.stage = "boundary_discovery"
    boundary = decode(get_bytes(store, boundary_object_key), RecoveryBoundary)
    if (boundary.namespace, boundary.tenant_id, boundary.wal_namespace) != (
        namespace,
        tenant,
        wal.namespace_id,
    ) or boundary_key(boundary) != boundary_object_key:
        raise CheckpointError("boundary identity/namespace mismatch")
    candidates = CheckpointCoordinator(store, namespace).list(tenant)
    if not candidates:
        raise CheckpointError("no published checkpoint")
    selected = candidates[0]
    if selected.sequence > boundary.sequence or selected.wal_namespace != wal.namespace_id:
        raise CheckpointError("checkpoint does not belong to selected sealed history")
    peers = [m for m in candidates if m.sequence == selected.sequence]
    for peer in peers:
        if (peer.head, peer.logical_checksum, peer.internal_checksum, peer.wal_namespace) != (
            selected.head,
            selected.logical_checksum,
            selected.internal_checksum,
            selected.wal_namespace,
        ):
            raise CheckpointError("conflicting checkpoint candidates at one sequence")
    discovery_done = time.perf_counter_ns()
    phases["discovery"] = discovery_done - started
    with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".restore-") as temporary:
        staging = Path(temporary) / "dataset"
        staging.mkdir()
        database = staging / "tenant-000001.db"
        store.stage = "snapshot_download"
        store.get_file(selected.snapshot_key, database)
        downloaded = time.perf_counter_ns()
        phases["download"] = downloaded - discovery_done
        validate_snapshot(database, selected)
        for peer in peers[1:]:
            other = Path(temporary) / "peer.db"
            store.get_file(peer.snapshot_key, other)
            validate_snapshot(other, peer)
            other.unlink()
        validated = time.perf_counter_ns()
        phases["snapshot_validation"] = validated - downloaded
        if hasattr(wal, "request_phase"):
            wal.request_phase = "wal_recovery"
        history = wal.inspect_history(tenant)
        ordered = validate_history(history, boundary)
        if (
            selected.sequence
            and ordered[selected.sequence - 1].record.record_checksum != selected.head
        ):
            raise CheckpointError("checkpoint not anchored in WAL history")
        discovered = time.perf_counter_ns()
        phases["wal_fetch_decode_validate"] = discovered - validated
        with closing(sqlite3.connect(database, isolation_level=None)) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            # Cross-check checkpoint retry mappings against authoritative WAL, not just its digest.
            for item in ordered[: selected.sequence]:
                row = connection.execute(
                    "SELECT w.record_checksum,i.fingerprint,w.wal_object_key,w.remote_durable "
                    "FROM _ephemeraldb_transaction_wal w "
                    "JOIN _ephemeraldb_request_identity i USING (tenant_id,transaction_id) "
                    "WHERE w.transaction_id=? AND w.wal_sequence=?",
                    (item.record.transaction_id, item.record.sequence_no),
                ).fetchone()
                matching = [
                    copy for copy in history if copy.record.sequence_no == item.record.sequence_no
                ]
                if (
                    row is None
                    or row[:2] != (item.record.record_checksum, item.record.request_fingerprint)
                    or not any(row[2:] == (copy.key, int(copy.remote_durable)) for copy in matching)
                ):
                    raise CheckpointError("checkpoint retry identity differs from remote WAL")
            for item in ordered[selected.sequence :]:
                replay_wal(
                    connection,
                    (item.record,),
                    tenant_id=tenant,
                    remote_durable=item.remote_durable,
                    object_keys={item.record.sequence_no: item.key},
                )
        replayed = time.perf_counter_ns()
        phases["replay"] = replayed - discovered
        facts = inspect_snapshot(database, tenant)
        for field, actual in facts.items():
            if actual != getattr(boundary, field):
                raise CheckpointError(f"recovered {field} does not match sealed boundary")
        with closing(sqlite3.connect(database)) as connection:
            counts = {
                table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("entities", "relationships", "metadata")
            }
        manifest = {
            "tenant_databases": [
                {
                    "tenant_id": tenant,
                    "filename": database.name,
                    "entity_count": counts["entities"],
                    "relationship_count": counts["relationships"],
                    "metadata_count": counts["metadata"],
                    "state_checksum": facts["logical_checksum"],
                }
            ],
            "recovery_boundary": boundary_object_key,
        }
        (staging / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        # Build admission state only after remote evidence and restored state agree.
        engine = TenantDatabaseEngine.from_dataset_dir(staging)
        engine.close_all()
        with closing(sqlite3.connect(staging / ".ephemeraldb-attempts.sqlite")) as journal:
            journal.execute("PRAGMA synchronous=FULL")
            journal.execute("PRAGMA user_version=1")
            journal.execute(
                "CREATE TABLE attempts (tenant TEXT PRIMARY KEY, namespace TEXT NOT NULL, "
                "state TEXT NOT NULL, record BLOB, object_key TEXT, "
                "is_batch INTEGER, remote INTEGER)"
            )
            last = ordered[-1] if ordered else None
            journal.execute(
                "INSERT INTO attempts VALUES (?,?,'READY',?,?,?,?)",
                (
                    tenant,
                    wal.namespace_id,
                    None if last is None else encode_record(last.record),
                    None if last is None else last.key,
                    int(last is not None and "wal-batches/" in last.key),
                    int(last is not None and last.remote_durable),
                ),
            )
            journal.commit()
        verified = time.perf_counter_ns()
        phases["final_verification"] = verified - replayed
        if destination.exists():
            raise CheckpointError("destination appeared during restore")
        staging.rename(destination)
        _fsync_directory(destination.parent)
        phases["installation"] = time.perf_counter_ns() - verified
    return {
        "artifact_schema_version": 1,
        "state": "READY",
        "namespace": namespace,
        "tenant": tenant,
        "checkpoint_sequence": selected.sequence,
        "final_sequence": boundary.sequence,
        "head": boundary.head,
        "logical_checksum": boundary.logical_checksum,
        "internal_checksum": boundary.internal_checksum,
        "records_applied": boundary.sequence - selected.sequence,
        "records_deduplicated": len(history) - len(ordered),
        "stage_durations_ns": phases,
        "observed_recovery_ns": time.perf_counter_ns() - started,
        "request_accounting": _accounting_delta(checkpoint_counts_before, store.accounting()),
        "wal_scan": getattr(wal, "scan_metrics", {}),
        "wal_request_accounting": _accounting_delta(wal_counts_before, _wal_accounting(wal)),
        "cleanup_status": "staging_removed; authoritative objects retained",
    }
