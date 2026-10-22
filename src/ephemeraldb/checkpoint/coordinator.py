"""Consistent capture, immutable publication, and explicit sealed boundaries."""

from __future__ import annotations

import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from ephemeraldb.checkpoint.models import (
    CheckpointError,
    CheckpointManifest,
    RecoveryBoundary,
    boundary_key,
    checkpoint_prefix,
    decode,
    encode,
    signed,
)
from ephemeraldb.checkpoint.snapshot import backup, inspect_snapshot, validate_snapshot
from ephemeraldb.checkpoint.store import ObjectStore, file_hash, get_bytes, put_bytes
from ephemeraldb.core.models import TenantId
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.ownership.coordinator import FencedCommitter
from ephemeraldb.wal.writer import safe_tenant_dir_name


class CheckpointCoordinator:
    def __init__(self, store: ObjectStore, namespace: str) -> None:
        self.store = store
        self.namespace = safe_tenant_dir_name(namespace)

    def create(self, engine: TenantDatabaseEngine, tenant: str) -> dict[str, object]:
        started = time.perf_counter_ns()
        guard = engine.executor.guard
        if guard is None:
            raise CheckpointError("checkpoint requires explicit WAL admission")
        authority_anchor: tuple[str | None, str | None] | None = None
        with tempfile.TemporaryDirectory(prefix="ephemeraldb-snapshot-") as directory:
            path = Path(directory) / "snapshot.db"
            with engine.executor._lock_for(tenant):
                with engine.connection_manager.connection_for(TenantId(tenant)) as connection:
                    guard.require_ready(tenant, connection)
                    backup(connection, path)
                    if isinstance(guard, FencedCommitter):
                        authority, _ = guard.coordinator.status(tenant)
                        authority_anchor = (
                            authority.descriptor_head_key,
                            authority.descriptor_head_checksum,
                        )
            captured = time.perf_counter_ns()
            facts = inspect_snapshot(path, tenant)
            checkpoint_id = uuid.uuid4().hex
            sequence = cast(int, facts["sequence"])
            key = (
                f"{self.namespace}/checkpoints/{safe_tenant_dir_name(tenant)}/"
                f"{sequence:020d}/{checkpoint_id}/snapshot.db"
            )
            manifest = signed(
                CheckpointManifest(
                    checkpoint_id=checkpoint_id,
                    namespace=self.namespace,
                    tenant_id=tenant,
                    sequence=sequence,
                    head=cast(str | None, facts["head"]),
                    snapshot_key=key,
                    size_bytes=path.stat().st_size,
                    snapshot_sha256=file_hash(path),
                    logical_checksum=cast(str, facts["logical_checksum"]),
                    internal_checksum=cast(str, facts["internal_checksum"]),
                    wal_namespace=guard.namespace,
                    created_at=datetime.now(UTC).isoformat(),
                )
            )
            validated = time.perf_counter_ns()
            result = self.publish(
                path,
                manifest,
                {
                    "lock_and_capture_ns": captured - started,
                    "validation_hash_ns": validated - captured,
                },
            )
            if isinstance(guard, FencedCommitter):
                assert authority_anchor is not None
                registered = guard.coordinator.register_checkpoint(
                    guard.token_for(tenant),
                    checkpoint_key=manifest.snapshot_key,
                    checkpoint_checksum=manifest.snapshot_sha256,
                    checkpoint_sequence=manifest.sequence,
                    descriptor_key=authority_anchor[0],
                    descriptor_checksum=authority_anchor[1],
                )
                result["authority_revision"] = registered.revision
                result["authority_checkpoint_sequence"] = registered.checkpoint_sequence
            return result

    def publish(
        self, path: Path, manifest: CheckpointManifest, timings: dict[str, int] | None = None
    ) -> dict[str, object]:
        if manifest.namespace != self.namespace:
            raise CheckpointError("checkpoint namespace mismatch")
        encode(manifest)
        validate_snapshot(path, manifest)
        started = time.perf_counter_ns()
        self.store.stage = "snapshot_upload"
        self.store.put_file(manifest.snapshot_key, path)
        # Verify bytes even with an injected adapter; ETag is never a content hash.
        with tempfile.TemporaryDirectory() as directory:
            downloaded = Path(directory) / "verify.db"
            self.store.get_file(manifest.snapshot_key, downloaded)
            if file_hash(downloaded) != manifest.snapshot_sha256 or (
                downloaded.stat().st_size != manifest.size_bytes
            ):
                raise CheckpointError("snapshot upload verification failed")
        uploaded = time.perf_counter_ns()
        self.store.stage = "manifest_publication"
        key = checkpoint_prefix(manifest) + "manifest.json"
        put_bytes(self.store, key, encode(manifest))
        if get_bytes(self.store, key) != encode(manifest):
            raise CheckpointError("manifest read-back mismatch")
        return {
            "manifest_key": key,
            "sequence": manifest.sequence,
            "snapshot_bytes": manifest.size_bytes,
            "timings_ns": {
                **(timings or {}),
                "upload_ns": uploaded - started,
                "publication_ns": time.perf_counter_ns() - uploaded,
            },
            "request_accounting": self.store.accounting(),
        }

    def list(self, tenant: str) -> tuple[CheckpointManifest, ...]:
        prefix = f"{self.namespace}/checkpoints/{safe_tenant_dir_name(tenant)}/"
        self.store.stage = "checkpoint_discovery"
        candidates = []
        total = 0
        for key in self.store.list_keys(prefix):
            if not key.endswith("/manifest.json"):
                continue
            body = get_bytes(self.store, key)
            total += len(body)
            if total > 4 * 1024 * 1024:
                raise CheckpointError("manifest discovery byte budget exceeded")
            manifest = decode(body, CheckpointManifest)
            if (
                manifest.namespace != self.namespace
                or manifest.tenant_id != tenant
                or (key != checkpoint_prefix(manifest) + "manifest.json")
            ):
                raise CheckpointError("manifest location/identity mismatch")
            candidates.append(manifest)
        return tuple(sorted(candidates, key=lambda m: (-m.sequence, m.checkpoint_id)))

    def seal(self, engine: TenantDatabaseEngine, tenant: str) -> RecoveryBoundary:
        """Permanently stop this source tenant before publishing its bounded frontier."""
        guard = engine.executor.guard
        if guard is None:
            raise CheckpointError("seal requires WAL admission")
        with engine.executor._lock_for(tenant):
            with engine.connection_manager.connection_for(TenantId(tenant)) as connection:
                guard.require_ready(tenant, connection)
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "boundary.db"
                    backup(connection, path)
                    facts = inspect_snapshot(path, tenant)
                boundary = signed(
                    RecoveryBoundary(
                        boundary_id=uuid.uuid4().hex,
                        namespace=self.namespace,
                        tenant_id=tenant,
                        sequence=cast(int, facts["sequence"]),
                        head=cast(str | None, facts["head"]),
                        logical_checksum=cast(str, facts["logical_checksum"]),
                        internal_checksum=cast(str, facts["internal_checksum"]),
                        wal_namespace=guard.namespace,
                        created_at=datetime.now(UTC).isoformat(),
                    )
                )
                guard.seal(tenant)
                self.store.stage = "boundary_publication"
                put_bytes(self.store, boundary_key(boundary), encode(boundary))
                return boundary
