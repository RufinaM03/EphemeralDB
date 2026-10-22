"""Versioned immutable checkpoint and sealed recovery-boundary manifests."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from dataclasses import fields as dataclass_fields
from datetime import datetime

from ephemeraldb.wal.codec import canonical_json_bytes
from ephemeraldb.wal.writer import safe_tenant_dir_name


class CheckpointError(ValueError):
    """Invalid, conflicting, or incomplete checkpoint/recovery evidence."""


@dataclass(frozen=True)
class CheckpointManifest:
    checkpoint_id: str
    namespace: str
    tenant_id: str
    sequence: int
    head: str | None
    snapshot_key: str
    size_bytes: int
    snapshot_sha256: str
    logical_checksum: str
    internal_checksum: str
    wal_namespace: str
    created_at: str
    format_version: int = 1
    schema_version: int = 1
    internal_version: int = 1
    logical_checksum_version: int = 1
    wal_versions: tuple[int, ...] = (1, 2)
    checksum: str = ""


@dataclass(frozen=True)
class RecoveryBoundary:
    boundary_id: str
    namespace: str
    tenant_id: str
    sequence: int
    head: str | None
    logical_checksum: str
    internal_checksum: str
    wal_namespace: str
    created_at: str
    format_version: int = 1
    quiescent: bool = True
    checksum: str = ""


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def signed[Manifest: CheckpointManifest | RecoveryBoundary](value: Manifest) -> Manifest:
    fields = asdict(value)
    fields.pop("checksum")
    return replace(value, checksum=digest(fields))


def encode(value: CheckpointManifest | RecoveryBoundary) -> bytes:
    validate(value)
    return canonical_json_bytes(asdict(value))


def decode[Manifest: CheckpointManifest | RecoveryBoundary](
    body: bytes, kind: type[Manifest]
) -> Manifest:
    try:
        fields = json.loads(body)
        if not isinstance(fields, dict) or set(fields) != {f.name for f in dataclass_fields(kind)}:
            raise CheckpointError("missing or unknown manifest fields")
        if kind is CheckpointManifest:
            fields["wal_versions"] = tuple(fields["wal_versions"])
        result = kind(**fields)
        validate(result)
        return result
    except (TypeError, KeyError, json.JSONDecodeError) as exc:
        raise CheckpointError("invalid manifest structure") from exc


def validate(value: CheckpointManifest | RecoveryBoundary) -> None:
    for name in ("namespace", "tenant_id"):
        safe_tenant_dir_name(getattr(value, name))
    if type(value.sequence) is not int or value.sequence < 0:
        raise CheckpointError("invalid checkpoint sequence")
    if type(value.format_version) is not int or value.format_version != 1:
        raise CheckpointError("unsupported manifest format")
    if not isinstance(value.wal_namespace, str) or not value.wal_namespace:
        raise CheckpointError("missing WAL namespace")
    timestamp = datetime.fromisoformat(value.created_at)
    if timestamp.utcoffset() is None:
        raise CheckpointError("timestamp must be timezone-aware")
    for name in ("logical_checksum", "internal_checksum", "checksum"):
        if not isinstance(getattr(value, name), str) or not re.fullmatch(
            r"[0-9a-f]{64}", getattr(value, name)
        ):
            raise CheckpointError(f"invalid {name}")
    if (value.sequence == 0 and value.head is not None) or (
        value.sequence > 0
        and (not isinstance(value.head, str) or not re.fullmatch(r"[0-9a-f]{64}", value.head))
    ):
        raise CheckpointError("invalid sequence/head binding")
    if isinstance(value, CheckpointManifest):
        safe_tenant_dir_name(value.checkpoint_id)
        if any(
            type(v) is not int or v != 1
            for v in (value.schema_version, value.internal_version, value.logical_checksum_version)
        ) or value.wal_versions != (1, 2):
            raise CheckpointError("unsupported schema or checksum version")
        if type(value.size_bytes) is not int or value.size_bytes <= 0:
            raise CheckpointError("invalid snapshot size")
        if not re.fullmatch(r"[0-9a-f]{64}", value.snapshot_sha256):
            raise CheckpointError("invalid snapshot hash")
        if value.snapshot_key != checkpoint_prefix(value) + "snapshot.db":
            raise CheckpointError("snapshot key escapes expected immutable layout")
    else:
        safe_tenant_dir_name(value.boundary_id)
        if value.quiescent is not True:
            raise CheckpointError("unresolved recovery boundary")
    if signed(value).checksum != value.checksum:
        raise CheckpointError("manifest checksum mismatch")


def checkpoint_prefix(value: CheckpointManifest) -> str:
    return (
        f"{value.namespace}/checkpoints/{value.tenant_id}/"
        f"{value.sequence:020d}/{value.checkpoint_id}/"
    )


def boundary_key(value: RecoveryBoundary) -> str:
    return f"{value.namespace}/boundaries/{value.tenant_id}/{value.boundary_id}.json"
