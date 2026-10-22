"""Tenant database registry with manifest-driven path safety."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ephemeraldb.core.models import TenantId
from ephemeraldb.schema.generator import load_manifest


class TenantRegistryError(ValueError):
    """Raised when a tenant database cannot be safely resolved."""


@dataclass(frozen=True, slots=True)
class TenantDatabaseRecord:
    tenant_id: TenantId
    database_path: Path
    entity_count: int
    relationship_count: int
    metadata_count: int
    state_checksum: str


class TenantDatabaseRegistry:
    """Manifest-backed mapping from tenant IDs to local SQLite files."""

    def __init__(self, database_root: Path, records: dict[str, TenantDatabaseRecord]) -> None:
        self.database_root = Path(database_root).resolve()
        self._records = dict(records)
        for record in self._records.values():
            _ensure_inside_root(self.database_root, record.database_path)

    @classmethod
    def from_dataset_dir(cls, dataset_dir: Path) -> TenantDatabaseRegistry:
        root = Path(dataset_dir).resolve()
        manifest = load_manifest(root / "manifest.json")
        records: dict[str, TenantDatabaseRecord] = {}
        for tenant_database in manifest["tenant_databases"]:
            database_path = _safe_database_path(root, _as_str(tenant_database["filename"]))
            tenant_id = TenantId(_as_str(tenant_database["tenant_id"]))
            records[tenant_id.value] = TenantDatabaseRecord(
                tenant_id=tenant_id,
                database_path=database_path,
                entity_count=_as_int(tenant_database["entity_count"]),
                relationship_count=_as_int(tenant_database["relationship_count"]),
                metadata_count=_as_int(tenant_database["metadata_count"]),
                state_checksum=_as_str(tenant_database["state_checksum"]),
            )
        return cls(root, records)

    @property
    def tenant_ids(self) -> tuple[TenantId, ...]:
        return tuple(record.tenant_id for record in self._records.values())

    @property
    def tenant_count(self) -> int:
        return len(self._records)

    def record_for(self, tenant_id: TenantId) -> TenantDatabaseRecord:
        try:
            return self._records[tenant_id.value]
        except KeyError as exc:
            msg = f"unknown tenant id: {tenant_id.value}"
            raise TenantRegistryError(msg) from exc

    def path_for(self, tenant_id: TenantId) -> Path:
        return self.record_for(tenant_id).database_path


def _safe_database_path(root: Path, filename: str) -> Path:
    path = (root / filename).resolve()
    _ensure_inside_root(root, path)
    if path.suffix != ".db":
        msg = f"tenant database must be a .db file: {filename}"
        raise TenantRegistryError(msg)
    return path


def _ensure_inside_root(root: Path, path: Path) -> None:
    if not path.resolve().is_relative_to(root.resolve()):
        msg = f"tenant database path escapes database root: {path}"
        raise TenantRegistryError(msg)


def _as_str(value: Any) -> str:
    if not isinstance(value, str):
        msg = f"expected string, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _as_int(value: Any) -> int:
    if type(value) is not int:
        msg = f"expected integer, got {type(value).__name__}"
        raise TypeError(msg)
    return value
