"""Typed records for the neutral synthetic schema."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

SCHEMA_VERSION = 1
GENERATION_TIMESTAMP_BASIS = "2026-01-01T00:00:00Z"

ENTITY_STATES = ("ACTIVE", "INACTIVE", "PENDING", "ARCHIVED")
RELATIONSHIP_TYPES = ("RELATED", "DEPENDS_ON", "LINKED", "REFERENCES")
TRANSACTION_STATUSES = ("PENDING", "COMMITTED", "ABORTED")
AUDIT_OPERATIONS = (
    "ENTITY_CREATED",
    "ENTITY_UPDATED",
    "RELATIONSHIP_CREATED",
    "RELATIONSHIP_REMOVED",
    "TRANSACTION_COMMITTED",
)


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    """Configuration for deterministic dataset generation."""

    tenant_count: int = 3
    entities_per_tenant: int = 100
    relationships_per_entity: int = 2
    metadata_entries_per_entity: int = 3
    initial_versions_per_entity: int = 1
    seed: int = 42

    def __post_init__(self) -> None:
        _require_at_least("tenant_count", self.tenant_count, 1)
        _require_at_least("entities_per_tenant", self.entities_per_tenant, 1)
        _require_at_least("relationships_per_entity", self.relationships_per_entity, 0)
        _require_at_least("metadata_entries_per_entity", self.metadata_entries_per_entity, 0)
        _require_at_least("initial_versions_per_entity", self.initial_versions_per_entity, 1)
        if (
            self.relationships_per_entity > 0
            and self.entities_per_tenant <= self.relationships_per_entity
        ):
            msg = "relationships_per_entity must be less than entities_per_tenant"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class TenantRecord:
    tenant_id: str
    created_at: str
    schema_version: int


@dataclass(frozen=True, slots=True)
class EntityRecord:
    entity_id: str
    tenant_id: str
    state: str
    numeric_value: int
    version: int
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class EntityVersionRecord:
    tenant_id: str
    entity_id: str
    version: int
    state: str
    numeric_value: int
    created_at: str


@dataclass(frozen=True, slots=True)
class RelationshipRecord:
    tenant_id: str
    source_entity_id: str
    target_entity_id: str
    relationship_type: str
    weight: float
    created_at: str


@dataclass(frozen=True, slots=True)
class MetadataRecord:
    tenant_id: str
    entity_id: str
    metadata_key: str
    metadata_value: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class TenantManifest:
    tenant_id: str
    filename: str
    entity_count: int
    relationship_count: int
    metadata_count: int
    state_checksum: str


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    schema_version: int
    seed: int
    tenant_count: int
    entities_per_tenant: int
    relationships_per_entity: int
    metadata_entries_per_entity: int
    initial_versions_per_entity: int
    generation_timestamp_basis: str
    tenant_ids: tuple[str, ...]
    total_entity_count: int
    total_relationship_count: int
    total_metadata_count: int
    state_checksum: str
    tenant_databases: tuple[TenantManifest, ...]


@dataclass(frozen=True, slots=True)
class GeneratedDataset:
    output_dir: Path
    manifest_path: Path
    manifest: DatasetManifest


@dataclass(frozen=True, slots=True)
class ValidationResult:
    tenant_id: str | None
    entity_count: int
    relationship_count: int
    metadata_count: int
    state_checksum: str | None
    errors: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


def _require_at_least(name: str, value: int, minimum: int) -> None:
    if type(value) is not int:
        msg = f"{name} must be an integer"
        raise TypeError(msg)
    if value < minimum:
        msg = f"{name} must be >= {minimum}"
        raise ValueError(msg)
