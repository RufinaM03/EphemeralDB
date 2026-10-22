"""Dataset identity catalog derived from Phase 1 manifests."""

from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path
from typing import Any

from ephemeraldb.schema.generator import deterministic_uuid, load_manifest


class DatasetIdentityCatalog:
    """Lightweight deterministic identity catalog for generated datasets."""

    def __init__(self, manifest: dict[str, Any]) -> None:
        self.manifest = manifest
        self.dataset_seed = _as_int(manifest["seed"])
        self.tenant_ids = tuple(_as_str(value) for value in manifest["tenant_ids"])
        self.tenant_count = _as_int(manifest["tenant_count"])
        self.entities_per_tenant = _as_int(manifest["entities_per_tenant"])
        self.metadata_entries_per_entity = _as_int(manifest["metadata_entries_per_entity"])
        self.initial_versions_per_entity = _as_int(manifest["initial_versions_per_entity"])
        self.dataset_manifest_checksum = dataset_manifest_checksum(manifest)
        self._tenant_index_by_id = {
            tenant_id: tenant_index for tenant_index, tenant_id in enumerate(self.tenant_ids)
        }
        self._entity_cache: dict[str, frozenset[str]] = {}
        if self.tenant_count != len(self.tenant_ids):
            msg = "tenant_count does not match tenant_ids length"
            raise ValueError(msg)

    @classmethod
    def from_path(cls, path: Path) -> DatasetIdentityCatalog:
        return cls(load_manifest(path))

    def tenant_index(self, tenant_id: str) -> int:
        try:
            return self._tenant_index_by_id[tenant_id]
        except KeyError as exc:
            msg = f"unknown tenant_id: {tenant_id}"
            raise ValueError(msg) from exc

    def entity_id(self, tenant_index: int, entity_index: int) -> str:
        _validate_index("tenant_index", tenant_index, self.tenant_count)
        _validate_index("entity_index", entity_index, self.entities_per_tenant)
        return deterministic_uuid(
            self.dataset_seed,
            "tenant",
            tenant_index,
            "entity",
            entity_index,
        )

    def entity_id_for_tenant(self, tenant_id: str, entity_index: int) -> str:
        return self.entity_id(self.tenant_index(tenant_id), entity_index)

    def metadata_key(self, metadata_index: int) -> str:
        _validate_index("metadata_index", metadata_index, self.metadata_entries_per_entity)
        return f"attribute_{metadata_index:02d}"

    def tenant_entity_ids(self, tenant_id: str) -> frozenset[str]:
        if tenant_id not in self._entity_cache:
            tenant_index = self.tenant_index(tenant_id)
            self._entity_cache[tenant_id] = frozenset(
                self.entity_id(tenant_index, entity_index)
                for entity_index in range(self.entities_per_tenant)
            )
        return self._entity_cache[tenant_id]

    def is_valid_entity_id(self, tenant_id: str, entity_id: str) -> bool:
        return entity_id in self.tenant_entity_ids(tenant_id)

    def identity_summary(self) -> dict[str, object]:
        return {
            "seed": self.dataset_seed,
            "dataset_seed": self.dataset_seed,
            "tenant_count": self.tenant_count,
            "entities_per_tenant": self.entities_per_tenant,
            "metadata_entries_per_entity": self.metadata_entries_per_entity,
            "initial_versions_per_entity": self.initial_versions_per_entity,
            "tenant_ids": list(self.tenant_ids),
        }


class HotSetCatalog:
    """Deterministic hot tenant/entity selections for contention profiles."""

    def __init__(
        self,
        catalog: DatasetIdentityCatalog,
        *,
        seed: int,
        hot_tenant_fraction: float,
        hot_entity_fraction: float,
    ) -> None:
        hot_tenant_count = _fractional_count(catalog.tenant_count, hot_tenant_fraction)
        hot_entity_count = _fractional_count(catalog.entities_per_tenant, hot_entity_fraction)
        tenant_rng = random.Random(f"{seed}:hot-tenants")
        tenant_indexes = sorted(tenant_rng.sample(range(catalog.tenant_count), hot_tenant_count))
        self.hot_tenant_ids = tuple(catalog.tenant_ids[index] for index in tenant_indexes)
        hot_entities: dict[str, tuple[str, ...]] = {}
        for tenant_index in tenant_indexes:
            tenant_id = catalog.tenant_ids[tenant_index]
            entity_rng = random.Random(f"{seed}:hot-entities:{tenant_index}")
            entity_indexes = sorted(
                entity_rng.sample(range(catalog.entities_per_tenant), hot_entity_count)
            )
            hot_entities[tenant_id] = tuple(
                catalog.entity_id(tenant_index, entity_index) for entity_index in entity_indexes
            )
        self.hot_entity_ids_by_tenant = hot_entities

    def hot_entities_for_tenant(self, tenant_id: str) -> tuple[str, ...]:
        return self.hot_entity_ids_by_tenant.get(tenant_id, ())

    def is_hot_tenant(self, tenant_id: str) -> bool:
        return tenant_id in set(self.hot_tenant_ids)

    def is_hot_entity(self, tenant_id: str, entity_id: str) -> bool:
        return entity_id in set(self.hot_entities_for_tenant(tenant_id))

    def to_dict(self) -> dict[str, object]:
        return {
            "hot_tenant_ids": list(self.hot_tenant_ids),
            "hot_entity_ids_by_tenant": {
                tenant_id: list(entity_ids)
                for tenant_id, entity_ids in sorted(self.hot_entity_ids_by_tenant.items())
            },
        }


def dataset_manifest_checksum(manifest: dict[str, Any]) -> str:
    canonical = json.dumps(manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def load_manifest_dict(path: Path) -> dict[str, Any]:
    return load_manifest(path)


def _fractional_count(total: int, fraction: float) -> int:
    if fraction <= 0.0:
        return 0
    return max(1, min(total, round(total * fraction)))


def _validate_index(name: str, value: int, upper_bound: int) -> None:
    if value < 0 or value >= upper_bound:
        msg = f"{name} must be in [0, {upper_bound})"
        raise ValueError(msg)


def _as_int(value: object) -> int:
    if type(value) is not int:
        msg = f"expected integer value, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        msg = f"expected string value, got {type(value).__name__}"
        raise TypeError(msg)
    return value
