import json
import os
from pathlib import Path

import pytest

from ephemeraldb.loadgen.catalog import DatasetIdentityCatalog
from ephemeraldb.loadgen.generator import generate_workload
from ephemeraldb.loadgen.models import (
    EntityDeltaPayload,
    MultiEntityTransactionPayload,
    OperationType,
)
from ephemeraldb.loadgen.profiles import get_builtin_profile
from ephemeraldb.loadgen.schedule import constant_schedule
from ephemeraldb.loadgen.serialization import (
    iter_workload,
    operation_from_dict,
    operation_to_dict,
)
from ephemeraldb.loadgen.stats import calculate_workload_file_stats
from ephemeraldb.loadgen.validation import validate_workload
from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig


def test_same_seed_generates_identical_workload_and_manifest(tmp_path: Path) -> None:
    dataset_manifest = _dataset_manifest(tmp_path)
    schedule = constant_schedule(name="determinism", duration_seconds=5, rate=20)
    profile = get_builtin_profile("balanced", seed=42).with_overrides(virtual_client_count=5)

    first = generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=profile,
        schedule=schedule,
        output_path=tmp_path / "first.jsonl",
    )
    second = generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=profile,
        schedule=schedule,
        output_path=tmp_path / "second.jsonl",
    )

    assert first.workload_path.read_text(encoding="utf-8") == second.workload_path.read_text(
        encoding="utf-8"
    )
    assert first.manifest_path.read_text(encoding="utf-8") == second.manifest_path.read_text(
        encoding="utf-8"
    )
    assert first.manifest["workload_checksum"] == second.manifest["workload_checksum"]


def test_different_seed_changes_workload_checksum(tmp_path: Path) -> None:
    dataset_manifest = _dataset_manifest(tmp_path)
    schedule = constant_schedule(name="different-seed", duration_seconds=5, rate=20)

    first = generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=get_builtin_profile("balanced", seed=42),
        schedule=schedule,
        output_path=tmp_path / "seed42.jsonl",
    )
    second = generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=get_builtin_profile("balanced", seed=43),
        schedule=schedule,
        output_path=tmp_path / "seed43.jsonl",
    )

    assert first.workload_path.read_text(encoding="utf-8") != second.workload_path.read_text(
        encoding="utf-8"
    )
    assert first.manifest["workload_checksum"] != second.manifest["workload_checksum"]


def test_read_heavy_ratio_matches_profile(tmp_path: Path) -> None:
    workload = _workload(
        tmp_path,
        profile_name="read-heavy",
        schedule_name="read-heavy-ratio",
        duration_seconds=20,
        rate=100,
    )
    stats = calculate_workload_file_stats(workload.workload_path)

    assert abs((stats.reads / stats.total_operations) - 0.90) < 0.04
    assert abs((stats.writes / stats.total_operations) - 0.10) < 0.04


def test_write_heavy_ratio_matches_profile(tmp_path: Path) -> None:
    workload = _workload(
        tmp_path,
        profile_name="write-heavy",
        schedule_name="write-heavy-ratio",
        duration_seconds=20,
        rate=100,
    )
    stats = calculate_workload_file_stats(workload.workload_path)

    assert abs((stats.reads / stats.total_operations) - 0.30) < 0.05
    assert abs((stats.writes / stats.total_operations) - 0.70) < 0.05


def test_multi_tenant_skew_prefers_busy_bucket(tmp_path: Path) -> None:
    dataset_manifest = _dataset_manifest(tmp_path, tenant_count=50, entities_per_tenant=10)
    schedule = constant_schedule(name="skew", duration_seconds=10, rate=100)
    workload = generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=get_builtin_profile("multi-tenant-skew", seed=42),
        schedule=schedule,
        output_path=tmp_path / "skew.jsonl",
    )
    catalog = DatasetIdentityCatalog.from_path(dataset_manifest)
    stats = calculate_workload_file_stats(workload.workload_path)
    counts = [stats.operations_by_tenant.get(tenant_id, 0) for tenant_id in catalog.tenant_ids]
    cold_half_average = sum(sorted(counts)[:25]) / 25

    assert max(counts) > cold_half_average * 5


def test_hot_contention_targets_hot_entities_for_majority_of_writes(tmp_path: Path) -> None:
    dataset_manifest = _dataset_manifest(tmp_path, tenant_count=20, entities_per_tenant=100)
    schedule = constant_schedule(name="hot-contention", duration_seconds=10, rate=100)
    workload = generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=get_builtin_profile("hot-contention", seed=42),
        schedule=schedule,
        output_path=tmp_path / "hot.jsonl",
    )
    hot_map = {
        tenant_id: set(entity_ids)
        for tenant_id, entity_ids in workload.manifest["hot_set"][
            "hot_entity_ids_by_tenant"
        ].items()
    }
    hot_write_items = 0
    total_write_items = 0

    for operation in iter_workload(workload.workload_path):
        if isinstance(operation.payload, EntityDeltaPayload):
            total_write_items += 1
            hot_write_items += int(
                operation.payload.entity_id in hot_map.get(operation.tenant_id, set())
            )
        elif isinstance(operation.payload, MultiEntityTransactionPayload):
            for item in operation.payload.items:
                total_write_items += 1
                hot_write_items += int(item.entity_id in hot_map.get(operation.tenant_id, set()))

    assert total_write_items > 0
    assert hot_write_items / total_write_items > 0.50


def test_all_normal_operations_reference_valid_dataset_entities(tmp_path: Path) -> None:
    workload = _workload(tmp_path, profile_name="balanced")

    result = validate_workload(workload.workload_path)

    assert result.ok
    assert result.errors == ()


def test_multi_entity_transactions_are_tenant_local_and_well_formed(tmp_path: Path) -> None:
    workload = _workload(
        tmp_path,
        profile_name="write-heavy",
        schedule_name="multi",
        duration_seconds=10,
        rate=100,
    )
    multi_payloads = [
        operation.payload
        for operation in iter_workload(workload.workload_path)
        if operation.operation_type == OperationType.MULTI_ENTITY_TRANSACTION
    ]

    assert multi_payloads
    for payload in multi_payloads:
        assert isinstance(payload, MultiEntityTransactionPayload)
        assert 2 <= len(payload.items) <= 5
        assert len({item.entity_id for item in payload.items}) == len(payload.items)
        assert all(item.delta != 0 for item in payload.items)


def test_client_and_global_schedule_ordering(tmp_path: Path) -> None:
    workload = _workload(tmp_path, profile_name="balanced")
    previous_global_offset = -1
    previous_client_offsets: dict[str, int] = {}
    previous_sequence = -1

    for operation in iter_workload(workload.workload_path):
        assert operation.sequence_no == previous_sequence + 1
        assert operation.scheduled_offset_ms >= previous_global_offset
        assert operation.scheduled_offset_ms >= previous_client_offsets.get(
            operation.virtual_client_id,
            -1,
        )
        previous_sequence = operation.sequence_no
        previous_global_offset = operation.scheduled_offset_ms
        previous_client_offsets[operation.virtual_client_id] = operation.scheduled_offset_ms


def test_serialization_round_trip(tmp_path: Path) -> None:
    workload = _workload(tmp_path, profile_name="balanced")
    operation = next(iter_workload(workload.workload_path))

    assert operation_from_dict(operation_to_dict(operation)) == operation


def test_streaming_reader_iterates_without_full_load(tmp_path: Path) -> None:
    workload = _workload(
        tmp_path,
        profile_name="balanced",
        schedule_name="streaming",
        duration_seconds=20,
        rate=100,
    )
    count = 0
    for _operation in iter_workload(workload.workload_path):
        count += 1

    assert count == workload.manifest["operation_count"]


def test_checksum_validation_detects_tampered_operation(tmp_path: Path) -> None:
    workload = _workload(tmp_path, profile_name="balanced")
    lines = workload.workload_path.read_text(encoding="utf-8").splitlines()
    first = json.loads(lines[0])
    first["operation_id"] = "tampered"
    lines[0] = json.dumps(first, sort_keys=True)
    workload.workload_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = validate_workload(workload.workload_path)

    assert not result.ok
    assert any("checksum" in error for error in result.errors)


def test_shadow_versions_generate_coherent_expected_versions(tmp_path: Path) -> None:
    workload = _workload(
        tmp_path,
        profile_name="hot-contention",
        schedule_name="shadow",
        duration_seconds=10,
        rate=100,
    )
    coherent_seen = False
    deliberate_seen = False

    for operation in iter_workload(workload.workload_path):
        if isinstance(operation.payload, EntityDeltaPayload):
            if operation.payload.expected_version_is_deliberately_stale:
                deliberate_seen = True
            elif operation.payload.expected_version is not None:
                coherent_seen = True
        elif isinstance(operation.payload, MultiEntityTransactionPayload):
            for item in operation.payload.items:
                if item.expected_version_is_deliberately_stale:
                    deliberate_seen = True
                elif item.expected_version is not None:
                    coherent_seen = True

    assert coherent_seen
    assert deliberate_seen


@pytest.mark.workload_scale
@pytest.mark.skipif(
    os.environ.get("EPHEMERALDB_RUN_WORKLOAD_SCALE_TESTS") != "1",
    reason="set EPHEMERALDB_RUN_WORKLOAD_SCALE_TESTS=1 to run 100k workload generation",
)
def test_workload_generation_100k_performance_sanity(tmp_path: Path) -> None:
    dataset_manifest = _dataset_manifest(tmp_path, tenant_count=10, entities_per_tenant=100)
    schedule = constant_schedule(name="scale", duration_seconds=100, rate=1000)
    workload = generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=get_builtin_profile("balanced", seed=42),
        schedule=schedule,
        output_path=tmp_path / "scale.jsonl",
    )

    assert workload.manifest["operation_count"] == 100_000


def _dataset_manifest(
    tmp_path: Path,
    *,
    tenant_count: int = 10,
    entities_per_tenant: int = 20,
) -> Path:
    dataset = generate_dataset(
        tmp_path / f"dataset-{tenant_count}-{entities_per_tenant}",
        DatasetConfig(
            tenant_count=tenant_count,
            entities_per_tenant=entities_per_tenant,
            relationships_per_entity=2,
            metadata_entries_per_entity=3,
            seed=42,
        ),
    )
    return dataset.manifest_path


def _workload(
    tmp_path: Path,
    *,
    profile_name: str,
    schedule_name: str = "test",
    duration_seconds: int = 5,
    rate: int = 50,
):
    dataset_manifest = _dataset_manifest(tmp_path)
    return generate_workload(
        dataset_manifest_path=dataset_manifest,
        profile=get_builtin_profile(profile_name, seed=42).with_overrides(virtual_client_count=10),
        schedule=constant_schedule(
            name=schedule_name,
            duration_seconds=duration_seconds,
            rate=rate,
        ),
        output_path=tmp_path / f"{profile_name}-{schedule_name}.jsonl",
    )
