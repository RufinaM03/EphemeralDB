from ephemeraldb.core.protocols import (
    BenchmarkEntity,
    CheckpointStore,
    DatabaseEngine,
    MetricsSink,
    MetricTags,
    OwnershipRegistry,
    TransactionPayload,
    WalDurabilityStore,
    WalRecord,
)


def test_protocols_and_type_aliases_are_importable() -> None:
    assert WalDurabilityStore.__name__ == "WalDurabilityStore"
    assert CheckpointStore.__name__ == "CheckpointStore"
    assert OwnershipRegistry.__name__ == "OwnershipRegistry"
    assert DatabaseEngine.__name__ == "DatabaseEngine"
    assert MetricsSink.__name__ == "MetricsSink"
    assert WalRecord is not None
    assert TransactionPayload is not None
    assert BenchmarkEntity is not None
    assert MetricTags is not None
