import os
import uuid
from dataclasses import replace

import pytest

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.aws.s3_standard import S3StandardStore
from ephemeraldb.checkpoint.coordinator import CheckpointCoordinator
from ephemeraldb.checkpoint.models import boundary_key
from ephemeraldb.config.settings import load_settings
from ephemeraldb.engine.engine import TenantDatabaseEngine
from ephemeraldb.recovery.coordinator import restore
from ephemeraldb.recovery.demo import operation
from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.wal.batching import BatchedWalCommitter, WalBatchingConfig

pytestmark = pytest.mark.aws


@pytest.mark.parametrize("mode", ["single", "batch"])
def test_tiny_standard_checkpoint_express_wal_restore(tmp_path, mode):
    if os.environ.get("RUN_AWS_TESTS") != "1":
        pytest.skip("set RUN_AWS_TESTS=1 and configure S3_CHECKPOINT_BUCKET and S3_EXPRESS_BUCKET")
    settings = load_settings()
    if not settings.s3_checkpoint_bucket or not settings.s3_express_bucket:
        pytest.skip("both S3_CHECKPOINT_BUCKET and S3_EXPRESS_BUCKET are required")
    boto3 = pytest.importorskip("boto3")
    if boto3.Session(profile_name=settings.aws_profile).get_credentials() is None:
        pytest.skip("configure AWS credentials using the normal SDK provider chain")
    run = "phase7-" + uuid.uuid4().hex
    settings = replace(
        settings,
        s3_express_prefix=f"{settings.s3_express_prefix}/{run}",
        s3_checkpoint_prefix=f"{settings.s3_checkpoint_prefix}/{run}",
    )
    print(
        {
            "run": run,
            "transactions": 2,
            "checkpoint_count": 2,
            "maximum_snapshot_bytes": 67108864,
            "mode": mode,
            "checkpoint_bucket": settings.s3_checkpoint_bucket,
            "wal_bucket": settings.s3_express_bucket,
        }
    )
    wal = S3ExpressWalStore(S3ExpressWalConfig.from_settings(settings))
    store = S3StandardStore(settings)
    coordinator = CheckpointCoordinator(store, run)
    dataset = generate_dataset(
        tmp_path / "source",
        DatasetConfig(
            tenant_count=1,
            entities_per_tenant=4,
            relationships_per_entity=1,
            metadata_entries_per_entity=1,
            seed=42,
        ),
    )
    tenant = operation(0, 0).tenant_id

    def open_engine(path, initialize=False):
        if mode == "single":
            return TenantDatabaseEngine.from_dataset_dir(
                path, wal_store=wal, initialize_durability=initialize
            )
        return TenantDatabaseEngine.from_dataset_dir(
            path,
            wal_committer=BatchedWalCommitter(wal, config=WalBatchingConfig(batch_window_ms=0)),
            initialize_durability=initialize,
        )

    engine = open_engine(dataset.output_dir, True)
    passed = False
    try:
        coordinator.create(engine, tenant)
        assert engine.execute(operation(0, 0)).success
        coordinator.create(engine, tenant)
        assert engine.execute(operation(0, 1)).success
        expected = engine.calculate_tenant_checksum(tenant)
        boundary = coordinator.seal(engine, tenant)
        engine.close_all()
        destination = tmp_path / "restored"
        result = restore(
            store,
            wal,
            namespace=run,
            tenant=tenant,
            boundary_object_key=boundary_key(boundary),
            destination=destination,
            old_writers_stopped=True,
        )
        assert result["logical_checksum"] == expected
        engine = open_engine(destination)
        assert engine.execute(operation(0, 0)).duplicate_transaction
        assert engine.execute(operation(0, 2)).wal_sequence_no == 3
        passed = True
    finally:
        engine.close_all()
        if passed:
            for key in store.list_keys(run + "/"):
                store.client.delete_object(Bucket=store.bucket, Key=store._key(key))
            for key in {item.key for item in wal.inspect_history(tenant)}:
                wal._request("delete_object", Bucket=wal.config.bucket, Key=key)
        else:
            print({"cleanup": "retained failed-run evidence", "isolated_run": run})
