from __future__ import annotations

import os
import uuid
from dataclasses import replace

import pytest

from ephemeraldb.config.settings import load_settings
from ephemeraldb.ownership.coordinator import OwnershipCoordinator
from ephemeraldb.ownership.s3_store import S3AuthorityStore

pytestmark = pytest.mark.aws


def test_real_s3_conditional_bootstrap_and_competing_takeover() -> None:
    if os.environ.get("RUN_AWS_TESTS") != "1":
        pytest.skip("set RUN_AWS_TESTS=1 and configure S3_CHECKPOINT_BUCKET")
    settings = load_settings()
    if not settings.s3_checkpoint_bucket:
        pytest.skip("S3_CHECKPOINT_BUCKET is required")
    boto3 = pytest.importorskip("boto3")
    if boto3.Session(profile_name=settings.aws_profile).get_credentials() is None:
        pytest.skip("configure AWS credentials using the normal SDK provider chain")
    run = "phase8-" + uuid.uuid4().hex
    settings = replace(
        settings,
        s3_checkpoint_prefix=f"{settings.s3_checkpoint_prefix}/{run}",
    )
    print(
        {
            "run": run,
            "maximum_control_puts": 3,
            "maximum_control_gets": 4,
            "maximum_descriptor_puts": 0,
            "retention": "run objects retained; no unsafe cleanup",
        }
    )
    coordinator = OwnershipCoordinator(S3AuthorityStore(settings), run)
    first = coordinator.bootstrap(
        "tenant-a",
        session_id="runtime-a",
        bootstrap_key="bootstrap.db",
        bootstrap_checksum="0" * 64,
    )
    second = coordinator.takeover("tenant-a", session_id="runtime-b")
    observed, _ = coordinator.status("tenant-a")
    assert second.owner_epoch == first.owner_epoch + 1
    assert observed == second
