import json

import pytest

from ephemeraldb.recovery.demo import run_demo


@pytest.mark.parametrize("mode", ["single", "batch"])
@pytest.mark.parametrize("scenario", ["post-ack", "durable-before-ack"])
def test_real_process_kill_cache_loss_and_independent_oracle(tmp_path, mode, scenario):
    result = run_demo(tmp_path / "run", mode=mode, scenario=scenario)
    assert result["child_exitcode"] != 0
    assert result["complete_cache_deleted"]
    assert result["acknowledged_loss"] == 0
    assert result["acknowledged_writes"] == (8 if scenario == "post-ack" else 7)
    assert result["durable_unacknowledged_writes"] == (0 if scenario == "post-ack" else 1)
    assert all(
        r["checkpoint_sequence"] == 2 and r["final_sequence"] == 4 and r["first_new_sequence"] == 5
        for r in result["recoveries"]
    )
    artifact = json.loads((tmp_path / "run" / "result.json").read_text())
    assert artifact["status"] == "passed"
    for recovery in result["recoveries"]:
        calls = recovery["wal_request_accounting"]["calls"]
        assert calls["wal_discovery.GET"] == recovery["wal_scan"]["objects_scanned"]
        assert not any(key.endswith("PUT") for key in calls)
        assert recovery["request_accounting"]["calls"]["checkpoint_discovery.LIST"] == 1
