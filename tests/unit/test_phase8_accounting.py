from pathlib import Path

from ephemeraldb.ownership.accounting_demo import run


def test_same_workload_accounting_includes_per_tenant_fence_cost(tmp_path: Path) -> None:
    result = run(tmp_path / "accounting")
    legacy = result["legacy_payload_only"]
    fenced = result["fenced"]
    assert legacy["payload_bytes"] == fenced["payload_bytes"]
    assert legacy["requests"] == {"IMMUTABLE_PUT": 1}
    assert fenced["requests"] == {
        "CONTROL_CREATE": 2,
        "IMMUTABLE_PUT": 3,
        "CONTROL_GET": 2,
        "CONTROL_CAS": 2,
    }
