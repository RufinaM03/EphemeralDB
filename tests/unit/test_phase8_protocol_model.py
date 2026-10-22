"""Independent bounded model; it does not call production protocol code."""

from __future__ import annotations

from itertools import permutations


def _run(events: tuple[str, ...]) -> tuple[int, tuple[str, ...], dict[str, bool]]:
    epoch = 1
    accepted: list[str] = []
    responses = {"a": False, "b": False}
    for event in events:
        if event == "takeover":
            epoch = 2
        elif event == "commit-a":
            if epoch == 1 and not accepted:
                accepted.append("a")
        elif event == "commit-b":
            if epoch == 2 and not accepted:
                accepted.append("b")
        elif event == "respond-a":
            responses["a"] = "a" in accepted
        elif event == "respond-b":
            responses["b"] = "b" in accepted
    return epoch, tuple(accepted), responses


def test_exhaustive_two_owner_takeover_model() -> None:
    events = ("commit-a", "takeover", "commit-b", "respond-a", "respond-b")
    explored = 0
    for ordering in permutations(events):
        # A cannot receive a response before its request; likewise B.
        if ordering.index("respond-a") < ordering.index("commit-a"):
            continue
        if ordering.index("respond-b") < ordering.index("commit-b"):
            continue
        epoch, accepted, responses = _run(ordering)
        explored += 1
        assert epoch == 2
        assert len(accepted) <= 1
        if "commit-a" in ordering[ordering.index("takeover") + 1 :]:
            assert "a" not in accepted
        if responses["a"]:
            assert accepted == ("a",)
        if responses["b"]:
            assert accepted == ("b",)
    assert explored == 30
