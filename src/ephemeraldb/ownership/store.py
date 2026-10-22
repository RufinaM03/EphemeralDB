"""Small primitive shared by local and S3 authority adapters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ControlSnapshot:
    payload: bytes
    validator: str


class CasResult(StrEnum):
    APPLIED = "APPLIED"
    CONFLICT = "CONFLICT"
    UNKNOWN = "UNKNOWN"


class AuthorityStore(Protocol):
    def create_control(self, key: str, payload: bytes) -> CasResult: ...

    def get_control(self, key: str) -> ControlSnapshot: ...

    def compare_and_swap(
        self, key: str, expected_validator: str, payload: bytes
    ) -> CasResult: ...

    def put_immutable(self, key: str, payload: bytes) -> None: ...

    def get_immutable(self, key: str) -> bytes: ...

    def counters(self) -> dict[str, int]: ...
