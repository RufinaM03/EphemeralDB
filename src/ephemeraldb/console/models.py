"""Bounded public experiment contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(default="Tenant workload", min_length=1, max_length=100)
    backend: Literal["local", "s3-express", "aurora", "http"] = "local"
    mode: Literal["single", "batch"] = "single"
    tenants: int = Field(default=8, ge=1, le=100)
    entities: int = Field(default=50, ge=5, le=1000)
    rate: int = Field(default=40, ge=1, le=1000)
    duration: int = Field(default=10, ge=1, le=300)
    warmup: int = Field(default=2, ge=0, le=30)
    concurrency: int = Field(default=8, ge=1, le=64)
    batch_window_ms: int = Field(default=5, ge=0, le=100)
    checkpoint_every: int = Field(default=0, ge=0, le=10000)
    profile: Literal[
        "read-heavy", "balanced", "write-heavy", "hot-contention", "multi-tenant-skew"
    ] = "balanced"
    seed: int = Field(default=42, ge=0, le=2**31 - 1)

    @model_validator(mode="after")
    def bounded_work(self) -> RunRequest:
        if self.rate * (self.duration + self.warmup) > 20000:
            raise ValueError("run exceeds 20,000 operations; reduce rate or duration")
        if self.tenants * self.entities > 20000:
            raise ValueError("run exceeds 20,000 entities")
        return self


class FailureRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    scenario: Literal["cache-loss", "ack-loss", "stale-owner"] = "cache-loss"
    mode: Literal["single", "batch"] = "single"


class FisRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    template_id: str = Field(pattern=r"^[A-Za-z0-9-]{1,80}$")
