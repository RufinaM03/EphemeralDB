"""Environment-derived settings for Phase 0.

The settings layer intentionally performs no network calls and does not require
AWS credentials. It only normalizes local process configuration.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings shared by benchmark components."""

    app_env: str
    aws_region: str
    aws_profile: str | None
    wal_store: str
    wal_persistence_mode: str
    s3_express_bucket: str | None
    s3_express_zone_id: str | None
    s3_express_prefix: str
    wal_batch_window_ms: int
    wal_batch_max_records: int
    wal_batch_max_bytes: int
    wal_batch_max_queue_records: int
    wal_batch_flush_on_shutdown: bool
    aws_connect_timeout_seconds: float
    aws_read_timeout_seconds: float
    aws_max_attempts: int
    aws_retry_mode: str
    cache_dir: Path
    log_level: str
    benchmark_seed: int
    s3_checkpoint_bucket: str | None = None
    s3_checkpoint_prefix: str = "checkpoints"


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """Load settings from environment-like key/value data."""

    source: Mapping[str, str] = os.environ if environ is None else environ
    return Settings(
        app_env=_read_text(source, "APP_ENV", "local"),
        aws_region=_read_text(source, "AWS_REGION", "ap-south-1"),
        aws_profile=_read_optional_text(source, "AWS_PROFILE"),
        wal_store=_read_text(source, "WAL_STORE", "local"),
        wal_persistence_mode=_read_text(source, "WAL_PERSISTENCE_MODE", "single"),
        s3_express_bucket=_read_optional_text(source, "S3_EXPRESS_BUCKET"),
        s3_express_zone_id=_read_optional_text(source, "S3_EXPRESS_ZONE_ID"),
        s3_express_prefix=_read_text(source, "S3_EXPRESS_PREFIX", "wal"),
        wal_batch_window_ms=_read_int(source, "WAL_BATCH_WINDOW_MS", 5),
        wal_batch_max_records=_read_int(source, "WAL_BATCH_MAX_RECORDS", 100),
        wal_batch_max_bytes=_read_int(source, "WAL_BATCH_MAX_BYTES", 1024 * 1024),
        wal_batch_max_queue_records=_read_int(source, "WAL_BATCH_MAX_QUEUE_RECORDS", 10000),
        wal_batch_flush_on_shutdown=_read_bool(source, "WAL_BATCH_FLUSH_ON_SHUTDOWN", True),
        aws_connect_timeout_seconds=_read_float(source, "AWS_CONNECT_TIMEOUT_SECONDS", 3.0),
        aws_read_timeout_seconds=_read_float(source, "AWS_READ_TIMEOUT_SECONDS", 10.0),
        aws_max_attempts=_read_int(source, "AWS_MAX_ATTEMPTS", 3),
        aws_retry_mode=_read_text(source, "AWS_RETRY_MODE", "standard"),
        cache_dir=Path(_read_text(source, "CACHE_DIR", "/cache")),
        log_level=_read_text(source, "LOG_LEVEL", "INFO").upper(),
        benchmark_seed=_read_int(source, "BENCHMARK_SEED", 42),
        s3_checkpoint_bucket=_read_optional_text(source, "S3_CHECKPOINT_BUCKET"),
        s3_checkpoint_prefix=_read_text(source, "S3_CHECKPOINT_PREFIX", "checkpoints"),
    )


def _read_text(source: Mapping[str, str], name: str, default: str) -> str:
    value = source.get(name, default).strip()
    if not value:
        msg = f"{name} must not be empty"
        raise ValueError(msg)
    return value


def _read_optional_text(source: Mapping[str, str], name: str) -> str | None:
    value = source.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _read_int(source: Mapping[str, str], name: str, default: int) -> int:
    raw_value = source.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        msg = f"{name} must be an integer"
        raise ValueError(msg) from exc


def _read_float(source: Mapping[str, str], name: str, default: float) -> float:
    raw_value = source.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        value = float(raw_value)
    except ValueError as exc:
        msg = f"{name} must be a number"
        raise ValueError(msg) from exc
    if value <= 0:
        msg = f"{name} must be positive"
        raise ValueError(msg)
    return value


def _read_bool(source: Mapping[str, str], name: str, default: bool) -> bool:
    raw_value = source.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    value = raw_value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    msg = f"{name} must be a boolean"
    raise ValueError(msg)
