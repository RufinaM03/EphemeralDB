"""Bounded streaming S3 Standard checkpoint storage with immutable publication."""

from __future__ import annotations

import importlib
import tempfile
from pathlib import Path
from typing import Any

from ephemeraldb.aws.s3_express import _client_error_code
from ephemeraldb.checkpoint.models import CheckpointError
from ephemeraldb.checkpoint.store import (
    MAX_OBJECT_BYTES,
    MAX_OBJECTS,
    Accounting,
    file_hash,
    safe_key,
)
from ephemeraldb.config.settings import Settings


class S3StandardStore(Accounting):
    def __init__(self, settings: Settings, *, client: Any = None) -> None:
        super().__init__()
        bucket = settings.s3_checkpoint_bucket
        if not bucket or bucket.endswith("--x-s3") or bucket == settings.s3_express_bucket:
            raise CheckpointError("configure a separate general-purpose S3_CHECKPOINT_BUCKET")
        self.bucket = bucket
        self.prefix = safe_key(settings.s3_checkpoint_prefix.strip("/"))
        if client is None:
            boto3 = importlib.import_module("boto3")
            config_type = importlib.import_module("botocore.config").Config
            config = config_type(
                connect_timeout=settings.aws_connect_timeout_seconds,
                read_timeout=settings.aws_read_timeout_seconds,
                retries={
                    "mode": settings.aws_retry_mode,
                    "max_attempts": settings.aws_max_attempts,
                },
            )
            client = boto3.Session(
                region_name=settings.aws_region, profile_name=settings.aws_profile
            ).client("s3", config=config)
        self.client = client

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{safe_key(key)}"

    def put_file(self, key: str, source: Path) -> None:
        remote_key = self._key(key)
        if source.stat().st_size > MAX_OBJECT_BYTES:
            raise CheckpointError("snapshot exceeds single-PUT budget")
        expected = file_hash(source)
        self.count("PUT")
        try:
            with source.open("rb") as body:
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=remote_key,
                    Body=body,
                    StorageClass="STANDARD",
                    IfNoneMatch="*",
                )
        except Exception as exc:
            self.count("PUT_FAILED")
            # Access denial is not corruption and must never trigger fallback.
            if _client_error_code(exc) in {"AccessDenied", "403", "InvalidAccessKeyId"}:
                raise
            # 412 or response loss: only identical positive read-back resolves it.
            try:
                self._verify(key, expected, source.stat().st_size)
            except Exception as verification:
                raise CheckpointError(f"upload unresolved/conflicting for {key}") from verification
            return
        self._verify(key, expected, source.stat().st_size)

    def _verify(self, key: str, expected: str, size: int) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "verify"
            self.get_file(key, path)
            if path.stat().st_size != size or file_hash(path) != expected:
                raise CheckpointError("immutable object conflict or corrupted upload")

    def get_file(self, key: str, target: Path) -> None:
        remote_key = self._key(key)
        self.count("GET")
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=remote_key)
        except Exception:
            self.count("GET_FAILED")
            raise
        body = response["Body"]
        size = 0
        try:
            with target.open("wb") as output:
                while chunk := body.read(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_OBJECT_BYTES:
                        raise CheckpointError("download exceeds byte budget")
                    output.write(chunk)
        finally:
            body.close()
            with self._lock:
                self.bytes_read += size

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        remote_prefix = f"{self.prefix}/{safe_key(prefix.rstrip('/'))}/"
        keys: list[str] = []
        token = None
        seen: set[str] = set()
        while True:
            args = {"Bucket": self.bucket, "Prefix": remote_prefix}
            if token is not None:
                args["ContinuationToken"] = token
            self.count("LIST")
            try:
                response = self.client.list_objects_v2(**args)
            except Exception:
                self.count("LIST_FAILED")
                raise
            for entry in response.get("Contents", ()):
                key = entry["Key"]
                if not key.startswith(remote_prefix):
                    raise CheckpointError("out-of-prefix listing")
                keys.append(key[len(self.prefix) + 1 :])
                if len(keys) > MAX_OBJECTS:
                    raise CheckpointError("object scan budget exceeded")
            if not response.get("IsTruncated"):
                return tuple(keys)
            token = response.get("NextContinuationToken")
            if not isinstance(token, str) or token in seen:
                raise CheckpointError("listing failed to advance")
            seen.add(token)
