"""S3 Standard implementation of the fenced authority primitive."""

from __future__ import annotations

import importlib
import threading
from typing import Any

from ephemeraldb.aws.s3_express import _client_error_code
from ephemeraldb.checkpoint.store import safe_key
from ephemeraldb.config.settings import Settings
from ephemeraldb.core.errors import OwnershipError

from .sqlite_store import MAX_AUTHORITY_OBJECT_BYTES
from .store import CasResult, ControlSnapshot


class S3AuthorityStore:
    """Conditional single-object CAS; ETags are treated only as opaque validators."""

    def __init__(self, settings: Settings, *, client: Any = None) -> None:
        bucket = settings.s3_checkpoint_bucket
        if not bucket or bucket.endswith("--x-s3") or bucket == settings.s3_express_bucket:
            raise OwnershipError("fenced authority requires a general-purpose S3 bucket")
        self.bucket = bucket
        self.prefix = safe_key(f"{settings.s3_checkpoint_prefix.strip('/')}/control")
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()
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

    def _count(self, operation: str) -> None:
        with self._lock:
            self._counts[operation] = self._counts.get(operation, 0) + 1

    def counters(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def create_control(self, key: str, payload: bytes) -> CasResult:
        _payload_budget(payload)
        self._count("CONTROL_CREATE")
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(key),
                Body=payload,
                ContentType="application/json",
                StorageClass="STANDARD",
                IfNoneMatch="*",
            )
        except Exception as exc:
            code = _client_error_code(exc)
            if code in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                return CasResult.CONFLICT
            if code in {"AccessDenied", "403", "InvalidAccessKeyId", "NoSuchBucket"}:
                raise OwnershipError(f"S3 authority bootstrap denied: {code}") from exc
            return CasResult.UNKNOWN
        return CasResult.APPLIED

    def get_control(self, key: str) -> ControlSnapshot:
        self._count("CONTROL_GET")
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
            body = response["Body"]
            try:
                payload = body.read(MAX_AUTHORITY_OBJECT_BYTES + 1)
            finally:
                body.close()
        except Exception as exc:
            raise OwnershipError(f"S3 authority read failed: {_client_error_code(exc)}") from exc
        if not isinstance(payload, bytes):
            raise OwnershipError("S3 authority returned a non-bytes body")
        _payload_budget(payload)
        etag = response.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise OwnershipError("S3 authority response lacks an ETag validator")
        return ControlSnapshot(payload, etag)

    def compare_and_swap(
        self, key: str, expected_validator: str, payload: bytes
    ) -> CasResult:
        _payload_budget(payload)
        self._count("CONTROL_CAS")
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(key),
                Body=payload,
                ContentType="application/json",
                StorageClass="STANDARD",
                IfMatch=expected_validator,
            )
        except Exception as exc:
            code = _client_error_code(exc)
            if code in {
                "PreconditionFailed", "412", "ConditionalRequestConflict", "409", "404",
                "NoSuchKey",
            }:
                return CasResult.CONFLICT
            if code in {"AccessDenied", "403", "InvalidAccessKeyId", "NoSuchBucket"}:
                raise OwnershipError(f"S3 authority CAS denied: {code}") from exc
            return CasResult.UNKNOWN
        return CasResult.APPLIED

    def put_immutable(self, key: str, payload: bytes) -> None:
        _payload_budget(payload)
        self._count("IMMUTABLE_PUT")
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(key),
                Body=payload,
                ContentType="application/json",
                StorageClass="STANDARD",
                IfNoneMatch="*",
            )
        except Exception as exc:
            code = _client_error_code(exc)
            if code not in {"PreconditionFailed", "412"}:
                # Any response loss is resolved only through positive read-back.
                if code in {"AccessDenied", "403", "InvalidAccessKeyId", "NoSuchBucket"}:
                    raise OwnershipError(f"S3 immutable PUT denied: {code}") from exc
        actual = self.get_immutable(key)
        if actual != payload:
            raise OwnershipError("S3 immutable object conflicts with stable key/content")

    def get_immutable(self, key: str) -> bytes:
        self._count("IMMUTABLE_GET")
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
            body = response["Body"]
            try:
                payload = body.read(MAX_AUTHORITY_OBJECT_BYTES + 1)
            finally:
                body.close()
        except Exception as exc:
            raise OwnershipError(
                f"required S3 immutable object unavailable: {_client_error_code(exc)}"
            ) from exc
        if not isinstance(payload, bytes):
            raise OwnershipError("S3 immutable object is not bytes")
        _payload_budget(payload)
        return payload


def _payload_budget(payload: bytes) -> None:
    if not payload or len(payload) > MAX_AUTHORITY_OBJECT_BYTES:
        raise OwnershipError("authority payload violates byte budget")
