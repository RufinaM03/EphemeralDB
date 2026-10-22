"""Amazon S3 Express One Zone WAL durability store."""

from __future__ import annotations

import importlib
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from ephemeraldb.config.settings import Settings
from ephemeraldb.core.errors import (
    RemoteWalConflictError,
    S3ExpressConfigurationError,
    S3ExpressReadError,
    S3ExpressWriteError,
)
from ephemeraldb.wal.batching.codec import decode_batch, encode_batch, validate_batch
from ephemeraldb.wal.batching.models import WAL_BATCH_FORMAT_VERSION, WalBatch
from ephemeraldb.wal.codec import decode_record, encode_record
from ephemeraldb.wal.durability import PersistedBatchReceipt, PersistedWalReceipt, StoredWalRecord
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.validation import validate_record
from ephemeraldb.wal.writer import safe_tenant_dir_name, sequence_filename


@dataclass(frozen=True, slots=True)
class S3ExpressWalConfig:
    bucket: str
    region: str = "ap-south-1"
    prefix: str = "wal"
    zone_id: str | None = None
    profile: str | None = None
    connect_timeout_seconds: float = 3.0
    read_timeout_seconds: float = 10.0
    max_attempts: int = 3
    retry_mode: str = "standard"

    def __post_init__(self) -> None:
        if not self.bucket.strip():
            msg = "S3 Express bucket must be configured"
            raise S3ExpressConfigurationError(msg)
        if not self.region.strip():
            msg = "AWS region must be configured"
            raise S3ExpressConfigurationError(msg)
        if self.connect_timeout_seconds <= 0 or self.read_timeout_seconds <= 0:
            msg = "AWS connect/read timeouts must be positive"
            raise S3ExpressConfigurationError(msg)
        if self.max_attempts < 1:
            msg = "AWS_MAX_ATTEMPTS must be >= 1"
            raise S3ExpressConfigurationError(msg)
        prefix = self.prefix.strip().strip("/")
        object.__setattr__(self, "bucket", self.bucket.strip())
        object.__setattr__(self, "region", self.region.strip())
        object.__setattr__(self, "prefix", prefix or "wal")
        if self.zone_id is not None and not self.zone_id.strip():
            object.__setattr__(self, "zone_id", None)
        if self.profile is not None and not self.profile.strip():
            object.__setattr__(self, "profile", None)

    @classmethod
    def from_settings(cls, settings: Settings) -> S3ExpressWalConfig:
        return cls(
            bucket=settings.s3_express_bucket or "",
            region=settings.aws_region,
            prefix=settings.s3_express_prefix,
            zone_id=settings.s3_express_zone_id,
            profile=settings.aws_profile,
            connect_timeout_seconds=settings.aws_connect_timeout_seconds,
            read_timeout_seconds=settings.aws_read_timeout_seconds,
            max_attempts=settings.aws_max_attempts,
            retry_mode=settings.aws_retry_mode,
        )


class S3ExpressWalStore:
    """Synchronous S3 Express durability store for WAL records and batches."""

    requires_bootstrap = True

    def __init__(
        self,
        config: S3ExpressWalConfig,
        *,
        client: Any | None = None,
    ) -> None:
        self.config = config
        self._client = _build_s3_client(config) if client is None else client
        self._request_lock = threading.Lock()
        self._request_counts: dict[str, int] = {}
        self.request_phase = "setup"
        self.scan_metrics: dict[str, int] = {}

    @property
    def namespace_id(self) -> str:
        return f"s3://{self.config.bucket}/{self.config.prefix}"

    def _request(self, operation: str, **kwargs: Any) -> Any:
        with self._request_lock:
            label = f"{self.request_phase}.{operation}"
            self._request_counts[label] = self._request_counts.get(label, 0) + 1
        try:
            return getattr(self._client, operation)(**kwargs)
        except Exception:
            with self._request_lock:
                failed = label + ".failed"
                self._request_counts[failed] = self._request_counts.get(failed, 0) + 1
            raise

    def request_accounting(self) -> dict[str, object]:
        with self._request_lock:
            return {
                "sdk_calls": dict(self._request_counts),
                "wire_attempts": None,
                "billable_usage": None,
                "scope": "SDK invocations, includes failed calls; "
                "internal SDK retries are not separately instrumented",
            }

    def inspect_history(self, tenant_id: str) -> tuple[StoredWalRecord, ...]:
        found: list[StoredWalRecord] = []
        prefixes = (
            f"{self.config.prefix}/{safe_tenant_dir_name(tenant_id)}/",
            f"{self.config.prefix}/wal-batches/",
        )
        scanned = total_bytes = total_records = irrelevant = 0
        for prefix in prefixes:
            token: str | None = None
            while True:
                request: dict[str, object] = {"Bucket": self.config.bucket, "Prefix": prefix}
                if token is not None:
                    request["ContinuationToken"] = token
                response = self._request("list_objects_v2", **request)
                for item in response.get("Contents", ()):
                    scanned += 1
                    if scanned > 1000:
                        raise S3ExpressReadError("history inspection exceeds 1000 objects")
                    key = str(item["Key"])
                    if not key.startswith(prefix):
                        raise S3ExpressReadError("listing returned an out-of-prefix key")
                    body = self._inspection_body(key, 128 * 1024 * 1024 - total_bytes)
                    total_bytes += len(body)
                    if prefix == prefixes[1]:
                        records = decode_batch(body).records
                    else:
                        record = decode_record(body)
                        validate_record(record, expected_tenant_id=tenant_id)
                        if record.sequence_no != _sequence_from_key(key):
                            raise S3ExpressReadError("WAL key/sequence mismatch")
                        records = (record,)
                    total_records += len(records)
                    irrelevant += sum(record.tenant_id != tenant_id for record in records)
                    found.extend(
                        StoredWalRecord(record, key)
                        for record in records
                        if record.tenant_id == tenant_id
                    )
                    if total_records > 10000:
                        raise S3ExpressReadError("history exceeds 10000 total records")
                if not response.get("IsTruncated"):
                    break
                next_token = str(response["NextContinuationToken"])
                if next_token == token:
                    raise S3ExpressReadError("listing did not advance")
                token = next_token
        self.scan_metrics = {
            "objects_scanned": scanned,
            "bytes_read": total_bytes,
            "irrelevant_records": irrelevant,
        }
        return tuple(sorted(found, key=lambda item: item.record.sequence_no))

    def _inspection_body(self, key: str, remaining: int) -> bytes:
        response = self._request("get_object", Bucket=self.config.bucket, Key=key)
        stream = response["Body"]
        try:
            body = stream.read(min(64 * 1024 * 1024, remaining) + 1)
            if len(body) > min(64 * 1024 * 1024, remaining):
                raise S3ExpressReadError("WAL inspection byte budget exceeded")
            if not isinstance(body, bytes):
                raise S3ExpressReadError("invalid WAL response body")
            return body
        finally:
            stream.close()

    def object_key(self, tenant_id: str, sequence_no: int) -> str:
        tenant_dir = safe_tenant_dir_name(tenant_id)
        return f"{self.config.prefix}/{tenant_dir}/{sequence_filename(sequence_no)}"

    def batch_object_key(self, batch: WalBatch) -> str:
        batch_date = batch.created_at[:10]
        return f"{self.config.prefix}/wal-batches/{batch_date}/{batch.batch_id}.json"

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        validate_record(record, expected_tenant_id=record.tenant_id)
        body = encode_record(record)
        key = self.object_key(record.tenant_id, record.sequence_no)
        metadata = _record_metadata(record)
        started_at = time.perf_counter_ns()
        try:
            response = self._request(
                "put_object",
                Bucket=self.config.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                StorageClass="EXPRESS_ONEZONE",
                Metadata=metadata,
                IfNoneMatch="*",
            )
        except Exception as exc:
            if _client_error_code(exc) in {"PreconditionFailed", "412"}:
                return self._handle_existing_object(record, body, key, started_at)
            if _client_error_code(exc) in {"ConditionalRequestConflict", "409"}:
                msg = f"S3 conditional write conflict for WAL object {key}"
                raise S3ExpressWriteError(msg) from exc
            msg = f"S3 Express WAL PUT failed for object {key}: {_safe_error_message(exc)}"
            raise S3ExpressWriteError(msg) from exc
        finished_at = time.perf_counter_ns()
        assert record.record_checksum is not None
        return PersistedWalReceipt(
            bucket=self.config.bucket,
            key=key,
            etag=_response_text(response, "ETag"),
            version_id=_response_text(response, "VersionId"),
            checksum=record.record_checksum,
            bytes_written=len(body),
            duration_ns=finished_at - started_at,
            remote_durable=True,
        )

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        body = self.fetch_record_bytes(tenant_id, sequence_no)
        record = decode_record(body)
        validate_record(record, expected_tenant_id=tenant_id)
        if record.sequence_no != sequence_no:
            msg = f"fetched WAL sequence {record.sequence_no} does not match {sequence_no}"
            raise S3ExpressReadError(msg)
        return record

    def fetch_record_bytes(self, tenant_id: str, sequence_no: int) -> bytes:
        key = self.object_key(tenant_id, sequence_no)
        try:
            response = self._request("get_object", Bucket=self.config.bucket, Key=key)
            body = response["Body"].read()
        except Exception as exc:
            msg = f"S3 Express WAL GET failed for object {key}: {_safe_error_message(exc)}"
            raise S3ExpressReadError(msg) from exc
        if not isinstance(body, bytes):
            msg = f"S3 Express WAL GET returned non-bytes body for object {key}"
            raise S3ExpressReadError(msg)
        return body

    def list_keys(
        self,
        tenant_id: str,
        *,
        start_sequence: int | None = None,
        end_sequence: int | None = None,
    ) -> tuple[str, ...]:
        tenant_prefix = f"{self.config.prefix}/{safe_tenant_dir_name(tenant_id)}/"
        keys: list[str] = []
        continuation_token: str | None = None
        while True:
            request: dict[str, object] = {
                "Bucket": self.config.bucket,
                "Prefix": tenant_prefix,
            }
            if continuation_token is not None:
                request["ContinuationToken"] = continuation_token
            try:
                response = self._request("list_objects_v2", **request)
            except Exception as exc:
                msg = f"S3 Express WAL LIST failed for prefix {tenant_prefix}"
                raise S3ExpressReadError(msg) from exc
            for item in cast(Iterable[dict[str, object]], response.get("Contents", ())):
                key = str(item["Key"])
                sequence = _sequence_from_key(key)
                if start_sequence is not None and sequence < start_sequence:
                    continue
                if end_sequence is not None and sequence > end_sequence:
                    continue
                keys.append(key)
            if not response.get("IsTruncated"):
                break
            continuation_token = str(response["NextContinuationToken"])
        return tuple(sorted(keys))

    def delete_record(self, tenant_id: str, sequence_no: int) -> None:
        key = self.object_key(tenant_id, sequence_no)
        self._request("delete_object", Bucket=self.config.bucket, Key=key)

    def check_bucket_reachable(self) -> None:
        try:
            self._request("head_bucket", Bucket=self.config.bucket)
        except Exception as exc:
            msg = f"S3 Express bucket is not reachable: {_safe_error_message(exc)}"
            raise S3ExpressConfigurationError(msg) from exc

    def _handle_existing_object(
        self,
        record: WalRecord,
        expected_body: bytes,
        key: str,
        started_at: int,
    ) -> PersistedWalReceipt:
        existing_body = self.fetch_record_bytes(record.tenant_id, record.sequence_no)
        finished_at = time.perf_counter_ns()
        if existing_body != expected_body:
            msg = f"S3 WAL object already exists with different content: {key}"
            raise RemoteWalConflictError(msg)
        assert record.record_checksum is not None
        return PersistedWalReceipt(
            bucket=self.config.bucket,
            key=key,
            etag=None,
            version_id=None,
            checksum=record.record_checksum,
            bytes_written=len(expected_body),
            duration_ns=finished_at - started_at,
            remote_durable=True,
            already_exists=True,
        )

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        validate_batch(batch)
        body = encode_batch(batch)
        key = self.batch_object_key(batch)
        metadata = _batch_metadata(batch)
        started_at = time.perf_counter_ns()
        try:
            response = self._request(
                "put_object",
                Bucket=self.config.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                StorageClass="EXPRESS_ONEZONE",
                Metadata=metadata,
                IfNoneMatch="*",
            )
        except Exception as exc:
            if _client_error_code(exc) in {"PreconditionFailed", "412"}:
                return self._handle_existing_batch(batch, body, key, started_at)
            if _client_error_code(exc) in {"ConditionalRequestConflict", "409"}:
                msg = f"S3 conditional write conflict for WAL batch object {key}"
                raise S3ExpressWriteError(msg) from exc
            msg = f"S3 Express WAL batch PUT failed for object {key}: {_safe_error_message(exc)}"
            raise S3ExpressWriteError(msg) from exc
        finished_at = time.perf_counter_ns()
        assert batch.batch_checksum is not None
        return PersistedBatchReceipt(
            bucket=self.config.bucket,
            key=key,
            etag=_response_text(response, "ETag"),
            version_id=_response_text(response, "VersionId"),
            batch_id=batch.batch_id,
            batch_checksum=batch.batch_checksum,
            record_count=batch.record_count,
            bytes_written=len(body),
            duration_ns=finished_at - started_at,
            remote_durable=True,
        )

    def fetch_batch(self, key: str) -> WalBatch:
        return decode_batch(self.fetch_batch_bytes(key))

    def fetch_batch_bytes(self, key: str) -> bytes:
        try:
            response = self._request("get_object", Bucket=self.config.bucket, Key=key)
            body = response["Body"].read()
        except Exception as exc:
            msg = f"S3 Express WAL batch GET failed for object {key}: {_safe_error_message(exc)}"
            raise S3ExpressReadError(msg) from exc
        if not isinstance(body, bytes):
            msg = f"S3 Express WAL batch GET returned non-bytes body for object {key}"
            raise S3ExpressReadError(msg)
        return body

    def delete_batch(self, key: str) -> None:
        self._request("delete_object", Bucket=self.config.bucket, Key=key)

    def _handle_existing_batch(
        self,
        batch: WalBatch,
        expected_body: bytes,
        key: str,
        started_at: int,
    ) -> PersistedBatchReceipt:
        existing_body = self.fetch_batch_bytes(key)
        finished_at = time.perf_counter_ns()
        if existing_body != expected_body:
            msg = f"S3 WAL batch object already exists with different content: {key}"
            raise RemoteWalConflictError(msg)
        assert batch.batch_checksum is not None
        return PersistedBatchReceipt(
            bucket=self.config.bucket,
            key=key,
            etag=None,
            version_id=None,
            batch_id=batch.batch_id,
            batch_checksum=batch.batch_checksum,
            record_count=batch.record_count,
            bytes_written=len(expected_body),
            duration_ns=finished_at - started_at,
            remote_durable=True,
            already_exists=True,
        )


def _build_s3_client(config: S3ExpressWalConfig) -> Any:
    try:
        boto3 = importlib.import_module("boto3")
        botocore_config = importlib.import_module("botocore.config")
    except ModuleNotFoundError as exc:
        msg = "boto3 and botocore are required for WAL_STORE=s3-express"
        raise S3ExpressConfigurationError(msg) from exc
    sdk_config = botocore_config.Config(
        connect_timeout=config.connect_timeout_seconds,
        read_timeout=config.read_timeout_seconds,
        retries={"mode": config.retry_mode, "max_attempts": config.max_attempts},
    )
    session_kwargs: dict[str, str] = {"region_name": config.region}
    if config.profile is not None:
        session_kwargs["profile_name"] = config.profile
    session = boto3.Session(**session_kwargs)
    return session.client("s3", config=sdk_config, region_name=config.region)


def _record_metadata(record: WalRecord) -> dict[str, str]:
    assert record.record_checksum is not None
    return {
        "tenant-id": record.tenant_id,
        "transaction-id": record.transaction_id,
        "sequence-no": str(record.sequence_no),
        "record-checksum": record.record_checksum,
        "wal-format-version": str(record.wal_format_version),
        "owner-epoch": str(record.owner_epoch),
    }


def _batch_metadata(batch: WalBatch) -> dict[str, str]:
    assert batch.batch_checksum is not None
    return {
        "batch-id": batch.batch_id,
        "batch-checksum": batch.batch_checksum,
        "record-count": str(batch.record_count),
        "batch-format-version": str(WAL_BATCH_FORMAT_VERSION),
    }


def _sequence_from_key(key: str) -> int:
    filename = key.rsplit("/", maxsplit=1)[-1]
    if not filename.endswith(".json"):
        msg = f"unexpected WAL object key: {key}"
        raise S3ExpressReadError(msg)
    return int(filename.removesuffix(".json"))


def _client_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    error_payload = response.get("Error")
    if not isinstance(error_payload, dict):
        return None
    code = error_payload.get("Code")
    return None if code is None else str(code)


def _response_text(response: object, key: str) -> str | None:
    if not isinstance(response, dict):
        return None
    value = response.get(key)
    if value is None:
        return None
    return str(value).strip('"')


def _safe_error_message(error: Exception) -> str:
    code = _client_error_code(error)
    if code is not None:
        return code
    return type(error).__name__
