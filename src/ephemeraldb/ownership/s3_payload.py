"""Collision-safe S3 Express candidates and descriptor-directed reads."""

from __future__ import annotations

import time

from ephemeraldb.aws.s3_express import (
    S3ExpressWalStore,
    _client_error_code,
    _record_metadata,
    _response_text,
    _safe_error_message,
)
from ephemeraldb.core.errors import RecoveryError, RemoteWalConflictError, S3ExpressWriteError
from ephemeraldb.wal.batching.codec import validate_batch
from ephemeraldb.wal.codec import decode_record, encode_record
from ephemeraldb.wal.durability import PersistedWalReceipt
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.validation import validate_record
from ephemeraldb.wal.writer import safe_tenant_dir_name

from .models import CommitDescriptor


class FencedS3ExpressWalStore(S3ExpressWalStore):
    """S3 Express store whose single-record keys cannot collide across candidates."""

    def candidate_key(self, record: WalRecord) -> str:
        assert record.record_checksum is not None
        tenant = safe_tenant_dir_name(record.tenant_id)
        return (
            f"{self.config.prefix}/fenced-candidates/{tenant}/"
            f"{record.sequence_no:020d}-{record.owner_epoch}-{record.record_checksum}.json"
        )

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        validate_record(record, expected_tenant_id=record.tenant_id)
        body = encode_record(record)
        key = self.candidate_key(record)
        started_at = time.perf_counter_ns()
        try:
            response = self._request(
                "put_object",
                Bucket=self.config.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                StorageClass="EXPRESS_ONEZONE",
                Metadata=_record_metadata(record),
                IfNoneMatch="*",
            )
        except Exception as exc:
            if _client_error_code(exc) in {"PreconditionFailed", "412"}:
                existing = self.fetch_record_by_key(key)
                if encode_record(existing) != body:
                    raise RemoteWalConflictError(
                        f"fenced candidate key contains different bytes: {key}"
                    ) from exc
                response = {}
            else:
                raise S3ExpressWriteError(
                    f"S3 Express fenced candidate PUT failed: {_safe_error_message(exc)}"
                ) from exc
        assert record.record_checksum is not None
        return PersistedWalReceipt(
            bucket=self.config.bucket,
            key=key,
            etag=_response_text(response, "ETag"),
            version_id=_response_text(response, "VersionId"),
            checksum=record.record_checksum,
            bytes_written=len(body),
            duration_ns=time.perf_counter_ns() - started_at,
            remote_durable=True,
            already_exists=not response,
        )

    def fetch_record_by_key(self, key: str) -> WalRecord:
        prefix = f"{self.config.prefix}/fenced-candidates/"
        if not key.startswith(prefix) or ".." in key.split("/"):
            raise RecoveryError("descriptor references an out-of-prefix WAL candidate")
        try:
            response = self._request("get_object", Bucket=self.config.bucket, Key=key)
            stream = response["Body"]
            try:
                body = stream.read(64 * 1024 * 1024 + 1)
            finally:
                stream.close()
        except Exception as exc:
            raise RecoveryError(f"required S3 Express candidate unavailable: {key}") from exc
        if not isinstance(body, bytes) or len(body) > 64 * 1024 * 1024:
            raise RecoveryError("S3 Express candidate violates recovery byte budget")
        record = decode_record(body)
        validate_record(record, expected_tenant_id=record.tenant_id)
        return record


class S3ExpressAcceptedPayloadReader:
    def __init__(self, store: FencedS3ExpressWalStore) -> None:
        self.store = store

    def read(self, descriptor: CommitDescriptor) -> WalRecord:
        locator = descriptor.locator
        if locator.batch_id is None:
            record = self.store.fetch_record_by_key(locator.key)
        else:
            batch = self.store.fetch_batch(locator.key)
            validate_batch(batch)
            if batch.batch_checksum != locator.batch_checksum:
                raise RecoveryError("accepted S3 Express batch checksum mismatch")
            if locator.record_index is None or locator.record_count != batch.record_count:
                raise RecoveryError("accepted S3 Express batch locator mismatch")
            record = batch.records[locator.record_index]
        validate_record(record, expected_tenant_id=descriptor.tenant_id)
        if record.record_checksum != descriptor.record_checksum:
            raise RecoveryError("accepted S3 Express record checksum mismatch")
        return record
