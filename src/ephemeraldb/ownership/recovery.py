"""Bounded unsealed recovery from the accepted descriptor chain."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ephemeraldb.core.errors import RecoveryError
from ephemeraldb.wal.batching.codec import validate_batch
from ephemeraldb.wal.batching.models import WalBatch
from ephemeraldb.wal.codec import decode_record, encode_record
from ephemeraldb.wal.durability import PersistedBatchReceipt, PersistedWalReceipt
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.replay import replay_wal
from ephemeraldb.wal.sequence import current_sequence_state
from ephemeraldb.wal.validation import validate_record

from .coordinator import OwnershipCoordinator
from .models import CommitDescriptor, OwnerToken
from .store import AuthorityStore

MAX_RECOVERY_WAL_BYTES = 256 * 1024 * 1024


class AcceptedPayloadReader(Protocol):
    def read(self, descriptor: CommitDescriptor) -> WalRecord: ...


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    tenant_id: str
    epoch: int
    committed_sequence: int
    record_head: str | None
    descriptors_read: int
    wal_bytes_read: int
    destination: Path


class AuthorityCandidateWalStore:
    """Offline candidate payload store with collision-safe immutable keys."""

    def __init__(self, store: AuthorityStore, namespace: str) -> None:
        self.store = store
        self.namespace = namespace
        self.namespace_id = f"authority-candidates:{namespace}"

    def persist_record(self, record: WalRecord) -> PersistedWalReceipt:
        validate_record(record, expected_tenant_id=record.tenant_id)
        assert record.record_checksum is not None
        raw = encode_record(record)
        key = (
            f"fenced/{self.namespace}/candidates/{record.tenant_id}/"
            f"{record.sequence_no:020d}-{record.owner_epoch}-{record.record_checksum}.json"
        )
        self.store.put_immutable(key, raw)
        return PersistedWalReceipt(
            bucket="offline-authority",
            key=key,
            etag=None,
            version_id=None,
            checksum=record.record_checksum,
            bytes_written=len(raw),
            duration_ns=0,
            remote_durable=True,
        )

    def fetch_record(self, tenant_id: str, sequence_no: int) -> WalRecord:
        raise RecoveryError("candidate records require an accepted descriptor locator")

    def close(self) -> None:
        return


class AuthorityCandidateBatchStore:
    """Offline shared-batch payload store using immutable authority objects."""

    def __init__(self, store: AuthorityStore, namespace: str) -> None:
        self.store = store
        self.namespace = namespace

    def persist_batch(self, batch: WalBatch) -> PersistedBatchReceipt:
        from ephemeraldb.wal.batching.codec import encode_batch
        validate_batch(batch)
        assert batch.batch_checksum is not None
        raw = encode_batch(batch)
        key = f"fenced/{self.namespace}/batch-candidates/{batch.batch_id}.json"
        self.store.put_immutable(key, raw)
        return PersistedBatchReceipt(
            bucket="offline-authority",
            key=key,
            etag=None,
            version_id=None,
            batch_id=batch.batch_id,
            batch_checksum=batch.batch_checksum,
            record_count=batch.record_count,
            bytes_written=len(raw),
            duration_ns=0,
            remote_durable=True,
        )

    def fetch_batch(self, key: str) -> WalBatch:
        from ephemeraldb.wal.batching.codec import decode_batch

        return decode_batch(self.store.get_immutable(key))

    def close(self) -> None:
        return


class AuthorityPayloadReader:
    def __init__(self, store: AuthorityStore) -> None:
        self.store = store

    def read(self, descriptor: CommitDescriptor) -> WalRecord:
        if descriptor.locator.batch_id is not None:
            from ephemeraldb.wal.batching.codec import decode_batch

            batch = decode_batch(self.store.get_immutable(descriptor.locator.key))
            validate_batch(batch)
            if batch.batch_checksum != descriptor.locator.batch_checksum:
                raise RecoveryError("accepted batch checksum differs from descriptor")
            assert descriptor.locator.record_index is not None
            assert descriptor.locator.record_count is not None
            if batch.record_count != descriptor.locator.record_count:
                raise RecoveryError("accepted batch count differs from descriptor")
            record = batch.records[descriptor.locator.record_index]
        else:
            record = decode_record(self.store.get_immutable(descriptor.locator.key))
        validate_record(record, expected_tenant_id=descriptor.tenant_id)
        if (
            record.record_checksum != descriptor.record_checksum
            or descriptor.locator.payload_checksum != descriptor.record_checksum
        ):
            raise RecoveryError("accepted payload checksum differs from descriptor")
        return record


class FencedRecovery:
    def __init__(
        self,
        coordinator: OwnershipCoordinator,
        payload_reader: AcceptedPayloadReader,
    ) -> None:
        self.coordinator = coordinator
        self.payload_reader = payload_reader

    def recover(
        self,
        token: OwnerToken,
        destination: Path,
        *,
        install_bootstrap: Callable[[str, Path], None],
        before_activation: Callable[[], None] | None = None,
    ) -> RecoveryResult:
        state, _ = self.coordinator.status(token.tenant_id)
        if state.token != token:
            raise RecoveryError("recovery token is not current owner")
        history = self.coordinator.accepted_history(state)
        if state.checkpoint_sequence:
            anchor = history[state.checkpoint_sequence - 1]
            expected_anchor = (
                self.coordinator._descriptor_key(anchor),
                anchor.checksum,
            )
            if expected_anchor != (
                state.checkpoint_descriptor_key,
                state.checkpoint_descriptor_checksum,
            ):
                raise RecoveryError("checkpoint is not anchored to accepted history")
        destination = Path(destination)
        _clean_fenced_staging(destination, token.epoch)
        staging = destination.with_name(
            f".{destination.name}.phase8-e{token.epoch}-{token.session_id}-"
            f"{uuid.uuid4().hex}.tmp"
        )
        if staging.exists():
            raise RecoveryError("unique recovery staging path already exists")
        wal_bytes = 0
        try:
            install_bootstrap(state.bootstrap_key, staging)
            if not staging.is_file():
                raise RecoveryError("bootstrap loader did not install a database file")
            if _file_hash(staging) != state.bootstrap_checksum:
                raise RecoveryError("bootstrap checksum mismatch")
            connection = sqlite3.connect(staging)
            connection.row_factory = sqlite3.Row
            try:
                for descriptor in history:
                    if descriptor.sequence_no <= state.checkpoint_sequence:
                        continue
                    record = self.payload_reader.read(descriptor)
                    encoded_size = len(encode_record(record))
                    wal_bytes += encoded_size
                    if wal_bytes > MAX_RECOVERY_WAL_BYTES:
                        raise RecoveryError("recovery WAL byte budget exceeded")
                    replay_wal(
                        connection,
                        (record,),
                        tenant_id=token.tenant_id,
                        remote_durable=True,
                        object_keys={record.sequence_no: descriptor.locator.key},
                    )
                if current_sequence_state(connection) != (
                    state.committed_sequence,
                    state.record_head,
                ):
                    raise RecoveryError("recovered SQLite head differs from authority")
            finally:
                connection.close()
            if before_activation is not None:
                before_activation()
            latest, _ = self.coordinator.status(token.tenant_id)
            if latest.token != token or latest.revision != state.revision:
                raise RecoveryError("ownership or accepted head changed before activation")
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, destination)
            return RecoveryResult(
                tenant_id=token.tenant_id,
                epoch=token.epoch,
                committed_sequence=state.committed_sequence,
                record_head=state.record_head,
                descriptors_read=len(history),
                wal_bytes_read=wal_bytes,
                destination=destination,
            )
        except Exception:
            if staging.exists():
                if staging.is_dir():
                    shutil.rmtree(staging)
                else:
                    staging.unlink()
            raise


def copy_bootstrap(source_root: Path) -> Callable[[str, Path], None]:
    root = Path(source_root)

    def install(key: str, target: Path) -> None:
        source = root / key
        if not source.is_file():
            raise RecoveryError(f"bootstrap is unavailable: {key}")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)

    return install


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _clean_fenced_staging(destination: Path, current_epoch: int) -> None:
    prefix = f".{destination.name}.phase8-e"
    pattern = re.compile(
        rf"^{re.escape(prefix)}(?P<epoch>[0-9]+)-[A-Za-z0-9_.-]+-[0-9a-f]{{32}}\.tmp$"
    )
    for candidate in destination.parent.glob(prefix + "*.tmp"):
        match = pattern.fullmatch(candidate.name)
        if match is None or int(match.group("epoch")) >= current_epoch:
            continue
        if candidate.is_file():
            candidate.unlink()
