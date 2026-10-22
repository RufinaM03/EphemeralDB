"""Fail-closed admission and explicit reconciliation under the tenant lock."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

from ephemeraldb.core.errors import (
    PersistenceRejectedError,
    RemoteDurabilityError,
    WalError,
    WalReplayError,
)
from ephemeraldb.wal.codec import decode_record, encode_record
from ephemeraldb.wal.durability import (
    ConfirmedDurabilityError,
    StoredWalRecord,
    WalCommitReceipt,
    WalCommitter,
)
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.replay import replay_wal
from ephemeraldb.wal.sequence import current_sequence_state
from ephemeraldb.wal.validation import validate_record


class TenantNotReadyError(RemoteDurabilityError):
    pass


class DurabilityGuard:
    def __init__(self, root: Path, committer: WalCommitter, *, initialize: bool = False) -> None:
        self.committer = committer
        self.store: Any = getattr(committer, "store", committer)
        self.initialize = initialize
        self.namespace = str(getattr(self.store, "namespace_id", type(self.store).__qualname__))
        self._checked: set[str] = set()
        self._failed: set[str] = set()
        self._lock = threading.RLock()
        self.path = root / ".ephemeraldb-attempts.sqlite"
        with self._journal() as journal:
            if journal.execute("PRAGMA user_version").fetchone()[0] not in {0, 1}:
                raise TenantNotReadyError("unsupported attempt journal schema")
            journal.execute("""CREATE TABLE IF NOT EXISTS attempts (
                tenant TEXT PRIMARY KEY, namespace TEXT NOT NULL, state TEXT NOT NULL,
                record BLOB, object_key TEXT, is_batch INTEGER, remote INTEGER)""")
            journal.execute("PRAGMA user_version=1")

    @contextmanager
    def _journal(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=10)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            with connection:
                yield connection
        finally:
            connection.close()

    def _row(self, tenant: str) -> tuple[object, ...] | None:
        with self._lock, self._journal() as journal:
            row = journal.execute(
                "SELECT namespace,state,record,object_key,is_batch,remote "
                "FROM attempts WHERE tenant=?",
                (tenant,),
            ).fetchone()
            return None if row is None else tuple(row)

    def state(self, tenant: str) -> str:
        if tenant in self._failed:
            return "FAILED"
        row = self._row(tenant)
        return "READY" if row is not None and row[1] == "READY" else "RECONCILING"

    def require_ready(self, tenant: str, connection: sqlite3.Connection) -> None:
        try:
            self._require_ready(tenant, connection)
        except TenantNotReadyError:
            raise
        except Exception as exc:
            self._failed.add(tenant)
            raise TenantNotReadyError(f"startup verification failed: {exc}") from exc

    def _require_ready(self, tenant: str, connection: sqlite3.Connection) -> None:
        if tenant not in self._checked:
            row = self._row(tenant)
            sequence, _ = current_sequence_state(connection)
            if row is None:
                requires_bootstrap = bool(getattr(self.store, "requires_bootstrap", False))
                if sequence != 0 or (requires_bootstrap and not self.initialize):
                    raise TenantNotReadyError(
                        "missing attempt journal; explicit fresh namespace "
                        "bootstrap is required; cache-loss recovery unavailable"
                    )
                history = self._history(tenant)
                if history:
                    raise TenantNotReadyError(
                        "fresh bootstrap requires empty remote tenant history"
                    )
                with self._lock, self._journal() as journal:
                    journal.execute(
                        "INSERT INTO attempts VALUES (?,?,'READY',NULL,NULL,0,0)",
                        (tenant, self.namespace),
                    )
            else:
                if row[0] != self.namespace:
                    raise TenantNotReadyError("durability namespace differs from attempt journal")
                if row[1] != "READY":
                    raise TenantNotReadyError(
                        "unresolved attempt; call reconcile_tenant explicitly"
                    )
                history = self._history(tenant)
                if history is not None:
                    self._validate_history(tenant, connection, history, allow_suffix=False)
                elif row[2] is not None:
                    stored = self._fetch_known(tenant, row)
                    if (stored.record.sequence_no, stored.record.record_checksum) != (
                        current_sequence_state(connection)
                    ):
                        raise TenantNotReadyError("remote/local head mismatch")
            self._checked.add(tenant)
        if self.state(tenant) != "READY":
            raise TenantNotReadyError("tenant is not READY; explicit reconciliation required")

    def commit(self, record: WalRecord) -> WalCommitReceipt:
        previous = self._row(record.tenant_id)
        with self._lock, self._journal() as journal:
            journal.execute(
                "UPDATE attempts SET state='PENDING',record=?,object_key=NULL,"
                "is_batch=0,remote=0 WHERE tenant=?",
                (encode_record(record), record.tenant_id),
            )
        confirmed_error: ConfirmedDurabilityError | None = None
        try:
            receipt = self.committer.commit(record)
        except PersistenceRejectedError:
            assert previous is not None
            with self._lock, self._journal() as journal:
                journal.execute(
                    "UPDATE attempts SET namespace=?,state=?,record=?,object_key=?,"
                    "is_batch=?,remote=? WHERE tenant=?",
                    (*previous, record.tenant_id),
                )
            raise
        except ConfirmedDurabilityError as exc:
            receipt = exc.receipt
            confirmed_error = exc
        try:
            with self._lock, self._journal() as journal:
                journal.execute(
                    "UPDATE attempts SET object_key=?,is_batch=?,remote=? WHERE tenant=?",
                    (
                        receipt.key,
                        int(receipt.wal_batch_id is not None),
                        int(receipt.remote_durable),
                        record.tenant_id,
                    ),
                )
        except Exception as exc:
            raise ConfirmedDurabilityError(receipt) from exc
        if confirmed_error is not None:
            raise confirmed_error
        return receipt

    def applied(self, tenant: str) -> None:
        with self._lock, self._journal() as journal:
            journal.execute("UPDATE attempts SET state='READY' WHERE tenant=?", (tenant,))

    def seal(self, tenant: str) -> None:
        with self._lock, self._journal() as journal:
            journal.execute("UPDATE attempts SET state='SEALED' WHERE tenant=?", (tenant,))

    def _history(self, tenant: str) -> tuple[StoredWalRecord, ...] | None:
        inspect = getattr(self.store, "inspect_history", None)
        if not callable(inspect):
            return None
        return cast(tuple[StoredWalRecord, ...], inspect(tenant))

    def _fetch_known(self, tenant: str, row: tuple[object, ...]) -> StoredWalRecord:
        if row[3] is None or row[2] is None:
            raise TenantNotReadyError("attempt identity cannot be verified; remain blocked")
        record = decode_record(cast(bytes, row[2]))
        if row[4]:
            fetch = self.store.fetch_batch
            batch = fetch(str(row[3]))
            from ephemeraldb.wal.batching.codec import validate_batch

            validate_batch(batch)
            records = [
                r
                for r in batch.records
                if r.tenant_id == tenant and r.sequence_no == record.sequence_no
            ]
            if len(records) != 1:
                raise WalReplayError("known batch lacks unique attempted record")
            actual = records[0]
        else:
            actual = self.store.fetch_record(tenant, record.sequence_no)
        validate_record(actual, expected_tenant_id=tenant)
        if encode_record(actual) != encode_record(record):
            raise WalReplayError("remote attempt content differs")
        return StoredWalRecord(actual, str(row[3]), bool(row[5]))

    def _validate_history(
        self,
        tenant: str,
        connection: sqlite3.Connection,
        history: tuple[StoredWalRecord, ...],
        *,
        allow_suffix: bool,
    ) -> tuple[StoredWalRecord, ...]:
        by_sequence: dict[int, StoredWalRecord] = {}
        transactions: set[str] = set()
        for item in history:
            validate_record(item.record, expected_tenant_id=tenant)
            previous = by_sequence.get(item.record.sequence_no)
            if previous is not None and encode_record(previous.record) != encode_record(
                item.record
            ):
                raise WalReplayError("conflicting remote records at one sequence")
            by_sequence[item.record.sequence_no] = item
        ordered = tuple(by_sequence[n] for n in sorted(by_sequence))
        head: str | None = None
        for expected, item in enumerate(ordered, 1):
            record = item.record
            if record.sequence_no != expected or record.previous_record_checksum != head:
                raise WalReplayError("remote history gap or checksum-chain conflict")
            if record.transaction_id in transactions:
                raise WalReplayError("transaction appears at multiple sequences")
            transactions.add(record.transaction_id)
            head = record.record_checksum
        sequence, checksum = current_sequence_state(connection)
        if sequence > len(ordered) or (
            sequence and ordered[sequence - 1].record.record_checksum != checksum
        ):
            raise WalReplayError("local state not anchored in complete remote history")
        if not allow_suffix and sequence != len(ordered):
            raise TenantNotReadyError("remote history is ahead; explicit reconciliation required")
        return ordered

    def reconcile(self, tenant: str, connection: sqlite3.Connection) -> None:
        row = self._row(tenant)
        if row is None or row[0] != self.namespace:
            raise TenantNotReadyError("no matching journal; full recovery is outside Phase 6")
        if row[1] == "SEALED":
            raise TenantNotReadyError("sealed runtime cannot resume; restore into a new directory")
        try:
            history = self._history(tenant)
            if history is None:
                history = (self._fetch_known(tenant, row),) if row[2] is not None else ()
            else:
                history = self._validate_history(tenant, connection, history, allow_suffix=True)
            if row[2] is not None:
                attempted = decode_record(cast(bytes, row[2]))
                if any(
                    item.record.sequence_no == attempted.sequence_no
                    and encode_record(item.record) != encode_record(attempted)
                    for item in history
                ):
                    raise WalReplayError("remote sequence conflicts with the recorded attempt")
                if not any(
                    encode_record(item.record) == encode_record(attempted) for item in history
                ):
                    raise TenantNotReadyError(
                        "attempt absent/unconfirmed; absence cannot clear uncertainty"
                    )
            for item in history:
                replay_wal(
                    connection,
                    (item.record,),
                    tenant_id=tenant,
                    remote_durable=item.remote_durable,
                    object_keys={item.record.sequence_no: item.key},
                )
            self.applied(tenant)
            self._failed.discard(tenant)
            self._checked.add(tenant)
        except (WalError, ValueError):
            self._failed.add(tenant)
            raise
