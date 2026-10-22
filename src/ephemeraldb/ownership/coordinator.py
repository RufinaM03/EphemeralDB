"""Fenced commit coordinator: the control CAS is the commit boundary."""

from __future__ import annotations

import hashlib
import sqlite3
import uuid
from dataclasses import dataclass, replace

from ephemeraldb.core.errors import OwnershipError, PersistenceRejectedError, StaleOwnerError
from ephemeraldb.wal.durability import WalCommitReceipt, WalCommitter
from ephemeraldb.wal.models import WalRecord
from ephemeraldb.wal.sequence import current_sequence_state
from ephemeraldb.wal.validation import validate_record

from .models import (
    AUTHORITY_FORMAT,
    CommitDecision,
    CommitDescriptor,
    ControlState,
    OwnerToken,
    PayloadLocator,
)
from .store import AuthorityStore, CasResult, ControlSnapshot

MAX_DESCRIPTOR_TRAVERSAL = 100_000


@dataclass(frozen=True, slots=True)
class CommitOutcome:
    decision: CommitDecision
    descriptor_key: str
    descriptor_checksum: str
    revision: int | None


def new_session_id(runtime_name: str) -> str:
    safe = "".join(
        character
        for character in runtime_name
        if character.isalnum() or character in "-_."
    )
    return f"{safe[:40] or 'runtime'}-{uuid.uuid4().hex}"


class OwnershipCoordinator:
    def __init__(self, store: AuthorityStore, namespace: str) -> None:
        if not namespace or "/" in namespace or ".." in namespace:
            raise ValueError("namespace must be one safe path segment")
        self.store = store
        self.namespace = namespace

    def _control_key(self, tenant_id: str) -> str:
        _safe_identity(tenant_id)
        return f"fenced/{self.namespace}/tenants/{tenant_id}/control.json"

    def bootstrap(
        self,
        tenant_id: str,
        *,
        session_id: str,
        bootstrap_key: str,
        bootstrap_checksum: str,
        transition_id: str | None = None,
    ) -> ControlState:
        _safe_identity(session_id)
        state = ControlState(
            format=AUTHORITY_FORMAT,
            namespace=self.namespace,
            tenant_id=tenant_id,
            revision=1,
            owner_session=session_id,
            owner_epoch=1,
            committed_sequence=0,
            record_head=None,
            descriptor_head_key=None,
            descriptor_head_checksum=None,
            bootstrap_key=bootstrap_key,
            bootstrap_checksum=bootstrap_checksum,
            checkpoint_sequence=0,
            checkpoint_descriptor_key=None,
            checkpoint_descriptor_checksum=None,
            last_transition_id=transition_id or f"bootstrap-{uuid.uuid4().hex}",
        )
        state.validate()
        result = self.store.create_control(self._control_key(tenant_id), state.encode())
        if result is not CasResult.APPLIED:
            try:
                observed, _ = self.status(tenant_id)
            except OwnershipError as exc:
                raise OwnershipError("fresh bootstrap outcome remains unknown") from exc
            if observed == state:
                return observed
            raise OwnershipError("fresh bootstrap rejected: authority already exists")
        return state

    def status(self, tenant_id: str) -> tuple[ControlState, ControlSnapshot]:
        snapshot = self.store.get_control(self._control_key(tenant_id))
        state = ControlState.decode(snapshot.payload)
        self._validate_identity(state, tenant_id)
        return state, snapshot

    def takeover(
        self,
        tenant_id: str,
        *,
        session_id: str,
        transition_id: str | None = None,
        max_conflicts: int = 8,
    ) -> ControlState:
        _safe_identity(session_id)
        operation = transition_id or f"takeover-{uuid.uuid4().hex}"
        for _ in range(max_conflicts + 1):
            current, snapshot = self.status(tenant_id)
            if current.owner_session == session_id and current.last_transition_id == operation:
                return current
            replacement = replace(
                current,
                revision=current.revision + 1,
                owner_session=session_id,
                owner_epoch=current.owner_epoch + 1,
                last_transition_id=operation,
            )
            result = self.store.compare_and_swap(
                self._control_key(tenant_id), snapshot.validator, replacement.encode()
            )
            if result is CasResult.APPLIED:
                return replacement
            if result is CasResult.UNKNOWN:
                observed, _ = self.status(tenant_id)
                if (
                    observed.owner_session == session_id
                    and observed.last_transition_id == operation
                ):
                    return observed
                if observed.revision > current.revision:
                    raise OwnershipError("takeover outcome lost and a later transition won")
                raise OwnershipError("takeover decision remains unknown")
        raise OwnershipError("takeover conflict retry budget exceeded")

    def register_checkpoint(
        self,
        token: OwnerToken,
        *,
        checkpoint_key: str,
        checkpoint_checksum: str,
        checkpoint_sequence: int,
        descriptor_key: str | None,
        descriptor_checksum: str | None,
        transition_id: str | None = None,
        max_conflicts: int = 8,
    ) -> ControlState:
        operation = transition_id or f"checkpoint-{uuid.uuid4().hex}"
        for _ in range(max_conflicts + 1):
            current, snapshot = self.status(token.tenant_id)
            if current.token != token:
                raise StaleOwnerError("stale owner cannot register checkpoint")
            if current.last_transition_id == operation:
                return current
            history = self.accepted_history(current)
            if checkpoint_sequence == 0:
                if descriptor_key is not None or descriptor_checksum is not None:
                    raise OwnershipError("genesis checkpoint cannot name a descriptor")
            else:
                if checkpoint_sequence > len(history):
                    raise OwnershipError("checkpoint is ahead of accepted history")
                anchor = history[checkpoint_sequence - 1]
                if (
                    self._descriptor_key(anchor) != descriptor_key
                    or anchor.checksum != descriptor_checksum
                ):
                    raise OwnershipError("checkpoint is not anchored to accepted ancestry")
            replacement = replace(
                current,
                revision=current.revision + 1,
                bootstrap_key=checkpoint_key,
                bootstrap_checksum=checkpoint_checksum,
                checkpoint_sequence=checkpoint_sequence,
                checkpoint_descriptor_key=descriptor_key,
                checkpoint_descriptor_checksum=descriptor_checksum,
                last_transition_id=operation,
            )
            result = self.store.compare_and_swap(
                self._control_key(token.tenant_id), snapshot.validator, replacement.encode()
            )
            if result is CasResult.APPLIED:
                return replacement
            if result is CasResult.UNKNOWN:
                observed, _ = self.status(token.tenant_id)
                if observed.last_transition_id == operation and observed.token == token:
                    return observed
        raise OwnershipError("checkpoint registration conflict budget exceeded")

    def commit(
        self,
        token: OwnerToken,
        record: WalRecord,
        receipt: WalCommitReceipt,
        *,
        attempt_id: str,
        observed: tuple[ControlState, ControlSnapshot] | None = None,
    ) -> CommitOutcome:
        validate_record(record, expected_tenant_id=token.tenant_id)
        _safe_identity(attempt_id)
        if record.owner_epoch != token.epoch:
            raise StaleOwnerError("WAL epoch does not match owner token")
        current, snapshot = observed or self.status(token.tenant_id)
        found = self.find_attempt(current, attempt_id)
        if found is not None:
            if found.record_checksum != record.record_checksum:
                raise OwnershipError("attempt ID is already accepted with different content")
            return CommitOutcome(
                CommitDecision.ALREADY_ACCEPTED,
                self._descriptor_key(found),
                found.checksum,
                current.revision,
            )
        if current.token != token:
            raise StaleOwnerError("owner token was fenced before commit acceptance")
        assert record.record_checksum is not None
        assert record.request_fingerprint is not None
        if record.sequence_no != current.committed_sequence + 1:
            raise PersistenceRejectedError("record is not the next authoritative sequence")
        if record.previous_record_checksum != current.record_head:
            raise PersistenceRejectedError("record does not extend authoritative WAL head")
        locator = PayloadLocator(
            bucket=receipt.bucket,
            key=receipt.key,
            payload_checksum=receipt.checksum,
            batch_id=receipt.wal_batch_id,
            batch_checksum=receipt.wal_batch_checksum,
            record_index=receipt.wal_batch_record_index,
            record_count=receipt.wal_batch_record_count,
        )
        descriptor = CommitDescriptor(
            format=AUTHORITY_FORMAT,
            namespace=self.namespace,
            tenant_id=record.tenant_id,
            sequence_no=record.sequence_no,
            owner_session=token.session_id,
            owner_epoch=token.epoch,
            attempt_id=attempt_id,
            transaction_id=record.transaction_id,
            request_fingerprint=record.request_fingerprint,
            record_checksum=record.record_checksum,
            previous_record_checksum=record.previous_record_checksum,
            previous_descriptor_key=current.descriptor_head_key,
            previous_descriptor_checksum=current.descriptor_head_checksum,
            locator=locator,
        )
        descriptor.validate()
        key = self._descriptor_key(descriptor)
        self.store.put_immutable(key, descriptor.encode())
        replacement = replace(
            current,
            revision=current.revision + 1,
            committed_sequence=record.sequence_no,
            record_head=record.record_checksum,
            descriptor_head_key=key,
            descriptor_head_checksum=descriptor.checksum,
            last_transition_id=attempt_id,
        )
        result = self.store.compare_and_swap(
            self._control_key(record.tenant_id), snapshot.validator, replacement.encode()
        )
        if result is CasResult.APPLIED:
            return CommitOutcome(
                CommitDecision.ACCEPTED,
                key,
                descriptor.checksum,
                replacement.revision,
            )
        observed_state, _ = self.status(record.tenant_id)
        accepted = self.find_attempt(observed_state, attempt_id)
        if accepted is not None:
            if accepted.record_checksum != record.record_checksum:
                raise OwnershipError("accepted attempt content differs from candidate")
            return CommitOutcome(
                CommitDecision.ALREADY_ACCEPTED,
                self._descriptor_key(accepted),
                accepted.checksum,
                observed_state.revision,
            )
        if observed_state.revision > current.revision or result is CasResult.CONFLICT:
            if observed_state.token != token:
                raise StaleOwnerError("takeover won before commit acceptance")
            raise PersistenceRejectedError("another commit won the expected authority revision")
        raise OwnershipError("commit decision remains unknown at the expected revision")

    def accepted_history(self, state: ControlState) -> tuple[CommitDescriptor, ...]:
        self._validate_identity(state, state.tenant_id)
        history: list[CommitDescriptor] = []
        key = state.descriptor_head_key
        checksum = state.descriptor_head_checksum
        expected_sequence = state.committed_sequence
        expected_record = state.record_head
        seen: set[str] = set()
        while key is not None:
            if len(history) >= MAX_DESCRIPTOR_TRAVERSAL:
                raise OwnershipError("descriptor traversal budget exceeded")
            if key in seen:
                raise OwnershipError("descriptor chain contains a cycle")
            seen.add(key)
            raw = self.store.get_immutable(key)
            descriptor = CommitDescriptor.decode(raw)
            if descriptor.checksum != checksum:
                raise OwnershipError("descriptor checksum mismatch")
            if descriptor.namespace != self.namespace or descriptor.tenant_id != state.tenant_id:
                raise OwnershipError("descriptor crosses authority identity")
            if descriptor.sequence_no != expected_sequence:
                raise OwnershipError("descriptor sequence gap")
            if descriptor.record_checksum != expected_record:
                raise OwnershipError("descriptor/WAL head mismatch")
            history.append(descriptor)
            key = descriptor.previous_descriptor_key
            checksum = descriptor.previous_descriptor_checksum
            expected_record = descriptor.previous_record_checksum
            expected_sequence -= 1
        if expected_sequence != 0 or expected_record is not None or checksum is not None:
            raise OwnershipError("descriptor chain does not terminate at genesis")
        history.reverse()
        return tuple(history)

    def find_attempt(
        self, state: ControlState, attempt_id: str
    ) -> CommitDescriptor | None:
        for descriptor in reversed(self.accepted_history(state)):
            if descriptor.attempt_id == attempt_id:
                return descriptor
        return None

    def _descriptor_key(self, descriptor: CommitDescriptor) -> str:
        return (
            f"fenced/{self.namespace}/tenants/{descriptor.tenant_id}/descriptors/"
            f"{descriptor.sequence_no:020d}-{descriptor.attempt_id}.json"
        )

    def _validate_identity(self, state: ControlState, tenant_id: str) -> None:
        state.validate()
        if state.namespace != self.namespace or state.tenant_id != tenant_id:
            raise OwnershipError("control object identity mismatch")


class FencedCommitter:
    """Ordinary engine committer whose success includes authoritative acceptance."""

    is_fenced_committer = True

    def __init__(
        self,
        payload_committer: WalCommitter,
        coordinator: OwnershipCoordinator,
        tokens: dict[str, OwnerToken],
    ) -> None:
        self.payload_committer = payload_committer
        self.coordinator = coordinator
        self.tokens = dict(tokens)
        self.namespace = coordinator.namespace
        self._blocked: set[str] = set()
        self._failed: set[str] = set()
        self._last_outcomes: dict[str, CommitOutcome] = {}

    def owner_epoch(self, tenant_id: str) -> int:
        return self._token(tenant_id).epoch

    def token_for(self, tenant_id: str) -> OwnerToken:
        return self._token(tenant_id)

    def require_ready(self, tenant: str, connection: sqlite3.Connection) -> None:
        if tenant in self._blocked:
            raise OwnershipError("accepted commit is not locally applied; recovery required")
        state, _ = self.coordinator.status(tenant)
        if state.token != self._token(tenant):
            raise StaleOwnerError("runtime no longer owns tenant")
        if current_sequence_state(connection) != (state.committed_sequence, state.record_head):
            raise OwnershipError("local head differs from authoritative committed head")

    def state(self, tenant: str) -> str:
        if tenant in self._blocked:
            return "RECONCILING"
        try:
            state, _ = self.coordinator.status(tenant)
            return "READY" if state.token == self._token(tenant) else "FENCED"
        except OwnershipError:
            return "FAILED"

    def commit(self, record: WalRecord) -> WalCommitReceipt:
        token = self._token(record.tenant_id)
        if record.owner_epoch != token.epoch:
            raise StaleOwnerError("prepared WAL does not carry current owner epoch")
        receipt = self.payload_committer.commit(record)
        attempt_id = _attempt_id(record)
        try:
            outcome = self.coordinator.commit(token, record, receipt, attempt_id=attempt_id)
        except Exception:
            self._blocked.add(record.tenant_id)
            raise
        self._last_outcomes[record.tenant_id] = outcome
        self._blocked.add(record.tenant_id)
        return replace(
            receipt,
            authority_epoch=token.epoch,
            authority_attempt_id=attempt_id,
            authority_descriptor_key=outcome.descriptor_key,
            authority_revision=outcome.revision,
            authority_decision=outcome.decision.value,
        )

    def applied(self, tenant: str) -> None:
        self._blocked.discard(tenant)

    def seal(self, tenant: str) -> None:
        raise OwnershipError("fenced ownership does not use cooperative sealing")

    def reconcile(self, tenant: str, connection: sqlite3.Connection) -> None:
        state, _ = self.coordinator.status(tenant)
        if state.token != self._token(tenant):
            raise StaleOwnerError("cannot reconcile without current ownership")
        if current_sequence_state(connection) != (state.committed_sequence, state.record_head):
            raise OwnershipError("full fenced recovery is required")
        self._blocked.discard(tenant)

    def close(self) -> None:
        self.payload_committer.close()

    def _token(self, tenant: str) -> OwnerToken:
        try:
            return self.tokens[tenant]
        except KeyError as exc:
            raise OwnershipError(f"no owner token for tenant: {tenant}") from exc


def _attempt_id(record: WalRecord) -> str:
    assert record.record_checksum is not None
    material = f"{record.tenant_id}\0{record.transaction_id}\0{record.request_fingerprint}"
    return "commit-" + hashlib.sha256(material.encode()).hexdigest()[:40]


def _safe_identity(value: str) -> None:
    if not value or len(value) > 200 or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
        for character in value
    ):
        raise ValueError("unsafe authority identity")
