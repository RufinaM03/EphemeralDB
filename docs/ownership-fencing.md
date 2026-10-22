# Ownership Fencing

Phase 8 adds an explicit `fenced-v1` mode. It is a fresh-namespace protocol and does not
reinterpret legacy sealed WAL. The authority is one mutable control object per tenant;
owner session, epoch, revision, accepted sequence/hash head, descriptor head, and checkpoint
anchor are updated in the same compare-and-swap domain.

## Commit And Takeover

The commit linearization point is the successful control CAS. A WAL payload and immutable
descriptor written before that CAS are candidates, not commits. Reachability by walking
backward from the control object's descriptor head defines the accepted history.

Takeover CASes the same object, preserving the accepted head and incrementing both epoch and
revision. A writer paused after checking epoch E can upload bytes after epoch E+1 exists, but
its old-validator CAS cannot be accepted. If its commit CAS won first, takeover preserves it.

The trust model is authenticated, non-Byzantine runtimes and explicit operator takeover.
There is no clock lease, automatic failure detector, or authority reset. Control-plane
unavailability blocks serving. The S3 production adapter relies on conditional `PutObject`
and should be protected by IAM/bucket policy that denies unconditional control overwrites.

## Serving States

`UNOWNED -> ACQUIRING -> RECOVERING -> READY` is the intended startup path. An accepted
commit that is not locally applied moves the tenant to `RECONCILING`; a different current
token is `FENCED`; corrupt/missing authority is `FAILED`. Every normal read re-reads control
and verifies token plus local sequence/hash before querying SQLite. That control read is the
read linearization point. A takeover may linearize immediately after it, so this is not a
long-lived lease or stale-read promise.

## Unknown Outcome Table

| Operation | Timeout classification | Resolution |
|---|---|---|
| Candidate PUT | Unknown | Read stable key; identical bytes confirm storage only. |
| Descriptor PUT | Unknown | Read stable key and checksum; it is still not committed. |
| Commit CAS | Unknown | Find attempt in accepted descriptor chain; a later incompatible revision excludes the old CAS; otherwise remain blocked. |
| Takeover CAS | Unknown | Read control for the stable transition/session ID; another later session excludes activation. |
| Read/control GET | Unavailable | Reject serving; never infer empty authority. |

An accepted response may be lost and found after later commits because attempts remain in the
immutable descriptor chain. Absence is conclusive only after a revision change makes the old
expected-validator CAS permanently ineligible.

## Recovery And Batching

The successor acquires ownership, captures the accepted head, installs the authority-bound
checkpoint into a unique staging file, traverses the bounded descriptor chain, verifies each
single or batch locator, replays only reachable records, and rechecks authority immediately
before atomic installation. The old cache, attempt journal, seal, PID, clock, and ACK ledger
are not inputs. Required missing/corrupt data fails closed; unreachable candidates are ignored.

A shared physical batch has one decision per tenant. Each descriptor binds batch key,
checksum, record count, and index. One tenant may be accepted while another is fenced. The
shared object must be retained while any accepted descriptor references it.

## Checkpoints And Cost

Checkpoint registration CASes control and names a descriptor ancestor. Publication may lag
new commits, but a stale owner cannot register. Phase 8 performs at least a payload PUT,
descriptor PUT, control GET, and conditional control PUT per fresh accepted transaction,
plus verification reads and takeover/recovery traffic. Batching saves payload PUTs, not the
per-tenant authority decisions. Counters report SDK/API invocations; internal SDK retries and
billable wire attempts remain unknown.
