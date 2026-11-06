# Non-Raft consensus and failover

Status: experimental implementation. An explicitly approved local fork and
SQLite persistence adapter are present. Bounded M0 evidence is not production
or multi-host qualification.

## Selection

Start with **OmniPaxos/Sequence Paxos** as the candidate replicated log, not
with a new home-grown consensus algorithm. The inspected research revision is
`9c772dcce5a9652192f45f67d9d2208b306f3bc9` from 2026-10-24.
The project describes itself as in development. Its repository did not expose
a latest GitHub release or tags during this investigation; choose and audit
an immutable implementation revision during M0.

Implement its persistence interface on SQLite rather than shipping its
RocksDB-backed storage implementation. This is real protocol-storage work,
not a drop-in database-driver substitution.

## M0 result and maintained fork

The initial simulation failed against immutable upstream revision
`9c772dcce5a9652192f45f67d9d2208b306f3bc9`, also the latest upstream revision
when checked. Both SQLite and upstream memory storage reproduced the failure;
it was not hidden by weakening the oracle or disabling synchronization.

During delta snapshot synchronization, `sync_log` advanced its cached decided
frontier before building the receiver's existing snapshot. The merge could
then include an old leader's undecided suffix, or lose a committed prefix,
while labeling the result with the new decided position.

The owner explicitly approved maintaining a local fork on 2026-11-07.
`vendor/omnipaxos` retains upstream's Apache-2.0 license and source. The local
change constructs the snapshot from the old decided frontier, stages cache
updates, commits the atomic storage batch, and only then publishes those
cache updates. The fork is not an independent new consensus algorithm.

`tests/protocol_simulation.rs` includes the minimized delta regression,
partitioned old-leader fencing, restart, snapshot/suffix catch-up, a
non-voting learner/reconfiguration model, real target-leader attempts,
duplicate/reordered/lost messages, and ignored-storage-error poisoning.
The history oracle checks overlapping decided prefixes throughout seeded
runs and preserves acknowledged history. Proposal admission is not mistaken
for a client acknowledgment; unacknowledged dropped proposals are retried
after connectivity returns.

`tests/protocol_storage.rs` covers durable atomic batches, process exit
without destructors, rollback, absolute indexes, snapshot/StopSign recovery,
counter/ballot bounds, and checksums. The adapter uses WAL, `FULL` sync,
macOS full-sync pragmas, and a poison flag that the runtime must check before
emitting messages or serving requests.

These are short, one-host M0 spikes. Actual secured transport, durable
application state, distributed membership operation completion, external
history checking, disk fault qualification, and the long-running acceptance
campaign remain mandatory later gates.

| Alternative | Value | Reason not to select it automatically |
| --- | --- | --- |
| Multi-Paxos through OmniPaxos | Embeddable log with pluggable storage/networking and partial-connectivity research | Needs durability, reconfiguration, learner, read, and operational qualification |
| Viewstamped Replication | Production precedent in TigerBeetle | TigerBeetle's implementation is tightly integrated with its engine; extraction is substantial work |
| EPaxos family | Can reduce coordination latency for some independent geo-distributed operations | Global revisions, range queries, and transactions complicate conflict tracking and ordering |
| S3 conditional-write coordination | T4 demonstrates a non-Raft operational model | Adds an external authority, clock/lease assumptions, and a different failure contract |
| Raft through dqlite/rqlite | Existing distributed SQLite implementations | Violates the non-Raft product requirement |

No candidate is assumed faster because of its algorithm name. If OmniPaxos
fails a mandatory gate, record a decision and obtain a new direction. Do not
silently introduce Raft, external S3 authority, or a custom protocol.

## Mandatory M0 feasibility spikes

| Gate | Required result |
| --- | --- |
| C01: durable storage | Atomic protocol batches, promises, accepted entries, decided index, and restart semantics fit the SQLite adapter |
| C02: read authority | A logged barrier gives a correct linearizable read path during leader changes and partitions |
| C03: reconfiguration | A completed stop/reconfiguration boundary cannot lose or duplicate acknowledged requests |
| C04: learners | A non-voter can catch up without accidentally contributing to quorum and can be safely promoted |
| C05: leadership transfer | A real safe transfer can support the observable `MoveLeader` contract |
| C06: recovery | Snapshot plus retained suffix restores exactly the decided state and current membership |
| C07: failure reporting | Library storage errors and panics cannot lead to continued serving with invalid authority |
| C08: metadata | Library epochs/configurations can be represented honestly in client-visible status fields |
| C09: licensing and maintenance | License, transitive dependencies, maintenance ownership, and patch strategy are acceptable |

Learners and transfer support are requirements to prove, not claims about the
candidate library. If achieving them requires protocol changes, the proof,
simulation, and maintenance cost belongs in the decision.

## Failure model

Assume crash/restart failures, message loss, duplication, reordering,
asymmetric partitions, delayed disks, and lost local replicas. Protect
against detectable storage corruption and refuse unsafe operation.

Do not claim Byzantine fault tolerance or protection from a malicious quorum.
Document the storage durability and operating-system assumptions.

Initially use a conventional three-voter majority configuration. Flexible
quorums, two-node automatic HA, and quorum-size reductions are not shortcuts
for the first implementation. Administrative reconfiguration must preserve
the protocol's intersection and transition requirements.

## Log and acknowledgment rules

- A replica acknowledges acceptance only after the required protocol state
  and command bytes survive a machine crash.
- A command is decided only by the selected protocol's rule.
- Apply decided commands in order, never merely locally accepted commands.
- Client success requires decided, applied, recoverable state.
- State-machine failure stops the replica; it cannot skip a command.
- Lost client responses produce uncertain outcomes, not fabricated aborts.
- Quorum loss never enables an automatic leader-only write fallback.

The durable log and SQLite application WAL are different logical concepts,
even when they share one physical database file.

## Elections and read fencing

Persist protocol promises and leadership state before messages that depend
on them leave the node. Old leaders cannot become authoritative simply by
restarting, hearing one follower, or reading a local lease deadline.

Use ordered read barriers first. A barrier that cannot reach the necessary
agreement fails rather than falling back to local reads. An explicitly
serializable client read remains a separate, weaker request.

Reduce barrier overhead only after a separate argument covers quorum
intersection, leadership changes, delayed messages, applied-index waits, and
snapshot races. Performance work must not smuggle in synchronized-clock
assumptions.

## Membership and configuration changes

Serialize membership operations. Track proposed versus committed
configurations, stable member identities, and removal tombstones.

For an added learner, authenticate the peer, transfer a validated snapshot,
stream the suffix, and measure catch-up. Promotion requires a caught-up
state and a committed protocol transition. A learner never counts as a voter
before that transition.

Map OmniPaxos stop/reconfiguration semantics to one explicit transition
state machine. Fence submissions at the old configuration boundary, preserve
outstanding request outcomes, establish the new configuration, and only then
complete the externally visible change. Do not label this mechanism "joint
consensus" unless the actual implementation has those semantics.

Self-removal stops service after commitment. Reusing a removed member's
storage or ID requires explicit recovery, not an ordinary restart.

## Leases and failover

The server's consensus authority is not the same as an application etcd
lease. Application lease timers cannot authorize an isolated server.

Renewal responses need a valid authority path. The first implementation
orders durable renewal batches; later local-only renewal optimization is
gated separately. On leadership change, reconstruct scheduling from committed
lease state with a reference-compatible extension/checkpoint policy.

Only the authoritative leader proposes expiry, and expiry becomes a decided
mutation before keys disappear or watch events are sent. Exercise keepalive
during partition, clock jumps, leader pause, restart, and transfer.

## Snapshots and catch-up

A protocol snapshot binds application revision, applied log position,
configuration, schema/codec versions, and integrity digest. Reject mismatched
snapshots or suffixes from another cluster/configuration.

Do not delete the log prefix until snapshot durability and protocol recovery
conditions are met. Quarantine corrupt replicas and resynchronize from a
verified source; do not vote with an empty replacement state.

## Expected performance

Stable-leader Raft, Multi-Paxos, and VSR all pay replication and durable-storage
costs. The initial SQLite adapter may incur extra serialization and flush
costs. Optimizations should target demonstrated queueing, batching, read
barriers, copying, or write amplification.

A favorable microbenchmark for a log library is not evidence for the full
Etcqlite server, and does not establish a universal faster consensus algorithm.
