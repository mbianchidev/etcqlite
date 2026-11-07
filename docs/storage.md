# SQLite storage and MVCC

Status: deterministic local KV state machine implemented and tested in
`src/storage/`. The concrete schema is `src/storage/schema.sql`; networking
and distributed application integration are separate milestones.

The implemented core has explicit private-directory initialization,
exclusive ownership, binary unsigned identities, metadata/history/result
checksums, atomic application plus request-result ledger, MVCC generations,
range/sort/filter/count semantics, pre-mutation nested comparison paths,
rollback, compaction floors, and durable watch event records.

Reference-derived initial counters are KV revision `1`, compaction boundary
`-1`, and auth revision `1`. Lease/auth/membership tables are present; their
client-facing behavior is not implied by the schema. Schema, command and
snapshot format qualification still follows the gates below.

## Engine rules

Use local storage, WAL mode, explicit transactions, foreign-key enforcement,
bounded lock waits, and full synchronization for protocol persistence.
Qualify stronger platform-specific synchronization settings where required.
Treat the storage device's flush guarantees as part of the durability model.

One dedicated writer owns mutations. Each read connection has bounded
snapshot lifetime. Background checkpoints and physical maintenance must not
starve writers or retain unbounded WAL files.

SQLite WAL is not distributed consensus and is not supported as a shared
multi-host network-filesystem database. Every replica owns its own file.

The general PostgreSQL conventions for timestamps and automatic IDs are not
appropriate here: use SQLite types and protocol-defined revisions. Do not
introduce automatic timestamp triggers or local-clock-derived revisions.

## Logical schema

| Table | Key and important fields | Invariant |
| --- | --- | --- |
| node_state | Singleton; cluster ID, member ID, persistent mode, schema version | Identity is never silently regenerated on restart |
| consensus_state | Configuration ID; promised ballot, accepted ballot, decided index, compacted index, stop/reconfiguration state | Protocol state survives a machine crash before its corresponding acknowledgment |
| consensus_entries | Configuration ID and log index; command envelope, codec version, integrity checksum | Never overwrite a decided entry; undecided replacement follows the protocol |
| app_state | Singleton; last applied position, current KV revision, compacted revision, auth revision | State and last-applied position commit atomically |
| current_kvs | Binary key; value, create revision, mod revision, version, nullable lease ID | Exactly the latest visible non-deleted version |
| mvcc_versions | Main revision and subrevision; binary key, event kind, value, create revision, version, lease ID | One durable, ordered history record per actual key mutation |
| leases | Lease ID; granted TTL, renewal/checkpoint state, generation, lifecycle | Lease IDs are identifiers, not TTL values |
| lease_keys | Lease ID and binary key | Inverse index agrees with the current key's lease |
| auth_users | Binary-safe username policy matching reference; password hash, user options | No plaintext passwords |
| auth_roles | Role identifier | Protected root behavior matches the reference |
| auth_user_roles | User and role IDs | Grants and revocations are atomic |
| auth_permissions | Role, permission type, binary range bounds | Uses etcd range semantics, not SQL text matching |
| cluster_members | Member ID; role, client URLs, peer URLs, committed configuration | Membership truth comes from a decided protocol transition |
| alarms | Member ID and alarm type | Alarms are meaningful persisted state, not synthetic status |

Do not duplicate a second persistent event log unless profiling proves a need.
`mvcc_versions` is initially both historical state and the watch replay source.

## Types and indexes

- Keys, values, range bounds, protocol ballots, checksums, and encoded
  unsigned identifiers use binary-safe representations.
- MVCC revision and key version counters fit the signed 64-bit protobuf
  contract; detect exhaustion rather than wrapping.
- Unsigned 64-bit cluster/member identifiers cannot be naively stored in
  SQLite's signed integer range. Use a canonical fixed-width binary encoding.
- Use a unique binary-key index for `current_kvs`.
- Index history by binary key followed by descending revision/subrevision.
- The history primary key supports ordered replay by revision/subrevision.
- Index lease ownership in both directions needed by reassignment and expiry.
- Add indexes from demonstrated query plans, not speculative ORM defaults.

No JSON or UTF-8 conversion may alter arbitrary key/value bytes. SQLite BLOB
comparison must match the reference's bytewise key ordering.

## Revision semantics

Initialize counters to the pinned reference's observable starting values.
Do not assume the initial revision is zero without checking the reference.

A modifying transaction advances the main revision once. All of its actual
key changes share that main revision, ordered by subrevision. A new key has a
new create revision and version one. Repeated puts advance version; delete
and recreation starts a new key generation.

Empty deletes, read-only transactions, failed comparison branches with no
mutations, auth changes, lease renewal, and membership updates need their
individual reference-derived revision behavior. Never use a consensus log
index as a substitute for the MVCC revision.

Multi-key transactions apply all comparisons and the selected branch in one
SQLite transaction. Support nested transactions and validate duplicate-key
restrictions, operation ordering, range reads, and error atomicity against
the reference. Do not approximate them as independent puts.

## Historical reads and range queries

Select the latest visible version per key at the requested revision, then
apply the exact reference filters, sorting, limits, count, and pagination.
Reject future and compacted revisions as the reference does.

Implement inclusive start/exclusive end and the special open-ended range
encoding. Prefix operations are binary ranges, not `LIKE` expressions.
Historical tombstones must prevent deleted keys from reappearing.

For `prev_kv` and watch previous values, retrieve the immediately preceding
visible key version. Compaction must retain enough floor state to answer all
still-supported requests correctly.

## Consensus persistence contract

Implement the selected library's atomic batch storage interface on SQLite.
Persist promised ballot, accepted round, log changes, decided position,
snapshot metadata, and reconfiguration state with the ordering its protocol
requires.

An accepted acknowledgment must not leave the node before required storage
transactions are durably committed. A successful SQLite API call with unsafe
sync settings is not sufficient.

Acceptance and application are separate transactions because a node cannot
hold a SQLite write transaction across a network quorum wait. Initially accept
the possible extra flush cost. Group adjacent durable writes only without
changing the acknowledgment boundary.

On restart, load durable protocol state, reject inconsistent boundaries, and
replay decided but unapplied entries. An already-applied entry cannot repeat
its effects because apply and its index are one atomic transaction.

## Lease storage

Replicate grant, revoke, renewal/checkpoint state, and expiry decisions.
Leaders schedule expiry using monotonic local timers derived from committed
state. Followers do not independently delete keys based on their wall clocks.

The correctness-first renewal path uses durable ordered renewal batches.
Renewals do not create KV revisions. Any optimization to leader-local renewal
state needs a reference-compatible failover argument and dedicated tests.

Delete lease-owned keys through the MVCC transaction path so watches,
versions, and lease indexes agree. Determine expiry batching and revision
boundaries from the pinned reference, not convenience.

## Compaction and physical maintenance

Logical compaction publishes a monotonic history boundary. Keep the latest
floor version required for each live key and all supported later history.
Handle tombstones and previous-value dependencies explicitly.

Separate logical compaction from reclaiming SQLite pages. A physical request
completes only after its promised work completes. Long history readers must be
bounded or cancelled according to an explicit policy.

Consensus log trimming is a different operation. Trim only after a validated
state snapshot and protocol-required recovery conditions make the prefix
unnecessary. Do not trim based only on the MVCC compaction revision.

## Schema and encoding evolution

Version the SQLite schema, command envelope, and snapshot formats separately.
Replicate only formats every active voter can interpret. Record the minimum
reader/writer versions in the cluster state.

Use backward-compatible additive changes first. Require a verified snapshot
and explicit compatibility transition before destructive migrations. A
binary rollback is not a database rollback.

## Required storage evidence

Cover byte-order edge cases, overflow, uniqueness, lease reassignment,
tombstones, compaction floors, crash at every durable boundary, disk full,
flush failure, corruption detection, WAL recovery, and repeated application.
The acceptance owner is the storage workstream in
[the implementation plan](implementation-plan.md).
