# Client API compatibility contract

Status: all 49 methods have native handlers in the experimental server.
The inventory below remains an acceptance contract, not a full-compatibility
claim. Bounded client, membership, snapshot, and recovery cases do not replace
the full differential/fault/platform qualification matrix.

Reference: etcd v3.7.3. Preserve upstream protobuf service/package names,
method paths, request/response fields, enum values, streaming direction,
headers, limits, and documented error semantics.

## Complete RPC inventory

The pinned core protobuf defines 42 RPCs; Lock and Election add seven.
Rows are acceptance obligations, not a claim that the proposed engine can
already satisfy every operational behavior.

| ID | Service | RPC | Acceptance focus |
| --- | --- | --- | --- |
| A01 | KV | Range | Binary ranges, current/historical snapshots, sort/filter/limit/count, default linearizability |
| A02 | KV | RangeStream | Reference chunking, ordering, revision, cancellation, and unsupported-option behavior |
| A03 | KV | Put | Create/update versions, lease attachment, ignore flags, previous KV, invalid input |
| A04 | KV | DeleteRange | Empty/single/range deletes, previous KVs, atomic revision and event behavior |
| A05 | KV | Txn | Every comparison target/operator, nested branches, range operations, atomic failure, duplicate-key rules |
| A06 | KV | Compact | Logical boundary, physical completion, future/already-compacted errors |
| A07 | Watch | Watch | Multiplexed bidirectional create/cancel/progress, replay, filters, previous values, fragmentation |
| A08 | Lease | LeaseGrant | Requested/generated IDs, TTL negotiation, duplicates, restart persistence |
| A09 | Lease | LeaseRevoke | Atomic lifecycle and all attached-key deletions |
| A10 | Lease | LeaseKeepAlive | Bidirectional streaming, authority, renewal, expiry and failover races |
| A11 | Lease | LeaseTimeToLive | Remaining/granted TTL and attached-key listing |
| A12 | Lease | LeaseLeases | Live lease enumeration and authorization |
| A13 | Cluster | MemberAdd | Stable identity, peer URLs, voter/learner creation, safe configuration transition |
| A14 | Cluster | MemberRemove | Committed removal, self-removal, unknown members, quorum safeguards |
| A15 | Cluster | MemberUpdate | Validated endpoint changes and persistence |
| A16 | Cluster | MemberList | Actual membership, IDs, URLs, role and consistency options |
| A17 | Cluster | MemberPromote | Learner-only transition, catch-up preconditions, safe promotion |
| A18 | Maintenance | Alarm | Real persisted list/activate/deactivate semantics and scoped permissions |
| A19 | Maintenance | Status | Honest sizes, versions, leader, indexes, errors, and learner state |
| A20 | Maintenance | Defragment | Meaningful storage reclamation and completion/error behavior |
| A21 | Maintenance | Hash | Pinned reference hash input and revision semantics |
| A22 | Maintenance | HashKV | MVCC history hash, compact boundary and cross-implementation comparability |
| A23 | Maintenance | Snapshot | Consistent stream, integrity, cancellation, and approved upstream tooling behavior |
| A24 | Maintenance | MoveLeader | Actual safe leadership transfer or an explicit blocking compatibility gap |
| A25 | Maintenance | Downgrade | Validate/enable/cancel behavior with an honest engine-version compatibility policy |
| A26 | Auth | AuthEnable | Root preconditions, atomic activation, persistence |
| A27 | Auth | AuthDisable | Permission checks and atomic deactivation |
| A28 | Auth | AuthStatus | Enabled state and actual auth revision |
| A29 | Auth | Authenticate | Reference-compatible credentials, tokens, expiry and error behavior |
| A30 | Auth | UserAdd | Hashing, options, duplicates, validation |
| A31 | Auth | UserGet | User metadata and role mapping without credential disclosure |
| A32 | Auth | UserList | Authorized deterministic enumeration |
| A33 | Auth | UserDelete | Protected users, grants, revocation and active-client effects |
| A34 | Auth | UserChangePassword | Safe hashing and token/auth revision effects |
| A35 | Auth | UserGrantRole | Existing user/role checks and committed grant |
| A36 | Auth | UserRevokeRole | Committed revocation and in-flight request handling |
| A37 | Auth | RoleAdd | Duplicates, validation, persistence |
| A38 | Auth | RoleGet | Exact binary permission ranges |
| A39 | Auth | RoleList | Authorized deterministic enumeration |
| A40 | Auth | RoleDelete | Protected roles and user mappings |
| A41 | Auth | RoleGrantPermission | Read/write types, special range encodings, updates |
| A42 | Auth | RoleRevokePermission | Exact range removal and authorization fencing |
| A43 | Lock | Lock | Lease-backed ownership, same-lease reacquisition, waiting and cancellation |
| A44 | Lock | Unlock | Ownership-key deletion and waiter progress |
| A45 | Election | Campaign | Lease-backed ordered candidacy, cancellation cleanup |
| A46 | Election | Proclaim | Leadership ownership validation and atomic value update |
| A47 | Election | Leader | Current leader and no-leader behavior |
| A48 | Election | Observe | Ordered leader/value stream and failover |
| A49 | Election | Resign | Ownership validation and transfer to next candidate |

The inventory must be mechanically checked against the pinned protobuf before
implementation; method names rather than row numbers are authoritative.

## KV and transaction semantics

Compare VALUE, VERSION, CREATE, MOD, and LEASE with every supported operator.
Exercise missing keys, binary values, range comparisons, and nested
transactions. Validate all operations before allowing a partial mutation.

One modifying transaction has one main revision. Preserve operation ordering
where the reference promises it; do not impose an undocumented guarantee
where nested transaction ordering is unspecified.

Honor keys-only, count-only, limits, sort target/order, min/max create/mod
revision filters, previous values, and zero/sentinel bounds. `RangeStream`
must mirror the reference's actual option restrictions, not silently ignore
unsupported sorting or filtering.

## Watch semantics

Watch creation and historical replay must not race with current writes.
Events share complete revisions, except where the protocol's explicit
fragmentation behavior permits transport fragments. A fragmented revision
cannot be exposed as independently committed partial mutations.

Preserve watch IDs, created/canceled responses, compact revision, filters,
previous KVs, progress requests, and progress notifications. Never advertise
a progress revision beyond events already guaranteed delivered.

Authorization changes must have the reference-compatible effect on existing
streams. A client disconnect must release the stream and its replay resources.

## Response metadata

Persist cluster/member identities; report the snapshot's actual revision.
Do not return a synthetic member list or hard-coded leader.

The protobuf uses Raft-named status fields even for an alternative engine.
Define and test the mapping to real protocol epochs, decided positions, and
applied positions. Do not fill these with zero or claim literal Raft state.
If required semantics cannot be represented, record a blocking gap.

Expose the actual Etcqlite build identity separately from the supported etcd
API version. The `Status.version` policy must pass real client/tool version
checks without misleading operators about which software is running.

## Errors and limits

Match gRPC status codes, etcd error identity/message behavior, stream
termination rules, and whether a mutation may have committed before failure.
Include invalid keys/ranges, request limits, oversized transactions, unknown
leases, future revisions, compacted revisions, auth failures, full quota,
corruption alarms, no leader, cancellation, and deadlines.

Never return an empty successful result for a missing implementation.
Transport-level unsupported responses remain documented gaps and block the
full compatibility label.

## HTTP and operational surfaces

Qualify the HTTP/JSON gateway's paths, base64 fields, integer encoding,
streaming framing, auth and TLS behavior against the reference. Cover health,
readiness, liveness, version discovery, and the operational metrics contract.
Do not imply identical internal Prometheus metric names without testing them.

## Tooling compatibility

The current Maintenance implementation includes persisted alarms, quota/corruption
gates, physical SQLite defragmentation, Castagnoli hashes over the exported etcd
buckets, revision-aware HashKV and compaction hash retention, and a real
upstream-compatible Snapshot stream. Snapshot requires the installed companion
`etcqlite-snapshot` binary. `/version` separates the engine/build, SQLite schema,
and exported tooling profile. The storage quota remains an explicitly reported
logical MVCC estimate, not a hard cap on the combined application/protocol file.

Clustered membership uses committed OmniPaxos StopSigns and a new-configuration
barrier before ordinary membership responses complete. Learners persist certified
decided prefixes without voting; promotion verifies catch-up. New join directories
claim unique incarnations through the replicated log before client admission or
voting. Reinitializing an already claimed member ID is rejected. Self-removal
acknowledges the committed removal and stops the local worker.

Current boundaries remain explicit: quorum configurations require at least two
voters; local mode cannot become a peer cluster merely through MemberAdd. Fresh
cluster bootstrap uses configuration 1; later configurations require verified join
or disaster restore. Downgrade manages the 3.6.0 client/tooling profile and disables
3.7-only RangeStream until cancellation. It does not install an older engine or
prove mixed native-binary compatibility.

Snapshot/Hash/HashKV/Downgrade qualification needs dedicated evidence.
SQLite-native output is not an etcd snapshot, and a SQLite page checksum is
not the etcd MVCC hash.

Require approved tool versions and workflows to succeed, including snapshot
save/status/restore and migration checks. The installed isolated Go/bbolt converter emits upstream snapshots without
introducing Raft into the product. A helper's existence does not excuse
incorrect exported metadata, revisions, auth state, or leases.

Kubernetes storage compatibility, client API compatibility, and snapshot
format compatibility are separate gates. The detailed cutover contract is in
[recovery and migration](recovery-and-migration.md).
