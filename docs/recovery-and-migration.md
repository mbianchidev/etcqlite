# Recovery, backup, and migration

Status: planned workflows. Nothing here authorizes a live cutover or restore.

## Three different artifacts

| Artifact | Purpose | Must not be confused with |
| --- | --- | --- |
| Native Etcqlite snapshot | Recover or bootstrap Etcqlite at a known committed state | A raw copy of the active SQLite main file |
| Upstream-compatible etcd snapshot | Approved etcd tooling interoperability and reversible migration | A renamed SQLite file |
| Read-only inspection projection | Consented sql-not-so-lite troubleshooting | A complete backup or recovery source |

Every native snapshot binds cluster identity, format versions, applied
protocol position/configuration, KV and compaction revisions, membership,
leases, auth state, and integrity digest.

## Creating a native snapshot

1. Choose a committed/applied frontier and prevent inconsistent metadata capture.
2. Use a supported SQLite consistent backup mechanism or a verified checkpointed
   snapshot. Do not copy a live main file while ignoring its WAL.
3. Include all state needed to replay the remaining protocol suffix.
4. Write into a new temporary artifact with restrictive permissions.
5. Flush the artifact, verify integrity and metadata, then atomically publish it.
6. Preserve the previous verified snapshot until the replacement is durable.

Bound snapshot I/O and memory. Cancellation removes only the operation's own
temporary files. Backup retention is enforced only after successful replacement.

## Restore and member replacement

Restore into a new directory, validate versions/checksums and SQLite
integrity, replay the required suffix, and compare the logical state at the
declared revision. Keep client traffic disabled until authority and readiness
are established.

A whole-cluster disaster restore must establish an explicit new cluster
identity or a fully justified original-identity recovery procedure. Never
allow a restored cluster and the original cluster to accept writes as the
same authority.

A replacement member uses a new identity unless a verified same-member
recovery protocol applies. Removed identities and stale configurations are
rejected. Never erase a replica's state merely to make startup succeed.

Document recovery from one lost voter, one corrupt voter, quorum loss,
all-node loss, missing backup segments, incompatible formats, and failed
snapshot publication. Replication is not backup.

## etcd import/export feasibility

Use the pinned etcd snapshot format and protobuf encodings as the reference.
Investigate an isolated Go/bbolt helper for offline conversion if necessary.
The product's consensus remains non-Raft; importing snapshots does not mean
participating in an etcd peer cluster.

Full-fidelity translation must account for:

- Binary keys and values, including embedded zero bytes and empty values.
- Key generations, versions, create/mod revisions, retained history and tombstones.
- Current revision, compacted boundary, and any supported revision bump.
- Lease IDs, granted TTLs, remaining-time/checkpoint policy, and key ownership.
- Users, password hashes, roles, permissions, and auth revision.
- Tool-specific metadata, snapshot digest, hash behavior, and identity rewriting.
- Cluster membership representation without pretending consensus metadata is portable.

Do not import raft log positions as Etcqlite protocol state. Start the new
consensus cluster through a documented fenced bootstrap using translated
application state.

## Cutover procedure

The first supported migration is an explicit maintenance-window cutover.
Online dual-writing is not in the initial scope.

1. Inventory source server/client/tool/Kubernetes versions and required APIs.
2. Verify a recent independent backup and an isolated restore of it.
3. Stop or fence all writers, controllers, operators, and background jobs.
4. Record the source revision, compaction boundary, logical hashes, counts,
   membership, auth state, and lease inventory.
5. Export a consistent source snapshot and translate it without text conversion.
6. Restore into an isolated destination, verify all in-scope metadata, and
   validate the new protocol cluster before opening client traffic.
7. Apply a reference-compatible revision bump/compaction policy when required
   to invalidate Kubernetes watch caches. Do not improvise a counter reset.
8. Change endpoints and trust material through the owning deployment tool.
9. Exercise read/write, watch, auth, lease and controller behavior with explicit
   operator approval to resume normal writers.
10. Keep the fenced source and rollback artifacts until the acceptance window ends.

Line-based `etcdctl get`/shell replay is not a valid general migration: values
are arbitrary bytes, metadata is lost, and watch revisions change.

## Rollback

Before destination writes begin, rollback can restore the original endpoints
to the still-fenced, unchanged source after removing destination authority.

After destination writes begin, the old source is stale. Rollback requires
fencing the destination and an approved reverse translation/replay into an
isolated source restore. Repointing clients to the old cluster would lose
acknowledged writes.

Do not describe a binary downgrade or endpoint switch as a data rollback.
Measure recovery time and the accepted recovery point with the operator.

## Format and tooling gates

Native recovery can ship experimentally before upstream snapshot
interoperability, but only under a clearly narrower label. Full approved
tooling compatibility requires both-direction conversion and real
`etcdctl`/`etcdutl` workflows to pass.

If full translation cannot be proven, document the gap and stop the broad
drop-in claim. Do not silently substitute current-key-only migration.

## Recovery acceptance cases

Cover crash during snapshot, checksum mismatch, truncated stream, missing WAL,
disk full, incompatible schema, interrupted restore, wrong cluster identity,
resuming stale members, reviving the fenced source, auth preservation,
lease expiry during maintenance, and Kubernetes cache invalidation.

Keep all fixtures synthetic. Preserve encrypted credentials and application
secrets as sensitive data in backups; inspection projections omit them.
