# Recovery, backup, and migration

Status: native backup/restore, verified member join, schema up/down migration and
both-direction application interchange are implemented experimentally. Nothing
here authorizes a production cutover or restore. One isolated multi-GiB
application cutover and post-write reverse restore passed on the owned Linux
fixtures. Multi-host recovery, failure campaigns and complete RPO/RTO
qualification remain pending.

## Runnable offline operations

Stop the owned server before opening its data directory with offline tools.
The node lock refuses concurrent writers. All destinations must be new; a
failed operation never overwrites an existing node or verified artifact.

```sh
etcqlite snapshot save --data-dir ./node --output ./native.snapshot
etcqlite snapshot status --input ./native.snapshot
etcqlite snapshot restore --input ./native.snapshot --data-dir ./restored \
  --fenced-source --revision-bump 1000000 --mark-compacted

etcqlite migrate --data-dir ./node --target 2 --backup ./before-up.snapshot
etcqlite migrate --data-dir ./compatible-node --target 1 --backup ./before-down.snapshot
```

New nodes use SQLite schema 2. Schema 1 remains readable and requires explicit
migration before dynamic administration. Numbered up/down SQL preserves the
original MVCC format. Downgrade refuses committed reconfiguration, incarnations,
multi-endpoint state and administrative log/outcome records an older binary
cannot interpret. It is not an online mixed-binary upgrade mechanism.

Native snapshots use SQLite's consistent backup API, complete application and
protocol integrity checks, a SHA-256 trailer, private staging and flushed,
no-overwrite publication. Restore replays decided-but-unapplied commands, creates
a new cluster identity, clears nonportable votes/tokens/operational alarms, and
preserves application history, auth and lease state. The returned `source_sha256`
identifies the input artifact, not the modified restored database.

For whole-cluster restore, provide a fresh shared `--cluster-config` and explicit
`--member-id` on each node. Every voter must use the **same verified input artifact**.
Its fingerprint becomes the configuration's bootstrap digest; peer authentication
rejects a different initial checkpoint under the same cluster ID. This does not
turn replicas on one host into host-level HA.

```sh
etcqlite snapshot export --data-dir ./node --output ./application.json
etcqlite-snapshot export ./application.json ./upstream.db
etcdutl snapshot status ./upstream.db --write-out=json
etcdutl snapshot restore ./upstream.db --data-dir ./isolated-etcd

etcqlite-snapshot import ./verified-etcd-snapshot.db ./application.json
etcqlite snapshot import --input ./application.json --data-dir ./isolated-etcqlite \
  --fenced-source --revision-bump 1000000 --mark-compacted
```

Interchange preserves binary revisions/tombstones, current generations, leases,
password hashes, roles/permissions and auth revision. Active source alarms,
unsupported records or unsafe fields fail explicitly. Consensus membership and
tokens are not portable authority. JSON manifests retain their 512-MiB limit.
For larger histories, use the disk-backed path rather than materializing a JSON
manifest:

```sh
etcqlite snapshot save --data-dir ./fenced-node --output ./native.snapshot
etcqlite snapshot status --input ./native.snapshot
etcqlite-snapshot export-native ./native.snapshot ./upstream.db
etcdutl snapshot status ./upstream.db --write-out=json

etcqlite-snapshot stage ./verified-etcd-snapshot.db ./application.sqlite
etcqlite snapshot import --input ./application.sqlite --data-dir ./isolated-etcqlite \
  --fenced-source --quota-bytes 8589934592 \
  --revision-bump 1000000 --mark-compacted
```

The native/bbolt/staged converter accepts files up to 16 GiB, with at most
16 MiB per key or value record. Native-to-bbolt export commits bounded 16-MiB
batches into an unpublished private file. Staging streams all allowlisted
buckets into SQLite. Import uses a disk-backed temporary current-key index,
not an in-memory history or live-value map. Administrative/auth metadata remains
bounded separately. The configured destination quota still applies.

Staged format 2 has one `interchange_meta` record with
`kind='bbolt-interchange'`, `format=2`, and `reference='3.7.3'`, plus fixed
`interchange_buckets` and `interchange_entries` tables. Types, primary keys,
singleton metadata, bucket ownership, record sizes and the SHA-256 footer
are validated. Unknown tables/views/triggers, malformed profiles and hidden
or orphan records fail before publication. `compact_revision=-1` is the valid
fresh-store sentinel. These files contain sensitive auth and historical KV;
they are not inspector projections or generic writable SQLite databases.

Live native and upstream snapshot RPCs pin a consistent WAL reader under the
owned writer, then perform backup and validation off that writer. Header and
peer-configuration metadata use the pinned frontier. Capture concurrency is
bounded across native endpoints; upstream conversion has a ten-minute deadline.
Backend Hash and HashKV scans share the bounded pinned-reader admission and do
not hold up protocol ticks or client mutations. Read views retain a duplicate
of the owned lock descriptor until completion. For large histories, set an
explicit client deadline, for example `etcdctl --command-timeout=60s endpoint hashkv`.
This implementation removes the JSON-history limitation. The measured large
application round trip below does not close RPO/RTO or fault qualification.

`--fenced-source` is an operator attestation, not software that fences a remote
cluster. Before destination writes, retain the unchanged fenced source. After
destination writes, export the destination's new state and reverse-translate it
into a new isolated authority; never repoint clients to the stale source.

Lease checkpoints are honored on restart; absent checkpoints use the granted-TTL
extension policy. Tokens must be reacquired. Revision bump/compaction is explicit
and invalidates old watch caches; it is not a counter reset.

## Measured large application round trip

The frozen `1fec3e7` Linux candidate's fully verified 5,907,914,784-byte native
image was translated to a pinned etcd v3.7.3 bbolt image. Disk-backed import into
a fresh native authority retained revision 35,154 and compact revision -1;
native and upstream HashKV both returned 1,758,762,700. Native import took
18.13 seconds with 25,472 KiB peak RSS. Upstream restore used a separate fresh
authority, not the stopped original voter directories.

After actual upstream destination writes, its verified snapshot was staged and
reverse-imported into a third new native authority. Both destination and reverse
restore returned HashKV 1,974,551,075 at revision 35,159. All 100,087 live synthetic
keys were retained, along with a binary NUL/0xff value, the deleted/recreated key's
history and generation counters, password-authenticated root access and auth
revision 4, and the same lease ID, granted TTL and key ownership. Reverse import
took 16.45 seconds with 25,472 KiB peak RSS. Native images from both restored
authorities were independently integrity-checked and retained privately.

These are measured component timings, not end-to-end RTO or a multi-voter
disaster recovery claim. Source writers stayed stopped, no stale source was
retargeted, and no production workload participated. The ordinary no-checkpoint
granted-TTL extension policy was used. See [qualification](verification-and-performance.md)
for the remaining gates.

## Joining and replacing a member

Add the new member through the Cluster API, issue its certificate from the trusted
CA with the required cluster/member SPIFFE URI, and obtain a verified native
snapshot. `etcqlite join --snapshot ./native.snapshot --data-dir ./new-member
--member-id <new-id> --name <name>` seeds only decided history and creates a new
local incarnation. Starting the server commits the identity claim before admission.
Promotion then verifies actual catch-up.

A lost established voter needs removal and a **new** member ID, not a fresh
directory with its old promises erased. A normal restart uses the same owned
directory. The tests reject stale pre-join identity reuse and preserve a valid
same-directory restart.

## Four different artifacts

| Artifact | Purpose | Must not be confused with |
| --- | --- | --- |
| Native Etcqlite snapshot | Recover or bootstrap Etcqlite at a known committed state | A raw copy of the active SQLite main file |
| Upstream-compatible etcd snapshot | Approved etcd tooling interoperability and reversible migration | A renamed SQLite file |
| Disk-backed application interchange | Bounded-memory fenced translation of complete application history | A native identity-preserving recovery image or writable inspector database |
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
