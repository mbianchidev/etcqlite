# Porto and sql-not-so-lite integrations

Status: Etcqlite-owned API/inspection contracts and separately approved
integration worktrees are implemented experimentally.
[mbianchidev/porto#191](https://github.com/mbianchidev/porto/pull/191) adds native
lifecycle and new-k3s selection;
[mbianchidev/sql-not-so-lite#75](https://github.com/mbianchidev/sql-not-so-lite/pull/75)
adds opt-in catalog/inspector projections and protected-store exclusion.
Both integration PRs were merged on 2026-11-07 after their existing CI passed.
This does not establish production or complete k3s qualification.
No primary checkout or existing workload is retargeted.

## Implemented service contract

An explicitly enabled consumer discovers the owned service through authenticated
`GET /etcqlite/v1/discovery`. The response distinguishes native snapshots from
inspection projections, reports real resource/schema/build identity, and returns
an ETag. Every native artifact request must carry that exact `If-Match`; a changed
identity returns 412 and missing preconditions return 428.

`POST /etcqlite/v1/snapshots` with `{}` streams a verified native recovery image.
`POST /etcqlite/v1/projections` with `{}` streams a new, metadata-only SQLite
projection. Both require admin access and bounded artifact admission. Optional
raw key/value data requires both `include_data` and `ack_sensitive_data`. Password
hashes, tokens, protocol commands and peer credentials are never projected.

Download native snapshots into private files and verify them with
`etcqlite snapshot status`. Open projections read-only; they are never backups
or writable registrations of the internal `node.db`. Discovery provides no raw
database path and does not authorize installation, startup or mutation.

An offline operator can use:

```sh
etcqlite inspect --data-dir ./stopped-node --output ./metadata.sqlite
etcqlite inspect --data-dir ./stopped-node --output ./consented.sqlite \
  --include-data --ack-sensitive-data
```

Projections are published no-overwrite with mode 0400. Native snapshots are mode
0600. A network consumer must apply equivalent local permissions when saving
streamed bytes. Disabled/missing integrations make no requests and do no external
work; the standalone server does not invoke Porto or sql-not-so-lite.

## Existing behavior

Porto already manages k3s, k0s, kind, containers, and VMs. Its optional
sql-not-so-lite integration discovers valid SQLite files beneath managed
project roots and invokes `sqnsl scan` for eligible roots.

Fresh single-server K3s already uses SQLite through Kine unless another
datastore is configured. Etcqlite must add demonstrable client compatibility,
HA, operational visibility, or developer workflow value rather than merely
renaming that existing storage choice.

sql-not-so-lite manages writable databases and separately inspects discovered
sources read-only. Its current replication is local table-level polling, not
remote consensus, synchronous replication, or HA. That subsystem must not
become Etcqlite's replication mechanism.

## Ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Etcqlite | Cluster authority, mutations, protocol storage, MVCC, leases, auth, verified snapshots | Porto runtime lifecycle or generic SQL editing |
| Porto | Opt-in provisioning, node/service lifecycle, logs, resource ownership, endpoints, cluster datastore configuration | Consensus elections or database repair decisions |
| sql-not-so-lite | Explicitly consented read-only projection inspection | Native/interchange SQL access, direct writes to internal state or election/failover |

Both integrations are optional. A missing or stopped integration cannot
corrupt state or prevent a standalone server from meeting its contract.

## Porto work package

Add an opt-in Etcqlite capability, not an automatic replacement for existing
Kubernetes storage. Preserve current clusters and defaults.

The desired lifecycle contract includes:

- Separately installed matching `etcqlite` and `etcqlite-snapshot` binaries;
  no automatic download.
- Separate data directories and identities for each managed cluster/node.
- Start/stop/status and bounded readiness with meaningful failure messages.
- Client/peer address, certificate, port and storage ownership.
- Persistent logs and resource accounting in existing Porto surfaces.
- An explicit datastore selection only for selected new k3s clusters; existing
  cluster retargeting and migration are outside this implementation.
- A safe native snapshot request before destructive runtime-data operations.

Keep the backing store independent of the Kubernetes cluster that needs it
to boot. A normal Pod requiring that cluster's API is not the bootstrap
mechanism for the cluster's only datastore.

Start with the k3s external etcd endpoint contract. Qualify k0s storage
configuration and kind/kubeadm configuration separately. Do not claim all
Porto providers work because one k3s test passes.

Account for host-to-Lima/container reachability: `localhost` inside a guest
is not the host. Require mTLS for non-loopback control-plane traffic. Keep
SQLite on node-local storage rather than sharing a live WAL file through
host/guest or multi-host filesystem mounts.

The approved implementation uses explicit standalone/three-member host-local
instances, a separate per-cluster mTLS CA, persisted PID/birth/executable
ownership proofs, and `node.lock`-protected offline retention. Native processes
survive Porto daemon exit. A bounded probe from an owned qualification guest
verified `host.lima.internal` readiness and exact discovery identities over mTLS;
it did not change a qualification node, port, service, mount or VM setting.
The native lifecycle profile is macOS/Linux only. One host-local quorum is not
host-level HA, and a readiness probe is not a full k3s-cluster drill.

## sql-not-so-lite work package

The default integration is a consented, read-only diagnostic projection or
verified snapshot view. A trusted local operator may separately opt into
full raw inspection with an explicit sensitive-data warning.

The normal view should expose key metadata, revisions, compaction boundary,
lease state, schema version, and selected operational information. Show raw
key/value contents only with explicit authorization because Kubernetes
Secrets and application credentials can live in the datastore.

Do not expose password hashes, tokens, credential-bearing command payloads,
or peer secrets through the default projection. Do not register Etcqlite's
internal file as an ordinary writable managed database.

The approved inspector integration is disabled by default, pins the identity
ETag, and stores strictly validated immutable projections in a private cache.
Metadata consent is separate from process-local raw-KV consent, which is revoked
on metadata refresh, removal, shutdown and restart. Shared scanner/managed/
CRUD/replication/recovery guards exclude native `node_state`/`app_state`,
inspection `projection_metadata`, and sensitive format-2 `interchange_meta`
schemas even when renamed or manually cataloged; entry rows need not be read to
recognize protected artifacts.

Direct SQL updates would bypass the replicated log, MVCC revisions, lease
indexes, and watches. If editing key/value data is ever offered, it must call
the normal authorized etcd API, not write SQLite.

Current sql-not-so-lite listeners do not provide built-in authentication or
TLS and are not an acceptable exposed administrative path. Gate direct
integration on a secured local boundary or use a sanitized offline
projection. Read-only access is not equivalent to safe network exposure.

A native snapshot must come from Etcqlite's consistent backup interface.
Do not treat sql-not-so-lite's polling replica or ordinary table export as a
recoverable HA snapshot.

## Proposed discovery descriptor

Define a versioned, read-only descriptor only if existing integration hooks
cannot carry the contract. Required information would be resource identity,
cluster/member IDs, approved inspection mode, safe path or endpoint, current
format/API versions, and supported capabilities.

Do not store secrets in a discovery descriptor. Resolve paths under an
approved resource root and verify identity again before opening them.
Discovery alone never authorizes installation, startup, mutation, or restore.

## Integration acceptance scenarios

| Scenario | Expected behavior |
| --- | --- |
| Integration disabled | No install, scan, startup, or storage retargeting |
| No matching resource | No external work and a truthful idle state |
| Binary or certificate missing | Actionable failure without a silent fallback |
| Resource deleted/recreated | Stale identity rejected; no attachment to a reused name |
| Guest endpoint unreachable | Readiness fails with the actual address/boundary |
| Inspector stopped | Database service remains correct and available |
| Inspector requests SQL write | Denied; no direct mutation of internal tables |
| Store stopped | Inspector shows disconnected/stale status rather than fresh success |
| Snapshot failure | Previous verified snapshot remains available |
| Porto restarts | Reconcile existing owned processes; do not start a duplicate node |
| Local three-node demo | Clearly labeled one-host experimentation, not host-level HA |

Work in Porto and sql-not-so-lite uses their own separately authorized focused
branches. The integration PRs are merged; the Etcqlite implementation PR remains
draft. Existing-workload migration, production deployment, CI enablement and
merging the Etcqlite PR remain unauthorized.
