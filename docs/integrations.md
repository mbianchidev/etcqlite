# Porto and sql-not-so-lite integrations

Status: proposed cross-repository contracts. This repository does not change
either project or authorize work in their primary checkouts.

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
| sql-not-so-lite | Explicitly consented read-only inspection and approved projection/snapshot browsing | Direct writes to internal state or election/failover |

Both integrations are optional. A missing or stopped integration cannot
corrupt state or prevent a standalone server from meeting its contract.

## Porto work package

Add an opt-in Etcqlite capability, not an automatic replacement for existing
Kubernetes storage. Preserve current clusters and defaults.

The desired lifecycle contract includes:

- Pinned, checksum-verified executable acquisition after explicit enablement.
- Separate data directories and identities for each managed cluster/node.
- Start/stop/status and bounded readiness with meaningful failure messages.
- Client/peer address, certificate, port and storage ownership.
- Persistent logs and resource accounting in existing Porto surfaces.
- An explicit datastore selection only when creating or deliberately migrating
  a cluster; never retarget an existing cluster on discovery.
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

Work in Porto and sql-not-so-lite must use their own focused branches and
draft PRs after separate authorization. No cross-repository PR is created
as part of this documentation delivery.
