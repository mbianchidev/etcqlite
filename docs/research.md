# Research and primary references

Investigation date: 2026-10-29. These observations informed the plan; they are
not independent production certification of any referenced project.

## Existing implementations

| Project | Verified overlap | Boundary relevant to Etcqlite |
| --- | --- | --- |
| [Kine](https://github.com/k3s-io/kine) | etcd v3 API over SQLite and other backends; used by K3s | README explicitly limits scope to Kubernetes' subset and SQLite to one instance |
| [k8s-dqlite](https://github.com/canonical/k8s-dqlite) | Kubernetes etcd replacement over distributed SQLite | HA uses Raft |
| [Rhino](https://github.com/calfonso/rhino) | Rust etcd facade supporting SQLite and other backends | Source rejects lease keepalive/revoke and many maintenance methods despite broad README language |
| [T4](https://github.com/t4db/t4) | Non-Raft etcd-compatible store with replication, watches, transactions and leases | Uses Pebble and S3-based coordination, not SQLite |
| [rqlite](https://github.com/rqlite/rqlite) | Distributed SQLite and strong consistency options | Uses Raft and SQL/HTTP rather than the etcd API |
| [LiteFS](https://docs.fly.io/litefs/faq) | SQLite replica distribution and failover integration | Async replication can lose recent writes; not an etcd consensus replacement |
| [Addendum](https://github.com/bigeasy/addendum) | Paxos-based etcd-like implementation | README identifies a JavaScript proof of concept for etcd v2, not this v3/SQLite contract |
| [Sqlited](https://github.com/sqlited-org/sqlited) | Existing SQLite-related project using the proposed alternative name | PostgreSQL-wire project; not verified as a full etcd-compatible non-Raft solution |

The combination of SQLite, complete client semantics, native non-Raft HA,
and these integrations was not established as a mature existing product by
this search. That is a bounded search finding, not proof of novelty.

## Important source-level caveats

[Rhino's lease adapter](https://github.com/calfonso/rhino/blob/main/src/server/lease.rs)
returns unsupported errors for revoke, keepalive, TTL and lease enumeration.
Its [maintenance adapter](https://github.com/calfonso/rhino/blob/main/src/server/maintenance.rs)
rejects snapshot, hashes, defragmentation and leadership transfer. Do not use
marketing language as a conformance result.

[T4's consistency document](https://github.com/t4db/t4/blob/main/docs/consistency.md)
describes S3 conditional-write leadership, bounded clock-skew assumptions and
leader-only durability when followers disconnect. Its
[migration matrix](https://t4db.github.io/t4/etcd-migration/) lists unsupported
administrative/transactional features. Its
[benchmarks](https://github.com/t4db/t4/blob/main/docs/benchmarks.md) show mixed
results on a particular Docker/macOS setup, not a universal non-Raft speedup.

## Pinned implementation references

| Reference | Observed baseline |
| --- | --- |
| [etcd release](https://github.com/etcd-io/etcd/releases/tag/v3.7.3) | v3.7.3, published 2026-10-22 |
| [etcd source](https://github.com/etcd-io/etcd/tree/0a173d0410a96d01091b484daae81365bbcde57e) | `0a173d0410a96d01091b484daae81365bbcde57e` |
| [Core RPC definitions](https://github.com/etcd-io/etcd/blob/v3.7.3/api/etcdserverpb/rpc.proto) | 42 RPCs across KV, Watch, Lease, Cluster, Maintenance and Auth |
| [Lock definitions](https://github.com/etcd-io/etcd/blob/v3.7.3/server/etcdserver/api/v3lock/v3lockpb/v3lock.proto) | Two RPCs |
| [Election definitions](https://github.com/etcd-io/etcd/blob/v3.7.3/server/etcdserver/api/v3election/v3electionpb/v3election.proto) | Five RPCs |
| [OmniPaxos](https://github.com/haraldng/omnipaxos/tree/9c772dcce5a9652192f45f67d9d2208b306f3bc9) | Candidate research revision `9c772dcce5a9652192f45f67d9d2208b306f3bc9` |

The inspected OmniPaxos persistent adapter uses RocksDB with explicit synced
writes and an atomic storage-batch interface. A custom SQLite adapter must
preserve those protocol obligations. A memory-backed example or disabled
sync setting is not the desired durability configuration.

## Semantic and performance references

- [etcd API guarantees](https://etcd.io/docs/v3.6/learning/api_guarantees/):
  durability, strict serializability, revisions and watch guarantees. This
  explanatory page is v3.6; use the pinned v3.7.3 source/tests as the authority
  when observable behavior differs.
- [etcd performance](https://etcd.io/docs/v3.6/op-guide/performance/):
  replication round-trip and durable I/O costs; the page's historical
  benchmark numbers are not a current performance baseline.
- [SQLite WAL](https://www.sqlite.org/wal.html):
  single-writer concurrency, checkpoints and same-host filesystem constraints.
- [TigerBeetle safety](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/concepts/safety.md)
  and [VSR internals](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md):
  a serious non-Raft implementation and its engineering scope, not an
  embeddable SQLite consensus library.
- [OmniPaxos project](https://github.com/haraldng/omnipaxos):
  in-development replicated log with pluggable storage/networking.
- [etcd robustness tests](https://github.com/etcd-io/etcd/tree/v3.7.3/tests/robustness):
  reference testing methods; a foreign-server adapter still needs real work.

## Existing integration references

- [Porto integration guide](https://github.com/mbianchidev/porto/blob/main/docs/integrations.md)
  and [integration manager](https://github.com/mbianchidev/porto/blob/main/internal/sqnsl/sqnsl.go):
  opt-in SQLite discovery and `sqnsl scan`.
- [Porto Kubernetes provisioning](https://github.com/mbianchidev/porto/blob/main/internal/kubernetes/provision.go):
  k3s/k0s provisioning boundaries.
- [sql-not-so-lite architecture](https://github.com/mbianchidev/sql-not-so-lite/blob/main/docs/development.md)
  and [replication limits](https://github.com/mbianchidev/sql-not-so-lite/blob/main/docs/usage.md):
  local table-level polling, read-only discovered sources and snapshot limits.
- [K3s datastore documentation](https://docs.k3s.io/datastore):
  default single-server SQLite and external datastore configuration.

Integration observations were checked against the available local project
documentation/source. Recheck the selected remote revisions before any
future implementation; moving `main` links are not immutable baselines.
