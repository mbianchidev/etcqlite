# Verification and performance plan

Status: manual acceptance contract with bounded M0/M1 tests and synthetic
reference baselines implemented. See [development](development.md).
There is no production/performance qualification or CI workflow.

## Current bounded qualification

The installed candidate passes official Go client KV/watch/transaction and
session/mutex/election workflows. A pinned etcd differential compares HashKV
history, repeated compaction and retained-floor behavior without normalizing
away hashes or revision boundaries. That differential found and fixed the
reference's revision-specific keep-set rules; compaction hashes remain a runtime
cache and are cleared on restart, matching the pinned reference.

`tools/conformance/kubernetes_test.go` constructs the real Kubernetes v1.37.2
etcd3 storage adapter and reuses upstream storage assertions for get, create,
conditional/unconditional delete, paging/prefix lists, conflicting updates, TTL
and key schema. It does not run a complete kube-apiserver/provider qualification.
The Go test graph includes upstream reference-server dependencies; the product
and snapshot converter do not use Raft.

```sh
ETCQLITE_SERVER=/path/to/installed/etcqlite \
ETCQLITE_ETCD=/path/to/verified/etcd-v3.7.3 \
go -C tools test -count=1 ./conformance
```

Reference startup can produce transient client retry logs; negative reference
cases deliberately produce duplicate-key/root-precondition errors. Kubernetes
watch cleanup can log context cancellation. These are recorded observations,
not permission to suppress unexplained failures.

The owner authorized three new isolated Linux VMs for qualification, each
4 vCPU/8 GiB with an independent 30-GiB local ext4 disk. The instances use a
checksum-pinned Ubuntu 24.04 ARM64 image, native VZ virtualization, no host/home
mounts, no forwarded SSH agent or proxy credentials, and only loopback client
forwarding. They share one physical host and must not be described as host-level
HA. Primary throughput, long campaigns and recovery results remain unqualified
until their actual evidence exists.

**No CI is authorized.** Run future checks manually in isolated development
workspaces after implementation is requested. Do not enable Actions or add
workflows merely because this document describes repeatable checks.

## Evidence discipline

For every run record source revision, reference version, dependency versions,
configuration, host/storage/network topology, synthetic workload seed,
commands, duration, result artifacts, and known gaps. A pass in one topology
does not qualify another.

Keep evidence outside production data and outside the source tree unless a
specific useful fixture belongs in the eventual project. Never put tokens,
real customer data, control-plane secrets, or private database contents in
test fixtures or published artifacts.

## Verification layers

| Layer | Work | Pass condition |
| --- | --- | --- |
| State-machine unit/property cases | MVCC, binary ranges, counters, transactions, leases, permissions | Invariants hold across generated valid and invalid sequences |
| Reference differential cases | Replay deterministic cases against pinned etcd and Etcqlite | Observable responses, revisions, errors and events agree after justified nondeterminism normalization |
| Official client compatibility | Official Go client, concurrency helpers, gateway clients, pinned tools | Named client workflows pass without application changes |
| Kubernetes storage | Upstream storage/apiserver cases through a compatible harness | Required behavior passes without weakening upstream assertions |
| Protocol simulation | Real protocol integration with deterministic network/disk/process faults | No divergent decided state, acknowledged data loss, or stale default read |
| External history checking | Register/CAS, transactional read-write, list append, watch histories | Linearizability/strict-serializability and watch checks pass |
| Recovery qualification | Snapshot/import/export/restore under fault | Verified state and metadata at the declared recovery point |
| Integration acceptance | Porto and inspector contracts | Ownership, consent, reachability and read-only boundaries hold |
| Manual release qualification | Install/upgrade/downgrade and operational drills | Repeatable operator-visible behavior, no unresolved blockers |

Reuse upstream semantic tests where practical. The etcd robustness framework
may assume etcd-specific process controls and fault hooks; build a truthful
adapter or an equivalent harness rather than pretending it runs unchanged.
Do not strip assertions to make a foreign server pass.

## Required deterministic cases

| Group | Cases |
| --- | --- |
| Binary API | Zero bytes, invalid UTF-8, empty values, all-0xff prefixes, open-ended ranges |
| MVCC | Starting revision, create/update/delete/recreate, version counters, overflow |
| Transactions | All comparison operators/targets, missing keys, nested branches, duplicates, read-your-writes, full rollback |
| History | Historical pagination, future revisions, compaction floor, tombstone retention |
| Watches | Replay/live race, reconnect, complete revisions, progress, filters, prev_kv, fragmentation, slow consumers |
| Leases | Explicit/generated IDs, TTL policy, reassignment, renewal/revoke/expiry races, restart |
| Auth | Root rules, every permission type/range, token expiry, revocation with in-flight RPCs and streams |
| Membership | Add voter/learner, catch-up, promote, update, remove, transfer, interruption |
| Maintenance | Quota alarms, storage sizes, hashes, real reclamation, snapshots, downgrade policy |
| Cancellation | Client timeout before proposal, after decision, during replay, during backup |
| Resource limits | Full queues, max request/txn/watch limits, large values, memory pressure |

Comparisons must preserve real uncertainty. Do not normalize away revisions,
atomicity errors, lost events, byte ordering, authorization decisions, or
uncommitted data. Cluster/member IDs and scheduling-dependent timings may
need explicit normalization.

## Failure campaign

Inject crashes immediately before and after each persistence and
acknowledgment boundary. Cover delayed/failed fsync, disk full, short writes,
truncation, detectable corruption, and repeated restart.

Exercise 1/2 partitions in three voters, asymmetric reachability, dropped and
duplicated messages, old-leader pauses, delayed accepted messages, snapshot
installation, learner promotion and configuration transitions. Restore
connectivity and require eventual progress once the documented availability
conditions hold.

Add wall-clock jumps and monotonic-timer scheduling delays to lease tests.
Keepalive during a partition is mandatory; checking only grant/revoke is
insufficient.

External history checking should include at least CAS-register,
transactional multi-key read/write, list-append, set, and watch-reconnect
workloads. Start with seeded short runs, then qualify longer runs and retain
all histories, including failed ones.

Proposed minimum candidate gate: 24 hours of aggregate seeded fault testing
across the declared matrix, plus at least one continuous six-hour mixed
workload/failure run. These are engineering thresholds, not mathematical proof.

## Benchmark fairness

Use pinned etcd v3.7.3 and the candidate build with:

- Equal three-voter topology, resource limits, storage class and network.
- Durable quorum acknowledgments and default linearizable reads.
- Matching key/value sizes, retained history, compaction cadence and auth/TLS.
- Equivalent client connection/concurrency settings and load generator.
- Separate client resources so neither server gains from client contention.
- Full required synchronization; no unsafe memory-only or async-ack shortcut.

Compare single-node with single-node separately. A local SQLite prototype
beating a three-node etcd cluster is not a valid consensus comparison.
Porto's one-host demo is not the primary HA benchmark topology.

## Predeclared primary workload

The following is a proposed benchmark specification to freeze at M0:

| Parameter | Value |
| --- | --- |
| Server topology | Three separate Linux hosts or VMs with independent durable volumes; document shared failure domains |
| Per-server budget | 4 vCPU, 8 GiB RAM, same SSD class and filesystem |
| Dataset | 100,000 live keys, 64-byte keys, 1,024-byte values |
| Retained history | 1,000,000 mutations before equivalent compaction |
| Requests | 60% single-key linearizable Range, 10% ten-key prefix Range, 20% Put, 10% CAS Txn with one Put |
| Distribution | Seeded uniform keys; hotspot variation is a separate result |
| Watches | 100 prefix streams; fixed prefixes cover 10% of keys |
| Security | Same client/peer TLS posture; authenticated workload variant reported separately |
| Runs | Five fresh paired repetitions; alternate server order |
| Timing | At least five-minute warmup and fifteen-minute measured steady state per sample |
| Load | Sweep offered concurrency/rate to find stable capacity, not just maximum briefly accepted traffic |

Match the 60/10/20/10 mix exactly. Report Range, Put, and Txn latency separately;
an aggregate read-heavy percentile cannot hide write regressions.

## Performance gate

Before measurement, freeze per-operation latency budgets from the reference
qualification and record them. Sustainable throughput must maintain bounded
queues, steady-state resource use, and no unexpected errors.

The proposed "faster" gate requires:

- At least 1.20 times reference sustainable throughput on the primary workload.
- Per-operation p99 latency no worse than reference at the common comparison
  point, and compliance with the frozen budgets at qualified capacity.
- No lost acknowledged writes, invalid transactions, stale default reads, or
  broken watch guarantees in accompanying correctness runs.
- The same configured resource budgets and durability.
- A repeatable advantage beyond run-to-run variance, not one favorable sample.

Report p50/p95/p99/p99.9, throughput, errors/timeouts, CPU, RSS, disk writes,
fsync counts/latency, SQLite WAL growth, replication lag, watch lag, and
failover interruption. Include sequential writes, write-heavy, read-heavy,
hotspot, large-value and high-watch-count secondary workloads.

Missing the performance gate is a stop/go decision, not permission to change
the workload, disable synchronization, or rename weak reads as linearizable.
An operationally simpler SQLite store may still be useful, but that is a
different product justification requiring review.

## Claim and release rules

No correctness result proves all executions. Publish topology, assumptions,
remaining gaps and failed experiments with any performance claim. Passing
manual checks does not enable CI, authorize production deployment, or justify
silently changing the stated compatibility boundary.
