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

The first full primary candidate run at `c626c41271683cd383ac8f086a0aa8a67b09d93a`
failed: the first guest's server was OOM-killed with 7,786,568 KiB anonymous RSS.
The client run was stopped and no successful primary percentile/throughput
result was emitted. The two surviving processes later had approximately
11-13 MiB anonymous RSS; their roughly 6-GiB cgroup accounting was predominantly
reclaimable file cache, not equivalent heap growth. Data and failure evidence
were retained. Subsequent serialization/snapshot changes require a fresh
same-budget run, not a larger-memory pass or a rewritten success report.

The bounded-transport candidate restarted the preserved three-voter dataset
without resetting identities or promises: all nodes converged at revision
37,456 and applied index 144,095. Observed restart RSS stayed below 223 MiB.
This is a recovery/readiness probe, not a completed primary workload run.
Large HashKV calls then exposed the bit-at-a-time CRC and owned-writer timeout;
hash scans now use a pinned read-only WAL view off the writer and accelerated
Castagnoli CRC with the same reference encoding. Large-tool calls still need
an appropriate client deadline; the exact primary and recovery gates remain open.

The first complete primary candidate sample at
`1fec3e7f43c73826f9a88f0479fcb2ed2f536b5f` passed the original resource limits:
100,000 live keys, 1,000,000 seeded mutations, 100 prefix watches and 32 client
workers. Actual warmup was 311.33 seconds and measurement 927.31 seconds, with
zero request, warmup or watch errors. Throughput was 55.65 operations/second;
per-operation p99 was approximately 1.69-1.88 seconds. Each server's observed
VmHWM stayed below 25 MiB. The prior OOM result remains retained.

All three replicas subsequently agreed on the exact retained-history HashKV
value at revision 34,934. A live native image of 5,907,914,784 bytes was captured
and fully verified, then translated into a 2,414,034,944-byte bbolt image accepted
by pinned etcdutl v3.7.3 and a private disk-backed interchange. Subsequent fresh
native and real upstream restores agreed on HashKV 1,758,762,700 at revision
35,154. Native import took 18.13 seconds and 25,472 KiB peak RSS.

The real upstream destination then accepted binary, delete/recreate, lease and
auth changes. After fencing it, a new snapshot and disk-backed reverse import
retained all 100,087 live synthetic keys and exact historical/generation, auth
and lease assertions. Native and upstream HashKV agreed at 1,974,551,075,
revision 35,159 and compact revision -1. Reverse import took 16.45 seconds and
25,472 KiB peak RSS. Both new native images were fully verified and retained.
This establishes one large isolated application cutover and post-write rollback,
not a complete RPO/RTO campaign, multi-voter recovery qualification or speed win.

The candidate report retained only two server-version strings due to a reporting
bug; live discovery/status separately verified all three exact candidate
identities. The reporting bug is fixed in the subsequent tooling work.

The `tools/qualification` runner seeds synthetic 64-byte keys/1024-byte values
through durable API transactions, starts fixed prefix watches, and issues exact
100-operation cycles with the frozen 60/10/20/10 mix. It records exact per-operation
latencies, including p99/p99.9, errors and warmup errors in new mode-0600 JSON
reports. It refuses a nonempty fixture namespace, HTTP endpoints, unbounded
workers, or silent latency sampling. Smaller/shorter probes are explicitly
`primary_workload_shape: false`, never production evidence.

Reports reserve a new private output before contacting the fixture. Setup,
seeding, warmup and measurement failures retain their phase, seeded mutation
count, actual elapsed timing and failure text. SIGINT/SIGTERM stops new requests
and preserves an incomplete report; exact-sample overflow stops the workload
without publishing truncated percentiles. Only completed runs with the actual
primary timing can carry the primary-shape flag. This is evidence retention,
not an external linearizability checker.

```sh
GOMAXPROCS=2 go -C tools run ./qualification \
  --endpoints https://node1:2379,https://node2:2379,https://node3:2379 \
  --ca ./fixture-ca.crt --cert ./fixture-client.crt --key ./fixture-client.key \
  --engine <exact-build> --output ./new-report.json --owned-fixture
```

Primary candidate/reference nodes use equal explicit 8-GiB quota settings
alongside their 4-vCPU/8-GiB process budgets. The larger quota permits the
predeclared retained-history dataset without changing the durability/read
contract. Sustainable capacity sweeps, five alternating paired samples, full
fault/history checks and topology disclosure remain required; one successful
probe does not meet the faster/production gate.

## Bounded external histories

`tools/qualification --correctness` uses pinned Porcupine v1.3.1 models for
register/CAS, atomic two-key read/write, append and set operations. It records
bounded 512-operation epochs across 16 independent groups with at most 32 workers.
Timed-out writes remain uncertain, including possible later committed effects;
they are never silently treated as aborted. Unary SDK calls make one attempt,
so a successful response cannot hide replayed mutations. Logical append still
uses explicit compare-and-swap attempts within its bounded request deadline.

The separate watch oracle checks revision order, duplicates, exact previous KV,
create/version/delete metadata, every acknowledged mutation, and explanations
for observed uncertain effects. Successful reads and no-op outcomes must agree
with the watch replay at their reported revision. The final range is pinned by
its fence revision; later observed pending effects are retained rather than
discarded. Reconnection occurs only after a complete response, never midway
through an atomic revision.

Private format-2 JSONL records preserve requested and actual monotonic timing,
operations, watch evidence, transient interruptions, epoch checks and terminal
completion/failure. Rechecking rejects unknown/duplicate/null fields, missing
required fields, wrong array shapes, nonprivate or changing files, missing or
repeated epochs/checks, and trailing records. Limits are 8 MiB per record and
16 GiB per streamed file; capture enforces them too. Every epoch has a 30-second
external-check deadline. An `Unknown` result is failure, not a pass.

A real three-voter host-local smoke trace recorded 512 operations, 267 watch
events and two reconnects. Both independent oracles accepted the finite trace,
with 12 deadline-exceeded outcomes retained as uncertain (11 append, one CAS).
The installed host build was labeled dirty. This checks the tooling, including
uncertainty; it is not a healthy availability, Linux fault campaign, or
production qualification result. Legacy format-1 probe evidence remains retained
but is not accepted by the stronger format-2 rechecker.

```sh
GOMAXPROCS=2 go -C tools run ./qualification --correctness \
  --endpoints https://node1:2379,https://node2:2379,https://node3:2379 \
  --ca ./fixture-ca.crt --cert ./fixture-client.crt --key ./fixture-client.key \
  --engine <exact-build> --seed 1002 --concurrency 16 --duration 10m \
  --output ./new-history.jsonl --owned-fixture

go -C tools run ./qualification --check-history ./new-history.jsonl
```

Namespace reuse and output overwrite are refused. The console reports uncertain
operation counts explicitly. A successful finite check is not an availability
or performance result, and does not inject or qualify any faults by itself.

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
