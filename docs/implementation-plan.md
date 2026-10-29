# Full implementation plan

Status: design only. **Do not execute these milestones under the current
request.** The authorized delivery is a private repository of documentation
with no code and no CI.

## Outcome

Build an etcd client/API-compatible service whose state is backed by SQLite,
whose clustered authority does not use Raft, and whose optional Porto and
sql-not-so-lite integrations improve the local development workflow.

Production candidacy and a speed claim are separate outcomes with separate
evidence. The plan does not assume either is achievable on a fixed schedule.

Read [requirements](requirements.md), [API compatibility](api-compatibility.md),
and [decisions](decisions-and-risks.md) before starting implementation.

## Dependency map

```text
M0 scope + feasibility
       |
M1 deterministic local storage
       |
M2 local API semantics -----------+
       |                         |
M3 durable replicated core       |
       |                         |
M4 distributed semantic closure  |
       |                         |
M5 administration + tooling <----+
       |
M6 recovery + migration
       |
M7 Porto + inspection integration
       |
M8 performance + qualification
```

Early performance experiments begin during M0/M3, not only at M8. Formal
acceptance waits until equivalent semantics and durability exist. Security,
diagnostics, documentation, and manual tests accompany every milestone.

## M0: approve the contract and prove the risky interfaces

**Prerequisites:** separate authorization for implementation; owner approval
of the proposed scope, platform, stack, and performance objective.

| Work package | Deliverable | Exit evidence |
| --- | --- | --- |
| M0.1 Scope freeze | Approved meaning of drop-in, supported versions/platforms, exclusions, claim levels | Decision record distinguishes user requirements from proposals |
| M0.2 Reference harness | Pinned etcd, client, tool and Kubernetes versions; synthetic fixture protocol | Baseline responses and capabilities reproducible manually |
| M0.3 Prior-art baseline | Kine local behavior and etcd comparison, without adopting Kine's subset as full scope | Reproducible baseline and known semantic gaps |
| M0.4 Consensus spike | SQLite-backed protocol batches, crash persistence, ordered barrier | C01/C02/C06/C07 evidence from the consensus gates |
| M0.5 Membership spike | Learner catch-up, promotion, reconfiguration, transfer | C03/C04/C05/C08 evidence; no claim that library supports them by default |
| M0.6 Tooling spike | Snapshot/hash/downgrade feasibility and version/metadata policy | Approved converter approach or an explicit blocking gap |
| M0.7 Threat/resource model | Listener trust, data sensitivity, queue and storage ownership | Reviewed operational assumptions and failure model |
| M0.8 Dependency review | Immutable protocol revision and dependency/license/maintenance policy | C09 evidence and reproducible dependency proposal |

**Stop/go:** do not scaffold the full product until mandatory protocol and
tooling gaps have a credible solution. A failed gate returns to design;
it does not authorize Raft or a weaker contract.

## M1: deterministic SQLite state machine

**Depends on:** M0 scope, storage/persistence decisions, and command format.

Implement node ownership, explicit initialization, schema versioning, binary
types, counters, current keys, version history, leases/auth metadata skeleton,
and atomic application boundaries. No externally successful stub methods.

| Work package | Deliverable | Exit evidence |
| --- | --- | --- |
| M1.1 Persistence boundary | Dedicated writer, short read snapshots, WAL/checkpoint policy, full sync | Restart and flush-failure cases preserve declared boundaries |
| M1.2 MVCC | Binary keys, create/mod/version semantics, tombstones, historical reads | Reference-derived unit/property cases |
| M1.3 Transactions | Compare/branch/nesting, range operations, one revision per modifying txn | Atomicity and error cases agree with reference |
| M1.4 Compaction | Correct floor preservation and separate physical maintenance | No resurrected keys or invalid supported history |
| M1.5 Command/apply ledger | Versioned commands, applied index, deterministic results | Replaying decided entries cannot repeat effects |
| M1.6 Limits/errors | Quotas, bounds, overflow and SQLite failure translation | No silent fallback or partial mutation |

**Exit:** deterministic, crash-recoverable local semantics without a transport.
All fixtures are synthetic. Record storage amplification and flush costs.

## M2: complete local client semantics

**Depends on:** M1. Network security and bounded admission start here, not as
a final patch.

| Work package | Deliverable | Exit evidence |
| --- | --- | --- |
| M2.1 Wire/admission | Pinned protobuf, gRPC, bounded queues, deadlines/cancellation | Real official client requests and negative cases |
| M2.2 KV | A01-A06 including RangeStream and nested transactions | Differential request/response/history corpus |
| M2.3 Watch | A07 replay/live handoff, filters, progress, fragmentation | No missing, duplicate, reordered or partial-revision events |
| M2.4 Lease | A08-A12, expiry through deterministic mutations | TTL, ownership and restart evidence |
| M2.5 Auth | A26-A42 | User/role/token/range-permission and in-flight revocation cases |
| M2.6 Concurrency helpers | Lock and Election | Official helpers and direct RPC workflows |
| M2.7 Gateway/health | HTTP encoding, streams, liveness/readiness/version surfaces | Pinned gateway clients and truthful startup state |

**Exit:** honest local-prototype label with exact coverage. Do not claim
distributed safety. Compare resource use against the M0 baseline.

## M3: durable non-Raft replicated core

**Depends on:** M1 command/apply model and M0 protocol spike; M2 establishes
the transport and client outcomes used for acceptance.

Implement the protocol adapter, authenticated peer transport, durable atomic
storage interface, proposal routing, decided-entry application, recovery,
snapshot catch-up, and ordered read barriers.

| Work package | Deliverable | Exit evidence |
| --- | --- | --- |
| M3.1 Peer identity | Cluster/member validation and authenticated transport | Wrong-cluster, stale-ID and unauthenticated peers rejected |
| M3.2 Persistence integration | Durable promises, accepted commands, decided boundary, reconfiguration state | Crashes at every persistence/message boundary |
| M3.3 Request routing | Leader forwarding, bounded retries, uncertain outcomes | No duplicated internal retry effects or fabricated aborts |
| M3.4 Read authority | Logged barriers and applied-frontier wait | Isolated old leaders cannot serve default stale reads |
| M3.5 Recovery | Replay, snapshot validation, retained suffix and catch-up | Lost/corrupt replica can recover without invalid votes |
| M3.6 Quorum availability | Conventional three-voter behavior | One unavailable voter tolerated; insufficient quorum refuses new authority-dependent work |

**Exit:** three-node clustered-experimental evidence with no successful
leader-only fallback. Re-run local semantics through the replicated path.
Measure preliminary latency/throughput and retain all failures.

## M4: distributed semantic closure

**Depends on:** M2 and M3.

Resolve the behaviors that a generic replicated KV demonstration does not
cover: auth revocation during queued/forwarded operations, watches across
leadership changes, compaction frontiers, lease renewal/expiry during
partitions, streaming cancellation, Lock/Election ownership, and overload.

Run the external history checker and deterministic fault campaign. Compare
default linearizable reads and explicit serializable reads separately.

**Exit:** no known semantic divergence for the implemented client API under
the declared crash/partition model. Keep the broad compatibility label
blocked until administration and tooling also qualify.

## M5: membership, maintenance, and tooling

**Depends on:** M0 operational/tooling decisions, M3, and M4.

| Work package | Deliverable | Exit evidence |
| --- | --- | --- |
| M5.1 Membership | A13-A17, real learners, endpoints, catch-up, promotion, removal | Faulted transitions preserve quorum and acknowledged outcomes |
| M5.2 Transfer | A24, safe leader handover | Request completes only with the promised transition |
| M5.3 Status/alarms | A18/A19 with real IDs, versions, positions and quota/corruption state | Official tool output matches the declared metadata policy |
| M5.4 Reclamation/hashes | A20-A22 with actual physical work and reference hash inputs | Cross-implementation checks where promised |
| M5.5 Snapshot stream | A23 and isolated converter if necessary | Approved upstream save/status/restore workflows |
| M5.6 Version control | A25 plus mixed-version and downgrade policy | No no-op success or incompatible format emission |
| M5.7 Inventory closure | All 49 unique RPCs and gateway obligations | Mechanical inventory and conformance evidence; no duplicate rows masking omissions |

**Exit:** client API/tooling compatibility labels only for the exact matrix
that passes. If a method remains impossible, stop the broad claim and seek a
scope decision rather than hiding an unsupported response.

## M6: recovery and reversible migration

**Depends on:** M5 snapshot/tooling and membership behavior.

Implement and document native consistent snapshots, atomic publication,
retention, isolated restore, member replacement, whole-cluster recovery,
both-direction application-state translation, and fenced cutover.

Prove binary fidelity, revisions/compaction, auth, leases, identity handling,
Kubernetes cache invalidation, and post-write rollback. Measure restore time
and recovery point on declared dataset sizes.

**Exit:** isolated end-to-end migration and reverse recovery with synthetic
Kubernetes-like data. No live source is touched without separate approval.

## M7: optional integrations

**Depends on:** stable lifecycle/snapshot/inspection contracts from M5/M6.
Any modifications to the two integration repositories require their own
explicitly assigned work.

| Work package | Deliverable | Exit evidence |
| --- | --- | --- |
| M7.1 Porto lifecycle | Opt-in binary/node ownership, logs, readiness and certificates | Disabled state does nothing; restarts do not create duplicate nodes |
| M7.2 Porto datastore selection | k3s external endpoint first; separately qualified providers | Existing clusters/defaults preserved and guest reachability verified |
| M7.3 Inspector boundary | Sanitized read-only projection and explicit sensitive-data consent | No direct SQL mutation or unauthenticated administrative exposure |
| M7.4 Snapshot integration | Verified native snapshots, not polling replicas | Interrupted backup keeps the prior verified artifact |
| M7.5 Cross-project documentation | Setup, lifecycle, limitations, recovery ownership | README stays minimal; detailed usage lives in existing docs directories |

**Exit:** standalone behavior remains intact when either integration is
disabled, missing, stopped, or disconnected.

## M8: performance and production-candidate qualification

**Depends on:** all required semantic, recovery, security and integration
gates. Unsupported platforms/integrations remain explicitly excluded.

Freeze the benchmark configuration before the final comparison. Use the
[verification protocol](verification-and-performance.md), paired samples,
per-operation tail latency, durability-equivalent configurations, and
multi-host failure-domain disclosure.

Optimize only demonstrated bottlenecks. Any change to acknowledgment timing,
read authority, log trimming, application batching or history retention
reopens its correctness/failure gates.

**Exit requirements:**

- Complete in-scope compatibility inventory with no success-shaped stubs.
- Passing recorded semantic, protocol, recovery, security and operator drills.
- Manual install and upgrade checks on each supported platform.
- A documented dependency vulnerability review and unresolved-warning record.
- No known lost acknowledged writes, stale default reads or invalid watches.
- Measured recovery objectives and operational runbooks.
- Performance claims limited to workloads that meet the frozen gate.
- No unexplained failure, open blocking review feedback, or hidden scope gap.

Missing the speed target requires an explicit product decision. Missing
correctness or durability blocks production candidacy outright.

## Workstream ownership and sequencing

Use named human owners when staffing is known. One person may own multiple
workstreams, but every gate needs an accountable reviewer.

| Workstream | Main responsibility | Dependencies |
| --- | --- | --- |
| Protocol/storage | Durable adapter, simulation, recovery, reconfiguration | Approved M0 contract |
| Client semantics | MVCC, all RPCs, streaming, auth and errors | M1 deterministic apply |
| Tooling/recovery | Hash/snapshot compatibility, conversion, cutover | M0 spike and M5 surfaces |
| Integrations | Porto lifecycle and inspector boundary | Stable service/snapshot contracts |
| Qualification | Differential/fault/performance evidence and runbooks | Accompanies every milestone |

Do not estimate a production completion date before M0. The snapshot,
membership and protocol-maintenance spikes can change the size of the project
substantially. This is not a thin SQL wrapper once broad HA compatibility is
required.

## Repository and change discipline

The present repository stays docs-only until implementation is requested.
No automatic CI or release automation is part of the plan. Future manual
commands and useful test helpers belong in the authorized implementation
work, not this delivery.

Keep feature changes on working branches and in draft PRs. Commit with the
configured human author, no tool attribution and no GPG signing. Do not merge,
push directly to main, stack PRs, or change other repositories without the
necessary authorization.
