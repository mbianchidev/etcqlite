# Alternatives and positioning

Evidence date: 2026-10-29. Implementation started on 2026-11-07. Every Etcqlite
capability below is a target, not a delivered feature or benchmark result.
See [development](development.md) for actual bounded implementation evidence.
This comparison does not change the [requirements](requirements.md).

## Proposed unique selling point

**Etcd-compatible control-plane state with SQLite visibility, self-contained
quorum HA, and a first-class local development workflow.**

The potential differentiation is the combination:

| Target | User benefit | Why it is not unique by itself |
| --- | --- | --- |
| Broad etcd v3 semantics, not just Kubernetes' subset | Keep existing clients, leases, watches, transactions and concurrency helpers | etcd already provides the contract; other adapters implement portions |
| SQLite-backed state and approved read-only snapshots/projections | Diagnose revisions, history and leases with familiar SQLite tooling | Kine, dqlite and rqlite already use SQLite |
| Standalone non-Raft quorum replication | No mandatory external SQL database, object-store election service or coordinator | Other self-contained consensus systems exist; a different protocol is not automatically easier or faster |
| Porto lifecycle plus sql-not-so-lite inspection | One explicit workflow for provisioning, endpoints, logs, inspection and recovery | Integration is useful packaging, not an exclusive technical invention |
| Same semantics in local and clustered modes | Reproduce client behavior locally before changing topology | Needs conformance evidence; local SQLite testing tools already exist |

"Self-contained" means no mandatory external datastore or election authority.
It does not mean no cluster to operate: HA still needs replicas, durable
storage, certificates, quorum management and recovery procedures.

The initial audience is platform and control-plane developers, especially
those already using Porto. Outside that ecosystem, the product needs a
measurable workflow or operational advantage, not just branded integrations.

## Direct client-facing choices

These are the closest choices when the application already speaks etcd.
API reach and failure contracts matter more than implementation language.

| Project | Storage and coordination | Client/API boundary | Prefer it when |
| --- | --- | --- | --- |
| [etcd](https://github.com/etcd-io/etcd) | bbolt-backed state with Raft replication | The reference contract; Etcqlite targets v3.7.3 client behavior, not its peer protocol | You need an existing implementation of the contract rather than a new storage model |
| [Kine](https://github.com/k3s-io/kine) | Local SQLite, or other backends that supply their own storage/HA behavior | Explicit Kubernetes-required v3 subset; SQLite supports a single Kine instance | Your scope is Kubernetes storage, particularly single-server local/edge use |
| [k8s-dqlite](https://github.com/canonical/k8s-dqlite) | Distributed SQLite through dqlite; Raft HA | Kubernetes-focused etcd replacement; do not infer generic client coverage from that label | You want existing distributed SQLite for Kubernetes and Raft is acceptable |
| [Rhino](https://github.com/calfonso/rhino) | SQLite, PostgreSQL, MySQL or Redis adapters | README claims broad v3 compatibility, but inspected lease/maintenance adapters reject important operations | Your required methods are explicitly verified against its implementation |
| [T4](https://github.com/t4db/t4) | Pebble, peer WAL replication and S3 conditional-write leadership | Broad core v3 interface; published matrix retains transaction/administrative gaps | Its object-store authority/recovery model and supported API scope fit your requirements |

### Kine

The [README](https://github.com/k3s-io/kine/blob/master/README.md) explicitly
says Kine is not intended as a generic etcd server. SQLite cannot be shared
between multiple Kine instances, even through a common file or shared mount.
HA is possible through supported external backends, not native replication of
that SQLite file.

Etcqlite's proposed difference is broader verified client semantics plus
native quorum replication. If the real need is only single-server K3s
storage, Kine already solves it. Wrapping Kine with a new name is not a USP.

### k8s-dqlite

The [README](https://github.com/canonical/k8s-dqlite/blob/master/README.md)
already describes a lightweight distributed SQLite etcd replacement for
Kubernetes, with automatic HA through Raft.

This is the strongest counterexample to claiming "SQLite plus HA" is novel.
Etcqlite would still need to demonstrate a broader verified client contract,
better relevant operator/developer workflows, or measured resource/performance
advantages. Merely replacing Raft does not establish a user benefit.

### Rhino

The inspected [lease implementation](https://github.com/calfonso/rhino/blob/main/src/server/lease.rs)
rejects revoke, keepalive, TTL and lease enumeration. Its
[maintenance implementation](https://github.com/calfonso/rhino/blob/main/src/server/maintenance.rs)
rejects snapshots, hashes and other operations.

Etcqlite can aim to differentiate on honest, complete conformance evidence.
That is a target, not a claim that Etcqlite is currently more capable.
Recheck the source before repeating these gaps after the evidence date.

### T4

T4 is the closest alternative to the "etcd without Raft" story. It should be
treated as a serious comparator, not dismissed because it is not SQLite.

Its [consistency document](https://github.com/t4db/t4/blob/main/docs/consistency.md)
describes S3 conditional-write leader coordination, bounded clock-skew
assumptions, and leader-only durability after followers disconnect. It also
documents its own fault-testing evidence and known gaps. These are stated
design tradeoffs, not an independent conclusion that the project is unsafe.

Etcqlite's proposed difference is a SQLite-backed, self-contained quorum
authority that refuses automatic durability downgrade when quorum is lost.
That removes mandatory object-store coordination from this design, not all
infrastructure or all failure assumptions.

T4's [compatibility matrix](https://t4db.github.io/t4/etcd-migration/) is the
source for its supported methods and intentional gaps. Its
[published benchmarks](https://github.com/t4db/t4/blob/main/docs/benchmarks.md)
have workload-dependent wins and losses; they do not prove a universal
advantage from avoiding Raft.

## Adjacent SQLite and workflow alternatives

These solve related needs but are not interchangeable etcd v3 servers.
Unknown properties are not marked as absent or safe.

| Project | What it provides | Why it is not the same product | Prefer it when |
| --- | --- | --- | --- |
| [rqlite](https://github.com/rqlite/rqlite) | Replicated SQLite, SQL/HTTP APIs, backups and tunable consistency | Uses Raft; an etcd API, MVCC/watch/lease semantics and adapters would still be needed | Your application can use SQL and wants a distributed SQLite service |
| [dqlite](https://github.com/canonical/dqlite) | Embeddable distributed SQLite library with failover | Uses Raft and its own SQL protocol; k8s-dqlite supplies the Kubernetes-facing layer | You are embedding a SQL engine and accept its protocol/platform constraints |
| [LiteFS](https://docs.fly.io/litefs/faq) | SQLite primary/replica distribution, local reads and lease-based failover integration | Async replication has a possible recent-write-loss window; not an etcd v3 API or equivalent quorum commit contract | Your SQLite application accepts that replication/failure model |
| [Litestream](https://litestream.io/how-it-works/) | Async SQLite WAL/LTX backup and restore | Disaster recovery, not an authoritative multi-node etcd service | You need backups for an existing single-writer SQLite application |
| [k8senv](https://github.com/giantswarm/k8senv) | Real kube-apiserver tests backed by Kine/SQLite, with pooling and namespace isolation | A development/testing workflow, not an HA generic etcd replacement | Your goal is faster isolated Kubernetes API-level tests |
| [Sqlited](https://github.com/sqlited-org/sqlited) | Experimental PostgreSQL-wire access to SQLite, with published replication/failover goals | Not an etcd v3 service; its HA claims were not independently qualified here for quorum/partition behavior | You specifically want its PostgreSQL-wire SQLite model and qualify its limits |
| [Addendum](https://github.com/bigeasy/addendum) | Paxos-based etcd-like JavaScript proof of concept | Its README identifies the etcd v2 API, not this v3/SQLite contract | You want to study a Paxos-based reference/prototype |

The [rqlite dependency manifest](https://github.com/rqlite/rqlite/blob/master/go.mod)
identifies its Raft integration. Its SQL interface is not a defect; it is a
different application contract.

The [dqlite README](https://github.com/canonical/dqlite/blob/master/README.md)
explicitly describes Raft replication and an embeddable SQL engine without
external database dependencies.

The [LiteFS FAQ](https://docs.fly.io/litefs/faq) describes asynchronous
replication and Consul-based distributed leases for failover. Do not equate
"LiteFS does not embed Raft itself" with a guarantee of no external consensus.
The same documentation currently warns that LiteFS is outside Fly.io's
support scope.

[Litestream's design](https://litestream.io/how-it-works/) explicitly calls its
replication asynchronous disaster recovery. Recoverable backup progress is
not the same as a quorum acknowledgment for a live coordination operation.

The [k8senv README](https://github.com/giantswarm/k8senv/blob/main/README.md)
shows that SQLite-based local Kubernetes developer workflows already exist.
Etcqlite cannot claim that replacing etcd with SQLite for tests is new.

## Building blocks, not drop-in alternatives

| Project | Relevance | Missing piece for this product |
| --- | --- | --- |
| [OmniPaxos](https://github.com/haraldng/omnipaxos) | Candidate non-Raft replicated log with pluggable storage/networking | SQLite persistence adapter, complete etcd semantics, membership/tooling qualification and operations |
| [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle) | Production precedent for VSR and rigorous failure engineering | It is a specialized accounting database, not an etcd API or a generic SQLite consensus library |

These are sources of implementation ideas and protocol evidence. Neither
turns a SQLite file into an etcd replacement by configuration alone. See
[consensus](consensus.md) for the actual candidate selection gates.

## What is defensible to say

| Claim | Current status | Evidence required before shipping language |
| --- | --- | --- |
| "Etcd client-compatible" | Planned | The pinned 49-RPC contract, error/stream semantics and approved client/tool workflows pass |
| "SQLite-backed and inspectable" | Planned | Approved snapshot/projection inspection works without direct SQL mutation or secret leakage |
| "HA without external datastore/election service" | Planned | Quorum/partition/reconfiguration/recovery qualification with no hidden external authority |
| "Integrated with Porto and sql-not-so-lite" | Planned | Opt-in lifecycle and secured read-only inspection contracts actually work |
| "Faster than etcd" | Unproven | Frozen, durability-equivalent comparisons meet the predeclared performance gate |
| "Simpler to operate" | Unproven | Measured provisioning, troubleshooting and recovery improvements at equivalent guarantees |
| "The only implementation with these properties" | Not established | A bounded repository/web search is not proof of exclusivity |

Do not lead with "a faster consensus algorithm." Non-Raft is an implementation
constraint until a measurable benefit is established. Durable storage,
networking, batching, read authority and full transaction/watch work can
dominate the result.

Likewise, do not claim stronger safety than etcd. The goal is to preserve its
contract while offering a different storage and development experience.
Consensus theory alone does not establish implementation correctness.

## Positioning against the nearest choices

| Against | Proposed reason to choose Etcqlite, if proven |
| --- | --- |
| Kine | Need generic client semantics and native SQLite-backed HA beyond its Kubernetes/single-instance SQLite scope |
| k8s-dqlite | Need a verified operational/developer advantage and the non-Raft design; SQLite plus HA alone is not enough |
| Rhino | Need complete lease/concurrency/tooling behavior demonstrated by conformance rather than broad adapter claims |
| T4 | Prefer SQLite state and self-contained quorum authority rather than Pebble plus mandatory S3 coordination |
| rqlite/dqlite | Keep the etcd client contract instead of adopting SQL or building a separate semantic shim |
| LiteFS/Litestream | Need live strongly consistent coordination rather than async replica/backup progress |
| k8senv | Need an inspectable standalone coordination service and clustered behavior, not only pooled API-level tests |
| etcd | Gain concrete local workflow/storage benefits without giving up the required behavior or durability |

Porto and sql-not-so-lite are useful initial distribution/integration paths,
not a durable technical moat. Other projects can add adapters and UIs. A
credible longer-term advantage would come from conformance evidence,
reliable recovery, and demonstrably better operations.

## Choose an existing alternative instead when

- Single-server K3s storage is the entire need: use the existing SQLite/Kine path.
- Production etcd behavior is needed now: use etcd, not this unimplemented plan.
- Distributed SQL is the application contract: evaluate rqlite or dqlite.
- Async SQLite replicas are acceptable: evaluate LiteFS and its current support boundaries.
- Only SQLite disaster recovery is missing: evaluate Litestream.
- Only isolated Kubernetes API tests are needed: evaluate k8senv/Kine.
- Object-store authority and T4's compatibility matrix fit: evaluate T4 directly.

Building Etcqlite is justified only if the combined contract and workflow
matter enough that these existing choices are insufficient. If broad
conformance or the non-Raft design is dropped, revisit reuse before writing
another thin SQLite shim.

## Validate the actual selling point

In addition to the [existing correctness/performance gates](verification-and-performance.md),
record cold-start-to-ready time, peak memory, explicit setup steps, external
services, time to diagnose a revision/lease/watch problem, consistent backup
steps, and verified restore time.

Run operator scenarios at equivalent topology, security and durability.
Compare current alternatives fairly, including their existing CLIs, APIs and
inspection tools. Etcqlite does not get credit merely because a workflow was
not explored on the other system.

Freeze success criteria before the experiments. If neither a meaningful
workflow benefit nor the declared performance objective survives comparison,
the appropriate outcome is a product/scope decision, not a stronger slogan.

## Evidence limits and maintenance

This document combines directly inspected primary source and official
documentation with the proposed Etcqlite plan. It is not a comparative
production audit. Moving source links may change after the evidence date.

Recheck important gaps before external positioning. Verify API behavior with
the exact selected releases rather than assuming a README, language choice,
project age, or consensus name proves compatibility.

The pinned etcd and consensus research revisions, source-level caveats and
integration baselines are retained in [research](research.md). No code,
benchmarks, dependency installation or CI enablement is authorized by this
comparison.
