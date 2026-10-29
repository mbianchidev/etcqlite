# Requirements and product boundary

Status: proposed implementation contract, written 2026-10-29.

## Confirmed requirements

| ID | Requirement |
| --- | --- |
| R01 | Create a private repository containing documentation only |
| R02 | No application code, generated stubs, dependency manifests, build files, executable helpers, or CI workflows in this delivery |
| R03 | Keep GitHub Actions disabled; enabling any CI requires explicit authorization |
| R04 | Plan an etcd replacement backed by SQLite |
| R05 | Integrate with Porto and sql-not-so-lite |
| R06 | Do not use Raft as the product's consensus protocol |
| R07 | Investigate a faster alternative without weakening etcd's consistency or durability to manufacture a win |
| R08 | Provide a full implementation plan, including compatibility, recovery, integration, and verification work |

Etcqlite is the working name. It is not a trademark or package-name clearance.

## Proposed interpretation of "drop-in"

The intended destination is compatibility with the **client-facing etcd v3
contract**, including the official client, its concurrency helpers, approved
`etcdctl`/`etcdutl` workflows, and the Kubernetes storage client.

The initial reference is etcd **v3.7.3**, commit
`0a173d0410a96d01091b484daae81365bbcde57e`. The
[API inventory](api-compatibility.md) covers its 49 RPCs across eight services.
An older supported Kubernetes/client version needs its own qualification
matrix; compatibility with one version does not prove all versions.

The following are proposed boundaries, not claims that the user explicitly
excluded them:

- No joining an existing etcd Raft cluster as a peer.
- No reading or writing an etcd data directory directly as SQLite.
- No automatic compatibility with every etcd binary flag or deployment manifest.
- No etcd v2 API in the first product contract.
- No cross-cluster multi-primary writes, sharding, or geo-active/active mode.
- No arbitrary remote SQL service in the Etcqlite write path.

These boundaries need approval at milestone M0 before implementation. If
"drop-in" means literal binary, flag, and peer compatibility, revise the scope
instead of silently narrowing it.

## Claim levels

| Label | Required evidence |
| --- | --- |
| Planning only | This repository; no runnable behavior implied |
| Local prototype | Single-node behavior for the explicitly listed implemented methods |
| Kubernetes-compatible | Pinned Kubernetes storage/client cases and integration scenarios pass |
| Clustered experimental | Non-Raft safety, crash recovery, quorum reads, and failover evidence exists |
| Client API-compatible | Every in-scope RPC and semantic requirement has conformance evidence |
| Approved tooling-compatible | Named tool versions and specific migration/maintenance workflows pass |
| Production candidate | Compatibility, durability, security, recovery, performance, and operational gates pass |

Do not describe a Kubernetes subset as a full etcd implementation. A stub,
synthetic successful response, or no-op for a meaningful operation is a gap,
not compatibility. Unresolved semantic differences block the relevant label.

## Safety invariants

| ID | Invariant |
| --- | --- |
| I01 | A successful mutation remains recoverable under the declared tolerated failures |
| I02 | Default KV operations are strictly serializable; stale reads require the client's explicit serializable option |
| I03 | Every modifying transaction receives one increasing MVCC revision, even if it changes multiple keys |
| I04 | Failed comparisons and read-only operations do not create synthetic KV mutations |
| I05 | A watch never exposes an uncommitted or partially applied transaction |
| I06 | Within retained history, watches preserve the reference ordering, replay, and progress guarantees |
| I07 | Quorum loss does not trigger an automatic downgrade to leader-only writes |
| I08 | An isolated former leader cannot acknowledge new mutations or default linearizable reads |
| I09 | Lease expiry, membership changes, and authorization mutations cannot diverge across replicas |
| I10 | Consensus acceptance, decided position, applied position, and MVCC revision are distinct quantities |
| I11 | Inspection cannot bypass the replicated mutation path |
| I12 | A failed restore or migration cannot silently replace the last verified usable database |

## Deployment targets

Proposed initial correctness target: native Linux on AMD64 and ARM64, using
local durable storage. Qualify macOS development mode next for Porto. Windows
support is a separate acceptance target, not implied by Porto supporting it.

Use three voting replicas for the first HA topology. One replica tolerates no
replica failure; three voters tolerate one unavailable voter with a normal
majority quorum. Three containers or VMs on one laptop do not provide
host-level HA.

## Performance objective

"Faster than etcd" is an experimental objective. The proposed go/no-go target
is at least **20% higher sustainable throughput** on the predeclared primary
three-node workload, with per-operation p99 latency no worse than the
reference, equal resource limits, equal durability, and no correctness failures.

The workload and measurement protocol are in
[verification and performance](verification-and-performance.md). This target
is proposed, not a result or a physical guarantee. If it fails, do not claim a
faster algorithm; return to a scope decision.

## Delivery boundaries

The current request authorizes the plan, not its execution. Future work in
Porto and sql-not-so-lite needs explicit assignment to those repositories.
Future code must use a working branch and draft PR; do not merge or push to
the default branch without authorization. No CI is part of this plan's
authorized delivery.
