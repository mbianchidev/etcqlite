# Architecture

Status: proposed. The consensus and tooling feasibility gates can change it.

## Component boundary

```text
etcd clients / Kubernetes / approved etcdctl
                      |
          etcd v3 gRPC and HTTP gateway
                      |
      admission, authentication, authorization
                      |
       request coordinator and bounded queues
              /                   \
      local executor       non-Raft replicated log
              \                   /
             deterministic state machine
                      |
       single SQLite writer and read snapshots
                      |
       MVCC history / leases / auth / log state

Porto ---------------- lifecycle, endpoints, logs, storage
sql-not-so-lite ------ consented read-only inspection
```

Porto is not a consensus participant. sql-not-so-lite is not the storage
server for mutations. Neither integration is required for server correctness.

## Proposed implementation stack

| Concern | Proposal | Rationale and gate |
| --- | --- | --- |
| Core | Rust | Direct integration with the candidate replicated-log library without a consensus FFI boundary |
| Async transport | Tokio and Tonic | Native gRPC streaming and cancellation |
| Wire definitions | Pinned upstream protobuf definitions | Preserve package names, method paths, fields, and error behavior |
| SQLite access | rusqlite with a controlled SQLite build | Explicit transactions and backup APIs; actual version chosen at implementation time |
| Non-Raft log | OmniPaxos/Sequence Paxos candidate | Replaceable only through a recorded architecture decision; see consensus gates |
| Gateway | Upstream-derived HTTP/JSON behavior | Required for an approved HTTP client compatibility claim |
| Tooling conversion | Isolated Go/bbolt helper if necessary | Only if an upstream-compatible snapshot cannot be implemented reliably in Rust |

These are proposals, not new dependencies in this repository. Do not add
versions or manifests until implementation is authorized. Pin audited
revisions during M0; avoid using a floating consensus branch.

Kine is a baseline and source of compatibility lessons, not a planned runtime
dependency of the Rust server. Reusing its Kubernetes subset alone would not
deliver the full API contract.

## Execution and storage ownership

One process owns a node's data directory through an exclusive lock. A dedicated
writer thread owns SQLite mutation transactions. Async RPC handlers submit
bounded requests and do not block the transport reactor on disk operations.

Use one local SQLite database per replica initially, containing both durable
consensus metadata/log entries and applied application state. This simplifies
crash boundaries and inspection, but creates contention and potentially two
durable commits per request. Measure that cost before optimizing it away.

A bounded read pool opens short SQLite snapshots. Never hold a SQLite write
transaction while waiting for network quorum, client input, or a watcher.
All background jobs use the same writer ownership rules.

Local mode bypasses distributed agreement but retains the same command,
revision, validation, apply, and watch machinery. Switching a local database
to a replicated cluster requires an explicit bootstrap procedure, not a
configuration flag that reinterprets existing state.

## Mutation path

1. Validate protobuf shape, size limits, credentials, and admission capacity.
2. Resolve a stable authenticated principal and validate its requested operation.
3. Allocate randomness or identifiers outside deterministic apply and include
   the required values in the command envelope.
4. Route to the current leader. Preserve one internal request identity across
   forwarding retries; do not give arbitrary caller retries an exactly-once claim.
5. Append the command through the selected protocol. Replicas persist the
   protocol-required state before sending acceptance acknowledgments.
6. Wait for a decided log entry. Apply it in order to SQLite, evaluating
   transaction comparisons and definitive authorization against the correct
   state at the operation's linearization point.
7. Atomically update current keys, MVCC history, lease indexes, relevant
   metadata, and the applied index. Capture the operation result.
8. Publish watch availability only after the application transaction commits.
9. Return success only when the operation is decided, applied, and recoverable
   under the selected durable storage contract.

Random numbers, local filesystem state, external RPC results, local clocks,
locale, and hash-map iteration must not determine replicated apply results.

## Read path

Default reads use an ordered read-barrier command initially. The node serves
the request only after that barrier is decided and locally applied, and opens
its SQLite read snapshot **after** the barrier wait. Read barriers do not
advance the KV revision.

A follower forwards the barrier/request or waits on an explicitly validated
leader-derived fence; it cannot substitute a local health check. An isolated
node returns an error rather than serving a default stale read.

Serializable reads may use local applied state when explicitly requested.
Their response must identify the actual snapshot revision. Historical
revision and compaction validation occurs inside the same read snapshot.

Replacing logged barriers with a faster quorum/read-index design is a separate
proof and benchmark task. A leader lease based only on wall-clock time is not
an acceptable shortcut.

## Watch path

MVCC history is the durable event source. In-memory notifications wake
watchers; they are not the only copy of events. Establishing a watch must
atomically bridge historical replay to live delivery without a revision gap.

Bound per-stream memory, per-watch queues, concurrent replay work, and total
watch count. Slow consumers must not block SQLite or consensus. Replay from
retained history or cancel with the correct recoverable error when the
history is unavailable. Progress notifications may only advertise a proven
delivery frontier.

## Failure and backpressure

Expose distinct request rejection, uncertain completion, storage failure,
quorum loss, permission failure, and cancellation outcomes. A timeout after
commit is not evidence that a write did not happen.

On local disk corruption or an unrecoverable persistence error, stop serving
mutations, fail readiness, retain diagnostics, and require repair or verified
resynchronization. Do not continue with a fresh empty database.

## Proposed source boundaries for future implementation

| Module | Responsibility |
| --- | --- |
| wire | Pinned protobuf types, gRPC and gateway adapters |
| admission | Limits, cancellation, request authentication |
| coordinator | Routing, proposal lifecycle, ordered read barriers |
| consensus | Protocol integration and peer transport |
| sqlite | Durable protocol storage and writer/read ownership |
| mvcc | Deterministic KV, transactions, history, compaction |
| watch | Replay, live notification, flow control, progress |
| lease | Grant, renewal, expiry commands, failover state |
| auth | Users, roles, permissions, token validation |
| membership | Voters, learners, endpoints, safe transitions |
| recovery | Native snapshots, restore, verification, import/export |
| operations | Status, metrics, alarms, maintenance, shutdown |

These are conceptual boundaries, not a requirement for a large workspace or
one crate per row. Start with the smallest implementation that keeps
transport, consensus, and deterministic state transitions separable.
