# Security and operations

Status: planned controls and operating procedures, not an existing security
assessment or a production-readiness claim.

## Trust and access boundaries

Client gRPC, peer replication, HTTP gateway, and administration are separate
surfaces. Default to loopback for local mode. Non-loopback client and peer
listeners require authenticated TLS configuration before accepting traffic.

Use mTLS between peers and bind certificate identity to the expected
cluster/member identity. A valid certificate from an unrelated cluster does
not authorize membership.

Store key material and database directories with restrictive permissions.
Resolve operator-selected paths safely; reject unexpected symlink/resource
identity changes before destructive operations.

## Authentication and authorization

Implement the pinned etcd users/roles/range-permissions contract, including
root requirements and token/auth revision behavior. Hash passwords before
their replicated command is persisted; never replicate plaintext passwords.

Generate salts, tokens, identifiers, and other randomness outside
deterministic apply. Replicate the required material or a deterministic
validation configuration without exposing credentials through logs.

Enforce authorization at the appropriate linearization point. Preflight
checks alone cannot allow a queued mutation to bypass a committed permission
revocation. Define behavior for existing watches, long-running operations,
and forwarded requests.

Do not trust caller-supplied principal headers. Authenticate forwarded
identity through the peer protocol and preserve auditable request context.

## Sensitive data

Treat all key/value contents, snapshots, raw SQLite files, protocol logs,
auth records and inspection exports as potentially sensitive. Kubernetes
Secrets are not safe merely because their API encoding is base64.

Default logs contain operation type, safe request identity, duration, outcome,
node/configuration position, and error cause, not raw keys, values, tokens or
password hashes. Diagnostic verbosity must preserve redaction.

Use transport encryption and filesystem/volume encryption where required.
Application-level encrypted snapshots can be designed later if needed, but
do not imply SQLite data-at-rest encryption exists by default.

## Admission, quotas, and isolation

Bound request size, transaction operations/depth, batch size, queue length,
concurrent RPCs, watch streams, replay work, snapshot I/O and read duration.
Use explicit overload errors and cancellation rather than unbounded memory.

Implement the reference quota/alarm semantics. A disk-full node stops unsafe
mutations, reports the condition, and does not hide it as an empty database.
Only documented safe reads and recovery operations remain available.

Separate expensive maintenance and inspector work from the hot path with
bounded scheduling. One slow client cannot monopolize SQLite or protocol
progress.

## Process lifecycle

Acquire an exclusive node lock, validate identity and formats, recover
protocol state, apply decided entries, establish authority, and then mark
readiness. Do not accept client writes merely because the TCP port is open.

Graceful shutdown stops admission, drains bounded work, resolves or reports
uncertain proposals, closes streams, flushes durable state, closes SQLite,
and releases only owned resources. It must not delete user data.

Service managers and Porto reconcile owned processes rather than launching
duplicate nodes. A crash restart uses persisted identity and never silently
initializes a new cluster.

## Health and diagnostics

| Signal | Meaning |
| --- | --- |
| Liveness | Process/event loops are running; not a quorum assertion |
| Readiness | Node can meet the configured client contract and has valid recovered state |
| Leader/authority | Actual protocol view/configuration and authority state |
| Durable/decided/applied positions | Distinct real progress counters |
| MVCC/auth/compaction revisions | Logical state boundaries, not protocol indexes |
| Storage | Actual allocated/in-use sizes, WAL growth, checkpoint and flush latency |
| Replication | Peer lag, catch-up progress, failed acknowledgments and configuration transitions |
| Watches | Active streams, replay lag, queue pressure and cancellations |

Write structured errors to stderr or the owning runtime log. Include the
operation and actionable cause without credentials. Never report a failed
maintenance operation as successful.

## Upgrade and downgrade

Record executable, schema, command-codec and snapshot-format versions.
Negotiate features across all voters before emitting a new replicated format.
Qualify supported mixed-version operation and rollback boundaries.

The etcd `Downgrade` RPC cannot become a no-op success. Define its relationship
to the actual engine compatibility state and approved version policy in M0.
If faithful behavior is impossible, it remains a blocker for the broad
compatibility claim.

No automatic downgrade may discard history, disable auth, reset revisions,
or reinterpret existing identities.

## Operator procedures required before production candidacy

Document normal startup/shutdown, certificate rotation, password/token
rotation, quota recovery, defragmentation, lease diagnostics, safe
reconfiguration, leadership transfer, lost/corrupt member replacement,
backup/restore, quorum loss, whole-cluster recovery, and rollback.

Each procedure needs preconditions, exact authority required, expected
effects, verification, failure handling and recovery point/time evidence.
Potentially destructive actions require explicit operator consent.

## Dependency and release discipline

Audit selected library licenses and maintenance state. Pin reproducible
versions, review integrity, and surface dependency vulnerabilities rather
than ignoring install warnings.

Local developer packaging uses manually run checks, complete dependency notices,
strict archive verification and explicit private-prefix activation. It never
publishes, changes system apps or upgrades live authorities automatically.
See [installation](installation.md) and [local releases](releases.md).
Signing/notarization, mixed-version and broader platform qualification remain
unverified; unsigned development artifacts are not production releases.

GitHub Actions remains disabled. The maintainer explicitly approved reviewable
`.disabled_yaml` workflow templates, not enabled workflows or scheduled jobs.
Security support/reporting decisions are in [SECURITY.md](../SECURITY.md).
