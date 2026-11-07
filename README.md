# Etcqlite

An experimental etcd v3-compatible store backed by SQLite, with a non-Raft
clustered mode and integrations with Porto and sql-not-so-lite.

**Private, experimental prototype.** Native gRPC/HTTP KV, watches, leases, auth,
Lock/Election helpers, deterministic SQLite state, protocol fault spikes,
and an offline snapshot converter are runnable. Static non-Raft quorum serving
has bounded three-node evidence. Dynamic administration, full recovery and
qualification are still being implemented. GitHub Actions remains
disabled. No production, full-compatibility, or performance claim is made.

## Development and scope

- [Development](docs/development.md): manual build, test, and reference tooling.
- [Full implementation plan](docs/implementation-plan.md): milestones,
  dependencies, work packages, acceptance gates, and delivery order.
- [Alternatives and positioning](docs/alternatives.md): existing options,
  potential differentiators, and when not to build Etcqlite.
- [Requirements](docs/requirements.md): confirmed constraints, proposed scope,
  compatibility claims, and performance objectives.
- [Documentation index](docs/README.md): architecture, storage, consensus,
  API coverage, integrations, recovery, verification, security, and research.

The initial compatibility reference is **etcd v3.7.3**. Non-Raft consensus and
better performance are design goals, not established properties. The plan
requires evidence before either production readiness or a speed advantage
can be claimed.

## Contributions

Use focused branches and draft pull requests. Keep detailed material in
`docs/` and keep this README short. Implementation was authorized on
2026-11-07; CI enablement and changes to integration repositories were not.

No project license has been selected.
