# Etcqlite

A proposed etcd v3-compatible store backed by SQLite, with a non-Raft
clustered mode and integrations with Porto and sql-not-so-lite.

**Private, documentation-only planning repository. Nothing is implemented or
runnable. GitHub Actions is disabled. There is no application code, dependency
manifest, build configuration, or CI workflow.**

## Read the plan

- [Full implementation plan](docs/implementation-plan.md): milestones,
  dependencies, work packages, acceptance gates, and delivery order.
- [Requirements](docs/requirements.md): confirmed constraints, proposed scope,
  compatibility claims, and performance objectives.
- [Documentation index](docs/README.md): architecture, storage, consensus,
  API coverage, integrations, recovery, verification, security, and research.

The initial compatibility reference is **etcd v3.7.3**. Non-Raft consensus and
better performance are design goals, not established properties. The plan
requires evidence before either production readiness or a speed advantage
can be claimed.

## Planning contributions

Use focused branches and draft pull requests. Keep detailed material in
`docs/` and keep this README short. Do not add code or enable CI under the
current documentation-only authorization.

No project license has been selected.
