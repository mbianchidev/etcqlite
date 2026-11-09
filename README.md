# Etcqlite

An experimental etcd v3-compatible store backed by SQLite, with a non-Raft
clustered mode and integrations with Porto and sql-not-so-lite.

**Private, experimental prototype.** Native gRPC/HTTP KV, watches, leases, auth,
Lock/Election helpers, deterministic SQLite state, protocol fault spikes,
and an offline snapshot converter are runnable. Dynamic non-Raft membership,
learner bootstrap/promotion, leadership transfer, Maintenance, native recovery,
reversible application translation, and read-only integration endpoints now
have bounded synthetic evidence, including a multi-GiB application cutover and
post-write reverse restore. Opt-in Porto and sql-not-so-lite wiring is merged
in the approved integration repositories. The first full Linux candidate sample
passed its resource gate. One continuous six-hour process-pause/history run
passed its correctness gate; availability, comparative performance and broader
fault qualification remain open. Earlier OOM and failed long-run evidence is
retained. GitHub Actions remains disabled. No production, full-compatibility,
or speed claim is made.

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
2026-11-07. CI enablement remains unauthorized; integration changes are limited
to the separately approved repository worktrees.

No project license has been selected.
