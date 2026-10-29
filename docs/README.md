# Documentation

All implementation details below are planned work, not descriptions of an
existing server. Repository state: documentation only. Actions: disabled.

## Reading order

| Document | Purpose |
| --- | --- |
| [Requirements](requirements.md) | Confirmed requirements, proposed product boundary, non-goals, and success criteria |
| [Implementation plan](implementation-plan.md) | Ordered work packages, dependencies, deliverables, and stop/go gates |
| [Architecture](architecture.md) | Components, execution model, request paths, and consistency boundaries |
| [Storage](storage.md) | SQLite schema, MVCC, revisions, persistence, compaction, and schema evolution |
| [Consensus](consensus.md) | Non-Raft selection, durability, reads, elections, reconfiguration, and failure handling |
| [API compatibility](api-compatibility.md) | Complete RPC inventory for the pinned reference and compatibility behavior |
| [Recovery and migration](recovery-and-migration.md) | Native snapshots, etcd interoperability, cutover, rollback, and disaster recovery |
| [Integrations](integrations.md) | Porto and sql-not-so-lite contracts and ownership boundaries |
| [Verification and performance](verification-and-performance.md) | Manual conformance, fault testing, reproducible benchmarks, and evidence requirements |
| [Security and operations](security-and-operations.md) | Trust model, credentials, lifecycle, diagnostics, quotas, and operator procedures |
| [Decisions and risks](decisions-and-risks.md) | Proposed decisions, unresolved blockers, alternatives, and review triggers |
| [Research](research.md) | Prior art, inspected source baselines, and primary references |

## How to change the plan

Separate confirmed user requirements from proposals. Update related documents
when a decision changes; the requirements and decision register are the
authoritative scope records. Link every implementation milestone to its
acceptance evidence rather than treating an unchecked task list as proof.

Implementation, dependency installation, runtime experiments, changes to other
repositories, and CI enablement require a separate request. Manual checks
described here are future work, not jobs to add to this repository.
