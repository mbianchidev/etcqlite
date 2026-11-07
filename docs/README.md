# Documentation

Implementation was authorized on 2026-11-07. The milestone documents remain
acceptance contracts, not blanket claims of delivered behavior. Actions
remains disabled. See [development](development.md) for runnable checks.

## Reading order

| Document | Purpose |
| --- | --- |
| [Development](development.md) | Manual build/test commands, pinned tools, and current implementation boundary |
| [Requirements](requirements.md) | Confirmed requirements, proposed product boundary, non-goals, and success criteria |
| [Alternatives and positioning](alternatives.md) | Competitor comparison, proposed USP, evidence boundaries, and reuse decisions |
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

Implementation, dependency installation, and isolated runtime experiments are
authorized. Changes to other repositories, live migration, and CI enablement
still require separate authorization. Checks remain manual.
