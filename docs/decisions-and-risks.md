# Decisions and risks

Status: implementation contract approved on 2026-11-07. Approval is not
production qualification. The initial consensus safety failure and local
fork decision are recorded in [consensus](consensus.md).

## Decision states

Confirmed requirements come from the user's request. Proposed decisions are
the recommended implementation direction. Blocked decisions need evidence or
explicit owner input before dependent work starts.

| ID | Decision | State | Reason / required resolution |
| --- | --- | --- | --- |
| D01 | Private implementation repository; inactive CI | Confirmed | Actions remains disabled; approved templates use `.disabled_yaml`, not active workflows |
| D02 | SQLite-backed state and non-Raft product consensus | Confirmed | Core requested properties |
| D03 | Etcqlite working name | Proposed | Matches the chosen concept; legal/package clearance not performed |
| D04 | Client/API compatibility, not etcd peer compatibility | Proposed | Non-Raft peers cannot transparently join an etcd Raft cluster |
| D05 | Initial reference etcd v3.7.3 | Proposed, source pinned | Latest release observed on 2026-10-29; older clients qualified separately |
| D06 | Rust core with Tokio/Tonic and SQLite | Proposed | Avoid a consensus FFI boundary; Porto integrations remain process/API based |
| D07 | Maintained OmniPaxos/Sequence Paxos fork | Owner approved | Upstream snapshot safety failure reproduced and repaired locally; bounded M0 simulations pass, production qualification remains pending |
| D08 | SQLite protocol storage and application state in one file | Proposed | Clear atomicity/ownership; throughput and flush cost need measurement |
| D09 | Logged read barriers before optimized reads | Proposed | Simple authority argument; may be a major performance cost |
| D10 | Three voters, conventional quorum; no degraded automatic writes | Proposed safety contract | Preserve clear partition and durability behavior |
| D11 | Full 49-RPC inventory plus gateway/tooling qualification | Proposed | Prevent accidental Kubernetes-subset marketing |
| D12 | Offline fenced first migration | Proposed | Avoid unproven dual-write synchronization |
| D13 | Isolated non-Raft Go/bbolt tooling converter | Selected after M0 spike | Binary buckets survive conversion and pinned etcdutl status/restore; full server migration remains a later gate |
| D14 | Porto opt-in lifecycle, inspector read-only | Proposed | Reuse existing product boundaries; no direct SQL mutations |
| D15 | Linux AMD64/ARM64 first; macOS dev next | Proposed | Native storage correctness before additional platform guarantees |
| D16 | At least 20% primary-workload throughput improvement | Proposed acceptance target | Not a measurement; owner freezes it before implementation |
| D17 | License | Confirmed | MIT, copyright 2026 Matteo Bianchi (mbianchidev); third-party terms preserved; visibility/publication not changed |
| D18 | Timeline and staffing | Open | Estimate after M0 evidence; do not promise a production-ready date |
| D19 | Private rotating service logs | Confirmed | Existing tracing/stderr preserved; configured node-base path, one live stream, UTC/size rotation and 30-day default retention |
| D20 | Explicit local developer distribution | Confirmed | Native CLI packages and offline private-prefix activation only; no desktop UI, automatic-update feed, release publication or live engine upgrade |
| D21 | Security and conduct policy | Confirmed | Maintainer-approved private reporting/enforcement; best effort, no response/fix deadline; security support current main/latest published release |

## Risk register

| Risk | Impact | Mitigation / stop trigger |
| --- | --- | --- |
| Library lacks safe learner/promotion/transfer behavior | Broad API scope cannot be delivered by simple embedding | Prove during M0; reject candidate or revise scope explicitly |
| SQLite log and apply require extra flushes | Slower writes than etcd | Baseline early; measure group commit without weakened durability |
| Global revisions serialize independent writes | Limited benefit from leaderless protocols | Prefer a simple ordered log; do not assume EPaxos removes the bottleneck |
| Read barriers dominate latency | Fails the speed objective | Measure before redesign; require a proof for any faster read authority path |
| etcd administrative fields do not map faithfully | Misleading tools or incompatible behavior | Record exact mappings; no synthetic success or hard-coded values |
| Snapshot/hash/downgrade interoperability is expensive | "Full drop-in" remains unattainable on schedule | Front-load an isolated tooling spike; label narrower prototypes honestly |
| Learner or configuration transition loses outcomes | Acknowledged data loss or unavailable cluster | Persist transition state and inject faults at every boundary |
| Lease failover resurrects or prematurely expires ownership | Broken distributed locks and controllers | Compare reference behavior; partitioned keepalive campaign |
| Watch replay/live race or compaction bug | Lost events, stale Kubernetes caches | Durable history source, frontier invariants, differential and fault tests |
| Inspector exposes secrets or permits raw writes | Data disclosure or broken replicated state | Explicit consent, sanitized projection, secured boundary, denied writes |
| Restore reuses stale identity or resets revisions | Split brain or invalid caches | Fenced isolated restore, explicit identity/revision policy |
| One-host benchmark is mistaken for HA evidence | Unsupported production claims | Separate local demos from multi-host qualification |
| Scope expands into arbitrary SQL or orchestration | Larger attack surface and delayed core delivery | Keep mutations behind etcd contract; integration adapters stay optional |
| Manual evidence is incomplete without CI | Regressions go unnoticed | Reproducible manual qualification and recorded evidence; do not enable CI without consent |

## Rejected shortcuts

Do not replace consensus with gossip, ordinary async WAL shipping, a raw
shared SQLite file, sql-not-so-lite's polling replicas, or a leader elected
only by a local timer.

Do not claim a speed win by disabling synchronization, shrinking durability,
using stale reads by default, comparing one node with three, dropping watch
history, or omitting transaction work.

Do not bypass unsupported maintenance methods with successful empty results.
Do not use a raw key/value replay as full-fidelity migration.

## Gate review process

For a decision, retain the question, considered alternatives, immutable source
references, test evidence, cost, failure assumptions, and the owner-approved
outcome. Update dependent documents before dependent implementation starts.

The local OmniPaxos fork has explicit owner approval. No external
coordination dependency, scope reduction, production readiness claim,
default-branch merge, or CI enablement is implied.
