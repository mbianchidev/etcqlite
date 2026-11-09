# Etcqlite

[Docs](docs/README.md) | [Issues](https://github.com/mbianchidev/etcqlite/issues/new/choose) | [Roadmap](docs/implementation-plan.md)

An experimental etcd v3-compatible datastore backed by SQLite, with non-Raft
clustered mode, native recovery tools and opt-in Porto/sql-not-so-lite
integrations. It serves local datastore and compatibility experiments, not
production workloads. This repository remains private.

## Contents

[Quickstart](#quickstart) | [Why](#why-etcqlite) | [How it works](#how-it-works) |
[Configuration](#get-started--configuration) | [Features](#features) |
[Documentation](#documentation) | [AI policy](#ai-policy) |
[Contributing](#contributing) | [Security](#security) | [License](#license)

## Quickstart

With Rust >=1.99, Go >=1.26, a C compiler, protoc and Make, use a fresh directory
and a free loopback port:

```sh
make build
NODE="$(mktemp -d)/node"
target/release/etcqlite init --data-dir "$NODE" --name local
target/release/etcqlite serve --data-dir "$NODE" --listen 127.0.0.1:12379
```

In another terminal, `curl --fail http://127.0.0.1:12379/readyz` must return
`ok`. [Installation](docs/installation.md) covers verified synthetic KV use,
native developer packages, prerequisites and unsigned-artifact limits.

## Why Etcqlite

Explore etcd-compatible interfaces and SQLite-backed operational/recovery
tooling with an explicit non-Raft authority model. The reference is etcd
**v3.7.3**; a smaller deployment footprint or speed advantage is not established.
Use [existing alternatives](docs/alternatives.md) when production guarantees
matter.

## How it works

One writer owns each private node directory. Clustered mutations/read barriers
pass through the maintained OmniPaxos core; durable decisions precede apply.
Local mode never becomes a quorum implicitly. See [architecture](docs/architecture.md)
and [consensus boundaries](docs/consensus.md).

## Get started / configuration

Start with [installation](docs/installation.md), [configuration](docs/configuration.md)
and [logging](docs/logging.md). Logs keep stderr diagnostics plus one rotating
private `app.log` per service. Package updates are explicit and offline; no
desktop UI, automatic-update feed or live rolling-upgrade guarantee exists.

## Features

Native gRPC/HTTP KV, watches, leases/auth and Lock/Election helpers; dynamic
membership/learners/leadership transfer; Maintenance and native snapshots;
reversible application translation; separately consented read-only inspection.
See [API coverage](docs/api-compatibility.md), [integrations](docs/integrations.md)
and the [roadmap](docs/implementation-plan.md).

One six-hour process-pause history passed its correctness gate. Availability,
24-hour fault coverage, comparative capacity and broader platform qualification
remain open. Failed/OOM evidence is retained; see [verification](docs/verification-and-performance.md).
No production, full-compatibility, HA or speed claim is made.

## Documentation

[User/developer index](docs/README.md) | [Troubleshooting](docs/troubleshooting.md) |
[Development](docs/development.md) | [Local releases / disabled CI](docs/releases.md)

## AI policy

You are responsible for everything you contribute, including AI-assisted code,
docs and assets. Review/test your contribution and be able to explain what it
does and why it is needed.

## Contributing

Use focused branches and draft PRs. Read [CONTRIBUTING.md](CONTRIBUTING.md),
[AGENTS.md](AGENTS.md), [accessibility guidance](ACCESSIBILITY.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md). Actions stays disabled; workflow
templates intentionally use `.disabled_yaml`.

## Security

Report vulnerabilities privately through [SECURITY.md](SECURITY.md).
General help follows [SUPPORT.md](SUPPORT.md); no support SLA is promised.

## License

[MIT](LICENSE.md), copyright 2026 Matteo Bianchi (mbianchidev).
Vendored/protocol/dependency notices retain their own licenses. The Contributor
Covenant retains its CC BY-SA attribution.
