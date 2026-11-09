# Agent guidance

Read this file and [CONTRIBUTING.md](CONTRIBUTING.md) before editing.

## Repository map

- `src/`: Rust native CLI, service, deterministic SQLite state and non-Raft core.
- `tools/`: Go snapshot/interchange, official-client and qualification tooling.
- `vendor/omnipaxos/`, `proto/`: maintained upstream inputs with their own notices.
- `tests/`: synthetic native integration/regression cases.
- `script/`: local-only packaging and transactional prefix installation.
- `scripts/`: existing coupled qualification runner; preserve its consumers.
- `docs/`: user/developer references and exact qualification limits.

## Verified workflow

Use Rust/Cargo >=1.99, Go >=1.26, Python >=3.12 for distribution, Make, Git,
a C compiler and protoc. Preserve `Cargo.lock`/`tools/go.sum`.

```sh
make build
make fmt
make lint
make test
python3 -m unittest discover -s script -p 'test_*.py'
bash script/release.sh --dry-run
```

Run the smallest relevant selector first. Use existing tools and synthetic
temporary directories; never access real customers or scan existing workloads.
For local installation use an explicitly new private prefix, never the user's
installed app. Follow [development](docs/development.md),
[logging](docs/logging.md) and [distribution](docs/releases.md).

## Safety and contribution boundaries

Preserve durable quorum acknowledgments, full sync, persisted identities,
incarnations, promises, checksums and existing `node.lock` ownership. Never erase
voter state or weaken assertions to make a qualification run pass.
Use matching native binaries; mixed-version/live-upgrade support is unqualified.

Keep logs private and bounded; never add raw KV, payload, token, password or
credential logging. Surface filesystem/process/package errors explicitly.
Do not delete retained failed evidence, unrelated installers or other sessions.
Keep changes focused and update the directly related docs.

Actions remains disabled. Workflows intentionally use `.disabled_yaml`; do not
rename/enable them, toggle PR readiness, change visibility/protections, create
release tags, merge or publish without explicit authorization.

Use short plain commit subjects, `git commit --no-gpg-sign`, and no tool/agent
attribution. Do not amend or force-push without an explicit request.
All contributors remain responsible for reviewing, testing and explaining
their work, including AI-assisted contributions.
