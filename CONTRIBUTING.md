# Contributing

Contributions to this experimental datastore are welcome: synthetic regression
cases, compatibility/recovery evidence, focused fixes and clearer docs.
This repository is currently private; the MIT license does not change access
permissions or establish production readiness.

Read the [Code of Conduct](CODE_OF_CONDUCT.md). Coding agents must read
[AGENTS.md](AGENTS.md) before editing. Contributors are accountable for all work,
including AI-assisted code, docs and assets: review/test it and be able to
explain what it does and why it is needed.

## Before changing code

Search existing issues/docs and use the bug or feature template. Discuss changes
to consensus, formats, supported platforms or public behavior before broad
implementation. Report vulnerabilities privately through
[SECURITY.md](SECURITY.md), never as public issues.

Follow [development](docs/development.md) for toolchains, architecture,
synthetic fixtures and pinned references. [Installation](docs/installation.md)
separates user setup from development, and [releases](docs/releases.md) explains
local packaging, provenance, isolated install checks and inactive CI.

## Pull requests

Use a focused feature branch and draft PR. Keep related behavior/tests/docs
together; preserve useful policies, licenses and working behavior. Use existing
Rust/Go formatting and strict Clippy/vet checks. Short plain commit subjects
match this project; Conventional Commits, CLA and DCO are not required.

Start with targeted checks, then run relevant full suites:

```sh
make fmt
make lint
make test
python3 -m unittest discover -s script -p 'test_*.py'
bash script/release.sh --dry-run
```

Use only synthetic records and owned temporary resources. Do not replace an
installed app, discover existing services or bypass full-sync/quorum contracts
for tests. Include commands/results, genuine skips/unverified platforms, and
any changed configuration or recovery behavior in the PR template.

Address review feedback without weakening protections or hiding failures.
CI templates are deliberately `.disabled_yaml` and Actions is off; do not claim
a live CI result from local validation. Reviews/support are best-effort with no
turnaround guarantee. Follow [accessibility guidance](ACCESSIBILITY.md) and
preserve all [third-party license notices](docs/releases.md#artifacts-and-cleanup).
