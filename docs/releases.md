# Local distribution and disabled CI

`script/release.sh` is a local-only, pager-free development packager. It reuses
Make/Cargo/Go, builds both real binaries, verifies platform/version and package
contents, and performs an isolated prefix installation. It never opens or
merges a release PR, commits a bump, tags, uploads an image, publishes a GitHub
Release, installs into the user's app location or launches a GUI.

Those remote phases of a production release are intentionally not implemented
as implicit side effects: this task explicitly forbids merging and publication.
A successful local artifact is not a published release.

## Commands and version synchronization

```sh
bash script/release.sh --help
bash script/release.sh --dry-run --version v0.1.1-rc.2+build.007
bash script/release.sh --version 0.1.1-rc.2+build.007 --jobs 2
```

Run from any directory using the script's full path. The default version comes
from `Cargo.toml`. SemVer accepts one lowercase `v` prefix and preserves
prerelease/build metadata; malformed/leading-zero versions and backwards
precedence are rejected. Visible gap warnings compare against the checkout's
version, not an invented published-release baseline. No release discovery or
network publication occurs.

The source must be committed and clean. A `git archive` snapshot is staged in
an owned temporary directory. Only that snapshot's root Cargo version and
lockfile root-package version change; unrelated dependencies and the checkout
remain untouched. Make stamps the Go companion from that same chosen version.
An override is marked dirty relative to its source commit rather than claiming
the modified version was exactly committed.

`--dry-run` creates no staging/output, build, commit, tag, remote request,
version mutation or package. It reports source dirtiness rather than hiding it.
Build failure/interruption returns nonzero and cleans only its captured
temporary build/prefix paths. Original version bytes never require rollback.
The installer has its separate durable activation journal described in
[installation](installation.md).

## Artifacts and cleanup

The default private output is `target/dist`. Overrides must be outside the
checkout or Git-ignored, so generated artifacts cannot masquerade as source
changes. An exclusive release lock prevents
cooperating packagers from racing. Each archive has a unique native target,
source and attempt identity; final archives are not overwritten.
SHA256/provenance record the actual source snapshot, version, dirty status,
owning worktree/branch, optional explicitly supplied session ID, target, time,
size, unsigned status and isolated-install result.

Only this invocation's temporary resources are automatically removed. Existing
qualification evidence, other sessions' installers, published/downloaded
artifacts, installed versions and user files are not stale-cleanup candidates.
In particular this draft's branch is ahead of the default branch, so its earlier
installers must not be deleted merely because a newer file exists. Missing or
unverified ownership/ancestry protects artifacts.

Packages carry the approved project license and third-party notices.
Apache/MIT/other dependency notices retain their own terms; the Contributor
Covenant retains CC BY-SA attribution. Never represent a local unsigned
artifact as a signed production package.

## Deliberately disabled workflow templates

The maintainer explicitly requested `.disabled_yaml` files instead of active
`.yml`/`.yaml` workflows. GitHub Actions remains disabled. These files are
reviewable templates, not running CI; no green check, billed-time reduction,
signing, upload or platform qualification is claimed from linting them.

Validation uses the private-repository `pull_request.types:
[ready_for_review]` contract. Draft opens/pushes, initially-ready creation and
later ready-PR commits do not emit that event. A new head needs a new explicitly
authorized ready transition or a PR-aware manual validation against the exact
current head. Do not toggle this draft or weaken protections to fabricate CI.

Protected/default-branch push and manual paths remain separate. Templates use
full action commit pins, bounded tool workers/jobs, disposable caches and
workflow-specific cancellation. Release-build templates only produce temporary
Actions artifacts; they do not publish releases or use signing credentials.
They must first be reviewed/renamed/enabled by an authorized maintainer.

No CI can be watched while both the repository setting and workflow extensions
are disabled. Local validation is reported separately. Enabling or publishing
requires a new explicit decision; never change visibility, checks or protections
as a shortcut.
