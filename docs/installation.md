# Installation and first use

Etcqlite is a headless experimental service and snapshot CLI, not a desktop app.
No production release or automatic-update feed exists. The existing managed
native profile is macOS/Linux; Windows lock/privacy support is not qualified.
ARM64 macOS and Ubuntu 24.04 Linux have native prototype evidence. AMD64 and
older OS/toolchain compatibility remain separate qualification gates.

## Build from source

Use authorized access to this still-private repository. Prerequisites are Git,
Make, a C compiler, `protoc`, Rust/Cargo 1.99 or later, and Go 1.26 or later.
Python 3.12 or later is needed for packaging/installing local development
archives, not for running the installed native binaries.

```sh
git clone git@github.com:mbianchidev/etcqlite.git
cd etcqlite
make build
target/release/etcqlite --version
target/release/etcqlite-snapshot --version
```

Both packaged tools must report the same application version. The etcd API
profile (3.7.3) is a separate compatibility version and is not bumped with the
application. Direct `go build ./snapshot` is an unstamped development build;
use Make or the release entrypoint for synchronized binary versions.

## Isolated local developer package

The working tree must be clean. This produces a local archive and provenance,
not a published release:

```sh
bash script/release.sh --help
bash script/release.sh --dry-run
bash script/release.sh
```

Outputs are under `target/dist/` with target/version/source identity, a trusted
builder-side SHA256 sidecar and provenance. The archive includes both binaries,
installer, docs and third-party notices. Packaging verifies native executable
headers/versions and installs into an owned temporary prefix before success.
It does not replace a system app, modify PATH, start services or change versions
in the original checkout.

Inspect the printed exact archive path and provenance. Check its checksum
against the value received through a trusted channel, then install into a new
explicit prefix:

```sh
python3 script/install.py --archive /absolute/path/to/verified-package.tar.gz \
  --sha256 <trusted-64-character-lowercase-sha256> \
  --prefix /absolute/path/to/new-private-prefix --allow-unsigned
```

The digest is integrity evidence, not authentication or a signature. Do not
trust a checksum merely because an untrusted download supplied it. Verify the
archive before extracting or executing its bundled installer.
`--allow-unsigned` is an explicit trust decision for local developer artifacts.

The prefix must be owned by the current user, private (0700), and empty or
already marked as owned by this installer. Existing unmanaged `bin` folders
and user installations are never adopted or overwritten.

## First useful workflow

Choose a new directory and free loopback port. Never point a quickstart at
existing user or cluster data:

```sh
NODE="$(mktemp -d)/node"
target/release/etcqlite init --data-dir "$NODE" --name local
target/release/etcqlite serve --data-dir "$NODE" --listen 127.0.0.1:12379
```

The service prints a JSON `started` record with its endpoint. In another
terminal, readiness must succeed:

```sh
curl --fail http://127.0.0.1:12379/readyz
curl --fail -H 'Content-Type: application/json' \
  -d '{"key":"bW9jay9xdWlja3N0YXJ0","value":"c3ludGhldGlj"}' \
  http://127.0.0.1:12379/v3/kv/put
curl --fail -H 'Content-Type: application/json' \
  -d '{"key":"bW9jay9xdWlja3N0YXJ0"}' \
  http://127.0.0.1:12379/v3/kv/range
```

The Range response contains the same base64 synthetic value. Installed packages
use `/absolute/prefix/bin/etcqlite` instead of `target/release/etcqlite`.
Ctrl-C/SIGTERM drains the owned service and preserves data. Default diagnostics
are on stderr and in `$NODE/logs/app.log`; see [logging](logging.md).

## Explicit offline upgrades and recovery

There is no background self-updater or desktop update UI. Mixed native versions
and live rolling engine upgrades are unqualified. Back up and fence the source,
stop all nodes/processes using the selected prefix, and use matching native
binaries across a quorum. Updating binaries does not migrate or downgrade data.
See [recovery and migration](recovery-and-migration.md) before changing an
established authority.

```sh
python3 script/install.py --archive /absolute/new-package.tar.gz \
  --sha256 <trusted-digest> --prefix /absolute/private-prefix \
  --allow-unsigned --acknowledge-stopped --data-dir /absolute/offline-node
```

Repeat `--data-dir` for every known node using this prefix. Their existing
`node.lock` files are locked without truncation/unlinking; a running node blocks
activation. The stop acknowledgment covers additional instances the operator
must stop; the installer deliberately does not scan for user workloads.
Porto-managed binaries belong to Porto's lifecycle, not this prefix installer.

Each package is verified before code executes, stored in an immutable named
version directory, then both tools activate through one atomic `current`
symlink. Old versions remain intact. Downgrades are rejected. A private journal
allows recovery after interruption:

```sh
python3 script/install.py --prefix /absolute/private-prefix \
  --recover --acknowledge-stopped --data-dir /absolute/offline-node
```

Prepared/uncommitted activation rolls back only its owned pointer; committed
activation retains the verified new version. Recovery refuses unexpected
pointer/tree changes rather than overwriting another writer. No node data is
changed, initialized, migrated or removed. For uninstall, stop owned instances
and preserve backups/data first; remove only the explicitly selected installer
prefix after inspecting its ownership marker. There is no system uninstaller.

## Signing and supported-platform limits

Developer packages have no Developer ID signature, notarization, authenticated
update metadata or production signing guarantee. Toolchains may attach ad-hoc
Mach-O signatures; these are not publisher authentication. No Gatekeeper,
quarantine or managed-device settings are changed. For a trusted verified item
blocked by macOS, use only an exception the OS explicitly offers, following
[Apple's guidance](https://support.apple.com/en-us/102445). Never bypass malware
warnings or disable protection globally.

Native packaging detects its actual host and executable architecture; it does
not pretend cross-compilation is installation qualification. CI templates are
intentionally inactive; see [releases](releases.md) and
[troubleshooting](troubleshooting.md).
