# Development

Implementation is experimental and checks are manual. GitHub Actions remains
disabled. The current package includes a local client-facing prototype. Do not deploy
it or claim full compatibility, HA, production readiness, or higher throughput.

## Local prototype

```sh
make build
target/release/etcqlite init --data-dir ./local-node --name local
target/release/etcqlite serve --data-dir ./local-node --listen 127.0.0.1:2379
```

Initialization is explicit and refuses nonempty data directories. Restart
uses the persisted identity. One dedicated writer owns the node lock and
SQLite application transactions. Ctrl-C/SIGTERM stops admission and drains
the owned runtime; user data is never deleted.

The same listener serves native gRPC and protobuf JSON at `/v3/...`, plus
`/health`, `/livez`, `/readyz`, `/version`, and `/metrics`. Default binding is
loopback. Non-loopback binding requires `--cert-file`, `--key-file`, and
`--client-ca-file`; private keys must not be group/world accessible.
Certificate common names and tokens are authenticated, not caller identity
headers. Password hashing is bounded and occurs outside deterministic apply;
plaintext passwords and tokens are not persisted.

The prototype implements the six KV methods, Watch, all five Lease methods,
all 17 Auth methods, both Lock methods, and all five Election methods.
RangeStream intentionally has no REST mapping, matching the pinned source.
Cluster/Maintenance serving and distributed semantic closure remain separate
pending gates, not successful no-op methods.

`/version` distinguishes the etcd protocol profile from the actual Etcqlite
version, commit, and dirty-build status. Response headers carry real persisted
cluster/member IDs and the local execution epoch.

Manual installation defaults to an isolated prefix rather than modifying a
global tool installation:

```sh
make install
target/install/bin/etcqlite --help
```

## Toolchain and checks

Use Rust/Cargo 1.99 or later and Go 1.26 or later. The initial local runs use Rust 1.99.1,
Go 1.27.3, and macOS ARM64. Linux/platform and minimum-toolchain qualification
are separate gates. Rust dependencies use `Cargo.lock`; Go uses `tools/go.sum`.

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo audit --deny warnings
go -C tools test ./...
go -C tools vet ./...
```

The protocol dependency is the maintained local fork under `vendor/omnipaxos`.
Its upstream provenance, initial failure, patch, and evidence limits are in
[consensus](consensus.md). Do not replace it with the unpatched upstream
revision merely because the package version matches.

All fixtures are synthetic. Tests own temporary directories and do not
discover, scan, modify, or stop existing user services.

The storage cases include process exit before destructors, full application
rollback after injected persistence failure, persisted internal retry
identity, unsigned IDs, exact request-size and transaction-operation limits,
nested transaction headers/comparison timing, and compaction/tombstone floors.

Wire definitions are unmodified upstream inputs under `proto/`; immutable
provenance and licenses accompany them. The generated descriptor inventory
must contain exactly 49 unique client RPCs. Generated serializer formatting
gets one scoped Clippy exception instead of edits to generated bindings.

## Pinned reference tooling

Use checksum-verified etcd, etcdctl, and etcdutl **v3.7.3**, from the
[upstream release](https://github.com/etcd-io/etcd/releases/tag/v3.7.3).
Verify the selected archive against the release's `SHA256SUMS` before
extracting or executing it. Do not use an unqualified system binary.

The snapshot interoperability case deliberately skips when the tool is not
configured; a skipped case is not tooling qualification:

```sh
ETCQLITE_ETCDUTL=/path/to/verified/etcdutl go -C tools test -count=1 ./snapshot
```

The M0 case creates an upstream bbolt snapshot with its SHA-256 trailer,
retained binary history/tombstones, lease/auth records, and storage metadata.
The pinned tool must report the expected revision, count, and storage version
and restore into a new isolated directory. It never connects to a live cluster.

The official client baseline can start its own synthetic loopback references:

```sh
mkdir -p target/reference
GOBIN="$(pwd)/target/reference" go install github.com/k3s-io/kine@v0.17.5
ETCQLITE_ETCD=/path/to/verified/etcd \
ETCQLITE_KINE="$(pwd)/target/reference/kine" \
go -C tools test -count=1 -v ./conformance
```

These tests require `lsof` to verify that the child owns its listener before
requests are sent. They scrub inherited Kine/AWS settings, disable Kine's
metrics listener, use private temporary data, and stop only their own child
processes. Skipped reference tests do not count as conformance.

The pinned Kine module is v0.17.5, commit
`f82bd25164a393b333815f41746ef89de0d5ae20`. Its default build label is
`dev (HEAD)`, so verify `go version -m` rather than trusting that label.
The SQLite baseline supports RangeStream, does not expose AuthStatus, and
returns an unsupported error for HashKV. It is not the full product scope.

## Offline converter spike

```sh
go -C tools build -o ../target/etcqlite-snapshot ./snapshot
target/etcqlite-snapshot export /path/to/manifest.json /path/to/new-snapshot.db
target/etcqlite-snapshot import /path/to/snapshot.db /path/to/new-manifest.json
```

The version-1 manifest preserves complete sorted binary bbolt buckets using
base64 JSON byte fields. This is an internal interchange format, not a
current-key-only migration recipe. The converter rejects incompatible
formats, duplicates, malformed revision keys, invalid checksums, truncation,
nested buckets, and existing output paths.

Outputs and staging files are mode `0600`. Publication is atomic and
no-overwrite; files and the parent directory are flushed. Failure removes
only the operation's staging file, never an existing output or source.
All manifests and snapshots are sensitive, including their auth records.

This initial spike has a 512-MiB file limit and materializes the manifest.
It is not yet the large-dataset streaming migration implementation. Full
native SQLite backup/restore, live API snapshots, application-state import,
revision/cache policy, and fenced reverse cutover remain M5/M6 obligations.
