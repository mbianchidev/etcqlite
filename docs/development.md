# Development

Implementation is experimental and checks are manual. GitHub Actions remains
disabled. The current M0 package is a library/spike, not a client-facing etcd
server. Do not deploy it or claim full compatibility or higher throughput.

## Toolchain and checks

Use Rust/Cargo and Go 1.26 or later. The initial local runs use Rust 1.99.1,
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
