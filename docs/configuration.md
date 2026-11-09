# Configuration reference

The CLI is authoritative: `etcqlite --help`, `etcqlite <command> --help`, and
`etcqlite snapshot <operation> --help`. There is no desktop preferences UI or
general server config file. Cluster JSON is the specific membership bootstrap
input, not a way to retarget an established database.

## Native service and initial state

| Surface | Relevant options / defaults |
| --- | --- |
| `init` | Required new `--data-dir`; `--name etcqlite`; optional explicit `--cluster-id`, `--member-id`, `--cluster-config`, positive `--quota-bytes` |
| `serve` | Required existing `--data-dir`; `--listen 127.0.0.1:2379`; `--request-timeout-ms 5000` |
| Client TLS | `--cert-file`, `--key-file`, `--client-ca-file`; non-loopback listeners require authenticated TLS |
| Peer TLS | `--peer-listen`, `--peer-cert-file`, `--peer-key-file`, `--peer-ca-file`; all supplied together |
| Snapshot companion | Optional `--snapshot-tool`; otherwise the matching installed companion is required for conversion |
| Logging | `--log-dir`, `--log-max-bytes`, `--log-retention-days`, `--no-file-log` and existing `RUST_LOG`; see [complete logging reference](logging.md) |

General CLI paths follow explicit command arguments and the invocation directory.
The deliberate exception is a **relative log-directory override**, resolved
against the initialized node data directory. No path is silently redirected.
Initialization refuses existing contents; serve/restart retains identity.
Default fresh-node MVCC quota remains 2 GiB and is not a physical combined
protocol/application file cap. Non-loopback use needs private credentials.

Cluster JSON specifies persisted identity and initial configuration 1, HTTPS
member client/peer URLs, bounded distinct protocol PIDs and optional learner
flags. Actual joins, reconfiguration, certificates and incarnation fencing are
described in [development](development.md) and
[recovery](recovery-and-migration.md). Never initialize empty votes for an
established identity.

## Offline tools

`snapshot save/status/restore/export/import`, `join`, `migrate` and `inspect`
require explicit paths. Native restore/import use fresh destinations and
fenced-source preconditions; sensitive inspection data requires separate
acknowledgment. Migration requires a new backup path. See
[recovery and migration](recovery-and-migration.md), not guessed version-based
upgrade shortcuts.

`etcqlite-snapshot --help`, `--version` and `--build-info` report the companion's
commands, application version and source identity. The build system stamps the
same application version into both tools; protocol/API version is separate.
No automatic downloader, update server, analytics opt-in, theme or hidden
desktop settings are introduced.

## Local release / install configuration

[Release commands](releases.md) expose version, read-only dry-run, explicit
private output directory, worker budget and optional actual session identity.
[Installation](installation.md) requires an explicit prefix, trusted SHA256 and
unsigned-development consent. Existing prefixes additionally require a stop
acknowledgment; list all known node directories to hold their existing locks.
No command changes global app locations, PATH, service registration or data.
