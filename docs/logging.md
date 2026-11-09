# Logging

The native service keeps structured JSON diagnostics on stderr. `serve` also
writes one private live `app.log` per node. Offline tools keep their existing
stdout result / stderr diagnostic contract; they do not capture snapshots,
inspection projections, key/value data or passwords into log files.

## Configuration

| Serve flag | Environment variable | Default |
| --- | --- | --- |
| `--log-dir` | `ETCQLITE_LOG_DIR` | `<initialized data-dir>/logs` |
| `--log-max-bytes` | `ETCQLITE_LOG_MAX_BYTES` | `10485760` (10 MiB), positive encoded bytes |
| `--log-retention-days` | `ETCQLITE_LOG_RETENTION_DAYS` | `30`, positive, at most 36500 |
| `--no-file-log` | `ETCQLITE_NO_FILE_LOG` | `false`; explicitly use stderr only |
| Existing `RUST_LOG` | `RUST_LOG` | `info`, using tracing filter syntax |

Relative log-directory overrides resolve against the initialized node data
directory, not the process launch directory. Absolute overrides are honored
without redirecting to a fallback location. Use a dedicated local writable
directory, owned by the service user, mode 0700. New files are mode 0600.
Existing nonprivate directories, links, special files and multiply-linked log
files are rejected. Logging never creates or initializes a missing node.

```sh
target/release/etcqlite serve --data-dir "$NODE" \
  --log-dir diagnostics --log-max-bytes 10485760 --log-retention-days 30
```

For service managers, persist these flags/environment values in their existing
configuration. Restart the owned service to change them. There is no desktop
Settings UI or independent preferences database. A read-only/ephemeral
environment must provide a writable directory or explicitly choose
`--no-file-log`; unavailable configured storage is not silently ignored.

## Files, coordination and privacy

All tracing producers use one synchronized writer and fixed `app.log`. An
exclusive `.etcqlite-log.lock` prevents independent processes from rotating the
same stream. Give separate nodes separate log directories. There is no
unbounded event queue: appends are synchronous, and diagnostics remain on stderr.
Do not configure an external log rotator for these files.

Records include UTC timestamps, severity, component, message and available span
context. Sensitive structured fields are redacted before JSON serialization,
as are recognized inline authorization headers, URL credentials, secret
assignments and private-key blocks. This is not permission to log arbitrary
request bodies, keys, values, tokens or customer records: never add those log
calls. Review any log before sharing it.

Field capture stops at 4096 bytes and adds an explicit truncation marker. An event exceeding
64 KiB after encoding is replaced by a marked bounded diagnostic. A record
larger than the configured file cap is rejected visibly, never split or written
past the cap. Very small positive caps can therefore reject ordinary records.
Use the 10 MiB default unless there is a specific operational need.

## Rotation and retention

Nonempty live files rotate at UTC calendar-day boundaries and before a complete
encoded record would exceed the size cap. Equality is allowed; UTF-8 bytes and
the newline count. Empty streams do not produce empty archives.

Archives use `app.<stream-id>.<last-record-unix-nanoseconds>.<nonce>.log`.
The persisted private stream identity and archive timestamp, not filesystem
mtime, determine ownership and age. Renames never overwrite an existing
archive. An interrupted rename/reopen leaves an archive and one fixed live
filename that startup can reopen, not two active streams.

Inactive owned archives are removed only when their last-record time is
strictly older than `now - retention`. Exact-boundary files remain. Cleanup
runs at startup, after rotation, and from a daily maintenance timer checked at
most once per minute even without new events. An old live file rotates before
overdue cleanup or fresh appends. A stopped service cannot schedule cleanup;
startup enforces overdue retention.

Unrelated files, other stream identities, directories, symlinks and their
targets are preserved. Cleanup attempts independent eligible archives even
when another unlink fails, then reports incomplete retention.

## Failures and recovery

Invalid configuration, unavailable initial storage and a competing writer
fail service startup with a clear stderr error. Runtime write/rotation/retention
failures preserve console diagnostics and report a nonrecursive logging error,
rate-limited to one per minute with a suppressed-error count. Ordinary file
logging is best-effort after startup, not a required audit trail. The service
does not claim complete file output while storage is failing.

A partial/corrupt active record is not silently truncated or treated as valid.
Preserve the affected directory for diagnosis and restart with a new private
`--log-dir`; do not delete the node or its lock to repair logging. Healthy
shutdown flushes/closes file output and releases the log-directory lock.
Filesystem stalls can block synchronous diagnostics, so logs belong on local
storage rather than an unreliable network mount.

Tests use private temporary directories, synthetic events and controlled UTC
times for size/midnight/restart behavior, concurrent writers, exact retention
cutoffs, inactive cleanup, symlinks and open/write/rename/unlink failures.
