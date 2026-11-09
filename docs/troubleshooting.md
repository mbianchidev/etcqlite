# Troubleshooting

Use synthetic reproductions and redact diagnostics before sharing. Never post
raw databases, snapshots, credentials or customer keys/values to an issue.
Private vulnerability reports follow [SECURITY.md](../SECURITY.md).

| Symptom | Check / safe response |
| --- | --- |
| `protoc` or compiler missing | Install the documented development prerequisites, then rerun the locked build. Do not remove lockfiles. |
| Cargo says the lockfile needs updating | Confirm the manifest change is intentional; update only the expected dependency/root version, then rerun locked checks. |
| Init refuses the directory | Choose a new empty directory. Do not delete an existing node or `node.lock` to make it work. |
| Node already owned / lock unavailable | Stop only the verified process you own or choose a separate fixture. A lock file is not proof of a stale lock. |
| Bind/port error | Select another free loopback port. Do not stop or retarget an existing workload. |
| `/readyz` fails | Check stderr, configured logs and quorum/identity/TLS state. A running writer or open TCP listener does not prove readiness. |
| Logging directory denied | Use a dedicated owned mode-0700 directory, writable by the service. Relative overrides resolve from node data, not launch cwd. No fallback is automatic. |
| Active log is incomplete/corrupt | Preserve it; select a new private log directory. Do not truncate evidence or remove node data. |
| Runtime logging failure | Stderr contains bounded nonrecursive diagnostics. Fix space/permissions; file output may be incomplete while console output continues. |
| Package hash/platform/version error | Stop. Obtain a verified package/digest for the actual host. No package code should execute before these validations pass. |
| Unmanaged install prefix | Use a new private prefix. This installer does not overwrite Cargo/system/Porto-managed app locations. |
| Interrupted installation | Stop instances, preserve the prefix and run the documented `--recover` operation with its data-directory locks. |
| Recovery detects concurrent edits | Preserve both versions and the journal. Do not reset the pointer or delete evidence blindly. |
| macOS blocks an artifact | Verify its trusted origin/hash and unsigned status. Follow only Apple's per-item exception if available; do not disable platform protections. |
| No PR checks | `.disabled_yaml` templates and the repository's disabled Actions setting are intentional. Local checks are not live CI. |

For non-security bugs, report the exact command, platform/architecture, source
commit, expected/observed behavior and a minimal synthetic fixture through the
[issue chooser](https://github.com/mbianchidev/etcqlite/issues/new/choose).
No support turnaround time or production compatibility is promised.
