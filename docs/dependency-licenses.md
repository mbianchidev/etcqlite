# Dependency notices

The project is MIT-licensed. This does not relicense its dependencies, vendored
OmniPaxos fork, protocol inputs, or Contributor Covenant.

Local packages bundle runtime Rust dependency notices, the Go snapshot tool's
actual dependency/module notices, Rust/Go standard-library notices and retained
vendored/protocol notices. Build-only tools and procedural macros are not
distributed as runtime libraries. This inventory is not a legal certification;
review license compatibility and notices when dependencies change.

The registry archives for `pbjson 0.9.0` and `tonic-prost 0.14.9` omit their
workspace license files. `script/notice_sources.json` records exact upstream
repository/commit URLs, declared licenses, local notice files and SHA256 values.
These notices were retrieved/verified from those pinned upstream commits.
Packaging additionally matches the locked crate's `.cargo_vcs_info.json` and
refuses stale, changed, missing or out-of-tree overrides.

The copied notices are under `third_party/licenses/` with their original
copyright holders and text. They are license evidence, not copied application
code or permission to replace other third-party policies.
