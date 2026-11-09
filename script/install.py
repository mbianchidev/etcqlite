#!/usr/bin/env python3
"""Verify and install a local unsigned Etcqlite development package into an explicit private prefix."""

import argparse
import contextlib
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import signal
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import uuid


MAX_ARCHIVE = 256 << 20
MAX_EXPANDED = 512 << 20
MAX_MANIFEST = 1 << 20
VERSION = re.compile(
    r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
)
BINARIES = ("bin/etcqlite", "bin/etcqlite-snapshot")
MARKER = {"format": 1, "kind": "etcqlite-private-prefix"}


def parse_version(text):
    match = VERSION.fullmatch(text) if isinstance(text, str) and len(text) <= 128 else None
    if match is None:
        raise ValueError(f"invalid SemVer: {text!r}")
    pre = tuple(match[4].split(".")) if match[4] else ()
    if any(part.isdigit() and len(part) > 1 and part.startswith("0") for part in pre):
        raise ValueError("numeric prerelease identifiers cannot have leading zeroes")
    return text.removeprefix("v"), tuple(int(match[i]) for i in (1, 2, 3)), pre


def compare_versions(first, second):
    _, a, ap = parse_version(first)
    _, b, bp = parse_version(second)
    if a != b:
        return (a > b) - (a < b)
    if not ap or not bp:
        return bool(bp) - bool(ap)
    for left, right in zip(ap, bp):
        if left == right:
            continue
        if left.isdigit() and right.isdigit():
            return (int(left) > int(right)) - (int(left) < int(right))
        if left.isdigit() != right.isdigit():
            return -1 if left.isdigit() else 1
        return (left > right) - (left < right)
    return (len(ap) > len(bp)) - (len(ap) < len(bp))


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def read_json(path, limit=MAX_MANIFEST):
    if not stat.S_ISREG(Path(path).lstat().st_mode):
        raise ValueError(f"JSON metadata is not a regular non-symlink file: {path}")
    with Path(path).open("rb") as source:
        data = source.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"JSON file exceeds {limit} bytes: {path}")
    return json.loads(data, object_pairs_hook=unique_fields)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path, value):
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
    try:
        with temporary.open("x", encoding="utf-8") as output:
            os.chmod(temporary, 0o600)
            json.dump(value, output, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def private_directory(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
        raise ValueError(f"expected a user-owned non-symlink private directory (mode 0700): {path}")


@contextlib.contextmanager
def locked_file(path, create):
    flags = os.O_RDWR | os.O_NOFOLLOW
    if create:
        flags |= os.O_CREAT
    descriptor = os.open(path, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.getuid():
            raise ValueError(f"lock is not an owned singly-linked regular file: {path}")
        current = path.lstat()
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise ValueError(f"lock identity changed: {path}")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError(f"resource is active or already locked: {path}") from error
        yield descriptor
    finally:
        os.close(descriptor)


def host_platform():
    system = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    arch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "AMD64": "amd64"}.get(platform.machine())
    if system is None or arch is None:
        raise ValueError("native packaging supports macOS/Linux ARM64 and AMD64 only; other platforms are unqualified")
    return f"{system}-{arch}"


def executable_platform(path):
    with Path(path).open("rb") as source:
        header = source.read(64)
    if len(header) >= 20 and header[:4] == b"\x7fELF" and header[4:6] == b"\x02\x01":
        arch = {62: "amd64", 183: "arm64"}.get(struct.unpack_from("<H", header, 18)[0])
        if arch:
            return f"linux-{arch}"
    if len(header) >= 8 and header[:4] == b"\xcf\xfa\xed\xfe":
        arch = {0x1000007: "amd64", 0x100000C: "arm64"}.get(struct.unpack_from("<I", header, 4)[0])
        if arch:
            return f"darwin-{arch}"
    raise ValueError(f"unsupported or corrupt native executable: {path}")


def verify_binaries(directory, manifest):
    for relative in BINARIES:
        path = directory / relative
        if executable_platform(path) != manifest["platform"]:
            raise ValueError(f"wrong executable architecture: {relative}")
        result = subprocess.run([str(path), "--version"], check=True, capture_output=True, text=True, timeout=15)
        expected = f"{path.name} {manifest['version']}"
        if result.stdout.strip() != expected:
            raise ValueError(f"embedded version mismatch: expected {expected!r}, received {result.stdout.strip()!r}")


def safe_name(name):
    return (
        isinstance(name, str) and len(name) <= 240
        and re.fullmatch(r"[A-Za-z0-9._+/-]+", name) is not None
        and not PurePosixPath(name).is_absolute()
        and str(PurePosixPath(name)) == name
        and all(part not in ("", ".", "..") for part in name.split("/"))
    )


def validate_manifest(manifest):
    fields = {"format", "kind", "version", "platform", "source_commit", "source_dirty", "repository", "signing", "files"}
    if not isinstance(manifest, dict) or set(manifest) != fields:
        raise ValueError("package manifest fields do not match format 1")
    if type(manifest["format"]) is not int or manifest["format"] != 1 or manifest["kind"] != "etcqlite-development-package":
        raise ValueError("unsupported development package format")
    if parse_version(manifest["version"])[0] != manifest["version"]:
        raise ValueError("manifest version must be normalized")
    if manifest["platform"] != host_platform() or manifest["signing"] != "unsigned-development":
        raise ValueError("package platform or signing profile does not match this installer")
    if type(manifest["source_dirty"]) is not bool or not isinstance(manifest["source_commit"], str) or re.fullmatch(r"[0-9a-f]{40}", manifest["source_commit"]) is None:
        raise ValueError("invalid package source identity")
    if manifest["repository"] != "mbianchidev/etcqlite":
        raise ValueError("package is not from the intended repository")
    files = manifest["files"]
    if not isinstance(files, dict) or not set(BINARIES).issubset(files) or not 2 <= len(files) <= 2048:
        raise ValueError("package must contain both native executables and a bounded file inventory")
    total = 0
    for name, metadata in files.items():
        if not safe_name(name) or name == "manifest.json":
            raise ValueError(f"unsafe package path: {name!r}")
        if not isinstance(metadata, dict) or set(metadata) != {"sha256", "size", "mode"}:
            raise ValueError(f"invalid file metadata: {name}")
        if type(metadata["size"]) is not int or not 0 <= metadata["size"] <= MAX_EXPANDED:
            raise ValueError(f"invalid file size: {name}")
        if type(metadata["mode"]) is not int or metadata["mode"] != (0o755 if name in BINARIES else 0o644):
            raise ValueError(f"unexpected file permissions: {name}")
        if not isinstance(metadata["sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", metadata["sha256"]) is None:
            raise ValueError(f"invalid file digest: {name}")
        total += metadata["size"]
    if total > MAX_EXPANDED:
        raise ValueError("package expands beyond its byte budget")
    return manifest


def unpack(archive, expected_sha, destination):
    if not isinstance(expected_sha, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None:
        raise ValueError("--sha256 must be the full trusted lowercase SHA256 of the archive")
    before = archive.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_ARCHIVE:
        raise ValueError("archive must be a regular non-symlink file within the 256 MiB budget")
    with archive.open("rb") as source, tempfile.TemporaryFile() as expanded:
        digest = hashlib.sha256()
        with tempfile.TemporaryFile() as frozen:
            copied = 0
            while block := source.read(1 << 20):
                copied += len(block)
                if copied > MAX_ARCHIVE:
                    raise ValueError("archive grew beyond its byte budget")
                digest.update(block)
                frozen.write(block)
            after = archive.lstat()
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError("archive changed while being captured")
            if digest.hexdigest() != expected_sha:
                raise ValueError("archive checksum mismatch; no package code was executed")
            frozen.seek(0)
            with gzip.GzipFile(fileobj=frozen, mode="rb") as compressed:
                size = 0
                while block := compressed.read(1 << 20):
                    size += len(block)
                    if size > MAX_EXPANDED + 8 * MAX_MANIFEST:
                        raise ValueError("compressed archive exceeds its expanded byte budget")
                    expanded.write(block)
        expanded.seek(0)
        with tarfile.open(fileobj=expanded, mode="r:") as package:
            members = {}
            for member in package:
                if len(members) >= 2049 or not safe_name(member.name) or member.type != tarfile.REGTYPE or member.pax_headers or member.offset_data - member.offset != 512:
                    raise ValueError("archive contains an unsafe path, link, extension, special file or too many members")
                if member.name in members or not 0 <= member.size <= MAX_EXPANDED:
                    raise ValueError("archive contains a duplicate path or invalid member size")
                members[member.name] = member
            end = max((member.offset_data + ((member.size + 511) // 512) * 512 for member in members.values()), default=0)
            expanded.seek(end)
            for block in iter(lambda: expanded.read(1 << 20), b""):
                if any(block):
                    raise ValueError("archive contains trailing nonzero data")
            member = members.get("manifest.json")
            if member is None or member.size > MAX_MANIFEST:
                raise ValueError("archive has no bounded package manifest")
            stream = package.extractfile(member)
            if stream is None:
                raise ValueError("manifest cannot be read")
            manifest = validate_manifest(json.loads(stream.read(), object_pairs_hook=unique_fields))
            if set(members) != set(manifest["files"]) | {"manifest.json"}:
                raise ValueError("archive inventory differs from its manifest")
            for name, metadata in manifest["files"].items():
                member = members[name]
                if member.size != metadata["size"]:
                    raise ValueError(f"archive file size differs from manifest: {name}")
                stream = package.extractfile(member)
                if stream is None:
                    raise ValueError(f"archive file cannot be read: {name}")
                target = destination / name
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                digest = hashlib.sha256()
                with target.open("xb") as output:
                    for block in iter(lambda: stream.read(1 << 20), b""):
                        digest.update(block)
                        output.write(block)
                    output.flush()
                    os.fsync(output.fileno())
                if digest.hexdigest() != metadata["sha256"]:
                    raise ValueError(f"package member checksum mismatch: {name}")
                target.chmod(metadata["mode"])
            atomic_json(destination / "manifest.json", manifest)
    return manifest


def current_target(prefix):
    current = prefix / "current"
    if not os.path.lexists(current):
        return None
    if not current.is_symlink():
        raise ValueError("current is not an installer-owned symlink")
    target = os.readlink(current)
    if re.fullmatch(r"versions/v[0-9A-Za-z.+-]+-[0-9a-f]{16}", target) is None:
        raise ValueError("current points outside the versioned installation layout")
    return target


def activate(prefix, target):
    temporary = prefix / f".current-{uuid.uuid4().hex}"
    try:
        temporary.symlink_to(target)
        os.replace(temporary, prefix / "current")
        sync_directory(prefix)
    finally:
        if os.path.lexists(temporary):
            temporary.unlink()


def verify_tree(directory):
    if directory.is_symlink() or directory.parent.is_symlink() or not directory.is_dir():
        raise ValueError("installed release is not an owned directory")
    manifest = validate_manifest(read_json(directory / "manifest.json"))
    for name, metadata in manifest["files"].items():
        path = directory / name
        for parent in path.parents:
            if parent == directory:
                break
            if parent.is_symlink() or not parent.is_dir():
                raise ValueError(f"installed release has an unsafe directory: {name}")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size != metadata["size"] or sha256(path) != metadata["sha256"]:
            raise ValueError(f"installed release was modified: {name}")
    return manifest


def recover(prefix):
    journal_path = prefix / ".install-journal.json"
    if not journal_path.exists():
        return False
    journal = read_json(journal_path, 8192)
    if not isinstance(journal, dict) or set(journal) != {"format", "old", "new", "phase"} or type(journal["format"]) is not int or journal["format"] != 1 or journal["phase"] not in ("prepared", "committed"):
        raise ValueError("invalid recovery journal; preserve the prefix for manual recovery")
    for target in (journal["old"], journal["new"]):
        if target is not None and (not isinstance(target, str) or re.fullmatch(r"versions/v[0-9A-Za-z.+-]+-[0-9a-f]{16}", target) is None):
            raise ValueError("recovery journal points outside the owned prefix")
    current = current_target(prefix)
    if current not in (journal["old"], journal["new"]):
        raise ValueError("installation changed concurrently; recovery preserves all versions")
    if journal["phase"] == "committed":
        if current != journal["new"]:
            raise ValueError("committed installation pointer changed; manual recovery required")
        verify_tree(prefix / current)
    elif current == journal["new"]:
        if journal["old"] is None:
            (prefix / "current").unlink()
            sync_directory(prefix)
        else:
            verify_tree(prefix / journal["old"])
            activate(prefix, journal["old"])
    journal_path.unlink()
    sync_directory(prefix)
    return True


def prepare_prefix(prefix):
    private_directory(prefix)
    marker = prefix / ".etcqlite-install.json"
    if marker.exists():
        if marker.is_symlink() or read_json(marker, 4096) != MARKER:
            raise ValueError("prefix does not have a recognized ownership marker")
    else:
        if any(prefix.iterdir()):
            raise ValueError("refusing to adopt a nonempty unmanaged prefix")
        atomic_json(marker, MARKER)


def install(args):
    prefix = Path(args.prefix).absolute()
    if not args.recover and (not args.allow_unsigned or not args.archive or not args.sha256):
        raise ValueError("installation needs --archive, a trusted --sha256 and explicit --allow-unsigned")
    prepare_prefix(prefix)
    with contextlib.ExitStack() as locks:
        locks.enter_context(locked_file(prefix / ".install.lock", True))
        for directory in args.data_dir:
            locks.enter_context(locked_file(Path(directory).absolute() / "node.lock", False))
        if args.recover:
            if current_target(prefix) is not None and not args.acknowledge_stopped:
                raise ValueError("stop all instances and use --acknowledge-stopped before recovering an activation")
            return {"event": "recovered", "changed": recover(prefix), "prefix": str(prefix)}
        if current_target(prefix) is not None and not args.acknowledge_stopped:
            raise ValueError("stop all instances using this prefix, then use --acknowledge-stopped and list their --data-dir locks")
        recover(prefix)
        old = current_target(prefix)
        old_manifest = verify_tree(prefix / old) if old else None
        with tempfile.TemporaryDirectory(prefix=".etcqlite-stage-", dir=prefix) as temporary:
            stage = Path(temporary)
            manifest = unpack(Path(args.archive).absolute(), args.sha256, stage)
            verify_binaries(stage, manifest)
            if old_manifest and compare_versions(manifest["version"], old_manifest["version"]) < 0:
                raise ValueError("automatic engine downgrades are not supported; use the documented fenced recovery process")
            versions = prefix / "versions"
            private_directory(versions)
            relative = f"versions/v{manifest['version']}-{args.sha256[:16]}"
            destination = prefix / relative
            if destination.exists():
                if verify_tree(destination) != manifest:
                    raise ValueError("existing package identity differs; no installed files were replaced")
            else:
                os.rename(stage, destination)
                sync_directory(versions)
            bin_link = prefix / "bin"
            if os.path.lexists(bin_link):
                if not bin_link.is_symlink() or os.readlink(bin_link) != "current/bin":
                    raise ValueError("refusing to replace an unmanaged bin directory")
            else:
                bin_link.symlink_to("current/bin")
                sync_directory(prefix)
            journal = {"format": 1, "old": old, "new": relative, "phase": "prepared"}
            atomic_json(prefix / ".install-journal.json", journal)
            try:
                activate(prefix, relative)
                verify_binaries(prefix / "current", manifest)
                journal["phase"] = "committed"
                atomic_json(prefix / ".install-journal.json", journal)
                recover(prefix)
            except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
                try:
                    recover(prefix)
                except (OSError, ValueError) as recovery_error:
                    raise RuntimeError(f"installation failed: {error}; recovery also failed: {recovery_error}; preserve {prefix}") from error
                raise
    return {"event": "installed", "prefix": str(prefix), "version": manifest["version"], "platform": manifest["platform"], "signing": manifest["signing"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True, help="new private prefix, never a system app directory")
    parser.add_argument("--archive")
    parser.add_argument("--sha256", help="trusted out-of-band archive digest; a checksum is not a signature")
    parser.add_argument("--allow-unsigned", action="store_true")
    parser.add_argument("--acknowledge-stopped", action="store_true")
    parser.add_argument("--data-dir", action="append", default=[], help="already initialized offline node; lock held through activation")
    parser.add_argument("--recover", action="store_true", help="recover only the owned interrupted installation journal")
    args = parser.parse_args()

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        print(json.dumps(install(args), sort_keys=True))
    except KeyboardInterrupt as error:
        parser.exit(130, f"etcqlite installer interrupted: {error}; use --recover with the same prefix\n")
    except (OSError, ValueError, RuntimeError, EOFError, tarfile.TarError, subprocess.SubprocessError) as error:
        parser.exit(1, f"etcqlite installer: {error}\n")
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
