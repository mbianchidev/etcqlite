#!/usr/bin/env python3
"""Build and verify a local development installer. Never commits, tags, merges or publishes."""

import sys
sys.dont_write_bytecode = True

import argparse
import base64
import datetime
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import selectors
import subprocess
import tarfile
import tempfile
import tomllib
import urllib.request
import uuid

import install


ROOT = Path(__file__).resolve().parent.parent


def command(args, cwd=ROOT, timeout=1800, capture=False, env=None):
    with subprocess.Popen(
        args, cwd=cwd, env=env, start_new_session=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    ) as child:
        try:
            stdout, stderr = child.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            try:
                os.killpg(child.pid, signal.SIGTERM)
                child.wait(timeout=10)
            except ProcessLookupError:
                pass
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=10)
            raise
        if child.returncode:
            detail = stderr.decode(errors="replace") if stderr else ""
            raise RuntimeError(f"step failed ({child.returncode}): {args!r}\n{detail}")
        return stdout


def git(*args):
    return command(["git", "--no-pager", "--no-optional-locks", *args], capture=True).decode().strip()


def stage_version(directory, chosen):
    manifest_path = directory / "Cargo.toml"
    text = manifest_path.read_text()
    parsed = tomllib.loads(text)
    if parsed["package"]["name"] != "etcqlite":
        raise ValueError("version operation is not targeting the Etcqlite package")
    previous = parsed["package"]["version"]
    text, count = re.subn(r'(?m)^version = "' + re.escape(previous) + r'"$', f'version = "{chosen}"', text, count=1)
    if count != 1:
        raise ValueError("cannot identify the package version without changing unrelated metadata")
    manifest_path.write_text(text)
    lock_path = directory / "Cargo.lock"
    text, count = re.subn(
        r'(\[\[package\]\]\nname = "etcqlite"\nversion = ")[^"]+(")',
        lambda match: match[1] + chosen + match[2], lock_path.read_text(),
    )
    if count != 1:
        raise ValueError("Cargo.lock must contain exactly one root package version")
    lock_path.write_text(text)


def version_warnings(previous, chosen):
    _, a, ap = install.parse_version(previous)
    _, b, bp = install.parse_version(chosen)
    comparison = install.compare_versions(chosen, previous)
    if comparison < 0:
        raise ValueError("local packaging does not automatically downgrade the current version")
    for label, old, new in zip(("major", "minor", "patch"), a, b):
        if old != new:
            return [f"{label} skips from {old} to {new}"] if new > old + 1 else []
    if ap and bp and len(ap) == len(bp) and ap[:-1] == bp[:-1] and ap[-1].isdigit() and bp[-1].isdigit() and int(bp[-1]) > int(ap[-1]) + 1:
        return [f"prerelease counter skips from {ap[-1]} to {bp[-1]}"]
    return []


def license_files(directory):
    matches = [
        path for path in directory.iterdir()
        if path.is_file() and not path.is_symlink()
        and re.fullmatch(r"(LICENSE|LICENCE|COPYING|NOTICE|COPYRIGHT)([-._A-Za-z0-9]*)", path.name, re.IGNORECASE)
    ]
    return sorted(matches)


def runtime_packages(cargo):
    packages = {package["id"]: package for package in cargo["packages"]}
    graph = {node["id"]: node for node in cargo["resolve"]["nodes"]}
    selected, pending = set(), [cargo["resolve"]["root"]]
    while pending:
        identity = pending.pop()
        if identity in selected:
            continue
        package = packages[identity]
        if any("proc-macro" in target["kind"] for target in package["targets"]):
            continue
        selected.add(identity)
        for dependency in graph[identity]["deps"]:
            if any(kind["kind"] is None for kind in dependency["dep_kinds"]):
                pending.append(dependency["pkg"])
    return [package for package in cargo["packages"] if package["id"] in selected]


def redistributable_notices(package, stage, sources):
    directory = Path(package["manifest_path"]).parent
    notices = license_files(directory)
    if notices:
        return notices
    identity = f"{package['name']}@{package['version']}"
    source = sources.get(identity)
    if source is None:
        raise ValueError(f"missing redistributable Rust dependency notices: {identity}")
    vcs = install.read_json(directory / ".cargo_vcs_info.json", 8192)
    if source["repository"] != package["repository"] or source["license"] != package["license"] or source["vcs_commit"] != vcs["git"]["sha1"]:
        raise ValueError(f"curated notice provenance no longer matches the locked dependency: {identity}")
    path = (stage / source["file"]).resolve()
    if not path.is_relative_to(stage.resolve()) or path.is_symlink() or install.sha256(path) != source["sha256"]:
        raise ValueError(f"curated notice content/containment verification failed: {identity}")
    print(f"Bundling verified workspace notice for {identity} from {source['source']}", file=sys.stderr)
    return [path]


def add_notices(stage, payload):
    host = re.search(r"(?m)^host: (.+)$", command(["rustc", "-vV"], capture=True).decode())
    if host is None:
        raise ValueError("cannot determine the native Rust target for dependency notices")
    cargo = json.loads(command(["cargo", "metadata", "--locked", "--format-version=1", "--filter-platform", host[1]], cwd=stage, capture=True))
    sources = install.read_json(stage / "script" / "notice_sources.json")
    for package in runtime_packages(cargo):
        if package["name"] == "etcqlite":
            continue
        notices = redistributable_notices(package, stage, sources)
        for notice in notices:
            target = payload / "licenses" / "rust" / f"{package['name']}-{package['version']}" / notice.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(notice, target)
    install.atomic_json(payload / "licenses" / "workspace-notice-sources.json", sources)
    sysroot = Path(command([os.environ.get("RUSTC", "rustc"), "--print", "sysroot"], capture=True).decode().strip())
    rust_notices = []
    for directory in (sysroot, sysroot / "share" / "doc" / "rust"):
        if directory.is_dir():
            rust_notices.extend(license_files(directory))
    if not rust_notices:
        raise ValueError("the installed Rust toolchain lacks standard-library redistribution notices")
    for notice in rust_notices:
        target = payload / "licenses" / "rust-stdlib" / notice.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(notice, target)
    module_data = command(["go", "-C", str(stage / "tools"), "list", "-mod=readonly", "-deps", "-json", "./snapshot"], capture=True).decode()
    decoder = json.JSONDecoder()
    modules = {}
    offset = 0
    while offset < len(module_data):
        while offset < len(module_data) and module_data[offset].isspace():
            offset += 1
        if offset == len(module_data):
            break
        package, offset = decoder.raw_decode(module_data, offset)
        module = package.get("Module")
        if module and not module.get("Main", False):
            modules[module["Path"]] = module
    for name, module in modules.items():
        notices = license_files(Path(module["Dir"]))
        if not notices:
            raise ValueError(f"missing redistributable Go dependency notices: {name}")
        for notice in notices:
            target = payload / "licenses" / "go" / name / notice.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(notice, target)
    go_root = Path(command(["go", "env", "GOROOT"], capture=True).decode().strip())
    go_license = next((path for path in (go_root / "LICENSE", go_root.parent / "LICENSE") if path.is_file() and not path.is_symlink()), None)
    if go_license is None:
        raise ValueError("the installed Go toolchain lacks its standard-library license (SDK root or Homebrew installation root)")
    target = payload / "licenses" / "go" / "stdlib" / "LICENSE"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(go_license, target)
    for relative in ("vendor/omnipaxos/LICENSE", "proto/etcd/LICENSE", "proto/google/LICENSE", "proto/protoc-gen-openapiv2/LICENSE"):
        target = payload / "licenses" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(stage / relative, target)


def write_package(payload, output, manifest):
    inventory = {}
    for path in sorted(payload.rglob("*")):
        if path.is_symlink():
            raise ValueError("package payload contains an unexpected symlink")
        if path.is_file():
            relative = path.relative_to(payload).as_posix()
            if not install.safe_name(relative):
                raise ValueError(f"unsupported package member name: {relative}")
            inventory[relative] = {"size": path.stat().st_size, "sha256": install.sha256(path), "mode": 0o755 if relative in install.BINARIES else 0o644}
    manifest["files"] = inventory
    install.validate_manifest(manifest)
    temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}")
    try:
        with tarfile.open(temporary, "w:gz", format=tarfile.USTAR_FORMAT) as package:
            data = json.dumps(manifest, sort_keys=True).encode()
            member = tarfile.TarInfo("manifest.json")
            member.mode, member.size = 0o644, len(data)
            package.addfile(member, io.BytesIO(data))
            for name, metadata in inventory.items():
                member = tarfile.TarInfo(name)
                member.mode, member.size = metadata["mode"], metadata["size"]
                with (payload / name).open("rb") as source:
                    package.addfile(member, source)
        with temporary.open("rb") as source:
            os.fsync(source.fileno())
        if output.exists():
            raise ValueError("refusing to overwrite an existing installer")
        os.rename(temporary, output)
        install.sync_directory(output.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def verify_quickstart(prefix, work):
    executable = prefix / "bin" / "etcqlite"
    node = work / "quickstart-node"
    environment = {"PATH": os.environ.get("PATH", ""), "HOME": str(work), "RUST_LOG": "info"}
    subprocess.run([str(executable), "init", "--data-dir", str(node), "--name", "mock-quickstart"], env=environment, check=True, capture_output=True, timeout=30)
    with (work / "quickstart-stderr.log").open("wb") as errors:
        child = subprocess.Popen(
            [str(executable), "serve", "--data-dir", str(node), "--listen", "127.0.0.1:0"],
            env=environment, stdout=subprocess.PIPE, stderr=errors,
        )
        failure = None
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=20):
                    raise ValueError("isolated quickstart did not report startup within 20 seconds")
                line = child.stdout.readline()
            started = json.loads(line)
            endpoint = started.get("endpoint", "")
            if started.get("event") != "started" or re.fullmatch(r"http://127\.0\.0\.1:[0-9]+", endpoint) is None:
                raise ValueError("isolated quickstart returned an unexpected endpoint")
            client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with client.open(endpoint + "/readyz", timeout=10) as response:
                if response.read(1024).strip() != b"ok":
                    raise ValueError("installed quickstart readiness failed")
            key = base64.b64encode(b"mock/installer-quickstart").decode()
            value = base64.b64encode(b"synthetic-only").decode()
            for operation, body in (("put", {"key": key, "value": value}), ("range", {"key": key})):
                request = urllib.request.Request(endpoint + "/v3/kv/" + operation, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
                with client.open(request, timeout=10) as response:
                    result = json.loads(response.read(1 << 20))
                if operation == "range" and (len(result.get("kvs", [])) != 1 or result["kvs"][0]["value"] != value):
                    raise ValueError("installed quickstart did not round-trip the synthetic KV")
            log = node / "logs" / "app.log"
            if not log.is_file() or log.stat().st_mode & 0o077:
                raise ValueError("installed service did not create its private default log")
        except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
            failure = error
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired as error:
                    child.kill()
                    child.wait(timeout=10)
                    raise ValueError(f"{failure or 'quickstart'}; installed service failed to drain on SIGTERM") from error
            if child.stdout is not None:
                child.stdout.close()
        if failure is not None or child.returncode != 0:
            errors.flush()
            with (work / "quickstart-stderr.log").open("rb") as source:
                source.seek(max(0, (work / "quickstart-stderr.log").stat().st_size - 8192))
                diagnostics = source.read().decode(errors="replace")
            if isinstance(failure, KeyboardInterrupt):
                raise failure
            raise ValueError(f"installed quickstart failed: {failure or child.returncode}\n{diagnostics}") from failure


def run(args):
    current = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    chosen = install.parse_version(args.version or current)[0]
    warnings = version_warnings(current, chosen)
    source = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
    target = install.host_platform()
    output = Path(args.output).absolute() if args.output else ROOT / "target" / "dist"
    plan = {
        "mode": "local-development-only", "version": chosen, "platform": target,
        "source_commit": source, "source_dirty": dirty or chosen != current,
        "version_override": chosen != current, "output": str(output), "warnings": warnings,
        "signing": "unsigned-development", "publication": "disabled",
    }
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)
    if args.dry_run:
        return plan
    if dirty:
        raise ValueError("commit or preserve unrelated working-tree changes first; no versions were modified")
    if output.is_relative_to(ROOT):
        check = subprocess.run(["git", "check-ignore", "--quiet", "--", str(output)], cwd=ROOT, check=False)
        if check.returncode == 1:
            raise ValueError("an output inside the checkout must be Git-ignored; use target/dist or an external private directory")
        if check.returncode != 0:
            raise ValueError("cannot verify that the packaging output is excluded from source inputs")
    for executable in ("cargo", "rustc", "go", "make", "git"):
        if shutil.which(executable) is None:
            raise ValueError(f"required packaging tool is missing: {executable}")
    install.private_directory(output)
    with install.locked_file(output / ".release.lock", True):
        with tempfile.TemporaryDirectory(prefix=".etcqlite-release-", dir=output) as temporary:
            work = Path(temporary)
            stage, payload = work / "source", work / "payload"
            stage.mkdir(mode=0o700)
            payload.mkdir(mode=0o700)
            archive = command(["git", "--no-pager", "archive", "--format=tar", source], capture=True)
            with tarfile.open(fileobj=io.BytesIO(archive)) as source_tar:
                source_tar.extractall(stage, filter="data")
            stage_version(stage, chosen)
            add_notices(stage, payload)
            env = dict(os.environ, GIT_PAGER="cat", GH_PAGER="cat", PAGER="cat")
            env["CARGO_TARGET_DIR"] = str(ROOT / "target" / "distribution-build")
            env["CARGO_BUILD_JOBS"] = str(args.jobs)
            env["GOMAXPROCS"] = str(args.jobs)
            command([
                "make", "-C", str(stage), "build",
                f"ETCQLITE_BUILD_SHA={source}", f"ETCQLITE_BUILD_DIRTY={str(plan['source_dirty']).lower()}",
                f"VERSION={chosen}",
            ], env=env)
            binaries = payload / "bin"
            binaries.mkdir(mode=0o700)
            shutil.copyfile(Path(env["CARGO_TARGET_DIR"]) / "release" / "etcqlite", binaries / "etcqlite")
            shutil.copyfile(stage / "target" / "release" / "etcqlite-snapshot", binaries / "etcqlite-snapshot")
            for binary in binaries.iterdir():
                binary.chmod(0o755)
            for name in ("README.md", "LICENSE.md", "AGENTS.md", "CONTRIBUTING.md", "CODE_OF_CONDUCT.md", "SECURITY.md", "SUPPORT.md", "ACCESSIBILITY.md"):
                shutil.copyfile(stage / name, payload / name)
            shutil.copyfile(stage / "script" / "install.py", payload / "install.py")
            shutil.copytree(stage / "docs", payload / "docs")
            manifest = {
                "format": 1, "kind": "etcqlite-development-package", "version": chosen,
                "platform": target, "source_commit": source, "source_dirty": plan["source_dirty"],
                "repository": "mbianchidev/etcqlite", "signing": "unsigned-development", "files": {},
            }
            install.verify_binaries(payload, manifest)
            name = f"etcqlite-{chosen}-{target}-{source[:12]}-{uuid.uuid4().hex[:8]}.tar.gz"
            package = work / name
            write_package(payload, package, manifest)
            digest = install.sha256(package)
            prefix = work / "isolated-prefix"
            test_args = argparse.Namespace(
                prefix=str(prefix), archive=str(package), sha256=digest, allow_unsigned=True,
                acknowledge_stopped=False, data_dir=[], recover=False,
            )
            installed = install.install(test_args)
            if installed["version"] != chosen:
                raise ValueError("isolated package installation did not retain the chosen version")
            verify_quickstart(prefix, work)
            if git("rev-parse", "HEAD") != source or git("status", "--porcelain", "--untracked-files=normal"):
                raise ValueError("source changed during packaging; candidate not handed off")
            final = output / name
            os.rename(package, final)
            install.sync_directory(output)
            provenance = {
                **plan, "artifact": str(final), "sha256": digest, "bytes": final.stat().st_size,
                "development_artifact": True, "owner_worktree": str(ROOT),
                "owner_branch": git("branch", "--show-current"), "session_id": args.session_id,
                "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "isolated_install_verified": True, "quickstart_kv_and_logging_verified": True,
            }
            install.atomic_json(final.with_suffix(final.suffix + ".provenance.json"), provenance)
            checksum = final.with_suffix(final.suffix + ".sha256")
            with checksum.open("x") as stream:
                stream.write(f"{digest}  {name}\n")
                stream.flush()
                os.fsync(stream.fileno())
            print("Local developer package only: no tag, release, upload or installed app was changed.", file=sys.stderr)
            return provenance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", help="SemVer, with an optional lowercase v; defaults to Cargo.toml")
    parser.add_argument("--dry-run", action="store_true", help="read-only preview; never builds or changes versions")
    parser.add_argument("--output", help="private output directory; defaults to this repository's target/dist")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--session-id", help="actual owning session ID when available; never inferred")
    args = parser.parse_args()
    if not 1 <= args.jobs <= 32:
        parser.error("--jobs must be between 1 and 32")

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        print(json.dumps(run(args), sort_keys=True))
    except KeyboardInterrupt as error:
        parser.exit(130, f"local packaging interrupted: {error}; original version files are unchanged\n")
    except (OSError, ValueError, RuntimeError, tarfile.TarError, subprocess.SubprocessError) as error:
        parser.exit(1, f"local packaging failed: {error}\n")
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
