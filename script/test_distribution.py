import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from unittest import mock

import install
import release


class DistributionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def package(self, version="0.1.0", mutate=None):
        payload = self.root / f"payload-{len(list(self.root.iterdir()))}"
        payload.mkdir()
        (payload / "bin").mkdir()
        target = install.host_platform()
        for name in install.BINARIES:
            header = bytearray(64)
            if target.startswith("darwin"):
                header[:4] = b"\xcf\xfa\xed\xfe"
                struct.pack_into("<I", header, 4, 0x100000C if target.endswith("arm64") else 0x1000007)
            else:
                header[:6] = b"\x7fELF\x02\x01"
                struct.pack_into("<H", header, 18, 183 if target.endswith("arm64") else 62)
            (payload / name).write_bytes(header + f"\nVERSION={version}\n".encode())
        (payload / "LICENSE.md").write_text("Synthetic license fixture\n")
        manifest = {
            "format": 1, "kind": "etcqlite-development-package", "version": version,
            "platform": target, "source_commit": "a" * 40, "source_dirty": False,
            "repository": "mbianchidev/etcqlite", "signing": "unsigned-development", "files": {},
        }
        archive = self.root / f"package-{payload.name}.tar.gz"
        release.write_package(payload, archive, manifest)
        if mutate:
            self.mutate(archive, mutate)
        return archive, install.sha256(archive), manifest

    def mutate(self, archive, change):
        members = []
        with install.tarfile.open(archive, "r:gz") as source:
            for entry in source:
                data = source.extractfile(entry).read()
                members.append((entry, data))
        members = change(members)
        with install.tarfile.open(archive, "w:gz", format=install.tarfile.USTAR_FORMAT) as output:
            for entry, data in members:
                entry.size = len(data)
                output.addfile(entry, io.BytesIO(data) if entry.isfile() else None)

    def args(self, archive=None, digest=None, **options):
        return argparse.Namespace(
            prefix=str(self.root / "private-prefix"), archive=str(archive) if archive else None,
            sha256=digest, allow_unsigned=True, acknowledge_stopped=options.get("acknowledge_stopped", False),
            data_dir=options.get("data_dir", []), recover=options.get("recover", False),
        )

    @staticmethod
    def execute(command, **_kwargs):
        path = Path(command[0])
        version = path.read_bytes().split(b"\nVERSION=")[1].splitlines()[0].decode()
        return subprocess.CompletedProcess(command, 0, stdout=f"{path.name} {version}\n")

    def install(self, version="0.1.0", **options):
        archive, digest, manifest = self.package(version)
        args = self.args(archive, digest, **options)
        with mock.patch.object(install.subprocess, "run", side_effect=self.execute):
            result = install.install(args)
        return args, result, manifest

    def test_complete_install_update_and_identical_retry_keep_versions_paired(self):
        self.install()
        args, result, _ = self.install("0.2.0", acknowledge_stopped=True)
        prefix = Path(args.prefix)
        with mock.patch.object(install.subprocess, "run", side_effect=self.execute):
            retry = install.install(args)
        self.assertEqual(result["version"], retry["version"])
        self.assertEqual(install.verify_tree((prefix / "current").resolve())["version"], "0.2.0")
        self.assertEqual(len(list((prefix / "versions").iterdir())), 2)
        self.assertEqual(os.readlink(prefix / "bin"), "current/bin")
        self.assertFalse((prefix / ".install-journal.json").exists())

    def test_update_requires_stop_attestation_and_does_not_retarget_live_nodes(self):
        self.install()
        archive, digest, _ = self.package("0.2.0")
        args = self.args(archive, digest)
        old = install.current_target(Path(args.prefix))
        with self.assertRaisesRegex(ValueError, "stop all instances"):
            install.install(args)
        node = self.root / "mock-node"
        node.mkdir()
        with install.locked_file(node / "node.lock", True):
            args.acknowledge_stopped = True
            args.data_dir = [str(node)]
            with self.assertRaisesRegex(ValueError, "active or already locked"):
                install.install(args)
        self.assertEqual(install.current_target(Path(args.prefix)), old)

    def test_checksum_failures_execute_no_code_and_preserve_the_old_installation(self):
        self.install()
        archive, _, _ = self.package("0.2.0")
        args = self.args(archive, "0" * 64, acknowledge_stopped=True)
        old = install.current_target(Path(args.prefix))
        with mock.patch.object(install.subprocess, "run") as execute:
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                install.install(args)
            execute.assert_not_called()
        self.assertEqual(install.current_target(Path(args.prefix)), old)

    def test_traversal_absolute_paths_duplicates_and_links_are_rejected_before_execution(self):
        def renamed(name):
            def change(members):
                members[1][0].name = name
                return members
            return change

        def symlink(members):
            member, _ = members[1]
            member.type = install.tarfile.SYMTYPE
            member.linkname = "/outside/mock"
            members[1] = (member, b"")
            return members

        def duplicate(members):
            return members + [members[1]]

        for mutation in [renamed("../escape"), renamed("/absolute"), renamed("C:\\escape"), renamed("bin/./etcqlite"), symlink, duplicate]:
            with self.subTest(mutation=mutation):
                archive, digest, _ = self.package(mutate=mutation)
                destination = self.root / f"stage-{len(list(self.root.iterdir()))}"
                destination.mkdir()
                with mock.patch.object(install.subprocess, "run") as execute:
                    with self.assertRaises(ValueError):
                        install.unpack(archive, digest, destination)
                    execute.assert_not_called()
        self.assertFalse((self.root.parent / "escape").exists())

    def test_bad_member_hash_and_duplicate_manifest_fields_are_rejected(self):
        def tampered(members):
            entry, data = members[1]
            members[1] = (entry, data + b"bad")
            return members

        def duplicate_field(members):
            entry, data = members[0]
            members[0] = (entry, data[:-1] + b', "format":1}')
            return members

        for change in (tampered, duplicate_field):
            archive, digest, _ = self.package(mutate=change)
            destination = self.root / f"stage-{len(list(self.root.iterdir()))}"
            destination.mkdir()
            with self.assertRaises(ValueError):
                install.unpack(archive, digest, destination)

    def test_compressed_budget_and_trailing_nonzero_data_are_not_ignored(self):
        archive, _, _ = self.package()
        raw = gzip.decompress(archive.read_bytes())
        archive.write_bytes(gzip.compress(raw + b"malicious trailing data"))
        with self.assertRaisesRegex(ValueError, "trailing nonzero"):
            install.unpack(archive, install.sha256(archive), self.root / "stage")
        archive, digest, _ = self.package()
        with mock.patch.object(install, "MAX_ARCHIVE", 10):
            with self.assertRaisesRegex(ValueError, "256 MiB budget"):
                install.unpack(archive, digest, self.root / "stage")
        archive.write_bytes(gzip.compress(b"\0" * (9 << 20)))
        with mock.patch.object(install, "MAX_EXPANDED", 64):
            with self.assertRaisesRegex(ValueError, "expanded byte budget"):
                install.unpack(archive, install.sha256(archive), self.root / "stage")

    def test_interruption_after_pointer_swap_rolls_back_without_deleting_either_release(self):
        self.install()
        archive, digest, _ = self.package("0.2.0")
        args = self.args(archive, digest, acknowledge_stopped=True)
        prefix = Path(args.prefix)
        old = install.current_target(prefix)
        original = install.activate
        calls = 0

        def interrupted(prefix, target):
            nonlocal calls
            original(prefix, target)
            calls += 1
            if calls == 1:
                raise KeyboardInterrupt("mock interruption")

        with mock.patch.object(install.subprocess, "run", side_effect=self.execute), \
                mock.patch.object(install, "activate", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                install.install(args)
        self.assertEqual(install.current_target(prefix), old)
        self.assertEqual(len(list((prefix / "versions").iterdir())), 2)
        self.assertFalse((prefix / ".install-journal.json").exists())

    def test_uncatchable_interruption_journal_can_restore_the_previous_pointer(self):
        old_args, _, _ = self.install()
        prefix = Path(old_args.prefix)
        old = install.current_target(prefix)
        self.install("0.2.0", acknowledge_stopped=True)
        new = install.current_target(prefix)
        install.atomic_json(prefix / ".install-journal.json", {"format": 1, "phase": "prepared", "old": old, "new": new})
        self.assertTrue(install.recover(prefix))
        self.assertEqual(install.current_target(prefix), old)
        self.assertEqual(len(list((prefix / "versions").iterdir())), 2)

    def test_committed_journal_recovery_keeps_a_verified_new_version(self):
        args, _, _ = self.install()
        prefix = Path(args.prefix)
        current = install.current_target(prefix)
        install.atomic_json(prefix / ".install-journal.json", {"format": 1, "phase": "committed", "old": None, "new": current})
        self.assertTrue(install.recover(prefix))
        self.assertEqual(install.current_target(prefix), current)

    def test_failed_activation_and_modified_installations_do_not_get_hidden(self):
        args, _, _ = self.install()
        prefix = Path(args.prefix)
        old = install.current_target(prefix)
        archive, digest, _ = self.package("0.2.0")
        update = self.args(archive, digest, acknowledge_stopped=True)
        with mock.patch.object(install.subprocess, "run", side_effect=self.execute), \
                mock.patch.object(install, "activate", side_effect=OSError("mock filesystem denied rename")):
            with self.assertRaisesRegex(OSError, "denied rename"):
                install.install(update)
        self.assertEqual(install.current_target(prefix), old)
        (prefix / "current" / "bin" / "etcqlite").write_bytes(b"modified")
        with self.assertRaisesRegex(ValueError, "was modified"):
            install.install(update)

    def test_unmanaged_prefix_unsigned_trust_and_bad_architectures_fail_closed(self):
        archive, digest, manifest = self.package()
        args = self.args(archive, digest)
        args.allow_unsigned = False
        with self.assertRaisesRegex(ValueError, "explicit --allow-unsigned"):
            install.install(args)
        self.assertFalse(Path(args.prefix).exists())
        Path(args.prefix).mkdir(mode=0o700)
        (Path(args.prefix) / "notes.txt").write_text("preserve")
        args.allow_unsigned = True
        with self.assertRaisesRegex(ValueError, "unmanaged"):
            install.install(args)
        self.assertEqual((Path(args.prefix) / "notes.txt").read_text(), "preserve")
        invalid = self.root / "invalid-executable"
        invalid.write_bytes(b"not executable")
        with self.assertRaisesRegex(ValueError, "corrupt native executable"):
            install.executable_platform(invalid)
        manifest["source_dirty"] = "false"
        with self.assertRaises(ValueError):
            install.validate_manifest(manifest)

    def test_recovery_refuses_foreign_paths_and_concurrent_pointer_changes(self):
        args, _, _ = self.install()
        prefix = Path(args.prefix)
        old = install.current_target(prefix)
        install.atomic_json(prefix / ".install-journal.json", {"format": 1, "phase": "prepared", "old": old, "new": "../../outside"})
        with self.assertRaisesRegex(ValueError, "outside"):
            install.recover(prefix)
        self.assertEqual(install.current_target(prefix), old)

    def test_hash_valid_package_with_false_embedded_version_executes_no_activation(self):
        def incorrect_version(members):
            rewritten = []
            manifest = json.loads(members[0][1])
            for entry, data in members[1:]:
                if entry.name in install.BINARIES:
                    data = data.replace(b"VERSION=0.1.0", b"VERSION=0.2.0")
                    manifest["files"][entry.name]["sha256"] = hashlib.sha256(data).hexdigest()
                rewritten.append((entry, data))
            return [(members[0][0], json.dumps(manifest).encode()), *rewritten]

        archive, digest, _ = self.package(mutate=incorrect_version)
        args = self.args(archive, digest)
        with mock.patch.object(install.subprocess, "run", side_effect=self.execute):
            with self.assertRaisesRegex(ValueError, "embedded version mismatch"):
                install.install(args)
        self.assertIsNone(install.current_target(Path(args.prefix)))


class ReleaseTests(unittest.TestCase):
    def test_semver_normalization_precedence_and_exact_gap_cases(self):
        for bad in ["1", "1.2", "01.2.3", "1.2.3-01", "vV1.2.3", "1.2.3_", "1.2.3-a..b", "1.2.3\n"]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                install.parse_version(bad)
        self.assertEqual(install.parse_version("v1.2.4-rc.2+build.007")[0], "1.2.4-rc.2+build.007")
        for previous, chosen, warning in [
            ("0.1.0", "0.3.0", True), ("0.1.0", "1.0.0", False),
            ("0.1.9", "0.2.0", False), ("1.0.0", "1.0.3", True),
            ("1.0.0-rc.1", "1.0.0-rc.3", True), ("v1.0.0", "1.0.0", False),
        ]:
            self.assertEqual(bool(release.version_warnings(previous, chosen)), warning)
        with self.assertRaises(ValueError):
            release.version_warnings("1.0.0", "0.9.9")
        self.assertEqual(install.compare_versions("1.0.0+build.1", "1.0.0+build.2"), 0)

    def test_version_staging_only_changes_the_root_package_and_lockfile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text('[package]\nname = "etcqlite"\nversion = "0.1.0"\n[dependencies]\nmock = "=1.2.3"\n')
            (root / "Cargo.lock").write_text('[[package]]\nname = "etcqlite"\nversion = "0.1.0"\n[[package]]\nname = "mock"\nversion = "1.2.3"\n')
            release.stage_version(root, "0.2.0-rc.1+build.007")
            self.assertIn('version = "0.2.0-rc.1+build.007"', (root / "Cargo.toml").read_text())
            self.assertIn('name = "mock"\nversion = "1.2.3"', (root / "Cargo.lock").read_text())
            self.assertEqual(sorted(path.name for path in root.iterdir()), ["Cargo.lock", "Cargo.toml"])

    def test_dry_run_has_no_filesystem_or_build_or_remote_side_effects(self):
        args = argparse.Namespace(version="0.1.0", output=None, jobs=2, dry_run=True, session_id=None)
        before = release.git("status", "--porcelain", "--untracked-files=normal")
        with mock.patch.object(release, "stage_version") as stage, \
                mock.patch.object(release.install, "private_directory") as create, \
                mock.patch.object(release, "write_package") as package:
            result = release.run(args)
        stage.assert_not_called()
        create.assert_not_called()
        package.assert_not_called()
        self.assertEqual(result["publication"], "disabled")
        self.assertEqual(release.git("status", "--porcelain", "--untracked-files=normal"), before)

    def test_real_help_runs_from_an_unrelated_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            for name in ("release.sh", "install.sh"):
                result = subprocess.run(["bash", str(Path(__file__).with_name(name)), "--help"], cwd=directory, text=True, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage:", result.stdout)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_failed_local_build_preserves_source_versions_and_cleans_only_owned_staging(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            args = argparse.Namespace(version="0.1.0", output=str(output), jobs=2, dry_run=False, session_id=None)
            before = {name: (release.ROOT / name).read_bytes() for name in ("Cargo.toml", "Cargo.lock")}
            original = release.command

            def failed(args, **options):
                if args[:2] == ["git", "--no-pager"] and "archive" in args:
                    raise RuntimeError("mock failed archive/build step")
                return original(args, **options)

            with mock.patch.object(release, "git", side_effect=lambda *args: "a" * 40 if args[0] == "rev-parse" else ""), \
                    mock.patch.object(release, "command", side_effect=failed):
                with self.assertRaisesRegex(RuntimeError, "mock failed"):
                    release.run(args)
            self.assertEqual(sorted(path.name for path in output.iterdir()), [".release.lock"])
            for name, data in before.items():
                self.assertEqual((release.ROOT / name).read_bytes(), data)


if __name__ == "__main__":
    unittest.main()
