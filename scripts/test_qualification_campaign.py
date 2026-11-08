import importlib.util
import json
import pathlib
import signal
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("qualification_campaign.py")
SPEC = importlib.util.spec_from_file_location("qualification_campaign", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Collector:
    def __init__(self, clock, exit_at, exit_code):
        self.clock = clock
        self.exit_at = exit_at
        self.exit_code = exit_code

    def poll(self):
        return self.exit_code if self.clock.now >= self.exit_at else None


class SupervisionTests(unittest.TestCase):
    def test_collector_failure_prevents_further_pauses(self):
        clock = Clock()
        collector = Collector(clock, 3.0, 1)
        pauses = []
        events = []
        result = campaign.supervise(
            collector, lambda: pauses.append(clock.now), 20.0, 5.0,
            lambda kind, **fields: events.append((kind, fields)),
            clock.monotonic, clock.sleep, wall_clock=clock.monotonic,
        )
        self.assertEqual(result, 1)
        self.assertEqual(pauses, [])
        self.assertEqual(events[-1][0], "collector_exit")
        self.assertEqual(events[-1][1]["code"], 1)

    def test_collector_exit_between_ticks_is_checked_before_signal(self):
        clock = Clock()
        collector = Collector(clock, 7.0, 1)
        pauses = []
        result = campaign.supervise(
            collector, lambda: pauses.append(clock.now), 20.0, 5.0,
            lambda *_args, **_fields: None, clock.monotonic, clock.sleep,
            wall_clock=clock.monotonic,
        )
        self.assertEqual(result, 1)
        self.assertEqual(pauses, [5.0])

    def test_successful_early_completion_does_not_continue_faulting(self):
        clock = Clock()
        pauses = []
        result = campaign.supervise(
            Collector(clock, 2.0, 0), lambda: pauses.append(clock.now), 20.0, 5.0,
            lambda *_args, **_fields: None, clock.monotonic, clock.sleep,
            wall_clock=clock.monotonic,
        )
        self.assertEqual(result, 0)
        self.assertEqual(pauses, [])

    def test_pause_failure_is_not_converted_to_completion(self):
        clock = Clock()

        def failed_pause():
            raise RuntimeError("mock owned process changed")

        with self.assertRaisesRegex(RuntimeError, "mock owned process changed"):
            campaign.supervise(
                Collector(clock, 30.0, 0), failed_pause, 20.0, 5.0,
                lambda *_args, **_fields: None, clock.monotonic, clock.sleep,
                wall_clock=clock.monotonic,
            )

    def test_delayed_pause_does_not_replay_an_overdue_fault_burst(self):
        clock = Clock()
        pauses = []

        def delayed_pause():
            pauses.append(clock.now)
            clock.sleep(8.0)

        campaign.supervise(
            Collector(clock, 18.0, 0), delayed_pause, 30.0, 5.0,
            lambda *_args, **_fields: None, clock.monotonic, clock.sleep,
            wall_clock=clock.monotonic,
        )
        self.assertEqual(pauses, [5.0])

    def test_schedule_deadline_drains_without_further_pauses(self):
        clock = Clock()
        pauses = []
        events = []
        result = campaign.supervise(
            Collector(clock, 9.0, 0), lambda: pauses.append(clock.now), 6.0, 3.0,
            lambda kind, **fields: events.append((kind, fields)),
            clock.monotonic, clock.sleep, wall_clock=clock.monotonic, drain_seconds=3,
        )
        self.assertEqual(result, 0)
        self.assertEqual(pauses, [3.0])
        self.assertEqual([kind for kind, _ in events], ["fault_schedule_complete", "collector_exit"])

    def test_unfinished_collector_has_a_bounded_drain_deadline(self):
        clock = Clock()
        with self.assertRaisesRegex(campaign.CampaignError, "drain deadline"):
            campaign.supervise(
                Collector(clock, 100.0, 0), lambda: None, 4.0, 5.0,
                lambda *_args, **_fields: None, clock.monotonic, clock.sleep,
                wall_clock=clock.monotonic, drain_seconds=3,
            )
        self.assertEqual(clock.now, 7.0)

    def test_unplanned_scheduling_gap_stops_before_the_next_pause(self):
        clock = Clock()
        pauses = []
        events = []

        def suspended_sleep(_seconds):
            clock.now += 7.0

        with self.assertRaisesRegex(campaign.CampaignError, "clock continuity"):
            campaign.supervise(
                Collector(clock, 100.0, 0), lambda: pauses.append(clock.now), 30.0, 10.0,
                lambda kind, **fields: events.append((kind, fields)),
                clock.monotonic, suspended_sleep, wall_clock=clock.monotonic,
            )
        self.assertEqual(pauses, [])
        self.assertEqual(events[-1][0], "clock_discontinuity")

    def test_wall_clock_discontinuity_is_not_counted_as_qualification_time(self):
        clock = Clock()
        pauses = []
        with self.assertRaisesRegex(campaign.CampaignError, "clock continuity"):
            campaign.supervise(
                Collector(clock, 100.0, 0), lambda: pauses.append(clock.now), 30.0, 10.0,
                lambda *_args, **_fields: None, clock.monotonic, clock.sleep,
                wall_clock=lambda: clock.now + (6.0 if clock.now >= 2.0 else 0.0),
            )
        self.assertEqual(pauses, [])


class ProfileTests(unittest.TestCase):
    def profile(self):
        return {
            "production_claim": False,
            "requested_seconds": 60,
            "fault_interval_seconds": 10,
            "fault_pause_seconds": 2,
            "seed": 42,
            "concurrency": 4,
            "data_relative_path": "etcqlite-qualification/mock/node",
            "executable_relative_path": "etcqlite-qualification/mock/etcqlite",
            "unit": "etcqlite-qual-mock.service",
            "linux_binary_sha256": "a" * 64,
            "candidate": "b" * 40,
            "nodes": [
                {"vm": f"etcqlite-qual-mock-{i}", "pid": 100 + i, "birth_ticks": 200 + i}
                for i in range(1, 4)
            ],
        }

    def load(self, profile):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "mock-profile.json"
            path.write_text(json.dumps(profile))
            return campaign.load_profile(path)

    def test_profile_and_signal_commands_pin_only_owned_resources(self):
        profile = self.load(self.profile())
        command = campaign.pause_command(profile, profile["nodes"][0])
        self.assertIn("/proc/101/stat", command)
        self.assertIn("= 201", command)
        self.assertIn("/bin/kill -STOP 101", command)
        self.assertIn("/bin/kill -CONT 101", command)
        self.assertIn("trap resume EXIT HUP INT TERM", command)
        self.assertIn(f"/proc/{profile['nodes'][0]['pid']}/cmdline", command)
        self.assertIn("--data-dir", command)
        self.assertNotIn("pkill", command)

    def test_invalid_scope_types_and_path_traversal_are_rejected(self):
        for field, value in [
            ("unit", []), ("candidate", None), ("linux_binary_sha256", {}),
            ("requested_seconds", True),
            ("seed", 1 << 63),
            ("unit", "etcqlite-existing-workload.service"),
            ("executable_relative_path", "etcqlite-qualification/../mock/etcqlite"),
            ("executable_relative_path", "etcqlite-qualification/mock/\netcqlite"),
            ("data_relative_path", "/home/mock/node"),
            ("status", "failed"),
        ]:
            profile = self.profile()
            profile[field] = value
            with self.assertRaises(ValueError):
                self.load(profile)
        profile = self.profile()
        profile["nodes"][0]["vm"] = "porto-existing-workload"
        with self.assertRaises(ValueError):
            self.load(profile)
        profile = self.profile()
        profile["nodes"][1]["vm"] = profile["nodes"][0]["vm"]
        with self.assertRaises(ValueError):
            self.load(profile)

    def test_duplicate_json_fields_are_rejected_before_any_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "mock-profile.json"
            payload = json.dumps(self.profile())
            path.write_text(payload[:-1] + ', "seed": 43}')
            with self.assertRaisesRegex(ValueError, "duplicate"):
                campaign.load_profile(path)

    def test_nonstandard_numbers_and_oversized_profiles_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "mock-profile.json"
            path.write_text(json.dumps(self.profile())[:-1] + ', "extra": NaN}')
            with self.assertRaises(ValueError):
                campaign.load_profile(path)
            path.write_text(" " * (64 * 1024 + 1))
            with self.assertRaisesRegex(ValueError, "64 KiB"):
                campaign.load_profile(path)
            path.write_text('{"extra":' + "[" * 1200 + "0" + "]" * 1200 + "}")
            with self.assertRaises(ValueError):
                campaign.load_profile(path)
            with mock.patch.object(campaign.json, "loads", side_effect=RecursionError), \
                    self.assertRaisesRegex(ValueError, "nesting bound"):
                campaign.load_profile(path)


class EntrypointTests(unittest.TestCase):
    def arguments(self, directory):
        root = pathlib.Path(directory)
        profile = root / "mock-profile.json"
        profile.write_text(json.dumps(ProfileTests().profile()))
        return types.SimpleNamespace(
            owned_fixture=True, profile=str(profile), qualifier="/usr/bin/false",
            history=str(root / "mock-history.jsonl"), fault_log=str(root / "mock-fault.jsonl"),
            endpoints="https://mock-1:2379,https://mock-2:2379,https://mock-3:2379",
            ca="mock-ca.crt", cert="mock-client.crt", key="mock-client.key",
        )

    def records(self, args):
        return [json.loads(line) for line in pathlib.Path(args.fault_log).read_text().splitlines()]

    def test_real_cli_stops_when_owned_collector_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            command = [sys.executable, str(SCRIPT), "--owned-fixture"]
            for name, value in vars(args).items():
                if name != "owned_fixture":
                    command.extend(["--" + name.replace("_", "-"), value])
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            records = self.records(args)
            self.assertIn("collector_exit", [record["kind"] for record in records])
            self.assertEqual(records[-1]["kind"], "failure")
            self.assertNotIn("pause_begin", [record["kind"] for record in records])
            self.assertEqual(pathlib.Path(args.fault_log).stat().st_mode & 0o777, 0o600)

    def test_startup_failure_is_recorded_without_starting_a_fault_driver(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            args.qualifier = str(pathlib.Path(directory) / "mock-missing-qualifier")
            with self.assertRaises(FileNotFoundError):
                campaign.run(args)
            self.assertEqual([r["kind"] for r in self.records(args)], ["manifest", "failure"])

    def test_driver_failure_terminates_only_the_owned_collector(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            child = mock.Mock()
            child.pid = 123
            child.poll.return_value = None
            child.wait.return_value = -15
            with mock.patch.object(campaign.subprocess, "Popen", return_value=child), \
                    mock.patch.object(campaign, "supervise", side_effect=campaign.CampaignError("mock driver failure")):
                with self.assertRaisesRegex(campaign.CampaignError, "mock driver failure"):
                    campaign.run(args)
            child.terminate.assert_called_once_with()
            child.wait.assert_called_once_with(timeout=10)
            child.kill.assert_not_called()
            self.assertIn("failure", [r["kind"] for r in self.records(args)])
            self.assertEqual(self.records(args)[-1]["kind"], "collector_stopped")

    def test_interrupt_preserves_failure_evidence_and_cleans_up_its_child(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            child = mock.Mock()
            child.pid = 123
            child.poll.return_value = None
            child.wait.return_value = -15
            with mock.patch.object(campaign.subprocess, "Popen", return_value=child), \
                    mock.patch.object(campaign, "supervise", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    campaign.run(args)
            child.terminate.assert_called_once_with()
            self.assertIn("interrupted", [r["kind"] for r in self.records(args)])
            self.assertNotIn("complete", [r["kind"] for r in self.records(args)])

    def test_owned_child_cleanup_escalates_only_after_its_bounded_wait(self):
        child = mock.Mock()
        child.poll.return_value = None
        child.wait.side_effect = [subprocess.TimeoutExpired("mock", 10), -9]
        events = []
        campaign.stop_collector(child, lambda kind, **fields: events.append((kind, fields)))
        child.terminate.assert_called_once_with()
        child.kill.assert_called_once_with()
        self.assertEqual(child.wait.call_args_list, [mock.call(timeout=10), mock.call(timeout=10)])
        self.assertEqual(events[-1][0], "collector_stopped")

    def test_sigterm_records_interruption_and_restores_the_cli_signal_handlers(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            command = [str(SCRIPT), "--owned-fixture"]
            for name, value in vars(args).items():
                if name != "owned_fixture":
                    command.extend(["--" + name.replace("_", "-"), value])
            child = mock.Mock(pid=123)
            child.poll.return_value = None
            child.wait.return_value = -15
            handlers = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
            with mock.patch.object(sys, "argv", command), \
                    mock.patch.object(campaign.subprocess, "Popen", return_value=child), \
                    mock.patch.object(campaign, "supervise", side_effect=lambda *_args: signal.raise_signal(signal.SIGTERM)):
                with self.assertRaises(SystemExit) as stopped:
                    campaign.main()
            self.assertEqual(stopped.exception.code, 143)
            self.assertEqual(handlers, {signum: signal.getsignal(signum) for signum in handlers})
            self.assertIn("interrupted", [r["kind"] for r in self.records(args)])
            child.terminate.assert_called_once_with()

    def test_success_without_a_history_cannot_become_a_complete_campaign(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            args.qualifier = "/usr/bin/true"
            with self.assertRaisesRegex(campaign.CampaignError, "without its history"):
                campaign.run(args)
            self.assertNotIn("complete", [r["kind"] for r in self.records(args)])

    def test_cleanup_stops_its_child_even_when_evidence_writes_fail(self):
        child = mock.Mock()
        child.poll.return_value = None
        child.wait.return_value = -15
        with self.assertRaises(OSError):
            campaign.stop_collector(child, mock.Mock(side_effect=OSError("mock disk full")))
        child.terminate.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=10)

    def test_failed_offline_recheck_never_records_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            child = mock.Mock(pid=123)
            child.poll.return_value = 0

            def launch(_command):
                pathlib.Path(args.history).write_text("mock invalid history\n")
                return child

            failure = subprocess.CalledProcessError(1, ["mock-recheck"])
            with mock.patch.object(campaign.subprocess, "Popen", side_effect=launch), \
                    mock.patch.object(campaign.subprocess, "run", side_effect=failure) as recheck:
                with self.assertRaises(subprocess.CalledProcessError):
                    campaign.run(args)
            recheck.assert_called_once_with(
                [args.qualifier, "--check-history", args.history], check=True, timeout=900,
            )
            self.assertEqual(self.records(args)[-1]["kind"], "failure")
            self.assertNotIn("complete", [r["kind"] for r in self.records(args)])

    def test_existing_output_and_missing_consent_start_no_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.arguments(directory)
            pathlib.Path(args.history).write_text("mock retained evidence\n")
            with mock.patch.object(campaign.subprocess, "Popen") as launch:
                with self.assertRaises(ValueError):
                    campaign.run(args)
                args.owned_fixture = False
                with self.assertRaises(ValueError):
                    campaign.run(args)
                launch.assert_not_called()
            self.assertFalse(pathlib.Path(args.fault_log).exists())


if __name__ == "__main__":
    unittest.main()
