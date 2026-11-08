#!/usr/bin/env python3
"""Run an owned synthetic collector and stop pauses when that collector exits."""

import argparse
import datetime
import json
import os
import pathlib
import random
import re
import shlex
import signal
import subprocess
import time


class CampaignError(RuntimeError):
    pass


class CampaignInterrupted(KeyboardInterrupt):
    def __init__(self, signum):
        super().__init__(f"interrupted by signal {signum}")
        self.signum = signum


def supervise(
    collector, pause, duration, interval, emit, clock=time.monotonic, sleep=time.sleep,
    *, wall_clock=time.time, drain_seconds=300,
):
    started = clock()
    wall_started = wall_clock()
    previous = started
    next_pause = started + interval
    draining = False

    def sample_clock(owned_pause=False):
        nonlocal previous
        now = clock()
        elapsed = now - started
        wall_elapsed = wall_clock() - wall_started
        gap = now - previous
        if gap < 0 or abs(wall_elapsed - elapsed) > 5 or gap > (20 if owned_pause else 5):
            emit(
                "clock_discontinuity", elapsed_seconds=elapsed, wall_elapsed_seconds=wall_elapsed,
                scheduling_gap_seconds=gap,
            )
            raise CampaignError("host clock continuity failed; retain this run as interrupted")
        previous = now
        return now

    while True:
        now = sample_clock()
        code = collector.poll()
        if code is not None:
            emit("collector_exit", code=code, elapsed_seconds=now - started)
            return code
        if now >= started + duration:
            if not draining:
                emit("fault_schedule_complete", elapsed_seconds=now - started)
                draining = True
            if now >= started + duration + drain_seconds:
                raise CampaignError("collector exceeded its bounded drain deadline")
            sleep(min(1.0, started + duration + drain_seconds - now))
            continue
        if now >= next_pause:
            if collector.poll() is not None:
                continue
            pause()
            next_pause = sample_clock(owned_pause=True) + interval
        else:
            sleep(min(1.0, next_pause - now, started + duration - now))


def unique_fields(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"duplicate profile field: {name}")
        result[name] = value
    return result


def invalid_constant(value):
    raise ValueError(f"profile contains a nonstandard number: {value}")


def load_profile(path):
    with open(path, "rb") as source:
        data = source.read(64 * 1024 + 1)
    if len(data) > 64 * 1024:
        raise ValueError("profile exceeds 64 KiB")
    try:
        profile = json.loads(data, object_pairs_hook=unique_fields, parse_constant=invalid_constant)
    except RecursionError as error:
        raise ValueError("profile JSON exceeds the decoder nesting bound") from error
    if not isinstance(profile, dict) or profile.get("production_claim") is not False:
        raise ValueError("profile must explicitly reject a production claim")
    if "status" in profile and profile["status"] != "prepared":
        raise ValueError("profile must be newly prepared, not a retained or active run")
    for name in ("requested_seconds", "fault_interval_seconds", "fault_pause_seconds", "seed", "concurrency"):
        if type(profile.get(name)) is not int:
            raise ValueError(f"profile {name} must be an integer")
    duration, interval, pause = (profile[k] for k in ("requested_seconds", "fault_interval_seconds", "fault_pause_seconds"))
    if not (0 < duration <= 86400 and 0 < pause <= 10 and pause < interval <= duration and 0 < profile["concurrency"] <= 32):
        raise ValueError("profile timing and concurrency must be bounded")
    if not 0 <= profile["seed"] < 1 << 63:
        raise ValueError("profile seed must fit a nonnegative signed 64-bit integer")
    for name in ("data_relative_path", "executable_relative_path"):
        value = profile.get(name)
        if not isinstance(value, str) or not re.fullmatch(r"etcqlite-qualification/[a-zA-Z0-9_./-]+", value):
            raise ValueError(f"profile {name} must identify the owned qualification surface")
        if any(part in ("", ".", "..") for part in value.split("/")):
            raise ValueError(f"profile {name} contains an unsafe path component")
    for name in ("unit", "linux_binary_sha256", "candidate"):
        if not isinstance(profile.get(name), str):
            raise ValueError(f"profile {name} must be a string")
    if not re.fullmatch(r"etcqlite-qual-[a-z0-9-]+\.service", profile["unit"]):
        raise ValueError("profile needs an explicit qualification unit")
    if not re.fullmatch(r"[0-9a-f]{64}", profile["linux_binary_sha256"]):
        raise ValueError("profile needs a full executable SHA256")
    if not re.fullmatch(r"[0-9a-f]{40}", profile["candidate"]):
        raise ValueError("profile needs a full source revision")
    nodes = profile.get("nodes")
    if not isinstance(nodes, list) or len(nodes) != 3:
        raise ValueError("profile needs exactly three owned qualification VMs")
    names = set()
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("vm"), str) or not re.fullmatch(r"etcqlite-qual-[a-z0-9-]+", node["vm"]):
            raise ValueError("profile contains an unowned VM name")
        if node["vm"] in names:
            raise ValueError("profile VM names must be distinct")
        names.add(node["vm"])
        if any(type(node.get(k)) is not int or node[k] <= 0 for k in ("pid", "birth_ticks")):
            raise ValueError("profile must pin positive process/birth identities")
        if node["pid"] >= 1 << 31 or node["birth_ticks"] >= 1 << 64:
            raise ValueError("profile process/birth identity exceeds Linux bounds")
    return profile


def pause_command(profile, node):
    pid, birth = node["pid"], node["birth_ticks"]
    unit = shlex.quote(profile["unit"])
    executable = shlex.quote(profile["executable_relative_path"])
    data = shlex.quote(profile["data_relative_path"])
    digest = profile["linux_binary_sha256"]
    seconds = profile["fault_pause_seconds"]
    return (
        f'set -eu; exe="$HOME"/{executable}; data="$HOME"/{data}; '
        f'test "$(readlink -f "$exe")" = "$exe"; test "$(readlink -f "$data")" = "$data"; '
        f'test -f "$data/node.lock"; test ! -L "$data/node.lock"; '
        f'test "$(sha256sum "$exe" | cut -d " " -f 1)" = {digest}; '
        f'check() {{ systemctl is-active --quiet {unit}; '
        f'test "$(systemctl show {unit} -p MainPID --value)" = {pid}; '
        f'test "$(stat -c %u /proc/{pid})" = "$(id -u)"; '
        f'test "$(readlink /proc/{pid}/exe)" = "$exe"; '
        f'test "$(sed \'s/.*) //\' /proc/{pid}/stat | awk \'{{print $20}}\')" = {birth}; '
        f'test "$(tr \'\\000\' \'\\n\' < /proc/{pid}/cmdline | '
        f'awk \'$0 == "--data-dir" {{getline; print; found++}} END {{if (found != 1) exit 1}}\')" = "$data"; }}; '
        f'check; resume() {{ check; /bin/kill -CONT {pid}; }}; '
        f'trap resume EXIT HUP INT TERM; /bin/kill -STOP {pid}; '
        f'sleep {seconds}; resume; trap - EXIT HUP INT TERM; '
        f'printf "resumed pid=%s birth=%s\\n" {pid} {birth}'
    )


def stop_collector(collector, emit):
    if collector is None or collector.poll() is not None:
        return
    collector.terminate()
    forced = False
    try:
        code = collector.wait(timeout=10)
    except subprocess.TimeoutExpired:
        forced = True
        collector.kill()
        code = collector.wait(timeout=10)
    emit("collector_stopped", code=code, forced=forced)


def run(args):
    if not args.owned_fixture:
        raise ValueError("--owned-fixture is required; profile discovery is not authorization")
    profile = load_profile(args.profile)
    if os.path.lexists(args.history):
        raise ValueError("history output must be new")
    if pathlib.Path(args.history).absolute() == pathlib.Path(args.fault_log).absolute():
        raise ValueError("history and fault evidence must be separate new files")
    log = os.open(args.fault_log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    started = time.monotonic()
    collector = None
    with os.fdopen(log, "w", encoding="utf-8") as output:
        try:
            def emit(kind, **fields):
                record = {
                    "format": 1, "kind": kind, "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "monotonic_ns": time.monotonic_ns(), "elapsed_seconds": time.monotonic() - started, **fields,
                }
                output.write(json.dumps(record) + "\n")
                output.flush()
                os.fsync(output.fileno())

            command = [
                args.qualifier, "--correctness", "--owned-fixture", "--endpoints", args.endpoints,
                "--ca", args.ca, "--cert", args.cert, "--key", args.key,
                "--engine", profile["candidate"], "--seed", str(profile["seed"]),
                "--concurrency", str(profile["concurrency"]),
                "--duration", f'{profile["requested_seconds"]}s', "--output", args.history,
            ]
            emit("manifest", profile=profile, production_claim=False)
            collector = subprocess.Popen(command)
            emit("collector_started", pid=collector.pid)
            rng = random.Random(profile["seed"])

            def pause():
                node = rng.choice(profile["nodes"])
                emit("pause_begin", vm=node["vm"], pid=node["pid"], birth_ticks=node["birth_ticks"])
                try:
                    receipt = subprocess.run(
                        ["porto", "vm", "exec", node["vm"], "sh", "-c", pause_command(profile, node)],
                        text=True, capture_output=True, timeout=20, check=True,
                    )
                except subprocess.CalledProcessError as error:
                    detail = (error.stderr or error.stdout or str(error)).strip()
                    raise CampaignError(f"owned pause failed in {node['vm']}: {detail}") from error
                emit("pause_end", vm=node["vm"], pid=node["pid"], receipt=receipt.stdout.strip())

            code = supervise(collector, pause, profile["requested_seconds"], profile["fault_interval_seconds"], emit)
            if code != 0:
                emit("failure", error=f"collector exited with status {code}")
                return code if code > 0 else 128 - code
            if not pathlib.Path(args.history).is_file():
                raise CampaignError("collector exited successfully without its history evidence")
            subprocess.run([args.qualifier, "--check-history", args.history], check=True, timeout=900)
            emit("complete", production_claim=False)
            return 0
        except (ValueError, OSError, subprocess.SubprocessError, CampaignError, KeyboardInterrupt) as error:
            kind = "interrupted" if isinstance(error, KeyboardInterrupt) else "failure"
            emit(kind, error=str(error) or "interrupted")
            raise
        finally:
            try:
                stop_collector(collector, emit)
            except (OSError, subprocess.SubprocessError) as error:
                emit("cleanup_failure", error=str(error))
                raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("profile", "qualifier", "history", "fault-log", "endpoints", "ca", "cert", "key"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--owned-fixture", action="store_true")
    args = parser.parse_args()

    def interrupt(signum, _frame):
        raise CampaignInterrupted(signum)

    handlers = {signum: signal.signal(signum, interrupt) for signum in (signal.SIGINT, signal.SIGTERM)}
    try:
        return run(args)
    except CampaignInterrupted as error:
        parser.exit(128 + error.signum, f"qualification campaign: {error}\n")
    except (ValueError, OSError, subprocess.SubprocessError, CampaignError) as error:
        parser.exit(1, f"qualification campaign: {error}\n")
    finally:
        for signum, handler in handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
