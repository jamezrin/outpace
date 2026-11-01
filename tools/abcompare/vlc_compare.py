#!/usr/bin/env python3
"""Bounded VLC observations of existing servers; no service or VPN management."""

import argparse
import base64
from collections import Counter
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request

from vlc_diagnostics import Collector, Correlation


COUNTERS = (
    "decodedvideo", "displayedpictures", "decodedaudio", "playedabuffers",
    "lostpictures", "lostabuffers", "demuxcorrupted", "demuxdiscontinuity",
)
PROGRESS = ("decodedvideo", "decodedaudio")


class Observation:
    def __init__(self):
        self.samples = 0
        self.errors = 0
        self.resets = 0
        self.states = Counter()
        self.last = None
        self.first = dict.fromkeys(PROGRESS)
        self.changed = dict.fromkeys(PROGRESS)
        self.gaps = dict.fromkeys(PROGRESS, 0.0)

    def sample(self, elapsed, status):
        stats = status.get("stats", {})
        if not isinstance(stats, dict) or any(
                type(stats.get(k)) is not int or stats[k] < 0 for k in COUNTERS):
            self.errors += 1
            return
        self.samples += 1
        self.states[str(status.get("state", "unknown"))] += 1
        reset = self.last is not None and any(stats[k] < self.last[k] for k in COUNTERS)
        self.resets += int(reset)
        for key in PROGRESS:
            if self.changed[key] is not None:
                self.gaps[key] = max(self.gaps[key], elapsed - self.changed[key])
            if stats[key] > 0 and self.first[key] is None:
                self.first[key] = elapsed
            if reset or self.last is None or stats[key] != self.last[key]:
                self.changed[key] = elapsed if stats[key] > 0 else None
        self.last = {k: stats[k] for k in COUNTERS}

    def report(self, elapsed):
        gaps = dict(self.gaps)
        for key in PROGRESS:
            if self.changed[key] is not None:
                gaps[key] = max(gaps[key], elapsed - self.changed[key])
        return {
            "samples": self.samples, "control_errors": self.errors,
            "counter_resets": self.resets, "states": dict(self.states),
            "last_counters": self.last,
            "first_decode_observed_seconds": self.first,
            "max_sampled_progress_gap_seconds": {
                k: round(gaps[k], 3) if self.first[k] is not None else None
                for k in PROGRESS
            },
        }


def server_base(value):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment or parsed.path not in ("", "/")
            or any(c.isspace() for c in value)):
        raise argparse.ArgumentTypeError("use an HTTP(S) server origin without credentials or path")
    try:
        parsed.port
    except ValueError:
        raise argparse.ArgumentTypeError("invalid server port") from None
    return value.rstrip("/")


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop_client(process):
    """Only the directly owned client PID; also reap clients that already exited."""
    if process.poll() is None:
        process.terminate()
    try:
        return process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=5)


def status_request(opener, port, password, timeout):
    request = urllib.request.Request(f"http://127.0.0.1:{port}/requests/status.json")
    auth = base64.b64encode(f":{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {auth}")
    with opener.open(request, timeout=timeout) as response:
        return json.loads(response.read(1024 * 1024))


def playback_targets(args, content_id, playback_urls):
    bases = {"engine": args.engine_url, "outpace": args.outpace_url}
    if playback_urls is None:
        return {name: base + "/ace/getstream?" + urllib.parse.urlencode({"id": content_id})
                for name, base in bases.items()}
    if not isinstance(playback_urls, dict) or set(playback_urls) != set(bases):
        raise ValueError("supply both owned playback URLs")
    targets = {}
    for name, base in bases.items():
        raw = playback_urls[name]
        if (not isinstance(raw, str) or len(raw) > 4096
                or any(ord(c) <= 32 or ord(c) >= 127 for c in raw)
                or any(c in raw for c in "?#%\\")):
            raise ValueError("invalid owned playback URL")
        parsed, origin = urllib.parse.urlsplit(raw), urllib.parse.urlsplit(base)
        try:
            same_origin = (parsed.scheme, parsed.hostname, parsed.port) == (
                origin.scheme, origin.hostname, origin.port)
        except ValueError:
            raise ValueError("invalid owned playback origin") from None
        width = 32 if name == "engine" else 64
        if (not same_origin or parsed.scheme not in ("http", "https")
                or parsed.username is not None or parsed.password is not None
                or not re.fullmatch(r"/ace/r/[0-9a-fA-F]{40}/[0-9a-fA-F]{" + str(width) + "}", parsed.path)):
            raise ValueError("invalid owned playback route")
        targets[name] = raw
    return targets


def compare(args, content_id, *, playback_urls=None, cancel_requested=None,
            artifact_parent=None, diagnostics=False):
    targets = playback_targets(args, content_id, playback_urls)
    if cancel_requested is not None and not callable(cancel_requested):
        raise ValueError("cancellation callback must be callable")
    def cancelled():
        return cancel_requested is not None and bool(cancel_requested())
    if artifact_parent is not None:
        artifact_parent = Path(artifact_parent).absolute()
        try:
            info = artifact_parent.stat()
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o700
                    or any(p.is_symlink() for p in (artifact_parent, *artifact_parent.parents))):
                raise ValueError("private artifact parent required")
        except OSError:
            raise ValueError("private artifact parent required") from None
    if cancelled():
        return 130
    root = Path(tempfile.mkdtemp(prefix="outpace-vlc-", dir=artifact_parent))
    # mkdtemp is private (0700); logs/playlists/status may contain stream metadata.
    clients = []
    started = time.monotonic()
    interrupted = False
    failure = None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    env = dict(os.environ)
    env.pop("ACE_CONTENT_ID", None)
    collector = Collector(started) if diagnostics else None
    diagnostic_failure = None
    capture_stopped = False
    private_logs = {}
    probe = None
    if not diagnostics:
        version = subprocess.run([args.vlc, "--version"], capture_output=True,
                                 timeout=10, env=env, check=True).stdout.decode(errors="replace")
    try:
        if diagnostics:
            # Precreate every private sink before any owned process is launched.
            for role in ("probe", "engine", "outpace"):
                private_logs[role] = (root / f"{role}-vlc.log").open("xb")
                os.fchmod(private_logs[role].fileno(), 0o600)
            probe = subprocess.Popen([args.vlc, "--version"], stdin=subprocess.DEVNULL,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            collector.attach("probe", probe)
            probe_deadline = time.monotonic() + 10
            while probe.poll() is None:
                if cancelled():
                    interrupted = True
                    break
                if collector.failed.is_set():
                    raise OSError("diagnostic capture failed")
                if time.monotonic() >= probe_deadline:
                    raise subprocess.TimeoutExpired("version probe", 10)
                time.sleep(0.05)
            if not interrupted and probe.wait() != 0:
                raise subprocess.CalledProcessError(probe.returncode, "version probe")
        if diagnostics:
            # The probe is reaped before playback; allow receipt of its final EOF.
            stop_client(probe)
            until = time.monotonic() + 0.25
            while time.monotonic() < until:
                with collector.lock:
                    if not any(c.role == "probe" for c in collector.pipes.values()):
                        break
                time.sleep(0.01)
            collector.close_role("probe")
            with collector.lock:
                rows = bytes(collector.data.get("probe", b""))
            first = next((json.loads(row) for row in rows.splitlines()
                          if json.loads(row)["stream"] == "stdout"), None)
            version = (base64.b64decode(first["raw_base64"]).decode(errors="replace")
                       if first else "unknown")
            version_truncated = bool(first and first["oversized"])
        config = {
            "origin_monotonic": started,
            "engine_url": args.engine_url, "outpace_url": args.outpace_url,
            "client_warp": args.client_warp, "server_warp": args.server_warp,
            "network_caching_ms": args.network_caching, "seconds": args.seconds,
            "poll_seconds": args.poll_seconds, "vlc_version": (version.splitlines() or ["unknown"])[0],
        }
        if diagnostics:
            config["diagnostics"] = {"schema_version": 1, "origin_monotonic": started,
                                     "probe_version_truncated": version_truncated}
        (root / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        print(f"Private artifacts: {root}", flush=True)
        for name, base in (("engine", args.engine_url), ("outpace", args.outpace_url)):
            if interrupted or cancelled():
                interrupted = True
                break
            playlist = root / f"{name}.m3u"
            playlist.touch(mode=0o600, exist_ok=False)
            playlist.write_text("#EXTM3U\n" + targets[name] + "\n")
            port, password = free_port(), secrets.token_urlsafe(24)
            log = None if diagnostics else (root / f"{name}-vlc.log").open("wb")
            try:
                process = subprocess.Popen([
                    args.vlc, "--ignore-config", "--no-one-instance", "--intf=dummy",
                    "--extraintf=http", "--http-host=127.0.0.1", f"--http-port={port}",
                    f"--http-password={password}", "--vout=vdummy", "--aout=adummy",
                    f"--network-caching={args.network_caching}", "--no-video-title-show",
                    "--stats", str(playlist),
                ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE if diagnostics else log,
                   stderr=subprocess.PIPE if diagnostics else log, env=env)
            finally:
                if log is not None:
                    log.close()
            clients.append({"name": name, "process": process, "port": port,
                            "password": password, "observation": Observation(),
                            "launch_seconds": time.monotonic() - started})
            if diagnostics:
                clients[-1]["correlation"] = Correlation()
                collector.attach(name, process)
            if cancelled():
                interrupted = True
                break
        deadline = started + args.seconds
        next_poll = started
        while not interrupted and time.monotonic() < deadline:
            if cancelled():
                interrupted = True
                break
            if collector is not None and collector.failed.is_set():
                failure = "diagnostic_capture_failed"
                break
            for client in clients:
                if cancelled():
                    interrupted = True
                    break
                if client["process"].poll() is not None:
                    failure = "client_exited_early"
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    status = status_request(opener, client["port"], client["password"],
                                            min(1.0, remaining))
                    if not isinstance(status, dict):
                        raise ValueError("invalid status")
                    elapsed = time.monotonic() - started
                    if diagnostics:
                        client["correlation"].sample(elapsed, status)
                        observed_status = dict(status, state=client["correlation"].previous[1])
                    else:
                        observed_status = status
                    client["observation"].sample(elapsed, observed_status)
                    with (root / f'{client["name"]}-status.jsonl').open("a") as out:
                        out.write(json.dumps({"seconds": elapsed, "status": status}) + "\n")
                except (OSError, ValueError):
                    client["observation"].errors += 1
                    if diagnostics:
                        client["correlation"].sample(time.monotonic() - started, {})
            if failure or interrupted:
                break
            next_poll += args.poll_seconds
            # Skip missed slots instead of issuing a burst of catch-up requests.
            next_poll = max(next_poll, time.monotonic())
            wake = min(next_poll, deadline)
            while not cancelled():
                if collector is not None and collector.failed.is_set():
                    break
                remaining = wake - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(remaining, 0.25))
        if cancelled():
            interrupted = True
    except KeyboardInterrupt:
        interrupted = True
    except (OSError, ValueError, subprocess.SubprocessError):
        failure = "runner_error_see_private_logs"
    finally:
        observed_seconds = time.monotonic() - started
        if collector is not None:
            collector.endpoint = started + observed_seconds
        if probe is not None:
            try:
                stop_client(probe)
            except (OSError, subprocess.TimeoutExpired):
                failure = failure or "probe_cleanup_failed"
        for client in clients:
            try:
                client["exit_code"] = stop_client(client["process"])
            except (OSError, subprocess.TimeoutExpired):
                client["exit_code"] = None
                failure = failure or "client_cleanup_failed"
        if collector is not None:
            try:
                collector.finish()
            except (Exception, KeyboardInterrupt) as error:
                interrupted |= isinstance(error, KeyboardInterrupt)
                diagnostic_failure = "cleanup_failed"
                collector.stop.set()
                try:
                    collector.thread.join(1)
                except (Exception, KeyboardInterrupt) as error:
                    interrupted |= isinstance(error, KeyboardInterrupt)
            capture_stopped = not collector.thread.is_alive()
            if not capture_stopped:
                diagnostic_failure = "cleanup_failed"
            if collector.failed.is_set() and diagnostic_failure is None:
                diagnostic_failure = "capture_failed"
            for role, log in private_logs.items():
                try:
                    # Never read mutable buffers while their owner may still run.
                    data = collector.data.get(role, b"") if capture_stopped else b""
                    if log.write(data) != len(data):
                        diagnostic_failure = "persistence_failed"
                except OSError:
                    diagnostic_failure = "persistence_failed"
                finally:
                    try:
                        log.close()
                    except OSError:
                        diagnostic_failure = "persistence_failed"
    # Completion boundary: reconcile cancellation through all owned cleanup.
    # Keep cleanup failures even when cancellation determines the exit status.
    if cancelled():
        interrupted = True
    reports = {}
    for client in clients:
        report = client["observation"].report(observed_seconds)
        report.update(launch_seconds=client["launch_seconds"], cleanup_exit_code=client["exit_code"])
        if diagnostics:
            report["diagnostic_correlation"] = client["correlation"].report()
        reports[client["name"]] = report
    complete = (len(reports) == 2 and not interrupted and failure is None
                and all(r["samples"] and any(v is not None for v in
                        r["first_decode_observed_seconds"].values()) for r in reports.values()))
    summary = {
        "origin_monotonic": started,
        "schema_version": 1, "observation_complete": bool(complete),
        "interrupted": interrupted, "failure": failure,
        "observed_seconds": observed_seconds,
        "requested_seconds": args.seconds, "poll_seconds": args.poll_seconds,
        "network_caching_ms": args.network_caching,
        "clients": reports,
        "limits": "Sampled dummy-output VLC decode observations; no quality pass/fail, "
                  "exact startup, sub-poll stall, content identity or live-position claim.",
    }
    if diagnostics:
        capture = collector.snapshot() if capture_stopped else {}
        summary["diagnostics"] = {
            "schema_version": 1, "origin_monotonic": started,
            "failure": diagnostic_failure,
            "complete": not diagnostic_failure and set(capture) == {"probe", "engine", "outpace"} and all(
                not r["retention_exhausted"] and not r["forced_close"] and r["eof_streams"] == 2
                for r in capture.values()),
            "capture": capture, "metadata_truncated": any(
                c["correlation"].omitted for c in clients),
        }
    try:
        (root / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
    except OSError:
        if not diagnostics:
            raise
        print("Unable to persist diagnostic comparison.")
        return 130 if interrupted else 2
    lines = [f"Observation complete: {bool(complete)} (no quality verdict)"]
    for name, report in reports.items():
        lines.append(f"{name}: counters={report['last_counters']}; "
                     f"max sampled gaps={report['max_sampled_progress_gap_seconds']}; "
                     f"control errors={report['control_errors']}; resets={report['counter_resets']}")
    lines.append(summary["limits"])
    try:
        (root / "comparison.txt").write_text("\n".join(lines) + "\n")
    except OSError:
        if not diagnostics:
            raise
        print("Unable to persist diagnostic comparison.")
        return 130 if interrupted else 2
    print("\n".join(lines))
    return 130 if interrupted else (0 if complete and diagnostic_failure is None else 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-url", required=True, type=server_base)
    parser.add_argument("--outpace-url", required=True, type=server_base)
    parser.add_argument("--seconds", type=positive, default=600)
    parser.add_argument("--poll-seconds", type=positive, default=5)
    parser.add_argument("--network-caching", type=int, default=1000, metavar="MS")
    parser.add_argument("--vlc", default="cvlc")
    parser.add_argument("--timestamped-diagnostics", action="store_true")
    parser.add_argument("--client-warp", choices=("on", "off", "unknown"), default="unknown")
    parser.add_argument("--server-warp", choices=("on", "off", "mixed", "unknown"), default="unknown")
    args = parser.parse_args()
    content_id = os.environ.get("ACE_CONTENT_ID", "")
    if not re.fullmatch(r"[0-9a-fA-F]{40}", content_id):
        parser.error("supply a current public content id privately in ACE_CONTENT_ID")
    if args.network_caching < 0:
        parser.error("network caching must be nonnegative")
    if not shutil.which(args.vlc):
        parser.error("VLC executable not found; install VLC or use --vlc")

    cancelled = False
    def terminate(_signum, _frame):
        nonlocal cancelled
        cancelled = True

    old_handlers = {sig: signal.signal(sig, terminate) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        return compare(args, content_id, cancel_requested=lambda: cancelled,
                       diagnostics=args.timestamped_diagnostics)
    except (OSError, subprocess.SubprocessError):
        print("Unable to initialize VLC comparison; verify VLC installation and permissions.")
        return 2
    finally:
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    raise SystemExit(main())
