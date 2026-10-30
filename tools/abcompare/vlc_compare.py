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
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request


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


def compare(args, content_id, *, playback_urls=None):
    targets = playback_targets(args, content_id, playback_urls)
    root = Path(tempfile.mkdtemp(prefix="outpace-vlc-"))
    # mkdtemp is private (0700); logs/playlists/status may contain stream metadata.
    clients = []
    started = time.monotonic()
    interrupted = False
    failure = None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    env = dict(os.environ)
    env.pop("ACE_CONTENT_ID", None)
    version = subprocess.run([args.vlc, "--version"], capture_output=True,
                             timeout=10, env=env, check=True).stdout.decode(errors="replace")
    config = {
        "engine_url": args.engine_url, "outpace_url": args.outpace_url,
        "client_warp": args.client_warp, "server_warp": args.server_warp,
        "network_caching_ms": args.network_caching, "seconds": args.seconds,
        "poll_seconds": args.poll_seconds, "vlc_version": version.splitlines()[0],
    }
    (root / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    print(f"Private artifacts: {root}", flush=True)
    try:
        for name, base in (("engine", args.engine_url), ("outpace", args.outpace_url)):
            playlist = root / f"{name}.m3u"
            playlist.touch(mode=0o600, exist_ok=False)
            playlist.write_text("#EXTM3U\n" + targets[name] + "\n")
            port, password = free_port(), secrets.token_urlsafe(24)
            log = (root / f"{name}-vlc.log").open("wb")
            try:
                process = subprocess.Popen([
                    args.vlc, "--ignore-config", "--no-one-instance", "--intf=dummy",
                    "--extraintf=http", "--http-host=127.0.0.1", f"--http-port={port}",
                    f"--http-password={password}", "--vout=vdummy", "--aout=adummy",
                    f"--network-caching={args.network_caching}", "--no-video-title-show",
                    "--stats", str(playlist),
                ], stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env)
            finally:
                log.close()
            clients.append({"name": name, "process": process, "port": port,
                            "password": password, "observation": Observation(),
                            "launch_seconds": time.monotonic() - started})
        deadline = started + args.seconds
        next_poll = started
        while time.monotonic() < deadline:
            for client in clients:
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
                    client["observation"].sample(elapsed, status)
                    with (root / f'{client["name"]}-status.jsonl').open("a") as out:
                        out.write(json.dumps({"seconds": elapsed, "status": status}) + "\n")
                except (OSError, ValueError):
                    client["observation"].errors += 1
            if failure:
                break
            next_poll += args.poll_seconds
            # Skip missed slots instead of issuing a burst of catch-up requests.
            next_poll = max(next_poll, time.monotonic())
            time.sleep(max(0, min(next_poll, deadline) - time.monotonic()))
    except KeyboardInterrupt:
        interrupted = True
    except (OSError, ValueError):
        failure = "runner_error_see_private_logs"
    finally:
        observed_seconds = time.monotonic() - started
        for client in clients:
            try:
                client["exit_code"] = stop_client(client["process"])
            except (OSError, subprocess.TimeoutExpired):
                client["exit_code"] = None
                failure = "client_cleanup_failed"
    reports = {}
    for client in clients:
        report = client["observation"].report(observed_seconds)
        report.update(launch_seconds=client["launch_seconds"], cleanup_exit_code=client["exit_code"])
        reports[client["name"]] = report
    complete = (len(reports) == 2 and not interrupted and failure is None
                and all(r["samples"] and any(v is not None for v in
                        r["first_decode_observed_seconds"].values()) for r in reports.values()))
    summary = {
        "schema_version": 1, "observation_complete": bool(complete),
        "interrupted": interrupted, "failure": failure,
        "observed_seconds": round(observed_seconds, 3),
        "requested_seconds": args.seconds, "poll_seconds": args.poll_seconds,
        "network_caching_ms": args.network_caching,
        "clients": reports,
        "limits": "Sampled dummy-output VLC decode observations; no quality pass/fail, "
                  "exact startup, sub-poll stall, content identity or live-position claim.",
    }
    (root / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [f"Observation complete: {bool(complete)} (no quality verdict)"]
    for name, report in reports.items():
        lines.append(f"{name}: counters={report['last_counters']}; "
                     f"max sampled gaps={report['max_sampled_progress_gap_seconds']}; "
                     f"control errors={report['control_errors']}; resets={report['counter_resets']}")
    lines.append(summary["limits"])
    (root / "comparison.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 130 if interrupted else (0 if complete else 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-url", required=True, type=server_base)
    parser.add_argument("--outpace-url", required=True, type=server_base)
    parser.add_argument("--seconds", type=positive, default=600)
    parser.add_argument("--poll-seconds", type=positive, default=5)
    parser.add_argument("--network-caching", type=int, default=1000, metavar="MS")
    parser.add_argument("--vlc", default="cvlc")
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

    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    old_handler = signal.signal(signal.SIGTERM, terminate)
    try:
        return compare(args, content_id)
    except (OSError, subprocess.SubprocessError):
        print("Unable to initialize VLC comparison; verify VLC installation and permissions.")
        return 2
    finally:
        signal.signal(signal.SIGTERM, old_handler)


if __name__ == "__main__":
    raise SystemExit(main())
