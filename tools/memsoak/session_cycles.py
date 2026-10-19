#!/usr/bin/env python3
"""Bounded private lifecycle soak for an explicitly selected local daemon."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import math
import os
import re
from pathlib import Path
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


class Inconclusive(Exception):
    pass


class Failed(Exception):
    pass


MAX_SECONDS = 86400
IDLE_GRACE_MIN = 65
MAX_BODY = 8 * 1024 * 1024


def positive_number(value, name, maximum):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be finite, positive, and <= {maximum}")
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be finite, positive, and <= {maximum}")
    if not math.isfinite(result) or result <= 0 or result > maximum:
        raise ValueError(f"{name} must be finite, positive, and <= {maximum}")
    return result


def remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise Inconclusive("operation deadline expired")
    return value


def median(values):
    if not values:
        raise Inconclusive("empty measurement window")
    return statistics.median(values)


def validate_count(value, name, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer in 1..{maximum}")
    return value


@contextmanager
def deadline_watch(deadline, expiry=Inconclusive):
    main_thread = threading_is_main_thread()
    if not main_thread:
        yield
        return
    prior = signal.getsignal(signal.SIGALRM)
    old_timer = signal.getitimer(signal.ITIMER_REAL)
    def expired(_sig, _frame):
        raise expiry("HTTP operation deadline expired")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, remaining(deadline))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *old_timer)
        signal.signal(signal.SIGALRM, prior)


def parse_metric(memstats, metric):
    if metric == "allocated":
        if not isinstance(memstats, dict):
            raise Inconclusive("jemalloc sample invalid")
        value = memstats.get("allocated")
        if (memstats.get("allocator") != "jemalloc" or "epoch_error" not in memstats
                or memstats["epoch_error"] is not None or isinstance(value, bool)
                or not isinstance(value, int) or value < 0):
            raise Inconclusive("jemalloc sample invalid")
        return value
    raise Inconclusive("RSS requires process identity verification")


def select_metric(memstats, rss):
    try:
        parse_metric(memstats, "allocated")
        return "allocated"
    except Inconclusive:
        if sys.platform == "linux" and isinstance(rss, int) and not isinstance(rss, bool) and rss >= 0:
            return "rss_kib"
        raise Inconclusive("no valid jemalloc metric or verified Linux RSS")


def process_identity(pid):
    if sys.platform != "linux":
        raise Inconclusive("Linux process identity verification required")
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        tail = raw[raw.rfind(")") + 2:].split()
        starttime = tail[19]
        exe_link = os.readlink(f"/proc/{pid}/exe")
        with open(f"/proc/{pid}/exe", "rb") as exe:
            digest = hashlib.sha256(exe.read()).hexdigest()
        return (starttime, exe_link, digest)
    except (OSError, IndexError, ValueError) as exc:
        raise Inconclusive("cannot verify child identity") from exc


def rss_kib(pid, expected):
    if sys.platform != "linux" or expected is None:
        raise Inconclusive("Linux RSS identity unavailable")
    try:
        if process_identity(pid) != expected:
            raise Inconclusive("child identity changed")
        fields = Path(f"/proc/{pid}/status").read_text().splitlines()
        line = next(x for x in fields if x.startswith("VmRSS:"))
        value = int(line.split()[1])
        if value < 0:
            raise ValueError
        return value
    except (OSError, StopIteration, ValueError) as exc:
        raise Inconclusive("cannot verify child RSS") from exc


def classify(deltas, baseline, budget):
    if not deltas or any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in deltas):
        raise Inconclusive("invalid cycle results")
    return "PASS" if all(x <= budget for x in deltas) else "FAIL"


def make_report(result, deltas, baseline, peaks, metric, media, teardown, budget):
    return {"result": result, "metric": metric, "units": "bytes" if metric == "allocated" else "KiB",
            "baseline": baseline, "budget": budget,
            "cycles": [{"cycle": i + 1, "median": baseline + v, "delta": v, "peak": peaks[i], "classification": "PASS" if v <= budget else "FAIL"}
                       for i, v in enumerate(deltas)],
            "media": media, "teardown": teardown, "reason": "all cycle deltas within budget" if result == "PASS" else "valid cycle exceeded budget",
            "interpretation": "Per-cycle observations; does not prove or disprove a leak."}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise Inconclusive("HTTP redirects are not permitted")


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def _read_limited(response, deadline, limit=MAX_BODY):
    body = bytearray()
    while True:
        if response.isclosed() or response.fp is None:
            break
        timeout = remaining(deadline)
        response.fp.raw._sock.settimeout(timeout)
        block = response.read1(min(65536, limit + 1 - len(body)))
        if not block:
            break
        body.extend(block)
        if len(body) > limit:
            raise Inconclusive("HTTP response too large")
    return bytes(body)


def http(url, deadline, method="GET", body_limit=MAX_BODY):
    request = urllib.request.Request(url, method=method)
    try:
        with deadline_watch(deadline):
            with OPENER.open(request, timeout=remaining(deadline)) as response:
                body = _read_limited(response, deadline, body_limit) if method != "DELETE" else b""
                return response.status, body
    except urllib.error.HTTPError as exc:
        code = exc.code
        exc.close()
        return code, b""


def threading_is_main_thread():
    return threading.current_thread() is threading.main_thread()


def json_get(base, path, deadline):
    status, body = http(base + path, deadline)
    if status != 200:
        raise Inconclusive("unexpected HTTP status")
    try:
        return json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise Inconclusive("invalid JSON response") from exc


def stream_items(base, deadline):
    value = json_get(base, "/streams", deadline)
    if not isinstance(value, dict) or not isinstance(value.get("streams"), list):
        raise Inconclusive("invalid streams response")
    for item in value["streams"]:
        if (not isinstance(item, dict) or not isinstance(item.get("network"), str)
                or not isinstance(item.get("id"), str) or isinstance(item.get("clients"), bool)
                or not isinstance(item.get("clients"), int) or item["clients"] < 0
                or not isinstance(item.get("metadata"), dict)):
            raise Inconclusive("invalid streams response")
    return value["streams"]


def streams_present(base, selector, deadline):
    return any(x["network"] == "ace" and x["id"] in (selector, "ih:" + selector, "cid:" + selector)
               for x in stream_items(base, deadline))


def target_clients(base, selector, deadline):
    items = stream_items(base, deadline)
    target = next((x for x in items if x["network"] == "ace" and
                   x["id"] in (selector, "ih:" + selector, "cid:" + selector)), None)
    if target is None:
        return 0
    clients = target.get("clients")
    if isinstance(clients, bool) or not isinstance(clients, int) or clients < 0:
        raise Inconclusive("invalid stream client count")
    return clients


def wait_absent(base, selector, deadline, reason="target session remained after idle grace", overall=None, request_timeout=1):
    overall = deadline if overall is None else overall
    while True:
        now = time.monotonic()
        if now >= overall:
            raise Inconclusive("overall operation deadline expired")
        probe_deadline = min(overall, now + request_timeout, deadline if now < deadline else now + request_timeout)
        if not streams_present(base, selector, probe_deadline):
            return
        clients = target_clients(base, selector, probe_deadline)
        if time.monotonic() >= deadline:
            if clients == 0:
                raise Failed(reason)
            raise Inconclusive("target still has clients after teardown deadline")
        time.sleep(min(0.2, remaining(min(deadline, overall))))


def delete_and_wait(base, stream, selector, deadline, overall=None, request_timeout=5):
    try:
        code, _ = http(base + stream, min(deadline, time.monotonic() + request_timeout), "DELETE")
        if code != 204:
            raise Inconclusive("DELETE response ambiguous")
        wait_absent(base, selector, deadline, "target session remained after DELETE", overall, request_timeout)
    except (Inconclusive, Failed):
        raise
    except Exception as exc:
        raise Inconclusive("DELETE response ambiguous") from exc


def check_identity(proc, expected, requested):
    if proc.poll() is not None:
        raise Inconclusive("daemon exited")
    actual = process_identity(proc.pid)
    if actual != expected or actual[2] != requested:
        raise Inconclusive("daemon identity changed")


def owns_listener(pid, port):
    if sys.platform != "linux":
        return False
    try:
        wanted = f"{port:04X}"
        inodes = set()
        for table in ("/proc/net/tcp", "/proc/net/tcp6"):
            for line in Path(table).read_text().splitlines()[1:]:
                fields = line.split()
                if fields[1].rsplit(":", 1)[-1] != wanted or fields[3] != "0A":
                    continue
                addr = fields[1].rsplit(":", 1)[0]
                if addr != "0100007F":
                    continue
                inodes.add(fields[9])
        if not inodes:
            return False
        for fd in Path(f"/proc/{pid}/fd").iterdir():
            target = os.readlink(fd)
            if target.startswith("socket:[") and target[8:-1] in inodes:
                return True
        return False
    except OSError:
        return False


def sample(base, metric, proc, expected, requested, timeout, port, deadline):
    check_identity(proc, expected, requested)
    if not all(owns_listener(proc.pid, p) for p in (port, proc.ports[1])):
        raise Inconclusive("daemon no longer owns isolated HTTP and RTMP listeners")
    sample_deadline = min(deadline, time.monotonic() + timeout)
    if streams_present(base, proc.selector, sample_deadline):
        raise Inconclusive("target session present in measurement window")
    memstats = json_get(base, "/debug/memstats", sample_deadline)
    if metric == "allocated":
        value = parse_metric(memstats, metric)
    else:
        value = rss_kib(proc.pid, expected)
    check_identity(proc, expected, requested)
    if not all(owns_listener(proc.pid, p) for p in (port, proc.ports[1])):
        raise Inconclusive("daemon no longer owns isolated HTTP and RTMP listeners")
    if streams_present(base, proc.selector, sample_deadline):
        raise Inconclusive("target session present in measurement window")
    return value


def sample_window(base, metric, proc, expected, requested, timeout, port, count, deadline):
    return [sample(base, metric, proc, expected, requested, timeout, port, deadline) for _ in range(count)]


def _same_loopback_route(base, candidate, prefix):
    b, c = urllib.parse.urlsplit(base), urllib.parse.urlsplit(urllib.parse.urljoin(base + prefix + ".m3u8", candidate))
    if (b.scheme, b.hostname, b.port) != ("http", "127.0.0.1", c.port) or c.hostname != "127.0.0.1":
        raise Inconclusive("HLS segment URI escaped native loopback route")
    path = urllib.parse.urlsplit(base + prefix).path
    if not c.path.startswith(path + "/"):
        raise Inconclusive("HLS segment URI escaped native route")
    return urllib.parse.urlunsplit(("http", b.netloc, c.path, c.query, ""))


class PlaybackIntervalComplete(Exception):
    pass


class PlaybackWindow:
    """Startup is bounded separately; media progress must span the active window."""
    def __init__(self, deadline, timeout, duration):
        self.overall = deadline
        self.timeout = timeout
        self.duration = duration
        self.explicit_duration = duration is not None
        self.started = None
        self.last = None
        self.end = deadline if duration is None else None
        self.startup = min(deadline, time.monotonic() + timeout)
        if duration is not None and time.monotonic() + duration >= deadline:
            raise Inconclusive("full playback interval cannot fit overall deadline")

    def operation_deadline(self):
        if self.started is None:
            return self.startup
        return min(self.overall, self.end, self.last + self.timeout)

    def progress(self):
        now = time.monotonic()
        if now >= self.operation_deadline():
            raise Inconclusive("media progress exceeded deadline")
        if self.started is None:
            self.started = now
            if self.duration is not None:
                self.end = now + self.duration
                if self.end >= self.overall:
                    raise Inconclusive("full playback interval cannot fit overall deadline")
            else:
                self.duration = self.end - now
        self.last = now

    def complete(self):
        if self.explicit_duration:
            remaining(self.overall)
        now = time.monotonic()
        if self.started is None:
            remaining(self.startup)
            return False
        if now < self.end:
            remaining(min(self.overall, self.last + self.timeout))
            return False
        if (self.last - self.started < self.duration / 2
                or self.end - self.last > self.timeout):
            raise Inconclusive("media progress did not span playback interval")
        return True


def play_ts(url, deadline, guard=None, operation_timeout=5, playback_duration=None):
    window = PlaybackWindow(deadline, operation_timeout, playback_duration)
    total = 0
    # Header acquisition shares the startup budget with the first media read.
    with deadline_watch(window.startup):
        response = OPENER.open(urllib.request.Request(url), timeout=remaining(window.startup))
    with response:
        if response.status != 200:
            raise Inconclusive("TS playback HTTP status")
        while not window.complete():
            if guard:
                guard()
            if response.isclosed():
                raise Inconclusive("TS source ended before playback duration")
            read_deadline = window.operation_deadline()
            response.fp.raw._sock.settimeout(remaining(read_deadline))
            try:
                with deadline_watch(read_deadline, PlaybackIntervalComplete if read_deadline == window.end else Inconclusive):
                    block = response.read1(64 * 1024)
            except PlaybackIntervalComplete:
                # Only the active-window timer can finish a healthy observation.
                if read_deadline == window.end and window.complete():
                    return total
                raise
            except (socket.timeout, TimeoutError) as exc:
                raise Inconclusive("TS source stalled before playback duration") from exc
            if not block:
                raise Inconclusive("TS source ended before playback duration")
            window.progress()
            total += len(block)
    return total


def play_hls(base, prefix, deadline, guard=None, progress_timeout=5, playback_duration=None):
    window = PlaybackWindow(deadline, progress_timeout, playback_duration)
    prior = set()
    total = 0
    sequences = []
    advanced_with_segment = False
    while not window.complete():
        if guard:
            guard()
        status, body = http(base + prefix + ".m3u8", window.operation_deadline())
        if status != 200:
            raise Inconclusive("HLS playlist HTTP status")
        try:
            lines = body.decode("utf-8", "strict").splitlines()
        except UnicodeDecodeError as exc:
            raise Inconclusive("invalid HLS playlist") from exc
        if "#EXT-X-ENDLIST" in lines:
            raise Inconclusive("HLS source ended before playback duration")
        seqline = next((line for line in lines if line.startswith("#EXT-X-MEDIA-SEQUENCE:")), None)
        if seqline is None or not seqline.split(":", 1)[1].isdigit():
            raise Inconclusive("HLS sequence missing or invalid")
        sequence = int(seqline.split(":", 1)[1])
        prior_sequence = sequences[-1] if sequences else None
        if prior_sequence is not None and sequence < prior_sequence:
            raise Inconclusive("HLS media sequence moved backwards")
        sequences.append(sequence)
        new_segment = False
        for line in lines:
            if guard:
                guard()
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            url = _same_loopback_route(base, line, prefix)
            if url in prior:
                continue
            if not urllib.parse.urlsplit(url).path.endswith(".ts"):
                raise Inconclusive("unsupported HLS segment URI")
            code, data = http(url, window.operation_deadline())
            if code != 200 or not data:
                raise Inconclusive("HLS segment did not progress")
            window.progress()
            total += len(data)
            prior.add(url)
            new_segment = True
        if prior_sequence is not None and sequence > prior_sequence and new_segment:
            advanced_with_segment = True
        if not window.complete():
            time.sleep(min(0.2, progress_timeout / 4, remaining(window.operation_deadline())))
    if total == 0 or len(prior) < 2 or not advanced_with_segment:
        raise Inconclusive("HLS sequence and segment set did not advance")
    return total


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def read_version(binary, env, deadline):
    child = subprocess.Popen([str(binary), "--version"], env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, close_fds=True)
    try:
        with deadline_watch(min(deadline, time.monotonic() + 3)):
            output = child.stdout.read(129)
            if len(output) > 128:
                raise Inconclusive("daemon version output too large")
            child.wait(timeout=remaining(deadline))
        if child.returncode != 0:
            raise Inconclusive("daemon version unavailable")
        return output.decode("utf-8", "strict").strip()
    finally:
        if child.stdout:
            child.stdout.close()
        stop_child(child)


def stop_child(proc):
    if proc is None:
        return
    try:
        if proc.poll() is not None:
            return
    except KeyboardInterrupt:
        try:
            proc.kill()
        except KeyboardInterrupt:
            pass
        try:
            proc.wait(timeout=5)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            pass
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        try:
            proc.kill()
        except KeyboardInterrupt:
            pass
        try:
            proc.wait(timeout=5)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            pass


def run_workload(base, stream, selector, args, proc, expected, requested, deadline, ports, observed):
    observed[0] = False
    def guard():
        check_identity(proc, expected, requested)
        if not all(owns_listener(proc.pid, p) for p in ports[:2]):
            raise Inconclusive("daemon HTTP or RTMP listener ownership changed during workload")
    def probe_deadline():
        return min(deadline, time.monotonic() + args.timeout)
    if args.media == "ts":
        play_ts(base + stream + ".ts", deadline, lambda: (guard(), _observe_target(base, selector, probe_deadline(), observed)), args.timeout, args.playback_duration)
    else:
        play_hls(base, stream, deadline, lambda: (guard(), _observe_target(base, selector, probe_deadline(), observed)), args.timeout, args.playback_duration)
    if not observed[0]:
        raise Inconclusive("target session not observed during playback")
    check_identity(proc, expected, requested)
    guard()
    if args.teardown == "delete":
        delete_and_wait(base, stream, selector, min(deadline, time.monotonic() + args.reap_timeout), deadline, args.timeout)
    else:
        idle_deadline = min(deadline, time.monotonic() + args.idle_grace)
        # Fixed full grace: query every iteration but never end early on absence.
        while time.monotonic() < idle_deadline:
            check_identity(proc, expected, requested)
            present = streams_present(base, selector, min(idle_deadline, deadline, time.monotonic() + args.timeout))
            if present:
                target_clients(base, selector, min(idle_deadline, deadline, time.monotonic() + args.timeout))
            time.sleep(min(0.5, remaining(idle_deadline)))
        final_probe = min(deadline, time.monotonic() + args.timeout)
        if streams_present(base, selector, final_probe):
            clients = target_clients(base, selector, final_probe)
            if clients == 0:
                raise Failed("target session remained after idle grace")
            raise Inconclusive("target still has clients after idle grace")

    grace_deadline = min(deadline, time.monotonic() + args.postclose_grace)
    while time.monotonic() < grace_deadline:
        guard()
        time.sleep(min(.1, grace_deadline - time.monotonic(), remaining(deadline)))
    final_probe = min(deadline, time.monotonic() + args.timeout)
    if streams_present(base, selector, final_probe):
        clients = target_clients(base, selector, final_probe)
        if clients == 0:
            raise Failed("target session remained after post-close grace")
        raise Inconclusive("target still has clients after post-close grace")


def _observe_target(base, selector, deadline, observed):
    if streams_present(base, selector, deadline):
        observed[0] = True


def run(args):
    selector = os.environ.get("ACE_CONTENT_ID", "")
    if len(selector) != 40 or any(c not in "0123456789abcdefABCDEF" for c in selector):
        raise ValueError("ACE_CONTENT_ID must contain a 40-hex selector")
    if sys.platform != "linux":
        raise Inconclusive("Linux-only process and listener verification")
    binary = args.binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("--binary must be an explicit executable file")
    requested = hashlib.sha256(binary.read_bytes()).hexdigest()
    ports = [free_port(), free_port()]
    if len(set(ports)) != len(ports):
        raise Inconclusive("could not allocate distinct listener ports")
    temp = Path(tempfile.mkdtemp(prefix="outpace-197-", dir="/tmp")); temp.chmod(0o700)
    data, cache = temp / "data", temp / "cache"
    data.mkdir(mode=0o700); cache.mkdir(mode=0o700)
    # Fixed allowlist: external variables cannot change loader, allocator, proxy, or daemon behavior.
    env = {k: os.environ[k] for k in ("PATH", "HOME", "USER", "LANG", "LC_ALL", "TZ") if k in os.environ}
    env.update(OUTPACE_BIND=f"127.0.0.1:{ports[0]}", OUTPACE_RTMP_BIND=f"127.0.0.1:{ports[1]}",
               OUTPACE_DATA_DIR=str(data), OUTPACE_CACHE_DIR=str(cache),
               OUTPACE_ENABLE_INBOUND="0", OUTPACE_ENABLE_PORT_MAPPING="0", OUTPACE_PORT_MAP_BACKEND="none")
    base = f"http://127.0.0.1:{ports[0]}"; selector = selector.lower(); stream = f"/streams/ace/cid:{selector}"
    deadline = time.monotonic() + args.overall_timeout; proc = None
    try:
        proc = subprocess.Popen([str(binary), "serve"], env=env, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, close_fds=True, start_new_session=True)
        proc.ports = ports
        proc.selector = selector
        requested_stat = binary.stat()
        expected = None
        ready_deadline = min(deadline, time.monotonic() + 20)
        while time.monotonic() < ready_deadline:
            if proc.poll() is not None:
                raise Inconclusive("daemon exited before readiness")
            actual = process_identity(proc.pid)
            try:
                child_exe_stat = os.stat(f"/proc/{proc.pid}/exe")
            except OSError as exc:
                raise Inconclusive("cannot verify child executable inode") from exc
            if (actual[2] != requested or child_exe_stat.st_dev != requested_stat.st_dev
                    or child_exe_stat.st_ino != requested_stat.st_ino
                    or requested_stat.st_ino != binary.stat().st_ino):
                raise Inconclusive("child executable does not match requested binary")
            expected = actual
            if all(owns_listener(proc.pid, p) for p in ports[:2]):
                try:
                    http(base + "/healthz", min(ready_deadline, time.monotonic() + 1))
                    break
                except Exception:
                    pass
            time.sleep(min(0.1, remaining(ready_deadline)))
        else:
            raise Inconclusive("daemon readiness or isolated listeners unavailable")
        check_identity(proc, expected, requested)
        rss = None
        observed = [False]
        for _ in range(args.warmups):
            run_workload(base, stream, selector, args, proc, expected, requested, deadline, ports, observed)
        if not observed[0]:
            raise Inconclusive("target session was not observed during playback")
        initial = json_get(base, "/debug/memstats", min(deadline, time.monotonic() + args.timeout))
        rss = rss_kib(proc.pid, expected)
        metric = select_metric(initial, rss)
        # Freeze the metric before this baseline window.
        try:
            version = read_version(binary, env, deadline)
        except Exception as exc:
            raise Inconclusive("daemon version unavailable") from exc
        if not re.fullmatch(r"outpace \d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", version):
            raise Inconclusive("daemon version unsupported")
        baseline = median(sample_window(base, metric, proc, expected, requested, args.timeout, ports[0], args.baseline_samples, deadline))
        budget = args.budget_mib * (1024 * 1024 if metric == "allocated" else 1024)
        deltas, peaks = [], []
        for _ in range(args.cycles):
            before = sample(base, metric, proc, expected, requested, args.timeout, ports[0], deadline)
            run_workload(base, stream, selector, args, proc, expected, requested, deadline, ports, observed)
            values = sample_window(base, metric, proc, expected, requested, args.timeout, ports[0], args.window_samples, deadline)
            peaks.append(max([before] + values)); deltas.append(median(values) - baseline)
        report = make_report(classify(deltas, baseline, budget), deltas, baseline, peaks, metric, args.media, args.teardown, budget)
        report.update(binary_sha256=requested, version=version)
        return report
    finally:
        stop_child(proc)
        shutil.rmtree(temp, ignore_errors=True)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", required=True, type=Path)
    p.add_argument("--budget-mib", required=True, type=float)
    p.add_argument("--cycles", type=int, default=3); p.add_argument("--warmups", type=int, default=1)
    p.add_argument("--baseline-samples", type=int, default=5); p.add_argument("--window-samples", type=int, default=5)
    p.add_argument("--playback-duration", type=float, default=30)
    p.add_argument("--idle-grace", type=float, default=65); p.add_argument("--reap-timeout", type=float, default=30)
    p.add_argument("--postclose-grace", type=float, default=5)
    p.add_argument("--timeout", type=float, default=5); p.add_argument("--overall-timeout", type=float, default=3600)
    p.add_argument("--media", choices=("ts", "hls"), default="ts")
    p.add_argument("--teardown", choices=("idle", "delete"), default="idle")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        args.budget_mib = positive_number(args.budget_mib, "budget MiB", 1_000_000)
        for key in ("playback_duration", "idle_grace", "reap_timeout", "postclose_grace", "timeout", "overall_timeout"):
            setattr(args, key, positive_number(getattr(args, key), key, MAX_SECONDS))
        if args.idle_grace < IDLE_GRACE_MIN:
            raise ValueError("idle grace must be at least 65 seconds")
        for key, maximum in (("cycles", 1000), ("warmups", 100), ("baseline_samples", 1000), ("window_samples", 1000)):
            setattr(args, key, validate_count(getattr(args, key), key, maximum))
        prior_term = signal.getsignal(signal.SIGTERM)
        def interrupted(_sig, _frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, interrupted)
        try:
            report = run(args)
        finally:
            signal.signal(signal.SIGTERM, prior_term)
        print(json.dumps(report, sort_keys=True)); return 0 if report["result"] == "PASS" else 1
    except Failed as exc:
        print(json.dumps({"result": "FAIL", "reason": str(exc)}), file=sys.stderr); return 1
    except KeyboardInterrupt:
        print(json.dumps({"result": "INCONCLUSIVE", "reason": "interrupted"}), file=sys.stderr); return 2
    except Exception:
        # Exception messages may contain URLs, selectors, environment values, or paths.
        print(json.dumps({"result": "INCONCLUSIVE", "reason": "operation_failed"}), file=sys.stderr); return 2


if __name__ == "__main__":
    raise SystemExit(main())
