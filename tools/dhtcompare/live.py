"""Linux live-worker owner: one default-deny namespace plus its supervisor relay.

The worker sees only a veth whose traffic is filtered by inner nftables and then by
the relay in this process; see live_policy and live_relay. Its filesystem is
read-only and every directory holding host UNIX sockets is covered by an empty
tmpfs. This owner bounds time, pipes and relay state, tears down the namespace
through its verified PID 1, and arms a parent-death SIGKILL so the namespace dies
with this process. A root supervisor launches the namespace as an unprivileged uid,
so the worker cannot read root-only host files. It does not prove library actor
joins; namespace teardown is the hard boundary.
"""
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import select
import selectors
import signal
import socket
import subprocess
import sys
import time
from isolation import verified_init_pidfd
import live_policy
from live_relay import Relay, host_address_monitor
from qualification import Artifact
from worker_records import extract_record

UNSHARE = '/usr/bin/unshare'
SETPRIV = '/usr/bin/setpriv'
# Every directory known to hold host UNIX sockets on the qualified hosts; any other
# bound path the supervisor can see must be unreachable inside the namespace.
HIDDEN_PATHS = ('/run', '/var/run', '/tmp', '/var/tmp', '/var/spool', '/dev/shm')
MAX_LISTENERS = 1024
UNPRIVILEGED_ID = 65534
BOOTSTRAP = Path(__file__).resolve().with_name('live_bootstrap.py')
MESSAGE = 65536
INNER_FIELDS = ('wait_status', 'elapsed_seconds', 'rss_hwm_kib_sampled', 'user_seconds', 'system_seconds',
                'threads_peak_sampled', 'sockets_peak_sampled', 'samples')
SAMPLED_FIELDS = ('rss_hwm_kib_sampled', 'threads_peak_sampled', 'sockets_peak_sampled')


@dataclass(frozen=True)
class LiveResult:
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    stdout_bytes: int
    stderr_bytes: int
    timed_out: bool
    cleanup_complete: bool
    truncated: bool
    cancelled_on_witness: bool = False
    attested: bool = False
    denial: str | None = None
    boundary: dict | None = None
    inner: dict | None = None


def _maps(path, identity):
    for line in Path(path).read_text().splitlines():
        inside, _, count = (int(field) for field in line.split())
        if inside <= identity < inside + count:
            return True
    return False


def _single_root_mapping(path):
    """The parent-namespace id of this namespace's root, if root is its only mapped id."""
    lines = Path(path).read_text().split('\n')
    rows = [line.split() for line in lines if line.strip()]
    if len(rows) == 1 and rows[0][0] == '0' and rows[0][2] == '1':
        return int(rows[0][1])
    return None


def launcher_identity(*, euid=None, uid_map='/proc/self/uid_map', gid_map='/proc/self/gid_map'):
    """setpriv arguments that drop a root supervisor's namespace launch to an unprivileged uid.

    The parent-death signal is armed after the credential change, so it survives it.
    A non-root supervisor launches as itself. A root supervisor that cannot map the
    unprivileged id may launch as itself only inside a namespace whose sole mapped id
    is root backed by a non-root parent id; any other root launch is refused.
    """
    if (os.geteuid() if euid is None else euid) != 0:
        return []
    if _maps(uid_map, UNPRIVILEGED_ID) and _maps(gid_map, UNPRIVILEGED_ID):
        return ['--reuid', str(UNPRIVILEGED_ID), '--regid', str(UNPRIVILEGED_ID), '--clear-groups']
    if _single_root_mapping(uid_map) not in (None, 0) and _single_root_mapping(gid_map) not in (None, 0):
        return []
    raise ValueError('root_equivalent_launch')


def bound_unix_paths(listing='/proc/net/unix'):
    """Absolute filesystem paths of UNIX sockets visible in this network namespace."""
    paths = set()
    with open(listing) as table:
        next(table)
        for line in table:
            fields = line.split(maxsplit=7)
            if len(fields) == 8 and fields[7].startswith('/'):
                paths.add(fields[7].rstrip('\n'))
    if len(paths) > MAX_LISTENERS:
        raise ValueError('listener_inventory')
    return sorted(paths)


def _config():
    return json.dumps({
        'ruleset': live_policy.nft_ruleset(), 'worker_device': live_policy.WORKER_DEVICE,
        'relay_device': live_policy.RELAY_DEVICE, 'worker_address': live_policy.WORKER_ADDRESS,
        'gateway_address': live_policy.GATEWAY_ADDRESS, 'prefix': live_policy.PREFIX_LENGTH,
        'hidden_paths': list(HIDDEN_PATHS), 'listeners': bound_unix_paths(),
    }).encode()


def validate_attestation(attestation):
    """Accept only the fixed inner topology and an unused policy; return (worker, relay) MACs."""
    worker, relay = live_policy.WORKER_DEVICE, live_policy.RELAY_DEVICE
    try:
        links = {link['ifname']: link for link in attestation['links']}
        if set(links) != {'lo', worker, relay} or len(attestation['links']) != 3:
            raise ValueError
        macs = [bytes.fromhex(links[name]['address'].replace(':', '')) for name in (worker, relay)]
        if any(len(mac) != 6 for mac in macs) or macs[0] == macs[1]:
            raise ValueError
        expected = {'lo': [('inet', '127.0.0.1', 8)],
                    worker: [('inet', live_policy.WORKER_ADDRESS, live_policy.PREFIX_LENGTH)],
                    relay: []}
        seen = {}
        for entry in attestation['addresses']:
            seen[entry['ifname']] = [(a['family'], a['local'], a['prefixlen']) for a in entry['addr_info']]
        if {name: seen.get(name, []) for name in expected} != expected or set(seen) - set(expected):
            raise ValueError
        defaults = 0
        for route in attestation['routes4']:
            if route.get('dev') not in ('lo', worker):
                raise ValueError
            if route.get('dst') == 'default':
                if (route.get('gateway') != live_policy.GATEWAY_ADDRESS or route.get('dev') != worker
                        or route.get('table', 'main') != 'main'):
                    raise ValueError
                defaults += 1
        if defaults != 1 or attestation['routes6']:
            raise ValueError
        counters = live_policy.ruleset_counters(attestation['ruleset'])
        if any(counters.values()) or attestation['ip_forward'] != '0':
            raise ValueError
        filesystem = attestation['filesystem']
        if (filesystem['root_readonly'] is not True or filesystem['reachable_listeners'] != 0
                or set(filesystem['hidden']) != set(HIDDEN_PATHS)
                or any(state not in ('empty', 'absent', 'link') for state in filesystem['hidden'].values())):
            raise ValueError
        return macs[0], macs[1]
    except (KeyError, TypeError, AttributeError, ValueError):
        raise ValueError('live_attestation') from None


def _inner_report(final):
    if type(final) is not dict or any(f not in final for f in INNER_FIELDS):
        return None
    report = {f: final[f] for f in INNER_FIELDS
              if (type(final[f]) in (int, float) and math.isfinite(final[f]))
              or (final[f] is None and f in SAMPLED_FIELDS)}
    if len(report) != len(INNER_FIELDS):
        return None
    try:
        report['policy_counters'] = live_policy.ruleset_counters(final['ruleset'])
    except ValueError:
        report['policy_counters'] = None
    return report


def run_live(argv, *, timeout, env, open_outer, prefix_cap=65536):
    """Run one argv inside a fresh default-deny live namespace and relay its UDP.

    `open_outer` returns one nonblocking root-namespace UDP socket per inner socket.
    The timeout must leave the caller 2.1 seconds of cleanup reserve.
    """
    if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout) or not 0 < timeout <= 28
            or not argv or len(argv) > 32 or not Path(argv[0]).is_absolute()
            or any(not isinstance(a, str) or not a or len(a) > 16384 for a in argv)):
        raise ValueError('invalid_live_worker')
    control, child_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    command = [SETPRIV, *launcher_identity(), '--pdeathsig', 'KILL', '--', UNSHARE, '--user', '--map-root-user', '--net', '--pid', '--fork',
               '--kill-child=KILL', '--mount-proc', sys.executable, '-I', str(BOOTSTRAP),
               str(child_end.fileno()), *argv]
    start = time.monotonic(); deadline = start + timeout; cleanup_deadline = deadline + 2
    selector = selectors.DefaultSelector(); proc = None; relay = None
    retained = [bytearray(), bytearray()]; counts = [0, 0]
    timed_out = False; uncertain = False; init_fd = None; init_terminal = False
    phase = 'attest'; denial = None; final = None; attested = False

    def kill():
        nonlocal init_fd, uncertain
        if proc is None or proc.poll() is not None:
            return
        if init_fd is None:
            init_fd = verified_init_pidfd(proc.pid)
        if init_fd is not None:
            try:
                signal.pidfd_send_signal(init_fd, signal.SIGKILL)
                return
            except ProcessLookupError:
                return
            except OSError:
                pass
        uncertain = True
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass

    try:
        config = _config()
        proc = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                start_new_session=True, env=env, close_fds=True,
                                pass_fds=(child_end.fileno(),))
        child_end.close()
        control.sendall(config)
        control.setblocking(False)
        for index, stream in enumerate((proc.stdout, proc.stderr)):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, index)
        selector.register(control, selectors.EVENT_READ, 'control')
        while True:
            if init_fd is None:
                init_fd = verified_init_pidfd(proc.pid)
            now = time.monotonic()
            if now >= deadline and proc.poll() is None:
                timed_out = True; kill()
            pipes_open = any(key.data in (0, 1) for key in selector.get_map().values())
            if proc.poll() is not None and not pipes_open:
                if phase == 'running' and final is None:
                    try:
                        final = json.loads(control.recv(MESSAGE))
                    except (OSError, ValueError):
                        pass
                break
            if now >= cleanup_deadline:
                uncertain = True; break
            for key, _ in selector.select(min(.05, max(0, cleanup_deadline - now))):
                if key.data == 'control':
                    try:
                        if phase == 'attest':
                            message, fds, _, _ = socket.recv_fds(control, MESSAGE, 1)
                        else:
                            message, fds = control.recv(MESSAGE), []
                    except BlockingIOError:
                        continue
                    except OSError:
                        message, fds = b'', []
                    if not message:
                        selector.unregister(control); continue
                    if phase == 'attest':
                        packet = socket.socket(fileno=fds[0]) if len(fds) == 1 else None
                        try:
                            if packet is None:
                                raise ValueError
                            worker_mac, gateway_mac = validate_attestation(json.loads(message))
                        except ValueError:
                            if packet is not None:
                                packet.close()
                            denial = 'attestation'; kill(); continue
                        try:
                            events = host_address_monitor()
                        except OSError:
                            packet.close()
                            denial = 'host_monitor'; kill(); continue
                        relay = Relay(packet, worker_mac=worker_mac, gateway_mac=gateway_mac,
                                      open_outer=open_outer, host_events=events)
                        relay.register(selector)
                        control.send(b'go'); attested = True; phase = 'running'
                    else:
                        try:
                            final = json.loads(message)
                        except ValueError:
                            final = None
                    continue
                if isinstance(key.data, tuple):
                    relay.handle(key.data); continue
                try:
                    chunk = os.read(key.fd, 65536)
                except BlockingIOError:
                    continue
                except OSError:
                    uncertain = True; chunk = b''
                if not chunk:
                    selector.unregister(key.fileobj); continue
                index = key.data; counts[index] += len(chunk)
                retained[index].extend(chunk[:prefix_cap - len(retained[index])])
    finally:
        kill()
        if proc is not None:
            try:
                proc.wait(timeout=min(2, max(.1, cleanup_deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                uncertain = True
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    stream.close()
        if init_fd is not None:
            poll = select.poll(); poll.register(init_fd, select.POLLIN)
            init_terminal = any(event & select.POLLIN for _, event in poll.poll(0))
            os.close(init_fd)
            if not init_terminal:
                uncertain = True
        boundary = relay.report() if relay is not None else None
        if relay is not None:
            relay.close()
        child_end.close(); control.close(); selector.close()
    reaped = proc is not None and proc.returncode is not None
    if reaped and proc.returncode < 0 and not init_terminal:
        uncertain = True
    return LiveResult(proc.returncode, bytes(retained[0]), bytes(retained[1]), *counts,
                      timed_out, reaped and not uncertain,
                      any(count > prefix_cap for count in counts), False, attested, denial,
                      boundary, _inner_report(final))


CUSTOM_TEST = 'dht::comparison::worker::custom_live_worker'
PREFIX = 'OUTPACE_DHT_COMPARE_'


@dataclass(frozen=True)
class LiveObservation:
    record: dict
    result: LiveResult
    elapsed: float


class LiveWorkerError(ValueError):
    """A failed live arm; `result` retains the owned process evidence when one ran."""

    def __init__(self, reason, result=None):
        super().__init__(reason)
        self.result = result


def worker_argv(role, artifact):
    if role == 'custom':
        return [str(artifact.path), '--exact', CUSTOM_TEST, '--ignored', '--nocapture',
                '--test-threads=1']
    return [str(artifact.path), '--live-worker', role]


def worker_environment(role, target, seeds):
    return {PREFIX + 'MODE': 'live-v1', PREFIX + 'ROLE': role,
            PREFIX + 'TARGET': target, PREFIX + 'SEEDS': seeds}


def run_live_worker(role, artifact, *, target, seeds, deadline, open_outer):
    """Run one pinned candidate through the live boundary within a 30-second envelope.

    Seeds must be one to four unique permitted global IPv4 endpoints. Identity-bearing
    input travels only in the worker environment; callers keep captures private.
    """
    try:
        if (role not in ('mainline', 'n0', 'custom') or type(target) is not str
                or len(target) != 40 or any(c not in '0123456789abcdefABCDEF' for c in target)
                or type(seeds) is not str or not 0 < len(seeds) <= 256):
            raise ValueError
        endpoints = [endpoint.rpartition(':') for endpoint in seeds.split(',')]
        if (not 1 <= len(endpoints) <= 4 or len(set(seeds.split(','))) != len(endpoints)
                or any(not port.isascii() or not port.isdigit()
                       or not live_policy.permitted(host, int(port)) for host, _, port in endpoints)):
            raise ValueError
    except (ValueError, TypeError):
        raise LiveWorkerError('invalid_live_worker') from None
    if deadline - time.monotonic() < 29.9:
        raise LiveWorkerError('live_worker_deadline')
    artifact.verify()
    env = {'LC_ALL': 'C', **worker_environment(role, target, seeds)}
    start = time.monotonic()
    result = run_live(worker_argv(role, artifact), timeout=27.8, env=env, open_outer=open_outer)
    elapsed = time.monotonic() - start
    if time.monotonic() > deadline:
        raise LiveWorkerError('live_worker_deadline', result)
    if not result.attested or result.boundary is None or result.inner is None:
        raise LiveWorkerError('live_boundary_evidence', result)
    try:
        record = extract_record(role, result, exact_custom_test=CUSTOM_TEST if role == 'custom' else None)
    except ValueError:
        raise LiveWorkerError('invalid_worker_record', result) from None
    return LiveObservation(record, result, elapsed)
