"""Offline qualification of the live boundary inside an isolated outer namespace.

Run only through test_live.py / qualification, as namespace root of a fresh user
and network namespace. A dummy egress device carries synthetic addresses; nothing
leaves the host. Listeners on private addresses detect any relay leak.
argv: CONFIG_JSON with `cases` and optional `upstream`/`custom` artifact paths.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import live
import live_block
from live_relay import egress_socket
from live_policy import permitted

EGRESS = 'dhtq0'
SEED = ('11.0.0.2', 6881)
SECOND = ('11.0.0.4', 6881)
STRANGERS = (('11.0.0.3', 6881), ('11.0.0.2', 6882))
LEAK_ADDRESSES = ('10.0.0.5', '192.168.1.9', '172.16.0.1', '100.64.0.1', '169.254.1.1')
TARGET = bytes.fromhex('0123456789abcdef0123456789abcdef01234567')
CUSTOM_TEST = 'dht::comparison::worker::custom_live_worker'


def ip(*args):
    subprocess.run(('/usr/bin/ip', *args), check=True, timeout=2, capture_output=True)


def setup():
    links = json.loads(subprocess.check_output(('/usr/bin/ip', '-j', 'link', 'show'), timeout=2))
    if [link['ifname'] for link in links] != ['lo'] or os.getuid() != 0:
        raise SystemExit('fixture requires a fresh isolated network namespace')
    ip('link', 'set', 'lo', 'up')
    ip('link', 'add', EGRESS, 'type', 'dummy')
    ip('link', 'set', EGRESS, 'up')
    ip('address', 'add', '11.0.0.1/8', 'dev', EGRESS)
    for address in ('11.0.0.2', '11.0.0.3', '11.0.0.4', *LEAK_ADDRESSES):
        ip('address', 'add', f'{address}/32', 'dev', EGRESS)
    ip('route', 'add', 'default', 'dev', EGRESS)


def encode(value):
    if isinstance(value, int):
        return b'i%de' % value
    if isinstance(value, bytes):
        return b'%d:%s' % (len(value), value)
    if isinstance(value, list):
        return b'l' + b''.join(encode(v) for v in value) + b'e'
    return b'd' + b''.join(encode(k) + encode(value[k]) for k in sorted(value)) + b'e'


def decode(data, index=0):
    head = data[index:index + 1]
    if head == b'i':
        end = data.index(b'e', index)
        return int(data[index + 1:end]), end + 1
    if head in (b'l', b'd'):
        items, index = [], index + 1
        while data[index:index + 1] != b'e':
            item, index = decode(data, index)
            items.append(item)
        if head == b'l':
            return items, index + 1
        return dict(zip(items[::2], items[1::2])), index + 1
    colon = data.index(b':', index)
    length = int(data[index:colon])
    return data[colon + 1:colon + 1 + length], colon + 1 + length


def compact(address, port):
    return socket.inet_aton(address) + port.to_bytes(2, 'big')


class Servers:
    """Synthetic UDP endpoints in the outer namespace, stopped and joined by close()."""

    def __init__(self, mode):
        self.mode = mode
        self.stop = threading.Event()
        self.sockets = {}
        self.queries = []
        self.leaks = 0
        self.lock = threading.Lock()
        endpoints = [SEED, SECOND, *STRANGERS] + [(a, 6881) for a in LEAK_ADDRESSES]
        for endpoint in endpoints:
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp.bind(endpoint)
            udp.settimeout(0.05)
            self.sockets[endpoint] = udp
        self.threads = [threading.Thread(target=self.serve, args=(e,)) for e in endpoints]
        for thread in self.threads:
            thread.start()

    def serve(self, endpoint):
        udp = self.sockets[endpoint]
        while not self.stop.is_set():
            try:
                data, source = udp.recvfrom(65536)
            except TimeoutError:
                continue
            except OSError:
                return
            if not permitted(*endpoint):
                with self.lock:
                    self.leaks += 1
                continue
            if self.mode == 'echo':
                self.echo(endpoint, data, source)
            elif endpoint in (SEED, SECOND):
                self.dht(endpoint, data, source)

    def echo(self, endpoint, data, source):
        if endpoint != SEED:
            return
        self.sockets[SEED].sendto(b'pong:' + data, source)
        if data == b'unsolicited':
            for stranger in STRANGERS:
                self.sockets[stranger].sendto(b'stranger', source)

    def dht(self, endpoint, data, source):
        try:
            query, _ = decode(data)
            kind = query[b'q']
            transaction = query[b't']
        except (ValueError, KeyError, IndexError, TypeError):
            return
        with self.lock:
            self.queries.append(kind)
        response = {b'id': bytes([42 if endpoint == SEED else 43]) * 20, b'nodes': b''}
        if kind == b'get_peers':
            response[b'token'] = b'synthetic'
            if self.mode == 'values' or endpoint == SECOND:
                response[b'values'] = [compact(f'11.1.0.{n}', 10000 + n) for n in range(1, 11)]
            else:
                nodes = [TARGET[:19] + bytes([n]) + compact(a, 6881)
                         for n, a in enumerate((*LEAK_ADDRESSES, SECOND[0]))]
                response[b'nodes'] = b''.join(nodes)
        reply = {b't': transaction, b'y': b'r', b'r': response}
        self.sockets[endpoint].sendto(encode(reply), source)

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(2)
        alive = sum(thread.is_alive() for thread in self.threads)
        for udp in self.sockets.values():
            udp.close()
        return alive


def summary(result):
    return {'exit_code': result.exit_code, 'timed_out': result.timed_out,
            'cleanup_complete': result.cleanup_complete, 'truncated': result.truncated,
            'attested': result.attested, 'denial': result.denial,
            'boundary': result.boundary, 'inner': result.inner}


def probe_case():
    servers = Servers('echo')
    with tempfile.TemporaryDirectory(dir='/tmp') as sockets, \
            tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as writable, \
            socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(f'{sockets}/probe.sock')
        listener.listen(4)
        listener.setblocking(False)
        target = f'{writable}/leak'
        try:
            result = live.run_live(['/usr/bin/python3', '-I', str(Path(__file__).with_name('live_probe.py')),
                                    f'{sockets}/probe.sock', target],
                                   timeout=20, env={'LC_ALL': 'C'},
                                   open_outer=lambda: egress_socket(EGRESS))
        finally:
            alive = servers.close()
        try:
            listener.accept()[0].close()
            unix_accepted = 1
        except BlockingIOError:
            unix_accepted = 0
        file_created = os.path.lexists(target)
    probe = None
    try:
        probe = json.loads(result.stdout)
    except ValueError:
        pass
    return dict(summary(result), probe=probe, leaks=servers.leaks, servers_alive=alive,
                unix_accepted=unix_accepted, file_created=file_created)


def candidate_case(role, artifact, mode):
    servers = Servers(mode)
    try:
        observation = live.run_live_worker(
            role, artifact, target=TARGET.hex(), seeds=f'{SEED[0]}:{SEED[1]}',
            deadline=time.monotonic() + 30, open_outer=lambda: egress_socket(EGRESS))
        record, result, error = observation.record, observation.result, None
    except ValueError as failure:
        record, result, error = None, getattr(failure, 'result', None), str(failure)
    finally:
        alive = servers.close()
    report = summary(result) if result is not None else {}
    return dict(report, record=record, error=error, leaks=servers.leaks, servers_alive=alive,
                get_peers=servers.queries.count(b'get_peers'))


def unconfined_case(role, artifact):
    env = {'LC_ALL': 'C', **live.worker_environment(role, TARGET.hex(), f'{SEED[0]}:{SEED[1]}')}
    completed = subprocess.run(live.worker_argv(role, artifact), env=env, capture_output=True,
                               timeout=10)
    return {'exit_code': completed.returncode, 'stdout_bytes': len(completed.stdout),
            'record_line': b'DHT_COMPARE_RECORD' in completed.stdout}


class Resolver(BaseHTTPRequestHandler):
    """Loopback stand-in for the daemon's analyze_content method."""
    encrypted = 0

    def do_GET(self):
        body = json.dumps({'result': {'infohash': TARGET.hex(), 'is_live': 1,
                                      'is_encrypted': self.encrypted, 'status': 'complete'},
                           'error': None}).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def block_case(config, encrypted):
    Resolver.encrypted = encrypted
    resolver = ThreadingHTTPServer(('127.0.0.1', 6878), Resolver)
    serving = threading.Thread(target=resolver.serve_forever)
    serving.start()
    servers = Servers('values')
    try:
        artifacts = {role: live.Artifact(Path(config[role]), config[role + '_sha256'])
                     for role in ('custom', 'upstream')}
        with tempfile.TemporaryDirectory() as directory:
            report = live_block.run_block(
                artifacts, content_id='0123456789abcdef0123456789abcdef01234567',
                egress_device=EGRESS, output=Path(directory) / 'block',
                bootstrap=(f'{SEED[0]}:{SEED[1]}',))
            files = sorted(p.name for p in (Path(directory) / 'block').iterdir())
            written = (Path(directory) / 'block' / 'report.json').read_bytes()
    finally:
        alive = servers.close()
        resolver.shutdown(); serving.join(2); resolver.server_close()
    return {'report': report, 'files': files, 'leaks': servers.leaks, 'servers_alive': alive,
            'report_has_target': TARGET.hex().encode() in written,
            'report_has_seed': SEED[0].encode() in written}


CASES = ('probe', 'values:custom', 'values:mainline', 'values:n0', 'private_nodes:custom',
         'private_nodes:mainline', 'private_nodes:n0', 'unconfined:custom',
         'unconfined:mainline', 'unconfined:n0', 'block', 'block_encrypted')
ZERO_CAPS = {key: '0000000000000000' for key in ('CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb')}


def _owned(case):
    return (case.get('exit_code') == 0 and case.get('cleanup_complete') is True
            and case.get('attested') is True and not case.get('timed_out')
            and not case.get('truncated') and case.get('leaks') == 0
            and case.get('servers_alive') == 0 and case.get('boundary') is not None
            and case.get('inner') is not None and case['inner'].get('policy_counters') is not None)


def _denials_agree(case):
    return case['boundary']['denied_destination'] == case['inner']['policy_counters']['out_udp_denied']


def evaluate(results):
    """Return the names of fixed cases whose qualification predicate failed."""
    failed = [name for name in CASES if name not in results]
    for name, case in results.items():
        try:
            if name == 'probe':
                probe = case['probe']
                ok = (_owned(case) and _denials_agree(case)
                      and probe['thread_a'] == 'pong:ping-a' and probe['thread_b'] == 'pong:ping-b'
                      and probe['unsolicited'] == 'pong:unsolicited' and probe['unsolicited_extra'] == []
                      and case['boundary']['unsolicited_in'] == len(STRANGERS)
                      and case['boundary']['admitted_out'] == 3
                      and case['boundary']['denied_destination'] == len(
                          [a for a in probe['private'] if not a.startswith('127.')])
                      and probe['tcp'] != 'sent' and probe['ipv6'] != 'sent' and probe['raw'] == 'EPERM'
                      and probe['nft_flush'] != 0 and probe['link_down'] != 0
                      and probe['unix_connect'] == 'ENOENT' and probe['file_write'] == 'EROFS'
                      and probe['run_listing'] == [] and case['unix_accepted'] == 0
                      and case['file_created'] is False
                      and probe['status'] == dict(ZERO_CAPS, NoNewPrivs='1'))
            elif name.startswith(('values:', 'private_nodes:')):
                record = case['record']
                ok = (_owned(case) and _denials_agree(case) and case['error'] is None
                      and record['terminal'] == 'target' and record['peer_count'] == 10
                      and case['get_peers'] >= 1)
                if name.startswith('values:'):
                    ok = ok and case['boundary']['denied_destination'] == 0
                else:
                    ok = ok and case['boundary']['denied_destination'] >= 1
            elif name.startswith('unconfined:'):
                expected = 101 if name.endswith('custom') else 65
                ok = (case['exit_code'] == expected and not case['record_line']
                      and (name.endswith('custom') or case['stdout_bytes'] == 0))
            elif name == 'block':
                report = case['report']
                ok = (report['verdict'] == 'complete' and case['leaks'] == 0
                      and all(a['consistency'] == {'denials_agree': True, 'udp_frames_agree': True,
                                                   'kernel_drops': False} for a in report['arms'])
                      and not case['report_has_target'] and not case['report_has_seed']
                      and [a['role'] for a in report['arms']] == ['custom', 'mainline', 'n0']
                      and all(a['record']['terminal'] == 'target' for a in report['arms'])
                      and report['preflight'] == report['postflight'])
            elif name == 'block_encrypted':
                report = case['report']
                ok = (report['verdict'] == 'inconclusive' and report['arms'] == []
                      and report['reason'] == 'encrypted_or_unknown_protection'
                      and case['files'] == ['report.json'])
            else:
                ok = False
        except (KeyError, TypeError):
            ok = False
        if not ok:
            failed.append(name)
    return failed


def main():
    config = json.loads(sys.argv[1])
    setup()
    results = {}
    for case in config['cases']:
        if case == 'probe':
            results[case] = probe_case()
            continue
        if case in ('block', 'block_encrypted'):
            results[case] = block_case(config, 1 if case == 'block_encrypted' else 0)
            continue
        kind, role = case.split(':')
        artifact = live.Artifact(Path(config[('custom' if role == 'custom' else 'upstream')]),
                                 config[('custom' if role == 'custom' else 'upstream') + '_sha256'])
        if kind == 'unconfined':
            results[case] = unconfined_case(role, artifact)
        else:
            results[case] = candidate_case(role, artifact, kind)
    print(json.dumps(results, sort_keys=True))


if __name__ == '__main__':
    main()
