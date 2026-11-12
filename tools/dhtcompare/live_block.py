"""One bounded three-arm live DHT comparison block (#44). Linux, run as host root.

stdin: one JSON object {"content_id": "<40 hex>"} resolved by the operator; this
program never reads any registry. The content id is resolved once to an infohash
through the local daemon's loopback `analyze_content` method; neither value nor
any endpoint is written. Bootstrap hostnames are resolved once, and the same
numeric seeds go to every arm. Workers run through the live boundary (live.py) in
the fixed order custom, mainline, n0, inside one 135-second deadline, without
retries. No arm starts unless all three arms and the postflight still fit, so a
block never spends its observation on an unmatched subset. SIGTERM ends the block
as inconclusive after owned cleanup. Gateway/VPN exit provenance is the
operator's separate responsibility.

Exit 0: all three arms produced valid records with confirmed cleanup.
Exit 1: an arm failed with confirmed cleanup. Exit 2: inconclusive.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import live
import live_policy
from live_relay import egress_socket
from qualification import Artifact

BLOCK_SECONDS = 135
ARM_RESERVE = 30
POSTFLIGHT_RESERVE = 7
DNS_SECONDS = 10
ORDER = ('custom', 'mainline', 'n0')
# Mirrors ace-swarm's production DHT bootstrap list; a test keeps them identical.
BOOTSTRAP = ('router.bittorrent.com:6881', 'router.utorrent.com:6881',
             'dht.transmissionbt.com:6881', 'dht.libtorrent.org:25401')
HTTP_CAP = 65536


class Inconclusive(Exception):
    pass


class Terminated(BaseException):
    pass


def _terminate(*_):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise Terminated


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _command_json(*args):
    output = subprocess.run(args, capture_output=True, check=True, timeout=3,
                            env={'LC_ALL': 'C'}).stdout
    return json.loads(output) if output.strip() else []


def network_state(egress_device):
    """Digest-only host network snapshot; requires one main default route via egress."""
    routes = _command_json('/usr/bin/ip', '-j', '-4', 'route', 'show', 'table', 'all')
    defaults = [r for r in routes if r.get('dst') == 'default' and r.get('table', 'main') == 'main']
    ruleset = _command_json('/usr/sbin/nft', '-j', '-s', 'list', 'ruleset')
    if type(ruleset) is dict:
        ruleset = [item for item in ruleset.get('nftables', ()) if 'metainfo' not in item]
    disabled = Path('/proc/sys/net/ipv6/conf/all/disable_ipv6').read_text().strip() == '1'
    return {'default_via_egress': len(defaults) == 1 and defaults[0].get('dev') == egress_device,
            'route_sha256': _digest(routes), 'ruleset_sha256': _digest(ruleset),
            'ipv6_disabled': disabled}


def resolve_target(content_id, port, deadline):
    """Resolve one public, unencrypted content id to an infohash via the loopback daemon."""
    if (type(content_id) is not str or len(content_id) != 40
            or any(c not in '0123456789abcdef' for c in content_id.lower())):
        raise Inconclusive('content_id')
    query = urllib.parse.urlencode({'method': 'analyze_content', 'content_id': content_id})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    timeout = min(15, deadline - time.monotonic())
    if timeout <= 0:
        raise Inconclusive('resolution_deadline')
    try:
        with opener.open(f'http://127.0.0.1:{port}/server/api?{query}', timeout=timeout) as response:
            body = response.read(HTTP_CAP + 1)
            if response.status != 200 or len(body) > HTTP_CAP:
                raise Inconclusive('resolution_http')
        envelope = json.loads(body)
    except (OSError, ValueError):
        raise Inconclusive('resolution') from None
    result = envelope.get('result') if type(envelope) is dict else None
    if type(result) is not dict or envelope.get('error') is not None:
        raise Inconclusive('resolution')
    infohash = result.get('infohash')
    if (type(infohash) is not str or len(infohash) != 40
            or any(c not in '0123456789abcdef' for c in infohash.lower())):
        raise Inconclusive('resolution')
    if result.get('is_encrypted') != 0:
        raise Inconclusive('encrypted_or_unknown_protection')
    return infohash.lower(), result.get('is_live') == 1


def resolve_seeds(hosts, deadline):
    """Resolve each host once in parallel; keep the first permitted IPv4 per host."""
    found = {}

    def lookup(entry):
        host, _, port = entry.rpartition(':')
        try:
            infos = socket.getaddrinfo(host, int(port), socket.AF_INET, socket.SOCK_DGRAM)
        except (OSError, ValueError):
            return
        for info in infos:
            address = info[4][0]
            if live_policy.permitted(address, int(port)):
                found[entry] = f'{address}:{port}'
                return
    threads = [threading.Thread(target=lookup, args=(h,), daemon=True) for h in hosts]
    for thread in threads:
        thread.start()
    joined = min(deadline, time.monotonic() + DNS_SECONDS)
    for thread in threads:
        thread.join(max(0, joined - time.monotonic()))
    seeds = []
    for entry in hosts:
        seed = found.get(entry)
        if seed is not None and seed not in seeds:
            seeds.append(seed)
    return seeds[:4], sum(thread.is_alive() for thread in threads)


def _write_private(directory, name, data):
    path = directory / name
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as out:
        out.write(data)


def _process(result):
    return {key: getattr(result, key) for key in
            ('exit_code', 'timed_out', 'cleanup_complete', 'truncated', 'attested', 'denial',
             'stdout_bytes', 'stderr_bytes')}


def _consistency(boundary, inner):
    """Relay and nftables observe the same worker UDP; disagreement invalidates the arm."""
    if boundary is None or inner is None or inner.get('policy_counters') is None:
        return None
    policy = inner['policy_counters']
    udp_frames = boundary['frames_in'] - sum(boundary[key] for key in
                                             ('non_ipv4', 'malformed', 'fragments', 'non_udp'))
    return {'denials_agree': boundary['denied_destination'] == policy['out_udp_denied'],
            'udp_frames_agree': udp_frames == policy['out_udp'] + policy['out_udp_denied'],
            'kernel_drops': boundary['packet_socket_kernel_drops'] != 0
            or boundary['outer_socket_kernel_drops'] != 0}


def run_block(artifacts, *, content_id, egress_device, output, resolver_port=6878,
              bootstrap=BOOTSTRAP, open_outer=None):
    start = time.monotonic(); deadline = start + BLOCK_SECONDS
    output = Path(output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    open_outer = open_outer or (lambda: egress_socket(egress_device))
    report = {'schema': 'dht-live-block-v1', 'order': list(ORDER), 'arms': [],
              'artifacts': {role: artifacts[role].sha256 for role in ('custom', 'upstream')},
              'egress_device': egress_device, 'verdict': 'inconclusive', 'reason': None}
    try:
        for artifact in artifacts.values():
            artifact.verify()
        preflight = network_state(egress_device)
        report['preflight'] = preflight
        if not preflight['default_via_egress']:
            raise Inconclusive('egress_route')
        resolution_start = time.monotonic()
        target, is_live = resolve_target(content_id, resolver_port, deadline)
        report['target_resolution_seconds'] = time.monotonic() - resolution_start
        report['target_is_live'] = is_live
        dns_start = time.monotonic()
        seeds, stuck = resolve_seeds(bootstrap, deadline)
        report['dns_seconds'] = time.monotonic() - dns_start
        report['dns_lookups_unfinished'] = stuck
        report['seed_count'] = len(seeds)
        if not seeds:
            raise Inconclusive('no_bootstrap_seed')
        if deadline - time.monotonic() < len(ORDER) * ARM_RESERVE + POSTFLIGHT_RESERVE:
            raise Inconclusive('admission_deadline')
        failed = False
        for role in ORDER:
            if deadline - time.monotonic() < ARM_RESERVE + POSTFLIGHT_RESERVE:
                raise Inconclusive('block_deadline')
            artifact = artifacts['custom' if role == 'custom' else 'upstream']
            arm = {'role': role}
            report['arms'].append(arm)
            try:
                observation = live.run_live_worker(
                    role, artifact, target=target, seeds=','.join(seeds),
                    deadline=time.monotonic() + ARM_RESERVE, open_outer=open_outer)
                result, arm['record'], arm['status'] = observation.result, observation.record, 'complete'
                arm['parent_elapsed_seconds'] = observation.elapsed
            except live.LiveWorkerError as error:
                result, arm['status'], arm['error'] = error.result, 'failed', str(error)
                failed = True
                if result is None:
                    raise Inconclusive(f'prelaunch_{error}') from None
            if result is not None:
                arm['process'] = _process(result)
                arm['record_line_seen'] = b'"schema_version"' in result.stdout
                arm['boundary'] = result.boundary
                arm['inner'] = result.inner
                arm['consistency'] = _consistency(result.boundary, result.inner)
                _write_private(output, f'{role}.stdout.private', result.stdout)
                _write_private(output, f'{role}.stderr.private', result.stderr)
            if result is None or not result.cleanup_complete:
                raise Inconclusive('cleanup_uncertain')
        postflight = network_state(egress_device)
        report['postflight'] = postflight
        if postflight != preflight:
            raise Inconclusive('host_network_drift')
        report['verdict'] = 'failed' if failed else 'complete'
    except Inconclusive as reason:
        report['reason'] = str(reason)
    except Terminated:
        report['reason'] = 'terminated'
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report['reason'] = type(error).__name__
    report['block_elapsed_seconds'] = time.monotonic() - start
    _write_private(output, 'report.json', json.dumps(report, sort_keys=True, indent=1).encode())
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--custom', type=Path, required=True)
    parser.add_argument('--custom-sha256', required=True)
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--upstream-sha256', required=True)
    parser.add_argument('--egress-device', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        payload = json.loads(sys.stdin.read(4096))
        content_id = payload['content_id'] if type(payload) is dict and set(payload) == {'content_id'} else None
    except (ValueError, KeyError):
        content_id = None
    if content_id is None:
        print('dht live block: invalid stdin payload', file=sys.stderr)
        return 2
    signal.signal(signal.SIGTERM, _terminate)
    artifacts = {'custom': Artifact(args.custom, args.custom_sha256),
                 'upstream': Artifact(args.upstream, args.upstream_sha256)}
    report = run_block(artifacts, content_id=content_id, egress_device=args.egress_device,
                       output=args.output)
    print(f"dht live block: {report['verdict']} ({report['reason'] or 'ok'})", file=sys.stderr)
    return {'complete': 0, 'failed': 1}.get(report['verdict'], 2)


if __name__ == '__main__':
    raise SystemExit(main())
