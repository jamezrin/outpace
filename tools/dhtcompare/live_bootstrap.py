"""Fixed PID-1 bootstrap for one live worker namespace. Run only by live.run_live.

argv: CONTROL_FD WORKER_ARGV... The supervisor sends one configuration message,
validates the attestation this process returns, then sends `go`. The worker runs
as a child with empty capability sets and no_new_privs; this process reports
resource usage and nftables counters after the worker exits.
"""
import ctypes
import json
import os
import socket
import subprocess
import sys
import time

IP = '/usr/bin/ip'
NFT = '/usr/sbin/nft'
MESSAGE = 65536
PR_CAPBSET_DROP, PR_SET_NO_NEW_PRIVS, PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL = 24, 38, 47, 4


def run(*args, data=None):
    return subprocess.run(args, input=data, capture_output=True, check=True, timeout=2,
                          env={'LC_ALL': 'C'}).stdout


def ip_json(*args):
    return json.loads(run(IP, '-j', *args))


def configure(config):
    for scope in ('all', 'default'):
        with open(f'/proc/sys/net/ipv6/conf/{scope}/disable_ipv6', 'w') as sysctl:
            sysctl.write('1')
    worker, relay = config['worker_device'], config['relay_device']
    run(IP, 'link', 'set', 'lo', 'up')
    run(IP, 'link', 'add', worker, 'type', 'veth', 'peer', 'name', relay)
    run(IP, 'link', 'set', relay, 'up')
    run(IP, 'link', 'set', worker, 'up')
    run(IP, 'address', 'add', f"{config['worker_address']}/{config['prefix']}", 'dev', worker)
    relay_mac = ip_json('link', 'show', relay)[0]['address']
    run(IP, 'neigh', 'add', config['gateway_address'], 'lladdr', relay_mac, 'dev', worker,
        'nud', 'permanent')
    run(IP, 'route', 'add', 'default', 'via', config['gateway_address'], 'dev', worker)
    run(NFT, '-f', '-', data=config['ruleset'].encode())
    packet = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3))
    packet.bind((relay, 0))
    attestation = {
        'links': ip_json('link', 'show'),
        'addresses': ip_json('address', 'show'),
        'routes4': ip_json('-4', 'route', 'show', 'table', 'all'),
        'routes6': ip_json('-6', 'route', 'show', 'table', 'all'),
        'ruleset': json.loads(run(NFT, '-j', 'list', 'ruleset')),
    }
    return packet, attestation


def drop_privileges():
    libc = ctypes.CDLL(None, use_errno=True)
    for capability in range(64):
        if libc.prctl(PR_CAPBSET_DROP, capability, 0, 0, 0) != 0 and ctypes.get_errno() != 22:
            raise OSError('capability bounding')
    if (libc.prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) != 0
            or libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0):
        raise OSError('privileges')
    header = (ctypes.c_uint32 * 2)(0x20080522, 0)
    data = (ctypes.c_uint32 * 6)()
    if libc.capset(header, data) != 0:
        raise OSError('capset')


def sample(pid):
    """Return (threads, sockets, VmHWM KiB) for the exec'd worker, or Nones once gone.

    wait4's ru_maxrss would retain the forked bootstrap's pre-exec peak, so the
    per-address-space VmHWM is sampled instead; the last sample is a lower bound.
    """
    fields = {}
    try:
        for line in open(f'/proc/{pid}/status'):
            key, _, value = line.partition(':')
            if key in ('Threads', 'VmHWM'):
                fields[key] = int(value.split()[0])
        sockets = sum(os.readlink(f'/proc/{pid}/fd/{fd}').startswith('socket:')
                      for fd in os.listdir(f'/proc/{pid}/fd'))
    except (OSError, ValueError, IndexError):
        return None, None, None
    return fields.get('Threads'), sockets, fields.get('VmHWM')


def main():
    if os.getpid() != 1 or len(sys.argv) < 3:
        raise SystemExit(125)
    control = socket.socket(fileno=int(sys.argv[1]))
    os.set_inheritable(control.fileno(), False)
    control.settimeout(5)
    config = json.loads(control.recv(MESSAGE))
    try:
        packet, attestation = configure(config)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, IndexError):
        control.send(json.dumps({'error': 'setup'}).encode())
        raise SystemExit(124)
    socket.send_fds(control, [json.dumps(attestation).encode()], [packet.fileno()])
    packet.close()
    if control.recv(16) != b'go':
        raise SystemExit(123)
    start = time.monotonic()
    child = os.fork()
    if child == 0:
        try:
            control.close()
            drop_privileges()
            os.execv(sys.argv[2], sys.argv[2:])
        finally:
            os._exit(126)
    peaks = {'threads': 0, 'sockets': 0, 'hwm': 0}
    samples = 0
    executable = os.stat(sys.argv[2])
    while True:
        pid, status, usage = os.wait4(child, os.WNOHANG)
        if pid == child:
            break
        try:
            image = os.stat(f'/proc/{child}/exe')
            execd = (image.st_dev, image.st_ino) == (executable.st_dev, executable.st_ino)
        except OSError:
            execd = False
        threads, sockets, hwm = sample(child) if execd else (None, None, None)
        if threads is not None and hwm is not None:
            samples += 1
            peaks['threads'] = max(peaks['threads'], threads)
            peaks['sockets'] = max(peaks['sockets'], sockets)
            peaks['hwm'] = max(peaks['hwm'], hwm)
        time.sleep(0.01)
    elapsed = time.monotonic() - start
    try:
        counters = json.loads(run(NFT, '-j', 'list', 'ruleset'))
    except (OSError, subprocess.SubprocessError, ValueError):
        counters = None
    control.send(json.dumps({
        'wait_status': status, 'elapsed_seconds': elapsed,
        'rss_hwm_kib_sampled': peaks['hwm'], 'user_seconds': usage.ru_utime,
        'system_seconds': usage.ru_stime, 'threads_peak_sampled': peaks['threads'],
        'sockets_peak_sampled': peaks['sockets'], 'samples': samples, 'ruleset': counters,
    }).encode())
    code = os.waitstatus_to_exitcode(status)
    raise SystemExit(code if code >= 0 else 128 - code)


if __name__ == '__main__':
    main()
