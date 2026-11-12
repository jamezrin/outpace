"""Fixed boundary probe run as a live worker in offline qualification only.

Exercises the destination matrix from two threads and sockets, inbound filtering,
privilege denials and filesystem confinement. argv: UNIX_SOCKET_PATH WRITE_PATH,
both supervisor-owned. Prints one JSON object.
"""
import errno
import json
import os
import socket
import subprocess
import sys
import threading

SYNTHETIC = ('11.0.0.2', 6881)
PRIVATE = ('10.0.0.5', '192.168.1.9', '172.16.0.1', '100.64.0.1', '169.254.1.1',
           '127.0.0.53', '10.200.0.1', '224.0.0.1', '255.255.255.255')


def exchange(results, name, message):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
        udp.settimeout(2)
        udp.sendto(message, SYNTHETIC)
        try:
            results[name] = udp.recvfrom(2048)[0].decode()
        except OSError:
            results[name] = None
        if message == b'unsolicited':
            extra = []
            udp.settimeout(0.7)
            try:
                while True:
                    extra.append(udp.recvfrom(2048)[0].decode())
            except OSError:
                pass
            results['unsolicited_extra'] = extra


def outcome(action):
    try:
        action()
        return 'sent'
    except OSError as error:
        return errno.errorcode.get(error.errno, str(error.errno))


def private_send(address):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
        udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp.sendto(b'leak', (address, 6881))


def tcp_connect():
    with socket.create_connection((SYNTHETIC[0], 80), timeout=1):
        pass


def ipv6_send():
    with socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as udp:
        udp.sendto(b'leak', ('2001:db8::1', 6881))


def raw_socket():
    socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_UDP).close()


def unix_connect(path):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as unix:
        unix.connect(path)


def file_write(path):
    with open(path, 'x') as out:
        out.write('leak')


def command(*args):
    return subprocess.run(args, capture_output=True, timeout=2).returncode


def main():
    results = {}
    threads = [threading.Thread(target=exchange, args=(results, name, message))
               for name, message in (('thread_a', b'ping-a'), ('thread_b', b'ping-b'),
                                     ('unsolicited', b'unsolicited'))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    results['private'] = {address: outcome(lambda a=address: private_send(a)) for address in PRIVATE}
    results['tcp'] = outcome(tcp_connect)
    results['ipv6'] = outcome(ipv6_send)
    results['raw'] = outcome(raw_socket)
    results['unix_connect'] = outcome(lambda: unix_connect(sys.argv[1]))
    results['file_write'] = outcome(lambda: file_write(sys.argv[2]))
    results['run_listing'] = sorted(os.listdir('/run'))
    results['nft_flush'] = command('/usr/sbin/nft', 'flush', 'ruleset')
    results['link_down'] = command('/usr/bin/ip', 'link', 'set', 'dhtw0', 'down')
    status = {}
    for line in open('/proc/self/status'):
        key, _, value = line.partition(':')
        if key in ('CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb', 'NoNewPrivs'):
            status[key] = value.strip()
    results['status'] = status
    print(json.dumps(results, sort_keys=True))


if __name__ == '__main__':
    main()
