"""Supervisor-side UDP relay enforcing the live destination and server policy.

Frames come from the worker namespace's relay-side veth. Only IPv4 UDP to permitted
global destinations is forwarded, from root-namespace sockets bound to the egress
device. Replies are injected only from endpoints the same inner socket contacted.
All state is bounded; counters carry no addresses. `admitted_out` and
`replies_admitted` count successful local sends, not delivery. Kernel receive-queue
drops on the packet and outer sockets are reported separately from relay decisions.
There is no rate limit beyond MAX_DATAGRAMS per worker.
"""
import ipaddress
import socket
import struct
from live_policy import MTU_PAYLOAD, WORKER_ADDRESS, permitted

MAX_MAPPINGS = 8
MAX_CONTACTS = 32768
MAX_DATAGRAMS = 100000
FRAMES_PER_WAKE = 256
COUNTERS = (
    'frames_in', 'non_ipv4', 'malformed', 'fragments', 'non_udp', 'oversized_out',
    'denied_destination', 'mapping_cap', 'contact_cap', 'datagram_cap', 'send_errors',
    'admitted_out', 'admitted_out_bytes', 'unique_contacts', 'replied_contacts',
    'replies_admitted', 'unsolicited_in', 'oversized_in', 'receive_errors', 'mappings_opened',
    'open_errors',
)
SOL_PACKET, PACKET_STATISTICS = 263, 6
SO_RXQ_OVFL = 40
_ETH_IPV4 = 0x0800
_WORKER = ipaddress.IPv4Address(WORKER_ADDRESS).packed


def checksum(data):
    if len(data) % 2:
        data += b'\0'
    total = sum(struct.unpack(f'!{len(data) // 2}H', data))
    while total >> 16:
        total = (total & 0xffff) + (total >> 16)
    return ~total & 0xffff


def parse_frame(frame):
    """Classify one Ethernet frame: ('udp', src_port, dst_ip, dst_port, payload) or (reason,)."""
    if len(frame) < 14 or struct.unpack_from('!H', frame, 12)[0] != _ETH_IPV4:
        return ('non_ipv4',)
    packet = frame[14:]
    if len(packet) < 20 or packet[0] >> 4 != 4:
        return ('malformed',)
    header = (packet[0] & 0x0f) * 4
    total = struct.unpack_from('!H', packet, 2)[0]
    if header < 20 or total < header or total > len(packet):
        return ('malformed',)
    if struct.unpack_from('!H', packet, 6)[0] & 0x3fff:
        return ('fragments',)
    if packet[9] != 17:
        return ('non_udp',)
    if total - header < 8:
        return ('malformed',)
    sport, dport, length = struct.unpack_from('!HHH', packet, header)
    if length < 8 or length > total - header:
        return ('malformed',)
    payload = packet[header + 8:header + length]
    if len(payload) > MTU_PAYLOAD:
        return ('oversized_out',)
    return ('udp', sport, socket.inet_ntoa(packet[16:20]), dport, payload)


def build_frame(worker_mac, gateway_mac, source_ip, source_port, inner_port, payload):
    """Build one checksummed Ethernet/IPv4/UDP reply addressed to the inner worker."""
    source = socket.inet_aton(source_ip)
    length = 8 + len(payload)
    pseudo = source + _WORKER + struct.pack('!BBH', 0, 17, length)
    udp = struct.pack('!HHHH', source_port, inner_port, length, 0) + payload
    udp_sum = checksum(pseudo + udp) or 0xffff
    udp = udp[:6] + struct.pack('!H', udp_sum) + udp[8:]
    ip = struct.pack('!BBHHHBBH4s4s', 0x45, 0, 20 + length, 0, 0x4000, 64, 17, 0,
                     source, _WORKER)
    ip = ip[:10] + struct.pack('!H', checksum(ip)) + ip[12:]
    return worker_mac + gateway_mac + struct.pack('!H', _ETH_IPV4) + ip + udp


def egress_socket(device):
    outer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        outer.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, device.encode())
        outer.bind(('0.0.0.0', 0))
        outer.setblocking(False)
    except OSError:
        outer.close()
        raise
    return outer


class Relay:
    """Bounded endpoint-dependent relay for one worker namespace."""

    def __init__(self, packet, *, worker_mac, gateway_mac, open_outer):
        if len(worker_mac) != 6 or len(gateway_mac) != 6:
            raise ValueError('relay_mac')
        self.packet = packet
        self.worker_mac = worker_mac
        self.gateway_mac = gateway_mac
        self.open_outer = open_outer
        self.mappings = {}
        self.contacts = {}
        self.replied = set()
        self.outer_drops = {}
        self.packet_drops = 0
        self.counters = dict.fromkeys(COUNTERS, 0)
        self.selector = None

    def register(self, selector):
        self.selector = selector
        self.packet.setblocking(False)
        selector.register(self.packet, 1, ('frame', None))

    def handle(self, tag):
        kind, inner_port = tag
        if kind == 'frame':
            self._frames()
        else:
            self._replies(inner_port)

    def _frames(self):
        for _ in range(FRAMES_PER_WAKE):
            try:
                frame, address = self.packet.recvfrom(65536)
            except BlockingIOError:
                return
            except OSError:
                self.counters['receive_errors'] += 1
                return
            if len(address) > 2 and address[2] == socket.PACKET_OUTGOING:
                continue
            self.forward(frame)

    def forward(self, frame):
        self.counters['frames_in'] += 1
        parsed = parse_frame(frame)
        if parsed[0] != 'udp':
            self.counters[parsed[0]] += 1
            return
        _, inner_port, address, port, payload = parsed
        if not permitted(address, port):
            self.counters['denied_destination'] += 1
            return
        if self.counters['admitted_out'] >= MAX_DATAGRAMS:
            self.counters['datagram_cap'] += 1
            return
        outer = self.mappings.get(inner_port)
        if outer is None:
            if len(self.mappings) >= MAX_MAPPINGS:
                self.counters['mapping_cap'] += 1
                return
            try:
                outer = self.open_outer()
                outer.setsockopt(socket.SOL_SOCKET, SO_RXQ_OVFL, 1)
            except OSError:
                self.counters['open_errors'] += 1
                return
            self.mappings[inner_port] = outer
            self.outer_drops[inner_port] = 0
            self.counters['mappings_opened'] += 1
            self.contacts[inner_port] = set()
            if self.selector is not None:
                self.selector.register(outer, 1, ('outer', inner_port))
        contacts = self.contacts[inner_port]
        endpoint = (address, port)
        if endpoint not in contacts:
            if self.counters['unique_contacts'] >= MAX_CONTACTS:
                self.counters['contact_cap'] += 1
                return
            contacts.add(endpoint)
            self.counters['unique_contacts'] += 1
        try:
            outer.sendto(payload, endpoint)
        except OSError:
            self.counters['send_errors'] += 1
            return
        self.counters['admitted_out'] += 1
        self.counters['admitted_out_bytes'] += len(payload)

    def _replies(self, inner_port):
        outer = self.mappings[inner_port]
        for _ in range(FRAMES_PER_WAKE):
            try:
                payload, ancillary, flags, source = outer.recvmsg(MTU_PAYLOAD + 1, socket.CMSG_SPACE(4))
            except BlockingIOError:
                return
            except OSError:
                self.counters['receive_errors'] += 1
                return
            for level, kind, data in ancillary:
                if level == socket.SOL_SOCKET and kind == SO_RXQ_OVFL and len(data) >= 4:
                    self.outer_drops[inner_port] = max(self.outer_drops[inner_port],
                                                       struct.unpack('=I', data[:4])[0])
            self.inject(inner_port, payload, flags, source)

    def inject(self, inner_port, payload, flags, source):
        if (source[0], source[1]) not in self.contacts[inner_port]:
            self.counters['unsolicited_in'] += 1
            return
        if flags & socket.MSG_TRUNC or len(payload) > MTU_PAYLOAD:
            self.counters['oversized_in'] += 1
            return
        frame = build_frame(self.worker_mac, self.gateway_mac, source[0], source[1],
                            inner_port, payload)
        try:
            self.packet.send(frame)
        except OSError:
            self.counters['send_errors'] += 1
            return
        self.counters['replies_admitted'] += 1
        key = (inner_port, source[0], source[1])
        if key not in self.replied:
            self.replied.add(key)
            self.counters['replied_contacts'] += 1

    def _packet_statistics(self):
        """Accumulate kernel drops on the packet socket; reading resets the kernel counters."""
        try:
            _, drops = struct.unpack('=II', self.packet.getsockopt(SOL_PACKET, PACKET_STATISTICS, 8))
        except (OSError, struct.error):
            return None
        self.packet_drops += drops
        return self.packet_drops

    def close(self):
        for outer in self.mappings.values():
            if self.selector is not None:
                self.selector.unregister(outer)
            outer.close()
        self.mappings.clear()
        if self.selector is not None:
            self.selector.unregister(self.packet)
            self.selector = None
        self.packet.close()

    def report(self):
        result = dict(self.counters)
        result['unanswered_contacts'] = result['unique_contacts'] - result['replied_contacts']
        result['packet_socket_kernel_drops'] = self._packet_statistics()
        result['outer_socket_kernel_drops'] = sum(self.outer_drops.values())
        return result
