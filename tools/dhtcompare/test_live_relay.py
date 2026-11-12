import ipaddress
import json
from pathlib import Path
import re
import socket
import struct
import tempfile
import types
import unittest
from unittest import mock
import live
import live_block
import live_policy
import live_relay
from live_relay import Relay, build_frame, checksum, parse_frame

WORKER_MAC = bytes.fromhex('020000000002')
GATEWAY_MAC = bytes.fromhex('020000000001')


def outbound(dst, dport, payload=b'q', *, sport=40000, proto=17, flags=0x4000, ethertype=0x0800):
    length = 8 + len(payload)
    udp = struct.pack('!HHHH', sport, dport, length, 0) + payload
    ip = struct.pack('!BBHHHBBH4s4s', 0x45, 0, 20 + length, 0, flags, 64, proto, 0,
                     socket.inet_aton(live_policy.WORKER_ADDRESS), socket.inet_aton(dst))
    return GATEWAY_MAC + WORKER_MAC + struct.pack('!H', ethertype) + ip + udp


class FakeOuter:
    def __init__(self):
        self.sent = []
        self.closed = False
        self.inbox = []

    def sendto(self, payload, endpoint):
        self.sent.append((payload, endpoint))

    def setsockopt(self, *option):
        self.option = option

    def recvmsg(self, *_):
        if not self.inbox:
            raise BlockingIOError
        return self.inbox.pop(0)


class FakePacket:
    def __init__(self):
        self.sent = []
        self.closed = False
        self.drops = [3, 2]

    def send(self, frame):
        self.sent.append(frame)

    def getsockopt(self, level, option, size):
        if self.closed:
            raise OSError
        return struct.pack('=II', 10, self.drops.pop(0) if self.drops else 0)

    def close(self):
        self.closed = True


def relay():
    outers = []

    def open_outer():
        outers.append(FakeOuter())
        outers[-1].close = lambda o=outers[-1]: setattr(o, 'closed', True)
        return outers[-1]
    return Relay(FakePacket(), worker_mac=WORKER_MAC, gateway_mac=GATEWAY_MAC,
                 open_outer=open_outer), outers


class PolicyTests(unittest.TestCase):
    def test_permits_only_global_unicast_with_nonzero_port(self):
        for address in ('11.0.0.2', '1.1.1.1', '8.8.8.8', '45.1.2.3'):
            self.assertTrue(live_policy.permitted(address, 6881), address)
        for address in ('10.0.0.5', '192.168.1.9', '172.16.0.1', '127.0.0.1', '100.64.0.1',
                        '169.254.1.1', '224.0.0.1', '255.255.255.255', '0.0.0.0',
                        '198.18.0.1', '192.0.2.1', '240.0.0.1', '10.203.0.1'):
            self.assertFalse(live_policy.permitted(address, 6881), address)
        self.assertFalse(live_policy.permitted('1.1.1.1', 0))
        self.assertFalse(live_policy.permitted('::1', 6881))
        self.assertFalse(live_policy.permitted('1.1.1.1', True))

    def test_policy_is_at_least_as_strict_as_python_global_scope(self):
        for high in range(1 << 16):
            for low in (1, 0xfffe):
                address = ipaddress.IPv4Address((high << 16) | low)
                if not address.is_global or address.is_multicast:
                    self.assertFalse(live_policy.permitted(str(address), 6881), str(address))

    def test_ruleset_names_every_prefix_and_rule(self):
        text = live_policy.nft_ruleset()
        for prefix in live_policy.DENIED_V4:
            self.assertIn(prefix, text)
        rules = [(m.group(2), m.group(1)) for m in
                 re.finditer(r'counter (accept|drop) comment "([a-z_0-9]+)"', text)]
        self.assertEqual(rules, [
            ('out_lo', 'accept'), ('out_udp_denied', 'accept'), ('out_udp', 'accept'),
            ('out_icmp_drop', 'drop'), ('out_drop', 'drop'),
            ('in_lo', 'accept'), ('in_udp_established', 'accept'), ('in_drop', 'drop')])
        self.assertEqual([name for name, _ in rules], list(live_policy.RULE_COMMENTS))
        self.assertEqual(text.count('policy drop;'), 2)
        self.assertIn('oifname "dhtw0" meta nfproto ipv4 meta l4proto udp ip daddr @denied4 counter', text)
        self.assertIn('iifname "dhtw0" meta nfproto ipv4 meta l4proto udp ct state established counter', text)

    def listing(self, comments=live_policy.RULE_COMMENTS, prefixes=live_policy.DENIED_V4):
        elem = [{'prefix': {'addr': p.split('/')[0], 'len': int(p.split('/')[1])}} for p in prefixes]
        items = [{'metainfo': {'version': 'x'}},
                 {'table': {'family': 'inet', 'name': 'dht_live'}},
                 {'chain': {'name': 'output', 'hook': 'output', 'policy': 'drop', 'type': 'filter'}},
                 {'chain': {'name': 'input', 'hook': 'input', 'policy': 'drop', 'type': 'filter'}},
                 {'set': {'name': 'denied4', 'type': 'ipv4_addr', 'elem': elem}}]
        for index, comment in enumerate(comments):
            items.append({'rule': {'table': 'dht_live', 'comment': comment,
                                   'expr': [{'counter': {'packets': index, 'bytes': 99}}]}})
        return {'nftables': items}

    def test_ruleset_counters_require_exact_structure(self):
        counters = live_policy.ruleset_counters(self.listing())
        self.assertEqual(list(counters), list(live_policy.RULE_COMMENTS))
        self.assertEqual(counters['out_drop'], live_policy.RULE_COMMENTS.index('out_drop'))
        bad = [self.listing(comments=live_policy.RULE_COMMENTS[:-1]),
               self.listing(prefixes=live_policy.DENIED_V4[1:])]
        extra = self.listing()
        extra['nftables'].append({'table': {'family': 'ip', 'name': 'other'}})
        bad.append(extra)
        accept = json.loads(json.dumps(self.listing()))
        accept['nftables'][2]['chain']['policy'] = 'accept'
        bad.append(accept)
        unknown = self.listing()
        unknown['nftables'].append({'flowtable': {}})
        bad.append(unknown)
        for listing in bad:
            with self.assertRaises(ValueError):
                live_policy.ruleset_counters(listing)


class FrameTests(unittest.TestCase):
    def test_parse_classifies_frames(self):
        self.assertEqual(parse_frame(outbound('11.0.0.2', 6881, b'abc')),
                         ('udp', 40000, '11.0.0.2', 6881, b'abc'))
        self.assertEqual(parse_frame(outbound('11.0.0.2', 6881, ethertype=0x86dd)), ('non_ipv4',))
        self.assertEqual(parse_frame(outbound('11.0.0.2', 6881, proto=6)), ('non_udp',))
        self.assertEqual(parse_frame(outbound('11.0.0.2', 6881, flags=0x2000)), ('fragments',))
        self.assertEqual(parse_frame(outbound('11.0.0.2', 6881)[:30]), ('malformed',))
        self.assertEqual(parse_frame(outbound('11.0.0.2', 6881, b'x' * 1473)), ('oversized_out',))

    def test_built_reply_has_valid_checksums_and_addresses(self):
        frame = build_frame(WORKER_MAC, GATEWAY_MAC, '11.0.0.2', 6881, 40000, b'reply')
        self.assertEqual(frame[:12], WORKER_MAC + GATEWAY_MAC)
        ip = frame[14:34]
        self.assertEqual(checksum(ip), 0)
        self.assertEqual(socket.inet_ntoa(ip[12:16]), '11.0.0.2')
        self.assertEqual(socket.inet_ntoa(ip[16:20]), live_policy.WORKER_ADDRESS)
        udp = frame[34:]
        pseudo = ip[12:20] + struct.pack('!BBH', 0, 17, len(udp))
        self.assertEqual(checksum(pseudo + udp), 0)
        self.assertEqual(struct.unpack('!HH', udp[:4]), (6881, 40000))
        self.assertEqual(udp[8:], b'reply')


class RelayTests(unittest.TestCase):
    def test_forwards_permitted_and_counts_denials(self):
        r, outers = relay()
        r.forward(outbound('11.0.0.2', 6881))
        for dst in ('10.0.0.5', '192.168.1.9', '127.0.0.1', '224.0.0.1', '10.200.0.1'):
            r.forward(outbound(dst, 6881))
        r.forward(outbound('11.0.0.2', 0))
        r.forward(outbound('11.0.0.2', 80, proto=6))
        self.assertEqual(len(outers), 1)
        self.assertEqual(outers[0].sent, [(b'q', ('11.0.0.2', 6881))])
        report = r.report()
        self.assertEqual(report['denied_destination'], 6)
        self.assertEqual(report['non_udp'], 1)
        self.assertEqual(report['admitted_out'], 1)
        self.assertEqual(report['unique_contacts'], 1)
        self.assertEqual(report['frames_in'], 8)

    def test_replies_only_from_contacted_endpoints_of_the_same_mapping(self):
        r, _ = relay()
        r.forward(outbound('11.0.0.2', 6881, sport=40000))
        r.forward(outbound('11.0.0.3', 6881, sport=40001))
        r.inject(40000, b'ok', 0, ('11.0.0.2', 6881))
        r.inject(40000, b'ok', 0, ('11.0.0.2', 6881))
        r.inject(40000, b'wrong-port', 0, ('11.0.0.2', 6882))
        r.inject(40000, b'other-mapping', 0, ('11.0.0.3', 6881))
        r.inject(40000, b'stranger', 0, ('11.0.0.9', 6881))
        r.inject(40000, b'x', socket.MSG_TRUNC, ('11.0.0.2', 6881))
        report = r.report()
        self.assertEqual(report['replies_admitted'], 2)
        self.assertEqual(report['replied_contacts'], 1)
        self.assertEqual(report['unsolicited_in'], 3)
        self.assertEqual(report['oversized_in'], 1)
        self.assertEqual(report['unanswered_contacts'], 1)
        self.assertEqual(len(r.packet.sent), 2)

    def test_mapping_contact_and_datagram_caps(self):
        r, outers = relay()
        for port in range(live_relay.MAX_MAPPINGS + 2):
            r.forward(outbound('11.0.0.2', 6881, sport=41000 + port))
        self.assertEqual(len(outers), live_relay.MAX_MAPPINGS)
        self.assertEqual(r.report()['mapping_cap'], 2)
        r, _ = relay()
        r.counters['unique_contacts'] = live_relay.MAX_CONTACTS
        r.forward(outbound('11.0.0.2', 6881))
        self.assertEqual(r.report()['contact_cap'], 1)
        r, _ = relay()
        r.counters['admitted_out'] = live_relay.MAX_DATAGRAMS
        r.forward(outbound('11.0.0.2', 6881))
        self.assertEqual(r.report()['datagram_cap'], 1)

    def test_open_failure_is_counted_and_not_mapped(self):
        def refuse():
            raise OSError('no egress')
        r = Relay(FakePacket(), worker_mac=WORKER_MAC, gateway_mac=GATEWAY_MAC, open_outer=refuse)
        r.forward(outbound('11.0.0.2', 6881))
        report = r.report()
        self.assertEqual((report['open_errors'], report['admitted_out'], report['mappings_opened']), (1, 0, 0))

    def test_reports_kernel_drops_separately(self):
        r, outers = relay()
        r.forward(outbound('11.0.0.2', 6881))
        self.assertEqual(outers[0].option, (socket.SOL_SOCKET, live_relay.SO_RXQ_OVFL, 1))
        overflow = [(socket.SOL_SOCKET, live_relay.SO_RXQ_OVFL, struct.pack('=I', count)) for count in (4, 7)]
        outers[0].inbox = [(b'a', overflow[:1], 0, ('11.0.0.2', 6881)), (b'b', overflow[1:], 0, ('11.0.0.2', 6881))]
        r.handle(('outer', 40000))
        self.assertEqual(r.report()['outer_socket_kernel_drops'], 7)
        self.assertEqual(r.report()['packet_socket_kernel_drops'], 5)
        r.close()
        self.assertIsNone(r.report()['packet_socket_kernel_drops'])

    def test_close_releases_every_owned_socket(self):
        r, outers = relay()
        r.forward(outbound('11.0.0.2', 6881, sport=40000))
        r.forward(outbound('11.0.0.2', 6881, sport=40001))
        r.close()
        self.assertTrue(all(o.closed for o in outers))
        self.assertTrue(r.packet.closed)
        self.assertEqual(r.report()['mappings_opened'], 2)


class ListenerInventoryTests(unittest.TestCase):
    def test_collects_absolute_bound_paths_only(self):
        rows = ['Num       RefCount Protocol Flags    Type St Inode Path',
                '0: 00000002 00000000 00010000 0001 01 1 /run/systemd/private',
                '0: 00000002 00000000 00010000 0001 01 2 private/anvil',
                '0: 00000002 00000000 00010000 0001 01 3 @abstract',
                '0: 00000002 00000000 00000000 0002 01 4 /run/a path with spaces',
                '0: 00000002 00000000 00000000 0001 03 5',
                '0: 00000002 00000000 00010000 0001 01 6 /run/systemd/private']
        with tempfile.NamedTemporaryFile('w') as listing:
            listing.write('\n'.join(rows) + '\n')
            listing.flush()
            self.assertEqual(live.bound_unix_paths(listing.name),
                             ['/run/a path with spaces', '/run/systemd/private'])


    def test_block_admission_counts_paths_outside_hidden_directories(self):
        original = live.bound_unix_paths
        live.bound_unix_paths = lambda: ['/run/systemd/private', '/var/run/x', '/tmp/a/b', '/srv/app.sock',
                                         '/runner.sock']
        try:
            self.assertEqual(live_block.uncovered_listeners(), 2)
        finally:
            live.bound_unix_paths = original


class FakeEvents:
    def __init__(self, messages):
        self.messages = messages
        self.closed = False

    def recv(self, size):
        if not self.messages:
            raise BlockingIOError
        return self.messages.pop(0)

    def close(self):
        self.closed = True


class HostChangeTests(unittest.TestCase):
    def test_any_host_address_event_stops_forwarding(self):
        events = FakeEvents([])
        r, _ = relay()
        r.host_events = events
        r.forward(outbound('11.0.0.2', 6881))
        events.messages.append(b'newaddr')
        r.forward(outbound('11.0.0.2', 6881))
        r.forward(outbound('10.0.0.5', 6881))
        events.messages.append(b'deladdr')
        r.forward(outbound('11.0.0.3', 6881))
        report = r.report()
        self.assertEqual((report['admitted_out'], report['host_change_denied'], report['denied_destination'],
                          report['host_address_events']), (1, 2, 1, 2))
        r.close()
        self.assertFalse(events.closed)

    def test_a_receive_error_counts_as_a_change(self):
        class Failing:
            def recv(self, size):
                raise OSError('ENOBUFS')
        self.assertEqual(live_relay.drain_host_events(Failing()), 1)


class BlockHostChangeTests(unittest.TestCase):
    """The block subscribes before its address check and drains changes before every arm."""

    def run_block(self, *, on_check=lambda events: None, on_arm=lambda events: None):
        events = FakeEvents([])
        artifact = types.SimpleNamespace(verify=lambda: None, sha256='0' * 64)
        result = types.SimpleNamespace(exit_code=0, timed_out=False, cleanup_complete=True, truncated=False,
                                       attested=True, denial=None, stdout_bytes=0, stderr_bytes=0,
                                       stdout=b'', stderr=b'', boundary=None, inner=None)
        arms = []

        def check(exempt):
            on_check(events)
            return 0

        def worker(role, *args, host_events, **kwargs):
            self.assertIs(host_events, events)
            arms.append(role)
            on_arm(events)
            return types.SimpleNamespace(result=result, record={}, elapsed=1.0)
        state = {'default_via_egress': True, 'ipv6_disabled': True}
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(live_block, 'host_address_monitor', lambda: events), \
                mock.patch.object(live_block, 'network_state', lambda device: state), \
                mock.patch.object(live_block, 'host_global_addresses', check), \
                mock.patch.object(live_block, 'resolve_target', lambda *a: ('0123456789abcdef' * 2 + '01234567', True)), \
                mock.patch.object(live_block, 'resolve_seeds', lambda *a: (['11.0.0.2:6881'], 0)), \
                mock.patch.object(live_block, 'uncovered_listeners', lambda: 0), \
                mock.patch.object(live, 'run_live_worker', worker):
            report = live_block.run_block({'custom': artifact, 'upstream': artifact},
                                          content_id='0123456789abcdef0123456789abcdef01234567',
                                          egress_device='dhtq0', output=Path(directory) / 'block')
        self.assertTrue(events.closed)
        return report, arms

    def test_unchanged_host_completes_all_arms(self):
        report, arms = self.run_block()
        self.assertEqual((report['verdict'], arms), ('complete', ['custom', 'mainline', 'n0']))

    def test_change_during_the_address_check_prevents_every_arm(self):
        report, arms = self.run_block(on_check=lambda events: events.messages.append(b'newaddr'))
        self.assertEqual((report['reason'], arms), ('host_address_change', []))

    def test_change_between_arms_prevents_the_next_arm(self):
        def change_once(events):
            if not hasattr(change_once, 'done'):
                change_once.done = events.messages.append(b'newaddr')
        report, arms = self.run_block(on_arm=change_once)
        self.assertEqual((report['reason'], arms), ('host_address_change', ['custom']))


class LauncherIdentityTests(unittest.TestCase):
    def maps(self, *contents):
        files = []
        for content in contents:
            table = tempfile.NamedTemporaryFile('w')
            table.write(content)
            table.flush()
            files.append(table)
            self.addCleanup(table.close)
        return [table.name for table in files]

    def test_root_launch_drops_refuses_or_runs_only_from_a_non_root_backed_namespace(self):
        container, = self.maps('         0     100000      65536\n')
        self.assertEqual(live.launcher_identity(euid=0, uid_map=container, gid_map=container)[:2],
                         ['--reuid', '65534'])
        nested, = self.maps('         0      65534          1\n')
        self.assertEqual(live.launcher_identity(euid=0, uid_map=nested, gid_map=nested, vouched=True), [])
        with self.assertRaises(ValueError):
            live.launcher_identity(euid=0, uid_map=nested, gid_map=nested, vouched=False)
        host_root, = self.maps('         0          0          1\n')
        with self.assertRaises(ValueError):
            live.launcher_identity(euid=0, uid_map=host_root, gid_map=host_root, vouched=True)
        partial, = self.maps('         0          0       1000\n')
        with self.assertRaises(ValueError):
            live.launcher_identity(euid=0, uid_map=partial, gid_map=partial, vouched=True)
        self.assertEqual(live.launcher_identity(euid=1000, uid_map=host_root, gid_map=host_root), [])

    def test_maps_checks_identity_ranges(self):
        with tempfile.NamedTemporaryFile('w') as table:
            table.write('         0     100000      65536\n')
            table.flush()
            self.assertTrue(live._maps(table.name, 65534))
            self.assertFalse(live._maps(table.name, 65536))
        with tempfile.NamedTemporaryFile('w') as table:
            table.write('0 1000 1\n')
            table.flush()
            self.assertFalse(live._maps(table.name, 65534))


class ConsistencyTests(unittest.TestCase):
    ROOT = Path(__file__).resolve().parents[2]
    TOOL_GATE = Path(__file__).resolve().parent / 'src/worker/live_isolation.rs'
    CUSTOM_GATE = ROOT / 'crates/ace-swarm/src/dht/comparison/worker_live_isolation.rs'

    def test_bootstrap_matches_production_list(self):
        source = (self.ROOT / 'crates/ace-swarm/src/dht.rs').read_text()
        block = re.search(r'const BOOTSTRAP: &\[&str\] = &\[(.*?)\];', source, re.S).group(1)
        self.assertEqual(tuple(re.findall(r'"([^"]+)"', block)), live_block.BOOTSTRAP)

    def test_rust_gates_share_the_denied_prefixes_and_mirror_exactly(self):
        for path in (self.TOOL_GATE, self.CUSTOM_GATE):
            block = re.search(r'DENIED_V4: \[&str; \d+\] = \[(.*?)\];', path.read_text(), re.S).group(1)
            self.assertEqual(tuple(re.findall(r'"([^"]+)"', block)), live_policy.DENIED_V4, path)
        tool = self.TOOL_GATE.read_text()
        custom = self.CUSTOM_GATE.read_text().split('\n', 1)[1]
        self.assertEqual(custom, tool)


if __name__ == '__main__':
    unittest.main()
