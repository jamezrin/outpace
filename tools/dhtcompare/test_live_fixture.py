"""Pure tests of the live-boundary qualification predicates; no namespaces."""
import copy
import unittest
import live_fixture
import live_policy


def owned(**extra):
    counters = dict.fromkeys(live_policy.RULE_COMMENTS, 0)
    case = {'exit_code': 0, 'cleanup_complete': True, 'attested': True, 'timed_out': False,
            'truncated': False, 'leaks': 0, 'servers_alive': 0,
            'boundary': {'denied_destination': 0, 'unsolicited_in': 0, 'admitted_out': 1},
            'inner': {'policy_counters': counters}}
    case.update(extra)
    return case


def passing():
    caps = dict(live_fixture.ZERO_CAPS, NoNewPrivs='1')
    private = {a: 'sent' for a in live_fixture.LEAK_ADDRESSES + ('127.0.0.53',)}
    probe = owned(probe={'thread_a': 'pong:ping-a', 'thread_b': 'pong:ping-b',
                         'unsolicited': 'pong:unsolicited', 'unsolicited_extra': [],
                         'private': private, 'tcp': 'timeout', 'ipv6': 'EADDRNOTAVAIL',
                         'raw': 'EPERM', 'nft_flush': 1, 'link_down': 2, 'status': caps,
                         'unix_connect': 'ENOENT', 'file_write': 'EROFS', 'run_listing': [],
                         'canary_read': 'EACCES'},
                  unix_accepted=0, file_created=False, canary_expected='EACCES')
    probe['boundary'] = {'denied_destination': 5, 'unsolicited_in': 2, 'admitted_out': 3}
    probe['inner']['policy_counters']['out_udp_denied'] = 5
    results = {'probe': probe}
    record = {'terminal': 'target', 'peer_count': 10}
    for role in ('custom', 'mainline', 'n0'):
        results[f'values:{role}'] = owned(record=record, error=None, get_peers=1)
        private_nodes = owned(record=record, error=None, get_peers=2)
        private_nodes['boundary']['denied_destination'] = 5
        private_nodes['inner']['policy_counters']['out_udp_denied'] = 5
        results[f'private_nodes:{role}'] = private_nodes
        results[f'unconfined:{role}'] = {'exit_code': 101 if role == 'custom' else 65,
                                         'record_line': False, 'stdout_bytes': 0}
    consistent = {'denials_agree': True, 'udp_frames_agree': True, 'kernel_drops': False}
    arms = [{'role': r, 'record': record, 'consistency': consistent} for r in ('custom', 'mainline', 'n0')]
    results['block'] = {'report': {'verdict': 'complete', 'arms': arms,
                                   'preflight': {'ipv6_disabled': True},
                                   'postflight': {'ipv6_disabled': True}, 'host_global_addresses': 0},
                        'leaks': 0, 'report_has_target': False, 'report_has_seed': False}
    results['host_change'] = owned(changed=True)
    results['host_change']['boundary'].update(host_address_events=1, admitted_out=8, host_change_denied=12)
    for name, reason in live_fixture.REFUSALS.items():
        results[name] = {'report': {'verdict': 'inconclusive', 'arms': [], 'reason': reason},
                         'files': ['report.json']}
    return results


class EvaluateTests(unittest.TestCase):
    def test_passing_results_have_no_failures(self):
        self.assertEqual(live_fixture.evaluate(passing()), [])

    def test_each_corruption_fails_its_case(self):
        mutations = {
            'probe': [lambda c: c.update(leaks=1),
                      lambda c: c['inner']['policy_counters'].update(out_udp_denied=4),
                      lambda c: c['probe']['status'].update(CapEff='0000000000000400'),
                      lambda c: c['probe'].update(unsolicited_extra=['stranger']),
                      lambda c: c['probe'].update(tcp='sent'),
                      lambda c: c['probe'].update(nft_flush=0),
                      lambda c: c.update(cleanup_complete=False),
                      lambda c: c.update(inner=None),
                      lambda c: c['probe'].update(unix_connect='sent'),
                      lambda c: c['probe'].update(file_write='EACCES'),
                      lambda c: c['probe'].update(run_listing=['systemd']),
                      lambda c: c.update(unix_accepted=1),
                      lambda c: c.update(file_created=True),
                      lambda c: c['probe'].update(canary_read='sent')],
            'values:mainline': [lambda c: c['record'].update(terminal='deadline'),
                                lambda c: c['boundary'].update(denied_destination=1)],
            'private_nodes:n0': [lambda c: c['boundary'].update(denied_destination=0),
                                 lambda c: c['boundary'].update(denied_destination=6)],
            'unconfined:n0': [lambda c: c.update(exit_code=0),
                              lambda c: c.update(stdout_bytes=10)],
            'unconfined:custom': [lambda c: c.update(record_line=True)],
            'block': [lambda c: c['report'].update(verdict='failed'),
                      lambda c: c.update(report_has_target=True),
                      lambda c: c.update(report_has_seed=True),
                      lambda c: c['report']['arms'].reverse(),
                      lambda c: c['report'].update(postflight={'drift': 1}),
                      lambda c: c['report'].update(preflight={'ipv6_disabled': False},
                                                   postflight={'ipv6_disabled': False}),
                      lambda c: c['report'].update(host_global_addresses=1),
                      lambda c: c['report']['arms'][1].update(
                          consistency={'denials_agree': True, 'udp_frames_agree': False, 'kernel_drops': False}),
                      lambda c: c['report']['arms'][2].update(
                          consistency={'denials_agree': True, 'udp_frames_agree': True, 'kernel_drops': True})],
            'block_encrypted': [lambda c: c['report'].update(arms=[{}]),
                                lambda c: c.update(files=['report.json', 'custom.stdout.private'])],
            'host_change': [lambda c: c.update(changed=False),
                            lambda c: c['boundary'].update(host_address_events=0),
                            lambda c: c['boundary'].update(host_change_denied=0, admitted_out=20),
                            lambda c: c['boundary'].update(admitted_out=0, host_change_denied=20)],
            'block_ipv6_enabled': [lambda c: c['report'].update(reason='egress_route')],
            'block_host_address': [lambda c: c['report'].update(verdict='complete')],
        }
        for name, changes in mutations.items():
            for change in changes:
                results = passing()
                if name.startswith('values:'):
                    results[name]['record'] = dict(results[name]['record'])
                results[name] = copy.deepcopy(results[name])
                change(results[name])
                self.assertEqual(live_fixture.evaluate(results), [name], name)

    def test_missing_case_fails(self):
        results = passing()
        del results['block']
        self.assertEqual(live_fixture.evaluate(results), ['block'])


if __name__ == '__main__':
    unittest.main()
