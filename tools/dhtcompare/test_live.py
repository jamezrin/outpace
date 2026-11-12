"""Real-namespace live-boundary tests needing no candidate binaries. Linux only.

Candidate cases need pinned builds; run them through live_qualification.py.
"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import live_fixture

FIXTURE = Path(__file__).resolve().with_name('live_fixture.py')


def run_cases(*cases):
    with tempfile.TemporaryDirectory(dir=FIXTURE.parent) as scratch:
        config = json.dumps({'cases': list(cases), 'scratch': scratch, 'canary': None,
                             'vouched_nested_root': True})
        completed = subprocess.run(
            ['/usr/bin/unshare', '--user', '--map-root-user', '--net', '--pid', '--fork',
             '--kill-child=KILL', '--mount-proc', sys.executable, '-I', str(FIXTURE), config],
            capture_output=True, timeout=60)
    if completed.returncode != 0:
        raise AssertionError(completed.stderr.decode(errors='replace')[-2000:])
    return json.loads(completed.stdout)


class LiveBoundaryTests(unittest.TestCase):
    def test_probe_destination_server_and_privilege_matrix(self):
        results = run_cases('probe')
        self.assertEqual([n for n in live_fixture.evaluate(results) if n == 'probe'], [])
        probe = results['probe']
        self.assertTrue(all(v == 'sent' for v in probe['probe']['private'].values()))
        self.assertEqual(probe['leaks'], 0)
        self.assertEqual(probe['inner']['policy_counters']['in_udp_established'], 3)
        self.assertGreater(probe['inner']['samples'], 0)

    def test_fixture_refuses_a_non_isolated_namespace(self):
        completed = subprocess.run([sys.executable, '-I', str(FIXTURE), '{"cases":[]}'],
                                   capture_output=True, timeout=10)
        self.assertNotEqual(completed.returncode, 0)


if __name__ == '__main__':
    unittest.main()
