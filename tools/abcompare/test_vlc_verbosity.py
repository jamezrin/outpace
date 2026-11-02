import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import vlc_compare as vlc


class VerbosityTests(unittest.TestCase):
    def args(self, level=0):
        return SimpleNamespace(vlc='unused', engine_url='http://localhost:1',
                               outpace_url='http://localhost:2', client_warp='off',
                               server_warp='unknown', network_caching=1000,
                               seconds=.2, poll_seconds=.05, diagnostic_verbosity=level)

    def test_invalid_or_unbounded_levels_reject_before_side_effects(self):
        for level in (-1, 3, True, 1.5, '2', None):
            with self.subTest(level=level), patch.object(vlc.subprocess, 'Popen') as launch, \
                    patch.object(vlc.tempfile, 'mkdtemp') as directory:
                with self.assertRaises(ValueError):
                    vlc.compare(self.args(level), '0123456789abcdef0123456789abcdef01234567', diagnostics=True)
                launch.assert_not_called()
                directory.assert_not_called()

    def test_nonzero_level_requires_bounded_diagnostics(self):
        with patch.object(vlc.subprocess, 'Popen') as launch, \
                patch.object(vlc.tempfile, 'mkdtemp') as directory:
            with self.assertRaises(ValueError):
                vlc.compare(self.args(2), '0123456789abcdef0123456789abcdef01234567')
            launch.assert_not_called()
            directory.assert_not_called()

    def test_probe_unchanged_client_level_recorded_and_owned(self):
        for level in (0, 1, 2):
            with self.subTest(level=level), tempfile.TemporaryDirectory() as directory:
                executable = Path(directory) / 'fake-vlc'
                executable.write_text(f'#!{sys.executable}\nimport sys,time\n'
                                      'if "--version" in sys.argv:\n print("version"); sys.exit(0)\n'
                                      'time.sleep(10)\n')
                executable.chmod(0o700)
                args = self.args(level)
                args.vlc = str(executable)
                real_launch = subprocess.Popen
                calls, children, environments = [], [], []
                def launch(argv, **kwargs):
                    calls.append(argv)
                    environments.append(dict(kwargs["env"]))
                    if len(calls) == 3:
                        raise OSError('controlled second launch failure')
                    child = real_launch(argv, **kwargs)
                    children.append(child)
                    return child
                with patch.dict(vlc.os.environ, {'VLC_VERBOSE': '0', 'OUTPACE_TEST_PRESERVED': 'yes'}), \
                        patch.object(vlc.subprocess, 'Popen', side_effect=launch), \
                        patch.object(vlc.tempfile, 'mkdtemp', return_value=directory), \
                        redirect_stdout(io.StringIO()):
                    self.assertEqual(vlc.compare(args, '0123456789abcdef0123456789abcdef01234567', diagnostics=True), 2)
                self.assertEqual(calls[0], [str(executable), '--version'])
                self.assertEqual(environments[0]['VLC_VERBOSE'], '0')
                for environment in environments[1:]:
                    self.assertEqual(environment['OUTPACE_TEST_PRESERVED'], 'yes')
                    if level:
                        self.assertFalse(
                            'VLC_VERBOSE' in environment,
                            'explicit playback verbosity must override inherited setting')
                    else:
                        self.assertEqual(environment['VLC_VERBOSE'], '0')
                self.assertEqual(len(children), 2)
                self.assertTrue(all(child.poll() is not None for child in children))
                for argv in calls[1:]:
                    self.assertEqual([v for v in argv if v.startswith('--verbose=')],
                                     [f'--verbose={level}'] if level else [])
                config = json.loads((Path(directory) / 'config.json').read_text())
                self.assertEqual(config['diagnostics']['verbosity'], level)
