"""Exercise the real installer orchestration with no package/network operations."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).with_name('install_media_tools.py')

class InstallMediaTests(unittest.TestCase):
    def module(self):
        spec=importlib.util.spec_from_file_location('install_media_tools',SCRIPT)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module

    def exercise(self, fail_at=None):
        module=self.module();calls=[];source_paths=[]
        with tempfile.TemporaryDirectory() as directory:
            def execute(argv, **kwargs):
                calls.append((argv,kwargs))
                if '/usr/bin/apt-get' in argv:
                    options=[argv[i+1] for i,x in enumerate(argv[:-1]) if x=='-o']
                    source=Path(next(x.split('=',1)[1] for x in options if x.startswith('Dir::Etc::sourcelist=')))
                    source_paths.append(source)
                    text=source.read_text()
                    self.assertIn('https://archive.ubuntu.com/ubuntu noble main universe',text)
                    self.assertIn('https://security.ubuntu.com/ubuntu noble-security main universe',text)
                    self.assertIn('signed-by=/usr/share/keyrings/ubuntu-archive-keyring.gpg',text)
                    self.assertNotIn('azure.archive',text)
                    parts=Path(next(x.split('=',1)[1] for x in options if x.startswith('Dir::Etc::sourceparts=')))
                    self.assertFalse(parts.exists())
                    self.assertIn('Acquire::Retries=0',options)
                    self.assertIn('APT::Update::Error-Mode=any',options)
                    self.assertIn('Acquire::https::Timeout=15',options)
                    self.assertEqual(argv[:5],['sudo','-n','/usr/bin/timeout','--signal=TERM','--kill-after=10s'])
                    self.assertNotIn('--allow-unauthenticated',argv)
                self.assertTrue(kwargs['check'])
                if len(calls)==fail_at:raise subprocess.CalledProcessError(124,argv)
                return subprocess.CompletedProcess(argv,0)
            with mock.patch.dict(os.environ,{'GITHUB_ACTIONS':'true','RUNNER_OS':'Linux','RUNNER_TEMP':directory}), mock.patch.object(module,'supported_runner',return_value=None), mock.patch.object(module.subprocess,'run',side_effect=execute):
                if fail_at:
                    with self.assertRaises(subprocess.CalledProcessError):module.install()
                else:module.install()
            self.assertTrue(source_paths)
            self.assertTrue(all(not p.exists() for p in source_paths),'temporary sources survived exit')
        return calls

    def test_success_is_bounded_signed_install_then_both_tool_probes(self):
        calls=self.exercise();self.assertEqual(len(calls),4)
        self.assertIn('90s',calls[0][0]);self.assertIn('update',calls[0][0])
        self.assertIn('240s',calls[1][0]);self.assertEqual(calls[1][0][-4:],['install','-y','--no-install-recommends','ffmpeg'])
        self.assertEqual(calls[2][0],['/usr/bin/ffmpeg','-hide_banner','-version'])
        self.assertEqual(calls[3][0],['/usr/bin/ffprobe','-hide_banner','-version'])
        self.assertLessEqual(sum(x[1]['timeout'] for x in calls),390)

    def test_each_failure_stops_without_retry_or_successful_skip(self):
        for phase in range(1,5):
            with self.subTest(phase=phase):self.assertEqual(len(self.exercise(phase)),phase)

    def test_wrong_context_denies_before_any_command(self):
        module=self.module()
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(module.subprocess,'run') as command:
            with self.assertRaises(RuntimeError):module.install()
        command.assert_not_called()

    def test_platform_gate_accepts_only_pinned_ubuntu_release(self):
        module=self.module()
        for content,allowed in [('ID=ubuntu\nVERSION_ID="24.04"\n',True),('ID=debian\nVERSION_ID="12"\n',False),('ID=ubuntu\nVERSION_ID="22.04"\n',False)]:
            with mock.patch.object(Path,'read_text',return_value=content),mock.patch.object(Path,'is_file',return_value=True),mock.patch.object(module.platform,'machine',return_value='x86_64'):
                if allowed:module.supported_runner()
                else:
                    with self.assertRaises(RuntimeError):module.supported_runner()

    def test_real_cli_denies_outside_ci(self):
        env=os.environ.copy();env.pop('GITHUB_ACTIONS',None)
        result=subprocess.run([sys.executable,'-I',str(SCRIPT)],env=env,capture_output=True,timeout=3)
        self.assertEqual(result.returncode,2)
        self.assertIn(b'CI media prerequisite setup failed',result.stderr)

    def test_platform_gate_rejects_missing_keyring_or_other_architecture(self):
        module=self.module()
        for exists,architecture in [(False,'x86_64'),(True,'aarch64')]:
            with mock.patch.object(Path,'read_text',return_value='ID=ubuntu\nVERSION_ID="24.04"\n'),mock.patch.object(Path,'is_file',return_value=exists),mock.patch.object(module.platform,'machine',return_value=architecture):
                with self.assertRaises(RuntimeError):module.supported_runner()
