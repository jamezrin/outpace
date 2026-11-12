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

    codename='resolute'

    def exercise(self, fail_at=(), fails=False):
        """Run install() with mocked commands; calls numbered in fail_at raise."""
        module=self.module();calls=[];sources=[];source_paths=[]
        with tempfile.TemporaryDirectory() as directory:
            def execute(argv, **kwargs):
                calls.append((argv,kwargs))
                if '/usr/bin/apt-get' in argv:
                    options=[argv[i+1] for i,x in enumerate(argv[:-1]) if x=='-o']
                    source=Path(next(x.split('=',1)[1] for x in options if x.startswith('Dir::Etc::sourcelist=')))
                    source_paths.append(source)
                    text=source.read_text();sources.append(text)
                    for line in text.splitlines():
                        self.assertTrue(line.startswith('deb [signed-by=/usr/share/keyrings/ubuntu-archive-keyring.gpg] '),line)
                    parts=Path(next(x.split('=',1)[1] for x in options if x.startswith('Dir::Etc::sourceparts=')))
                    self.assertFalse(parts.exists())
                    self.assertIn('Acquire::Retries=0',options)
                    self.assertIn('APT::Update::Error-Mode=any',options)
                    self.assertIn('Acquire::https::Timeout=15',options)
                    self.assertEqual(argv[:5],['sudo','-n','/usr/bin/timeout','--signal=TERM','--kill-after=10s'])
                    self.assertNotIn('--allow-unauthenticated',argv)
                self.assertTrue(kwargs['check'])
                if len(calls) in fail_at:raise subprocess.CalledProcessError(124,argv)
                return subprocess.CompletedProcess(argv,0)
            with mock.patch.dict(os.environ,{'GITHUB_ACTIONS':'true','RUNNER_OS':'Linux','RUNNER_TEMP':directory}), mock.patch.object(module,'supported_runner',return_value=self.codename), mock.patch.object(module.subprocess,'run',side_effect=execute):
                if fails:
                    with self.assertRaises(subprocess.CalledProcessError):module.install()
                else:module.install()
            self.assertTrue(source_paths)
            self.assertTrue(all(not p.exists() for p in source_paths),'temporary sources survived exit')
        return calls,sources

    def assert_canonical(self,text):
        self.assertIn(f'https://archive.ubuntu.com/ubuntu {self.codename} main universe',text)
        self.assertIn(f'https://security.ubuntu.com/ubuntu {self.codename}-security main universe',text)
        self.assertNotIn('kernel.org',text)

    def assert_fallback(self,text):
        for pocket in (self.codename,f'{self.codename}-updates',f'{self.codename}-security'):
            self.assertIn(f'https://mirrors.edge.kernel.org/ubuntu {pocket} main universe',text)
        self.assertNotIn('ubuntu.com',text,'fallback must not depend on *.ubuntu.com DNS')

    def test_success_uses_canonical_only_then_both_tool_probes(self):
        calls,sources=self.exercise();self.assertEqual(len(calls),4)
        self.assertIn('55s',calls[0][0]);self.assertEqual(calls[0][0][-1],'update')
        self.assertIn('200s',calls[1][0]);self.assertEqual(calls[1][0][-4:],['install','-y','--no-install-recommends','ffmpeg'])
        self.assertEqual(calls[2][0],['/usr/bin/ffmpeg','-hide_banner','-version'])
        self.assertEqual(calls[3][0],['/usr/bin/ffprobe','-hide_banner','-version'])
        self.assertEqual(len(sources),2);self.assert_canonical(sources[0]);self.assertEqual(sources[0],sources[1])

    def test_canonical_index_failure_falls_back_once(self):
        calls,sources=self.exercise(fail_at=(1,))
        self.assertEqual(len(calls),5)
        self.assertEqual([c[0][-1] for c in calls[:2]],['update','update'])
        self.assertEqual(calls[2][0][-1],'ffmpeg')
        self.assert_canonical(sources[0]);self.assert_fallback(sources[1])
        self.assertEqual(sources[1],sources[2],'install must use the index that succeeded')

    def test_both_index_failures_fail_without_install(self):
        calls,_=self.exercise(fail_at=(1,2),fails=True)
        self.assertEqual(len(calls),2)
        self.assertTrue(all(c[0][-1]=='update' for c in calls))

    def test_install_or_probe_failure_never_falls_back(self):
        for phase in range(2,5):
            with self.subTest(phase=phase):
                calls,sources=self.exercise(fail_at=(phase,),fails=True)
                self.assertEqual(len(calls),phase)
                self.assertTrue(all('kernel.org' not in t for t in sources))

    def test_worst_case_budget_fits_step_timeout(self):
        calls,_=self.exercise(fail_at=(1,))
        self.assertLessEqual(sum(x[1]['timeout'] for x in calls),390)

    def test_wrong_context_denies_before_any_command(self):
        module=self.module()
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(module.subprocess,'run') as command:
            with self.assertRaises(RuntimeError):module.install()
        command.assert_not_called()

    def test_platform_gate_accepts_only_pinned_ubuntu_releases(self):
        module=self.module()
        for content,codename in [('ID=ubuntu\nVERSION_ID="24.04"\n','noble'),('ID=ubuntu\nVERSION_ID="26.04"\n','resolute'),('ID=debian\nVERSION_ID="12"\n',None),('ID=ubuntu\nVERSION_ID="22.04"\n',None),('ID=ubuntu\nVERSION_ID="25.10"\n',None)]:
            with mock.patch.object(Path,'read_text',return_value=content),mock.patch.object(Path,'is_file',return_value=True),mock.patch.object(module.platform,'machine',return_value='x86_64'):
                if codename:self.assertEqual(module.supported_runner(),codename)
                else:
                    with self.assertRaises(RuntimeError):module.supported_runner()

    def test_real_cli_denies_outside_ci(self):
        env=os.environ.copy();env.pop('GITHUB_ACTIONS',None)
        result=subprocess.run([sys.executable,'-I',str(SCRIPT)],env=env,capture_output=True,timeout=3)
        self.assertEqual(result.returncode,2)
        self.assertIn(b'CI media prerequisite setup failed',result.stderr)

    def test_mirrors_follow_the_runner_codename(self):
        for codename in ('noble','resolute'):
            for _,lines in self.module().mirrors(codename):
                self.assertEqual([l.split()[1] for l in lines],[codename,f'{codename}-updates',f'{codename}-security'])

    def test_platform_gate_rejects_missing_keyring_or_other_architecture(self):
        module=self.module()
        for exists,architecture in [(False,'x86_64'),(True,'aarch64')]:
            with mock.patch.object(Path,'read_text',return_value='ID=ubuntu\nVERSION_ID="24.04"\n'),mock.patch.object(Path,'is_file',return_value=exists),mock.patch.object(module.platform,'machine',return_value=architecture):
                with self.assertRaises(RuntimeError):module.supported_runner()
