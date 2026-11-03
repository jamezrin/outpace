"""Linux-only offline confinement acceptance; no public DHT requests."""
import json
import os
from unittest import mock
from pathlib import Path
import sys
import unittest
from isolation import run_offline

class IsolationTests(unittest.TestCase):
    def run_python(self, code, timeout=3, cap=65536):
        return run_offline([sys.executable, '-I', '-c', code], timeout=timeout, prefix_cap=cap)

    def test_private_pid_and_network_namespace_with_loopback_udp(self):
        code = '''import os,socket,subprocess,json
assert os.getpid()==1
assert json.loads(subprocess.check_output(['/usr/bin/ip','-j','route']))==[]
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.bind(('127.0.0.1',0));s.settimeout(.5)
s.sendto(b'owned',s.getsockname());assert s.recv(16)==b'owned'
print(json.dumps({'net':os.stat('/proc/self/ns/net').st_ino,'pid':os.getpid()}))'''
        result = self.run_python(code)
        self.assertEqual(result.exit_code, 0, result.stderr)
        self.assertTrue(result.cleanup_complete)
        state = json.loads(result.stdout)
        self.assertEqual(state['pid'], 1)
        self.assertNotEqual(state['net'], Path('/proc/self/ns/net').stat().st_ino)

    def test_parent_reap_and_pipe_eof_after_detached_descendant(self):
        result = self.run_python('import os,time\nif os.fork()==0:\n os.setsid();time.sleep(60)\nelse:\n print("done",flush=True)')
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(result.cleanup_complete)
        self.assertEqual(result.stdout.strip(), b'done')

    def test_deadline_kills_and_reaps_owned_namespace(self):
        with mock.patch('isolation.os.killpg', wraps=os.killpg) as outer_kill:
            result = self.run_python('import time;time.sleep(60)', timeout=.3)
        outer_kill.assert_not_called()
        self.assertTrue(result.timed_out)
        self.assertTrue(result.cleanup_complete)
        self.assertIsNotNone(result.exit_code)

    def test_query_witness_cancels_owned_namespace_before_deadline(self):
        result=run_offline([sys.executable,'-I','-c','import sys,time;sys.stdout.write("query-");sys.stdout.flush();time.sleep(.02);print("witness",flush=True);time.sleep(60)'],timeout=3,cancel_on_stdout=b'query-witness\n')
        self.assertTrue(result.cancelled_on_witness)
        self.assertFalse(result.timed_out)
        self.assertTrue(result.cleanup_complete)
        self.assertNotEqual(result.exit_code,0)
        self.assertIn(b'query-witness',result.stdout)

    def test_stderr_marker_does_not_trigger_witness_cancellation(self):
        result=run_offline([sys.executable,'-I','-c','import sys,time;print("query-witness",file=sys.stderr,flush=True);time.sleep(60)'],timeout=.3,cancel_on_stdout=b'query-witness\n')
        self.assertFalse(result.cancelled_on_witness)
        self.assertTrue(result.timed_out)
        self.assertTrue(result.cleanup_complete)
    def test_invalid_witness_bounds_deny_before_spawn(self):
        with mock.patch('isolation.subprocess.Popen') as spawn:
            for marker in [b'',b'x'*257,'query-witness',True]:
                with self.subTest(marker=marker),self.assertRaises(ValueError):
                    run_offline(['/usr/bin/true'],timeout=1,cancel_on_stdout=marker)
        spawn.assert_not_called()

    def test_both_pipes_are_drained_after_prefix_retention(self):
        result = self.run_python('import os\nfor _ in range(64):\n os.write(1,b"x"*4096);os.write(2,b"y"*4096)', cap=1024)
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(result.cleanup_complete)
        self.assertEqual(len(result.stdout), 1024)
        self.assertEqual(len(result.stderr), 1024)
        self.assertEqual(result.stdout_bytes, 262144)
        self.assertEqual(result.stderr_bytes, 262144)
        self.assertTrue(result.truncated)

    def test_original_nonzero_exit_is_preserved(self):
        result = self.run_python('raise SystemExit(7)')
        self.assertEqual(result.exit_code, 7)
        self.assertTrue(result.cleanup_complete)

    def test_timeout_reaps_namespace_with_detached_closed_pipes(self):
        code = 'import os,time\nif os.fork()==0:\n os.setsid();os.close(1);os.close(2);time.sleep(60)\nelse:\n print("ready",flush=True);time.sleep(60)'
        with mock.patch('isolation.os.killpg', wraps=os.killpg) as fallback:
            result = self.run_python(code, timeout=.5)
        self.assertIn(b'ready', result.stdout)
        fallback.assert_not_called()
        self.assertTrue(result.timed_out)
        self.assertTrue(result.cleanup_complete)

    def test_namespace_launcher_denial_preserves_failure(self):
        import subprocess
        original = subprocess.Popen
        def deny(command, **kwargs):
            return original([command[0], '--invalid-isolation-option'], **kwargs)
        with mock.patch('isolation.subprocess.Popen', side_effect=deny):
            result = self.run_python('raise SystemExit(0)')
        self.assertNotEqual(result.exit_code, 0)
        self.assertTrue(result.cleanup_complete)

    def test_exception_after_spawn_reaps_and_closes_pipes(self):
        import selectors
        import subprocess
        selector = selectors.DefaultSelector()
        original = subprocess.Popen
        children = []
        def launch(*args, **kwargs):
            proc = original(*args, **kwargs); children.append(proc); return proc
        with mock.patch('isolation.selectors.DefaultSelector', return_value=selector), mock.patch.object(selector, 'register', side_effect=KeyboardInterrupt), mock.patch('isolation.subprocess.Popen', side_effect=launch):
            with self.assertRaises(KeyboardInterrupt):
                self.run_python('import time;time.sleep(60)')
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].poll())
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)

    def test_invalid_bounds_and_relative_programs_do_not_spawn(self):
        for timeout,cap in [(0,100), (29,100), (1,0), (1,65537)]:
            with self.assertRaises(ValueError):
                run_offline(['/usr/bin/true'],timeout=timeout,prefix_cap=cap)
        with self.assertRaises(ValueError):
            run_offline(['true'],timeout=1)

if __name__ == '__main__':
    unittest.main()
