"""Bounded offline qualification orchestration; no public network entry."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from isolation import Result
from qualification import Artifact, qualify

class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.artifacts=[]
        for name in ('custom','upstream'):
            path=self.root/name; path.write_bytes(b'offline fixture');path.chmod(0o700)
            self.artifacts.append(Artifact(path,hashlib.sha256(path.read_bytes()).hexdigest()))
    def tearDown(self): self.temp.cleanup()
    def result(self,**changes):
        stdout=b'test dht::comparison::custom_real_udp_target ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n'
        values=dict(exit_code=0,stdout=stdout,stderr=b'',stdout_bytes=len(stdout),stderr_bytes=0,
                    timed_out=False,cleanup_complete=True,truncated=False)
        values.update(changes);return Result(**values)
    def test_fixed_schedule_and_numeric_report(self):
        def actual_case(argv,**kwargs):
            name=argv[2]
            stdout=f"test {name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n".encode()
            return self.result(stdout=stdout,stdout_bytes=len(stdout))
        with mock.patch('qualification.run_offline',side_effect=actual_case) as run:
            report=qualify(*self.artifacts,output=self.root/'run')
        self.assertTrue(report['passed']);self.assertEqual(len(report['cases']),9)
        self.assertEqual(run.call_count,9)
        names=[call.args[0][2] for call in run.call_args_list]
        self.assertEqual(names[:3],['dht::comparison::custom_real_udp_target','mainline_real_udp_target','n0_real_udp_target'])
        for call in run.call_args_list:
            self.assertIn('--ignored',call.args[0]);self.assertIn('--exact',call.args[0])
            self.assertLessEqual(call.kwargs['timeout'],20)
        self.assertEqual((self.root/'run').stat().st_mode&0o777,0o700)
        self.assertEqual(len(list((self.root/'run').glob('*.stdout.private'))),9)
    def test_stale_binary_with_zero_matching_tests_is_not_qualified(self):
        empty=self.result(stdout=b"running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored;\n")
        with mock.patch('qualification.run_offline',return_value=empty) as run:
            report=qualify(*self.artifacts,output=self.root/'run')
        self.assertFalse(report['passed']);self.assertEqual(run.call_count,1)
    def test_mismatched_pin_denies_before_spawn(self):
        self.artifacts[1].path.write_bytes(b'changed')
        with mock.patch('qualification.run_offline') as run:
            with self.assertRaisesRegex(ValueError,'artifact_pin'):
                qualify(*self.artifacts,output=self.root/'run')
        run.assert_not_called()
    def test_uncertain_cleanup_stops_before_next_case(self):
        with mock.patch('qualification.run_offline',return_value=self.result(cleanup_complete=False)) as run:
            report=qualify(*self.artifacts,output=self.root/'run')
        self.assertFalse(report['passed']);self.assertEqual(run.call_count,1)
        self.assertTrue(report['cases'][0]['executed_exact_case'])
        self.assertEqual(report['stop_reason'],'case_failed')
    def test_original_failure_and_truncation_are_not_success(self):
        for result in [self.result(exit_code=7),self.result(truncated=True),self.result(timed_out=True)]:
            with self.subTest(result=result),mock.patch('qualification.run_offline',return_value=result) as run:
                report=qualify(*self.artifacts,output=self.root/('run'+str(id(result))))
            self.assertFalse(report['passed']);self.assertEqual(run.call_count,1)
            self.assertEqual(report['cases'][0]['exit_code'],result.exit_code)
            self.assertTrue(report['cases'][0]['executed_exact_case'])
    def test_external_cancel_block_requires_query_witness_and_signal_exit(self):
        result=self.result(stdout=b"running 1 test\ndht_lookup_query_witness\n",exit_code=-9,cancelled_on_witness=True)
        with mock.patch('qualification.run_offline',return_value=result) as run:
            report=qualify(*self.artifacts,output=self.root/'run',mode='lookup-cancellation')
        self.assertTrue(report['passed']);self.assertEqual(run.call_count,3)
        for call in run.call_args_list:
            self.assertEqual(call.kwargs['cancel_on_stdout'],b'dht_lookup_query_witness\n')
            self.assertEqual(call.kwargs['timeout'],8)
        self.assertTrue(all(case['query_witness'] for case in report['cases']))
    def test_external_cancel_does_not_accept_panic_timeout_or_early_return(self):
        valid=dict(stdout=b"running 1 test\ndht_lookup_query_witness\n",exit_code=-9,cancelled_on_witness=True)
        for changes in [dict(exit_code=101),dict(timed_out=True),dict(cleanup_complete=False),dict(cancelled_on_witness=False),dict(stdout=valid['stdout']+b'dht_lookup_returned_before_cancel\n')]:
            result=self.result(**(valid|changes))
            with self.subTest(changes=changes),mock.patch('qualification.run_offline',return_value=result) as run:
                report=qualify(*self.artifacts,output=self.root/('run'+str(id(result))),mode='lookup-cancellation')
                self.assertFalse(report['passed']);self.assertEqual(run.call_count,1)
    def test_unknown_mode_denies_before_artifact_access_or_spawn(self):
        with mock.patch('qualification.run_offline') as run,mock.patch.object(Artifact,'verify') as verify:
            with self.assertRaisesRegex(ValueError,'qualification_mode'):
                qualify(*self.artifacts,output=self.root/'run',mode='live')
        run.assert_not_called();verify.assert_not_called()
    def test_pin_verification_cannot_extend_launch_deadline(self):
        with mock.patch('qualification.time.monotonic',side_effect=[0,0,179,179,179]),mock.patch('qualification.run_offline') as run:
            report=qualify(*self.artifacts,output=self.root/'run')
        self.assertFalse(report['passed']);self.assertEqual(report['stop_reason'],'deadline')
        run.assert_not_called()
    def test_absolute_deadline_never_extends_for_next_case(self):
        with mock.patch('qualification.time.monotonic',side_effect=[0,179,179,179]),mock.patch('qualification.run_offline') as run:
            report=qualify(*self.artifacts,output=self.root/'run')
        self.assertFalse(report['passed']);self.assertEqual(report['stop_reason'],'deadline')
        run.assert_not_called()

if __name__=='__main__':unittest.main()
