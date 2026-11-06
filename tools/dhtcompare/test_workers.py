"""Fixed offline launch boundary: no live source or registry input."""
import json
from pathlib import Path
import time
from unittest import TestCase, mock
from isolation import Result
from workers import run_worker

class WorkerLaunchTests(TestCase):
    def artifact(self):
        artifact=mock.Mock()
        artifact.path=Path('/fixed/worker')
        return artifact

    def test_denies_nonloopback_unknown_role_and_missing_budget_before_launch(self):
        with mock.patch('workers.run_offline') as launch:
            for role,target,seeds,deadline in [
                ('other','0123456789abcdef0123456789abcdef01234567','127.0.0.1:1',time.monotonic()+40),
                ('mainline','0123456789abcdef0123456789abcdef01234567','192.0.2.1:1',time.monotonic()+40),
                ('n0','bad','127.0.0.1:1',time.monotonic()+40),
                ('n0','0123456789abcdef0123456789abcdef01234567','127.0.0.1:1',time.monotonic()+29),
            ]:
                with self.subTest(role=role,seeds=seeds), self.assertRaises(ValueError):
                    run_worker(role,self.artifact(),target=target,seeds=seeds,deadline=deadline)
        launch.assert_not_called()

    def test_cleans_inherited_prefix_fixed_argv_and_validates_actual_capture(self):
        artifact=self.artifact()
        with mock.patch.dict('os.environ',{'OUTPACE_DHT_COMPARE_EXTRA':'secret','KEEP':'yes'}), mock.patch('workers.run_offline',return_value=Result(0,b'{}\n',b'',3,0,False,True,False)) as launch:
            with self.assertRaisesRegex(ValueError,'invalid_worker_record'):
                run_worker('mainline',artifact,target='0123456789abcdef0123456789abcdef01234567',seeds='127.0.0.1:1',deadline=time.monotonic()+40)
        artifact.verify.assert_called_once()
        args,kwargs=launch.call_args
        self.assertEqual(args[0],['/fixed/worker','--offline-worker','mainline'])
        self.assertEqual(kwargs['timeout'],27.8)
        self.assertEqual(set(k for k in kwargs['env'] if k.startswith('OUTPACE_DHT_COMPARE_')),{'OUTPACE_DHT_COMPARE_MODE','OUTPACE_DHT_COMPARE_ROLE','OUTPACE_DHT_COMPARE_TARGET','OUTPACE_DHT_COMPARE_SEEDS'})
        self.assertEqual(kwargs['env']['KEEP'],'yes')

    def test_custom_uses_only_fixed_exact_ignored_entry(self):
        with mock.patch('workers.run_offline',return_value=Result(0,b'',b'',0,0,False,True,False)) as launch:
            with self.assertRaises(ValueError):
                run_worker('custom',self.artifact(),target='0123456789abcdef0123456789abcdef01234567',seeds='127.0.0.1:1',deadline=time.monotonic()+40)
        self.assertEqual(launch.call_args.args[0],['/fixed/worker','--exact','dht::comparison::worker::custom_offline_worker','--ignored','--nocapture','--test-threads=1'])

    def test_verification_cost_cannot_consume_cleanup_reserve(self):
        artifact=self.artifact()
        with mock.patch('workers.time.monotonic',side_effect=[0,1]), mock.patch('workers.run_offline') as launch:
            with self.assertRaises(ValueError):
                run_worker('mainline',artifact,target='0123456789abcdef0123456789abcdef01234567',seeds='127.0.0.1:1',deadline=30)
        launch.assert_not_called()

    def test_success_preserves_capture_and_shared_validated_record(self):
        vector=json.loads((Path(__file__).parent/'vectors/worker-v1.json').read_text())[0]['record']
        payload=(json.dumps(vector)+'\n').encode()
        capture=Result(0,payload,b'',len(payload),0,False,True,False)
        with mock.patch('workers.run_offline',return_value=capture):
            observation=run_worker('mainline',self.artifact(),target='0123456789abcdef0123456789abcdef01234567',seeds='127.0.0.1:1',deadline=time.monotonic()+40)
        self.assertIs(observation.capture,capture)
        self.assertEqual(observation.record['peer_count'],10)
        self.assertGreaterEqual(observation.elapsed,0)
