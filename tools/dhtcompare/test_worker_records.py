"""Pure wire/process contract; no network or executable launches."""
import json
import unittest
from pathlib import Path
from dataclasses import replace
from isolation import Result
from worker_records import validate_record, extract_record

def row():
    return dict(schema_version=1,candidate='mainline',terminal='target',peer_count=10,
                first_peer_seconds=0.01,elapsed_seconds=0.02,batch_sizes=[10],batches_observed=1,
                batch_history_truncated=False,target_overshoot=2,nodes_queried=None,
                valid_responses=None,diagnostics_unavailable_reason='library_not_exposed')
def wire(value):return json.dumps(value,separators=(',',':')).encode()+b'\n'
class Records(unittest.TestCase):
    def test_shared_versioned_vectors(self):
        vectors=json.loads((Path(__file__).parent/'vectors'/'worker-v1.json').read_bytes())
        for case in vectors:
            with self.subTest(name=case['name']):self.assertEqual(validate_record(wire(case['record']),case['role']),case['record'])
    def test_valid_numeric_record_and_empty_deadline(self):
        self.assertEqual(validate_record(wire(row()),'mainline'),row())
        empty=dict(row(),terminal='deadline',peer_count=0,first_peer_seconds=None,
                   batch_sizes=[],batches_observed=0,target_overshoot=0)
        self.assertEqual(validate_record(wire(empty),'mainline'),empty)
    def test_schema_numbers_consistency_and_redaction(self):
        changes=[dict(schema_version=True),dict(candidate='n0'),dict(terminal='unknown'),dict(peer_count=True),
                 dict(peer_count=1025),dict(first_peer_seconds=None),dict(first_peer_seconds=1),
                 dict(elapsed_seconds=float('nan')),dict(elapsed_seconds=float('inf')),
                 dict(batch_sizes=[True]),dict(batch_sizes=[0]),dict(batch_sizes=[-1]),dict(batch_sizes=[2**64]),
                 dict(batches_observed=65),dict(batch_history_truncated=True),dict(target_overshoot=0),
                 dict(nodes_queried=False),dict(diagnostics_unavailable_reason='sensitive-value')]
        for change in changes:
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'invalid_worker_record'):
                validate_record(wire(dict(row(),**change)),'mainline')
        for payload in (b'{}\n',b'[]\n',b'{"schema_version":1,"schema_version":1}\n',b'\xff\n',
                        wire(row())+wire(row()),b'['*2000+b']'*2000+b'\n',b'x'*8194):
            with self.subTest(payload_size=len(payload)),self.assertRaisesRegex(ValueError,'invalid_worker_record'):
                validate_record(payload,'mainline')
        unknown=dict(row(),unknown='sensitive-value')
        with self.assertRaisesRegex(ValueError,'invalid_worker_record'):validate_record(wire(unknown),'mainline')
    def test_stream_bounds_and_cancellation_fail_closed(self):
        custom=dict(row(),candidate='custom',nodes_queried=1,valid_responses=1,diagnostics_unavailable_reason=None)
        name='dht::comparison::custom_offline_worker'
        out=b'DHT_COMPARE_RECORD '+wire(custom)+f'test {name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n'.encode()
        result=Result(0,out,b'',len(out),0,False,True,False)
        for invalid in (replace(result,stderr=b'x'*65537,stderr_bytes=65537),
                        replace(result,stdout=out+b'x'*65536,stdout_bytes=len(out)+65536),
                        replace(result,cancelled_on_witness=True)):
            with self.subTest(bytes=invalid.stdout_bytes),self.assertRaisesRegex(ValueError,'invalid_worker_record'):
                extract_record('custom',invalid,exact_custom_test=name)
    def test_process_and_custom_framing_are_mandatory(self):
        data=wire(row());result=Result(0,data,b'',len(data),0,False,True,False)
        self.assertEqual(extract_record('mainline',result),row())
        for invalid in (replace(result,exit_code=1),replace(result,timed_out=True),
                        replace(result,cleanup_complete=False),replace(result,truncated=True)):
            with self.assertRaisesRegex(ValueError,'invalid_worker_record'):extract_record('mainline',invalid)
        custom=dict(row(),candidate='custom',nodes_queried=1,valid_responses=1,diagnostics_unavailable_reason=None)
        name='dht::comparison::custom_offline_worker'
        out=b'DHT_COMPARE_RECORD '+wire(custom)+f'test {name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n'.encode()
        result=Result(0,out,b'',len(out),0,False,True,False)
        self.assertEqual(extract_record('custom',result,exact_custom_test=name),custom)
        serial=f'test {name} ... \n'.encode()+b'DHT_COMPARE_RECORD '+wire(custom)+b'ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n'
        self.assertEqual(extract_record('custom',Result(0,serial,b'',len(serial),0,False,True,False),exact_custom_test=name),custom)
        for out in (b'running 0 tests\n',out+b'DHT_COMPARE_RECORD '+wire(custom)):
            result=Result(0,out,b'',len(out),0,False,True,False)
            with self.assertRaisesRegex(ValueError,'invalid_worker_record'):extract_record('custom',result,exact_custom_test=name)
if __name__=='__main__':unittest.main()
