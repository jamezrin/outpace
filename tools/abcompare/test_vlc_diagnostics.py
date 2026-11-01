import base64
import json
import subprocess
import sys
import time
import unittest

import vlc_diagnostics as diag


class CaptureTests(unittest.TestCase):
    def test_both_bursting_pipes_drain_after_cap(self):
        collector = diag.Collector(time.monotonic())
        process = subprocess.Popen([sys.executable, '-c',
            'import os,threading,resource; resource.setrlimit(resource.RLIMIT_FSIZE,(1024,1024)); '
            't=threading.Thread(target=lambda: [os.write(2,b"e"*8192) for _ in range(160)]); '
            't.start(); [os.write(1,b"x\\n"*4096) for _ in range(160)]; t.join()'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            collector.attach('engine', process)
            self.assertEqual(process.wait(timeout=10), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            collector.finish()
        self.assertFalse(collector.thread.is_alive())
        meta = collector.snapshot()['engine']
        self.assertTrue(meta['retention_exhausted'])
        self.assertEqual(meta['eof_streams'], 2)
        self.assertLessEqual(len(collector.data['engine']), 1048576)
        self.assertGreater(meta['bytes_read'], 2000000)
        self.assertTrue(process.stdout.closed and process.stderr.closed)

    def test_receipt_prefix_eof_invalid_bytes_and_cleanup_phase(self):
        capture = diag.Capture('engine', 'stderr', 100)
        capture.feed(b'\xff\r', 101, None)
        capture.feed(b'\n' + b'x'*10000, 102, 101.5)
        capture.eof(103, 101.5)
        rows = [json.loads(line) for line in capture.data.splitlines()]
        self.assertEqual(base64.b64decode(rows[0]['raw_base64']), b'\xff\r\n')
        self.assertEqual(rows[0]['first_receipt_seconds'], 1)
        self.assertEqual(rows[0]['last_receipt_seconds'], 2)
        self.assertEqual(rows[0]['phase'], 'cleanup')
        self.assertFalse(rows[1]['complete_line'])
        self.assertEqual(rows[1]['omitted_bytes'], 5904)
        self.assertEqual(len(base64.b64decode(rows[1]['raw_base64'])), 4096)

    def test_correlation_unknown_reset_and_flat_intervals(self):
        correlation = diag.Correlation()
        def row(n, state='playing'):
            return {'state': state, 'stats': dict.fromkeys(diag.SELECTED, n)}
        correlation.sample(1, row(5))
        correlation.sample(2, row(5, 'paused'))
        self.assertEqual(correlation.records[0]['activity'], 'input_flat_decode_flat')
        correlation.sample(3, row(1))
        self.assertEqual(correlation.records[-1]['activity'], 'unknown')
        self.assertEqual(correlation.records[-1]['deltas']['readbytes'], 'reset')

    def test_attach_failure_closes_both_pipes_and_joins(self):
        from unittest.mock import patch
        collector = diag.Collector(time.monotonic())
        child = subprocess.Popen([sys.executable, '-c', 'pass'], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        try:
            with patch.object(collector.selector, 'register', side_effect=OSError('private')):
                with self.assertRaises(OSError):
                    collector.attach('engine', child)
        finally:
            child.wait(timeout=5)
            collector.finish()
        self.assertTrue(child.stdout.closed and child.stderr.closed)
        self.assertTrue(collector.failed.is_set())
        self.assertFalse(collector.thread.is_alive())

    def test_held_pipe_is_forced_closed_without_waiting_for_writer(self):
        import os
        from types import SimpleNamespace
        read1, write1 = os.pipe()
        read2, write2 = os.pipe()
        collector = diag.Collector(time.monotonic())
        try:
            collector.attach('engine', SimpleNamespace(stdout=os.fdopen(read1, 'rb'),
                                                       stderr=os.fdopen(read2, 'rb')))
            started = time.monotonic()
            collector.finish()
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertTrue(collector.snapshot()['engine']['forced_close'])
            self.assertFalse(collector.thread.is_alive())
        finally:
            os.close(write1)
            os.close(write2)

    def test_selector_and_read_failures_exit_and_close(self):
        from unittest.mock import patch
        import os
        for target in ('select', 'read'):
            collector = diag.Collector(time.monotonic())
            child = subprocess.Popen([sys.executable, '-c', 'import os; os.write(1,b"hello")'],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                with (patch.object(collector.selector, 'select', side_effect=OSError('private'))
                      if target == 'select' else patch.object(diag.os, 'read', side_effect=OSError('private'))):
                    collector.attach('engine', child)
                    self.assertTrue(collector.failed.wait(2))
            finally:
                child.wait(timeout=5)
                collector.finish()
            self.assertTrue(child.stdout.closed and child.stderr.closed)

    def test_saturation_and_transition_metadata_bound(self):
        capture = diag.Capture('engine', 'stdout', 0)
        capture.budget.bytes_read = diag.MAX_COUNT
        capture.feed(b'x', 1, None)
        self.assertEqual(capture.budget.bytes_read, diag.MAX_COUNT)
        self.assertTrue(capture.budget.counts_saturated)
        correlation = diag.Correlation()
        for n in range(1000):
            correlation.sample(n, {'state': 'private-secret', 'stats': dict.fromkeys(diag.SELECTED, n)})
        self.assertLessEqual(len(json.dumps(correlation.report()).encode()), 29000)
        self.assertTrue(correlation.report()['truncated'])
        self.assertNotIn('private-secret', json.dumps(correlation.report()))

    def test_advancing_input_flat_decode_and_invalid_counters(self):
        correlation = diag.Correlation()
        baseline = dict.fromkeys(diag.SELECTED, 10)
        correlation.sample(1, {'state': 'playing', 'stats': baseline})
        correlation.sample(2, {'state': 'paused', 'stats': dict(baseline, readbytes=11)})
        self.assertEqual(correlation.records[-1]['activity'], 'input_advancing_decode_flat')
        correlation.sample(3, {'stats': dict(baseline, readbytes=True)})
        self.assertEqual(correlation.records[-1]['deltas']['readbytes'], 'unknown')
        self.assertEqual(correlation.records[-1]['activity'], 'unknown')

    def test_encoded_byte_cap_and_stream_sequence(self):
        capture = diag.Capture('engine', 'stdout', 0)
        for n in range(1000):
            capture.feed(b'x'*4095+b'\n', n+1, None)
        self.assertTrue(capture.budget.retention_exhausted)
        self.assertLessEqual(len(capture.data), 1048576)
        rows = [json.loads(line) for line in capture.data.splitlines()]
        self.assertEqual([row['sequence'] for row in rows], list(range(1, len(rows)+1)))
        self.assertTrue(all(len(line)+1 <= 16384 for line in capture.data.splitlines()))
        self.assertEqual(capture.budget.bytes_read, 4096000)


class CleanupFaultTests(unittest.TestCase):
    def test_unregister_and_selector_close_faults_do_not_skip_other_pipes(self):
        import threading
        from unittest.mock import Mock
        for fault in ('unregister', 'selector_close'):
            with self.subTest(fault=fault):
                collector = diag.Collector.__new__(diag.Collector)
                collector.lock = threading.Lock()
                collector.stop = threading.Event(); collector.stop.set()
                collector.failed = threading.Event()
                collector.selector = Mock()
                pipes = [Mock(), Mock()]
                collector.pipes = {p: diag.Capture('engine', 'stdout', 0) for p in pipes}
                if fault == 'unregister':
                    collector.selector.unregister.side_effect = [KeyError('private'), None]
                else:
                    collector.selector.close.side_effect = OSError('private')
                collector.thread = threading.Thread(target=collector._run)
                collector.thread.start(); collector.thread.join(1)
                self.assertFalse(collector.thread.is_alive())
                self.assertTrue(collector.failed.is_set())
                for pipe in pipes:pipe.close.assert_called_once()
                collector.selector.close.assert_called_once()
                self.assertFalse(collector.pipes)
