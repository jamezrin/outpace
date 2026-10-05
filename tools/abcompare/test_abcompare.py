import csv
import contextlib
import io
import http.server
import json
import stat
from pathlib import Path
import subprocess
import socket
import sys
import tempfile
import threading
import time
import unittest

import ab_analyze as analyze
import ab_capture as capture
import run as runner


def packet(cc, pcr=None, rai=False, disc=False, tag=1, pid=256):
    flags = (0x40 if rai else 0) | (0x80 if disc else 0)
    payload = bytes([tag]) * 184
    afc = 1
    if pcr is not None:
        flags |= 0x10
        ticks = round(pcr * 27000000)
        base, ext = divmod(ticks, 300)
        encoded = bytes([base >> 25, (base >> 17) & 255,
                         (base >> 9) & 255, (base >> 1) & 255,
                         ((base & 1) << 7) | 0x7e | (ext >> 8), ext & 255])
        payload = bytes([7, flags]) + encoded + bytes([tag]) * 176
        afc = 3
    elif flags:
        payload = bytes([1, flags]) + bytes([tag]) * 182
        afc = 3
    return bytes([0x47, (pid >> 8) & 31, pid & 255, (afc << 4) | cc]) + payload


def psi(pid, section):
    payload = b'\x00' + section
    return bytes([0x47, 0x40 | (pid >> 8), pid & 255, 0x10]) + payload.ljust(184, b'\xff')


class AnalyzerTests(unittest.TestCase):
    def test_video_rai_uses_complete_packet_arrival_and_skips_tables(self):
        pat = psi(0, bytes.fromhex('00 b0 0d 00 01 c1 00 00 00 01 e0 64 00 00 00 00'))
        pmt = psi(100, bytes.fromhex('02 b0 12 00 01 c1 00 00 e1 00 f0 00 1b e1 00 f0 00 00 00 00 00'))
        video = packet(0, 1, rai=True)
        tables = psi(17, b'\x42' + b'\x00' * 15)
        data = pat + pmt + video
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)
            path.joinpath('x.ts').write_bytes(data)
            path.joinpath('x.arrivals.csv').write_text('t,nbytes,offset\n1,470,0\n2,94,470\n')
            r = analyze.analyze(path, 'x', 4)
            self.assertEqual(r['first_key_t'], 2)
            self.assertEqual(r['ts']['stypes'], {256: 0x1b})
            identity = analyze.packet_identity(analyze.parse_ts(data), analyze.parse_ts(tables + data))
            self.assertEqual(identity['considered_packets'], 1)
            self.assertEqual(identity['matched_fraction'], 1)

    def test_exact_duplicates_report_media_separately_from_psi(self):
        pat = psi(0, bytes.fromhex('00 b0 0d 00 01 c1 00 00 00 01 e0 64 00 00 00 00'))
        video = packet(0)
        r = analyze.parse_ts(pat + pat + video + video)
        self.assertEqual(r['exact_duplicate_packets_by_pid'], {0: 1, 256: 1})
        self.assertEqual(r['exact_duplicate_media_packets'], 1)

    def test_sync_loss_and_bad_adaptation(self):
        first = packet(0)
        r = analyze.parse_ts(b'junk' + first + packet(1) + packet(2) + b'broken' + packet(3))
        self.assertEqual(r['lead'], 4)
        self.assertEqual(r['sync_loss'], 1)
        self.assertEqual(len(r['pkts']), 4)
        invalid = bytearray(packet(4, 2))
        invalid[4] = 184
        r = analyze.parse_ts(first + packet(1) + bytes(invalid))
        self.assertEqual(r['malformed_packets'], 1)

    def test_empty_summary_and_invalid_arrivals(self):
        with tempfile.TemporaryDirectory() as d:
            s = analyze.summarize(d, 10, ['engine', 'outpace'])
            self.assertIsNone(s['streams']['outpace']['first_byte_seconds'])
            self.assertIsNone(s['comparison']['packet_mapping']['matched_fraction'])
            Path(d, 'x.arrivals.csv').write_text('t,nbytes,offset\n1,188,10\n')
            with self.assertRaises(ValueError):
                analyze.analyze(d, 'x', 10)

    def test_short_capture_rai_and_exact_duplicate(self):
        p = packet(0, 1, rai=True)
        ts = analyze.parse_ts(p + p)
        self.assertEqual(len(ts['pkts']), 2)
        self.assertEqual(ts['rai'][256], [0, 188])
        self.assertEqual(ts['cc_err'], {})

    def test_more_than_one_retransmission_is_not_accepted(self):
        r = analyze.parse_ts(packet(0) * 4)
        self.assertEqual(r['cc_err'], {256: 2})
        self.assertEqual(r['exact_duplicate_media_packets'], 3)

    def test_changed_same_cc_and_discontinuity(self):
        ts = analyze.parse_ts(packet(0) + packet(0, tag=2) +
                              packet(8, disc=True) + packet(9))
        self.assertEqual(ts['cc_err'], {256: 1})

    def test_rollover_is_unwrapped_per_pid(self):
        rollover = (1 << 33) / 90000
        ts = analyze.parse_ts(packet(0, rollover - 1) + packet(1, 0.5))
        self.assertAlmostEqual(ts['pcrs'][1][1] - ts['pcrs'][0][1], 1.5)

    def test_ambiguous_reference_packets_do_not_count_as_holes(self):
        a = packet(0, tag=1)
        ambiguous = packet(1, tag=2)
        b = packet(2, tag=3)
        result = analyze.packet_identity(analyze.parse_ts(a + ambiguous + ambiguous + b),
                                         analyze.parse_ts(a + b))
        self.assertEqual(result['reference_holes'], 0)
        self.assertEqual(len(result['runs']), 1)

    def test_foreign_packet_does_not_hide_missing_reference_packet(self):
        a = analyze.parse_ts(packet(0, tag=1) + packet(1, tag=2) + packet(2, tag=3))
        b = analyze.parse_ts(packet(0, tag=1) + packet(1, tag=9) + packet(2, tag=3))
        result = analyze.packet_identity(a, b)
        self.assertEqual(result['reference_holes'], 1)
        self.assertEqual(result['matched_packets'], 2)
        self.assertEqual(result['runs'][1]['kind'], 'miss')

    def test_identity_counts_holes_and_duplicate_breaks(self):
        a = analyze.parse_ts(b''.join(packet(i, tag=i + 1) for i in range(5)))
        b = analyze.parse_ts(packet(0, tag=1) + packet(2, tag=3) +
                             packet(2, tag=3) + packet(3, tag=4))
        result = analyze.packet_identity(a, b)
        self.assertEqual(result['matched_packets'], 4)
        self.assertEqual(result['runs'][0]['packets'], 1)
        self.assertEqual(result['reference_holes'], 1)
        self.assertEqual(result['backward_or_duplicate_steps'], 1)

    def test_stale_prefix_and_never_started_player(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)
            path.joinpath('x.ts').write_bytes(packet(0, 1) + packet(1, 80))
            path.joinpath('x.arrivals.csv').write_text('t,nbytes,offset\n1,188,0\n2,188,188\n')
            result = analyze.analyze(path, 'x', 10)
            self.assertEqual(result['stale_prefix'][0], 188)
            self.assertIsNone(analyze.player(result, 10)['start'])

    def test_summary_no_start_has_unavailable_stall_metrics(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)
            path.joinpath('x.ts').write_bytes(packet(0, 1) + packet(1, 2))
            path.joinpath('x.arrivals.csv').write_text('t,nbytes,offset\n1,188,0\n2,188,188\n')
            summary = analyze.summarize(path, 10, ['x'])
            player = summary['streams']['x']['player']
            self.assertIsNone(player['start'])
            self.assertIsNone(player['stalls'])
            self.assertIsNone(player['stall_time'])
            self.assertIsNone(player['stall_list'])
            result = subprocess.run([sys.executable, str(Path(__file__).with_name('ab_analyze.py')),
                                     d, '10', 'x'], capture_output=True, timeout=3)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            written = json.loads(path.joinpath('summary.json').read_text())
            self.assertIsNone(written['streams']['x']['player']['stall_time'])

    def test_player_stalls_and_no_media_overlap(self):
        r = {'pcrs': [(0, 0, 0), (1, 4, 188), (8, 8, 376)]}
        p = analyze.player(r, 10)
        self.assertAlmostEqual(p['start'], 1, delta=.05)
        self.assertGreater(p['stall_time'], 2)
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)
            for name, start in [('engine', 0), ('outpace', 100)]:
                path.joinpath(name + '.ts').write_bytes(packet(0, start) + packet(1, start+1))
                path.joinpath(name + '.arrivals.csv').write_text('t,nbytes,offset\n1,188,0\n2,188,188\n')
            s = analyze.summarize(path, 4, ['engine', 'outpace'])
            self.assertIsNone(s['comparison']['identity_in_overlap'])


class CaptureTests(unittest.TestCase):
    def capture_cli(self, outdir):
        # Change umask inside the subprocess, avoiding process-global test state
        # and preexec_fn while HTTP tests may have background handler threads.
        code = ('import os,runpy,sys; os.umask(0o022); sys.argv=sys.argv[1:]; '
                'runpy.run_path(sys.argv[0], run_name="__main__")')
        return subprocess.run([sys.executable, '-c', code,
            str(Path(__file__).with_name('ab_capture.py')),
            '0123456789abcdef0123456789abcdef01234567', '.05', str(outdir),
            'x=nativecid:http://127.0.0.1:1'], capture_output=True, timeout=3)

    def test_public_capture_is_private_under_permissive_umask(self):
        with tempfile.TemporaryDirectory() as d:
            outdir = Path(d, 'new-run')
            result = self.capture_cli(outdir)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(stat.S_IMODE(outdir.stat().st_mode), 0o700)
            files = list(outdir.iterdir())
            self.assertEqual(len(files), 5)
            self.assertTrue(all(stat.S_IMODE(f.stat().st_mode) == 0o600 for f in files))

    def test_existing_private_directory_accepts_runner_metadata(self):
        with tempfile.TemporaryDirectory() as d:
            outdir = Path(d)
            outdir.joinpath('run.json').write_text('{"owned_runner_metadata": true}')
            outdir.joinpath('candidate-state').mkdir()
            before = outdir.joinpath('run.json').read_bytes()
            result = self.capture_cli(outdir)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(outdir.joinpath('run.json').read_bytes(), before)
            self.assertEqual(stat.S_IMODE(outdir.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(outdir.joinpath('x.ts').stat().st_mode), 0o600)

    def test_existing_unsafe_directory_is_rejected_without_chmod(self):
        with tempfile.TemporaryDirectory() as d:
            outdir = Path(d, 'shared')
            outdir.mkdir()
            outdir.chmod(0o755)
            outdir.joinpath('keep.txt').write_text('unrelated')
            result = self.capture_cli(outdir)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(stat.S_IMODE(outdir.stat().st_mode), 0o755)
            self.assertEqual([f.name for f in outdir.iterdir()], ['keep.txt'])
            self.assertEqual(outdir.joinpath('keep.txt').read_text(), 'unrelated')

    def test_symlink_output_directory_is_rejected_without_touching_target(self):
        with tempfile.TemporaryDirectory() as d:
            target = Path(d, 'target')
            target.mkdir(mode=0o700)
            link = Path(d, 'link')
            link.symlink_to(target, target_is_directory=True)
            result = self.capture_cli(link)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(target.iterdir()), [])


    def test_stop_process_leaves_unrelated_child_running(self):
        owned = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
        other = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
        try:
            runner.stop_process(owned)
            self.assertIsNotNone(owned.poll())
            self.assertIsNone(other.poll())
            runner.stop_process(owned)  # idempotent on an exited owned PID
        finally:
            runner.stop_process(owned)
            other.terminate()
            other.wait(timeout=5)

    def test_stat_polling_records_shared_clock(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                if self.path.endswith('/status'):
                    payload = json.dumps({'status': 'playing'}).encode()
                else:
                    payload = packet(0, 1)
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            with tempfile.TemporaryDirectory() as d, contextlib.redirect_stdout(io.StringIO()):
                capture.Capture('0123456789abcdef0123456789abcdef01234567', .3, d,
                    [('x', 'nativecid', f'http://127.0.0.1:{server.server_port}')],
                    poll_interval=.05).run()
                stats = [json.loads(line) for line in Path(d, 'x.stats.jsonl').read_text().splitlines()]
                self.assertGreaterEqual(len(stats), 2)
                self.assertTrue(all(0 < row['t'] < .3 and row['stat']['status'] == 'playing' for row in stats))
        finally:
            server.shutdown()
            server.server_close()

    def test_rejects_unsafe_duplicate_names_and_existing_artifacts(self):
        with self.assertRaises(Exception):
            capture.engine_spec('../escape=http://localhost')
        with tempfile.TemporaryDirectory() as d:
            Path(d, 'x.ts').write_bytes(b'keep')
            with self.assertRaises(ValueError):
                capture.Capture('0123456789abcdef0123456789abcdef01234567', 1,
                    d, [('x', 'nativecid', 'http://localhost')]).run()
            self.assertEqual(Path(d, 'x.ts').read_bytes(), b'keep')

    def test_runner_refuses_occupied_port_without_stopping_owner(self):
        server = socket.socket()
        try:
            server.bind(('0.0.0.0', 0))
            with self.assertRaises(OSError):
                runner.available_ports([(server.getsockname()[1], False)])
            self.assertGreater(server.fileno(), -1)
        finally:
            server.close()

    def test_redirect_exact_bytes_and_bounded_silent_reader(self):
        data = packet(0, 1) + packet(1, 2)
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                if self.path.startswith('/ace/getstream'):
                    body = json.dumps({'response': {'playback_url': '/redirect'}}).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path == '/redirect':
                    self.send_response(302)
                    self.send_header('Location', '/content/test')
                    self.end_headers()
                elif self.path.startswith('/streams/ace/'):
                    self.send_response(200)
                    self.end_headers()
                    time.sleep(2)
                else:
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            with tempfile.TemporaryDirectory() as d:
                base = f'http://127.0.0.1:{server.server_port}'
                start = time.monotonic()
                p = subprocess.run([sys.executable, str(Path(__file__).with_name('ab_capture.py')),
                    '0123456789abcdef0123456789abcdef01234567', '.5', d,
                    'engine='+base, 'outpace=nativecid:'+base], capture_output=True, timeout=3)
                self.assertEqual(p.returncode, 0, p.stderr.decode())
                self.assertLess(time.monotonic()-start, 1.5)
                captured = Path(d, 'engine.ts').read_bytes()
                self.assertEqual(captured, data)
                with Path(d, 'engine.arrivals.csv').open() as f:
                    rows = list(csv.DictReader(f))
                self.assertEqual(sum(int(r['nbytes']) for r in rows), len(data))
                self.assertEqual(Path(d, 'outpace.ts').stat().st_size, 0)
                self.assertIn('302', Path(d, 'engine.events.txt').read_text())
        finally:
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    unittest.main()
