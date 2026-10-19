"""Offline contract tests using loopback fakes and synthetic values only."""
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
import json
import threading
import time
import unittest
from unittest import mock

import session_cycles as soak

SELECTOR = "0123456789abcdef0123456789abcdef01234567"


class MetricTests(unittest.TestCase):
    def test_epoch_field_must_exist_and_be_null(self):
        good = {"allocator": "jemalloc", "epoch_error": None, "allocated": 12}
        self.assertEqual(soak.parse_metric(good, "allocated"), 12)
        for bad in ({k: v for k, v in good.items() if k != "epoch_error"},
                    {**good, "epoch_error": "x"}, {**good, "allocated": True},
                    {**good, "allocated": -1}, {**good, "allocated": 1.2}):
            with self.subTest(bad=bad), self.assertRaises(soak.Inconclusive):
                soak.parse_metric(bad, "allocated")

    def test_metric_fallback_freezes_and_rss_validation(self):
        with mock.patch.object(soak.sys, "platform", "linux"):
            self.assertEqual(soak.select_metric({"allocator": "system"}, 2), "rss_kib")
            for rss in (None, -1, True, 1.5):
                with self.assertRaises(soak.Inconclusive):
                    soak.select_metric({"allocator": "system"}, rss)
        self.assertEqual(soak.select_metric({"allocator": "jemalloc", "epoch_error": None, "allocated": 1}, 2), "allocated")

    def test_budget_inclusive_recovery_and_growth(self):
        self.assertEqual(soak.classify([10, 0, 8], 100, 10), "PASS")
        self.assertEqual(soak.classify([11], 100, 10), "FAIL")
        with self.assertRaises(soak.Inconclusive):
            soak.classify([float("nan")], 0, 1)

    def test_later_invalid_allocator_sample_cannot_switch_metric(self):
        for bad in ({"allocator": "jemalloc", "allocated": 1},
                    {"allocator": "jemalloc", "epoch_error": False, "allocated": 1},
                    {"allocator": "jemalloc", "epoch_error": 1, "allocated": 1}):
            with self.subTest(bad=bad), self.assertRaises(soak.Inconclusive):
                soak.parse_metric(bad, "allocated")

    def test_rss_rejects_changed_identity(self):
        with mock.patch.object(soak.sys, "platform", "linux"), \
             mock.patch.object(soak, "process_identity", return_value=("new", "exe", "hash")):
            with self.assertRaises(soak.Inconclusive):
                soak.rss_kib(12, ("old", "exe", "hash"))

    def test_argument_counts_reject_zero_and_boolean(self):
        for value in (0, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                soak.validate_count(value, "cycles", 1000)

    def test_positive_number_rejects_nonfinite_and_boolean(self):
        for value in (True, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                soak.positive_number(value, "timeout", 10)


class GuardTests(unittest.TestCase):
    def test_hls_rejects_external_and_wrong_route(self):
        for uri in ("https://example.invalid/a.ts", "http://127.0.0.1:8/a.ts", "/elsewhere/a.ts"):
            with self.subTest(uri=uri), self.assertRaises(soak.Inconclusive):
                soak._same_loopback_route("http://127.0.0.1:8", uri, "/streams/ace/cid:x")

    def test_deadline_is_hard_and_never_extended(self):
        with self.assertRaises(soak.Inconclusive):
            soak.remaining(time.monotonic() - 0.01)

    def test_child_cleanup_is_idempotent_and_owned_process_only(self):
        child = mock.Mock()
        child.poll.side_effect = [None, 0]
        soak.stop_child(child)
        child.terminate.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=5)
        other = mock.Mock()
        other.poll.return_value = 0
        soak.stop_child(other)
        other.terminate.assert_not_called()

    def test_child_cleanup_kills_after_term_timeout(self):
        child = mock.Mock()
        child.poll.side_effect = [None, None]
        child.wait.side_effect = [soak.subprocess.TimeoutExpired("serve", 5), None]
        soak.stop_child(child)
        child.terminate.assert_called_once_with()
        child.kill.assert_called_once_with()
        self.assertEqual(child.wait.call_count, 2)

    def test_child_cleanup_survives_interrupt_during_initial_poll(self):
        child = mock.Mock()
        child.poll.side_effect = KeyboardInterrupt
        soak.stop_child(child)
        child.kill.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=5)

    def test_ipv4_wildcard_is_not_an_owned_loopback_listener(self):
        line = "0: 00000000:1234 00000000:0000 0A 0 0 0 1000 0 55"
        with mock.patch.object(soak.Path, "read_text", return_value="header\n" + line):
            self.assertFalse(soak.owns_listener(123, 0x1234))

    def test_interruption_is_reported_as_inconclusive_exit_two(self):
        with mock.patch.object(soak, "run", side_effect=KeyboardInterrupt):
            self.assertEqual(soak.main(["--binary", "unused", "--budget-mib", "1"]), 2)

    def test_report_has_budget_and_no_identifiers_or_urls(self):
        report = soak.make_report("PASS", [1], 10, [11], "allocated", "ts", "idle", 4)
        encoded = json.dumps(report)
        for forbidden in ("cid:", "http://", "0123456789abcdef"):
            self.assertNotIn(forbidden, encoded)
        self.assertEqual(report["cycles"][0]["median"], 11)
        self.assertIsInstance(report["baseline"], (int, float))
        self.assertIn("bytes", encoded)


class FakeApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            present = True
            delete_status = 204
            sequence = 0
            disconnects = 0
            malformed = False
            def do_GET(self):
                if self.path == "/streams":
                    if type(self).malformed:
                        body = b'{"streams":{}}'
                    else:
                        item = {"network": "ace", "id": "ih:" + SELECTOR, "clients": 0,
                                "metadata": {}}
                        body = json.dumps({"streams": [item] if type(self).present else []}).encode()
                elif self.path.endswith(".m3u8"):
                    type(self).sequence += 1
                    seq = type(self).sequence
                    body = (f"#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:{seq}\n#EXTINF:1,\n/streams/ace/cid:x/seg/{seq}.ts\n").encode()
                elif self.path == "/debug/memstats":
                    body = b'{"allocator":"jemalloc","epoch_error":null,"allocated":100}'
                else:
                    if self.path.startswith("/streams/ace/cid:") and self.path.endswith(".ts"):
                        type(self).present = True
                        self.send_response(200); self.end_headers()
                        for _ in range(20):
                            try:
                                self.wfile.write(b"synthetic-segment"); self.wfile.flush(); time.sleep(.01)
                            except (BrokenPipeError, ConnectionResetError):
                                break
                        return
                    body = b"synthetic-segment"
                self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
            def do_DELETE(self):
                self.send_response(type(self).delete_status); self.end_headers()
                if type(self).delete_status == 204:
                    type(self).present = False
            def log_message(self, *_):
                pass
        cls.handler = Handler
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.daemon_threads = True
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True); cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown(); cls.thread.join(timeout=2); cls.server.server_close()

    def setUp(self):
        self.handler.present = True; self.handler.delete_status = 204; self.handler.sequence = 0
        self.handler.disconnects = 0; self.handler.malformed = False

    def test_validated_presence_and_malformed_shape(self):
        self.assertTrue(soak.streams_present(self.base, SELECTOR, time.monotonic() + 2))
        self.handler.malformed = True
        with self.assertRaises(soak.Inconclusive):
            soak.streams_present(self.base, SELECTOR, time.monotonic() + 2)

    def test_fresh_empty_stream_list_then_ts_get_starts_session(self):
        self.handler.present = False
        self.assertFalse(soak.streams_present(self.base, SELECTOR, time.monotonic() + 2))
        status, body = soak.http(self.base + "/streams/ace/cid:" + SELECTOR + ".ts", time.monotonic() + 2)
        self.assertEqual(status, 200); self.assertTrue(body)
        self.assertTrue(soak.streams_present(self.base, SELECTOR, time.monotonic() + 2))

    def test_content_length_memstats_and_native_streams_are_read_fully(self):
        status, body = soak.http(self.base + "/streams", time.monotonic() + 2)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["streams"][0]["network"], "ace")
        self.assertEqual(soak.json_get(self.base, "/streams", time.monotonic() + 2)["streams"][0]["clients"], 0)

    def test_delete_failure_and_native_clients_field(self):
        self.handler.delete_status = 500
        with self.assertRaises(soak.Inconclusive):
            soak.delete_and_wait(self.base, "/streams/example", SELECTOR, time.monotonic() + 2)
        self.assertEqual(soak.target_clients(self.base, SELECTOR, time.monotonic() + 2), 0)

    def test_system_allocator_does_not_need_epoch_error(self):
        self.assertEqual(soak.select_metric({"allocator": "system"}, 17), "rss_kib")

    def test_api_rejects_missing_native_item_fields(self):
        class Malformed(BaseHTTPRequestHandler):
            def do_GET(self):
                body = b'{"streams":[{"network":"ace","id":"cid:x","clients":0}]}'
                self.send_response(200); self.end_headers(); self.wfile.write(body)
            def log_message(self, *_): pass
        server = HTTPServer(("127.0.0.1", 0), Malformed)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            with self.assertRaises(soak.Inconclusive):
                soak.streams_present(f"http://127.0.0.1:{server.server_port}", SELECTOR, time.monotonic() + 2)
        finally:
            server.shutdown(); thread.join(timeout=2); server.server_close()

    def test_delete_stuck_disconnected_is_failure_but_schema_is_inconclusive(self):
        self.handler.present = True
        original = soak.streams_present
        with mock.patch.object(soak, "streams_present", return_value=True), \
             mock.patch.object(soak, "target_clients", return_value=0), \
             mock.patch.object(soak, "http", return_value=(204, b"")):
            with self.assertRaises(soak.Failed):
                soak.delete_and_wait(self.base, "/streams/ace/x", SELECTOR, time.monotonic() - 1, time.monotonic() + 1)
        with mock.patch.object(soak, "streams_present", side_effect=soak.Inconclusive("bad schema")), \
             mock.patch.object(soak, "http", return_value=(204, b"")):
            with self.assertRaises(soak.Inconclusive):
                soak.delete_and_wait(self.base, "/streams/ace/x", SELECTOR, time.monotonic() + 1)

    def test_hls_requires_advancing_sequence_and_new_segment_bytes(self):
        self.assertGreater(soak.play_hls(self.base, "/streams/ace/cid:x", time.monotonic() + 2), 0)

    def test_ts_returns_after_duration_with_two_reads(self):
        class Source(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200); self.end_headers()
                for _ in range(8):
                    try:
                        self.wfile.write(b"chunk"); self.wfile.flush(); time.sleep(.03)
                    except BrokenPipeError:
                        break
            def log_message(self, *_): pass
        server = HTTPServer(("127.0.0.1", 0), Source)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            self.assertGreater(soak.play_ts(f"http://127.0.0.1:{server.server_port}/", time.monotonic() + .08), 0)
        finally:
            server.shutdown(); thread.join(timeout=2); server.server_close()

    def test_ts_stalled_source_is_inconclusive_before_deadline(self):
        class Source(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200); self.end_headers(); self.wfile.write(b"first"); self.wfile.flush()
                time.sleep(.2)
            def log_message(self, *_): pass
        server = HTTPServer(("127.0.0.1", 0), Source)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            with self.assertRaises(soak.Inconclusive):
                soak.play_ts(f"http://127.0.0.1:{server.server_port}/", time.monotonic() + .05)
        finally:
            server.shutdown(); thread.join(timeout=2); server.server_close()

    def test_hls_stall_after_initial_advancement_is_inconclusive(self):
        real_sleep = time.sleep
        class Stalling(BaseHTTPRequestHandler):
            calls = 0
            def do_GET(self):
                type(self).calls += 1
                if self.path.endswith(".m3u8"):
                    seq = min(type(self).calls, 2)
                    body = f"#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:{seq}\n#EXTINF:1,\n/streams/ace/cid:x/seg/{seq}.ts\n".encode()
                else:
                    body = b"segment"
                self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
            def log_message(self, *_): pass
        server = HTTPServer(("127.0.0.1", 0), Stalling)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            with mock.patch.object(soak.time, "sleep", side_effect=lambda _delay: real_sleep(.01)):
                with self.assertRaises(soak.Inconclusive):
                    soak.play_hls(f"http://127.0.0.1:{server.server_port}", "/streams/ace/cid:x", time.monotonic() + .08, progress_timeout=.02)
        finally:
            server.shutdown(); thread.join(timeout=2); server.server_close()

    def test_absence_wait_obeys_overall_deadline_without_extension(self):
        with mock.patch.object(soak, "streams_present", return_value=True), \
             mock.patch.object(soak, "target_clients", return_value=0):
            with self.assertRaises(soak.Inconclusive):
                soak.wait_absent(self.base, SELECTOR, time.monotonic() + 1, overall=time.monotonic() - .01)

    def test_idle_and_postclose_graces_are_fully_observed_when_absent(self):
        args = type("Args", (), {"media": "ts", "playback_duration": .01, "teardown": "idle",
            "reap_timeout": .1, "idle_grace": .05, "postclose_grace": .05, "timeout": .02})()
        proc = mock.Mock(); proc.pid = 123
        present = [True] + [False] * 20
        with mock.patch.object(soak, "check_identity"), mock.patch.object(soak, "owns_listener", return_value=True), \
             mock.patch.object(soak, "streams_present", side_effect=lambda *_a: present.pop(0) if present else False), \
             mock.patch.object(soak, "play_ts", side_effect=lambda _u, _d, guard, *_x: (guard(), 100)[1]):
            started = time.monotonic()
            soak.run_workload(self.base, "/streams/ace/cid:x", SELECTOR, args, proc, (), "hash",
                              started + 2, [1, 2], [False])
        self.assertGreaterEqual(time.monotonic() - started, .095)

    def test_interrupted_run_finally_cleans_only_its_child(self):
        import os
        from pathlib import Path
        binary = Path(soak.sys.executable).resolve()
        digest = soak.hashlib.sha256(binary.read_bytes()).hexdigest()
        args = type("Args", (), {"binary": binary, "overall_timeout": 5, "warmups": 1,
                                  "budget_mib": 1})()
        proc = mock.Mock(); proc.pid = 123; proc.poll.return_value = None
        real_stat = os.stat
        with mock.patch.dict(os.environ, {"ACE_CONTENT_ID": SELECTOR}), \
             mock.patch.object(soak, "free_port", side_effect=[14001, 14002]), \
             mock.patch.object(soak.subprocess, "Popen", return_value=proc), \
             mock.patch.object(soak, "process_identity", return_value=("start", str(binary), digest)), \
             mock.patch.object(soak.os, "stat", side_effect=lambda path, **kw: binary.stat() if str(path).startswith("/proc/123/exe") else real_stat(path, **kw)), \
             mock.patch.object(soak, "owns_listener", return_value=True), \
             mock.patch.object(soak, "http", return_value=(200, b"ok")), \
             mock.patch.object(soak, "run_workload", side_effect=KeyboardInterrupt), \
             mock.patch.object(soak, "stop_child") as stop:
            with self.assertRaises(KeyboardInterrupt):
                soak.run(args)
        stop.assert_called_once_with(proc)

    def test_full_workload_starts_from_empty_and_checks_during_play(self):
        args = type("Args", (), {"media": "ts", "playback_duration": .1, "teardown": "delete",
                                  "reap_timeout": 1, "idle_grace": .1, "postclose_grace": .01, "timeout": .1})()
        proc = mock.Mock(); proc.pid = 123
        observed = [False]
        with mock.patch.object(soak, "check_identity"), mock.patch.object(soak, "owns_listener", return_value=True) as listener, \
             mock.patch.object(soak, "streams_present", side_effect=[True, False]), \
             mock.patch.object(soak, "play_ts", side_effect=lambda _u, _d, guard, *_extra: (guard(), 100)[1]), \
             mock.patch.object(soak, "delete_and_wait") as delete:
            soak.run_workload(self.base, "/streams/ace/cid:x", SELECTOR, args, proc, (), "hash",
                              time.monotonic() + 4, [1, 2, 3], observed)
        self.assertTrue(observed[0]); delete.assert_called_once()
        self.assertFalse(any(call.args[1] == 3 for call in listener.call_args_list))

    def test_full_workload_hls_path_uses_guard(self):
        args = type("Args", (), {"media": "hls", "playback_duration": .1, "teardown": "delete",
                                  "reap_timeout": 1, "idle_grace": .1, "postclose_grace": .01, "timeout": .1})()
        proc = mock.Mock(); proc.pid = 123
        observed = [False]
        with mock.patch.object(soak, "check_identity"), mock.patch.object(soak, "owns_listener", return_value=True), \
             mock.patch.object(soak, "streams_present", side_effect=[True, False, False]), \
             mock.patch.object(soak, "play_hls", side_effect=lambda _b, _p, _d, guard, *_extra: guard()), \
             mock.patch.object(soak, "delete_and_wait"):
            soak.run_workload(self.base, "/streams/ace/cid:x", SELECTOR, args, proc, (), "hash",
                              time.monotonic() + 4, [1, 2], observed)
        self.assertTrue(observed[0])

    def test_idle_teardown_fixed_grace_and_zero_client_failure(self):
        args = type("Args", (), {"media": "ts", "playback_duration": .01, "teardown": "idle",
                                  "reap_timeout": 1, "idle_grace": .01, "postclose_grace": .01, "timeout": .1})()
        proc = mock.Mock(); proc.pid = 123
        observed = [False]
        with mock.patch.object(soak, "check_identity"), mock.patch.object(soak, "owns_listener", return_value=True), \
             mock.patch.object(soak, "streams_present", side_effect=[True, False, False, False]), \
             mock.patch.object(soak, "play_ts", side_effect=lambda _u, _d, guard, *_extra: (guard(), 100)[1]):
            soak.run_workload(self.base, "/streams/ace/cid:x", SELECTOR, args, proc, (), "hash",
                              time.monotonic() + 4, [1, 2], observed)
        self.assertTrue(observed[0])

    def test_idle_target_with_clients_is_inconclusive_after_grace(self):
        args = type("Args", (), {"media": "ts", "playback_duration": .01, "teardown": "idle",
                                  "reap_timeout": 1, "idle_grace": .001, "postclose_grace": .001, "timeout": .1})()
        proc = mock.Mock(); proc.pid = 123
        with mock.patch.object(soak, "check_identity"), mock.patch.object(soak, "owns_listener", return_value=True), \
             mock.patch.object(soak, "streams_present", return_value=True), \
             mock.patch.object(soak, "target_clients", return_value=1), \
             mock.patch.object(soak, "play_ts", side_effect=lambda _u, _d, guard, *_extra: guard()):
            with self.assertRaises(soak.Inconclusive):
                soak.run_workload(self.base, "/streams/ace/cid:x", SELECTOR, args, proc, (), "hash",
                                  time.monotonic() + 4, [1, 2], [False])

    def test_cycle_uses_exact_window_samples_after_warmup(self):
        import os
        from pathlib import Path
        binary = Path(soak.sys.executable).resolve()
        args = type("Args", (), {"binary": binary, "budget_mib": 1, "warmups": 1, "cycles": 1,
                                      "baseline_samples": 2, "window_samples": 3, "playback_duration": 1,
                                      "idle_grace": 65, "reap_timeout": 2, "postclose_grace": .001, "timeout": 1,
                                      "timeout": 1, "overall_timeout": 10, "media": "ts", "teardown": "delete"})()
        events = []
        proc = mock.Mock(); proc.pid = os.getpid(); proc.poll.return_value = None
        def workload(*_args): events.append("workload"); _args[-1][0] = True
        def json_value(*_args): events.append("memstats"); return {"allocator": "system"}
        windows = []
        def window(*_args):
            count = _args[-2]; windows.append(count); return [100] * count
        with mock.patch.dict(os.environ, {"ACE_CONTENT_ID": SELECTOR}), \
                 mock.patch.object(soak, "free_port", side_effect=[11001, 11002, 11003]), \
                 mock.patch.object(soak.subprocess, "Popen", return_value=proc), \
                 mock.patch.object(soak.subprocess, "run", return_value=mock.Mock(stdout="outpace 1.2.3\n")), \
                 mock.patch.object(soak, "read_version", return_value="outpace 1.2.3"), \
                 mock.patch.object(soak, "process_identity", return_value=("start", str(binary), soak.hashlib.sha256(binary.read_bytes()).hexdigest())), \
                 mock.patch.object(soak, "owns_listener", return_value=True), \
                 mock.patch.object(soak, "http", return_value=(200, b"")), \
                 mock.patch.object(soak, "json_get", side_effect=json_value), \
                 mock.patch.object(soak, "rss_kib", return_value=100), \
                 mock.patch.object(soak, "run_workload", side_effect=workload), \
                 mock.patch.object(soak, "sample_window", side_effect=window), \
                 mock.patch.object(soak, "sample", return_value=100), \
                 mock.patch.object(soak, "stop_child"):
            report = soak.run(args)
        self.assertEqual(events, ["workload", "memstats", "workload"])
        self.assertEqual(windows, [2, 3])
        self.assertEqual(report["version"], "outpace 1.2.3")
        self.assertEqual(len(report["cycles"]), 1)

    def test_run_full_fake_api_lifecycle_from_empty_through_delete_and_idle_sample(self):
        self.handler.present = False
        args = type("Args", (), {"binary": __import__("pathlib").Path(soak.sys.executable).resolve(),
            "budget_mib": 1, "warmups": 1, "cycles": 1, "baseline_samples": 1, "window_samples": 1,
            "playback_duration": .12, "idle_grace": .01, "reap_timeout": .2, "postclose_grace": .01,
            "timeout": .2, "overall_timeout": 3, "media": "ts", "teardown": "delete"})()
        proc = mock.Mock(); proc.pid = 123; proc.poll.return_value = None
        expected_hash = soak.hashlib.sha256(args.binary.read_bytes()).hexdigest()
        fake_identity = ("start", str(args.binary), expected_hash)
        import os
        real_stat = os.stat
        binary_stat = args.binary.stat()
        with mock.patch.dict(__import__("os").environ, {"ACE_CONTENT_ID": SELECTOR}), \
             mock.patch.object(soak, "free_port", side_effect=[self.server.server_port, 13002]), \
             mock.patch.object(soak.subprocess, "Popen", return_value=proc), \
             mock.patch.object(soak, "read_version", return_value="outpace 1.2.3"), \
             mock.patch.object(soak, "process_identity", return_value=fake_identity), \
             mock.patch.object(soak.os, "stat", side_effect=lambda path, **kwargs: binary_stat if str(path).startswith("/proc/123/exe") else real_stat(path, **kwargs)), \
             mock.patch.object(soak, "owns_listener", return_value=True), \
             mock.patch.object(soak, "rss_kib", return_value=100), \
             mock.patch.object(soak, "sample", return_value=100), \
             mock.patch.object(soak, "stop_child") as stop:
            report = soak.run(args)
        stop.assert_called_once_with(proc)
        self.assertEqual(report["result"], "PASS")
        self.assertFalse(self.handler.present)

    def test_redirect_is_denied(self):
        class Redirect(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302); self.send_header("Location", "http://example.invalid/"); self.end_headers()
            def log_message(self, *_): pass
        server = HTTPServer(("127.0.0.1", 0), Redirect)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            with self.assertRaises(soak.Inconclusive):
                soak.http(f"http://127.0.0.1:{server.server_port}/", time.monotonic() + 2)
        finally:
            server.shutdown(); thread.join(timeout=2); server.server_close()


if __name__ == "__main__":
    unittest.main()
