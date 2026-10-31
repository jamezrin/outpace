import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import vlc_compare as vlc


def status(video=0, audio=0, **extra):
    counters = dict.fromkeys(vlc.COUNTERS, 0)
    counters.update(decodedvideo=video, decodedaudio=audio, **extra)
    return {"state": "playing", "stats": counters}


class ObservationTests(unittest.TestCase):
    def test_progress_then_stall_includes_unsampled_tail(self):
        observation = vlc.Observation()
        observation.sample(5, status(10, 20))
        observation.sample(10, status(20, 40))
        observation.sample(15, status(20, 40))
        report = observation.report(21)
        self.assertEqual(report["max_sampled_progress_gap_seconds"]["decodedvideo"], 11)
        self.assertEqual(report["first_decode_observed_seconds"]["decodedvideo"], 5)

    def test_video_only_does_not_fabricate_audio_progress(self):
        observation = vlc.Observation()
        observation.sample(5, status(10))
        report = observation.report(10)
        self.assertIsNone(report["first_decode_observed_seconds"]["decodedaudio"])
        self.assertIsNone(report["max_sampled_progress_gap_seconds"]["decodedaudio"])

    def test_missing_or_invalid_stats_are_errors(self):
        observation = vlc.Observation()
        for sample in ({}, {"stats": None}, {"stats": {"decodedvideo": True}}):
            observation.sample(5, sample)
        self.assertEqual(observation.errors, 3)
        self.assertEqual(observation.samples, 0)

    def test_reset_and_loss_are_visible(self):
        observation = vlc.Observation()
        observation.sample(5, status(100, 200, lostpictures=2))
        observation.sample(10, status(1, 2))
        self.assertEqual(observation.report(10)["counter_resets"], 1)


class LifecycleTests(unittest.TestCase):
    def test_stop_escalates_and_reaps_only_owned_process(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("vlc", 5), -9]
        self.assertEqual(vlc.stop_client(process), -9)
        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()

    def test_already_exited_client_is_reaped(self):
        process = Mock()
        process.poll.return_value = 1
        vlc.stop_client(process)
        process.terminate.assert_not_called()
        process.wait.assert_called_once()

    def run_interrupted(self, second_launch_fails=False):
        args = SimpleNamespace(vlc="cvlc", engine_url="http://127.0.0.1:6878",
                               outpace_url="http://127.0.0.1:16879", client_warp="off",
                               server_warp="unknown", network_caching=1000,
                               seconds=10, poll_seconds=5)
        placeholder = "0123456789abcdef0123456789abcdef01234567"
        processes = [Mock(), Mock()]
        for process in processes:
            process.poll.return_value = None
            process.wait.return_value = 0
        with tempfile.TemporaryDirectory() as root:
            output = io.StringIO()
            with patch.object(vlc.tempfile, "mkdtemp", return_value=root), \
                    patch.object(vlc.subprocess, "run", return_value=SimpleNamespace(stdout=b"VLC test\n")), \
                    patch.object(vlc.subprocess, "Popen", side_effect=(
                        [processes[0], OSError("launch failed")] if second_launch_fails else processes
                    )) as launch, \
                    patch.object(vlc, "status_request", side_effect=KeyboardInterrupt), \
                    redirect_stdout(output):
                code = vlc.compare(args, placeholder)
            summary = json.loads((Path(root) / "comparison.json").read_text())
            self.assertFalse(summary["observation_complete"])
            self.assertNotIn(placeholder, output.getvalue())
            self.assertNotIn(placeholder, json.dumps(summary))
            for call in launch.call_args_list:
                self.assertNotIn(placeholder, str(call.args))
                self.assertNotIn("ACE_CONTENT_ID", call.kwargs["env"])
            processes[0].terminate.assert_called_once()
            processes[0].wait.assert_called_once()
            if not second_launch_fails:
                processes[1].terminate.assert_called_once()
            return code

    def test_interrupt_stops_both_clients_and_writes_partial_report(self):
        self.assertEqual(self.run_interrupted(), 130)

    def test_second_launch_failure_stops_first_client(self):
        self.assertEqual(self.run_interrupted(second_launch_fails=True), 2)


class OwnedPlaybackTests(unittest.TestCase):
    def args(self):
        return SimpleNamespace(vlc="cvlc", engine_url="http://127.0.0.1:6878",
                               outpace_url="http://127.0.0.1:16879", client_warp="off",
                               server_warp="unknown", network_caching=1000,
                               seconds=10, poll_seconds=5)

    def urls(self):
        identity = "0123456789abcdef0123456789abcdef01234567"
        return {"engine": f"http://127.0.0.1:6878/ace/r/{identity}/" + "a" * 32,
                "outpace": f"http://127.0.0.1:16879/ace/r/{identity}/" + "b" * 64}

    def test_owned_urls_only_enter_private_playlists_and_clients_are_reaped(self):
        processes = [Mock(), Mock()]
        for process in processes:
            process.poll.return_value = None
            process.wait.return_value = 0
        urls = self.urls()
        with tempfile.TemporaryDirectory() as root:
            output = io.StringIO()
            with patch.object(vlc.tempfile, "mkdtemp", return_value=root), \
                    patch.object(vlc.subprocess, "run", return_value=SimpleNamespace(stdout=b"VLC test\n")), \
                    patch.object(vlc.subprocess, "Popen", side_effect=processes) as launch, \
                    patch.object(vlc, "status_request", side_effect=KeyboardInterrupt), redirect_stdout(output):
                code = vlc.compare(self.args(), None, playback_urls=urls)
            self.assertEqual(code, 130)
            for role, url in urls.items():
                playlist = Path(root, role + ".m3u")
                self.assertEqual(playlist.read_text(), "#EXTM3U\n" + url + "\n")
                self.assertEqual(playlist.stat().st_mode & 0o777, 0o600)
                self.assertNotIn(url, output.getvalue())
                self.assertNotIn(url, Path(root, "config.json").read_text())
                self.assertNotIn(url, Path(root, "comparison.json").read_text())
                self.assertNotIn(url, str(launch.call_args_list))
            for process in processes:
                process.terminate.assert_called_once()
                process.wait.assert_called_once()

    def test_absolute_origin_uses_initial_clock_even_when_launch_fails(self):
        ticks = iter(12345.125 + i * 0.25 for i in range(100))
        with tempfile.TemporaryDirectory() as parent, \
                patch.object(vlc.time, 'monotonic', side_effect=lambda: next(ticks)), \
                patch.object(vlc.subprocess, 'run', return_value=SimpleNamespace(stdout=b'VLC fixture\n')), \
                patch.object(vlc.subprocess, 'Popen', side_effect=OSError('fixture launch')), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(vlc.compare(self.args(), None, playback_urls=self.urls(),
                                        artifact_parent=Path(parent)), 2)
            root, = Path(parent).iterdir()
            config = json.loads((root / 'config.json').read_text())
            report = json.loads((root / 'comparison.json').read_text())
            self.assertEqual(config['origin_monotonic'], 12345.125)
            self.assertEqual(report['origin_monotonic'], config['origin_monotonic'])
            self.assertEqual(report['observed_seconds'], 0.25)

    def test_serialized_endpoint_preserves_terminal_samples_and_stall_gap(self):
        # Rounding the endpoint down must not put a real terminal sample after it.
        for stalled in (False, True):
            with self.subTest(stalled=stalled):
                origin = 128.0
                clock = [origin]
                args = self.args()
                args.seconds = 65.0004
                args.poll_seconds = 5
                processes = [Mock(), Mock()]
                for process in processes:
                    process.poll.return_value = None
                    process.wait.return_value = 0

                def sleep(seconds):
                    clock[0] += seconds

                def sample(*_args):
                    if clock[0] >= origin + 65:
                        clock[0] = origin + 65.00031
                        return status(10 if stalled else 140, 20 if stalled else 280)
                    count = 10 if stalled else 10 + int(clock[0] - origin) * 2
                    return status(count, count * 2)

                with tempfile.TemporaryDirectory() as root, \
                        patch.object(vlc.tempfile, 'mkdtemp', return_value=root), \
                        patch.object(vlc.subprocess, 'run', return_value=SimpleNamespace(stdout=b'VLC fixture\n')), \
                        patch.object(vlc.subprocess, 'Popen', side_effect=processes), \
                        patch.object(vlc, 'free_port', side_effect=[18001, 18002]), \
                        patch.object(vlc, 'status_request', side_effect=sample), \
                        patch.object(vlc.time, 'monotonic', side_effect=lambda: clock[0]), \
                        patch.object(vlc.time, 'sleep', side_effect=sleep), \
                        redirect_stdout(io.StringIO()):
                    self.assertEqual(vlc.compare(args, None, playback_urls=self.urls()), 0)
                    report = json.loads(Path(root, 'comparison.json').read_text())
                    endpoint = report['observed_seconds']
                    self.assertEqual(report['origin_monotonic'], origin)
                    for role in ('engine', 'outpace'):
                        samples = [json.loads(line) for line in
                                   Path(root, role + '-status.jsonl').read_text().splitlines()]
                        self.assertEqual(len(samples), 14)
                        terminal = samples[-1]['seconds']
                        self.assertGreater(terminal, round(args.seconds, 3))
                        self.assertLessEqual(terminal, endpoint)
                        # An exact boundary needs no tolerance that admits future samples.
                        self.assertGreater(65.0005, endpoint)
                        gaps = report['clients'][role]['max_sampled_progress_gap_seconds']
                        for key in vlc.PROGRESS:
                            if stalled:
                                self.assertGreater(gaps[key], 60)
                            else:
                                self.assertLess(gaps[key], 60)
                    self.assertEqual(endpoint, (origin + 65.0004) - origin)
                    self.assertNotEqual(endpoint, round(endpoint, 3))
                    # Completion is an observation result, never a playback quality pass.
                    self.assertIn('no quality pass/fail', report['limits'])

    def test_cancel_after_first_launch_registers_and_reaps_before_second(self):
        cancelled = False
        process = Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        def spawn(*args, **kwargs):
            nonlocal cancelled
            cancelled = True
            return process
        with tempfile.TemporaryDirectory() as parent, \
                patch.object(vlc.subprocess, 'run', return_value=SimpleNamespace(stdout=b'VLC fixture\n')), \
                patch.object(vlc.subprocess, 'Popen', side_effect=spawn) as launch, \
                redirect_stdout(io.StringIO()):
            code = vlc.compare(self.args(), None, playback_urls=self.urls(),
                               cancel_requested=lambda: cancelled, artifact_parent=Path(parent))
            self.assertEqual(code, 130)
            launch.assert_called_once()
            process.terminate.assert_called_once()
            process.wait.assert_called_once()
            roots = list(Path(parent).iterdir())
            self.assertEqual(len(roots), 1)
            self.assertEqual(roots[0].stat().st_mode & 0o777, 0o700)
            report = json.loads((roots[0] / 'comparison.json').read_text())
            self.assertTrue(report['interrupted'])
            self.assertEqual(set(report['clients']), {'engine'})

    def test_cli_signals_latch_through_first_launch_and_restore_handlers(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        old = {sig: vlc.signal.getsignal(sig) for sig in (vlc.signal.SIGINT, vlc.signal.SIGTERM)}
        def spawn(*args, **kwargs):
            vlc.os.kill(vlc.os.getpid(), vlc.signal.SIGTERM)
            vlc.os.kill(vlc.os.getpid(), vlc.signal.SIGINT)
            return process
        with tempfile.TemporaryDirectory() as root, \
                patch.object(vlc.tempfile, 'mkdtemp', return_value=root), \
                patch.object(vlc.subprocess, 'run', return_value=SimpleNamespace(stdout=b'VLC fixture\n')), \
                patch.object(vlc.subprocess, 'Popen', side_effect=spawn) as launch, \
                patch.object(vlc.shutil, 'which', return_value='/fixture/cvlc'), \
                patch.dict(vlc.os.environ, ACE_CONTENT_ID='0123456789abcdef0123456789abcdef01234567'), \
                patch('sys.argv', ['vlc_compare.py', '--engine-url', self.args().engine_url,
                                   '--outpace-url', self.args().outpace_url]), redirect_stdout(io.StringIO()):
            self.assertEqual(vlc.main(), 130)
            launch.assert_called_once()
            process.terminate.assert_called_once()
            process.wait.assert_called_once()
        for sig, handler in old.items():
            self.assertEqual(vlc.signal.getsignal(sig), handler)

    def terminal_cancel(self, when):
        clock = [0.0]
        cancelled = [False]
        sleeps = []
        args = self.args()
        if when == 'long_sleep':
            args.seconds = 600
            args.poll_seconds = 300
        processes = [Mock(), Mock()]
        for process in processes:
            process.poll.return_value = None
            process.wait.return_value = 0
        old = {sig: vlc.signal.getsignal(sig) for sig in (vlc.signal.SIGINT, vlc.signal.SIGTERM)}
        def sleep(_seconds):
            sleeps.append(_seconds)
            clock[0] = clock[0] + _seconds if when == 'long_sleep' else 10.0
            if when in ('sleep', 'long_sleep'):
                cancelled[0] = True
        def cleanup_wait(*args, **kwargs):
            vlc.os.kill(vlc.os.getpid(), vlc.signal.SIGTERM)
            return 0
        if when == 'cleanup':
            processes[0].wait.side_effect = cleanup_wait
        with tempfile.TemporaryDirectory() as root, \
                patch.object(vlc.tempfile, 'mkdtemp', return_value=root), \
                patch.object(vlc.subprocess, 'run', return_value=SimpleNamespace(stdout=b'VLC fixture\n')), \
                patch.object(vlc.subprocess, 'Popen', side_effect=processes), \
                patch.object(vlc, 'status_request', return_value=status(10, 20)), \
                patch.object(vlc.time, 'monotonic', side_effect=lambda: clock[0]), \
                patch.object(vlc.time, 'sleep', side_effect=sleep), \
                patch.object(vlc.shutil, 'which', return_value='/fixture/cvlc'), \
                patch.dict(vlc.os.environ, ACE_CONTENT_ID='0123456789abcdef0123456789abcdef01234567'), \
                patch('sys.argv', ['vlc_compare.py', '--engine-url', self.args().engine_url,
                                   '--outpace-url', self.args().outpace_url, '--seconds', '10']), \
                redirect_stdout(io.StringIO()):
            code = (vlc.main() if when == 'cleanup' else
                    vlc.compare(args, None, playback_urls=self.urls(), cancel_requested=lambda: cancelled[0]))
            report = json.loads(Path(root, 'comparison.json').read_text())
        for sig, handler in old.items():
            self.assertEqual(vlc.signal.getsignal(sig), handler)
        for process in processes:
            process.terminate.assert_called_once()
            process.wait.assert_called_once()
        self.assertEqual(code, 130)
        self.assertTrue(report['interrupted'])
        self.assertFalse(report['observation_complete'])
        if when == 'long_sleep':
            self.assertTrue(sleeps)
            self.assertLessEqual(max(sleeps), .25)

    def test_long_poll_wait_checks_cancel_in_bounded_slices(self):
        self.terminal_cancel('long_sleep')

    def test_cancel_in_final_sleep_is_reported(self):
        self.terminal_cancel('sleep')

    def test_cli_cancel_during_cleanup_is_reported_after_both_reaps(self):
        self.terminal_cancel('cleanup')

    def test_precancel_and_unsafe_parent_do_not_launch(self):
        with patch.object(vlc.tempfile, 'mkdtemp') as directory, patch.object(vlc.subprocess, 'run') as version:
            self.assertEqual(vlc.compare(self.args(), None, playback_urls=self.urls(), cancel_requested=lambda: True), 130)
            directory.assert_not_called()
            version.assert_not_called()
        with tempfile.TemporaryDirectory() as root:
            bad = Path(root, 'public')
            bad.mkdir()
            bad.chmod(0o755)
            link = Path(root, 'link'); link.symlink_to(Path(root), target_is_directory=True)
            for parent in [bad, link, Path(root, 'missing')]:
                with self.subTest(parent=parent), patch.object(vlc.tempfile, 'mkdtemp') as directory, patch.object(vlc.subprocess, 'run') as version:
                    with self.assertRaises(ValueError):
                        vlc.compare(self.args(), None, playback_urls=self.urls(), artifact_parent=parent)
                    directory.assert_not_called()
                    version.assert_not_called()

    def test_invalid_owned_inputs_fail_before_launch_or_artifact_creation(self):
        good = self.urls()
        bad = [dict(engine=good['engine']), dict(good, extra=good['engine'])]
        for role in good:
            for suffix in ('#', '?', '/extra', '?method=stop', '%2f', '\\x'):
                bad.append(dict(good, **{role: good[role] + suffix}))
            bad.append(dict(good, **{role: good[role].replace('127.0.0.1', '127.0.0.2')}))
            bad.append(dict(good, **{role: good[role].replace('/ace/r/', '/ace/cmd/')}))
            bad.append(dict(good, **{role: good[role][:-1]}))
        for urls in bad:
            with self.subTest(urls=urls), patch.object(vlc.tempfile, 'mkdtemp') as directory, \
                    patch.object(vlc.subprocess, 'run') as version:
                with self.assertRaises(ValueError):
                    vlc.compare(self.args(), None, playback_urls=urls)
                directory.assert_not_called()
                version.assert_not_called()


class ArgumentsTests(unittest.TestCase):
    def test_origins_reject_credentials_paths_and_query(self):
        for value in ("http://user:pass@localhost", "http://localhost/ace",
                      "http://localhost?id=secret", "file:///tmp/input",
                      "http://localhost:invalid", "http://localhost\n"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                vlc.server_base(value)
        self.assertEqual(vlc.server_base("http://127.0.0.1:6878/"), "http://127.0.0.1:6878")

    def test_duration_rejects_unbounded_values(self):
        for value in ("nan", "inf", "0", "-1"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                vlc.positive(value)


if __name__ == "__main__":
    unittest.main()
