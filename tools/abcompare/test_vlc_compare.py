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
