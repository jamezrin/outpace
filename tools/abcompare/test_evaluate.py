"""Synthetic policy-gate tests for abcompare's offline evaluator."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import evaluate


HERE = Path(__file__).resolve().parent


def policy(**updates):
    result = {
        "schema_version": 1,
        "max_startup_delta_seconds": 2.0,
        "max_excess_modeled_stall_count": 1,
        "max_excess_modeled_stall_seconds": 3.0,
        "min_byte_ratio": 0.8,
        "timing_allowance_seconds": 0.05,
        "attestation": {
            "simultaneous": True,
            "same_current_public_source": True,
            "same_settings": True,
            "same_routes": True,
            "fresh_state": True,
            "viewers_observed": True,
            "warp_off": True,
            "direct_route_verified": True,
            "complete": True,
        },
        "provenance": {
            "candidate_build": "synthetic-build-1",
            "reference_version": "synthetic-reference-1",
            "reference_image_digest": "sha256:synthetic",
        },
    }
    result.update(updates)
    return result


def summary(**updates):
    result = {
        "schema_version": 1,
        "duration_seconds": 30.0,
        "player_prebuffer_seconds": 3.0,
        "streams": {
            "reference": {
                "bytes": 1000,
                "first_byte_seconds": 1.0,
                "last_byte_seconds": 29.0,
                "stale_prefix_suspect": None,
                "arrival_gaps": [],
                "pcr_jumps": [],
                "player": {"start": 5.0, "stalls": 2, "stall_time": 4.0,
                           "stall_list": [[10.0, 2.0], [20.0, 2.0]]},
            },
            "candidate": {
                "bytes": 1000,
                "first_byte_seconds": 1.0,
                "last_byte_seconds": 29.0,
                "stale_prefix_suspect": None,
                "arrival_gaps": [],
                "pcr_jumps": [],
                "player": {"start": 5.5, "stalls": 2, "stall_time": 4.0,
                           "stall_list": [[10.5, 2.0], [20.5, 2.0]]},
            },
        },
        "comparison": {
            "reference": "reference",
            "candidate": "candidate",
            "first_byte_candidate_minus_reference": 0.0,
            "identity_in_overlap": {
                "window_seconds": 20.0,
                "reference_packets": 100,
                "candidate_packets": 100,
                "reference_in_candidate": 1.0,
                "candidate_in_reference": 1.0,
            },
        },
    }
    result.update(updates)
    return result


class EvaluateTests(unittest.TestCase):
    _default = object()

    def result(self, data=_default, config=_default):
        return evaluate.evaluate(summary() if data is self._default else data,
                                 policy() if config is self._default else config)

    def test_pass_reports_version_outcome_reasons_and_metrics(self):
        result = self.result()
        self.assertEqual(result["schema_version"], 1)
        self.assertEqual(result["outcome"], "pass")
        self.assertIsInstance(result["reasons"], list)
        self.assertIsInstance(result["metrics"], dict)

    def test_inclusive_thresholds_include_explicit_timing_allowance(self):
        data = summary()
        data["streams"]["candidate"]["player"]["start"] = 7.05
        data["streams"]["candidate"]["player"]["stall_time"] = 7.05
        data["streams"]["candidate"]["player"]["stalls"] = 3
        self.assertEqual(self.result(data)["outcome"], "pass")

    def test_threshold_breaches_fail(self):
        cases = []
        data = summary()
        data["streams"]["candidate"]["bytes"] = 799
        cases.append((data, policy()))
        data = summary()
        data["streams"]["candidate"]["player"]["start"] = 7.051
        cases.append((data, policy()))
        data = summary()
        data["streams"]["candidate"]["player"]["stalls"] = 4
        cases.append((data, policy()))
        data = summary()
        data["streams"]["candidate"]["player"]["stall_time"] = 7.051
        cases.append((data, policy()))
        for data, config in cases:
            with self.subTest(data=data):
                self.assertEqual(self.result(data, config)["outcome"], "fail")

    def test_zero_candidate_bytes_and_missing_start_with_usable_reference_fail(self):
        data = summary()
        data["streams"]["candidate"]["bytes"] = 0
        data["streams"]["candidate"]["first_byte_seconds"] = None
        data["streams"]["candidate"]["player"] = None
        self.assertEqual(self.result(data)["outcome"], "fail")
        data = summary()
        data["streams"]["candidate"]["player"] = {
            "start": None, "stalls": None, "stall_time": None, "stall_list": None,
        }
        self.assertEqual(self.result(data)["outcome"], "fail")

    def test_null_metrics_are_inconclusive_when_reference_is_unusable(self):
        data = summary()
        data["streams"]["reference"]["player"]["start"] = None
        data["streams"]["reference"]["player"]["stalls"] = None
        data["streams"]["reference"]["player"]["stall_time"] = None
        self.assertEqual(self.result(data)["outcome"], "inconclusive")

    def test_missing_overlap_and_clock_discontinuities_are_inconclusive(self):
        data = summary()
        data["comparison"]["identity_in_overlap"] = None
        self.assertEqual(self.result(data)["outcome"], "inconclusive")
        data = summary()
        data["streams"]["candidate"]["pcr_jumps"] = [[8.0, 4.0]]
        self.assertEqual(self.result(data)["outcome"], "inconclusive")
        data = summary()
        data["streams"]["candidate"]["stale_prefix_suspect"] = [188, 80, 2.0]
        self.assertEqual(self.result(data)["outcome"], "inconclusive")

    def test_false_attestation_is_inconclusive_and_incomplete_attestation_is_error(self):
        config = policy()
        config["attestation"]["same_settings"] = False
        self.assertEqual(self.result(config=config)["outcome"], "inconclusive")
        config = policy()
        config["attestation"]["complete"] = False
        self.assertEqual(self.result(config=config)["outcome"], "error")

    def test_malformed_artifacts_and_incomplete_provenance_are_errors(self):
        for data in (None, {}, {"schema_version": 2},
                     summary(streams={"reference": {}, "candidate": {}})):
            with self.subTest(data=data):
                self.assertEqual(self.result(data)["outcome"], "error")

    def test_missing_required_metrics_are_errors(self):
        mutations = [
            lambda data: data["streams"]["candidate"].pop("bytes"),
            lambda data: data["streams"]["candidate"].pop("first_byte_seconds"),
            lambda data: data["streams"]["candidate"].pop("last_byte_seconds"),
            lambda data: data["streams"]["candidate"].pop("arrival_gaps"),
            lambda data: data["streams"]["candidate"].pop("pcr_jumps"),
            lambda data: data["streams"]["candidate"].pop("stale_prefix_suspect"),
            lambda data: data["streams"]["candidate"]["player"].pop("start"),
            lambda data: data["streams"]["candidate"]["player"].pop("stalls"),
            lambda data: data["streams"]["candidate"]["player"].pop("stall_time"),
            lambda data: data["streams"]["candidate"]["player"].pop("stall_list"),
            lambda data: data["comparison"].pop("identity_in_overlap"),
            lambda data: data["comparison"]["identity_in_overlap"].pop("window_seconds"),
            lambda data: data["comparison"]["identity_in_overlap"].pop("reference_packets"),
            lambda data: data["comparison"]["identity_in_overlap"].pop("candidate_packets"),
            lambda data: data["comparison"]["identity_in_overlap"].pop("reference_in_candidate"),
            lambda data: data["comparison"]["identity_in_overlap"].pop("candidate_in_reference"),
            lambda data: data.pop("player_prebuffer_seconds"),
        ]
        for mutate in mutations:
            data = summary()
            mutate(data)
            with self.subTest(data=data):
                self.assertEqual(self.result(data)["outcome"], "error")

    def test_nonfinite_and_boolean_numbers_are_not_accepted_as_metrics(self):
        for bad in (True, float("nan"), float("inf"), -1):
            data = summary()
            data["streams"]["candidate"]["bytes"] = bad
            with self.subTest(value=bad):
                self.assertEqual(self.result(data)["outcome"], "error")

    def test_policy_requires_exact_keys_finite_types_and_ranges(self):
        invalid = [policy(extra=True), {**policy(), "schema_version": 2},
                   policy(min_byte_ratio=0), policy(min_byte_ratio=1.1),
                   policy(timing_allowance_seconds=0.049),
                   policy(max_startup_delta_seconds=True),
                   policy(max_excess_modeled_stall_count=float("nan")),
                   policy(provenance={"candidate_build": "build"})]
        for config in invalid:
            with self.subTest(policy=config):
                self.assertEqual(self.result(config=config)["outcome"], "error")

    def test_threshold_sum_overflow_is_an_error(self):
        config = policy(max_startup_delta_seconds=1e308,
                        timing_allowance_seconds=1e308)
        result = self.result(config=config)
        self.assertEqual(result["outcome"], "error")
        self.assertEqual(result["reasons"], ["numeric_overflow"])

    def test_cli_exit_codes_and_writes_gate_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, config, output = root / "summary.json", root / "policy.json", root / "gate.json"
            source.write_text(json.dumps(summary()))
            config.write_text(json.dumps(policy()))
            command = [sys.executable, str(HERE / "evaluate.py"), str(source),
                       "--policy", str(config), "--output", str(output)]
            passed = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertEqual(passed.returncode, 0, passed.stderr)
            self.assertEqual(json.loads(output.read_text())["outcome"], "pass")
            data = summary()
            data["streams"]["candidate"]["bytes"] = 0
            source.write_text(json.dumps(data))
            failed = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertEqual(failed.returncode, 1, failed.stderr)
            self.assertEqual(json.loads(output.read_text())["outcome"], "fail")
            data = summary()
            data["streams"]["reference"]["player"]["start"] = None
            data["streams"]["reference"]["player"]["stalls"] = None
            data["streams"]["reference"]["player"]["stall_time"] = None
            source.write_text(json.dumps(data))
            inconclusive = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertEqual(inconclusive.returncode, 2, inconclusive.stderr)
            self.assertEqual(json.loads(output.read_text())["outcome"], "inconclusive")
            source.write_text("{")
            errored = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertEqual(errored.returncode, 2, errored.stderr)
            self.assertEqual(json.loads(output.read_text())["outcome"], "error")


if __name__ == "__main__":
    unittest.main()
