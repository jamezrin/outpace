#!/usr/bin/env python3
"""Evaluate an existing abcompare summary against an explicit private policy."""
import argparse
import json
import math
from pathlib import Path

THRESHOLDS = {'max_startup_delta_seconds', 'max_excess_modeled_stall_count',
              'max_excess_modeled_stall_seconds', 'min_byte_ratio', 'timing_allowance_seconds'}
ATTESTATIONS = {'simultaneous', 'same_current_public_source', 'same_settings',
                'same_routes', 'fresh_state', 'viewers_observed', 'warp_off',
                'direct_route_verified', 'complete'}
PROVENANCE = {'candidate_build', 'reference_version', 'reference_image_digest'}


def number(value, *, positive=False, integer=False):
    if (type(value) not in (int, float) or not math.isfinite(value) or
            value < 0 or (positive and value == 0) or
            (integer and int(value) != value)):
        raise ValueError('invalid numeric metric')
    return value


def exact_keys(value, keys):
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError('invalid object keys')


def version(value):
    if type(value['schema_version']) is not int or value['schema_version'] != 1:
        raise ValueError('unsupported schema')


def evaluate(summary, policy):
    """Return gate schema 1. Diagnostics never echo untrusted artifact text."""
    metrics = {}

    def result(outcome, *reasons):
        return dict(schema_version=1, outcome=outcome, reasons=list(reasons), metrics=metrics)

    try:
        exact_keys(policy, THRESHOLDS | {'schema_version', 'attestation', 'provenance'})
        version(policy)
        for key in THRESHOLDS:
            number(policy[key])
        if not 0 < policy['min_byte_ratio'] <= 1 or policy['timing_allowance_seconds'] < .05:
            raise ValueError('invalid policy range')
        exact_keys(policy['attestation'], ATTESTATIONS)
        if any(type(v) is not bool for v in policy['attestation'].values()):
            raise ValueError('invalid attestation')
        exact_keys(policy['provenance'], PROVENANCE)
        if any(not isinstance(v, str) or not v.strip() for v in policy['provenance'].values()):
            raise ValueError('missing provenance')
        version(summary)
        duration = number(summary['duration_seconds'], positive=True)
        if number(summary['player_prebuffer_seconds']) != 3:
            raise ValueError('unsupported player model')
        comparison = summary['comparison']
        ref_name, cand_name = comparison['reference'], comparison['candidate']
        if not isinstance(ref_name, str) or not isinstance(cand_name, str) or ref_name == cand_name:
            raise ValueError('invalid comparison')
        streams = [summary['streams'][name] for name in (ref_name, cand_name)]
        for stream in streams:
            number(stream['bytes'], integer=True)
            for key in ('first_byte_seconds', 'last_byte_seconds'):
                if stream[key] is not None:
                    if number(stream[key]) > duration:
                        raise ValueError('metric outside observation')
            if stream['bytes'] and (stream['first_byte_seconds'] is None or stream['last_byte_seconds'] is None):
                raise ValueError('missing delivery timing')
            for key in ('arrival_gaps', 'pcr_jumps'):
                if not isinstance(stream[key], list):
                    raise ValueError('invalid events')
                for pair in stream[key]:
                    if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                        raise ValueError('invalid event')
                    number(pair[0])
                    if type(pair[1]) not in (int, float) or not math.isfinite(pair[1]):
                        raise ValueError('invalid event timing')
            stale = stream['stale_prefix_suspect']
            if stale is not None:
                if not isinstance(stale, (list, tuple)) or len(stale) != 3:
                    raise ValueError('invalid stale prefix')
                for value in stale:
                    number(value)
            player = stream['player']
            if player is not None:
                for key in ('start', 'stalls', 'stall_time', 'stall_list'):
                    player[key]
                if player['start'] is not None:
                    if number(player['start']) > duration:
                        raise ValueError('startup outside observation')
                    number(player['stalls'], integer=True)
                    number(player['stall_time'])
                    if not isinstance(player['stall_list'], list):
                        raise ValueError('missing stall events')
                    for pair in player['stall_list']:
                        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                            raise ValueError('invalid stall event')
                        for value in pair:
                            number(value)
        overlap = comparison['identity_in_overlap']
        if overlap is not None:
            number(overlap['window_seconds'], positive=True)
            for key in ('reference_packets', 'candidate_packets'):
                number(overlap[key], positive=True, integer=True)
            for key in ('reference_in_candidate', 'candidate_in_reference'):
                if number(overlap[key]) > 1:
                    raise ValueError('invalid overlap ratio')
        if not policy['attestation']['complete']:
            return result('error', 'incomplete_observation')
        if not all(policy['attestation'].values()):
            return result('inconclusive', 'comparability_not_established')
        reference, candidate = streams
        if any(s['pcr_jumps'] or s['stale_prefix_suspect'] is not None for s in streams):
            return result('inconclusive', 'suspect_clock')
        rp, cp = reference['player'], candidate['player']
        if not reference['bytes'] or rp is None or rp['start'] is None:
            return result('inconclusive', 'unusable_reference')
        if not candidate['bytes']:
            return result('fail', 'candidate_zero_delivery')
        if cp is None:
            return result('inconclusive', 'candidate_missing_pcr_model')
        if cp['start'] is None:
            return result('fail', 'candidate_modeled_no_start')
        if overlap is None:
            return result('inconclusive', 'missing_pcr_overlap')
        metrics.update(startup_delta_seconds=cp['start'] - rp['start'],
                       excess_modeled_stall_count=cp['stalls'] - rp['stalls'],
                       excess_modeled_stall_seconds=cp['stall_time'] - rp['stall_time'],
                       byte_ratio=candidate['bytes'] / reference['bytes'],
                       reference_arrival_gap_count=len(reference['arrival_gaps']),
                       candidate_arrival_gap_count=len(candidate['arrival_gaps']))
        allowance = policy['timing_allowance_seconds']
        limits = [('startup_delta_seconds', policy['max_startup_delta_seconds'] + allowance),
                  ('excess_modeled_stall_count', policy['max_excess_modeled_stall_count']),
                  ('excess_modeled_stall_seconds', policy['max_excess_modeled_stall_seconds'] + allowance)]
        if any(not math.isfinite(value) for value in metrics.values()) or any(
                not math.isfinite(limit) for _, limit in limits):
            metrics.clear()
            return result('error', 'numeric_overflow')
        breaches = [key + '_exceeded' for key, limit in limits if metrics[key] > limit]
        if metrics['byte_ratio'] < policy['min_byte_ratio']:
            breaches.append('byte_ratio_below_minimum')
        return result('fail', *breaches) if breaches else result('pass', 'all_thresholds_met')
    except (KeyError, TypeError, ValueError, OverflowError):
        return result('error', 'invalid_artifact_or_policy')


def load(path):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate JSON key')
            value[key] = item
        return value
    return json.loads(path.read_text(), object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('summary', type=Path)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        gate = evaluate(load(args.summary), load(args.policy))
    except (OSError, ValueError, UnicodeError):
        gate = dict(schema_version=1, outcome='error', reasons=['unreadable_or_invalid_json'], metrics={})
    try:
        args.output.write_text(json.dumps(gate, indent=2, allow_nan=False) + '\n')
    except OSError:
        return 2
    return {'pass': 0, 'fail': 1, 'inconclusive': 2, 'error': 2}[gate['outcome']]


if __name__ == '__main__':
    raise SystemExit(main())
