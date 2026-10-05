#!/usr/bin/env python3
"""Analyze simultaneous MPEG-TS captures; no decoder or registry access required."""
import argparse
import bisect
from collections import Counter, defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path

PACKET_SIZE = 188
PCR_PERIOD = (1 << 33) / 90000
PREBUF = 3.0
VIDEO_TYPES = {0x02, 0x1B, 0x24}


def parse_ts(data):
    """Parse TS headers and single-packet PSI; keep packet bytes for exact identity."""
    packets, pcrs = [], []
    rai, errors, duplicate_pids = defaultdict(list), Counter(), Counter()
    last, repeats, wraps, prev_pcr = {}, {}, defaultdict(float), {}
    pmt_pids, types = set(), {}
    lead, sync_losses, malformed, duplicates, transport_errors = 0, 0, 0, 0, 0
    i = 0

    def aligned(pos):
        # Require confirmation where available, but accept a one/two-packet capture.
        return (pos + 188 <= len(data) and data[pos] == 0x47 and
                all(data[j] == 0x47 for j in range(pos + 188, min(pos + 564, len(data)), 188)
                    if j + 188 <= len(data)))

    while i + 188 <= len(data) and not aligned(i):
        i += 1
    lead = i
    while i + 188 <= len(data):
        if data[i] != 0x47:
            sync_losses += 1
            i += 1
            while i + 188 <= len(data) and not aligned(i):
                i += 1
            last.clear()
            repeats.clear()
            continue
        pk = data[i:i + 188]
        pid = ((pk[1] & 31) << 8) | pk[2]
        afc, cc = (pk[3] >> 4) & 3, pk[3] & 15
        transport_errors += bool(pk[1] & 0x80)
        pay, disc = 4, False
        if afc == 0 or (afc in (2, 3) and (pk[4] > 183 or (afc == 3 and pk[4] > 182))):
            malformed += 1
            i += 188
            continue
        if afc in (2, 3):
            al = pk[4]
            pay = 5 + al
            if al:
                flags = pk[5]
                disc = bool(flags & 0x80)
                if flags & 0x40:
                    rai[pid].append(i)
                if flags & 0x10 and al >= 7:
                    b = pk[6:12]
                    base = (b[0] << 25) | (b[1] << 17) | (b[2] << 9) | (b[3] << 1) | (b[4] >> 7)
                    value = (base * 300 + ((b[4] & 1) << 8) + b[5]) / 27000000
                    if pid in prev_pcr and prev_pcr[pid] - value > PCR_PERIOD / 2:
                        wraps[pid] += PCR_PERIOD
                    prev_pcr[pid] = value
                    pcrs.append((i, value + wraps[pid], pid))
        if disc:
            last.pop(pid, None)
            repeats.pop(pid, None)
        if pid != 0x1fff and afc in (1, 3):
            if pid in last:
                old_cc, old_pk = last[pid]
                if cc == old_cc and pk == old_pk:
                    if repeats.get(pid, False):
                        errors[pid] += 1
                    duplicates += 1
                    duplicate_pids[pid] += 1
                    repeats[pid] = True
                else:
                    if cc != (old_cc + 1) & 15:
                        errors[pid] += 1
                    repeats[pid] = False
            last[pid] = (cc, pk)
        # Minimal PSI discovery: incomplete multi-packet sections are not decoded.
        if pk[1] & 0x40 and afc in (1, 3) and pay < 188:
            sec = pay + 1 + pk[pay]
            if sec + 8 <= 188:
                slen = ((pk[sec + 1] & 15) << 8) | pk[sec + 2]
                end = sec + 3 + slen - 4
                if end <= 188 and pid == 0 and pk[sec] == 0:
                    for k in range(sec + 8, end - 3, 4):
                        if pk[k] or pk[k + 1]:
                            pmt_pids.add(((pk[k + 2] & 31) << 8) | pk[k + 3])
                elif end <= 188 and pid in pmt_pids and pk[sec] == 2 and sec + 12 <= 188:
                    k = sec + 12 + (((pk[sec + 10] & 15) << 8) | pk[sec + 11])
                    while k + 5 <= end:
                        types[((pk[k + 1] & 31) << 8) | pk[k + 2]] = pk[k]
                        k += 5 + (((pk[k + 3] & 15) << 8) | pk[k + 4])
        packets.append((i, pid, pk))
        i += 188
    return dict(pkts=packets, pcrs=pcrs, rai=dict(rai), cc_err=dict(errors),
                lead=lead, sync_loss=sync_losses, malformed_packets=malformed,
                transport_errors=transport_errors, exact_duplicate_packets=duplicates,
                exact_duplicate_packets_by_pid=dict(duplicate_pids),
                exact_duplicate_media_packets=sum(count for pid, count in duplicate_pids.items()
                    if pid >= 0x20 and pid not in pmt_pids and pid != 0x1fff),
                stypes=types, pmt_pids=pmt_pids, trailing_bytes=len(data) - i)


def load_arrivals(rundir, name):
    path = Path(rundir, name + '.arrivals.csv')
    times, ends = [], []
    if path.exists():
        with path.open() as f:
            for row in csv.DictReader(f):
                t, size, offset = float(row['t']), int(row['nbytes']), int(row['offset'])
                if size <= 0 or offset != (ends[-1] if ends else 0) or (times and t < times[-1]):
                    raise ValueError(f'{name}: invalid arrival offsets/times')
                times.append(t)
                ends.append(offset + size)
    return times, ends


def analyze(rundir, name, duration):
    times, ends = load_arrivals(rundir, name)
    path = Path(rundir, name + '.ts')
    data = path.read_bytes() if path.exists() else b''
    ts = parse_ts(data)
    r = dict(name=name, bytes=len(data), t=times, end=ends, ts=ts, pcrs=[],
             pcr_jumps=[], stale_prefix=None, first_key_t=None, n_key=0,
             ttfb=times[0] if times else None, last=times[-1] if times else None,
             gaps=[], media_span=None)
    if not times:
        return r
    if ends[-1] != len(data):
        raise ValueError(f'{name}: capture size differs from arrival log')

    def arrival_of(offset):
        # The last byte of a packet must have arrived before the packet is usable.
        k = bisect.bisect_left(ends, offset + 188)
        return times[k] if k < len(times) else None

    r['gaps'] = [(a, b - a) for a, b in zip(times, times[1:]) if b - a > 1]
    if duration - times[-1] > 1:
        r['gaps'].append((times[-1], duration - times[-1]))
    counts = Counter(pid for _, _, pid in ts['pcrs'])
    pcr_pid = counts.most_common(1)[0][0] if counts else None
    pcrs = [(arrival_of(o), v, o) for o, v, pid in ts['pcrs'] if pid == pcr_pid]
    r['pcr_jumps'] = [(b[0], b[1] - a[1]) for a, b in zip(pcrs, pcrs[1:])
                      if not -0.5 < b[1] - a[1] < 3]
    for k in range(1, len(pcrs)):
        delta = pcrs[k][1] - pcrs[k - 1][1]
        if delta > 60:
            r['stale_prefix'] = (pcrs[k][2], delta, pcrs[k][0])
            pcrs = pcrs[k:]
            break
    r['pcrs_raw'] = pcrs
    maximum = float('-inf')
    for t, value, offset in pcrs:
        maximum = max(maximum, value)
        r['pcrs'].append((t, maximum, offset))
    if pcrs:
        r['media_span'] = maximum - min(v for _, v, _ in pcrs)
    video = next((pid for pid, kind in ts['stypes'].items() if kind in VIDEO_TYPES), None)
    if video is not None and ts['rai'].get(video):
        r['first_key_t'] = arrival_of(ts['rai'][video][0])
        r['n_key'] = len(ts['rai'][video])
    r['pcr_pid'] = pcr_pid
    return r


def frontier(r, wall):
    p = r.get('pcrs', [])
    k = bisect.bisect_right([x[0] for x in p], wall)
    return p[k - 1][1] if k else None


def player(r, duration, prebuf=PREBUF, dt=0.05):
    p = r.get('pcrs', [])
    if not p:
        return None
    arrival, values = [x[0] for x in p], [x[1] for x in p]
    head, start, playing, stall_at = values[0], None, False, None
    stalls, samples = [], []
    steps = max(0, int((duration - arrival[0]) / dt) + 1)
    for step in range(steps):
        wall = arrival[0] + step * dt
        k = bisect.bisect_right(arrival, wall)
        f = values[k - 1] if k else head
        if not playing:
            if f - head >= prebuf:
                playing = True
                if start is None:
                    start = wall
                elif stall_at is not None:
                    stalls.append((stall_at, wall - stall_at))
                    stall_at = None
        else:
            head += dt
            if head > f + 1e-9:
                head, playing, stall_at = f, False, wall
        samples.append((wall, head if start is not None else None))
    if stall_at is not None:
        stalls.append((stall_at, max(0, duration - stall_at)))
    return dict(start=start, stalls=len(stalls) if start is not None else None,
                stall_time=sum(s for _, s in stalls) if start is not None else None,
                stall_list=stalls if start is not None else None, disp=samples)


def media_packets(ts):
    # Reserved PSI/SI PIDs, PAT, discovered PMTs and null packets are not media.
    return [(o, pid, pk) for o, pid, pk in ts['pkts']
            if pid >= 0x20 and pid not in ts['pmt_pids'] and pid != 0x1fff]


def digest(packet):
    return hashlib.blake2b(packet, digest_size=12).digest()


def packet_identity(reference, candidate):
    index, ambiguous, total_reference = {}, set(), 0
    for rank, (_, _, pk) in enumerate(media_packets(reference)):
        total_reference += 1
        h = digest(pk)
        if h in index:
            ambiguous.add(h)
        index[h] = rank
    for h in ambiguous:
        index.pop(h)
    # Dict insertion order preserves first occurrence order. Rank only unique
    # packets: excluded ambiguous packets must not manufacture apparent holes.
    ambiguous_reference_packets = total_reference - len(index)
    index = {h: rank for rank, h in enumerate(index)}
    runs, matched, holes, backwards, skipped = [], 0, 0, 0, 0
    previous = None
    for _, _, pk in media_packets(candidate):
        h = digest(pk)
        if h in ambiguous:
            skipped += 1
            continue
        rank = index.get(h)
        if rank is not None:
            matched += 1
            if previous is not None:
                holes += max(0, rank - previous - 1)
                backwards += rank <= previous
            contiguous = bool(runs and runs[-1]['kind'] == 'hit' and rank == runs[-1]['end'] + 1)
            if contiguous:
                runs[-1]['end'] = rank
                runs[-1]['packets'] += 1
            else:
                runs.append(dict(kind='hit', start=rank, end=rank, packets=1))
            previous = rank
        else:
            if runs and runs[-1]['kind'] == 'miss':
                runs[-1]['packets'] += 1
            else:
                runs.append(dict(kind='miss', packets=1))
    considered = sum(run['packets'] for run in runs)
    return dict(matched_packets=matched, considered_packets=considered,
                ambiguous_packets_skipped=skipped, ambiguous_reference_packets=ambiguous_reference_packets,
                matched_fraction=matched / considered if considered else None,
                reference_holes=holes, backward_or_duplicate_steps=backwards, runs=runs)


def overlap_identity(a, b):
    if not a['pcrs'] or not b['pcrs']:
        return None
    lo = max(a['pcrs'][0][1], b['pcrs'][0][1]) + 1
    hi = min(a['pcrs'][-1][1], b['pcrs'][-1][1]) - 1
    if hi <= lo:
        return None

    def window(r):
        offsets = [x[2] for x in r['pcrs_raw']]
        values = [x[1] for x in r['pcrs_raw']]
        result = []
        for o, _, pk in media_packets(r['ts']):
            k = bisect.bisect_right(offsets, o)
            if k and lo <= values[k - 1] <= hi:
                result.append(digest(pk))
        return result

    wa, wb = window(a), window(b)
    if not wa or not wb:
        return None
    sa, sb = set(wa), set(wb)
    return dict(window_seconds=hi - lo, reference_packets=len(wa), candidate_packets=len(wb),
                reference_in_candidate=sum(h in sb for h in wa) / len(wa),
                candidate_in_reference=sum(h in sa for h in wb) / len(wb))


def display_at(pl, wall):
    if not pl or not pl['disp']:
        return None
    k = bisect.bisect_right([x[0] for x in pl['disp']], wall)
    return pl['disp'][k - 1][1] if k else None


def summarize(rundir, duration, names):
    results = {name: analyze(rundir, name, duration) for name in names}
    summary = dict(schema_version=1, duration_seconds=duration, player_prebuffer_seconds=PREBUF,
                   streams={}, comparison=None)
    for name, r in results.items():
        ts = r['ts']
        r['player'] = player(r, duration)
        pl = r['player']
        summary['streams'][name] = dict(
            bytes=r['bytes'], first_byte_seconds=r['ttfb'], last_byte_seconds=r['last'],
            first_video_rai_seconds=r['first_key_t'], video_rai_count=r['n_key'],
            media_span_seconds=r['media_span'], stale_prefix_suspect=r['stale_prefix'],
            arrival_gaps=r['gaps'], pcr_jumps=r['pcr_jumps'],
            ts=dict(leading_bytes=ts['lead'], trailing_bytes=ts['trailing_bytes'],
                    sync_losses=ts['sync_loss'], continuity_errors_by_pid=ts['cc_err'],
                    malformed_packets=ts['malformed_packets'], transport_errors=ts['transport_errors'],
                    exact_duplicate_packets=ts['exact_duplicate_packets'],
                    exact_duplicate_packets_by_pid=ts['exact_duplicate_packets_by_pid'],
                    exact_duplicate_media_packets=ts['exact_duplicate_media_packets'],
                    stream_types=ts['stypes']),
            player={k: v for k, v in pl.items() if k != 'disp'} if pl else None)
    if len(names) == 2:
        a, b = (results[n] for n in names)
        samples = []
        for w in range(10, int(duration) + 1, 20):
            fa, fb = frontier(a, w), frontier(b, w)
            da, db = display_at(a['player'], w), display_at(b['player'], w)
            samples.append(dict(wall_seconds=w,
                frontier_reference_minus_candidate=fa-fb if fa is not None and fb is not None else None,
                displayed_reference_minus_candidate=da-db if da is not None and db is not None else None,
                reference_buffer=fa-da if fa is not None and da is not None else None,
                candidate_buffer=fb-db if fb is not None and db is not None else None))
        summary['comparison'] = dict(reference=names[0], candidate=names[1],
            first_byte_candidate_minus_reference=b['ttfb']-a['ttfb'] if a['ttfb'] is not None and b['ttfb'] is not None else None,
            packet_mapping=packet_identity(a['ts'], b['ts']), identity_in_overlap=overlap_identity(a, b),
            frontier_samples=samples)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('rundir', type=Path)
    parser.add_argument('seconds', type=float)
    parser.add_argument('names', nargs='*', default=['engine', 'outpace'])
    args = parser.parse_args()
    if not math.isfinite(args.seconds) or args.seconds <= 0:
        parser.error('seconds must be positive')
    summary = summarize(args.rundir, args.seconds, args.names)
    path = args.rundir / 'summary.json'
    path.write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
    for name, stream in summary['streams'].items():
        print(f'{name}: {json.dumps(stream)}')
    if summary['comparison']:
        comparison = dict(summary['comparison'])
        comparison['packet_mapping'] = dict(comparison['packet_mapping'])
        runs = comparison['packet_mapping']['runs']
        comparison['packet_mapping']['runs'] = runs[:40]
        comparison['packet_mapping']['total_runs'] = len(runs)
        print('comparison:', json.dumps(comparison))
    print(f'Summary: {path}')


if __name__ == '__main__':
    main()
