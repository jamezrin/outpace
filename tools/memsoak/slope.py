#!/usr/bin/env python3
"""Summarize a memsoak run: RSS slope, plateau, per-stream liveness.

Usage: slope.py <results_dir>
Reads process.csv and streams.csv; prints a human summary and writes summary.txt.
"""
import csv
import sys
from pathlib import Path


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load(path):
    if not path.exists():
        return []
    with path.open() as fh:
        return list(csv.DictReader(fh))


def linfit(xs, ys):
    """Least-squares slope (y per unit x) and simple stats."""
    pts = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    n = len(pts)
    if n < 2:
        return None
    sx = sum(p[0] for p in pts)
    sy = sum(p[1] for p in pts)
    sxx = sum(p[0] * p[0] for p in pts)
    sxy = sum(p[0] * p[1] for p in pts)
    denom = n * sxx - sx * sx
    if denom == 0:
        return None
    return (n * sxy - sx * sy) / denom


def main():
    if len(sys.argv) != 2:
        print("usage: slope.py <results_dir>", file=sys.stderr)
        sys.exit(2)
    d = Path(sys.argv[1])
    proc = load(d / "process.csv")
    streams = load(d / "streams.csv")
    out = []

    def emit(s):
        out.append(s)
        print(s)

    emit(f"== memsoak summary: {d.name} ==")
    if proc:
        el = [_f(r["elapsed_s"]) for r in proc]
        rss = [_f(r["rss_kb"]) for r in proc]
        rss_v = [v for v in rss if v is not None]
        if rss_v:
            first, last, peak = rss[0], rss[-1], max(rss_v)
            slope_kb_s = linfit(el, rss)  # kB per second
            emit(f"samples: {len(proc)}  duration: {el[-1]:.0f}s")
            emit(f"RSS  first={first/1024:.1f}MB  last={last/1024:.1f}MB  "
                 f"peak={peak/1024:.1f}MB  delta={(last-first)/1024:+.1f}MB")
            if slope_kb_s is not None:
                emit(f"RSS slope: {slope_kb_s*60/1024:+.2f} MB/min "
                     f"({slope_kb_s*3600/1024:+.1f} MB/hr)")
            # plateau check: slope over last third
            third = max(2, len(proc) // 3)
            s_tail = linfit(el[-third:], rss[-third:])
            if s_tail is not None:
                emit(f"RSS slope (last third): {s_tail*60/1024:+.2f} MB/min  "
                     f"-> {'PLATEAU' if abs(s_tail*3600/1024) < 20 else 'STILL GROWING'}")
        # heap truth layer, if present
        ma = [_f(r.get("mem_allocated")) for r in proc]
        mr = [_f(r.get("mem_resident")) for r in proc]
        if any(v is not None for v in ma):
            a_slope = linfit(el, ma)
            emit(f"jemalloc allocated slope: "
                 f"{(a_slope*60/1024/1024 if a_slope else 0):+.2f} MB/min  "
                 f"(real-leak signal)")
            if any(v is not None for v in mr):
                gap = [(m - a) for m, a in zip(mr, ma) if m is not None and a is not None]
                if gap:
                    emit(f"resident-allocated gap: first={gap[0]/1048576:.1f}MB "
                         f"last={gap[-1]/1048576:.1f}MB  (fragmentation/retention signal)")
        threads = [_f(r["threads"]) for r in proc if _f(r["threads"]) is not None]
        fds = [_f(r["fds"]) for r in proc if _f(r["fds"]) is not None]
        if threads:
            emit(f"threads: {threads[0]:.0f} -> {threads[-1]:.0f} (max {max(threads):.0f})")
        if fds:
            emit(f"fds: {fds[0]:.0f} -> {fds[-1]:.0f} (max {max(fds):.0f})")
    else:
        emit("no process.csv samples")

    # per-stream liveness
    by_id = {}
    for r in streams:
        by_id.setdefault(r["id"], []).append(r)
    for sid, rows in by_id.items():
        frames = [_f(r["frames"]) for r in rows]
        frames_v = [f for f in frames if f is not None]
        codes = {r["http_code"] for r in rows}
        peers = [_f(r["peers"]) for r in rows if _f(r["peers"]) is not None]
        grew = frames_v and frames_v[-1] > (frames_v[0] or 0)
        emit(f"stream {sid[:12]}..: frames {frames_v[0] if frames_v else '?'}"
             f"->{frames_v[-1] if frames_v else '?'} "
             f"{'DECODING' if grew else 'STALLED/NO-DATA'}  "
             f"codes={sorted(codes)}  peers~{max(peers) if peers else 0:.0f}")

    (d / "summary.txt").write_text("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
