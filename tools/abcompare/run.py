#!/usr/bin/env python3
"""Start isolated engines, capture simultaneously, analyze, clean up owned clients."""
import argparse
import json
import math
import os
import re
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

import ab_analyze
import ab_capture


def available_ports(ports):
    sockets = []
    try:
        for port, udp in ports:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM if udp else socket.SOCK_STREAM)
            sockets.append(sock)
            sock.bind(('0.0.0.0', port))
    finally:
        for sock in sockets:
            sock.close()


def wait_ready(url, process, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f'client exited before API readiness (exit {process.returncode})')
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(.2)
    raise TimeoutError(f'API did not become ready: {url}')


def stop_process(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--id', default=os.environ.get('ACE_CONTENT_ID'), help='content id (or ACE_CONTENT_ID env)')
    parser.add_argument('--seconds', type=float, default=180)
    parser.add_argument('--outpace', type=Path, required=True, help='already built candidate binary')
    parser.add_argument('--outdir', type=Path, help='fresh directory beneath the system temporary directory')
    parser.add_argument('--image', default='swarmtest-engine:latest')
    parser.add_argument('--candidate-port', type=int, default=16879)
    parser.add_argument('--peer-port', type=int, default=18622)
    parser.add_argument('--rtmp-port', type=int, default=19350)
    args = parser.parse_args()
    if not args.id or not re.fullmatch('[0-9a-fA-F]{40}', args.id):
        parser.error('supply a 40-hex content id via --id or ACE_CONTENT_ID')
    if not math.isfinite(args.seconds) or args.seconds <= 0:
        parser.error('--seconds must be finite and positive')
    binary = args.outpace.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error('--outpace must be an executable binary')
    # The original image uses its standard API/peer ports; fail rather than attach
    # to an existing client and accidentally reuse cached state.
    ports = [6878, 8621, args.candidate_port, args.peer_port, args.rtmp_port]
    if any(not 1 <= port <= 65535 for port in ports) or len(set(ports)) != len(ports):
        parser.error('ports must be distinct and in 1..65535')
    available_ports([(port, False) for port in ports] + [(8621, True), (args.peer_port, True)])
    if args.outdir:
        outdir = args.outdir.resolve()
        if not outdir.is_relative_to(Path(tempfile.gettempdir()).resolve()):
            parser.error('--outdir must be beneath the system temporary directory (private live artifacts)')
        outdir.mkdir(parents=True, exist_ok=False)
    else:
        outdir = Path(tempfile.mkdtemp(prefix='outpace-abcompare-'))
    outdir.chmod(0o700)
    data = outdir / 'candidate-state'
    data.mkdir()
    name = 'abcompare-' + uuid.uuid4().hex[:12]
    env = os.environ.copy()
    # Retain playback tuning explicitly supplied by the operator. Isolate all
    # local storage/listeners, including a possible inherited disk-cache path.
    env.update(OUTPACE_BIND=f'127.0.0.1:{args.candidate_port}',
               OUTPACE_PEER_LISTEN=f'0.0.0.0:{args.peer_port}',
               OUTPACE_RTMP_BIND=f'127.0.0.1:{args.rtmp_port}',
               OUTPACE_DATA_DIR=str(data), OUTPACE_CACHE_DIR=str(data / 'cache'))
    settings = {key: value for key, value in env.items() if key.startswith('OUTPACE_')}
    # Env overrides may contain private trackers/peers; this file is raw evidence.
    (outdir / 'run.json').write_text(json.dumps(dict(candidate_binary=str(binary),
        image=args.image, container=name, settings=settings), indent=2) + '\n')
    candidate, engine = None, None
    def interrupted(*_):
        raise KeyboardInterrupt()

    old_term = signal.signal(signal.SIGTERM, interrupted)
    print(f'Private artifacts: {outdir}', flush=True)
    try:
        with (outdir / 'engine.log').open('wb') as engine_log, (outdir / 'outpace.log').open('wb') as candidate_log:
            engine = subprocess.Popen(['docker', 'run', '--rm', '--name', name, '--network', 'host',
                args.image], stdout=engine_log, stderr=subprocess.STDOUT)
            candidate = subprocess.Popen([str(binary), 'serve'], env=env,
                stdout=candidate_log, stderr=subprocess.STDOUT)
            wait_ready('http://127.0.0.1:6878/webui/api/service?method=get_version', engine)
            wait_ready(f'http://127.0.0.1:{args.candidate_port}/healthz', candidate)
            specs = [('engine', 'compat', 'http://127.0.0.1:6878'),
                     ('outpace', 'nativecid', f'http://127.0.0.1:{args.candidate_port}')]
            read_timeout = float(os.environ.get('READ_TIMEOUT', '60'))
            if not math.isfinite(read_timeout) or read_timeout <= 0:
                raise ValueError('READ_TIMEOUT must be finite and positive')
            ab_capture.Capture(args.id, args.seconds, outdir, specs, read_timeout).run()
            summary = ab_analyze.summarize(outdir, args.seconds, ['engine', 'outpace'])
            (outdir / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
            print(f'Analysis: {outdir / "summary.json"}', flush=True)
    finally:
        stop_process(candidate)
        if engine is not None:
            # Exact unique container name; never global pkill or docker prune.
            subprocess.run(['docker', 'stop', '--time', '5', name], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=20, check=False)
            stop_process(engine)
        signal.signal(signal.SIGTERM, old_term)


if __name__ == '__main__':
    main()
