#!/usr/bin/env python3
"""Simultaneously capture live HTTP MPEG-TS and arrival/stat samples.

Usage: ab_capture.py ID SECONDS OUTDIR name=[nativecid:|native:]BASE ...
Identifiers come exclusively from argv; no registry access.
"""
import argparse
import csv
import http.client
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import threading
import time
import urllib.parse

REDIRECTS = {301, 302, 303, 307, 308}


def engine_spec(value):
    name, sep, base = value.partition('=')
    if not sep or not re.fullmatch(r'[A-Za-z0-9_-]+', name):
        raise argparse.ArgumentTypeError('engine spec must be safe-name=BASE')
    mode = 'compat'
    for prefix in ('nativecid:', 'native:'):
        if base.startswith(prefix):
            mode, base = prefix[:-1], base[len(prefix):]
            break
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.query or parsed.fragment:
        raise argparse.ArgumentTypeError('BASE must be an HTTP(S) URL without query/fragment')
    return name, mode, base.rstrip('/')


class Capture:
    def __init__(self, identifier, duration, outdir, specs, read_timeout=60, poll_interval=5):
        self.identifier, self.duration, self.outdir = identifier, duration, Path(outdir)
        self.specs, self.read_timeout, self.poll_interval = specs, read_timeout, poll_interval
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.active = set()
        self.t0 = None

    def now(self):
        return time.monotonic() - self.t0

    def timeout(self, maximum=None):
        remaining = self.duration - self.now()
        if remaining <= 0 or self.stop.is_set():
            raise TimeoutError('capture deadline')
        return min(remaining, self.read_timeout if maximum is None else maximum)

    def log(self, name, message):
        line = f'[{self.now():8.3f}] {name}: {message}'
        print(line, flush=True)
        with self.outdir.joinpath(name + '.events.txt').open('a') as f:
            f.write(line + '\n')

    def close(self, conn):
        conn.close()
        with self.lock:
            self.active.discard(conn)

    def open_url(self, url, name, maximum=None):
        for hop in range(6):
            u = urllib.parse.urlsplit(url)
            if u.scheme not in ('http', 'https') or not u.hostname:
                raise ValueError('unsupported HTTP URL')
            cls = http.client.HTTPSConnection if u.scheme == 'https' else http.client.HTTPConnection
            conn = cls(u.hostname, u.port, timeout=self.timeout(maximum))
            with self.lock:
                self.active.add(conn)
            try:
                path = urllib.parse.urlunsplit(('', '', u.path or '/', u.query, ''))
                conn.request('GET', path)
                resp = conn.getresponse()
                self.log(name, f'HTTP {resp.status} url={url}')
                if resp.status in REDIRECTS:
                    location = resp.getheader('Location')
                    if not location:
                        raise ValueError('redirect missing Location')
                    self.close(conn)
                    url = urllib.parse.urljoin(url, location)
                    continue
                # HTTPConnection releases its socket for Connection: close. Track the
                # response-owned socket as well so the deadline can interrupt read1.
                with self.lock:
                    self.active.add(resp)
                return conn, resp
            except Exception:
                self.close(conn)
                raise
        raise ValueError('too many redirects')

    def finish_response(self, conn, resp):
        resp.close()
        with self.lock:
            self.active.discard(resp)
        self.close(conn)

    def get_json(self, url, name, maximum=None):
        conn, resp = self.open_url(url, name, maximum)
        try:
            if resp.status != 200:
                raise ValueError(f'HTTP {resp.status}')
            parts, size = [], 0
            while True:
                self.set_response_timeout(resp, maximum)
                chunk = resp.read1(65536)
                if not chunk:
                    break
                size += len(chunk)
                if size > 2 * 1024 * 1024:
                    raise ValueError('JSON response exceeds 2 MiB')
                parts.append(chunk)
            return json.loads(b''.join(parts))
        finally:
            self.finish_response(conn, resp)

    def set_response_timeout(self, resp, maximum=None):
        timeout = self.timeout(maximum)
        if resp.fp is not None:
            resp.fp.raw._sock.settimeout(timeout)

    def getstream(self, name, mode, base):
        identifier = urllib.parse.quote(self.identifier, safe='')
        if mode in ('native', 'nativecid'):
            route = 'cid:' + identifier if mode == 'nativecid' else identifier
            return f'{base}/streams/ace/{route}', f'{base}/streams/ace/{route}/status'
        url = f'{base}/ace/getstream?' + urllib.parse.urlencode({'id': self.identifier, 'format': 'json'})
        while not self.stop.is_set() and self.now() < self.duration:
            try:
                body = self.get_json(url, name)
                response = body.get('response') or {}
                if response.get('playback_url'):
                    return (urllib.parse.urljoin(url, response['playback_url']),
                            urllib.parse.urljoin(url, response['stat_url']) if response.get('stat_url') else None)
                self.log(name, f'no playback_url: {body}')
            except Exception as exc:
                self.log(name, f'getstream error: {exc!r}')
            self.stop.wait(min(2, max(0, self.duration - self.now())))
        return None, None

    def reader(self, name, mode, base, state):
        offset, attempt = 0, 0
        with self.outdir.joinpath(name + '.ts').open('wb') as raw, \
                self.outdir.joinpath(name + '.arrivals.csv').open('w', newline='') as arrivals:
            writer = csv.writer(arrivals)
            writer.writerow(['t', 'nbytes', 'offset'])
            playback, state['stat_url'] = self.getstream(name, mode, base)
            if not playback:
                return
            while not self.stop.is_set() and self.now() < self.duration:
                attempt += 1
                conn, resp = None, None
                try:
                    start = self.now()
                    conn, resp = self.open_url(playback, name)
                    self.log(name, f'connect#{attempt} headers_after={self.now()-start:.3f}s')
                    if resp.status != 200:
                        raise ValueError(f'playback HTTP {resp.status}')
                    while not self.stop.is_set():
                        self.set_response_timeout(resp)
                        chunk = resp.read1(65536)
                        at = self.now()
                        if not chunk:
                            self.log(name, 'EOF')
                            break
                        if at >= self.duration:
                            break
                        if offset == 0:
                            self.log(name, f'first byte at {at:.3f}s')
                        raw.write(chunk)
                        writer.writerow([f'{at:.6f}', len(chunk), offset])
                        arrivals.flush()
                        offset += len(chunk)
                except Exception as exc:
                    self.log(name, f'read error: {exc!r}')
                finally:
                    if resp is not None:
                        self.finish_response(conn, resp)
                    elif conn is not None:
                        self.close(conn)
                self.stop.wait(min(1, max(0, self.duration - self.now())))
        self.log(name, f'done, {offset} bytes')

    def poller(self, name, state):
        with self.outdir.joinpath(name + '.stats.jsonl').open('w') as out:
            while not self.stop.wait(min(self.poll_interval, max(0, self.duration - self.now()))):
                if self.now() >= self.duration:
                    break
                url = state.get('stat_url')
                if not url:
                    continue
                try:
                    row = dict(t=round(self.now(), 6), stat=self.get_json(url, name, maximum=5))
                except Exception as exc:
                    row = dict(t=round(self.now(), 6), error=repr(exc))
                out.write(json.dumps(row) + '\n')
                out.flush()

    def run(self):
        # Only create the requested final directory. Existing parent directories
        # belong to the operator; never create/chmod a chain of unrelated paths.
        try:
            self.outdir.mkdir(mode=0o700)
        except FileExistsError:
            pass
        info = self.outdir.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or
                stat.S_IMODE(info.st_mode) & 0o777 != 0o700):
            raise ValueError('output must be a non-symlink directory owned by the caller with mode 0700')
        expected = ['capture.json'] + [name + suffix for name, _, _ in self.specs
                    for suffix in ('.ts', '.arrivals.csv', '.events.txt', '.stats.jsonl')]
        if any(self.outdir.joinpath(p).exists() or self.outdir.joinpath(p).is_symlink() for p in expected):
            raise ValueError('capture artifacts already exist; use a fresh directory')
        # Reserve every capture artifact exclusively and privately before threads
        # write. Ordinary later opens preserve these modes, including event append.
        for name in expected:
            fd = os.open(self.outdir / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(fd)
        threads = []
        self.t0 = time.monotonic()
        for name, mode, base in self.specs:
            state = {}
            threads.extend([threading.Thread(target=self.reader, args=(name, mode, base, state), daemon=True),
                            threading.Thread(target=self.poller, args=(name, state), daemon=True)])
        self.outdir.joinpath('capture.json').write_text(json.dumps(dict(
            schema_version=1, duration_seconds=self.duration, read_timeout_seconds=self.read_timeout,
            engines=[dict(name=n, mode=m, base=b) for n, m, b in self.specs]), indent=2) + '\n')
        try:
            for thread in threads:
                thread.start()
            self.stop.wait(max(0, self.duration - self.now()))
        finally:
            self.stop.set()
            with self.lock:
                active = list(self.active)
            for obj in active:
                if isinstance(obj, http.client.HTTPConnection):
                    sock = obj.sock
                else:
                    # A worker may close its response after the snapshot.
                    fp = obj.fp
                    sock = getattr(getattr(fp, 'raw', None), '_sock', None)
                if sock:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            for thread in threads:
                thread.join(timeout=2)
            if any(thread.is_alive() for thread in threads):
                raise RuntimeError('HTTP worker did not stop at capture deadline')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('identifier')
    parser.add_argument('seconds', type=float)
    parser.add_argument('outdir', type=Path)
    parser.add_argument('engines', nargs='+', type=engine_spec)
    args = parser.parse_args()
    read_timeout = float(os.environ.get('READ_TIMEOUT', '60'))
    if not math.isfinite(args.seconds) or args.seconds <= 0 or not math.isfinite(read_timeout) or read_timeout <= 0:
        parser.error('seconds and READ_TIMEOUT must be finite and positive')
    if not re.fullmatch('[0-9a-fA-F]{40}', args.identifier):
        parser.error('identifier must be 40 hex characters')
    if len({spec[0] for spec in args.engines}) != len(args.engines):
        parser.error('engine names must be unique')
    Capture(args.identifier, args.seconds, args.outdir, args.engines, read_timeout).run()


if __name__ == '__main__':
    main()
