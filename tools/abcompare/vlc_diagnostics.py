"""Private bounded receipt capture. No emission or delivery timing inference."""
import base64
import json
import os
import selectors
import threading
import time

MAX_COUNT = (1 << 63) - 1
SELECTED = ('readbytes', 'inputreadpackets', 'demuxreadbytes', 'decodedvideo', 'decodedaudio')


class Budget:
    def __init__(self, probe=False):
        self.data = bytearray()
        self.limit = 65536 if probe else 1048576
        self.record_limit = 64 if probe else 4096
        self.records = 0
        self.retention_exhausted = False
        self.counts_saturated = False
        self.bytes_read = self.reads = self.eof_streams = self.dropped_records = 0
        self.forced_close = False
        self.launch_seconds = None

    def add(self, key, value=1):
        result = getattr(self, key) + value
        self.counts_saturated |= result > MAX_COUNT
        setattr(self, key, min(result, MAX_COUNT))

    def retain(self, record):
        encoded = (json.dumps(record, separators=(',', ':'), allow_nan=False) + '\n').encode()
        if (len(encoded) > 16384 or self.records >= self.record_limit
                or len(self.data) + len(encoded) > self.limit):
            self.retention_exhausted = True
            self.add('dropped_records')
            return
        self.data.extend(encoded)
        self.records += 1


class Capture:
    def __init__(self, role, stream, origin, budget=None):
        self.role, self.stream, self.origin = role, stream, origin
        self.budget = budget if budget is not None else Budget()
        self.data = self.budget.data
        self.prefix = bytearray()
        self.length = self.sequence = 0
        self.first = self.last = None

    def feed(self, raw, receipt, endpoint):
        self.budget.add('bytes_read', len(raw))
        self.budget.add('reads')
        if self.budget.retention_exhausted:
            self.prefix.clear()
            return
        offset = 0
        while offset < len(raw):
            end = raw.find(b'\n', offset)
            stop = len(raw) if end < 0 else end + 1
            part = raw[offset:stop]
            if self.first is None:
                self.first = receipt
            self.last = receipt
            self.prefix.extend(part[:max(0, 4096 - len(self.prefix))])
            self.budget.counts_saturated |= self.length + len(part) > MAX_COUNT
            self.length = min(MAX_COUNT, self.length + len(part))
            if end >= 0:
                self.emit(True, endpoint)
                if self.budget.retention_exhausted:
                    return
            offset = stop

    def emit(self, complete, endpoint):
        self.budget.counts_saturated |= self.sequence == MAX_COUNT
        self.sequence = min(MAX_COUNT, self.sequence + 1)
        self.budget.retain({
            'role': self.role, 'stream': self.stream, 'sequence': self.sequence,
            'first_receipt_seconds': self.first - self.origin,
            'last_receipt_seconds': self.last - self.origin,
            'phase': ('probe' if self.role == 'probe' else
                      'cleanup' if endpoint is not None and self.last >= endpoint else 'observation'),
            'raw_base64': base64.b64encode(self.prefix).decode('ascii'),
            'complete_line': complete, 'oversized': self.length > 4096,
            'omitted_bytes': max(0, self.length - len(self.prefix)),
        })
        self.prefix.clear()
        self.length = 0
        self.first = self.last = None

    def eof(self, receipt, endpoint):
        self.budget.add('eof_streams')
        if self.length and not self.budget.retention_exhausted:
            self.emit(False, endpoint)


class Collector:
    """One selector owner; attach and finish serialize only bounded state."""
    def __init__(self, origin):
        self.origin = origin
        self.endpoint = None
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.failed = threading.Event()
        self.budgets = {}
        self.data = {}
        self.pipes = {}
        self.selector = selectors.DefaultSelector()
        self.thread = threading.Thread(target=self._run, name='vlc-diagnostics')
        try:
            self.thread.start()
        except BaseException:
            self.selector.close()
            raise

    def attach(self, role, process):
        pipes = [process.stdout, process.stderr]
        try:
            with self.lock:
                if self.failed.is_set() or self.stop.is_set():
                    raise OSError('capture unavailable')
                budget = self.budgets.setdefault(role, Budget(role == 'probe'))
                budget.launch_seconds = time.monotonic() - self.origin
                self.data[role] = budget.data
                for stream, pipe in zip(('stdout', 'stderr'), pipes):
                    os.set_blocking(pipe.fileno(), False)
                    capture = Capture(role, stream, self.origin, budget)
                    self.selector.register(pipe, selectors.EVENT_READ, capture)
                    self.pipes[pipe] = capture
        except BaseException:
            self.failed.set()
            with self.lock:
                for pipe in pipes:
                    if pipe in self.pipes:
                        self._close(pipe, True)
                    elif pipe is not None:
                        try:
                            pipe.close()
                        except BaseException:
                            self.failed.set()
            raise

    def _close(self, pipe, forced=False):
        capture = self.pipes[pipe]
        capture.budget.forced_close |= forced
        try:
            self.selector.unregister(pipe)
        except BaseException:
            self.failed.set()
        try:
            pipe.close()
        except BaseException:
            self.failed.set()
        # Keep uncertain ownership visible rather than forgetting an open fd.
        if pipe.closed:
            self.pipes.pop(pipe, None)


    def _run(self):
        try:
            while not self.stop.is_set():
                # Do not hold attach lock across select or a blocking operation.
                ready = self.selector.select(0.05)
                for key, _ in ready:
                    if self.stop.is_set():
                        break
                    with self.lock:
                        if key.fileobj not in self.pipes:
                            continue
                        try:
                            raw = os.read(key.fd, 8192)
                            receipt = time.monotonic()
                        except BlockingIOError:
                            continue
                        if raw:
                            key.data.feed(raw, receipt, self.endpoint)
                        else:
                            key.data.eof(receipt, self.endpoint)
                            self._close(key.fileobj)
        except BaseException:
            self.failed.set()
        finally:
            with self.lock:
                for pipe in list(self.pipes):
                    try:
                        self._close(pipe, True)
                    except OSError:
                        self.failed.set()
                try:
                    self.selector.close()
                except BaseException:
                    self.failed.set()

    def close_role(self, role):
        with self.lock:
            for pipe, capture in list(self.pipes.items()):
                if capture.role == role:
                    self._close(pipe, True)

    def finish(self):
        deadline = time.monotonic() + 0.25
        while time.monotonic() < deadline:
            with self.lock:
                if not self.pipes:
                    break
            if self.failed.wait(0.01):
                break
        self.stop.set()
        self.thread.join(1)
        if self.thread.is_alive():
            raise RuntimeError('capture cleanup failed')

    def snapshot(self):
        return {role: {key: getattr(budget, key) for key in (
            'records', 'bytes_read', 'reads', 'eof_streams', 'dropped_records',
            'retention_exhausted', 'counts_saturated', 'forced_close', 'launch_seconds')}
            for role, budget in self.budgets.items()}


class Correlation:
    def __init__(self):
        self.previous = None
        self.records = []
        self.omitted = 0

    def sample(self, seconds, status):
        state = status.get('state')
        if state not in ('opening', 'buffering', 'playing', 'paused', 'stopped', 'ended', 'error'):
            state = 'unknown'
        stats = status.get('stats', {})
        if not isinstance(stats, dict):
            stats = {}
        values = {k: v if type(v := stats.get(k)) is int and 0 <= v <= MAX_COUNT
                  else None for k in SELECTED}
        current = (seconds, state, values)
        if self.previous is not None:
            before, old_state, old = self.previous
            deltas = {k: ('unknown' if old[k] is None or values[k] is None else
                          'reset' if values[k] < old[k] else values[k] - old[k])
                      for k in SELECTED}
            activity = 'unknown'
            if all(type(v) is int for v in deltas.values()):
                if deltas['decodedvideo'] == deltas['decodedaudio'] == 0:
                    activity = ('input_flat_decode_flat' if all(deltas[k] == 0 for k in SELECTED[:3])
                                else 'input_advancing_decode_flat')
            if old_state != state or any(v != 0 for v in deltas.values()) or activity != 'unknown':
                record = dict(previous_seconds=before, seconds=seconds,
                              previous_state=old_state, state=state, deltas=deltas, activity=activity)
                # Two roles plus collector/config metadata stay below 64 KiB.
                if len(self.records) < 64 and len(json.dumps(self.records + [record], indent=2).encode()) <= 20000:
                    self.records.append(record)
                else:
                    self.omitted = min(MAX_COUNT, self.omitted + 1)
        self.previous = current

    def report(self):
        return dict(transitions=self.records, omitted_transitions=self.omitted,
                    truncated=self.omitted > 0, counts_saturated=self.omitted == MAX_COUNT)
