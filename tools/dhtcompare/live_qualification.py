"""Fixed offline qualification of the live boundary (#44). No public network.

Runs every live_fixture case inside one owned outer user/network/PID namespace whose
only egress is a dummy device with synthetic addresses, under a 240-second hard
deadline. Pins detect stale operator-built binaries, not hostile concurrent edits.
Exit 0 only when every fixed predicate passes and the outer namespace was reaped.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import live_fixture
from qualification import Artifact

DEADLINE = 240


def qualify_live(custom, upstream, *, output):
    custom.verify(); upstream.verify()
    output = Path(output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    config = json.dumps({'cases': list(live_fixture.CASES),
                         'custom': str(custom.path), 'custom_sha256': custom.sha256,
                         'upstream': str(upstream.path), 'upstream_sha256': upstream.sha256})
    command = ['/usr/bin/unshare', '--user', '--map-root-user', '--net', '--pid', '--fork',
               '--kill-child=KILL', '--mount-proc', sys.executable, '-I',
               str(Path(live_fixture.__file__).resolve()), config]
    try:
        completed = subprocess.run(command, capture_output=True, timeout=DEADLINE,
                                   env={'LC_ALL': 'C', 'PATH': '/usr/bin:/usr/sbin'})
        exit_code, stdout, stderr, timed_out = completed.returncode, completed.stdout, completed.stderr, False
    except subprocess.TimeoutExpired as expired:
        exit_code, stdout, stderr, timed_out = None, expired.stdout or b'', expired.stderr or b'', True
    for name, data in (('fixture.stdout.private', stdout), ('fixture.stderr.private', stderr)):
        descriptor = os.open(output / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'wb') as out:
            out.write(data)
    try:
        results = json.loads(stdout) if exit_code == 0 else {}
    except ValueError:
        results = {}
    failed = live_fixture.evaluate(results) if results else list(live_fixture.CASES)
    report = {'passed': not failed and exit_code == 0 and not timed_out, 'failed_cases': failed,
              'exit_code': exit_code, 'timed_out': timed_out, 'cases': len(results),
              'artifacts': {'custom': custom.sha256, 'upstream': upstream.sha256}}
    descriptor = os.open(output / 'report.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as out:
        json.dump(report, out, sort_keys=True, indent=1)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--custom', type=Path, required=True)
    parser.add_argument('--custom-sha256', required=True)
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--upstream-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    report = qualify_live(Artifact(args.custom, args.custom_sha256),
                          Artifact(args.upstream, args.upstream_sha256), output=args.output)
    print(json.dumps({k: report[k] for k in ('passed', 'failed_cases', 'cases')}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
