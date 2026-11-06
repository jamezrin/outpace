"""Install signed Ubuntu media packages on the pinned ephemeral CI runner.

No persistent APT source files are changed. The caller retains its outer job
limit; timeout(1) bounds each privileged operation. This is not a deployment tool.
"""
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

KEYRING = Path('/usr/share/keyrings/ubuntu-archive-keyring.gpg')


def supported_runner():
    """Reject another distribution/release before executing package commands."""
    release = {}
    for line in Path('/etc/os-release').read_text().splitlines():
        key, separator, value = line.partition('=')
        if separator:
            release[key] = value.strip().strip('"')
    if (release.get('ID') != 'ubuntu' or release.get('VERSION_ID') != '24.04'
            or platform.machine() not in ('x86_64', 'amd64') or not KEYRING.is_file()):
        raise RuntimeError('unsupported_runner')


def install():
    """Acquire once from signed HTTPS archives, then require both executables."""
    if (os.environ.get('GITHUB_ACTIONS') != 'true'
            or os.environ.get('RUNNER_OS') != 'Linux'
            or not os.environ.get('RUNNER_TEMP')):
        raise RuntimeError('ci_context_required')
    supported_runner()
    with tempfile.TemporaryDirectory(prefix='outpace-media-', dir=os.environ['RUNNER_TEMP']) as name:
        directory = Path(name)
        # Public repository configuration must be readable by APT's _apt user.
        directory.chmod(0o755)
        source = directory / 'ubuntu.list'
        source.write_text(
            f'deb [signed-by={KEYRING}] https://archive.ubuntu.com/ubuntu noble main universe\n'
            f'deb [signed-by={KEYRING}] https://archive.ubuntu.com/ubuntu noble-updates main universe\n'
            f'deb [signed-by={KEYRING}] https://security.ubuntu.com/ubuntu noble-security main universe\n'
        )
        source.chmod(0o644)
        options = [
            '-o', f'Dir::Etc::sourcelist={source}',
            '-o', f'Dir::Etc::sourceparts={directory / "no-source-parts"}',
            '-o', 'APT::Get::List-Cleanup=0',
            '-o', 'APT::Update::Error-Mode=any',
            '-o', 'Acquire::Languages=none',
            '-o', 'Acquire::Retries=0',
            '-o', 'Acquire::http::Timeout=15',
            '-o', 'Acquire::https::Timeout=15',
            '-o', 'DPkg::Lock::Timeout=20',
        ]
        for arguments, seconds in [(['update'], 90),
                                   (['install', '-y', '--no-install-recommends', 'ffmpeg'], 240)]:
            command = ['sudo', '-n', '/usr/bin/timeout', '--signal=TERM', '--kill-after=10s',
                       f'{seconds}s', '/usr/bin/env', 'DEBIAN_FRONTEND=noninteractive',
                       'LC_ALL=C', '/usr/bin/apt-get', *options, *arguments]
            subprocess.run(command, check=True, timeout=seconds + 20)
    for executable in ('/usr/bin/ffmpeg', '/usr/bin/ffprobe'):
        subprocess.run([executable, '-hide_banner', '-version'], check=True, timeout=10)


if __name__ == '__main__':
    try:
        if len(sys.argv) != 1:
            raise RuntimeError('no_arguments_supported')
        install()
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f'CI media prerequisite setup failed: {type(error).__name__}', file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(f'child_exit={error.returncode}', file=sys.stderr)
        raise SystemExit(2)
