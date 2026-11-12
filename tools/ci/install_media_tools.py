"""Install signed Ubuntu media packages on a pinned ephemeral CI runner release.

Canonical's HTTPS archives are tried first; kernel.org's Ubuntu mirror is the
fallback when the Canonical index cannot be fetched (for example a runner DNS
outage for *.ubuntu.com).

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
# Explicit allowlist of runner releases and their archive codenames. Adding a
# release is a deliberate change; anything else is rejected before apt runs.
RELEASES = {'24.04': 'noble', '26.04': 'resolute'}


def supported_runner():
    """Return the archive codename, rejecting another distribution/release first."""
    release = {}
    for line in Path('/etc/os-release').read_text().splitlines():
        key, separator, value = line.partition('=')
        if separator:
            release[key] = value.strip().strip('"')
    codename = RELEASES.get(release.get('VERSION_ID'))
    if (release.get('ID') != 'ubuntu' or codename is None
            or platform.machine() not in ('x86_64', 'amd64') or not KEYRING.is_file()):
        raise RuntimeError('unsupported_runner')
    return codename


# Canonical first: #333 moved off the runner's cloud mirror because slow
# downloads consumed the job. The fallback is kernel.org's official Ubuntu
# mirror, deliberately outside *.ubuntu.com, so a runner DNS outage for that
# domain (which also takes out azure.archive.ubuntu.com) does not block CI.
# Every index is still verified against the pinned keyring through signed-by.
def mirrors(codename):
    """Signed sources for the runner's release, Canonical first."""
    return [
        ('canonical', [
            f'https://archive.ubuntu.com/ubuntu {codename} main universe',
            f'https://archive.ubuntu.com/ubuntu {codename}-updates main universe',
            f'https://security.ubuntu.com/ubuntu {codename}-security main universe',
        ]),
        ('kernel.org', [
            f'https://mirrors.edge.kernel.org/ubuntu {codename} main universe',
            f'https://mirrors.edge.kernel.org/ubuntu {codename}-updates main universe',
            f'https://mirrors.edge.kernel.org/ubuntu {codename}-security main universe',
        ]),
    ]
UPDATE_SECONDS = 55
INSTALL_SECONDS = 200


def apt(directory, name, lines, arguments, seconds):
    """Run one bounded apt-get call scoped to a temporary sourcelist."""
    source = directory / f'{name}.list'
    source.write_text(''.join(f'deb [signed-by={KEYRING}] {line}\n' for line in lines))
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
    command = ['sudo', '-n', '/usr/bin/timeout', '--signal=TERM', '--kill-after=10s',
               f'{seconds}s', '/usr/bin/env', 'DEBIAN_FRONTEND=noninteractive',
               'LC_ALL=C', '/usr/bin/apt-get', *options, *arguments]
    subprocess.run(command, check=True, timeout=seconds + 20)


def install():
    """Index from the first reachable signed mirror, install once, require both tools."""
    if (os.environ.get('GITHUB_ACTIONS') != 'true'
            or os.environ.get('RUNNER_OS') != 'Linux'
            or not os.environ.get('RUNNER_TEMP')):
        raise RuntimeError('ci_context_required')
    sources = mirrors(supported_runner())
    with tempfile.TemporaryDirectory(prefix='outpace-media-', dir=os.environ['RUNNER_TEMP']) as name:
        directory = Path(name)
        # Public repository configuration must be readable by APT's _apt user.
        directory.chmod(0o755)
        for index, (mirror, lines) in enumerate(sources):
            try:
                apt(directory, mirror, lines, ['update'], UPDATE_SECONDS)
            except subprocess.SubprocessError:
                if index == len(sources) - 1:
                    raise
                print(f'CI media prerequisite: {mirror} index failed; trying next mirror',
                      file=sys.stderr)
                continue
            print(f'CI media prerequisite: using {mirror} mirror', file=sys.stderr)
            # Install failures never fall back: the index that succeeded is the
            # only one apt resolves candidates from.
            apt(directory, mirror, lines,
                ['install', '-y', '--no-install-recommends', 'ffmpeg'], INSTALL_SECONDS)
            break
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
