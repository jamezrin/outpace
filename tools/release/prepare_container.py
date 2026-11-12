#!/usr/bin/env python3
"""Verify a release's checksummed assets and copy only the container's Linux executables.

Only the archives in LINUX are required, because only they go into the image. Every asset
listed in SHA256SUMS must exist and match, so a tampered or missing archive still fails.
Other release assets (new targets, packages, best-effort uploads) are never opened, which
keeps adding a release target from breaking container publication, and lets this tooling
republish images for older releases with a smaller target set.
"""
import argparse
import hashlib
from pathlib import Path
import re
import tarfile

LINUX = {
    "x86_64-unknown-linux-musl": "amd64",
    "aarch64-unknown-linux-musl": "arm64",
    "armv7-unknown-linux-musleabihf": "arm-v7",
}


def prepare(tag: str, assets: Path, output: Path) -> None:
    if not re.fullmatch(r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", tag):
        raise ValueError("Expected a stable release tag vX.Y.Z")
    version = tag[1:]
    required = {f"outpace-{version}-{target}.tar.gz" for target in LINUX}
    sums = assets / "SHA256SUMS"
    if sums.is_symlink() or not sums.is_file():
        raise ValueError("Release must contain a regular SHA256SUMS file")
    checksums = {}
    for line in sums.read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (outpace-[^/\\\s]+)", line)
        if not match or match[2] in checksums:
            raise ValueError("Invalid or duplicate checksum entry")
        checksums[match[2]] = match[1]
    missing = sorted(required - set(checksums))
    if missing:
        raise ValueError(f"Checksums must cover every container archive: missing {missing}")
    for name, digest in checksums.items():
        archive = assets / name
        if archive.is_symlink() or not archive.is_file():
            raise ValueError(f"Not a regular archive: {name}")
        with archive.open("rb") as source:
            actual = hashlib.file_digest(source, "sha256").hexdigest()
        if actual != digest:
            raise ValueError(f"Checksum mismatch: {name}")
    # Validate every selected member before creating the context. Never extract paths.
    binaries = {}
    for target, platform in LINUX.items():
        with tarfile.open(assets / f"outpace-{version}-{target}.tar.gz", "r:gz") as archive:
            name = f"outpace-{version}-{target}/outpace"
            matches = [member for member in archive.getmembers() if member.name == name]
            if len(matches) != 1 or not matches[0].isfile() or not matches[0].mode & 0o111:
                raise ValueError(f"Expected exactly one regular executable: {name}")
            with archive.extractfile(matches[0]) as source:
                binaries[platform] = source.read()
            if not binaries[platform]:
                raise ValueError(f"Empty executable: {name}")
    output.mkdir(parents=True, exist_ok=False)
    for platform, binary in binaries.items():
        destination = output / platform / "outpace"
        destination.parent.mkdir()
        destination.write_bytes(binary)
        destination.chmod(0o755)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("assets", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    prepare(args.tag, args.assets, args.output)
