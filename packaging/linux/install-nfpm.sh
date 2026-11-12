#!/usr/bin/env bash
# Install the pinned nfpm release into <dir> (default: ./.nfpm) after checking its SHA-256.
# Usage: packaging/linux/install-nfpm.sh [dir]   (x86_64 Linux hosts)
set -euo pipefail

version=2.48.0
sha256=6d7f9a28b2f397934403950fcbba83f598b08d02edbe79a0815b3b4897341750
dir=${1:-.nfpm}
archive="nfpm_${version}_Linux_x86_64.tar.gz"

mkdir -p "$dir"
curl -fsSL --retry 3 -o "$dir/$archive" \
	"https://github.com/goreleaser/nfpm/releases/download/v${version}/${archive}"
echo "${sha256}  $dir/$archive" | sha256sum --check --quiet
tar -xzf "$dir/$archive" -C "$dir" nfpm
rm "$dir/$archive"
"$dir/nfpm" --version >/dev/null
cd "$dir" && pwd
