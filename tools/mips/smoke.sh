#!/usr/bin/env bash
# Start a MIPS outpace binary under qemu-user and wait for /healthz.
#
# Usage: tools/mips/smoke.sh <mips-unknown-linux-musl|mipsel-unknown-linux-musl> [binary]
#
# Needs Docker and qemu-user (qemu-mips / qemu-mipsel). The binary links dynamically against
# soft-float musl and libgcc_s, so the matching runtime is copied out of the same cross image
# that built it. Keep CROSS_IMAGE_TAG equal to the installed cross version.
set -euo pipefail

target=$1
root=$(cd "$(dirname "$0")/../.." && pwd)
binary=${2:-$root/target/$target/release/outpace}
case "$target" in
	mips-unknown-linux-musl) arch=mips ;;
	mipsel-unknown-linux-musl) arch=mipsel ;;
	*)
		echo "unsupported target: $target" >&2
		exit 2
		;;
esac

image="ghcr.io/cross-rs/${target}:${CROSS_IMAGE_TAG:-0.2.5}"
sysroot=$(mktemp -d)
container=$(docker create "$image")
docker cp "$container:/usr/local/${arch}-linux-muslsf/lib" "$sysroot/" >/dev/null
docker rm "$container" >/dev/null
# In the image the loader is an absolute symlink to libc.so; make it resolve in the copy.
ln -sf libc.so "$sysroot/lib/ld-musl-${arch}-sf.so.1"

SMOKE_RUNNER="qemu-${arch} -L $sysroot" sh "$root/tools/ci/smoke_daemon.sh" "$binary"
