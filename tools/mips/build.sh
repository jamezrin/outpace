#!/usr/bin/env bash
# Best-effort build of outpace for 32-bit MIPS musl (OpenWrt routers).
#
# Usage: tools/mips/build.sh <mips-unknown-linux-musl|mipsel-unknown-linux-musl> [profile]
#
# Needs Docker and `cross`. MIPS is tier 3, so this uses the nightly in MIPS_TOOLCHAIN with
# -Zbuild-std instead of the pinned stable toolchain. rtmp-rs 0.6 uses std's AtomicU64, which
# 32-bit MIPS lacks; this build alone patches it (session ids become usize) through a
# `patch.crates-io` override, so every other build keeps the published crate.
#
# Tier-3 MIPS musl links dynamically against the soft-float musl loader
# (/lib/ld-musl-mips{,el}-sf.so.1) and libgcc_s, which is exactly what OpenWrt provides.
# A static build needs a libunwind that build-std does not supply. The binary is written to
# target/<target>/<profile>/outpace.
set -euo pipefail

target=$1
profile=${2:-release}
toolchain=${MIPS_TOOLCHAIN:-nightly-2026-11-01}
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

case "$target" in
	mips-unknown-linux-musl | mipsel-unknown-linux-musl) ;;
	*)
		echo "unsupported target: $target" >&2
		exit 2
		;;
esac

# Locate the locked rtmp-rs source and stage a patched copy inside the workspace, which
# cross mounts into its build container.
cargo fetch --locked
src=$(cargo metadata --locked --format-version 1 |
	python3 -c 'import json,sys,os; m=json.load(sys.stdin); print(next(os.path.dirname(p["manifest_path"]) for p in m["packages"] if p["name"]=="rtmp-rs"))')
staged=target/mips-patched/rtmp-rs
rm -rf "$staged"
mkdir -p "$(dirname "$staged")"
cp -R "$src" "$staged"
patch -d "$staged" -p1 --forward <tools/mips/rtmp-rs-session-id-usize.patch

# The override changes rtmp-rs's lockfile source, so --locked cannot be used here; keep a
# pristine Cargo.lock for every other build.
cp Cargo.lock "target/mips-patched/Cargo.lock.orig"
trap 'cp target/mips-patched/Cargo.lock.orig Cargo.lock' EXIT

CROSS_CONFIG=tools/mips/Cross.toml cross "+$toolchain" build \
	--profile "$profile" -p ace-engine --bin outpace --target "$target" \
	--config "patch.crates-io.rtmp-rs.path='$staged'"
