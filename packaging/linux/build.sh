#!/usr/bin/env bash
# Build the .deb and .rpm packages for one static musl release binary.
#
# Usage: packaging/linux/build.sh <rust-target> <version> <outpace-binary> <out-dir>
#
# Needs nfpm on PATH. Writes outpace_<version>_<deb-arch>.deb and
# outpace_<version>_<rpm-arch>.rpm. The underscore names keep packages out of the
# `outpace-*` archive pattern that container publication downloads.
set -euo pipefail

target=$1
version=$2
binary=$3
out=$4
root=$(cd "$(dirname "$0")/../.." && pwd)

case "$target" in
	x86_64-unknown-linux-musl) nfpm_arch=amd64 deb_arch=amd64 rpm_arch=x86_64 ;;
	aarch64-unknown-linux-musl) nfpm_arch=arm64 deb_arch=arm64 rpm_arch=aarch64 ;;
	armv7-unknown-linux-musleabihf) nfpm_arch=arm7 deb_arch=armhf rpm_arch=armv7hl ;;
	*)
		echo "no package mapping for target: $target" >&2
		exit 2
		;;
esac

mkdir -p "$root/target/package" "$out"
install -m 0755 "$binary" "$root/target/package/outpace"
cd "$root"
export VERSION="$version" NFPM_ARCH="$nfpm_arch"
nfpm package --config packaging/linux/nfpm.yaml --packager deb \
	--target "$out/outpace_${version}_${deb_arch}.deb"
nfpm package --config packaging/linux/nfpm.yaml --packager rpm \
	--target "$out/outpace_${version}_${rpm_arch}.rpm"
