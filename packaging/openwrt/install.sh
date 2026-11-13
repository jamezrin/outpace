#!/bin/sh
# Install Outpace on an OpenWrt router: binary, procd service and UCI config.
#
#   sh install.sh <version>              download the release for this router and install it
#   sh install.sh --binary <path>        install an outpace binary you already have
#
# Run as root on the router. The service is installed disabled; see /etc/config/outpace.
# Re-running upgrades the binary and keeps /etc/config/outpace and /etc/outpace.
set -eu

repo=https://github.com/jamezrin/outpace-stream
here=$(cd "$(dirname "$0")" && pwd)
version=""
binary=""
mode=install

case "${1:-}" in
--target-for)
	# Print the release target for an OpenWrt architecture (used by the tests).
	mode="target-for"
	;;
--binary)
	binary=${2:?usage: install.sh --binary <path>}
	;;
"" | -h | --help)
	sed -n '2,9p' "$0"
	exit 2
	;;
*)
	version=${1#v}
	;;
esac

# OpenWrt's package architecture (for example aarch64_cortex-a53) names the release target.
target_for_arch() {
	case "$1" in
	x86_64) echo x86_64-unknown-linux-musl ;;
	aarch64_*) echo aarch64-unknown-linux-musl ;;
	# ARMv6 (Raspberry Pi 1/Zero) before the generic ARMv7 hard-float pattern.
	arm_arm1176jzf-s_vfp) echo arm-unknown-linux-musleabihf ;;
	arm_*vfp* | arm_*neon*) echo armv7-unknown-linux-musleabihf ;;
	riscv64_*) echo riscv64gc-unknown-linux-musl ;;
	*) return 1 ;;
	esac
}

if [ "$mode" = target-for ]; then
	target_for_arch "${2:?usage: install.sh --target-for <openwrt-arch>}"
	exit
fi

if [ -z "$binary" ]; then
	# shellcheck source=/dev/null
	. /etc/openwrt_release
	arch=$DISTRIB_ARCH
	target=$(target_for_arch "$arch") || {
		echo "no Outpace build for OpenWrt architecture $arch (supported: x86_64, aarch64, ARMv6/ARMv7 hard-float, riscv64)" >&2
		exit 1
	}
	archive="outpace-${version}-${target}.tar.gz"
	base="$repo/releases/download/v${version}"
	work=$(mktemp -d)
	trap 'rm -rf "$work"' EXIT
	echo "outpace: downloading $archive for $arch"
	wget -q -O "$work/$archive" "$base/$archive"
	wget -q -O "$work/SHA256SUMS" "$base/SHA256SUMS"
	(cd "$work" && grep "  $archive\$" SHA256SUMS | sha256sum -c)
	tar -xzf "$work/$archive" -C "$work"
	binary="$work/outpace-${version}-${target}/outpace"
fi

"$binary" --version >/dev/null || {
	echo "the outpace binary does not run on this router" >&2
	exit 1
}

# Unprivileged service account, created the way OpenWrt packages do it.
# /lib/functions.sh reads unset variables, so relax -u while it is in use.
set +u
# shellcheck source=/dev/null
. /lib/functions.sh
group_exists outpace || group_add outpace 6878
user_exists outpace || user_add outpace 6878 6878 outpace /etc/outpace /bin/false
set -u

# OpenWrt's busybox has no install(1).
put() {
	cp "$2" "$3.new"
	chmod "$1" "$3.new"
	mv "$3.new" "$3"
}
put 0755 "$binary" /usr/bin/outpace
put 0755 "$here/files/etc/init.d/outpace" /etc/init.d/outpace
mkdir -p /lib/upgrade/keep.d
put 0644 "$here/files/lib/upgrade/keep.d/outpace" /lib/upgrade/keep.d/outpace
[ -e /etc/config/outpace ] || put 0644 "$here/files/etc/config/outpace" /etc/config/outpace
mkdir -p /etc/outpace
chown outpace:outpace /etc/outpace
chmod 0700 /etc/outpace

if /etc/init.d/outpace running 2>/dev/null; then
	/etc/init.d/outpace restart
fi
echo "outpace: installed $(/usr/bin/outpace --version)"
echo "outpace: enable with: uci set outpace.main.enabled=1 && uci commit outpace && /etc/init.d/outpace enable && /etc/init.d/outpace start"
