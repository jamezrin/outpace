#!/usr/bin/env bash
# End-to-end test of the OpenWrt service in an official OpenWrt rootfs container running
# procd: install with install.sh, enable through UCI, start, probe /healthz, check the
# service user, stop, and check the process is gone.
#
# Usage: packaging/openwrt/test-openwrt.sh <x86_64 static outpace binary> [image]
set -euo pipefail

binary=$1
image=${2:-openwrt/rootfs:x86-64-24.10.4}
here=$(cd "$(dirname "$0")" && pwd)

# Architecture mapping, including the ARMv6 pattern that must win over generic ARMv7.
check_target() {
	local got
	got=$(sh "$here/install.sh" --target-for "$1")
	[ "$got" = "$2" ] || { echo "arch $1: expected $2, got $got" >&2; exit 1; }
}
check_target x86_64 x86_64-unknown-linux-musl
check_target aarch64_cortex-a53 aarch64-unknown-linux-musl
check_target arm_cortex-a7_neon-vfpv4 armv7-unknown-linux-musleabihf
check_target arm_arm1176jzf-s_vfp arm-unknown-linux-musleabihf
check_target mipsel_24kc mipsel-unknown-linux-musl
check_target mips_24kc mips-unknown-linux-musl
check_target riscv64_riscv64 riscv64gc-unknown-linux-musl
if sh "$here/install.sh" --target-for powerpc_8548 >/dev/null; then
	echo "unsupported arch was mapped" >&2
	exit 1
fi

container=$(docker run -d --rm "$image" /sbin/init)
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
sleep 3
docker exec "$container" mkdir -p /tmp/outpace-pkg
docker cp "$here/." "$container:/tmp/outpace-pkg/"
docker cp "$binary" "$container:/tmp/outpace-pkg/outpace"

docker exec "$container" sh -eu -c '
	sh /tmp/outpace-pkg/install.sh --binary /tmp/outpace-pkg/outpace
	uci set outpace.main.enabled=1
	uci add_list outpace.main.env=OUTPACE_ENABLE_INBOUND=0
	uci add_list outpace.main.env=OUTPACE_ENABLE_SEEDING=0
	uci commit outpace
	/etc/init.d/outpace enable
	/etc/init.d/outpace start
	ok=0
	for i in $(seq 60); do
		if wget -q -O - http://127.0.0.1:6878/healthz 2>/dev/null; then ok=1; break; fi
		sleep 1
	done
	echo
	logread -e outpace | tail -n 15 || true
	[ "$ok" = 1 ] || { echo "outpace did not answer /healthz" >&2; exit 1; }
	# busybox ps: PID USER VSZ STAT COMMAND...
	user=$(ps | awk "\$5 == \"/usr/bin/outpace\" {print \$2; exit}")
	[ "$user" = outpace ] || { echo "service runs as ${user:-nobody?}, expected outpace" >&2; ps; exit 1; }
	# This busybox has no stat(1); ls shows the mode and owner.
	ls -ld /etc/outpace | grep -q "^drwx------ .* outpace .*outpace" || { ls -ld /etc/outpace; exit 1; }
	/etc/init.d/outpace stop
	sleep 2
	if pidof outpace >/dev/null; then echo "outpace still running after stop" >&2; exit 1; fi
	echo "openwrt service test passed"
'
