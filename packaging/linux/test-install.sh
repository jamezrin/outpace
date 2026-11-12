#!/bin/sh
# Install-test a package inside a throwaway container (no systemd inside it).
#
# Usage: packaging/linux/test-install.sh <package.deb|package.rpm>
#
# Run inside debian/ubuntu (for .deb) or fedora (for .rpm) as root. Checks the service
# account, file layout and modes, starts the installed binary as the service user with the
# shipped environment file, then removes the package.
set -eu

pkg=$1
# apt reads "dist/x.deb" as package "dist" from release "x.deb"; make the path explicit.
case "$pkg" in
	/* | ./*) ;;
	*) pkg=./$pkg ;;
esac
here=$(cd "$(dirname "$0")" && pwd)
case "$pkg" in
	*.deb)
		apt-get update -qq
		apt-get install -y -qq curl >/dev/null
		apt-get install -y "$pkg"
		remove() { apt-get remove -y outpace; }
		;;
	*.rpm)
		dnf install -y -q curl util-linux >/dev/null
		dnf install -y "$pkg"
		remove() { dnf remove -y outpace; }
		;;
	*)
		echo "unknown package type: $pkg" >&2
		exit 2
		;;
esac

outpace --version
getent passwd outpace
test -x /usr/bin/outpace
test -f /usr/lib/systemd/system/outpace.service
test "$(stat -c '%a %U %G' /etc/outpace)" = "750 root outpace"
test "$(stat -c '%a %U %G' /etc/outpace/outpace.env)" = "640 root outpace"

# What systemd would do: StateDirectory creates /var/lib/outpace for the service user.
install -d -o outpace -g outpace -m 0700 /var/lib/outpace
cp "$here/../../tools/ci/smoke_daemon.sh" /tmp/smoke_daemon.sh
setpriv --reuid=outpace --regid=outpace --init-groups \
	sh -c 'set -a; . /etc/outpace/outpace.env; set +a; sh /tmp/smoke_daemon.sh /usr/bin/outpace'

remove
test ! -e /usr/bin/outpace
# The env file is a config file: kept (dpkg) or saved (rpm) on remove, never silently lost.
echo "package install test passed: $pkg"
