#!/bin/sh
# Install (or remove) outpace as a per-user launchd LaunchAgent on macOS.
#
# Usage: packaging/macos/install-launchagent.sh [install|uninstall] [path/to/outpace]
#
# install   writes ~/Library/LaunchAgents/outpace.plist from outpace.plist.in, then loads it
#           (starts now and at every login; restarted if it exits with an error).
# uninstall unloads it and removes the plist. Data in ~/Library/Application Support/outpace
#           and the log in ~/Library/Logs/outpace.log are kept.
#
# Settings: edit the EnvironmentVariables in the installed plist, then run install again.
set -eu

action=${1:-install}
binary=${2:-/usr/local/bin/outpace}
label=outpace
here=$(cd "$(dirname "$0")" && pwd)
plist="$HOME/Library/LaunchAgents/$label.plist"
# The GUI login session; over SSH or in headless sessions only the per-user domain exists.
domain="gui/$(id -u)"
launchctl print "$domain" >/dev/null 2>&1 || domain="user/$(id -u)"

case "$action" in
install)
	if [ ! -x "$binary" ]; then
		echo "not executable: $binary (pass the outpace path as the second argument)" >&2
		exit 1
	fi
	binary=$(cd "$(dirname "$binary")" && pwd)/$(basename "$binary")
	mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs"
	# Reloading replaces a previous definition.
	launchctl bootout "$domain/$label" 2>/dev/null || true
	sed -e "s|@BINARY@|$binary|g" -e "s|@HOME@|$HOME|g" "$here/outpace.plist.in" >"$plist"
	plutil -lint "$plist" >/dev/null
	launchctl bootstrap "$domain" "$plist"
	echo "outpace: loaded $plist; log: $HOME/Library/Logs/outpace.log"
	;;
uninstall)
	launchctl bootout "$domain/$label" 2>/dev/null || true
	rm -f "$plist"
	echo "outpace: removed $plist"
	;;
*)
	echo "usage: $0 [install|uninstall] [path/to/outpace]" >&2
	exit 2
	;;
esac
