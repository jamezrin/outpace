#!/bin/sh
# Install (or remove) outpace as a per-user launchd LaunchAgent on macOS.
#
# Usage: packaging/macos/install-launchagent.sh [install|reload|uninstall] [path/to/outpace]
#
# install   creates ~/Library/LaunchAgents/outpace.plist from outpace.plist.in, then loads it
#           (starts now and at every login; restarted if it exits with an error). If the plist
#           already exists, only its program path is updated, so edited settings are kept.
# reload    reloads the installed plist unchanged, after you edit its EnvironmentVariables.
# uninstall unloads it and removes the plist. Data in ~/Library/Application Support/outpace
#           and the log in ~/Library/Logs/outpace.log are kept.
set -eu

action=${1:-install}
binary=${2:-/usr/local/bin/outpace}
label=outpace
here=$(cd "$(dirname "$0")" && pwd)
plist="$HOME/Library/LaunchAgents/$label.plist"
log="$HOME/Library/Logs/outpace.log"
# The GUI login session; over SSH or in headless sessions only the per-user domain exists.
domain="gui/$(id -u)"
launchctl print "$domain" >/dev/null 2>&1 || domain="user/$(id -u)"

load() {
	# Reloading replaces a previous definition.
	launchctl bootout "$domain/$label" 2>/dev/null || true
	launchctl bootstrap "$domain" "$plist"
	echo "outpace: loaded $plist; log: $log"
}

case "$action" in
install)
	if [ ! -x "$binary" ]; then
		echo "not executable: $binary (pass the outpace path as the second argument)" >&2
		exit 1
	fi
	binary=$(cd "$(dirname "$binary")" && pwd)/$(basename "$binary")
	mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs"
	tmp=$(mktemp "$plist.XXXXXX")
	trap 'rm -f "$tmp"' EXIT
	if [ -f "$plist" ]; then
		cp "$plist" "$tmp"
	else
		cp "$here/outpace.plist.in" "$tmp"
		# plutil writes the values as plist strings, so any path is stored literally.
		plutil -replace StandardOutPath -string "$log" "$tmp"
		plutil -replace StandardErrorPath -string "$log" "$tmp"
	fi
	# On an array element, plutil -replace inserts instead of replacing, so remove the old
	# program path first.
	plutil -remove ProgramArguments.0 "$tmp"
	plutil -insert ProgramArguments.0 -string "$binary" "$tmp"
	# Validate before touching the running agent.
	plutil -lint "$tmp" >/dev/null
	mv "$tmp" "$plist"
	trap - EXIT
	load
	;;
reload)
	plutil -lint "$plist" >/dev/null
	load
	;;
uninstall)
	launchctl bootout "$domain/$label" 2>/dev/null || true
	rm -f "$plist"
	echo "outpace: removed $plist"
	;;
*)
	echo "usage: $0 [install|reload|uninstall] [path/to/outpace]" >&2
	exit 2
	;;
esac
