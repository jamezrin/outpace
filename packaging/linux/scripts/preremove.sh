#!/bin/sh
# Stop and disable the service on removal, but not on upgrade.
# dpkg passes "remove" (or "upgrade"); rpm passes the remaining package count (0 on erase).
set -e
case "$1" in
	remove | 0)
		if [ -d /run/systemd/system ]; then
			systemctl disable --now outpace.service >/dev/null 2>&1 || true
		fi
		;;
esac
