#!/bin/sh
# Reload units. The service is never enabled or started automatically; on upgrade a running
# service is restarted onto the new binary.
set -e
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload >/dev/null 2>&1 || true
	systemctl try-restart outpace.service >/dev/null 2>&1 || true
fi
if [ ! -e /etc/systemd/system/multi-user.target.wants/outpace.service ]; then
	echo "outpace: installed. Edit /etc/outpace/outpace.env, then: systemctl enable --now outpace"
fi
