#!/bin/sh
# Forget the removed unit. The outpace user and /var/lib/outpace (identity and broadcast keys)
# are kept so a reinstall keeps its identity; delete them by hand to purge.
set -e
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload >/dev/null 2>&1 || true
fi
