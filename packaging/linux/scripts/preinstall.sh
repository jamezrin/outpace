#!/bin/sh
# Create the service account before files owned by it are unpacked.
set -e
if ! getent group outpace >/dev/null 2>&1; then
	groupadd --system outpace
fi
if ! getent passwd outpace >/dev/null 2>&1; then
	useradd --system --gid outpace --home-dir /var/lib/outpace --no-create-home \
		--shell /usr/sbin/nologin --comment "Outpace streaming engine" outpace
fi
