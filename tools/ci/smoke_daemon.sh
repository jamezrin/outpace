#!/bin/sh
# Start an outpace binary, wait for /healthz, then stop it.
#
# Usage: smoke_daemon.sh <outpace-binary> [port]
#
# Set SMOKE_RUNNER to prefix the binary with an emulator (for example
# "qemu-aarch64 -L /usr/aarch64-linux-musl"). POSIX sh so it also runs inside
# BSD VMs and minimal containers; needs curl.
set -eu

binary=$1
port=${2:-16878}
runner=${SMOKE_RUNNER:-}

data_dir=$(mktemp -d)
log="$data_dir/outpace.log"

# shellcheck disable=SC2086 # SMOKE_RUNNER is intentionally word-split.
$runner "$binary" --version

# A daemon already on this port would answer /healthz for us, so refuse to run.
if curl --silent --max-time 2 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
	echo "port $port is already serving; stop that process or pass another port" >&2
	exit 1
fi

OUTPACE_BIND="127.0.0.1:$port" \
OUTPACE_RTMP_BIND="127.0.0.1:$((port + 1))" \
OUTPACE_DATA_DIR="$data_dir" \
OUTPACE_ENABLE_INBOUND=0 \
OUTPACE_ENABLE_SEEDING=0 \
OUTPACE_ENABLE_PORT_MAPPING=0 \
	$runner "$binary" serve >"$log" 2>&1 &
pid=$!

cleanup() {
	kill "$pid" 2>/dev/null || true
	wait "$pid" 2>/dev/null || true
}
trap cleanup EXIT

attempt=0
until curl --fail --silent --max-time 2 "http://127.0.0.1:$port/healthz" 2>/dev/null; do
	attempt=$((attempt + 1))
	if ! kill -0 "$pid" 2>/dev/null; then
		echo "outpace exited before answering /healthz" >&2
		cat "$log" >&2
		exit 1
	fi
	if [ "$attempt" -ge 60 ]; then
		echo "outpace did not answer /healthz within 60 attempts" >&2
		cat "$log" >&2
		exit 1
	fi
	sleep 1
done
echo
# The answer must come from the daemon started here: it is still running and logged its
# own listening banner for this address.
if ! kill -0 "$pid" 2>/dev/null || ! grep -q "listening on http://127.0.0.1:$port" "$log"; then
	echo "/healthz answered, but not by the outpace started here" >&2
	cat "$log" >&2
	exit 1
fi
echo "outpace answered /healthz"
