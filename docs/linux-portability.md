# Linux portability

Outpace release containers support `linux/amd64`, `linux/arm64`, and `linux/arm/v7`. Release
archives use the matching Rust targets, including `armv7-unknown-linux-musleabihf` for ARMv7.

## ARMv7 scope and constraints

ARMv7 is intended for Raspberry Pi 2/3-class systems, older NAS devices, and comparable ARMv7
Linux hardware with a hard-float ABI. It does not cover ARMv6, soft-float ARM, or CPUs without the
ARMv7 instruction set. QEMU CI is a compatibility gate, not a performance benchmark; operators
should expect less throughput and fewer practical concurrent sessions than on 64-bit systems.

The automatic portability gate cross-compiles the release binary for ARMv7. This catches Rust,
native dependency, linker, and target-ABI incompatibilities without paying the cost of running a
full build and test suite through CPU emulation. The workflow also provides a manually enabled
QEMU container smoke that starts the published container shape with inbound serving disabled and
checks `/healthz` when runtime evidence is needed.

The initial dependency and source audit found no ARMv7-specific blocker in the crypto/bigint,
atomics, Tokio, RTMP, tracker, or NAT stacks. Transport geometry already caps individual piece
allocations at 8 MiB and metadata/transport payloads at 1 MiB. A bare
`cargo check --target armv7-unknown-linux-musleabihf` still requires an ARM musl C compiler because
`ring` builds native code; CI and release builds install it with
`taiki-e/setup-cross-toolchain-action` rather than relying on the host toolchain.

For constrained devices, start with the disk cache and a conservative session buffer:

```sh
docker run --rm \
  -p 6878:6878 \
  -p 8621:8621/tcp \
  -e OUTPACE_CACHE_TYPE=disk \
  -e OUTPACE_SEED_STORE_BYTES=67108864 \
  -e OUTPACE_SESSION_BUFFER=64 \
  -v outpace-data:/var/lib/outpace \
  ghcr.io/jamezrin/outpace:latest serve
```

The current inbound peer listener is TCP; it does not bind UDP port 8621, so an ARMv7 deployment
does not need to publish that UDP port. DHT and tracker UDP traffic use outbound sockets.

Cache accounting uses `u64` so disk budgets may exceed 4 GiB. Memory cache budgets are capped per
pool at one quarter of the target's maximum Rust object size, with an 8 GiB absolute ceiling on
64-bit targets. On 32-bit ARM this is approximately 512 MiB, leaving address space for allocator
overhead, indexes, runtime buffers, and other active sessions; it is not a guarantee that several
pools can all reach the cap concurrently. HLS validates both one segment and the complete retained
window (completed segments plus the current segment) against the same conservative bound using
checked arithmetic. `OUTPACE_HLS_SEGMENT_PACKETS` is a hard ceiling in PCR-timed and packet-
fallback modes alike; PCR/random-access boundaries may cut earlier but cannot grow past it.

## AArch64 kernels with 16 KiB pages

Outpace does not embed the precompiled Android libraries that caused the reference AceServe image
to fail on 16 KiB-page kernels. That architectural difference is encouraging but is not proof of
runtime compatibility. A real 16 KiB-page aarch64 kernel is required; QEMU user-mode execution and
ELF inspection cannot validate the kernel loader and memory-mapping behavior.

Run both parts of this procedure on an aarch64 host whose kernel was built for 16 KiB pages. Use
the pinned Rust toolchain and provide native `musl-gcc`, `file`, and `readelf` tools before
starting.

First verify the exact release commit and build the same aarch64 musl target used by the release
workflow:

```sh
test "$(uname -m)" = aarch64
test "$(getconf PAGESIZE)" = 16384

# Check out the exact release candidate commit with no local modifications.
COMMIT="${COMMIT:?set COMMIT to the full release-candidate commit SHA}"
test "$(git rev-parse HEAD)" = "$COMMIT"
test -z "$(git status --porcelain)"

rustup target add aarch64-unknown-linux-musl
CC_aarch64_unknown_linux_musl=musl-gcc \
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
  cargo build --locked --release -p ace-engine --bin outpace \
  --target aarch64-unknown-linux-musl

MUSL_BINARY=target/aarch64-unknown-linux-musl/release/outpace
file "$MUSL_BINARY"
readelf -hW "$MUSL_BINARY"
readelf -lW "$MUSL_BINARY"
"$MUSL_BINARY" --version

rm -rf /tmp/outpace-16k-musl
env OUTPACE_BIND=127.0.0.1:16879 \
  OUTPACE_RTMP_BIND=127.0.0.1:0 \
  OUTPACE_DATA_DIR=/tmp/outpace-16k-musl \
  OUTPACE_ENABLE_INBOUND=0 \
  OUTPACE_ENABLE_SEEDING=0 \
  "$MUSL_BINARY" serve > /tmp/outpace-16k-musl.log 2>&1 &
MUSL_PID=$!
trap 'kill "$MUSL_PID" >/dev/null 2>&1 || true' EXIT
curl --fail --retry 30 --retry-delay 1 --retry-all-errors \
  http://127.0.0.1:16879/healthz
kill "$MUSL_PID"
wait "$MUSL_PID" || true
trap - EXIT
cat /tmp/outpace-16k-musl.log
```

Then build and run the glibc-based release container shape from that same clean commit. Run it by
the immutable local image ID, not the temporary tag:

```sh
docker build --pull --platform linux/arm64 --tag "outpace:16k-${COMMIT}" .
IMAGE="$(docker image inspect "outpace:16k-${COMMIT}" --format '{{.Id}}')"
case "$IMAGE" in
  sha256:*) ;;
  *) echo "candidate build did not produce an immutable image ID" >&2; exit 1 ;;
esac
docker image inspect "$IMAGE" --format 'candidate_id={{.Id}}'

container="$(docker run --detach --rm --platform linux/arm64 \
  --publish 127.0.0.1:6878:6878 \
  --env OUTPACE_ENABLE_INBOUND=0 \
  --env OUTPACE_ENABLE_SEEDING=0 \
  "$IMAGE" serve)"
trap 'docker rm --force "$container" >/dev/null 2>&1 || true' EXIT

curl --fail --retry 30 --retry-delay 1 --retry-all-errors \
  http://127.0.0.1:6878/healthz
docker logs "$container"
docker exec "$container" getconf PAGESIZE
```

Record the hardware model, distribution, `uname -a`, `getconf PAGESIZE`, the exact `COMMIT` and
`IMAGE` values, musl `file`/`readelf` output, both startup logs, and both health responses. Run the
container by its immutable image ID as above, not by a mutable tag such as `latest`. Then run the
offline workspace suite natively from that same release commit:

```sh
cargo test --locked --workspace --all-targets
```

As of 2026-07-12, no suitable native 16 KiB-page runner was available to the project, so this
procedure remains a manual release qualification gate. Do not infer 16 KiB-page support solely
from the ordinary arm64 build or 4 KiB-page CI.
