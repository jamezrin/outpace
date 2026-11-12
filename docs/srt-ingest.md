# Optional SRT broadcast ingest

SRT is an additional encrypted MPEG-TS input to the existing signed broadcast
pipeline. It does not replace HTTP PUT or RTMP, add SRT playback, or transcode the
publisher's codecs. The publisher must mux its input as MPEG-TS.

## Build and defaults

Normal builds have no SRT native dependency. With `OUTPACE_SRT_BIND` unset, no SRT
listener, native owner thread, or passphrase-file read is started. Enabling a bind
in a binary built without the feature fails startup before identity/cache changes.

The optional implementation currently supports native Linux builds with a C
compiler, `pkg-config`, and encryption-enabled **system libsrt 1.5.7 or later**, with
matching development headers. Cross-compiling this feature is not qualified; the
build rejects it. Default builds, including the existing ARMv7 build, are unchanged.

```bash
cargo build --release -p ace-engine --bin outpace --features srt --locked
```

The small private C shim uses the installed reference-library headers. Rust owns
socket lifetimes on one dedicated thread; the SDK also creates its own protocol
threads. The runtime version and effective receiver options are checked before use.
This avoids adding an alternative Rust protocol implementation or bundling another
crypto stack. It requires the operator to provide and update the native library.
The reference SDK is [MPL-2.0 licensed](https://github.com/Haivision/srt/blob/v1.5.7/LICENSE);
Outpace's existing license still applies to Outpace. Account for the SDK and its
crypto dependencies when packaging a feature-enabled binary.

| Setting | Default | Contract |
| --- | --- | --- |
| `OUTPACE_SRT_BIND` | unset | Numeric socket address; explicitly opt in, for example `127.0.0.1:9000`. No hostname resolution. |
| `OUTPACE_SRT_PASSPHRASE_FILE` | unset | Required when enabled; regular file owned by the daemon UID with no group/other permissions. The file itself must not be a symlink. |
| `OUTPACE_SRT_LATENCY_MS` | `120` | Receiver latency, `20`–`2000` ms. This is not an end-to-end latency guarantee. |
| `OUTPACE_SRT_MAX_CONNECTIONS` | `4` | `1`–`16` accepted connections, including pending and retiring connections. |

A passphrase is 10–79 bytes, without embedded NUL or line breaks. One terminal LF
or CRLF is accepted. A generated hexadecimal key avoids URL-escaping ambiguity in
publisher examples. The file is read once during runtime preparation; editing it
does not rotate a running listener's key. Restart the daemon to apply a new key.
Secrets are excluded from configuration serialization and debug output; the
configuration contains only the file path.

## Publisher URL and security boundary

Publish in caller mode to the configured UDP listener. The SRT `streamid` is the
literal broadcast name: 1–64 ASCII letters, digits, `.`, `_`, or `-`, excluding
`.` and `..`. Extended `#!::` resource syntax and paths are not supported.

The listener requires encrypted PSK negotiation and checks encryption before a
connection can claim a broadcast. There is no plaintext fallback. As required by
the [reference unidirectional API](https://github.com/Haivision/srt/blob/v1.5.7/docs/API/API-socket-options.md#srto_pbkeylen),
the receiver leaves key length unspecified; the sender selects AES-128, AES-192 or
AES-256. This is not an AES-256-only listener.

The PSK authenticates access to the listener, not an individual broadcast name.
Anyone holding it can attempt any valid name. Existing HTTP, RTMP, pull, and SRT
producer claims remain exclusive: an active or unresolved producer is not replaced.
The StreamID is not a credential.

**Encryption protects the SRT input leg only.** The output remains the existing
signed, unencrypted broadcast. Do not treat an encrypted input as private output.
Only publish content appropriate for that broadcast network.

Prefer a loopback bind or a narrowly firewalled UDP interface. An explicit wildcard
bind exposes this additional UDP service. Restrict allowed publisher addresses and
use a private key file and parent directory. Outpace does not map or advertise an
SRT UDP port automatically. Peer discovery/advertising continues to follow the
existing node policy; the private **peer** allowlist is not an SRT publisher ACL.
HTTP and RTMP listener settings are unchanged.

## Generated-media smoke

Use FFmpeg built with SRT and `libx264` support on a trusted publisher host. The
example uses H.264/AAC for the existing raw HTTP playback access gate; SRT does
not expand that gate's H.264/HEVC policy. First create a
private key and start the feature-enabled daemon:

```bash
install -d -m 700 "$HOME/.config/outpace"
(umask 077; openssl rand -hex 24 > "$HOME/.config/outpace/srt.key")
OUTPACE_SRT_BIND=127.0.0.1:9000 \
OUTPACE_SRT_PASSPHRASE_FILE="$HOME/.config/outpace/srt.key" \
  target/release/outpace serve
```

In another shell, set `BROADCAST_NAME` to the desired valid name. This example
publishes generated video/audio for a bounded 20 seconds:

```bash
: "${BROADCAST_NAME:?set a valid broadcast name}"
IFS= read -r SRT_KEY < "$HOME/.config/outpace/srt.key"
ffmpeg -hide_banner -loglevel error -re \
  -f lavfi -i testsrc=size=160x90:rate=25 \
  -f lavfi -i sine=frequency=440:sample_rate=48000 \
  -t 20 -c:v libx264 -preset ultrafast -tune zerolatency -pix_fmt yuv420p \
  -g 12 -c:a aac -f mpegts \
  "srt://127.0.0.1:9000?mode=caller&streamid=${BROADCAST_NAME}&passphrase=${SRT_KEY}&pkt_size=1316"
unset SRT_KEY
```

The expanded publisher URL contains the secret in process arguments. Disable shell
tracing, do not paste expanded URLs into logs/issues, and use a trusted host. For a
remote publisher, substitute the explicitly permitted listener address and transfer
the key using the publisher's protected credential mechanism.

Once admitted, the broadcast descriptor is available at
`http://<http-host>:<http-port>/broadcast/<name>`. Use the existing
[broadcast playback procedure](../README.md#broadcast-origination) and
[transport URL policy](native-api.md#content-id-resolution)
for playback; a descriptor download alone does not prove decoded playback.
A receiver must have a permitted peer path to the origin. Generated media does not
remove the need to follow the deployment's network-validation policy.

## Backpressure, disconnect, and cleanup

Each accepted connection has four handoff slots, with messages at most 64 KiB,
plus the active ingest chunk and existing signing/framing staging. Capacity is
reserved before a native read/copy. A full handoff pauses that connection's reads;
it does not block cancellation or another admitted publisher. Oversized native
messages are errors, not successful truncated TS. MPEG-TS packets may cross SRT
message boundaries.

The SDK is configured with approximately 1 MiB each of send/receive buffering
(quantized by the SDK) and a 1024-packet flow window. Its listen backlog is a
separate bound in addition to application connections. These are not a process RSS
limit: SDK, crypto, thread and kernel memory also contribute. Under sustained
pressure, SRT's live latency/drop behavior applies; this is not a lossless archive
input with unlimited buffering.

Disconnect is a live boundary, not finite-file EOF. Incomplete TS/signing tails are
discarded; already published signed pieces remain. Reconnecting to the same name
reuses the persisted identity and existing piece-index reservation rules. Piece
indices are not reused; reserved ranges can leave gaps across restart.

A connection continues to occupy admission and fence its producer claim until
native socket retirement and outstanding ingest work are confirmed. Native garbage
collection can delay replacement after disconnect. Unconfirmed cleanup reports an
error and retains the claim rather than admitting a competing producer. Do not
remove identity/cursor files to force reuse. Shutdown cancels ingress and joins the
native owner before broadcast cleanup releases ownership.

## Validation limits

The implementation has offline configuration, producer/store, cancellation, native
retirement, and generated loopback publisher controls. Whole-path playback and
performance acceptance must be recorded separately; source inspection, HTTP byte
counts, and a successful SRT handshake do not establish decoder progress or
production readiness. This transport does not broaden protected-content support.
