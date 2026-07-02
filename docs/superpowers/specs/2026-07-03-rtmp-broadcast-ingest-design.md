# RTMP Broadcast Ingest Design

Date: 2026-07-03

## Goal

Add native RTMP ingest for Outpace-originated broadcasts while keeping the existing raw
MPEG-TS HTTP ingest supported. The broadcast CLI should no longer call the raw endpoint an
OBS URL; it should print protocol-specific ingest URLs:

- `RAW Ingest URL: http://<http-host>:<http-port>/broadcast/<name> (MPEG-TS)`
- `RTMP Ingest URL: rtmp://<rtmp-host>:<rtmp-port>/live/<name>`

## Scope

In scope:

- A separate RTMP listener for broadcast ingest.
- A new `OUTPACE_RTMP_BIND` env override, defaulting to `127.0.0.1:1935`.
- RTMP publish support for app `live` and stream key `<name>`.
- H.264 video and AAC audio ingest from RTMP.
- Remuxing RTMP/FLV media into MPEG-TS before feeding Outpace's existing broadcast
  signing, chunking, and seeding path.
- Tests for config defaults/env parsing, CLI output labels/URL formation, RTMP publish
  routing, and remuxed TS bytes reaching the broadcast piece store.

Out of scope:

- RTMPS.
- RTMP playback/subscriber support.
- Non-H.264 video codecs, enhanced RTMP codecs, or non-AAC audio.
- Authentication or secret stream keys.
- Solving the existing broadcast reconnect continuity gap where a second ingest task for
  the same name restarts piece numbering.

## Architecture

The existing `PUT /broadcast/{name}` path accepts MPEG-TS and performs transport minting,
piece signing, chunking, and store registration. That logic should be factored into a small
shared broadcast ingest helper so both HTTP and RTMP enter the same downstream pipeline.

RTMP will run as a sibling listener to the HTTP server. The runtime owns both bind addresses
and starts the RTMP task when `outpace broadcast <name>` or normal serving enables
broadcast state. The RTMP handler validates `connect` app `live`, treats the publish stream
key as the broadcast name, starts or resumes that broadcast through `BroadcastRegistry`, and
announces the infohash/content-id exactly like HTTP-created broadcasts.

## RTMP Media Flow

Use `rtmp-rs` for RTMP protocol handling because it exposes publisher callbacks for raw FLV
tags and parsed H.264/AAC frames. Use a TS muxing layer, preferably the `mpeg2ts` crate if it
fits the API cleanly, otherwise a minimal internal muxer for PAT/PMT/PES/188-byte packetization.

The muxer converts accepted RTMP media to a continuous MPEG-TS byte stream:

- H.264 AVC sequence headers provide SPS/PPS.
- H.264 video frames are emitted as Annex B access units with SPS/PPS before keyframes.
- AAC sequence headers configure ADTS headers for subsequent AAC frames.
- RTMP millisecond timestamps are converted to 90 kHz PTS values for PES packets.
- Output is emitted as aligned 188-byte TS packets and pushed into the shared broadcast
  ingest helper.

Unsupported codecs or malformed codec headers are rejected for that publish stream and logged
without affecting other broadcasts.

## Operator Interface

Defaults:

- HTTP raw MPEG-TS bind remains `OUTPACE_BIND`, default `127.0.0.1:6878`.
- RTMP bind is `OUTPACE_RTMP_BIND`, default `127.0.0.1:1935`.

`outpace broadcast mychan` prints:

```text
outpace broadcast: mychan
RAW Ingest URL: http://127.0.0.1:6878/broadcast/mychan (MPEG-TS)
RTMP Ingest URL: rtmp://127.0.0.1:1935/live/mychan
Content ID: <40hex>
Ace link: acestream://<40hex>
Infohash: <40hex>
Transport URL: http://127.0.0.1:6878/broadcast/mychan
Peer listen: 0.0.0.0:8621
```

For public hosts, `--public-host` continues to control externally displayed HTTP transport
host. The RTMP URL uses the RTMP bind host by default; if a public-host is provided, it should
also be used for the displayed RTMP host, preserving the RTMP bind port.

## Testing

Use TDD for each behavior:

- Config test: default RTMP bind is `127.0.0.1:1935`; `OUTPACE_RTMP_BIND` overrides it.
- CLI formatting test: broadcast output contains `RAW Ingest URL` and `RTMP Ingest URL`, and
  does not contain `OBS ingest URL`.
- Shared ingest test: the existing HTTP broadcast tests still pass through the shared helper.
- Muxer tests: known H.264/AAC inputs produce MPEG-TS output that is 188-byte aligned and
  includes PAT/PMT and media PES packets.
- RTMP loopback test: an RTMP publisher sends a short H.264/AAC stream to `/live/<name>` and
  the corresponding broadcast store receives signed chunks.

## Risks

The main risk is TS muxing correctness. Keep the muxer surface deliberately narrow for the
first version: H.264/AAC only, monotonically increasing RTMP timestamps, and aligned TS output.
This is enough for common RTMP publishers and avoids expanding Outpace into a general media
transcoder.
