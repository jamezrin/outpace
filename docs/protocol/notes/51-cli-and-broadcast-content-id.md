# 51 - CLI broadcast/play and outpace broadcast content IDs

Date: 2026-07-02

`outpace` now has three primary commands:

- `outpace serve`
- `outpace broadcast <name>`
- `outpace play <acestream-url>`

For outpace-originated broadcasts, `content_id` is the raw transport-file hash.
The broadcaster announces that key and serves the minted `AceStreamTransport` over
BEP-9 `ut_metadata`, so `outpace play acestream://<content_id>` resolves metadata
before joining the actual broadcast infohash.

Broadcast ingest is now exposed through two inputs:

- raw MPEG-TS over HTTP PUT: `http://<host>:<http-port>/broadcast/<name>`
- RTMP publish: `rtmp://<host>:<rtmp-port>/live/<name>`

The RTMP path accepts H.264/AAC publisher input and remuxes it to MPEG-TS before entering
the same signing/chunking/store pipeline used by raw ingest.

Smoke result:

- broadcaster bind: `127.0.0.1:6990`
- peer listen: `127.0.0.1:6991`
- content id: `b123456789abcdef0123456789abcdef01234567`
- infohash: `e123456789abcdef0123456789abcdef01234567`
- captured bytes: `16544`
- MPEG-TS alignment: pass; 188-byte aligned and first byte is `0x47`
