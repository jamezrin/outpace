# SDT `service_name` title injection — design

**Issue:** #135 (Expose stream title to players via MPEG-TS SDT `service_name`)
**Related:** #134 (removed `icy-name` from HLS manifests)
**Date:** 2026-07-15

## Goal

Surface the resolved stream title to media players by writing it into the MPEG-TS
**SDT** (Service Description Table) `service_name`, and make that the **single** title
mechanism for **both** the `.ts` and `.m3u8` paths — replacing the `icy-name` header
entirely.

Players read the channel/program name from the TS SDT (for both direct `.ts` and HLS,
since segments are TS). Today outpace never emits or rewrites an SDT, so the resolved
title (e.g. `EUROSPORT 1 1080 …`, which lives only in `StreamMetadata` from the swarm and
is exposed via the JSON endpoints) never reaches the player through the media. What a
player shows today is the generic **upstream** SDT that passes through in the passthrough
TS (`service_name=Service01`, `service_provider=FFmpeg`).

### Acceptance (from the issue)

- ffprobe on `/streams/ace/cid:<id>.ts` reports `service_name=<title>` (not `Service01`).
- ffprobe on an HLS segment (`/seg/N.ts`) likewise reports the title.
- VLC displays the title while playing **both** the `.ts` and `.m3u8` URLs.
- No `icy-*` header on any response (including `.ts`); no `icyx://` promotion.
- No regression to HLS playback (segments still fetched; no busy-loop).

## Settled decisions

These resolve the issue's open questions:

1. **Filter upstream SDT + synthesize our own** (not plain prepend, not in-stream
   rewrite). The passthrough TS carries a *repeating* generic upstream SDT on PID
   `0x0011`. A plain "synthesize + prepend once" risks that repeating upstream SDT
   overriding our title downstream. So: drop all PID `0x0011` packets from the
   passthrough and inject a single synthesized SDT alongside PAT/PMT. Deterministic,
   simple, guarantees only our `service_name` reaches the player.
2. **Raw sanitized title, no shortening.** Reuse the existing icy sanitization (trim,
   strip control chars) and byte-cap the title so the whole SDT section fits one 188-byte
   TS packet. No ad/promo-stripping heuristics (YAGNI).
3. **`service_id` binds to the real program.** Take the `service_id` from the cached
   PAT's `program_number` so players associate the `service_name` with the program.
4. **`service_provider_name = "outpace"`** (from the issue).
5. **Gate the whole mechanism on title-present.** When there is no resolved title,
   behave exactly as today: nothing is filtered, the upstream SDT passes through, output
   is byte-for-byte unchanged.
6. **One SDT per prefix, no extra periodic re-injection.** The `.ts` path emits the SDT
   once at keyframe lock; HLS re-carries it per segment. Since upstream `0x0011` is
   filtered, ours is the sole SDT and demuxers cache PSI, so one authoritative copy per
   prefix is sufficient.

## Architecture

Both playback paths already converge on `VideoAccessPointState::table_prefix()` (in the
zero-dependency `ace-media` leaf crate) for their PAT/PMT prefix:

- **`.ts`** — `KeyframeGate` emits the prefix once, at the first keyframe lock, then
  passes packets through verbatim.
- **HLS** — `HlsPackager` splices the prefix in at the start of every segment (multiple
  call sites in `hls.rs`).

`table_prefix()` is therefore the single chokepoint: making it return `[PAT][PMT][SDT]`
covers `.ts` and `.m3u8` uniformly.

`ace-media` has no dependencies, so the title crosses the crate boundary as a **plain
sanitized string** — no `ace-swarm`/`StreamMetadata` coupling.

## Components

### 1. `ace-media::mpegts` — SDT synthesis (new)

- `build_sdt(service_id: u16, service_name: &str) -> [u8; 188]`: one TS packet on PID
  `0x0011`:
  - `table_id = 0x42` (actual TS SDT), `section_syntax_indicator = 1`.
  - `transport_stream_id` / `original_network_id`: fixed constants (players key off
    `service_id`, not these).
  - one service-loop entry with `service_id = <program_number>`.
  - a service descriptor (`0x48`): `service_type` (digital TV), length-prefixed
    `service_provider_name = "outpace"` and length-prefixed `service_name = <title>`.
  - correct DVB `section_length` / descriptor-loop-length fields.
  - a computed **CRC-32** (MPEG-2 systems poly `0x04C11DB7`, MSB-first, init `0xFFFFFFFF`,
    no final XOR — same CRC PSI/SI uses).
  - remainder of the 188 bytes padded with `0xFF`.
- **Title sanitization lives here** (the leaf crate owns "produce a valid `service_name`
  that fits one packet"): trim, strip control chars, trim again, then byte-cap on a UTF-8
  char boundary so the assembled section fits within one 184-byte TS payload. This is the
  logic moved out of `http.rs::icy_name_header`, whose only caller is being deleted. An
  empty result after sanitization yields no SDT.

### 2. `VideoAccessPointState` — carry the title

- Add `service_name: Option<String>` (already-sanitized), set at construction via a new
  constructor (e.g. `with_service_name`); `new()`/`Default` keep `None`.
- When caching the PAT, also parse and store the `program_number` (extend the existing
  `parse_pat_pmt_pid` walk, which already reads `program`).
- `table_prefix()` returns:
  - `[PAT][PMT][SDT]` when `service_name` is `Some` and a `program_number` is known,
  - `[PAT][PMT]` otherwise (unchanged behavior).

### 3. Upstream SDT filter — only when titled

Engages only when a `service_name` is present; otherwise both paths are unchanged.

- **`KeyframeGate`** owns a `VideoAccessPointState`, so it knows whether a title is set.
  In the post-lock passthrough loop, drop packets with `pid == 0x0011`.
- **`HlsPackager::scan`** (and `scan_lookahead`): when a scanned packet has
  `pid == 0x0011`, `drain` it from `st.cur` — the same in-place drain the discontinuity
  branch already performs — keeping the segment 188-aligned. Upstream SDT never reaches an
  emitted segment; the synthesized SDT arrives via `table_prefix()` at each segment start.

### 4. `ace-engine::http` — wire the title, delete icy

- Thread `session.metadata().title` into the `.ts` responses:
  `KeyframeGate::new(...)` gains the sanitized title (via the new
  `VideoAccessPointState` constructor). Both `stream_session_response` and
  `ace_stream_session_response` pass it.
- `HlsPackager` already holds `&StreamSession`, so it reads the title from
  `session.metadata()` when constructing its `VideoAccessPointState`.
- **Delete** `icy_name_header`, `MAX_ICY_NAME_BYTES`, every `icy-name` response header on
  the `.ts` paths, and the associated tests. After this, **no `icy-*` header on any
  path** and no `icyx://` promotion anywhere.

## Data flow

```
StreamMetadata.title
  └─ sanitize + byte-cap (ace-media)         → valid service_name (fits one TS packet)
       └─ VideoAccessPointState.service_name
            └─ build_sdt(program_number, name) → one 0x0011 TS packet (CRC-correct)
                 └─ table_prefix() = [PAT][PMT][SDT]
                      ├─ .ts:  emitted once at keyframe lock (KeyframeGate)
                      └─ HLS:  spliced at the start of every segment (HlsPackager)
   upstream 0x0011 packets ── filtered out of both passthroughs (when titled) ──▶ dropped
```

## Testing

- **`ace-media` unit tests:**
  - `build_sdt` CRC matches a known-good vector; the section parses back with the expected
    `service_id`, `service_provider_name`, and `service_name`.
  - `service_id` equals the PAT `program_number` (not a hardcoded 1).
  - Control-char strip + trim + byte-cap: a control-laden / overlong / multibyte title
    yields a section that fits one packet and never splits a UTF-8 char.
  - `table_prefix()` includes the SDT iff a title is set and a program_number is known.
- **`KeyframeGate` / `HlsPackager` tests:**
  - Upstream `0x0011` packets are dropped from output when titled, preserved (byte-for-
    byte) when untitled; emitted bytes remain 188-aligned.
- **Round-trip / acceptance-in-process:** feed a synthesized TS (PAT + PMT + keyframe +
  an upstream `Service01` SDT) through each path and re-read the emitted SDT, asserting
  `service_name` round-trips to the injected title and `Service01` is gone. This mirrors
  the ffprobe acceptance check without needing a live source.
- **`http.rs`:** remove the icy tests; add/adjust assertions that no `icy-*` header is
  present on `.ts` or `.m3u8` responses.

## Out of scope (YAGNI)

- No title shortening / ad-stripping — the raw sanitized title is used.
- No periodic SDT re-injection on the long-lived `.ts` beyond the one at keyframe lock;
  filtering makes ours the sole SDT and PSI is cached by demuxers, and each HLS segment
  already re-carries it.
- No in-stream rewrite of upstream SDT fields (we discard and replace, not patch).
