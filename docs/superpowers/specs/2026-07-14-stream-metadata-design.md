# Stream Metadata Design

**Issue:** #130 — Expose transport stream metadata to playback clients

## Problem

Acestream transport descriptors already carry a human-readable `name`, a bitrate hint, and
optional categories. Outpace decodes those values in `TransportDescriptor`, but
`stream_info_from_transport` drops them when it constructs download-only `StreamInfo`. The
engine consequently has no title available when it creates HTTP playback responses. VLC falls
back to the URL and displays values such as `cid:<content-id>.ts` as the title.

## Goals

- Preserve the descriptor title, bitrate hint, and categories through live-stream resolution.
- Make resolved metadata available on the shared `StreamSession` without a second descriptor
  lookup.
- Set VLC's title for native and Acestream-compatible continuous MPEG-TS playback.
- Expose stable structured metadata in active-stream and compatibility JSON responses.
- Keep bare-infohash playback and providers without metadata working unchanged.
- Treat descriptor metadata as untrusted input at every HTTP boundary.

## Non-goals

- Rewriting MPEG-TS packets or injecting DVB SDT tables.
- Inventing metadata for bare infohashes.
- Changing the current HLS media-playlist shape into a master playlist.
- Adding dynamic programme, artist, or now-playing metadata that is not present in the
  transport descriptor.

## Considered approaches

### 1. Session metadata with an ICY title header (selected)

Add a small `StreamMetadata` value to the resolved stream, expose it from `TsSource`, and copy it
into `StreamSession` before the source moves into the background pump. Continuous TS response
builders add `Icy-Name` when a safe title is present. VLC's HTTP access code explicitly maps
`Icy-Name` to `vlc_meta_Title`, so this reaches the General/Metadata UI without changing media
bytes.

This approach performs no extra network lookup, keeps metadata aligned with the exact descriptor
used for playback, and works for both native and compatibility routes because they share a
`StreamSession`.

### 2. Resolve metadata again in each HTTP handler

Handlers could perform a separate descriptor lookup before building a response. That duplicates
catalog or transport-URL I/O, creates separate caching and failure semantics, and can disagree
with the source selected by the provider. It is rejected.

### 3. Inject MPEG-TS SDT service metadata

VLC also maps DVB SDT service name and provider fields to programme metadata. Injecting SDT into
an arbitrary pass-through stream, however, requires discovering transport/program identifiers,
handling an existing PID 0x11, maintaining continuity counters, repeating PSI at the right
cadence, and avoiding corruption across discontinuities. It is unnecessary because VLC consumes
`Icy-Name` directly, so it is rejected for this issue.

`Content-Disposition` was also investigated. It supplies a filename hint but VLC does not map it
to `vlc_meta_Title`, so it is not the primary title mechanism.

## Data model and flow

`ace-swarm` defines `StreamMetadata` with:

- `title: Option<String>`
- `bitrate: Option<u64>` in bits per second
- `categories: Vec<String>`

`StreamInfo` owns a `metadata: StreamMetadata`. Transport resolution populates it from the
decoded descriptor. Bare-infohash resolution uses `StreamMetadata::default()`.

`ace-engine::provider::TsSource` gains a default `metadata()` method returning empty metadata.
`AceSource` retains the resolved metadata and overrides this method. `StreamSession::start`
copies the metadata before moving the source into its pump and exposes it through an immutable
accessor. Existing providers require no changes unless they have metadata to publish.

The data flow is:

```text
transport descriptor
    -> StreamInfo.metadata
    -> AceSource.metadata()
    -> StreamSession.metadata()
    -> HTTP headers and JSON
```

## HTTP behavior

Native `/streams/{network}/{id}.ts`, direct `/ace/getstream`, and tokenized `/ace/r/...`
responses add `Icy-Name` when the session has a usable title. Responses without a title remain
byte-for-byte and header-compatible with current behavior.

All metadata-derived headers are constructed with the HTTP library's validated `HeaderValue`.
Titles are trimmed, empty values are omitted, control characters are removed, and the output is
bounded to 256 UTF-8 bytes without splitting a code point. If the result cannot be represented as
a header value, the header is omitted while playback continues.

HLS endpoints continue returning media playlists. `EXT-X-SESSION-DATA` is valid only in a master
playlist, and segment `EXTINF` titles are not stream titles, so neither is added. Compatibility
manifest JSON may expose metadata obtained during selector resolution, and active-session status
surfaces expose session metadata.

## JSON behavior

Metadata is represented as a nested object with stable fields:

```json
{
  "metadata": {
    "title": "Synthetic Demo Channel",
    "bitrate": 100000,
    "categories": ["sports"]
  }
}
```

The object is included in native `/streams` entries, native per-stream `/status`, compatibility
getstream/manifest JSON when selector resolution supplied metadata, and compatibility `/stat`
for an active session. Missing scalar values serialize as `null`; missing categories serialize
as an empty array. Existing top-level statistics, including measured `bitrate`, keep their current
meaning and names.

## Descriptor decoding

`TransportDescriptor` promotes `categories` from its existing raw dictionary to a decoded
`Vec<String>`. The decoder accepts either a list of byte strings or a single byte string, ignores
non-string elements, trims empty entries, and caps the retained category count and lengths. A
negative or zero bitrate becomes `None`; a positive bitrate is converted to `u64`.

The title is normalized at resolution time for API use, then independently validated at the HTTP
header boundary. Keeping header validation separate prevents a future metadata source from
bypassing response safety.

## Testing

- `ace-wire`: transport decoding extracts categories without changing descriptor hashing.
- `ace-swarm`: transport resolution preserves title, bitrate, and categories; bare infohashes
  produce empty metadata.
- `ace-engine`: source metadata is captured by `StreamSession`; native and compatibility TS
  responses set `Icy-Name`; missing/unsafe titles do not break responses; native status/list and
  compatibility JSON/stat expose stable metadata.
- Existing multiclient, HLS, resolution, and compatibility tests remain green.

## Documentation

Update `docs/native-api.md` with the metadata object and continuous-TS title header. Update
`docs/protocol/compat-matrix.md` with the additive compatibility JSON/stat fields and clarify that
HLS media playlists do not carry a portable stream-title tag.
