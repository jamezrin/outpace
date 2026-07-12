# 56 — Legacy live HLS adapter and evidence boundary

Issue #108 exposes Outpace's native live HLS window through the opt-in AceStream route family. It
is an HTTP adapter, not a second downloader or segmenter.

## Observed contract

The official [Start Playback documentation](https://docs.acestream.net/developers/start-playback/)
documents the following behavior:

- `/ace/manifest.m3u8` selects HLS output;
- its default output redirects or resolves to a media playlist;
- `format=json` returns `playback_url`, `stat_url`, `command_url`, `infohash`,
  `playback_session_id`, `is_live`, `is_encrypted`, and `client_session_id`;
- the JSON `playback_url` has the shape `/ace/m/<infohash>/<token>.m3u8`;
- media playlists reference `/ace/c/<session>/<sequence>.ts` and use absolute sequence numbers.

The official [API reference](https://docs.acestream.net/developers/api-reference/) documents
`redirect` and `json` as the playback endpoint formats, with `redirect` as the default. It also
lists selector and playlist/transcode-related parameters. The cloned AceServe repository packages
closed binaries and provides no readable implementation or stronger header/lifecycle evidence.
No current controlled-engine capture was available for malformed selectors, missing/expired
segments, cache headers, token expiry, VOD behavior, or coexistence with a second client.

## Pinned Outpace subset

Outpace therefore implements the documented successful live flow while pinning conservative local
behavior for unobserved edges:

| Request | Behavior |
| --- | --- |
| `/ace/manifest.m3u8?<selector>` | `302` to one stable tokenized playlist URL |
| same with `format=redirect` or empty `format` | same `302` behavior |
| same with `format=json` | `/ace/*` JSON envelope with HLS playback/stat/command URLs |
| another non-empty format | HTTP 200 error envelope: `unsupported format` |
| `/ace/m/<id>/<token>.m3u8` | live media playlist, `application/vnd.apple.mpegurl`, `Cache-Control: no-store` |
| `/ace/c/<token>/<seq>.ts` | retained bytes, `video/mp2t`, `Cache-Control: no-store` |
| malformed, future, evicted, expired, forged, or stopped media URL | HTTP 404 |

Selectors and catalog resolution are shared with note 55: `content_id` > `infohash` > legacy `id`
> guarded `url` > validated `magnet`. This avoids a second interpretation of `id=` and preserves
the SSRF-safe transport URL path. In production, resolved content ids use the resolved infohash in
public control URLs while retaining the catalog-backed `cid:` provider key internally.

The official example exposes an infohash-shaped segment session. Outpace instead emits the
unpredictable client lease token in `/ace/c/`; media players consume the URI from the playlist, and
the token prevents an unauthenticated segment probe from crossing client/session boundaries. This
is an intentional security hardening, not a claim of byte-for-byte route parity.

## One download and one packager

`StreamManager::get_or_start_hls` remains the only creation path. Native and compatibility
manifests for the same `(network, provider key)` receive the same `Arc<HlsPackager>`. The adapter
only asks that object to render its existing media sequence, durations, and discontinuity markers
with a different segment prefix. Segment requests read the same retained `Bytes`; they never start
a provider, session, or packager.

An HLS client has no long-lived response body, so its bounded `AceSessionStore` entry owns one
otherwise-unread `Subscription`. This makes the existing manager reaper see the client without
turning the internal packager receiver into a permanent subscriber. Stop, six-hour expiry, or
4096-entry capacity eviction drops only that lease's subscription. Other compatibility clients,
native consumers, and the shared packager remain intact.

Expiry uses one weak-reference reaper per `AceSessionStore`, not one sleeper per manifest request.
Mint, capacity eviction, stop, and revoke wake that reaper so it cancels its old wait and schedules
the exact earliest remaining HLS deadline. Thus request churn leaves at most the configured lease
capacity in stored pins and one cleanup worker, which exits when the store is dropped.

## Deliberate limits

- Only public, unencrypted live playback is claimed.
- VOD compatibility is deferred; native `/vod` HLS is unchanged.
- Playlist output, quality, and transcode flags are inert hints. Output is the native raw MPEG-TS
  HLS view; Outpace does not report that transcoding occurred. Optional transcoding belongs to #49.
- Player UI, EPG/catalog, premium/encrypted, and remote-control APIs remain non-goals.

A future official-engine capture may refine status codes, headers, token/path spelling, and VOD
behavior. It must not introduce duplicate package/download state or weaken selector and token
isolation without separate evidence and security review.
