# Extensionless MPEG-TS Playback Design

## Goal

Make `GET /streams/{network}/{id}` the default native MPEG-TS playback URL while preserving the existing explicit `.ts` and `.m3u8` formats. The extensionless response streams directly with `200 video/mp2t`; it does not redirect.

## Scope

This change affects only native live playback routes. VOD routes and the experimental `/ace/*` compatibility routes remain unchanged.

The final path component is interpreted as follows:

- `{id}.m3u8` uses the existing live HLS path.
- `{id}.ts` uses the existing continuous MPEG-TS path after stripping `.ts`.
- A component containing no `.` uses the continuous MPEG-TS path unchanged.
- Any other dotted component, including `{id}.mp4`, `{id}.foo`, and `{id}.`, returns `404` without starting a session.

Provider identifiers accepted by the extensionless form therefore cannot contain a dot. Callers with a provider identifier containing a dot must use the explicit `.ts` form only if that identifier can be represented unambiguously by the provider and routing contract; this change does not introduce escaping or content negotiation.

## HTTP Handler Design

The existing `/streams/:network/:file` route and `stream_file` handler remain the single entry point. The handler first recognizes `.m3u8`. It then normalizes `.ts` or an extensionless component to the same borrowed `id` and calls `StreamManager::get_or_start(&network, id)`. Unsupported dotted suffixes return `404` before provider work begins.

Both MPEG-TS URL forms pass the returned session to `stream_session_response`. That existing response builder supplies `Content-Type: video/mp2t`, creates the streaming body, and emits no `Location` header.

## Session Sharing

`StreamManager` keys live sessions by `(network, id)`. Stripping `.ts` and leaving the extensionless identifier unchanged produces the same key for `/streams/ace/channel.ts` and `/streams/ace/channel`. Concurrent or sequential clients therefore share one provider source and appear as separate subscribers on one session. No manager changes are needed.

## Error Handling

Unknown networks and provider start failures continue to map to `404`. Unsupported dotted suffixes are rejected locally with `404`. HLS start failures remain `404`, and successful `.m3u8` responses retain `application/vnd.apple.mpegurl`.

## Tests

HTTP tests will prove that:

- an extensionless request returns `200 video/mp2t` with no `Location` header;
- extensionless and `.ts` requests for the same identifier create one manager session with two direct subscribers;
- `.m3u8` continues to return an HLS playlist;
- `.foo` and a trailing dot continue to return `404` without starting a session.

The implementation follows test-driven development: add the extensionless behavior test first, run it to observe the current `404`, then make the smallest handler change and rerun the focused tests.

## Documentation

`docs/native-api.md` will list the extensionless route as the default continuous MPEG-TS endpoint and use it as the primary VLC example. The explicit `.ts` URL remains documented as an equivalent form, while `.m3u8` remains the explicit HLS opt-in. The daemon startup message will print the extensionless MPEG-TS URL so runtime guidance matches the supported default.
