# Native HLS Title Metadata Design

## Problem

The continuous MPEG-TS endpoint exposes the resolved stream title in the `icy-name` response
header. The native HLS media-playlist endpoint resolves and starts the same stream but returns only
the HLS content type, so players that use HTTP metadata display no title for the `.m3u8` URL.

## Scope

The native `GET /streams/{network}/{id}.m3u8` response will include the same sanitized and bounded
`icy-name` header as `GET /streams/{network}/{id}.ts` when the resolved stream has a non-empty
title. It will reuse the existing `icy_name_header` helper so both playback modes have identical
control-character filtering, length limits, and empty-title behavior.

The media playlist body and segment responses will remain unchanged. This avoids adding
`EXT-X-SESSION-DATA`, which belongs in a multivariant playlist rather than this media playlist,
and avoids introducing a second playlist layer solely for display metadata.

## Data flow and errors

After the manager starts or retrieves the native HLS packager, the handler will read the matching
session metadata already held by the manager and attach `icy-name` to the playlist response when
available. Provider and HLS startup failures will continue to return `404`. If no title is
available, the endpoint will still return a valid playlist without an `icy-name` header, matching
the direct TS endpoint's current behavior.

## Testing

A route-level regression test will request a native `.m3u8` stream from a provider with fixture
metadata and assert that the successful playlist response contains `icy-name: Synthetic Demo Channel`.
The test will be written and observed failing before the production change. Existing helper tests
continue to cover sanitization, bounds, and empty titles. Final verification will run formatting,
the focused engine tests, the full workspace suite, and Clippy with warnings denied.
