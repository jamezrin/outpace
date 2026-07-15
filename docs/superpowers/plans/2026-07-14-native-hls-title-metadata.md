# Native HLS Title Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make native `.m3u8` playlist responses expose the same `icy-name` stream title as direct MPEG-TS responses.

**Architecture:** Keep the existing native HLS media playlist and packager unchanged. After `get_or_start_hls` succeeds, retrieve the matching live session from `StreamManager`, pass its metadata through the existing `icy_name_header` sanitizer, and attach the resulting header to the playlist response.

**Tech Stack:** Rust, Axum HTTP responses, Tokio route tests, Cargo.

## Global Constraints

- The media playlist body and segment responses remain unchanged.
- Reuse `icy_name_header`; do not duplicate title sanitization or length limits.
- Missing or empty titles omit `icy-name`, matching direct TS behavior.
- Provider and HLS startup failures continue to return `404`.

---

### Task 1: Add native HLS title response metadata

**Files:**
- Modify: `crates/ace-engine/src/http.rs:1381-1390`
- Test: `crates/ace-engine/src/http.rs:3649-3667`

**Interfaces:**
- Consumes: `StreamManager::get(&self, network: &str, id: &str) -> Option<Arc<StreamSession>>`, `StreamSession::metadata(&self) -> &StreamMetadata`, and `icy_name_header(&StreamMetadata) -> Option<HeaderValue>`.
- Produces: a successful native `.m3u8` `Response` with optional `icy-name` header and the existing playlist body.

- [ ] **Step 1: Write the failing route test**

Change the existing HLS playlist test to use the metadata-bearing fixture provider and assert the
title header before consuming the body:

```rust
#[tokio::test]
async fn m3u8_serves_hls_playlist_with_stream_title() {
    let resp = router(fixture_state(0))
        .oneshot(
            Request::get("/streams/fix/chan.m3u8")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()[header::CONTENT_TYPE],
        "application/vnd.apple.mpegurl"
    );
    assert_eq!(resp.headers()["icy-name"], "Synthetic Demo Channel");
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).starts_with("#EXTM3U"));
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine http::tests::m3u8_serves_hls_playlist_with_stream_title -- --exact
```

Expected: FAIL when indexing the absent `icy-name` header, proving the route does not yet expose
the title.

- [ ] **Step 3: Add the minimal playlist response header**

Replace the successful native HLS response arm with a mutable response that looks up the session
started by `get_or_start_hls` and inserts the existing sanitized header:

```rust
Ok(pkg) => {
    let icy_name = s
        .manager
        .get(&network, id)
        .await
        .and_then(|session| icy_name_header(session.metadata()));
    let mut response = (
        [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
        pkg.playlist(&network, id),
    )
        .into_response();
    if let Some(value) = icy_name {
        response.headers_mut().insert("icy-name", value);
    }
    response
}
```

- [ ] **Step 4: Run the focused test and verify GREEN**

Run:

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine http::tests::m3u8_serves_hls_playlist_with_stream_title -- --exact
```

Expected: PASS.

- [ ] **Step 5: Verify formatting, regression tests, and lints**

Run:

```bash
cargo fmt --all --check
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test --workspace
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo clippy --workspace --all-targets -- -D warnings
```

Expected: all commands exit successfully with no formatting differences, test failures, or Clippy
warnings.

- [ ] **Step 6: Commit the implementation**

```bash
git add crates/ace-engine/src/http.rs
git commit -m "fix(ace-engine): expose title on native hls playlists"
```

- [ ] **Step 7: Publish and restart the test daemon**

Push `fix/native-hls-playback`, stop the currently running test daemon, start the updated daemon
with the shared build directory, and verify health and live HLS metadata:

```bash
git push origin fix/native-hls-playback
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo run -p ace-engine --bin outpace -- serve
curl --fail --silent http://127.0.0.1:6878/healthz
curl --head --max-time 20 http://127.0.0.1:6878/streams/ace/cid:cid5.m3u8
```

Expected: health returns `ok`; the playlist response includes
`content-type: application/vnd.apple.mpegurl` and the same `icy-name` title as the `.ts` endpoint.
