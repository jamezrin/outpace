# Extensionless MPEG-TS Playback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `GET /streams/{network}/{id}` a direct MPEG-TS alias of `GET /streams/{network}/{id}.ts` while preserving HLS and unknown-suffix behavior.

**Architecture:** Keep the existing `/streams/:network/:file` route and normalize the final path component inside `stream_file`. Both extensionless and `.ts` forms call the same `StreamManager::get_or_start` path with the same identifier, so the existing manager provides shared-session behavior and the existing response builder provides direct `video/mp2t` streaming.

**Tech Stack:** Rust, Tokio, Axum 0.7, Tower test utilities, Cargo.

## Global Constraints

- Work on branch `feat/extensionless-mpegts-129`, created from `main`.
- Native live playback only; do not change VOD routes or experimental `/ace/*` compatibility routes.
- `.m3u8` remains explicit HLS, `.ts` remains explicit MPEG-TS, and unsupported dotted suffixes return `404` without starting a session.
- The extensionless response is direct `200 video/mp2t` and must not contain a `Location` header.
- Do not add content negotiation or new dependencies.

---

### Task 1: Add the extensionless MPEG-TS alias

**Files:**
- Modify: `crates/ace-engine/src/http.rs:1329-1352`
- Test: `crates/ace-engine/src/http.rs:3324-3402`

**Interfaces:**
- Consumes: `StreamManager::get_or_start(&Arc<Self>, &str, &str) -> Result<Arc<StreamSession>, ProviderError>` and `stream_session_response(Arc<StreamSession>) -> Response`.
- Produces: `stream_file` behavior that maps `.m3u8` to HLS, `.ts` and dot-free path components to MPEG-TS, and all other dotted components to `404`.

- [ ] **Step 1: Write the failing extensionless alias test**

Add this test beside `unknown_network_returns_404` and keep both responses alive until after the subscriber assertion:

```rust
#[tokio::test]
async fn extensionless_stream_is_a_direct_mpegts_alias() {
    let st = state();
    let app = router(st.clone());

    let extensionless = app
        .clone()
        .oneshot(
            Request::get("/streams/test/channel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(extensionless.status(), StatusCode::OK);
    assert_eq!(
        extensionless.headers()[header::CONTENT_TYPE],
        "video/mp2t"
    );
    assert!(!extensionless.headers().contains_key(header::LOCATION));

    let explicit = app
        .oneshot(
            Request::get("/streams/test/channel.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(explicit.status(), StatusCode::OK);
    assert_eq!(st.manager.list().await, vec![(
        "test".to_string(),
        "channel".to_string(),
        2,
    )]);
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
cargo test -p ace-engine http::tests::extensionless_stream_is_a_direct_mpegts_alias -- --exact --nocapture
```

Expected: FAIL at the first status assertion because the extensionless response is currently `404` instead of `200`.

- [ ] **Step 3: Implement minimal path normalization**

Replace the `.ts`-only extraction in `stream_file` with:

```rust
let id = if let Some(id) = file.strip_suffix(".ts") {
    id
} else if file.contains('.') {
    return StatusCode::NOT_FOUND.into_response();
} else {
    &file
};
```

Leave the earlier `.m3u8` branch and the later `get_or_start`/`stream_session_response` calls unchanged. Update the handler doc comment to mention the extensionless MPEG-TS form.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run:

```bash
cargo test -p ace-engine http::tests::extensionless_stream_is_a_direct_mpegts_alias -- --exact --nocapture
```

Expected: PASS; both responses use one manager entry whose subscriber count is `2`.

- [ ] **Step 5: Extend unknown-suffix regression coverage**

Replace `non_ts_extension_returns_404` with:

```rust
#[tokio::test]
async fn unsupported_dotted_suffixes_return_404_without_starting_sessions() {
    for path in [
        "/streams/test/x.mp4",
        "/streams/test/x.foo",
        "/streams/test/x.",
    ] {
        let st = state();
        let resp = router(st.clone())
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{path}");
        assert!(st.manager.list().await.is_empty(), "{path}");
    }
}
```

- [ ] **Step 6: Run all ace-engine HTTP tests**

Run:

```bash
cargo test -p ace-engine http::tests
```

Expected: all HTTP tests pass, including the existing `.m3u8` playlist test.

- [ ] **Step 7: Commit the behavior and tests**

```bash
git add crates/ace-engine/src/http.rs
git commit -m "feat(ace-engine): default extensionless playback to MPEG-TS"
```

### Task 2: Document extensionless playback as the default

**Files:**
- Modify: `docs/native-api.md:29-38,64-75`
- Modify: `crates/ace-engine/src/runtime.rs:625`

**Interfaces:**
- Consumes: the native playback contract implemented in Task 1.
- Produces: route documentation, VLC examples, and startup output that present the extensionless URL as the default while retaining explicit `.ts` and `.m3u8` alternatives.

- [ ] **Step 1: Update the native API route table**

Add the default route immediately before the explicit `.ts` route and clarify equivalence:

```markdown
| `GET /streams/<network>/<id>` | `200 video/mp2t` streaming body | Default live playback; starts or joins the same shared session as the explicit `.ts` form. Unknown networks or invalid ids return `404`. |
| `GET /streams/<network>/<id>.ts` | `200 video/mp2t` streaming body | Explicit continuous MPEG-TS form; equivalent to the extensionless route. |
```

Keep the `.m3u8`, segment, status, deletion, VOD, broadcast, and compatibility documentation unchanged.

- [ ] **Step 2: Update the VLC and middleware examples**

Replace the player introduction and URL block with:

````markdown
Point VLC or a media-server channel at the extensionless native URL for direct MPEG-TS playback:

```text
http://127.0.0.1:6878/streams/ace/<id>
```

The explicit MPEG-TS and HLS forms remain available:

```text
http://127.0.0.1:6878/streams/ace/<id>.ts
http://127.0.0.1:6878/streams/ace/<id>.m3u8
```
````

Change the following dispatcharr sentence to say “generate entries using one of these URLs.”

- [ ] **Step 3: Update the daemon startup hint**

Change the MPEG-TS startup line to:

```rust
eprintln!("  MPEG-TS: http://{}/streams/ace/<id>", config.bind);
```

- [ ] **Step 4: Check formatting and inspect the documentation diff**

Run:

```bash
cargo fmt --all --check
git diff --check
git diff -- docs/native-api.md crates/ace-engine/src/runtime.rs
```

Expected: formatting and whitespace checks pass; the diff contains only the documented route/example and startup-hint changes.

- [ ] **Step 5: Commit the documentation**

```bash
git add docs/native-api.md crates/ace-engine/src/runtime.rs
git commit -m "docs(ace-engine): show extensionless MPEG-TS playback"
```

### Task 3: Verify the complete change

**Files:**
- Verify: `crates/ace-engine/src/http.rs`
- Verify: `crates/ace-engine/src/runtime.rs`
- Verify: `docs/native-api.md`

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: evidence that issue #129 meets its acceptance criteria without regressions.

- [ ] **Step 1: Run the full offline test suite**

```bash
cargo test --workspace
```

Expected: all non-ignored workspace tests pass.

- [ ] **Step 2: Run the lint gate**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: exit code `0` with no warnings.

- [ ] **Step 3: Run formatting and whitespace checks**

```bash
cargo fmt --all --check
git diff --check main...HEAD
```

Expected: both commands exit `0`.

- [ ] **Step 4: Confirm branch ancestry and review the final diff**

```bash
git merge-base --is-ancestor main HEAD
git status --short --branch
git diff --stat main...HEAD
git diff main...HEAD -- crates/ace-engine/src/http.rs crates/ace-engine/src/runtime.rs docs/native-api.md
```

Expected: `main` is an ancestor, the worktree is clean, and the implementation diff is limited to the native handler/tests, startup hint, and native API documentation (plus the committed design and plan documents).
