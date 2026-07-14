# Stream Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve Acestream descriptor metadata and expose its title to VLC and structured metadata to HTTP API consumers.

**Architecture:** Decode bounded descriptor categories in `ace-wire`, attach normalized metadata to `ace-swarm::StreamInfo`, and carry it through `TsSource` into the shared `StreamSession`. Continuous MPEG-TS responses derive a validated `Icy-Name` header from the session, while native and compatibility JSON serialize the same immutable metadata value.

**Tech Stack:** Rust 2021, Tokio, Axum HTTP responses, serde_json, existing Acestream transport decoder and engine provider/session abstractions.

## Global Constraints

- Work only in `/tmp/outpace-stream-metadata-130` on `feat/stream-metadata-130`, based on `main`.
- Use test-driven development: each production behavior must be preceded by a failing focused test.
- Do not rewrite MPEG-TS packets or inject DVB SDT tables.
- Do not change HLS media playlists into master playlists or add non-standard playlist tags.
- Missing metadata must preserve current playback and JSON behavior except for additive metadata fields.
- Treat descriptor strings as untrusted; bound decoded collections and validate every HTTP header value.
- Keep live-network tests ignored and keep the normal suite offline.

---

### Task 1: Decode and preserve descriptor metadata

**Files:**
- Modify: `crates/ace-wire/src/transport.rs`
- Modify: `crates/ace-swarm/src/types.rs`
- Modify: `crates/ace-swarm/src/resolve.rs`
- Modify: all `StreamInfo` test fixtures reported by `rg -n 'StreamInfo \\{' crates`
- Test: `crates/ace-wire/src/transport.rs`
- Test: `crates/ace-swarm/src/resolve.rs`

**Interfaces:**
- Produces: `ace_swarm::types::StreamMetadata { title, bitrate, categories }`.
- Produces: `StreamInfo::metadata: StreamMetadata` for the engine provider layer.
- Consumes: `TransportDescriptor::{name, bitrate, categories}` from `ace-wire`.

- [ ] **Step 1: Write failing transport-decoder tests**

Extend the existing synthetic transport round-trip test so the descriptor contains both list and
single-value category forms. Assert that list entries decode in order, empty/non-byte entries are
ignored, and the retained values are bounded. Add the public field:

```rust
pub struct TransportDescriptor {
    // existing fields
    pub categories: Vec<String>,
}
```

- [ ] **Step 2: Verify the decoder test fails**

Run: `cargo test -p ace-wire transport::tests::encode_then_decode_roundtrips -- --exact`

Expected: compilation fails because `TransportDescriptor` has no `categories` field.

- [ ] **Step 3: Implement bounded category decoding**

Add private constants `MAX_CATEGORIES: usize = 32` and `MAX_CATEGORY_BYTES: usize = 128`. Decode
`categories` when it is either `Bencode::List` or `Bencode::Bytes`, convert bytes lossily to UTF-8,
trim whitespace, truncate on a UTF-8 boundary, omit empty values, and stop after 32 values. Populate
the new descriptor field without modifying the raw dictionary used for infohash calculation.

- [ ] **Step 4: Verify transport decoding passes**

Run: `cargo test -p ace-wire transport::tests::encode_then_decode_roundtrips -- --exact`

Expected: PASS.

- [ ] **Step 5: Write failing stream metadata tests**

Define the desired public type and field in test assertions:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamMetadata {
    pub title: Option<String>,
    pub bitrate: Option<u64>,
    pub categories: Vec<String>,
}
```

Update `transport_yields_stream_info` to assert `Test`, `100000`, and decoded categories. Update
`infohash_form_uses_default_geometry` to assert `StreamMetadata::default()`.

- [ ] **Step 6: Verify stream metadata tests fail**

Run: `cargo test -p ace-swarm resolve::tests::transport_yields_stream_info -- --exact && cargo test -p ace-swarm resolve::tests::infohash_form_uses_default_geometry -- --exact`

Expected: compilation fails because `StreamMetadata` and `StreamInfo::metadata` do not exist.

- [ ] **Step 7: Implement metadata normalization in resolution**

Add `StreamMetadata` and `StreamInfo::metadata` in `types.rs`. In `resolve.rs`, construct metadata
from the decoded descriptor with these rules: title is trimmed and capped at 256 UTF-8 bytes;
positive bitrate converts to `u64`; zero/negative bitrate becomes `None`; categories come from the
bounded decoder. Bare infohash construction uses `StreamMetadata::default()`. Add empty metadata to
all direct `StreamInfo` fixtures.

- [ ] **Step 8: Run crate tests**

Run: `cargo test -p ace-wire && cargo test -p ace-swarm`

Expected: PASS with only existing ignored live-network tests.

- [ ] **Step 9: Commit the metadata model**

```bash
git add crates/ace-wire/src/transport.rs crates/ace-swarm/src/types.rs crates/ace-swarm/src/resolve.rs crates/ace-swarm/tests crates/ace-engine/src/ace_provider.rs
git commit -m "feat(ace-swarm): preserve stream descriptor metadata"
```

### Task 2: Carry metadata into shared engine sessions

**Files:**
- Modify: `crates/ace-engine/src/provider.rs`
- Modify: `crates/ace-engine/src/ace_provider.rs`
- Modify: `crates/ace-engine/src/session.rs`
- Modify: `crates/ace-engine/src/manager.rs`
- Test: `crates/ace-engine/src/session.rs`
- Test: `crates/ace-engine/src/manager.rs`

**Interfaces:**
- Consumes: `ace_swarm::types::StreamMetadata` from Task 1.
- Produces: `TsSource::metadata(&self) -> StreamMetadata` with an empty default.
- Produces: `StreamSession::metadata(&self) -> &StreamMetadata`.
- Produces: `StreamManager::list() -> Vec<(String, String, u64, StreamMetadata)>`.

- [ ] **Step 1: Write a failing session metadata test**

Create a minimal `MetadataSource` whose `metadata()` returns a title, bitrate, and category. Start a
session and assert that `session.metadata()` immediately equals the source value, before reading a
chunk.

- [ ] **Step 2: Verify the session test fails**

Run: `cargo test -p ace-engine session::tests::session_captures_source_metadata_before_pumping -- --exact`

Expected: compilation fails because the trait and session have no metadata methods.

- [ ] **Step 3: Implement the provider/session seam**

Add this default to `TsSource`:

```rust
fn metadata(&self) -> StreamMetadata {
    StreamMetadata::default()
}
```

Store a `StreamMetadata` on `AceSource` and return its clone. In `StreamSession::start`, call
`source.metadata()` before moving `source` into the spawned task, store the result, and expose an
immutable accessor. This keeps every metadata-free provider source-compatible.

- [ ] **Step 4: Verify the session test passes**

Run: `cargo test -p ace-engine session::tests::session_captures_source_metadata_before_pumping -- --exact`

Expected: PASS.

- [ ] **Step 5: Write and implement a manager-list metadata test**

Update the manager list test to start a metadata-bearing session and assert the fourth tuple item
is the session metadata. First run the test with the old three-item tuple and confirm a compile
failure, then update `StreamManager::list` to clone `s.metadata()` into each tuple.

- [ ] **Step 6: Run engine provider/session tests**

Run: `cargo test -p ace-engine session::tests && cargo test -p ace-engine manager::tests && cargo test -p ace-engine provider::tests`

Expected: PASS.

- [ ] **Step 7: Commit session propagation**

```bash
git add crates/ace-engine/src/provider.rs crates/ace-engine/src/ace_provider.rs crates/ace-engine/src/session.rs crates/ace-engine/src/manager.rs
git commit -m "feat(ace-engine): retain metadata on stream sessions"
```

### Task 3: Expose VLC title headers and structured HTTP metadata

**Files:**
- Modify: `crates/ace-engine/src/http.rs`
- Test: `crates/ace-engine/src/http.rs`

**Interfaces:**
- Consumes: `StreamSession::metadata()` and manager list metadata from Task 2.
- Produces: `stream_metadata_json(&StreamMetadata) -> serde_json::Value`.
- Produces: `icy_name_header(&StreamMetadata) -> Option<HeaderValue>`.
- Produces: additive `metadata` JSON objects and optional `Icy-Name` headers.

- [ ] **Step 1: Write failing title-header unit tests**

Add pure tests proving `icy_name_header` returns `Synthetic Demo Channel`, removes CR/LF/control characters,
caps output at 256 UTF-8 bytes without splitting a code point, and returns `None` for empty titles.

- [ ] **Step 2: Verify title-header tests fail**

Run: `cargo test -p ace-engine http::tests::icy_name_`

Expected: compilation fails because `icy_name_header` does not exist.

- [ ] **Step 3: Implement safe header construction**

Import `HeaderValue` and add an `ICY_NAME` static header name. Build the value only from the
trimmed title, filter control characters, truncate at a char boundary to 256 bytes, and call
`HeaderValue::from_bytes`. Return `None` on an empty or rejected result.

- [ ] **Step 4: Write failing continuous-TS response tests**

Use a metadata-bearing test provider to assert that native `.ts`, direct `/ace/getstream`, and
tokenized `/ace/r/...` responses contain `icy-name: Synthetic Demo Channel`. Add a metadata-free assertion
showing the header is absent.

- [ ] **Step 5: Verify response tests fail**

Run: `cargo test -p ace-engine http::tests::stream_metadata_title_`

Expected: assertions fail because responses only contain `Content-Type`.

- [ ] **Step 6: Add the title header to both response builders**

In `stream_session_response` and `ace_stream_session_response`, clone or borrow session metadata
before moving response state into the body stream. Build the response with `Content-Type`, then
conditionally insert `Icy-Name`. Do not add the header to HLS playlists or segments.

- [ ] **Step 7: Verify continuous response tests pass**

Run: `cargo test -p ace-engine http::tests::stream_metadata_title_`

Expected: PASS.

- [ ] **Step 8: Write failing JSON response tests**

Extend tests for `/streams`, `/status`, compatibility getstream/manifest JSON, and `/ace/stat` to
assert the nested object has exactly `title`, `bitrate`, and `categories`. Assert missing metadata
serializes as `{"title":null,"bitrate":null,"categories":[]}` and existing measured top-level
`bitrate` remains unchanged.

- [ ] **Step 9: Implement structured JSON exposure**

Add `stream_metadata_json`. Use the fourth manager-list tuple item for `/streams`; use session
metadata for `/status` and `/ace/stat`; add metadata to `AceStreamSelection`, populate it from a
successful catalog resolution, and use it for getstream JSON. Use the already-started session's
metadata for manifest JSON so URL and fallback content-id resolution are covered without another
lookup.

- [ ] **Step 10: Run HTTP tests**

Run: `cargo test -p ace-engine http::tests`

Expected: PASS.

- [ ] **Step 11: Commit client exposure**

```bash
git add crates/ace-engine/src/http.rs
git commit -m "feat(ace-engine): expose stream metadata to clients"
```

### Task 4: Document and verify issue #130

**Files:**
- Modify: `docs/native-api.md`
- Modify: `docs/protocol/compat-matrix.md`

**Interfaces:**
- Documents: optional `Icy-Name` on continuous MPEG-TS responses.
- Documents: stable nested `metadata` JSON object and HLS limitation.

- [ ] **Step 1: Update API documentation**

Document the `metadata` object fields, state that descriptor title is authoritative, describe the
optional `Icy-Name` header on continuous TS, and state that bare infohashes return null/empty
metadata. In the compatibility matrix, mark the JSON/stat fields additive and explain why media
playlists do not carry a portable stream title.

- [ ] **Step 2: Run formatting and focused tests**

Run: `cargo fmt --all --check && cargo test -p ace-wire && cargo test -p ace-swarm && cargo test -p ace-engine`

Expected: PASS.

- [ ] **Step 3: Run the repository quality gates**

Run: `cargo test --workspace`

Expected: PASS with only existing ignored live-network tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS with no warnings.

Run: `cargo fmt --all --check`

Expected: PASS.

- [ ] **Step 4: Review the diff for scope and safety**

Run: `git diff main...HEAD --check && git status --short && git diff --stat main...HEAD`

Expected: no whitespace errors, only issue #130 files, and no secrets, captures, binaries, or
local state.

- [ ] **Step 5: Commit documentation and final fixes**

```bash
git add docs/native-api.md docs/protocol/compat-matrix.md
git commit -m "docs: describe stream metadata responses"
```

- [ ] **Step 6: Publish a draft PR**

Push `feat/stream-metadata-130` and open a draft PR targeting `main`. The title is
`feat(ace-engine): expose stream metadata to clients`; the description must close #130, summarize
metadata propagation and VLC behavior, list all verification commands, and state that there are no
new network, Docker, release, environment-variable, or exposed-default impacts.
