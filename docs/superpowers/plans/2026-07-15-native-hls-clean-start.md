# Native HLS Clean-Start Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure a cold or late native HLS client receives a non-empty playlist whose retained TS segments begin with PAT/PMT and a real H.264 video access point.

**Architecture:** Extract the PAT/PMT/video-PID/access-point state already embedded in `KeyframeGate` into a reusable `ace-media` component. Use it in the live HLS packager to drop partial startup GOPs, build clean segment prefixes, and notify HTTP handlers only after the first complete segment is ready.

**Tech Stack:** Rust, Tokio `Notify`, Axum, MPEG-TS/H.264 packet inspection, Cargo tests, FFmpeg live validation.

## Global Constraints

- Do not transcode, convert codecs, synthesize SPS/PPS, or rewrite encoded audio/video payloads.
- Preserve existing native and compatibility HLS URLs, `icy-name`, token pins, activity leases, invalid-segment `404` behavior, and VOD HLS behavior.
- Every successfully advertised live segment starts with cached PAT and PMT packets followed by a detected H.264 video access point.
- A successful live playlist response contains at least one completed segment; timeout returns `503 Service Unavailable` with `Retry-After: 1`.
- Provider/network work remains outside manager lifecycle/map locks, and the HLS readiness wait creates no subscriber.
- `OUTPACE_HLS_SEGMENT_PACKETS` must be at least 3; the default remains 16,384.

---

### Task 1: Reusable MPEG-TS video access-point state

**Files:**
- Modify: `crates/ace-media/src/mpegts.rs:125-307`

**Interfaces:**
- Produces: `pub struct VideoAccessPointState` with `new()`, `reset()`, `observe(&mut self, packet: &[u8]) -> bool`, and `table_prefix(&self) -> Option<Vec<u8>>`.
- Preserves: `KeyframeGate::new`, `KeyframeGate::with_max_scan_packets`, and `KeyframeGate::push` behavior.

- [ ] **Step 1: Add failing access-point tests**

Add tests proving that the state learns PAT/PMT, ignores a random-access flag on a non-video PID,
accepts the same flag on the PMT-declared H.264 PID, returns PAT+PMT as a 376-byte prefix, and
forgets all state after `reset()`:

```rust
#[test]
fn video_access_point_state_uses_pmt_video_pid_and_caches_tables() {
    let mut state = VideoAccessPointState::new();
    let pat = pat(PMT_PID);
    let pmt = pmt(PMT_PID, VIDEO_PID);
    assert!(!state.observe(&pat));
    assert!(!state.observe(&pmt));
    assert!(!state.observe(&random_access_packet(0x102)));
    assert!(state.observe(&random_access_packet(VIDEO_PID)));
    assert_eq!(state.table_prefix().unwrap(), [pat, pmt].concat());
}

#[test]
fn video_access_point_state_reset_requires_fresh_tables() {
    let mut state = VideoAccessPointState::new();
    state.observe(&pat(PMT_PID));
    state.observe(&pmt(PMT_PID, VIDEO_PID));
    state.reset();
    assert!(!state.observe(&random_access_packet(VIDEO_PID)));
    assert!(state.table_prefix().is_none());
}
```

- [ ] **Step 2: Run the tests and verify RED**

```bash
cargo test -p ace-media mpegts::tests::video_access_point_state -- --nocapture
```

Expected: compilation fails because `VideoAccessPointState` does not exist.

- [ ] **Step 3: Extract the shared state and refactor `KeyframeGate`**

Implement the component with the existing private parsing helpers:

```rust
pub struct VideoAccessPointState {
    pmt_pid: Option<u16>,
    video_pid: Option<u16>,
    cached_pat: Option<Vec<u8>>,
    cached_pmt: Option<Vec<u8>>,
}

impl VideoAccessPointState {
    pub fn new() -> Self { Self::default() }

    pub fn reset(&mut self) { *self = Self::default(); }

    pub fn observe(&mut self, packet: &[u8]) -> bool {
        if packet.len() != TS_PACKET_LEN || packet[0] != 0x47 {
            return false;
        }
        let pid = ts_pid(packet);
        if pid == 0 {
            self.cached_pat = Some(packet.to_vec());
            self.pmt_pid = parse_pat_pmt_pid(packet);
        } else if Some(pid) == self.pmt_pid {
            self.cached_pmt = Some(packet.to_vec());
            self.video_pid = parse_pmt_video_pid(packet);
        }
        Some(pid) == self.video_pid && is_random_access_point(packet)
    }

    pub fn table_prefix(&self) -> Option<Vec<u8>> {
        let mut prefix = Vec::with_capacity(TS_PACKET_LEN * 2);
        prefix.extend_from_slice(self.cached_pat.as_ref()?);
        prefix.extend_from_slice(self.cached_pmt.as_ref()?);
        Some(prefix)
    }
}
```

Replace `KeyframeGate`'s duplicated PAT/PMT/video fields with `access: VideoAccessPointState` and
use `access.observe(pkt)` plus `access.table_prefix()` at lock-on.

- [ ] **Step 4: Verify GREEN and existing gate behavior**

```bash
cargo test -p ace-media mpegts::tests
```

Expected: all MPEG-TS tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-media/src/mpegts.rs
git commit -m "refactor(ace-media): share video access-point state"
```

---

### Task 2: Build independently joinable live HLS segments

**Files:**
- Modify: `crates/ace-engine/src/hls.rs:1-330`
- Modify: `crates/ace-engine/src/config.rs:140-180`
- Test: impacted live/VOD HTTP fixtures in `crates/ace-engine/src/http.rs`

**Interfaces:**
- Consumes: `ace_media::mpegts::VideoAccessPointState`.
- Produces: `HlsPackager::is_ready(&self) -> bool` and `HlsPackager::wait_ready(&self, timeout: Duration) -> bool`.

- [ ] **Step 1: Add failing clean-segment tests**

Construct PAT, PMT, video access-point, non-video random-access, and timed PCR packets. Add tests
that feed an arbitrary partial GOP before the first video access point and assert:

```rust
let segment = p.segment(0).expect("first clean segment");
assert_eq!(&segment[..188], &pat(PMT_PID));
assert_eq!(&segment[188..376], &pmt(PMT_PID, VIDEO_PID));
assert_eq!(&segment[376..564], &video_access_packet(VIDEO_PID, first_pcr));
assert!(!segment.windows(188).any(|packet| packet == partial_gop_packet));
```

Add a second-boundary test proving the next segment repeats PAT/PMT and begins on the next video
access point, while a random-access flag on another PID does not cut. Add a discontinuity test
proving stale PAT/PMT are not reused until fresh tables arrive.

- [ ] **Step 2: Run the clean-segment tests and verify RED**

```bash
cargo test -p ace-engine hls::tests::clean -- --nocapture
```

Expected: assertions fail because startup bytes are retained and table packets are not prefixed.

- [ ] **Step 3: Implement clean-start and clean-boundary packaging**

Add `VideoAccessPointState` and `awaiting_access_point` to `HlsState`. Observe each scanned TS
packet before boundary decisions. While awaiting the first clean start, discard buffered prefixes
until `observe` returns true and `table_prefix` is available. At a target-duration boundary, cut
only before a detected video access point and seed the new current segment with the cached table
prefix before the original access-point packet.

If the hard ceiling is reached without a later video access point, discard that incomplete run,
mark a discontinuity, reset timing, and await a fresh access point instead of advertising an
undecodable segment. On transport discontinuity, also reset `VideoAccessPointState`.

Ensure the two prefix packets count toward `max_segment_bytes` and no completed/current segment
exceeds the configured ceiling.

Validate a minimum of three packets. For an exact three-packet ceiling, treat the following access
packet as bounded lookahead so PAT + PMT + the prior access packet can complete without exceeding
the ceiling. Reject one and two explicitly; leave the 16,384 default unchanged.

- [ ] **Step 4: Add failing readiness tests**

```rust
#[tokio::test]
async fn readiness_waits_for_first_completed_segment_and_then_stays_ready() {
    let p = pkg();
    assert!(!p.is_ready());
    assert!(!p.wait_ready(Duration::from_millis(1)).await);
    feed_one_clean_segment(&p);
    assert!(p.wait_ready(Duration::from_millis(50)).await);
    assert!(p.is_ready());
}
```

Expected before implementation: compilation fails because readiness methods do not exist.

- [ ] **Step 5: Implement lost-wakeup-safe readiness**

Add `ready: tokio::sync::Notify` to `HlsPackager`. Notify after the first segment is pushed. Use a
state-check/notified loop bounded by `tokio::time::timeout`:

```rust
pub async fn wait_ready(&self, timeout: Duration) -> bool {
    if self.is_ready() { return true; }
    tokio::time::timeout(timeout, async {
        loop {
            let notified = self.ready.notified();
            if self.is_ready() { break; }
            notified.await;
        }
    }).await.is_ok()
}
```

- [ ] **Step 6: Verify all packager tests**

```bash
cargo test -p ace-engine hls::tests
```

Expected: all HLS tests pass, including duration, ceiling, activity, compatibility, and new
clean-start/readiness coverage.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/hls.rs
git commit -m "fix(ace-engine): emit clean live hls segments"
```

---

### Task 3: Wait for playable playlists at HTTP boundaries

**Files:**
- Modify: `crates/ace-engine/src/http.rs:920-950,1370-1405,2184-2205,3655-3680`

**Interfaces:**
- Consumes: `HlsPackager::wait_ready(Duration) -> bool`.
- Produces: native and tokenized live playlist responses that are non-empty on `200`, or retryable `503` on startup timeout.

- [ ] **Step 1: Add failing HTTP behavior tests**

Add a response helper that accepts an explicit short test timeout, then test:

```rust
assert_eq!(response.status(), StatusCode::OK);
let body = response_text(response).await;
assert!(body.lines().any(|line| !line.starts_with('#')));
```

For a provider that never yields media, assert:

```rust
assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
assert_eq!(response.headers()[header::RETRY_AFTER], "1");
```

Keep the existing native title assertion and add the same non-empty guarantee to the compatibility
playlist route without changing its no-touch activity test.

- [ ] **Step 2: Run focused HTTP tests and verify RED**

```bash
cargo test -p ace-engine http::tests::m3u8 -- --nocapture
cargo test -p ace-engine http::tests::ace_hls -- --nocapture
```

Expected: the cold native test observes an empty successful playlist and timeout behavior is
missing.

- [ ] **Step 3: Implement the bounded readiness response**

Define a production startup deadline of ten seconds and a shared async helper:

```rust
const HLS_PLAYLIST_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

async fn await_hls_ready(pkg: &HlsPackager, timeout: Duration) -> Result<(), Response> {
    if pkg.wait_ready(timeout).await {
        Ok(())
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
        ).into_response())
    }
}
```

Call it before rendering native or compatibility playlists. Preserve `icy-name`, content type,
cache control, activity/no-touch choice, token validation, and existing `404` paths.

- [ ] **Step 4: Verify focused engine suites**

```bash
cargo test -p ace-engine hls::tests
cargo test -p ace-engine manager::tests
cargo test -p ace-engine http::tests
```

Expected: all focused suites pass.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/http.rs
git commit -m "fix(ace-engine): wait for playable hls playlists"
```

---

### Task 4: Full and live verification

**Files:**
- Modify only if verification exposes a requirement gap in the files above.

- [ ] **Step 1: Run repository gates**

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 2: Cold-start the reported live stream**

Restart Outpace from this worktree, stop any existing instance of the reported stream, and request:

```text
http://127.0.0.1:6878/streams/ace/cid:cid5.m3u8
```

Expected: the first `200` playlist body already contains at least one segment URI and includes the
same `icy-name` title as direct TS.

- [ ] **Step 3: Verify immediate decoding**

Fetch the first URI from that first playlist and run:

```bash
ffmpeg -hide_banner -loglevel warning -i <segment-url> -map 0:v:0 -frames:v 1 -f null -
```

Expected: exit zero, at least one video frame decoded, and no `non-existing PPS`/`no frame` startup
errors.

- [ ] **Step 4: Publish and restart for user testing**

Push `fix/native-hls-playback`, confirm PR #133 points at the new head and is mergeable, then leave
the verified daemon running on `127.0.0.1:6878` with the existing worktree preserved.
