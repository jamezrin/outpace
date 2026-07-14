# Native HLS Playback Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep actively consumed native HLS streams alive and let default live segments reach PCR/keyframe boundaries without losing the existing memory ceiling.

**Architecture:** Store monotonic last-access time inside each `HlsPackager`; successful playlist and segment reads refresh it. The manager reaper snapshots recently active packager keys without nesting manager-map locks, retains matching zero-subscriber sessions, and removes truly idle session/packager pairs. Increase only the default packet ceiling to 16,384 while preserving explicit overrides and the current PCR-first segmentation algorithm.

**Tech Stack:** Rust 2024 workspace, Tokio, Axum, MPEG-TS/HLS packager, existing unit and HTTP test infrastructure.

## Global Constraints

- Preserve `GET /streams/{network}/{id}.m3u8` and existing playlist/segment response shapes.
- Production HLS inactivity grace remains exactly 30 seconds; do not add a configuration knob.
- Successful playlist and retained-segment reads refresh activity; invalid segment probes do not.
- `OUTPACE_HLS_SEGMENT_PACKETS` remains an authoritative hard ceiling/fallback when explicitly set.
- Default `segment_packets` becomes exactly `16_384` (3,080,192 bytes per segment).
- Do not transcode, remux, change continuous TS playback, or alter tokenized Ace compatibility leases.
- Do not hold the manager session and packager map locks simultaneously.
- Work on branch `fix/native-hls-playback`, based on `main`.

---

### Task 1: Track successful HLS media access

**Files:**
- Modify: `crates/ace-engine/src/hls.rs`
- Test: `crates/ace-engine/src/hls.rs` (`tests` module)

**Interfaces:**
- Produces: `HlsPackager::was_accessed_within(&self, now: Instant, grace: Duration) -> bool` for manager retention decisions.
- Changes: playlist rendering and successful `segment` reads refresh `HlsState.last_access`; missing segments leave it unchanged.
- Consumes: `std::time::{Duration, Instant}` and the existing `HlsPackager.state` mutex.

- [ ] **Step 1: Write failing access tests**

Add inside `hls.rs::tests`:

```rust
#[test]
fn playlist_and_valid_segment_reads_refresh_activity() {
    let p = pkg();
    p.feed(&packets(2));
    let stale = Instant::now() - Duration::from_secs(60);

    p.state.lock().unwrap().last_access = stale;
    let _playlist = p.playlist("test", "active");
    assert!(p.state.lock().unwrap().last_access > stale);

    p.state.lock().unwrap().last_access = stale;
    assert!(p.segment(0).is_some());
    assert!(p.state.lock().unwrap().last_access > stale);
}

#[test]
fn invalid_segment_probe_does_not_refresh_activity() {
    let p = pkg();
    let stale = Instant::now() - Duration::from_secs(60);
    p.state.lock().unwrap().last_access = stale;

    assert!(p.segment(99).is_none());
    assert_eq!(p.state.lock().unwrap().last_access, stale);
}
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine playlist_and_valid_segment_reads_refresh_activity
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine invalid_segment_probe_does_not_refresh_activity
```

Expected: compilation fails because `HlsState` has no `last_access` field.

- [ ] **Step 3: Implement monotonic activity tracking**

Import time types, add `last_access: Instant` to `HlsState`, and initialize it with
`Instant::now()`. In `playlist_with_segment_prefix`, replace the existing immutable state guard
with these two lines before constructing `out`:

```rust
let mut st = self.state.lock().unwrap();
st.last_access = Instant::now();
```

Add `use std::time::{Duration, Instant};` at module scope. Replace `segment` with:

```rust

pub fn segment(&self, seq: u64) -> Option<Bytes> {
    let mut st = self.state.lock().unwrap();
    if seq < st.media_seq {
        return None;
    }
    let bytes = st
        .segments
        .get((seq - st.media_seq) as usize)
        .map(|segment| segment.bytes.clone());
    if bytes.is_some() {
        st.last_access = Instant::now();
    }
    bytes
}

pub(crate) fn was_accessed_within(&self, now: Instant, grace: Duration) -> bool {
    now.saturating_duration_since(self.state.lock().unwrap().last_access) < grace
}
```

- [ ] **Step 4: Run HLS tests and verify GREEN**

Run: `CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine hls::tests`

Expected: all `hls::tests` pass, including both new activity tests.

- [ ] **Step 5: Commit the activity primitive**

```bash
git add crates/ace-engine/src/hls.rs
git commit -m "fix(ace-engine): track successful hls media access"
```

---

### Task 2: Retain recently active native HLS sessions

**Files:**
- Modify: `crates/ace-engine/src/manager.rs`
- Test: `crates/ace-engine/src/manager.rs` (`tests` module)

**Interfaces:**
- Consumes: `HlsPackager::was_accessed_within(Instant, Duration) -> bool` from Task 1.
- Produces: private `StreamManager::reap_idle_at(&self, now: Instant)` used by production and tests.
- Preserves: `StreamManager::stop` immediately removes both maps regardless of activity.

- [ ] **Step 1: Write failing lifecycle tests**

Add inside `manager.rs::tests`:

```rust
#[tokio::test]
async fn recent_hls_playlist_access_survives_idle_reap() {
    let m = StreamManager::new(registry());
    let pkg = m.get_or_start_hls("test", "active-hls").await.unwrap();
    let _playlist = pkg.playlist("test", "active-hls");

    m.reap_idle_at(Instant::now() + m.grace / 2).await;

    assert!(m.get("test", "active-hls").await.is_some());
    assert!(m.get_hls("test", "active-hls").await.is_some());
}

#[tokio::test]
async fn inactive_hls_session_and_packager_are_reaped_together() {
    let m = StreamManager::new(registry());
    m.get_or_start_hls("test", "stale-hls").await.unwrap();

    m.reap_idle_at(Instant::now() + m.grace + Duration::from_secs(1)).await;

    assert!(m.get("test", "stale-hls").await.is_none());
    assert!(m.get_hls("test", "stale-hls").await.is_none());
}
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine recent_hls_playlist_access_survives_idle_reap
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine inactive_hls_session_and_packager_are_reaped_together
```

Expected: compilation fails because `StreamManager::reap_idle_at` does not exist.

- [ ] **Step 3: Implement a non-nested reap pass**

Import `HashSet` and add:

```rust
async fn reap_idle_at(&self, now: Instant) {
    let active_hls: HashSet<_> = {
        let packagers = self.packagers.lock().await;
        packagers
            .iter()
            .filter(|(_, pkg)| pkg.was_accessed_within(now, self.grace))
            .map(|(key, _)| key.clone())
            .collect()
    };

    let retained_sessions: HashSet<_> = {
        let mut sessions = self.sessions.lock().await;
        sessions.retain(|key, session| {
            session.subscriber_count() > 0 || active_hls.contains(key)
        });
        sessions.keys().cloned().collect()
    };

    self.packagers
        .lock()
        .await
        .retain(|key, _| retained_sessions.contains(key));
}
```

Replace the two nested retain operations in `spawn_reaper` with:

```rust
tokio::time::sleep(me.grace).await;
me.reap_idle_at(Instant::now()).await;
```

- [ ] **Step 4: Run manager and HTTP tests and verify GREEN**

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine manager::tests
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine http::tests
```

Expected: both suites pass; direct TS and tokenized HLS lifecycle behavior remains unchanged.

- [ ] **Step 5: Commit lifecycle retention**

```bash
git add crates/ace-engine/src/manager.rs
git commit -m "fix(ace-engine): retain active native hls sessions"
```

---

### Task 3: Increase the default bounded segment ceiling

**Files:**
- Modify: `crates/ace-engine/src/config.rs`
- Modify: `crates/ace-engine/src/runtime.rs`
- Modify: `crates/ace-engine/src/hls.rs`
- Modify: `README.md`
- Test: `crates/ace-engine/src/hls.rs` (`tests` module)
- Test: `crates/ace-engine/src/runtime.rs` (`tests` module)

**Interfaces:**
- Changes: `HlsConfig::default().segment_packets` from `256` to `16_384`.
- Preserves: explicit overrides, hard-ceiling logic, PCR timing, discontinuity handling, and validation arithmetic.

- [ ] **Step 1: Write the failing default-boundary regression**

Add inside `hls.rs::tests`:

```rust
#[test]
fn default_ceiling_allows_a_timed_keyframe_beyond_256_packets() {
    let p = HlsPackager::new(HlsConfig::default());
    let input: Vec<u8> = (0..=300)
        .flat_map(|i| timed_packet(i * 360, i == 300, false, i as u8))
        .collect();

    p.feed(&input);

    assert_eq!(p.segment(0).unwrap().len(), 300 * TS_PACKET);
    assert!(p
        .playlist("test", "default-ceiling")
        .contains("#EXTINF:1.200,"));
}
```

In `runtime.rs::tests::default_config_has_live_recovery_and_hls_defaults`, change the assertion to:

```rust
assert_eq!(c.hls.segment_packets, 16_384);
```

- [ ] **Step 2: Run focused tests and verify RED**

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine default_ceiling_allows_a_timed_keyframe_beyond_256_packets
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine default_config_has_live_recovery_and_hls_defaults
```

Expected: the HLS test observes a forced 256-packet segment, and the runtime test observes the old
default `256`.

- [ ] **Step 3: Change only the default and documentation**

In `HlsConfig::default`:

```rust
Self {
    segment_packets: 16_384,
    window_segments: 6,
    segment_duration_ms: 1000,
}
```

Update the README entries to:

```markdown
- `OUTPACE_HLS_SEGMENT_PACKETS` - hard MPEG-TS packet ceiling per HLS segment and packet-count
  fallback for streams without usable PCR, default `16384` (about 3.1 MB).
- `OUTPACE_HLS_WINDOW_SEGMENTS` - retained HLS live window size, default `6`.
- `OUTPACE_HLS_SEGMENT_DURATION_MS` - requested PCR-timed HLS segment duration, default `1000`.
```

Do not change configured small-ceiling tests; they prove explicit overrides remain authoritative.

- [ ] **Step 4: Run configuration and HLS tests and verify GREEN**

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine hls::tests
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine runtime::tests
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test -p ace-engine config::tests
```

Expected: all three suites pass, including hard-ceiling and simulated ARMv7 allocation tests.

- [ ] **Step 5: Commit the default geometry fix**

```bash
git add crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs crates/ace-engine/src/hls.rs README.md
git commit -m "fix(ace-engine): size default hls segments for live video"
```

---

### Task 4: Verify the complete change

**Files:**
- Modify if needed: only files already listed in Tasks 1-3
- Verify: entire workspace

**Interfaces:**
- Consumes: all behavior delivered by Tasks 1-3.
- Produces: formatting, test, lint, and diff evidence suitable for review.

- [ ] **Step 1: Check formatting**

Run: `cargo fmt --all --check`

Expected: exit status 0. If it fails, run `cargo fmt --all`, inspect the formatting-only diff, and
rerun the check.

- [ ] **Step 2: Run the complete workspace suite with socket access**

Run outside the restricted sandbox because existing Ace/RTMP tests bind loopback sockets:

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo test --workspace
```

Expected: all non-ignored workspace tests pass; documented live-network tests remain ignored.

- [ ] **Step 3: Run the lint gate**

```bash
CARGO_TARGET_DIR=/home/jamezrin/dev/outpace/target cargo clippy --workspace --all-targets -- -D warnings
```

Expected: exit status 0 with no warnings.

- [ ] **Step 4: Review scope and repository state**

```bash
git diff --check main...HEAD
git diff --stat main...HEAD
git status --short --branch
git merge-base --is-ancestor main HEAD
```

Expected: no whitespace errors; diff limited to the approved docs, HLS activity, manager reaping,
default configuration tests, and README; clean `fix/native-hls-playback` worktree; base check exits
0.

- [ ] **Step 5: Commit only a necessary formatting adjustment**

If Step 1 changed files, commit only that verified adjustment:

```bash
git add crates/ace-engine/src/hls.rs crates/ace-engine/src/manager.rs crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs README.md
git commit -m "chore: format native hls playback fix"
```

If formatting made no changes, do not create an empty commit.
