# Direct MPEG-TS Periodic SDT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make VLC reliably retain the resolved title on continuous direct MPEG-TS playback by repeating Outpace's synthesized SDT at later video access points.

**Architecture:** Extend the existing titled `ace_media::mpegts::KeyframeGate` post-lock path to keep observing PAT, PMT, and video packets. When it recognizes a later clean video access point, prepend the existing cached `[PAT][PMT][SDT]` prefix before forwarding that access packet. Untitled gates and the separate HLS packager retain their current behavior.

**Tech Stack:** Rust, workspace `ace-media` MPEG-TS helpers, Cargo tests, Clippy, rustfmt.

## Global Constraints

- Change only titled continuous MPEG-TS output produced by `KeyframeGate`.
- Continue filtering upstream PID `0x0011` for titled streams.
- Untitled streams retain existing passthrough behavior.
- HLS packaging, HTTP headers, configuration, and unrelated transport rewriting remain unchanged.
- Do not persist real content IDs, infohashes, or stream names.
- Use `ace-log`'s `alog!` only for operational events; this change requires no new logging.

---

### Task 1: Repeat titled SDT at later direct-stream access points

**Files:**
- Modify: `crates/ace-media/src/mpegts.rs`
- Test: `crates/ace-media/src/mpegts.rs`

**Interfaces:**
- Consumes: `VideoAccessPointState::observe(&mut self, packet: &[u8]) -> bool`, `VideoAccessPointState::table_prefix(&self) -> Option<Vec<u8>>`, and `KeyframeGate::push(&mut self, data: &[u8]) -> Vec<u8>`.
- Produces: unchanged public interfaces; titled post-lock output becomes `[PAT][PMT][SDT][access packet]` at every detectable later video access point.

- [ ] **Step 1: Write a failing repeated-SDT test**

Add a test beside `keyframe_gate_replaces_upstream_sdt_with_titled_sdt` that locks a titled gate,
feeds an ordinary video packet and a second random-access packet, and asserts exactly two synthesized
SDT packets are present while the ordinary packet appears exactly once:

```rust
#[test]
fn titled_keyframe_gate_repeats_sdt_at_later_access_points() {
    let mut gate = KeyframeGate::with_service_name(Some("Titled".to_string()));
    let ordinary = ts(VIDEO_PID, false, false, &[0x11, 0x22]);
    let access = random_access_packet(VIDEO_PID);
    let mut input = Vec::new();
    input.extend_from_slice(&pat(PMT_PID));
    input.extend_from_slice(&pmt(PMT_PID, VIDEO_PID));
    input.extend_from_slice(&access);
    input.extend_from_slice(&ordinary);
    input.extend_from_slice(&access);

    let out = gate.push(&input);
    let titled_sdt_count = out
        .chunks_exact(TS_PACKET_LEN)
        .filter(|packet| read_sdt_service_name(packet).as_deref() == Some("Titled"))
        .count();
    assert_eq!(titled_sdt_count, 2);
    assert_eq!(
        out.chunks_exact(TS_PACKET_LEN)
            .filter(|packet| *packet == ordinary.as_slice())
            .count(),
        1
    );
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
cargo test -p ace-media titled_keyframe_gate_repeats_sdt_at_later_access_points -- --exact
```

Expected: FAIL because the post-lock path currently forwards the later access packet without a
second synthesized SDT.

- [ ] **Step 3: Add an untitled no-repetition regression**

Add a focused test proving that a gate created with `KeyframeGate::new()` emits no synthesized SDT
and does not prepend extra tables before a later access point. Compare the output after the initial
lock with the exact ordinary/access input bytes:

```rust
#[test]
fn untitled_keyframe_gate_does_not_repeat_tables_at_later_access_points() {
    let mut gate = KeyframeGate::new();
    let access = random_access_packet(VIDEO_PID);
    let mut opening = Vec::new();
    opening.extend_from_slice(&pat(PMT_PID));
    opening.extend_from_slice(&pmt(PMT_PID, VIDEO_PID));
    opening.extend_from_slice(&access);
    let _ = gate.push(&opening);

    let ordinary = ts(VIDEO_PID, false, false, &[0x33, 0x44]);
    let mut later = ordinary.clone();
    later.extend_from_slice(&access);
    assert_eq!(gate.push(&later), later);
}
```

- [ ] **Step 4: Implement the minimal titled post-lock behavior**

In `KeyframeGate::push`, retain the existing upstream-SDT filter. In the `self.locked` branch, only
for `self.filter_sdt`, call `self.access.observe(pkt)`. When it returns true, append the existing
`table_prefix()` before appending `pkt`. Otherwise append `pkt` exactly once. Do not change the
untitled path or public API:

```rust
if self.locked {
    if self.filter_sdt && ts_pid(pkt) == SDT_PID {
        i += TS_PACKET_LEN;
        continue;
    }
    if self.filter_sdt && self.access.observe(pkt) {
        if let Some(prefix) = self.access.table_prefix() {
            out.extend_from_slice(&prefix);
        }
    }
    out.extend_from_slice(pkt);
    i += TS_PACKET_LEN;
    continue;
}
```

- [ ] **Step 5: Run focused MPEG-TS tests and verify GREEN**

Run:

```bash
cargo test -p ace-media mpegts
```

Expected: all MPEG-TS tests pass, including repeated titled SDT, untitled passthrough, reset,
discontinuity-marker, and scan-budget fallback coverage.

- [ ] **Step 6: Run direct HTTP and HLS regression tests**

Run:

```bash
cargo test -p ace-engine --lib http::tests
cargo test -p ace-engine --lib hls::tests
```

Expected: both suites pass; direct HTTP still uses `KeyframeGate::with_service_name`, while HLS
behavior remains unchanged.

- [ ] **Step 7: Run full verification**

Run:

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 tools/hygiene/check_identifiers.py
git diff --check
cargo build --release -p ace-engine --bin outpace
```

Expected: every command exits zero.

- [ ] **Step 8: Commit the implementation**

```bash
git add crates/ace-media/src/mpegts.rs
git commit -m "fix(ace-media): repeat direct ts service title"
```

### Task 2: Publish and manually validate the updated daemon

**Files:**
- Modify: none
- Test: running release daemon and existing draft PR

**Interfaces:**
- Consumes: `target/release/outpace`, branch `fix/live-playback-prebuffer`, draft PR #147.
- Produces: updated remote branch and a restarted local daemon at `http://127.0.0.1:6878`.

- [ ] **Step 1: Push the reviewed implementation commit**

Run:

```bash
git push origin fix/live-playback-prebuffer
```

Expected: the existing draft PR updates without creating a second PR.

- [ ] **Step 2: Stop only the currently managed manual-test daemon**

Send Ctrl-C to the existing PTY session that runs this worktree's release `outpace serve`. Verify
that no other user's Outpace process is targeted.

- [ ] **Step 3: Start the rebuilt daemon**

Run from the PR worktree:

```bash
./target/release/outpace serve
```

Expected startup banner: `outpace: listening on http://127.0.0.1:6878`.

- [ ] **Step 4: Hand off VLC validation**

Ask the user to reopen the direct `.ts` endpoint in VLC and confirm that its title matches the HLS
title. Keep the daemon running for the test.
