# SDT `service_name` Title Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write the resolved stream title into the MPEG-TS SDT `service_name` on both the `.ts` and HLS paths, filtering the upstream SDT so ours is authoritative, and delete the `icy-name` header entirely.

**Architecture:** Both playback paths converge on `VideoAccessPointState::table_prefix()` (in the zero-dependency `ace-media` leaf crate). Make it append a synthesized SDT packet (`[PAT][PMT][SDT]`) when a title is present, thread the title in as a plain sanitized string, and drop upstream PID `0x0011` packets from both passthroughs when titled. `ace-engine::http` wires `StreamMetadata.title` in and removes all `icy-*` headers.

**Tech Stack:** Rust, `ace-media` (pure MPEG-TS), `ace-engine` (axum HTTP + HLS packager), `cargo test`.

**Design doc:** `docs/superpowers/specs/2026-07-15-sdt-service-name-title-design.md`

**Conventions (repo memory):**
- Do NOT run `cargo fmt --all` (churns unrelated files). Format only touched files: `rustfmt <file>` or `cargo fmt -p <crate>`.
- If a build rewrites `Cargo.lock` (tower dedup), revert it before committing: `git checkout Cargo.lock`.
- One pre-existing `ace-media` fixture test fails on a missing fixture — that is not a regression from this work.

---

## File Structure

- `crates/ace-media/src/mpegts.rs` — **all SDT logic** (CRC, `build_sdt`, `sanitize_service_name`, `read_sdt_service_name`), plus title-carrying fields on `VideoAccessPointState` and `KeyframeGate`, and the `0x0011` filter in `KeyframeGate`.
- `crates/ace-engine/src/hls.rs` — thread the title into `HlsPackager`'s `VideoAccessPointState`; filter `0x0011` in `scan`/`scan_lookahead`.
- `crates/ace-engine/src/http.rs` — wire the title into both `.ts` responses; delete `icy_name_header`, `MAX_ICY_NAME_BYTES`, all `icy-name` headers and tests; update fixture tests to assert the SDT.

---

## Task 1: CRC-32 and `build_sdt` in `ace-media`

**Files:**
- Modify: `crates/ace-media/src/mpegts.rs` (add functions + module constants near the other `fn` helpers, e.g. after `psi_section_start`)
- Test: same file, `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing tests**

Add to `mod tests`:

```rust
#[test]
fn mpeg_crc32_matches_canonical_check_value() {
    // Canonical CRC-32/MPEG-2 check value for the ASCII string "123456789".
    assert_eq!(mpeg_crc32(b"123456789"), 0x0376_E6E7);
}

#[test]
fn build_sdt_is_one_packet_with_correct_service_fields_and_crc() {
    let pkt = build_sdt(0x0001, "EUROSPORT 1");
    assert_eq!(pkt.len(), TS_PACKET_LEN);
    assert_eq!(pkt[0], TS_SYNC);
    assert_eq!(pid_of(&pkt), 0x0011); // SDT PID
    let sec = psi_section_start(&pkt).unwrap();
    assert_eq!(pkt[sec], 0x42); // table_id = SDT (actual TS)
    // section_length spans from just after the length field through the CRC.
    let section_length = (((pkt[sec + 1] & 0x0F) as usize) << 8) | pkt[sec + 2] as usize;
    let crc_at = sec + 3 + section_length - 4;
    let computed = mpeg_crc32(&pkt[sec..crc_at]);
    let stored = u32::from_be_bytes(pkt[crc_at..crc_at + 4].try_into().unwrap());
    assert_eq!(computed, stored);
    assert_eq!(read_sdt_service_name(&pkt).as_deref(), Some("EUROSPORT 1"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-media build_sdt mpeg_crc32 -- --nocapture`
Expected: FAIL — `cannot find function ... in this scope`.

- [ ] **Step 3: Implement CRC, constants, `build_sdt`, and `read_sdt_service_name`**

Add these module constants (near the top of the non-test code, after `TS_PACKET_LEN`):

```rust
/// PID carrying the Service Description Table (and BAT).
const SDT_PID: u16 = 0x0011;
/// SDT `table_id` for the actual transport stream.
const SDT_TABLE_ID: u8 = 0x42;
/// DVB service descriptor tag.
const SERVICE_DESCRIPTOR_TAG: u8 = 0x48;
/// Fixed provider name advertised in every synthesized SDT.
const SERVICE_PROVIDER_NAME: &[u8] = b"outpace";
/// Largest `service_name` (in bytes) that still lets the whole SDT section fit one TS packet:
/// 188 - 4 (TS header) - 1 (pointer_field) - 25 (fixed section fields + CRC) - provider length.
const MAX_SERVICE_NAME_BYTES: usize =
    TS_PACKET_LEN - 4 - 1 - 25 - SERVICE_PROVIDER_NAME.len();
```

Add the functions (private except `read_sdt_service_name`):

```rust
/// CRC-32/MPEG-2 (poly 0x04C11DB7, MSB-first, init 0xFFFFFFFF, no final XOR) — the CRC used by
/// MPEG-2 PSI/SI sections.
fn mpeg_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= (byte as u32) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Build a single-packet SDT on PID 0x0011 advertising `service_name` for `service_id`, with
/// `service_provider_name = "outpace"`. `service_name` must already be sanitized/bounded by
/// [`sanitize_service_name`] so the section fits one packet. Continuity counter is 0 (PSI
/// tolerates a repeated CC on an unchanged section).
fn build_sdt(service_id: u16, service_name: &str) -> [u8; TS_PACKET_LEN] {
    let name = service_name.as_bytes();

    // Service descriptor (0x48): service_type + provider + service_name.
    let mut desc = Vec::new();
    desc.push(SERVICE_DESCRIPTOR_TAG);
    desc.push((3 + SERVICE_PROVIDER_NAME.len() + name.len()) as u8); // descriptor_length
    desc.push(0x01); // service_type = digital television
    desc.push(SERVICE_PROVIDER_NAME.len() as u8);
    desc.extend_from_slice(SERVICE_PROVIDER_NAME);
    desc.push(name.len() as u8);
    desc.extend_from_slice(name);

    // SDT section (table_id through CRC).
    let mut sec = Vec::new();
    sec.push(SDT_TABLE_ID);
    let section_length = 8 + 5 + desc.len() + 4; // tsid..reserved, loop header, descriptors, CRC
    sec.push(0xF0 | ((section_length >> 8) as u8 & 0x0F)); // syntax=1 + reserved bits + len hi
    sec.push((section_length & 0xFF) as u8);
    sec.extend_from_slice(&[0x00, 0x01]); // transport_stream_id
    sec.push(0xC1); // version 0, current_next_indicator = 1
    sec.push(0x00); // section_number
    sec.push(0x00); // last_section_number
    sec.extend_from_slice(&[0xFF, 0x01]); // original_network_id
    sec.push(0xFF); // reserved_future_use
    sec.extend_from_slice(&service_id.to_be_bytes());
    sec.push(0xFC); // reserved(6) + EIT_schedule=0 + EIT_present_following=0
    let dll = desc.len();
    sec.push(0x80 | ((dll >> 8) as u8 & 0x0F)); // running_status=running + free_CA=0 + len hi
    sec.push((dll & 0xFF) as u8);
    sec.extend_from_slice(&desc);
    let crc = mpeg_crc32(&sec);
    sec.extend_from_slice(&crc.to_be_bytes());

    // Wrap into a payload-only TS packet, PUSI set, pointer_field 0, rest padded 0xFF.
    let mut pkt = [0xFFu8; TS_PACKET_LEN];
    pkt[0] = TS_SYNC;
    pkt[1] = 0x40 | ((SDT_PID >> 8) as u8 & 0x1F); // PUSI = 1
    pkt[2] = (SDT_PID & 0xFF) as u8;
    pkt[3] = 0x10; // AFC = 01 (payload only), CC = 0
    pkt[4] = 0x00; // pointer_field
    pkt[5..5 + sec.len()].copy_from_slice(&sec);
    pkt
}

/// Read the first SDT `service_name` from a run of 188-aligned TS packets, if present.
pub fn read_sdt_service_name(ts: &[u8]) -> Option<String> {
    for pkt in ts.chunks_exact(TS_PACKET_LEN) {
        if pkt[0] != TS_SYNC || ts_pid(pkt) != SDT_PID {
            continue;
        }
        let Some(sec) = psi_section_start(pkt) else {
            continue;
        };
        if *pkt.get(sec)? != SDT_TABLE_ID {
            continue;
        }
        // sec + table_id(1) + section_length(2) + tsid..reserved(8) = first service loop entry.
        let mut pos = sec + 3 + 8;
        pos += 3; // service_id(2) + EIT flags(1)
        let dll = (((*pkt.get(pos)? & 0x0F) as usize) << 8) | *pkt.get(pos + 1)? as usize;
        pos += 2;
        let end = (pos + dll).min(TS_PACKET_LEN);
        while pos + 2 <= end {
            let tag = pkt[pos];
            let len = pkt[pos + 1] as usize;
            if tag == SERVICE_DESCRIPTOR_TAG {
                let mut p = pos + 2 + 1; // skip service_type
                let provlen = *pkt.get(p)? as usize;
                p += 1 + provlen;
                let namelen = *pkt.get(p)? as usize;
                p += 1;
                let name = pkt.get(p..p + namelen)?;
                return String::from_utf8(name.to_vec()).ok();
            }
            pos += 2 + len;
        }
    }
    None
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-media build_sdt mpeg_crc32 -- --nocapture`
Expected: PASS (2 tests). `MAX_SERVICE_NAME_BYTES` may be reported as unused by the compiler until Task 2 — that is expected; do not add `#[allow]`.

- [ ] **Step 5: Commit**

```bash
rustfmt crates/ace-media/src/mpegts.rs
git checkout Cargo.lock 2>/dev/null || true
git add crates/ace-media/src/mpegts.rs
git commit -m "feat(mpegts): synthesize a single-packet SDT with CRC-32 (#135)"
```

---

## Task 2: `sanitize_service_name` (title → bounded service_name)

**Files:**
- Modify: `crates/ace-media/src/mpegts.rs`
- Test: same file

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn sanitize_service_name_strips_control_chars_and_trims() {
    assert_eq!(
        sanitize_service_name("  Synthetic Demo\r\n\0 Channel  ").as_deref(),
        Some("Synthetic Demo Channel")
    );
}

#[test]
fn sanitize_service_name_rejects_empty_and_whitespace() {
    assert_eq!(sanitize_service_name(""), None);
    assert_eq!(sanitize_service_name(" \r\n\t "), None);
}

#[test]
fn sanitize_service_name_byte_caps_on_char_boundary() {
    // Multibyte chars must never be split, and the result must fit build_sdt (one packet).
    let name = sanitize_service_name(&"é".repeat(200)).unwrap();
    assert!(name.len() <= MAX_SERVICE_NAME_BYTES);
    assert!(name.chars().all(|c| c == 'é'));
    // Proves the cap keeps the SDT within a single TS packet.
    assert_eq!(build_sdt(1, &name).len(), TS_PACKET_LEN);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-media sanitize_service_name`
Expected: FAIL — `cannot find function sanitize_service_name`.

- [ ] **Step 3: Implement `sanitize_service_name`**

Add near `build_sdt`:

```rust
/// Turn a raw title into an SDT `service_name`: trim, strip control characters, and byte-cap on a
/// UTF-8 boundary so the whole SDT section fits one TS packet. Returns `None` if nothing remains.
fn sanitize_service_name(raw: &str) -> Option<String> {
    let stripped: String = raw.trim().chars().filter(|c| !c.is_control()).collect();
    let stripped = stripped.trim();
    if stripped.is_empty() {
        return None;
    }
    let mut end = stripped.len().min(MAX_SERVICE_NAME_BYTES);
    while !stripped.is_char_boundary(end) {
        end -= 1;
    }
    Some(stripped[..end].to_string())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-media sanitize_service_name`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
rustfmt crates/ace-media/src/mpegts.rs
git checkout Cargo.lock 2>/dev/null || true
git add crates/ace-media/src/mpegts.rs
git commit -m "feat(mpegts): sanitize and byte-cap titles into SDT service_name (#135)"
```

---

## Task 3: `VideoAccessPointState` carries the title; `table_prefix` appends the SDT

**Files:**
- Modify: `crates/ace-media/src/mpegts.rs` — struct fields (`~119`), `reset` (`~132`), `observe` PAT branch (`~141`), `table_prefix` (`~161`), and `parse_pat_pmt_pid` (`~305`).
- Test: same file

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn table_prefix_appends_sdt_with_pat_program_number_when_titled() {
    // PMT program_number is 1 (see the `pat`/`pmt` helpers).
    let mut state = VideoAccessPointState::with_service_name(Some("My Channel".to_string()));
    state.observe(&pat(PMT_PID));
    state.observe(&pmt(PMT_PID, VIDEO_PID));
    let prefix = state.table_prefix().unwrap();
    // PAT + PMT + one SDT packet.
    assert_eq!(prefix.len(), TS_PACKET_LEN * 3);
    assert_eq!(read_sdt_service_name(&prefix).as_deref(), Some("My Channel"));
}

#[test]
fn table_prefix_omits_sdt_when_untitled() {
    let mut state = VideoAccessPointState::new();
    state.observe(&pat(PMT_PID));
    state.observe(&pmt(PMT_PID, VIDEO_PID));
    let prefix = state.table_prefix().unwrap();
    assert_eq!(prefix.len(), TS_PACKET_LEN * 2);
    assert!(read_sdt_service_name(&prefix).is_none());
}

#[test]
fn reset_preserves_service_name() {
    let mut state = VideoAccessPointState::with_service_name(Some("Keep Me".to_string()));
    state.reset();
    assert!(state.has_service_name());
    state.observe(&pat(PMT_PID));
    state.observe(&pmt(PMT_PID, VIDEO_PID));
    assert_eq!(
        read_sdt_service_name(&state.table_prefix().unwrap()).as_deref(),
        Some("Keep Me")
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-media table_prefix reset_preserves`
Expected: FAIL — `no function ... with_service_name` / `has_service_name`.

- [ ] **Step 3: Add fields, constructor, program-number parse, and SDT in `table_prefix`**

Add two fields to the struct:

```rust
#[derive(Default)]
pub struct VideoAccessPointState {
    pmt_pid: Option<u16>,
    video_pid: Option<u16>,
    video_codec: Option<VideoCodec>,
    cached_pat: Option<Vec<u8>>,
    cached_pmt: Option<Vec<u8>>,
    service_name: Option<String>,
    program_number: Option<u16>,
}
```

Add the constructor and accessor (next to `new`):

```rust
/// Like [`new`](Self::new) but injects a synthesized SDT `service_name` into every
/// [`table_prefix`](Self::table_prefix). `raw` is sanitized/bounded here; a title that
/// sanitizes to empty leaves the state untitled.
pub fn with_service_name(raw: Option<String>) -> Self {
    Self {
        service_name: raw.as_deref().and_then(sanitize_service_name),
        ..Self::default()
    }
}

pub fn has_service_name(&self) -> bool {
    self.service_name.is_some()
}
```

Change `reset` to preserve the title:

```rust
pub fn reset(&mut self) {
    let name = self.service_name.take();
    *self = Self {
        service_name: name,
        ..Self::default()
    };
}
```

In `observe`, the `pid == 0` (PAT) branch, capture the program number. Replace the `parse_pat_pmt_pid` call:

```rust
if pid == 0 {
    if let Some((program, pmt_pid)) = parse_pat_first_program(packet) {
        self.cached_pat = Some(packet.to_vec());
        self.program_number = Some(program);
        if self.pmt_pid != Some(pmt_pid) {
            self.cached_pmt = None;
            self.video_pid = None;
            self.video_codec = None;
        }
        self.pmt_pid = Some(pmt_pid);
    }
}
```

Refactor the PAT parser into a program-and-pid variant, and keep the old name as a thin wrapper:

```rust
/// Parse a PAT packet, returning `(program_number, pmt_pid)` of the first real program.
fn parse_pat_first_program(pkt: &[u8]) -> Option<(u16, u16)> {
    let sec = psi_section_start(pkt)?;
    if *pkt.get(sec)? != 0x00 {
        return None; // table_id must be PAT
    }
    let section_length = ((pkt.get(sec + 1)? & 0x0F) as usize) << 8 | *pkt.get(sec + 2)? as usize;
    let end = (sec + 3 + section_length)
        .saturating_sub(4)
        .min(TS_PACKET_LEN); // drop CRC
    let mut pos = sec + 8;
    while pos + 4 <= end {
        let program = ((pkt[pos] as u16) << 8) | pkt[pos + 1] as u16;
        let pid = (((pkt[pos + 2] & 0x1F) as u16) << 8) | pkt[pos + 3] as u16;
        if program != 0 {
            return Some((program, pid));
        }
        pos += 4;
    }
    None
}

/// Parse a PAT packet, returning the PMT PID of the first real program.
fn parse_pat_pmt_pid(pkt: &[u8]) -> Option<u16> {
    parse_pat_first_program(pkt).map(|(_, pid)| pid)
}
```

Append the SDT in `table_prefix`:

```rust
pub fn table_prefix(&self) -> Option<Vec<u8>> {
    let mut prefix = Vec::with_capacity(TS_PACKET_LEN * 3);
    prefix.extend_from_slice(self.cached_pat.as_ref()?);
    prefix.extend_from_slice(self.cached_pmt.as_ref()?);
    if let (Some(name), Some(program)) = (self.service_name.as_deref(), self.program_number) {
        prefix.extend_from_slice(&build_sdt(program, name));
    }
    Some(prefix)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-media table_prefix reset_preserves`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the whole crate to check for regressions**

Run: `cargo test -p ace-media`
Expected: PASS except the one known pre-existing missing-fixture failure (see conventions).

- [ ] **Step 6: Commit**

```bash
rustfmt crates/ace-media/src/mpegts.rs
git checkout Cargo.lock 2>/dev/null || true
git add crates/ace-media/src/mpegts.rs
git commit -m "feat(mpegts): inject SDT into table_prefix, keyed to PAT program_number (#135)"
```

---

## Task 4: `KeyframeGate` carries the title and filters upstream SDT

**Files:**
- Modify: `crates/ace-media/src/mpegts.rs` — `KeyframeGate` struct (`~177`), constructors (`~189-204`), `push` locked branch (`~213`), add a `reset` method.
- Test: same file

- [ ] **Step 1: Write the failing tests**

Add helpers + tests to `mod tests` (reuse existing `pat`/`pmt`/`ts`/`random_access_packet`; add an SDT source packet):

```rust
/// An upstream SDT packet on PID 0x0011 advertising "Upstream" (a generic passthrough SDT).
fn upstream_sdt() -> Vec<u8> {
    build_sdt(1, "Upstream").to_vec()
}

#[test]
fn keyframe_gate_replaces_upstream_sdt_with_titled_sdt() {
    let mut gate = KeyframeGate::with_service_name(Some("Titled".to_string()));
    let mut input = Vec::new();
    input.extend_from_slice(&pat(PMT_PID));
    input.extend_from_slice(&pmt(PMT_PID, VIDEO_PID));
    input.extend_from_slice(&upstream_sdt());
    input.extend_from_slice(&random_access_packet(VIDEO_PID)); // keyframe -> lock
    input.extend_from_slice(&upstream_sdt()); // must be filtered post-lock
    let out = gate.push(&input);
    assert_eq!(read_sdt_service_name(&out).as_deref(), Some("Titled"));
    // No upstream "Upstream" SDT survives anywhere in the output.
    assert!(!out
        .chunks_exact(TS_PACKET_LEN)
        .any(|p| read_sdt_service_name(p).as_deref() == Some("Upstream")));
}

#[test]
fn keyframe_gate_untitled_passes_upstream_sdt_through() {
    let mut gate = KeyframeGate::new();
    let mut input = Vec::new();
    input.extend_from_slice(&pat(PMT_PID));
    input.extend_from_slice(&pmt(PMT_PID, VIDEO_PID));
    input.extend_from_slice(&random_access_packet(VIDEO_PID));
    input.extend_from_slice(&upstream_sdt());
    let out = gate.push(&input);
    assert_eq!(read_sdt_service_name(&out).as_deref(), Some("Upstream"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-media keyframe_gate_replaces keyframe_gate_untitled`
Expected: FAIL — `no function ... with_service_name`.

- [ ] **Step 3: Add the field, constructor, filter, and reset**

Add `filter_sdt: bool` to the struct:

```rust
pub struct KeyframeGate {
    buf: Vec<u8>,
    locked: bool,
    access: VideoAccessPointState,
    scanned: usize,
    max_scan_packets: usize,
    filter_sdt: bool,
}
```

Set it in the existing constructors (both currently build `access: VideoAccessPointState::new()` → keep `filter_sdt: false`), and add a titled constructor:

```rust
pub fn with_max_scan_packets(max_scan_packets: usize) -> Self {
    KeyframeGate {
        buf: Vec::new(),
        locked: false,
        access: VideoAccessPointState::new(),
        scanned: 0,
        max_scan_packets,
        filter_sdt: false,
    }
}

/// Like [`new`](Self::new) but injects a synthesized SDT `service_name` at keyframe lock and
/// drops upstream SDT (PID 0x0011) packets from the post-lock passthrough, so the injected
/// title is the only SDT the client sees.
pub fn with_service_name(raw: Option<String>) -> Self {
    let access = VideoAccessPointState::with_service_name(raw);
    let filter_sdt = access.has_service_name();
    KeyframeGate {
        buf: Vec::new(),
        locked: false,
        access,
        scanned: 0,
        max_scan_packets: DEFAULT_MAX_SCAN_PACKETS,
        filter_sdt,
    }
}

/// Re-arm the gate (drop its buffer and re-require a keyframe) while preserving its configured
/// title, so a mid-stream reset keeps injecting/filtering the SDT.
pub fn reset(&mut self) {
    self.buf.clear();
    self.locked = false;
    self.scanned = 0;
    self.access.reset();
}
```

In `push`, filter the locked passthrough branch:

```rust
if self.locked {
    if self.filter_sdt && ts_pid(pkt) == SDT_PID {
        i += TS_PACKET_LEN;
        continue;
    }
    out.extend_from_slice(pkt);
    i += TS_PACKET_LEN;
    continue;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-media keyframe_gate`
Expected: PASS (new tests + existing keyframe_gate tests).

- [ ] **Step 5: Commit**

```bash
rustfmt crates/ace-media/src/mpegts.rs
git checkout Cargo.lock 2>/dev/null || true
git add crates/ace-media/src/mpegts.rs
git commit -m "feat(mpegts): filter upstream SDT and inject title in KeyframeGate (#135)"
```

---

## Task 5: `HlsPackager` carries the title and filters upstream SDT

**Files:**
- Modify: `crates/ace-engine/src/hls.rs` — `HlsState` (`~29`), `new` (`~56`, add param), `start` (`~86`), `scan` (`~146`), `scan_lookahead` (`~245`), and the three test helpers `pkg`/`timed_pkg`/`clean_pkg` (`~594`, `~611`, `~707`).
- Test: same file

- [ ] **Step 1: Write the failing test**

Add to `mod tests`. It reuses the module's existing helpers (`pat`, `pmt`, `video_access_packet`, `with_pid`, `packets`, `PMT_PID`, `VIDEO_PID`) and a plain PID-`0x0011` packet as the "upstream SDT" — the filter keys off PID, so the upstream packet's contents don't matter, and asserting "exactly one `0x0011` packet remains" proves ours is the only SDT:

```rust
#[test]
fn segments_replace_upstream_sdt_with_titled_sdt() {
    use ace_media::mpegts::read_sdt_service_name;
    let p = HlsPackager::new(
        HlsConfig {
            segment_packets: 100,
            window_segments: 4,
            segment_duration_ms: 1000,
        },
        Some("HLS Title".to_string()),
    );
    p.feed(&pat(PMT_PID));
    p.feed(&pmt(PMT_PID, VIDEO_PID));
    p.feed(&video_access_packet(VIDEO_PID, 0)); // opens segment 0 with [PAT][PMT][SDT]
    p.feed(&with_pid(packets(1), 0x0011)); // upstream SDT-PID packet -> must be filtered
    p.feed(&video_access_packet(VIDEO_PID, 108_000)); // 1.2s elapsed -> emits segment 0
    let seg = p.segment(0).expect("a segment should be emitted");
    // The injected SDT carries the title.
    assert_eq!(read_sdt_service_name(&seg).as_deref(), Some("HLS Title"));
    // Exactly one SDT-PID packet in the segment: ours. The upstream one was dropped.
    let sdt_packets = seg
        .chunks(TS_PACKET)
        .filter(|c| c.len() == TS_PACKET && ((((c[1] & 0x1f) as u16) << 8) | c[2] as u16) == 0x0011)
        .count();
    assert_eq!(sdt_packets, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ace-engine segments_replace_upstream_sdt`
Expected: FAIL — `this function takes 1 argument but 2 arguments were supplied` (on `HlsPackager::new`).

- [ ] **Step 3: Thread the title and add the filter**

Add `filter_sdt: bool` to `HlsState` and a `SDT_PID` const at the top of `hls.rs`:

```rust
const SDT_PID: u16 = 0x0011;
```

Change `new` to accept the title and build a titled `VideoAccessPointState`:

```rust
fn new(config: HlsConfig, service_name: Option<String>) -> Arc<HlsPackager> {
    let seg_packets = config.segment_packets.max(3);
    let max_segment_bytes = seg_packets
        .checked_mul(TS_PACKET)
        .expect("HLS config must be validated before packager construction");
    let access = VideoAccessPointState::with_service_name(service_name);
    let filter_sdt = access.has_service_name();
    Arc::new(HlsPackager {
        state: Mutex::new(HlsState {
            media_seq: 0,
            segments: VecDeque::new(),
            cur: Vec::new(),
            scanned_packets: 0,
            segment_start_pcr: None,
            last_pcr: None,
            pcr_pid: None,
            max_segment_duration: config.segment_duration_secs(),
            discontinuity_pending: false,
            last_access: None,
            access,
            awaiting_access_point: true,
            lookahead: [0; TS_PACKET],
            lookahead_len: 0,
            filter_sdt,
        }),
        ready: tokio::sync::Notify::new(),
        max_segment_bytes,
        window: config.window_segments.max(1),
        seg_duration: config.segment_duration_secs(),
    })
}
```

Change `start` to read the title from the session:

```rust
pub fn start(session: &StreamSession, config: HlsConfig) -> Arc<HlsPackager> {
    let me = Self::new(config, session.metadata().title.clone());
    // ...unchanged...
}
```

In `scan`, right after `let is_access_point = st.access.observe(&packet);`, drop titled upstream SDT before it becomes segment content:

```rust
if st.filter_sdt && timing.pid == SDT_PID {
    st.cur.drain(packet_offset..packet_offset + TS_PACKET);
    continue; // scanned_packets unchanged: the next packet slides into this offset
}
```

In `scan_lookahead`, right after its `let is_access_point = st.access.observe(packet);`, skip a titled SDT lookahead packet (feed already reset `lookahead_len`, so the next packet refills cleanly):

```rust
if st.filter_sdt && timing.pid == SDT_PID {
    return;
}
```

Update the three test helpers to pass `None`:

```rust
// pkg(), timed_pkg(), clean_pkg() — each `HlsPackager::new(HlsConfig { .. })` becomes:
HlsPackager::new(HlsConfig { /* .. */ }, None)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-engine -- hls`
Expected: PASS, including the new test and all existing `hls` tests.

- [ ] **Step 5: Commit**

```bash
rustfmt crates/ace-engine/src/hls.rs
git checkout Cargo.lock 2>/dev/null || true
git add crates/ace-engine/src/hls.rs crates/ace-media/src/mpegts.rs
git commit -m "feat(hls): filter upstream SDT and inject title per segment (#135)"
```

---

## Task 6: Wire the title into `.ts` responses and delete `icy-name`

**Files:**
- Modify: `crates/ace-engine/src/http.rs` — delete `MAX_ICY_NAME_BYTES` (`~28`) and `icy_name_header` (`~31-43`); `stream_session_response` (`~1465`); `ace_stream_session_response` (`~1502`); `reset_stream_keyframe_gate` (`~1546`); delete the two icy unit tests (`~1566`, `~1588`); update fixture tests (`~2619`, `~2689`, `~2719`).

- [ ] **Step 1: Update the fixture tests first (they become the failing tests)**

In `stream_ts_serves_mpegts_first_frame` (`~2619`), replace the icy assertion. The `.ts` gate emits `[PAT][PMT][SDT][keyframe]` as the first chunk, so read the SDT from it:

```rust
assert_eq!(resp.status(), StatusCode::OK);
assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp2t");
assert!(!resp.headers().contains_key("icy-name"));
let mut stream = resp.into_body().into_data_stream();
let first = stream.next().await.unwrap().unwrap();
assert_eq!(first[0], 0x47);
assert_eq!(
    ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
    Some("Synthetic Demo Channel")
);
```

In `ace_getstream_content_id_returns_a_playback_url_that_streams` (`~2689`) and `ace_getstream_without_format_directly_streams_legacy_id_as_mpegts` (`~2719`), replace each `assert_eq!(..headers()["icy-name"], "Synthetic Demo Channel");` with an absence check plus an SDT check on the first chunk:

```rust
assert!(!media.headers().contains_key("icy-name")); // or `response` in the second test
let mut stream = media.into_body().into_data_stream(); // or `body` in the second test
let first = stream.next().await.unwrap().unwrap();
assert_eq!(first[0], 0x47);
assert_eq!(
    ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
    Some("Synthetic Demo Channel")
);
```

(Adjust the local variable names to match each test — the second uses `response`/`body`.) Leave `stream_metadata_title_header_is_absent_without_metadata` (`~2627`) unchanged: it already asserts the header is absent, which now always holds.

Delete the two now-obsolete unit tests entirely: `icy_name_header_sanitizes_and_bounds_titles` and `icy_name_header_omits_empty_titles`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ace-engine stream_ts_serves ace_getstream_content_id ace_getstream_without_format`
Expected: FAIL to compile — `cannot find function icy_name_header` is still referenced by production code, and the deleted unit tests referenced it too. (Compile error is an acceptable "fail" here; Step 3 makes it build.)

- [ ] **Step 3: Delete `icy_name_header` and wire the title**

Delete `const MAX_ICY_NAME_BYTES: usize = 256;` and the entire `fn icy_name_header(...)`.

In `stream_session_response`, drop the icy lines and pass the title to the gate:

```rust
fn stream_session_response(session: Arc<StreamSession>) -> Response {
    let sub = session.subscribe();
    // Per-client keyframe gate: holds a mid-GOP joiner until the first clean keyframe (with
    // PAT/PMT — and, when the stream has a resolved title, a synthesized SDT — prepended).
    let gate = ace_media::mpegts::KeyframeGate::with_service_name(session.metadata().title.clone());
    let stream = futures::stream::unfold((sub, gate), |(mut sub, mut gate)| async move {
        // ...unchanged loop body...
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}
```

In `ace_stream_session_response`, same substitution:

```rust
let sub = session.subscribe();
let gate = ace_media::mpegts::KeyframeGate::with_service_name(session.metadata().title.clone());
```

and at the end:

```rust
Response::builder()
    .header(header::CONTENT_TYPE, "video/mp2t")
    .body(Body::from_stream(stream))
    .unwrap()
```

Change `reset_stream_keyframe_gate` to preserve the title (it must NOT re-`new()` an untitled gate):

```rust
fn reset_stream_keyframe_gate(gate: &mut ace_media::mpegts::KeyframeGate) {
    gate.reset();
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ace-engine stream_ts_serves ace_getstream_content_id ace_getstream_without_format stream_metadata_title_header_is_absent`
Expected: PASS.

- [ ] **Step 5: Grep to confirm no `icy` remains**

Run: `grep -rn "icy" crates/ace-engine/src crates/ace-media/src`
Expected: no matches (empty output).

- [ ] **Step 6: Commit**

```bash
rustfmt crates/ace-engine/src/http.rs
git checkout Cargo.lock 2>/dev/null || true
git add crates/ace-engine/src/http.rs
git commit -m "feat(http): serve title via SDT and drop icy-name entirely (#135)"
```

---

## Task 7: Full verification

- [ ] **Step 1: Build and test the whole workspace**

Run: `cargo test`
Expected: PASS except the one known pre-existing `ace-media` missing-fixture failure. No other failures.

- [ ] **Step 2: Clippy on the touched crates**

Run: `cargo clippy -p ace-media -p ace-engine --all-targets`
Expected: no new warnings from the changed code.

- [ ] **Step 3: Confirm Cargo.lock is clean**

Run: `git status --porcelain Cargo.lock`
Expected: empty (revert with `git checkout Cargo.lock` if the build rewrote it).

- [ ] **Step 4: (Optional, if an engine/live source is available) ffprobe acceptance**

The acceptance criteria call for ffprobe/VLC checks against a live `cid:`. These need a running engine + live source (not available in unit tests); the in-process `read_sdt_service_name` round-trip tests stand in for them. If a live source is available:

```bash
ffprobe -v error -show_programs "http://localhost:6878/streams/ace/cid:<id>.ts" 2>&1 | grep service_name
# Expected: TAG:service_name=<title>   (not Service01)
ffprobe -v error -show_programs "http://localhost:6878/streams/ace/cid:<id>/seg/0.ts" 2>&1 | grep service_name
# Expected: TAG:service_name=<title>
```

- [ ] **Step 5: Final commit if anything was adjusted**

```bash
git checkout Cargo.lock 2>/dev/null || true
git add -A
git commit -m "chore: verification fixups for SDT title (#135)" || echo "nothing to commit"
```

---

## Self-Review Notes (addressed)

- **Spec coverage:** SDT synthesis (Task 1), sanitize/bound (Task 2), `service_id` from PAT `program_number` (Task 3), `.ts` injection at lock + filter (Task 4), HLS per-segment injection + filter incl. lookahead (Task 5), title threading + `icy-name` removal (Task 6), no-`icy` grep + ffprobe acceptance (Tasks 6–7). Title-present gating is enforced by `filter_sdt`/`has_service_name` (Tasks 3–5); untitled streams stay byte-for-byte unchanged (explicit tests in Tasks 3 & 4).
- **Reset paths:** `VideoAccessPointState::reset` and `KeyframeGate::reset` preserve the title (Tasks 3–4), and `reset_stream_keyframe_gate` calls `gate.reset()` instead of re-`new()` (Task 6) — otherwise a `Lagged`/discontinuity reset would silently stop injecting.
- **Type consistency:** `with_service_name`, `has_service_name`, `reset`, `build_sdt`, `read_sdt_service_name`, `sanitize_service_name`, `parse_pat_first_program`, `filter_sdt`, `SDT_PID` are used with identical signatures across tasks.
- **Placeholders:** none. Task 5's test reuses helpers confirmed present in `hls.rs` tests (`pat`, `pmt`, `video_access_packet`, `with_pid`, `packets`) and a raw PID-`0x0011` packet, so no ace-media test seam is needed. The only `pub` API added to `ace-media` is `read_sdt_service_name` (a genuine read-side utility used by both crates' tests); everything else stays private.
