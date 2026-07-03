# RTMP Broadcast Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add native RTMP ingest for Outpace broadcasts while preserving raw HTTP MPEG-TS ingest.

**Architecture:** Factor the current HTTP broadcast body processing into a shared `BroadcastIngest` helper, then feed that helper from both `PUT /broadcast/{name}` and a new RTMP listener. The RTMP listener accepts `rtmp://host:port/live/<name>`, parses H.264/AAC publisher frames with `rtmp-rs`, remuxes them to MPEG-TS, and writes aligned TS bytes into the same signing/chunking/store path as raw ingest.

**Tech Stack:** Rust 2021, tokio, axum, rtmp-rs 0.5, bytes, futures, existing `ace_media`, `ace_wire`, `ace_swarm` crates.

---

## File Structure

- Modify `crates/ace-engine/Cargo.toml`: add `rtmp-rs`.
- Modify `crates/ace-engine/src/config.rs`: add `rtmp_bind: SocketAddr` with default and env parsing support through runtime.
- Modify `crates/ace-engine/src/runtime.rs`: parse `OUTPACE_RTMP_BIND`, start RTMP listener, expose URL formatting helpers.
- Modify `crates/ace-engine/src/cli.rs`: replace `OBS ingest URL` output with raw and RTMP URLs.
- Modify `crates/ace-engine/src/http.rs`: delegate HTTP broadcast body processing to the shared ingest helper.
- Create `crates/ace-engine/src/broadcast_ingest.rs`: shared TS byte ingestion into `SigningChunker` and `PieceStore`.
- Create `crates/ace-engine/src/rtmp.rs`: RTMP handler, stream-key routing, and media forwarding into the shared ingest helper.
- Create `crates/ace-engine/src/rtmp_ts.rs`: narrow H.264/AAC to MPEG-TS remuxer.
- Modify `crates/ace-engine/src/lib.rs`: export new modules.
- Modify `docs/protocol/notes/51-cli-and-broadcast-content-id.md` and `README.md`: document raw + RTMP ingest surfaces.

---

### Task 1: Shared Broadcast Ingest Helper

**Files:**
- Create: `crates/ace-engine/src/broadcast_ingest.rs`
- Modify: `crates/ace-engine/src/lib.rs`
- Modify: `crates/ace-engine/src/http.rs`

- [ ] **Step 1: Write the failing shared-helper test**

Add this test to a `#[cfg(test)]` module in new file `crates/ace-engine/src/broadcast_ingest.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::{CHUNK_LENGTH, PIECE_LENGTH};
    use ace_swarm::store::PieceStore;
    use ace_wire::live_auth::LiveSourceAuth;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn ts_body(bytes: usize) -> Vec<u8> {
        const TS_PACKET_LEN: usize = 188;
        let packets = bytes.div_ceil(TS_PACKET_LEN);
        let mut body = Vec::with_capacity(packets * TS_PACKET_LEN);
        for i in 0..packets {
            body.push(0x47);
            body.extend(std::iter::repeat_n((i % 251) as u8, TS_PACKET_LEN - 1));
        }
        body
    }

    #[tokio::test]
    async fn shared_ingest_writes_signed_ts_chunks_to_store() {
        let store = Arc::new(Mutex::new(PieceStore::new(
            PIECE_LENGTH,
            CHUNK_LENGTH,
            4 << 20,
        )));
        let auth = Arc::new(LiveSourceAuth::generate());
        let ingest = BroadcastIngest::new(store.clone(), auth);

        ingest.push_bytes(&ts_body(CHUNK_LENGTH as usize + 188)).await;
        ingest.finish().await;

        let guard = store.lock().await;
        assert!(
            guard.chunk(0, 0).is_some(),
            "shared ingest must write chunk (0, 0)"
        );
        let header = guard.piece_header(0).expect("piece header is recorded");
        assert_ne!(header, [0; 8], "source ingest must generate a live header");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p ace-engine broadcast_ingest::tests::shared_ingest_writes_signed_ts_chunks_to_store
```

Expected: compile failure because `broadcast_ingest` module and `BroadcastIngest` do not exist.

- [ ] **Step 3: Implement the shared helper**

Create `crates/ace-engine/src/broadcast_ingest.rs`:

```rust
//! Shared MPEG-TS broadcast ingest path used by raw HTTP PUT and RTMP ingest.

use crate::broadcast::{CHUNK_LENGTH, PIECE_LENGTH};
use ace_swarm::store::PieceStore;
use ace_wire::live_auth::LiveSourceAuth;
use ace_wire::live_codec::piece_header_from_unix_seconds;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub struct BroadcastIngest {
    store: Arc<Mutex<PieceStore>>,
    auth: Arc<LiveSourceAuth>,
    resync: ace_media::mpegts::TsResync,
    chunker: ace_wire::signing_chunker::SigningChunker,
    current_header: Option<(u64, [u8; 8])>,
}

impl BroadcastIngest {
    pub fn new(store: Arc<Mutex<PieceStore>>, auth: Arc<LiveSourceAuth>) -> Self {
        let sig_len = auth.signature_len() as u64;
        Self {
            store,
            auth,
            resync: ace_media::mpegts::TsResync::new(),
            chunker: ace_wire::signing_chunker::SigningChunker::new(
                PIECE_LENGTH,
                CHUNK_LENGTH,
                0,
                sig_len,
            ),
            current_header: None,
        }
    }

    pub async fn push_bytes(&mut self, bytes: &[u8]) {
        let aligned = self.resync.push(bytes);
        self.store_outputs(self.chunker.push(&aligned, &self.auth)).await;
    }

    pub async fn finish(&mut self) {
        self.store_outputs(self.chunker.flush(&self.auth)).await;
    }

    async fn store_outputs(
        &mut self,
        outputs: Vec<ace_wire::signing_chunker::SignedChunk>,
    ) {
        for out in outputs {
            let header = header_for_piece(&mut self.current_header, out.piece);
            self.store
                .lock()
                .await
                .put_chunk_with_header(out.piece, out.chunk, header, &out.data);
        }
    }
}

fn current_live_piece_header() -> [u8; 8] {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    piece_header_from_unix_seconds(seconds)
}

fn header_for_piece(current: &mut Option<(u64, [u8; 8])>, piece: u64) -> [u8; 8] {
    match current {
        Some((current_piece, header)) if *current_piece == piece => *header,
        _ => {
            let header = current_live_piece_header();
            *current = Some((piece, header));
            header
        }
    }
}
```

If `SignedChunk` is not the exported output type name, inspect `crates/ace-wire/src/signing_chunker.rs` and use the concrete type returned by `SigningChunker::push` and `flush`.

Add to `crates/ace-engine/src/lib.rs`:

```rust
pub mod broadcast_ingest;
```

- [ ] **Step 4: Wire HTTP ingest to the helper**

In `crates/ace-engine/src/http.rs`, remove the local `current_live_piece_header` and `header_for_piece` helper functions and replace the spawned ingest body with:

```rust
let store = bc.store.clone();
let auth = bc.auth.clone();
tokio::spawn(async move {
    let mut ingest = crate::broadcast_ingest::BroadcastIngest::new(store, auth);
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        ingest.push_bytes(&chunk).await;
    }
    ingest.finish().await;
});
```

Remove these now-unused imports from `http.rs`:

```rust
use ace_wire::live_codec::piece_header_from_unix_seconds;
use std::time::{SystemTime, UNIX_EPOCH};
```

- [ ] **Step 5: Run tests to verify helper and existing HTTP behavior pass**

Run:

```bash
cargo test -p ace-engine broadcast_ingest::tests::shared_ingest_writes_signed_ts_chunks_to_store
cargo test -p ace-engine broadcast
```

Expected: both commands pass.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/src/broadcast_ingest.rs crates/ace-engine/src/lib.rs crates/ace-engine/src/http.rs
git commit -m "ace-engine: share broadcast ts ingest"
```

---

### Task 2: RTMP Bind Config and CLI Output

**Files:**
- Modify: `crates/ace-engine/src/config.rs`
- Modify: `crates/ace-engine/src/runtime.rs`
- Modify: `crates/ace-engine/src/cli.rs`

- [ ] **Step 1: Write failing config tests**

Add to `crates/ace-engine/src/config.rs` tests:

```rust
#[test]
fn default_config_has_rtmp_bind_on_localhost_1935() {
    let c = Config::default();
    assert_eq!(c.rtmp_bind, "127.0.0.1:1935".parse().unwrap());
}
```

Add to `crates/ace-engine/src/runtime.rs` tests in a new `#[cfg(test)] mod tests`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcast_urls_use_raw_and_rtmp_labels() {
        let http = "127.0.0.1:6878".parse().unwrap();
        let rtmp = "127.0.0.1:1935".parse().unwrap();
        let urls = broadcast_ingest_urls(http, rtmp, None, "mychan");

        assert_eq!(
            urls.raw,
            "http://127.0.0.1:6878/broadcast/mychan"
        );
        assert_eq!(
            urls.rtmp,
            "rtmp://127.0.0.1:1935/live/mychan"
        );
    }

    #[test]
    fn broadcast_urls_use_public_host_for_displayed_hosts() {
        let http = "0.0.0.0:6878".parse().unwrap();
        let rtmp = "0.0.0.0:1935".parse().unwrap();
        let urls = broadcast_ingest_urls(http, rtmp, Some("stream.example".to_string()), "mychan");

        assert_eq!(
            urls.raw,
            "http://stream.example:6878/broadcast/mychan"
        );
        assert_eq!(
            urls.rtmp,
            "rtmp://stream.example:1935/live/mychan"
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p ace-engine default_config_has_rtmp_bind_on_localhost_1935
cargo test -p ace-engine broadcast_urls_use_raw_and_rtmp_labels
```

Expected: compile failures because `Config::rtmp_bind`, `BroadcastIngestUrls`, and `broadcast_ingest_urls` do not exist.

- [ ] **Step 3: Add config field and env parsing**

In `crates/ace-engine/src/config.rs`, add field:

```rust
/// Address the RTMP ingest listener binds to.
pub rtmp_bind: SocketAddr,
```

In `Config::default()`, add:

```rust
rtmp_bind: "127.0.0.1:1935".parse().unwrap(),
```

In `crates/ace-engine/src/runtime.rs`, inside `config_from_env()`, after `OUTPACE_BIND` parsing, add:

```rust
if let Ok(bind) = std::env::var("OUTPACE_RTMP_BIND") {
    config.rtmp_bind = bind.parse()?;
}
```

- [ ] **Step 4: Add URL formatter**

Add to `crates/ace-engine/src/runtime.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastIngestUrls {
    pub raw: String,
    pub rtmp: String,
}

pub fn broadcast_ingest_urls(
    http_bind: std::net::SocketAddr,
    rtmp_bind: std::net::SocketAddr,
    public_host: Option<String>,
    name: &str,
) -> BroadcastIngestUrls {
    let raw_host = public_host
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| http_bind.ip().to_string());
    let rtmp_host = public_host.unwrap_or_else(|| rtmp_bind.ip().to_string());
    BroadcastIngestUrls {
        raw: format!("http://{}:{}/broadcast/{}", raw_host, http_bind.port(), name),
        rtmp: format!("rtmp://{}:{}/live/{}", rtmp_host, rtmp_bind.port(), name),
    }
}
```

- [ ] **Step 5: Change CLI output labels**

In `crates/ace-engine/src/cli.rs`, inside `run_broadcast`, replace the existing `host` and `OBS ingest URL` formatting with:

```rust
let urls = crate::runtime::broadcast_ingest_urls(
    runtime.config.bind,
    runtime.config.rtmp_bind,
    public_host.clone(),
    &name,
);
let transport_host = public_host.unwrap_or_else(|| bind.ip().to_string());
```

Replace the `OBS ingest URL` block with:

```rust
eprintln!("RAW Ingest URL: {} (MPEG-TS)", urls.raw);
eprintln!("RTMP Ingest URL: {}", urls.rtmp);
```

Keep the transport URL output, but use `transport_host`:

```rust
eprintln!(
    "Transport URL: http://{}:{}/broadcast/{}",
    transport_host,
    bind.port(),
    name
);
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p ace-engine default_config_has_rtmp_bind_on_localhost_1935
cargo test -p ace-engine broadcast_urls_use
cargo test -p ace-engine parses_broadcast_public_host
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs crates/ace-engine/src/cli.rs
git commit -m "ace-engine: print raw and rtmp ingest urls"
```

---

### Task 3: RTMP-to-TS Remuxer

**Files:**
- Modify: `crates/ace-engine/Cargo.toml`
- Create: `crates/ace-engine/src/rtmp_ts.rs`
- Modify: `crates/ace-engine/src/lib.rs`

- [ ] **Step 1: Add dependency before the failing test**

Add to `crates/ace-engine/Cargo.toml`:

```toml
rtmp-rs = "0.5"
```

- [ ] **Step 2: Write failing muxer test**

Create `crates/ace-engine/src/rtmp_ts.rs` with this test shell:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rtmp_rs::media::aac::{generate_adts_header, AudioSpecificConfig};
    use rtmp_rs::media::{AacData, H264Data};

    fn avc_config() -> rtmp_rs::media::h264::AvcConfig {
        rtmp_rs::media::h264::AvcConfig {
            profile: 100,
            compatibility: 0,
            level: 31,
            nalu_length_size: 4,
            sps: vec![Bytes::from_static(&[0x67, 0x64, 0x00, 0x1f])],
            pps: vec![Bytes::from_static(&[0x68, 0xef, 0x38])],
            raw: Bytes::new(),
        }
    }

    fn aac_config() -> AudioSpecificConfig {
        AudioSpecificConfig::parse(Bytes::from_static(&[0x12, 0x10])).unwrap()
    }

    #[test]
    fn muxer_emits_aligned_ts_for_h264_and_aac() {
        let mut muxer = RtmpTsMuxer::new();
        let mut out = Vec::new();

        out.extend(muxer.push_video(&H264Data::SequenceHeader(avc_config()), 0));
        out.extend(muxer.push_audio(&AacData::SequenceHeader(aac_config()), 0));
        out.extend(muxer.push_video(
            &H264Data::Frame {
                keyframe: true,
                composition_time: 0,
                nalus: Bytes::from_static(&[
                    0x00, 0x00, 0x00, 0x05,
                    0x65, 0x88, 0x84, 0x21, 0xa0,
                ]),
            },
            40,
        ));
        out.extend(muxer.push_audio(
            &AacData::Frame {
                data: Bytes::from_static(&[0x21, 0x10, 0x56, 0xe5]),
            },
            42,
        ));

        assert!(!out.is_empty());
        assert_eq!(out.len() % ace_media::mpegts::TS_PACKET_LEN, 0);
        assert!(out.chunks(188).all(|packet| packet[0] == 0x47));

        let adts = generate_adts_header(&aac_config(), 4);
        assert!(
            out.windows(adts.len()).any(|window| window == adts),
            "AAC frames must be wrapped in ADTS before MPEG-TS muxing"
        );
        assert!(
            out.windows(4).any(|window| window == [0x00, 0x00, 0x00, 0x01]),
            "H.264 frames must be converted to Annex B start-code form"
        );
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run:

```bash
cargo test -p ace-engine rtmp_ts::tests::muxer_emits_aligned_ts_for_h264_and_aac
```

Expected: compile failure because `RtmpTsMuxer` does not exist.

- [ ] **Step 4: Implement the narrow muxer**

Add to `crates/ace-engine/src/rtmp_ts.rs` above the test:

```rust
//! Narrow RTMP H.264/AAC to MPEG-TS remuxing for broadcast ingest.

use bytes::{Buf, Bytes};
use rtmp_rs::media::aac::{generate_adts_header, AudioSpecificConfig};
use rtmp_rs::media::h264::{AvcConfig, NaluIterator};
use rtmp_rs::media::{AacData, H264Data};

const PAT_PID: u16 = 0x0000;
const PMT_PID: u16 = 0x1000;
const VIDEO_PID: u16 = 0x0100;
const AUDIO_PID: u16 = 0x0101;
const VIDEO_STREAM_ID: u8 = 0xe0;
const AUDIO_STREAM_ID: u8 = 0xc0;

pub struct RtmpTsMuxer {
    video_config: Option<AvcConfig>,
    audio_config: Option<AudioSpecificConfig>,
    pat_cc: u8,
    pmt_cc: u8,
    video_cc: u8,
    audio_cc: u8,
    wrote_tables: bool,
}

impl RtmpTsMuxer {
    pub fn new() -> Self {
        Self {
            video_config: None,
            audio_config: None,
            pat_cc: 0,
            pmt_cc: 0,
            video_cc: 0,
            audio_cc: 0,
            wrote_tables: false,
        }
    }

    pub fn push_video(&mut self, frame: &H264Data, timestamp_ms: u32) -> Vec<u8> {
        match frame {
            H264Data::SequenceHeader(config) => {
                self.video_config = Some(config.clone());
                Vec::new()
            }
            H264Data::Frame {
                keyframe,
                composition_time,
                nalus,
            } => {
                let Some(config) = self.video_config.clone() else {
                    return Vec::new();
                };
                let mut access_unit = Vec::new();
                access_unit.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x09, 0xf0]);
                if *keyframe {
                    for sps in &config.sps {
                        push_annex_b_nalu(&mut access_unit, sps);
                    }
                    for pps in &config.pps {
                        push_annex_b_nalu(&mut access_unit, pps);
                    }
                }
                for nalu in NaluIterator::new(nalus.as_ref(), config.nalu_length_size) {
                    push_annex_b_nalu(&mut access_unit, nalu);
                }
                let dts = pts90(timestamp_ms);
                let pts = pts90(timestamp_ms.saturating_add((*composition_time).max(0) as u32));
                self.packetize_pes(VIDEO_PID, VIDEO_STREAM_ID, dts, Some(pts), &access_unit)
            }
            H264Data::EndOfSequence => Vec::new(),
        }
    }

    pub fn push_audio(&mut self, frame: &AacData, timestamp_ms: u32) -> Vec<u8> {
        match frame {
            AacData::SequenceHeader(config) => {
                self.audio_config = Some(config.clone());
                Vec::new()
            }
            AacData::Frame { data } => {
                let Some(config) = self.audio_config.clone() else {
                    return Vec::new();
                };
                let mut aac = Vec::with_capacity(7 + data.len());
                aac.extend_from_slice(&generate_adts_header(&config, data.len()));
                aac.extend_from_slice(data);
                let pts = pts90(timestamp_ms);
                self.packetize_pes(AUDIO_PID, AUDIO_STREAM_ID, pts, None, &aac)
            }
        }
    }

    fn packetize_pes(
        &mut self,
        pid: u16,
        stream_id: u8,
        dts: u64,
        pts: Option<u64>,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        if !self.wrote_tables {
            self.write_tables(&mut out);
            self.wrote_tables = true;
        }
        let pes = pes_packet(stream_id, dts, pts, payload);
        let cc = if pid == VIDEO_PID {
            &mut self.video_cc
        } else {
            &mut self.audio_cc
        };
        write_ts_payload(&mut out, pid, cc, true, &pes);
        out
    }

    fn write_tables(&mut self, out: &mut Vec<u8>) {
        let pat = pat_section();
        let pmt = pmt_section();
        write_ts_payload(out, PAT_PID, &mut self.pat_cc, true, &pat);
        write_ts_payload(out, PMT_PID, &mut self.pmt_cc, true, &pmt);
    }
}

fn push_annex_b_nalu(out: &mut Vec<u8>, nalu: &[u8]) {
    out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    out.extend_from_slice(nalu);
}

fn pts90(ms: u32) -> u64 {
    u64::from(ms) * 90
}

fn pes_packet(stream_id: u8, dts: u64, pts: Option<u64>, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(19 + payload.len());
    out.extend_from_slice(&[0x00, 0x00, 0x01, stream_id]);
    out.extend_from_slice(&[0x00, 0x00]);
    out.push(0x80);
    match pts {
        Some(pts) if pts != dts => {
            out.push(0xc0);
            out.push(10);
            write_ts_timestamp(&mut out, 0x03, pts);
            write_ts_timestamp(&mut out, 0x01, dts);
        }
        Some(pts) => {
            out.push(0x80);
            out.push(5);
            write_ts_timestamp(&mut out, 0x02, pts);
        }
        None => {
            out.push(0x80);
            out.push(5);
            write_ts_timestamp(&mut out, 0x02, dts);
        }
    }
    out.extend_from_slice(payload);
    out
}

fn write_ts_timestamp(out: &mut Vec<u8>, prefix: u8, ts: u64) {
    let ts = ts & 0x1ffffffff;
    out.push((prefix << 4) | (((ts >> 30) as u8 & 0x07) << 1) | 1);
    out.push((ts >> 22) as u8);
    out.push((((ts >> 15) as u8 & 0x7f) << 1) | 1);
    out.push((ts >> 7) as u8);
    out.push(((ts as u8 & 0x7f) << 1) | 1);
}

fn write_ts_payload(out: &mut Vec<u8>, pid: u16, cc: &mut u8, start: bool, payload: &[u8]) {
    let mut rest = payload;
    let mut first = true;
    while !rest.is_empty() {
        let payload_start = first && start;
        let payload_capacity = 184;
        let take = rest.len().min(payload_capacity);
        let remaining_space = payload_capacity - take;
        let adaptation = remaining_space > 0;

        out.push(0x47);
        out.push(((payload_start as u8) << 6) | ((pid >> 8) as u8 & 0x1f));
        out.push(pid as u8);
        out.push(if adaptation { 0x30 } else { 0x10 } | (*cc & 0x0f));
        *cc = (*cc + 1) & 0x0f;

        if adaptation {
            out.push(remaining_space as u8 - 1);
            if remaining_space > 1 {
                out.push(0x00);
                out.extend(std::iter::repeat_n(0xff, remaining_space - 2));
            }
        }

        out.extend_from_slice(&rest[..take]);
        rest = &rest[take..];
        first = false;
    }
}

fn pat_section() -> Vec<u8> {
    let mut section = vec![
        0x00, 0x00, 0xb0, 0x0d, 0x00, 0x01, 0xc1, 0x00, 0x00,
        0x00, 0x01, 0xf0 | ((PMT_PID >> 8) as u8 & 0x1f), PMT_PID as u8,
    ];
    let crc = mpeg_crc32(&section[1..]);
    section.extend_from_slice(&crc.to_be_bytes());
    section
}

fn pmt_section() -> Vec<u8> {
    let section_len = 13 + 5 + 5 + 4;
    let mut section = vec![
        0x00,
        0x02,
        0xb0 | ((section_len >> 8) as u8 & 0x0f),
        section_len as u8,
        0x00,
        0x01,
        0xc1,
        0x00,
        0x00,
        0xe0 | ((VIDEO_PID >> 8) as u8 & 0x1f),
        VIDEO_PID as u8,
        0xf0,
        0x00,
        0x1b,
        0xe0 | ((VIDEO_PID >> 8) as u8 & 0x1f),
        VIDEO_PID as u8,
        0xf0,
        0x00,
        0x0f,
        0xe0 | ((AUDIO_PID >> 8) as u8 & 0x1f),
        AUDIO_PID as u8,
        0xf0,
        0x00,
    ];
    let crc = mpeg_crc32(&section[1..]);
    section.extend_from_slice(&crc.to_be_bytes());
    section
}

fn mpeg_crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffffffffu32;
    for &byte in bytes {
        crc ^= u32::from(byte) << 24;
        for _ in 0..8 {
            if (crc & 0x80000000) != 0 {
                crc = (crc << 1) ^ 0x04c11db7;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}
```

Remove the unused `bytes::{Buf, Bytes}` import member `Buf` if the compiler reports it unused.

Add to `crates/ace-engine/src/lib.rs`:

```rust
pub mod rtmp_ts;
```

- [ ] **Step 5: Run muxer test**

Run:

```bash
cargo test -p ace-engine rtmp_ts::tests::muxer_emits_aligned_ts_for_h264_and_aac
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/Cargo.toml Cargo.lock crates/ace-engine/src/rtmp_ts.rs crates/ace-engine/src/lib.rs
git commit -m "ace-engine: mux rtmp h264 aac to ts"
```

---

### Task 4: RTMP Server Handler

**Files:**
- Create: `crates/ace-engine/src/rtmp.rs`
- Modify: `crates/ace-engine/src/runtime.rs`
- Modify: `crates/ace-engine/src/lib.rs`

- [ ] **Step 1: Write failing handler unit tests**

Create `crates/ace-engine/src/rtmp.rs` with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::BroadcastState;
    use rtmp_rs::protocol::message::{ConnectParams, PublishParams};
    use rtmp_rs::server::handler::AuthResult;
    use rtmp_rs::session::SessionContext;
    use std::net::SocketAddr;

    fn state() -> BroadcastState {
        BroadcastState {
            registry: crate::broadcast::BroadcastRegistry::new(),
            seed_registry: ace_swarm::listen::SeedRegistry::new(),
            trackers: vec![],
            store_bytes: 4 << 20,
            inbound_peer_port: None,
        }
    }

    fn session() -> SessionContext {
        SessionContext {
            session_id: 1,
            peer_addr: "127.0.0.1:40000".parse::<SocketAddr>().unwrap(),
        }
    }

    #[tokio::test]
    async fn connect_accepts_only_live_app() {
        let handler = RtmpIngestHandler::new(state());
        assert!(matches!(
            handler
                .on_connect(
                    &session(),
                    &ConnectParams {
                        app: "live".to_string(),
                        ..Default::default()
                    },
                )
                .await,
            AuthResult::Accept
        ));
        assert!(matches!(
            handler
                .on_connect(
                    &session(),
                    &ConnectParams {
                        app: "vod".to_string(),
                        ..Default::default()
                    },
                )
                .await,
            AuthResult::Reject(_)
        ));
    }

    #[tokio::test]
    async fn publish_mints_stream_key_broadcast() {
        let bs = state();
        let registry = bs.registry.clone();
        let handler = RtmpIngestHandler::new(bs);
        let result = handler
            .on_publish(
                &session(),
                &PublishParams {
                    stream_key: "mychan".to_string(),
                    publish_type: "live".to_string(),
                },
            )
            .await;

        assert!(matches!(result, AuthResult::Accept));
        assert!(
            registry.get("mychan").await.is_some(),
            "RTMP publish must mint/resume the broadcast named by stream key"
        );
    }
}
```

If `SessionContext` fields differ, inspect `rtmp-rs/src/session/context.rs` and initialize the exact public fields.

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p ace-engine rtmp::tests::connect_accepts_only_live_app
```

Expected: compile failure because `RtmpIngestHandler` and module export do not exist.

- [ ] **Step 3: Implement RTMP handler**

Add to `crates/ace-engine/src/rtmp.rs` above tests:

```rust
//! RTMP broadcast ingest listener and handler.

use crate::broadcast_ingest::BroadcastIngest;
use crate::http::BroadcastState;
use crate::rtmp_ts::RtmpTsMuxer;
use rtmp_rs::media::{AacData, H264Data};
use rtmp_rs::protocol::message::{ConnectParams, PublishParams};
use rtmp_rs::server::handler::{AuthResult, MediaDeliveryMode, RtmpHandler};
use rtmp_rs::session::{SessionContext, StreamContext};
use rtmp_rs::{RtmpServer, ServerConfig};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Mutex;

pub async fn serve_rtmp(
    bind: SocketAddr,
    broadcasts: BroadcastState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ServerConfig::with_addr(bind).disable_gop_buffer();
    let server = RtmpServer::new(config, RtmpIngestHandler::new(broadcasts));
    server.run().await?;
    Ok(())
}

pub struct RtmpIngestHandler {
    broadcasts: BroadcastState,
    streams: Mutex<BTreeMap<String, RtmpStreamIngest>>,
}

struct RtmpStreamIngest {
    ingest: BroadcastIngest,
    muxer: RtmpTsMuxer,
}

impl RtmpIngestHandler {
    pub fn new(broadcasts: BroadcastState) -> Self {
        Self {
            broadcasts,
            streams: Mutex::new(BTreeMap::new()),
        }
    }
}

impl RtmpHandler for RtmpIngestHandler {
    async fn on_connect(&self, _ctx: &SessionContext, params: &ConnectParams) -> AuthResult {
        if params.app == "live" {
            AuthResult::Accept
        } else {
            AuthResult::Reject("unsupported RTMP app; use /live".to_string())
        }
    }

    async fn on_publish(&self, _ctx: &SessionContext, params: &PublishParams) -> AuthResult {
        let name = params.stream_key.trim();
        if name.is_empty() {
            return AuthResult::Reject("empty stream key".to_string());
        }
        let (bc, freshly_minted) = self
            .broadcasts
            .registry
            .start_or_resume(
                name,
                name,
                &self.broadcasts.trackers,
                &self.broadcasts.seed_registry,
                self.broadcasts.store_bytes,
            )
            .await;
        if freshly_minted {
            announce_broadcast(&self.broadcasts, &bc);
        }
        let ingest = BroadcastIngest::new(bc.store.clone(), bc.auth.clone());
        self.streams.lock().unwrap().insert(
            name.to_string(),
            RtmpStreamIngest {
                ingest,
                muxer: RtmpTsMuxer::new(),
            },
        );
        AuthResult::Accept
    }

    async fn on_video_frame(&self, ctx: &StreamContext, frame: &H264Data, timestamp: u32) {
        let bytes = {
            let mut streams = self.streams.lock().unwrap();
            let Some(stream) = streams.get_mut(&ctx.stream_key) else {
                return;
            };
            stream.muxer.push_video(frame, timestamp)
        };
        if !bytes.is_empty() {
            let mut ingest = {
                let mut streams = self.streams.lock().unwrap();
                let Some(stream) = streams.remove(&ctx.stream_key) else {
                    return;
                };
                stream
            };
            ingest.ingest.push_bytes(&bytes).await;
            self.streams
                .lock()
                .unwrap()
                .insert(ctx.stream_key.clone(), ingest);
        }
    }

    async fn on_audio_frame(&self, ctx: &StreamContext, frame: &AacData, timestamp: u32) {
        let bytes = {
            let mut streams = self.streams.lock().unwrap();
            let Some(stream) = streams.get_mut(&ctx.stream_key) else {
                return;
            };
            stream.muxer.push_audio(frame, timestamp)
        };
        if !bytes.is_empty() {
            let mut ingest = {
                let mut streams = self.streams.lock().unwrap();
                let Some(stream) = streams.remove(&ctx.stream_key) else {
                    return;
                };
                stream
            };
            ingest.ingest.push_bytes(&bytes).await;
            self.streams
                .lock()
                .unwrap()
                .insert(ctx.stream_key.clone(), ingest);
        }
    }

    async fn on_unpublish(&self, ctx: &StreamContext) {
        let stream = self.streams.lock().unwrap().remove(&ctx.stream_key);
        if let Some(mut stream) = stream {
            stream.ingest.finish().await;
        }
    }

    async fn on_disconnect(&self, _ctx: &SessionContext) {}

    fn media_delivery_mode(&self) -> MediaDeliveryMode {
        MediaDeliveryMode::ParsedFrames
    }
}

fn announce_broadcast(bs: &BroadcastState, bc: &crate::broadcast::Broadcast) {
    if let Some(port) = bs.inbound_peer_port {
        let trackers = bs.trackers.clone();
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers.clone(),
            bc.infohash,
            port,
        ));
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers,
            bc.content_id,
            port,
        ));
    }
}
```

If `BroadcastIngest` cannot be moved out of the map while awaiting cleanly, replace the `Mutex<BTreeMap<...>>` with `tokio::sync::Mutex<BTreeMap<...>>` and ensure no guard is held across `.await` in methods.

Add to `crates/ace-engine/src/lib.rs`:

```rust
pub mod rtmp;
```

- [ ] **Step 4: Start RTMP from runtime**

In `crates/ace-engine/src/runtime.rs`, inside `serve_http` after the inbound seeding block and before binding HTTP listener, add:

```rust
let rtmp_bind = config.rtmp_bind;
let rtmp_broadcasts = broadcasts.clone();
tokio::spawn(async move {
    if let Err(e) = crate::rtmp::serve_rtmp(rtmp_bind, rtmp_broadcasts).await {
        eprintln!("outpace: RTMP ingest stopped: {e}");
    }
});
eprintln!("outpace: RTMP ingest listening on rtmp://{}/live/<name>", rtmp_bind);
```

- [ ] **Step 5: Run handler tests**

Run:

```bash
cargo test -p ace-engine rtmp::tests::connect_accepts_only_live_app
cargo test -p ace-engine rtmp::tests::publish_mints_stream_key_broadcast
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-engine/src/rtmp.rs crates/ace-engine/src/runtime.rs crates/ace-engine/src/lib.rs
git commit -m "ace-engine: add rtmp broadcast ingest listener"
```

---

### Task 5: RTMP Loopback Integration

**Files:**
- Modify: `crates/ace-engine/src/rtmp.rs`

- [ ] **Step 1: Write failing loopback test**

Add to `crates/ace-engine/src/rtmp.rs` tests:

```rust
#[tokio::test]
async fn rtmp_publish_reaches_broadcast_piece_store() {
    use std::process::Stdio;
    use tokio::process::Command;

    let bs = state();
    let registry = bs.registry.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = tokio::spawn(async move {
        let _ = serve_rtmp(addr, bs).await;
    });

    let status = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=128x96:rate=10",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=44100",
            "-t",
            "2",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-g",
            "10",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-f",
            "flv",
            &format!("rtmp://{addr}/live/loop"),
        ])
        .stdin(Stdio::null())
        .status()
        .await
        .expect("ffmpeg is installed for RTMP loopback test");
    assert!(status.success(), "ffmpeg RTMP publish failed: {status}");

    let bc = registry.get("loop").await.expect("broadcast was minted");
    for _ in 0..100 {
        if bc.store.lock().await.chunk(0, 0).is_some() {
            server.abort();
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    server.abort();
    panic!("RTMP publish did not reach broadcast piece store");
}
```

- [ ] **Step 2: Run loopback test to verify it fails or is unsupported**

Run:

```bash
cargo test -p ace-engine rtmp::tests::rtmp_publish_reaches_broadcast_piece_store -- --nocapture
```

Expected before final fixes: fail because server readiness, handler async map handling, muxing, or ffmpeg availability is not yet fully integrated. If `ffmpeg` is not installed, convert this test to `#[ignore]` with the message `requires ffmpeg`.

- [ ] **Step 3: Make loopback deterministic**

If server startup races the publisher, add this helper to tests and use it before invoking ffmpeg:

```rust
async fn wait_for_tcp(addr: std::net::SocketAddr) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("RTMP server did not start on {addr}");
}
```

Call:

```rust
wait_for_tcp(addr).await;
```

- [ ] **Step 4: Run loopback and broadcast tests**

Run:

```bash
cargo test -p ace-engine rtmp::tests::rtmp_publish_reaches_broadcast_piece_store -- --nocapture
cargo test -p ace-engine broadcast
```

Expected: loopback passes when ffmpeg is available; broadcast tests still pass.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/rtmp.rs
git commit -m "ace-engine: verify rtmp broadcast loopback"
```

---

### Task 6: Documentation and Final Verification

**Files:**
- Modify: `docs/protocol/notes/51-cli-and-broadcast-content-id.md`
- Modify: `README.md`

- [ ] **Step 1: Update protocol note**

In `docs/protocol/notes/51-cli-and-broadcast-content-id.md`, add:

```markdown
Broadcast ingest is now exposed through two inputs:

- raw MPEG-TS over HTTP PUT: `http://<host>:<http-port>/broadcast/<name>`
- RTMP publish: `rtmp://<host>:<rtmp-port>/live/<name>`

The RTMP path accepts H.264/AAC publisher input and remuxes it to MPEG-TS before entering
the same signing/chunking/store pipeline used by raw ingest.
```

- [ ] **Step 2: Update resume**

In `README.md`, replace the sentence that says RTMP/SRT ingest is a future follow-up with:

```markdown
RTMP ingest is implemented for broadcast origination at `rtmp://<host>:<rtmp-port>/live/<name>`;
it accepts H.264/AAC and remuxes to the same MPEG-TS ingest path as raw HTTP PUT. SRT ingest
remains a future follow-up.
```

- [ ] **Step 3: Run full verification**

Run:

```bash
cargo test -p ace-engine
cargo test -p ace-media
cargo test -p ace-wire
cargo test -p ace-swarm
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 4: Manual smoke command**

Run:

```bash
timeout 5 env OUTPACE_BIND=127.0.0.1:6990 OUTPACE_RTMP_BIND=127.0.0.1:19935 cargo run -p ace-engine --bin outpace -- broadcast smoke 2>/tmp/outpace-rtmp-smoke.log || true
rg -n "RAW Ingest URL|RTMP Ingest URL|OBS ingest URL" /tmp/outpace-rtmp-smoke.log
```

Expected output contains `RAW Ingest URL` and `RTMP Ingest URL`; it does not contain `OBS ingest URL`.

- [ ] **Step 5: Commit**

```bash
git add docs/protocol/notes/51-cli-and-broadcast-content-id.md README.md
git commit -m "docs: document rtmp broadcast ingest"
```

---

## Self-Review

Spec coverage:

- Separate RTMP listener: Task 4.
- `OUTPACE_RTMP_BIND`: Task 2.
- Raw and RTMP CLI output labels: Task 2 and Task 6 smoke.
- RTMP `/live/<name>` routing: Task 4 and Task 5.
- H.264/AAC RTMP media remux to MPEG-TS: Task 3.
- Existing raw HTTP ingest preserved through shared helper: Task 1 and Task 5.
- Tests and documentation: Tasks 1-6.

Placeholder scan: every code-changing step names exact files, concrete snippets, commands, and expected outcomes. Conditional instructions point to concrete fallback edits if a third-party crate API differs.

Type consistency: `BroadcastIngest`, `RtmpTsMuxer`, `RtmpIngestHandler`, `BroadcastIngestUrls`, and `Config::rtmp_bind` are introduced before subsequent tasks use them.
