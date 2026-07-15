//! MPEG-TS packet basics (pure).
//!
//! An MPEG-TS stream is a sequence of fixed 188-byte packets, each starting with the
//! sync byte `0x47`. Before re-exposing or segmenting we must confirm the byte stream is
//! packet-aligned (and find the alignment offset if it isn't).

/// MPEG-TS sync byte at the start of every packet.
pub const TS_SYNC: u8 = 0x47;
/// MPEG-TS packet length in bytes.
pub const TS_PACKET_LEN: usize = 188;

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
const MAX_SERVICE_NAME_BYTES: usize = TS_PACKET_LEN - 4 - 1 - 25 - SERVICE_PROVIDER_NAME.len();

/// Number of consecutive packets to check when probing for sync alignment.
const PROBE_PACKETS: usize = 4;

/// Find the smallest offset `0..188` at which `buf` is TS-packet-aligned — i.e. the sync
/// byte `0x47` recurs every 188 bytes for up to [`PROBE_PACKETS`] packets. Returns `None`
/// if no alignment is found (not a TS stream / too short).
pub fn find_sync_offset(buf: &[u8]) -> Option<usize> {
    for off in 0..TS_PACKET_LEN {
        if off >= buf.len() {
            break;
        }
        let mut ok = true;
        let mut checked = 0;
        let mut pos = off;
        while pos < buf.len() && checked < PROBE_PACKETS {
            if buf[pos] != TS_SYNC {
                ok = false;
                break;
            }
            pos += TS_PACKET_LEN;
            checked += 1;
        }
        if ok && checked > 0 {
            return Some(off);
        }
    }
    None
}

/// True iff `buf` is a whole number of TS packets and every packet starts with `0x47`.
pub fn is_aligned(buf: &[u8]) -> bool {
    if buf.is_empty() || !buf.len().is_multiple_of(TS_PACKET_LEN) {
        return false;
    }
    buf.chunks_exact(TS_PACKET_LEN).all(|p| p[0] == TS_SYNC)
}

/// Number of complete TS packets in `buf` (truncating any trailing partial packet).
pub fn packet_count(buf: &[u8]) -> usize {
    buf.len() / TS_PACKET_LEN
}

/// Maximum unconfirmed tail retained between pushes. Confirming a packet start needs the
/// byte AND its `+TS_PACKET_LEN` neighbor, so only the final `2 * TS_PACKET_LEN - 1` bytes
/// can still become sync-lockable once more data arrives — any earlier byte already has its
/// neighbor present and has been scan-rejected. Capping the retained tail to this window
/// bounds memory against a peer that streams unsynchronizable (non-TS) data forever (#14),
/// without dropping anything a later push could have re-locked.
const MAX_UNSYNCED_TAIL: usize = 2 * TS_PACKET_LEN - 1;

/// Stateful TS packet re-aligner for a continuous byte stream that may contain misaligned
/// regions (e.g. Acestream live pieces, which are each internally 188-aligned but don't
/// byte-chain — ~one partial packet of junk at each piece boundary). Feed arbitrary byte
/// runs; it emits only sync-locked 188-byte packets, discarding bytes as needed to re-lock,
/// and buffers the unconfirmed tail (bounded to [`MAX_UNSYNCED_TAIL`]) until more data arrives.
#[derive(Default)]
pub struct TsResync {
    buf: Vec<u8>,
}

impl TsResync {
    pub fn new() -> Self {
        TsResync { buf: Vec::new() }
    }

    /// Append `data`; return all newly emittable sync-locked packets (a whole number of
    /// 188-byte packets, each starting with `0x47`). Sync is confirmed with one packet of
    /// lookahead (`0x47` at the candidate AND at +188), so output trails input by ≤1 packet.
    pub fn push(&mut self, data: &[u8]) -> Vec<u8> {
        self.buf.extend_from_slice(data);
        let n = self.buf.len();
        let mut out = Vec::new();
        let mut i = 0;
        while i + 2 * TS_PACKET_LEN <= n {
            if self.buf[i] == TS_SYNC && self.buf[i + TS_PACKET_LEN] == TS_SYNC {
                out.extend_from_slice(&self.buf[i..i + TS_PACKET_LEN]);
                i += TS_PACKET_LEN;
            } else {
                // Lost lock: scan forward to the next confirmable packet start.
                let mut j = i + 1;
                while j + TS_PACKET_LEN < n
                    && !(self.buf[j] == TS_SYNC && self.buf[j + TS_PACKET_LEN] == TS_SYNC)
                {
                    j += 1;
                }
                if j + TS_PACKET_LEN < n {
                    i = j;
                } else {
                    break; // can't confirm yet; keep from i for the next push
                }
            }
        }
        self.buf.drain(0..i);
        // Bound the unconfirmed tail: if a run of unsynchronizable bytes has accumulated
        // past the sync lookahead, everything before the last `MAX_UNSYNCED_TAIL` bytes has
        // already been scan-rejected (its `+TS_PACKET_LEN` neighbor is present) and can never
        // re-lock, so drop it. Bounds memory against a non-TS junk flood (#14).
        if self.buf.len() > MAX_UNSYNCED_TAIL {
            let excess = self.buf.len() - MAX_UNSYNCED_TAIL;
            self.buf.drain(0..excess);
        }
        out
    }
}

/// Tracks the MPEG-TS tables and video PID needed to identify a decodable access point.
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

impl VideoAccessPointState {
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn reset(&mut self) {
        let name = self.service_name.take();
        *self = Self {
            service_name: name,
            ..Self::default()
        };
    }

    pub fn observe(&mut self, packet: &[u8]) -> bool {
        if packet.len() != TS_PACKET_LEN || packet[0] != 0x47 {
            return false;
        }
        let pid = ts_pid(packet);
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
        } else if Some(pid) == self.pmt_pid {
            if let Some((video_pid, codec)) = parse_pmt_video(packet) {
                self.cached_pmt = Some(packet.to_vec());
                self.video_pid = Some(video_pid);
                self.video_codec = Some(codec);
            }
        }
        Some(pid) == self.video_pid && is_random_access_point(packet, self.video_codec)
    }

    pub fn table_prefix(&self) -> Option<Vec<u8>> {
        let mut prefix = Vec::with_capacity(TS_PACKET_LEN * 3);
        prefix.extend_from_slice(self.cached_pat.as_ref()?);
        prefix.extend_from_slice(self.cached_pmt.as_ref()?);
        if let (Some(name), Some(program)) = (self.service_name.as_deref(), self.program_number) {
            prefix.extend_from_slice(&build_sdt(program, name));
        }
        Some(prefix)
    }
}

/// Per-client gate that drops a stream's opening packets until the first clean keyframe, so a
/// player joining a live MPEG-TS mid-GOP starts on a decodable picture instead of garbage.
///
/// Feed it sync-locked, 188-aligned TS packets (e.g. [`TsResync`] output). Until it locks it
/// emits nothing; on the first random-access point it emits the most-recent PAT and PMT
/// followed by the keyframe packet, then passes everything through verbatim. If no keyframe is
/// found within `max_scan_packets`, it gives up gating and passes through (better imperfect
/// video than a starved client).
pub struct KeyframeGate {
    buf: Vec<u8>,
    locked: bool,
    access: VideoAccessPointState,
    scanned: usize,
    max_scan_packets: usize,
}

/// Default packet budget before the gate gives up looking for a keyframe and passes through.
/// ~60k packets ≈ 11 MB ≈ a few seconds of HD — far longer than any sane GOP.
const DEFAULT_MAX_SCAN_PACKETS: usize = 60_000;

impl KeyframeGate {
    pub fn new() -> Self {
        Self::with_max_scan_packets(DEFAULT_MAX_SCAN_PACKETS)
    }

    /// Like [`new`](Self::new) but with an explicit safety budget (packets scanned before
    /// falling back to passthrough when no keyframe is found).
    pub fn with_max_scan_packets(max_scan_packets: usize) -> Self {
        KeyframeGate {
            buf: Vec::new(),
            locked: false,
            access: VideoAccessPointState::new(),
            scanned: 0,
            max_scan_packets,
        }
    }

    /// Append `data` (assumed 188-aligned TS) and return the bytes to forward to the client.
    pub fn push(&mut self, data: &[u8]) -> Vec<u8> {
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        let mut i = 0;
        while i + TS_PACKET_LEN <= self.buf.len() {
            let pkt = &self.buf[i..i + TS_PACKET_LEN];
            if self.locked {
                out.extend_from_slice(pkt);
                i += TS_PACKET_LEN;
                continue;
            }
            self.scanned += 1;
            if self.access.observe(pkt) {
                // Lock on the keyframe: hand the player the tables, then the keyframe.
                if let Some(prefix) = self.access.table_prefix() {
                    out.extend_from_slice(&prefix);
                }
                out.extend_from_slice(pkt);
                self.locked = true;
            } else if self.scanned >= self.max_scan_packets {
                // Safety fallback: never found a keyframe; passthrough from here.
                out.extend_from_slice(pkt);
                self.locked = true;
            }
            // otherwise: drop this prefix packet and keep scanning.
            i += TS_PACKET_LEN;
        }
        self.buf.drain(0..i);
        out
    }
}

/// PID of a TS packet (13-bit).
fn ts_pid(pkt: &[u8]) -> u16 {
    (((pkt[1] & 0x1F) as u16) << 8) | pkt[2] as u16
}

/// True iff the payload_unit_start_indicator is set.
fn ts_pusi(pkt: &[u8]) -> bool {
    pkt[1] & 0x40 != 0
}

/// Byte offset of the payload within `pkt`, or `None` if the packet carries no payload.
fn ts_payload_offset(pkt: &[u8]) -> Option<usize> {
    let afc = (pkt[3] >> 4) & 0x03;
    let start = match afc {
        0b01 => 4,                   // payload only
        0b11 => 5 + pkt[4] as usize, // adaptation field, then payload
        _ => return None,            // 0b10 adaptation-only / 0b00 reserved
    };
    (start < TS_PACKET_LEN).then_some(start)
}

/// A random-access point: the start of a video access unit (PUSI) that is also a keyframe —
/// flagged either by the adaptation field's random_access_indicator or by an H.264 IDR/SPS
/// NAL in the payload (belt-and-suspenders across encoders).
fn is_random_access_point(pkt: &[u8], codec: Option<VideoCodec>) -> bool {
    if !ts_pusi(pkt) {
        return false;
    }
    ts_random_access_indicator(pkt) || payload_has_irap(pkt, codec)
}

fn ts_random_access_indicator(pkt: &[u8]) -> bool {
    let afc = (pkt[3] >> 4) & 0x03;
    if afc & 0b10 == 0 {
        return false; // no adaptation field
    }
    let af_len = pkt[4] as usize;
    af_len >= 1 && pkt[5] & 0x40 != 0
}

/// Scan the packet payload for an Annex-B start code `00 00 01` followed by a keyframe NAL for
/// the stream's `codec`: H.264 IDR (5) / SPS (7), or HEVC IRAP (16..=21) / VPS/SPS/PPS (32..=34).
/// The scan is codec-gated because the two codecs read the NAL-type bits differently (H.264 uses
/// `byte & 0x1F`, HEVC uses `(byte >> 1) & 0x3F`), so an H.264 slice type would otherwise be
/// misread as an HEVC parameter set (and vice versa). Without a resolved codec, returns false and
/// leaves keyframe detection to the codec-agnostic random_access_indicator.
fn payload_has_irap(pkt: &[u8], codec: Option<VideoCodec>) -> bool {
    let Some(codec) = codec else {
        return false;
    };
    let Some(off) = ts_payload_offset(pkt) else {
        return false;
    };
    let p = &pkt[off..];
    p.windows(4).any(|w| {
        if w[0] != 0 || w[1] != 0 || w[2] != 1 {
            return false;
        }
        match codec {
            VideoCodec::H264 => matches!(w[3] & 0x1F, 5 | 7),
            VideoCodec::Hevc => matches!((w[3] >> 1) & 0x3F, 16..=21 | 32..=34),
        }
    })
}

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

/// Elementary-stream video codec, as identified from the PMT `stream_type`. Determines which
/// Annex-B NAL layout [`payload_has_irap`] scans for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum VideoCodec {
    H264,
    Hevc,
}

/// Parse a PMT packet, returning the elementary PID and codec of the first recognized video
/// stream: H.264 (`stream_type 0x1B`) or HEVC (`0x24`).
fn parse_pmt_video(pkt: &[u8]) -> Option<(u16, VideoCodec)> {
    let sec = psi_section_start(pkt)?;
    if *pkt.get(sec)? != 0x02 {
        return None; // table_id must be PMT
    }
    let section_length = ((pkt.get(sec + 1)? & 0x0F) as usize) << 8 | *pkt.get(sec + 2)? as usize;
    let end = (sec + 3 + section_length)
        .saturating_sub(4)
        .min(TS_PACKET_LEN); // drop CRC
    let program_info_length =
        ((pkt.get(sec + 10)? & 0x0F) as usize) << 8 | *pkt.get(sec + 11)? as usize;
    let mut pos = sec + 12 + program_info_length;
    while pos + 5 <= end {
        let stream_type = pkt[pos];
        let epid = (((pkt[pos + 1] & 0x1F) as u16) << 8) | pkt[pos + 2] as u16;
        let es_info_length = ((pkt[pos + 3] & 0x0F) as usize) << 8 | pkt[pos + 4] as usize;
        match stream_type {
            0x1B => return Some((epid, VideoCodec::H264)),
            0x24 => return Some((epid, VideoCodec::Hevc)),
            _ => {}
        }
        pos += 5 + es_info_length;
    }
    None
}

/// Start offset of the PSI section within a table packet (skips payload offset + pointer_field).
fn psi_section_start(pkt: &[u8]) -> Option<usize> {
    let po = ts_payload_offset(pkt)?;
    let pointer = *pkt.get(po)? as usize;
    let sec = po + 1 + pointer;
    (sec < TS_PACKET_LEN).then_some(sec)
}

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

impl Default for KeyframeGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(fill: u8) -> Vec<u8> {
        let mut p = vec![fill; TS_PACKET_LEN];
        p[0] = TS_SYNC;
        p
    }

    /// Build a 188-byte TS packet for `pid` with the given PUSI / random-access-indicator
    /// flags and an optional payload (truncated/padded to fill the packet).
    fn ts(pid: u16, pusi: bool, rai: bool, payload: &[u8]) -> Vec<u8> {
        let mut p = vec![0xFFu8; TS_PACKET_LEN];
        p[0] = TS_SYNC;
        p[1] = ((pid >> 8) as u8 & 0x1F) | if pusi { 0x40 } else { 0 };
        p[2] = (pid & 0xFF) as u8;
        let payload_start = if rai {
            // adaptation field (1 byte length + 1 flags byte) + payload, AFC = 11
            p[3] = 0x30;
            p[4] = 1; // adaptation_field_length = just the flags byte
            p[5] = 0x40; // random_access_indicator
            6
        } else {
            p[3] = 0x10; // AFC = 01, payload only
            4
        };
        let n = payload.len().min(TS_PACKET_LEN - payload_start);
        p[payload_start..payload_start + n].copy_from_slice(&payload[..n]);
        p
    }

    /// A PSI table packet (PUSI set, pointer_field 0) carrying `section` on `pid`.
    fn psi(pid: u16, section: &[u8]) -> Vec<u8> {
        let mut payload = vec![0u8]; // pointer_field = 0
        payload.extend_from_slice(section);
        ts(pid, true, false, &payload)
    }

    /// Minimal PAT mapping program 1 → `pmt_pid` (CRC left zero; the gate doesn't check it).
    fn pat(pmt_pid: u16) -> Vec<u8> {
        // section after table_id: tsid(2) + flags(1) + sec#(1) + last#(1) + 1 program(4) + crc(4)
        let section_length = 5 + 4 + 4;
        let mut s = vec![0x00u8]; // table_id = PAT
        s.push(0xB0 | ((section_length >> 8) as u8 & 0x0F));
        s.push((section_length & 0xFF) as u8);
        s.extend_from_slice(&[0x00, 0x01]); // transport_stream_id
        s.push(0xC1); // version 0, current
        s.push(0x00); // section_number
        s.push(0x00); // last_section_number
        s.extend_from_slice(&[0x00, 0x01]); // program_number = 1
        s.extend_from_slice(&[0xE0 | ((pmt_pid >> 8) as u8 & 0x1F), (pmt_pid & 0xFF) as u8]);
        s.extend_from_slice(&[0, 0, 0, 0]); // CRC32 placeholder
        psi(0, &s)
    }

    /// Minimal PMT declaring one H.264 video elementary stream on `video_pid`.
    fn pmt(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
        pmt_codec(pmt_pid, video_pid, 0x1B) // H.264
    }

    /// PMT declaring one video elementary stream of `stream_type` on `video_pid`.
    fn pmt_codec(pmt_pid: u16, video_pid: u16, stream_type: u8) -> Vec<u8> {
        // after table_id: prog#(2)+flags(1)+sec#(1)+last#(1)+pcrpid(2)+pinfolen(2)+stream(5)+crc(4)
        let section_length = 5 + 4 + 5 + 4;
        let mut s = vec![0x02u8]; // table_id = PMT
        s.push(0xB0 | ((section_length >> 8) as u8 & 0x0F));
        s.push((section_length & 0xFF) as u8);
        s.extend_from_slice(&[0x00, 0x01]); // program_number
        s.push(0xC1); // version 0, current
        s.push(0x00); // section_number
        s.push(0x00); // last_section_number
        s.extend_from_slice(&[
            0xE0 | ((video_pid >> 8) as u8 & 0x1F),
            (video_pid & 0xFF) as u8,
        ]); // PCR_PID = video
        s.extend_from_slice(&[0xF0, 0x00]); // program_info_length = 0
        s.push(stream_type);
        s.extend_from_slice(&[
            0xE0 | ((video_pid >> 8) as u8 & 0x1F),
            (video_pid & 0xFF) as u8,
        ]);
        s.extend_from_slice(&[0xF0, 0x00]); // ES_info_length = 0
        s.extend_from_slice(&[0, 0, 0, 0]); // CRC32 placeholder
        psi(pmt_pid, &s)
    }

    /// True iff `pkt` is on `pid` (single-packet helper for assertions).
    fn pid_of(pkt: &[u8]) -> u16 {
        (((pkt[1] & 0x1F) as u16) << 8) | pkt[2] as u16
    }

    const PMT_PID: u16 = 0x0100;
    const VIDEO_PID: u16 = 0x0101;
    const AUDIO_PID: u16 = 0x0102;

    fn random_access_packet(pid: u16) -> Vec<u8> {
        ts(pid, true, true, &[])
    }

    #[test]
    fn video_access_point_state_uses_pmt_video_pid_and_caches_tables() {
        let mut state = VideoAccessPointState::new();
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        assert!(!state.observe(&pat));
        assert!(!state.observe(&pmt));
        assert!(!state.observe(&random_access_packet(AUDIO_PID)));
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

    #[test]
    fn video_access_point_state_ignores_malformed_pat_without_corrupting_valid_state() {
        let mut state = VideoAccessPointState::new();
        let valid_pat = pat(PMT_PID);
        let valid_pmt = pmt(PMT_PID, VIDEO_PID);
        state.observe(&valid_pat);
        state.observe(&valid_pmt);
        let expected_prefix = [valid_pat, valid_pmt].concat();

        let mut malformed_pat = pat(PMT_PID);
        malformed_pat[5] = 0xff;
        assert!(!state.observe(&malformed_pat));
        assert_eq!(state.table_prefix().unwrap(), expected_prefix);
        assert!(state.observe(&random_access_packet(VIDEO_PID)));
    }

    #[test]
    fn video_access_point_state_ignores_malformed_pmt_without_corrupting_valid_state() {
        let mut state = VideoAccessPointState::new();
        let valid_pat = pat(PMT_PID);
        let valid_pmt = pmt(PMT_PID, VIDEO_PID);
        state.observe(&valid_pat);
        state.observe(&valid_pmt);
        let expected_prefix = [valid_pat, valid_pmt].concat();

        let mut malformed_pmt = pmt(PMT_PID, VIDEO_PID);
        malformed_pmt[5] = 0xff;
        assert!(!state.observe(&malformed_pmt));
        assert_eq!(state.table_prefix().unwrap(), expected_prefix);
        assert!(state.observe(&random_access_packet(VIDEO_PID)));
    }

    #[test]
    fn video_access_point_state_invalidates_dependent_state_when_pat_remaps_pmt() {
        const NEW_PMT_PID: u16 = 0x0200;
        const NEW_VIDEO_PID: u16 = 0x0201;

        let mut state = VideoAccessPointState::new();
        state.observe(&pat(PMT_PID));
        state.observe(&pmt(PMT_PID, VIDEO_PID));

        assert!(!state.observe(&pat(NEW_PMT_PID)));
        assert!(state.table_prefix().is_none());
        assert!(!state.observe(&random_access_packet(VIDEO_PID)));

        let new_pmt = pmt(NEW_PMT_PID, NEW_VIDEO_PID);
        assert!(!state.observe(&new_pmt));
        assert!(state.observe(&random_access_packet(NEW_VIDEO_PID)));
        assert_eq!(
            state.table_prefix().unwrap(),
            [pat(NEW_PMT_PID), new_pmt].concat()
        );
    }

    #[test]
    fn gate_drops_mid_gop_prefix_and_prepends_tables() {
        let mut g = KeyframeGate::new();
        let mut input = Vec::new();
        // Stream opens mid-GOP: tables, then a non-keyframe video packet, then the keyframe.
        input.extend(pat(PMT_PID));
        input.extend(pmt(PMT_PID, VIDEO_PID));
        input.extend(ts(VIDEO_PID, false, false, &[0xAA; 16])); // mid-GOP — must be dropped
        input.extend(ts(VIDEO_PID, true, true, &[0xBB; 16])); // keyframe (RAI)
        input.extend(ts(AUDIO_PID, true, false, &[0xCC; 16])); // post-keyframe audio — kept

        let out = g.push(&input);
        assert!(is_aligned(&out), "output must stay packet-aligned");
        let pkts: Vec<&[u8]> = out.chunks_exact(TS_PACKET_LEN).collect();
        // PAT, PMT, keyframe, audio — the mid-GOP video packet is gone.
        assert_eq!(pkts.len(), 4, "mid-GOP packet should be dropped");
        assert_eq!(pid_of(pkts[0]), 0, "first emitted packet is PAT");
        assert_eq!(pid_of(pkts[1]), PMT_PID, "then PMT");
        assert_eq!(pid_of(pkts[2]), VIDEO_PID, "then the keyframe");
        assert!(pkts[2][1] & 0x40 != 0, "keyframe packet has PUSI set");
        assert_eq!(pid_of(pkts[3]), AUDIO_PID, "then passthrough resumes");
    }

    #[test]
    fn gate_emits_nothing_before_a_keyframe_arrives() {
        let mut g = KeyframeGate::new();
        let mut input = Vec::new();
        input.extend(pat(PMT_PID));
        input.extend(pmt(PMT_PID, VIDEO_PID));
        input.extend(ts(VIDEO_PID, false, false, &[0xAA; 16])); // mid-GOP only
        input.extend(ts(AUDIO_PID, true, false, &[0xCC; 16]));
        assert!(g.push(&input).is_empty(), "no keyframe yet -> emit nothing");
    }

    #[test]
    fn gate_passes_everything_through_after_lock() {
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID));
        let locked = g.push(&ts(VIDEO_PID, true, true, &[0xBB; 16]));
        assert_eq!(packet_count(&locked), 3); // PAT + PMT + keyframe

        // After lock, even a non-keyframe mid-GOP video packet is forwarded verbatim.
        let after = g.push(&ts(VIDEO_PID, false, false, &[0xDD; 16]));
        assert_eq!(packet_count(&after), 1);
        assert_eq!(pid_of(&after), VIDEO_PID);
    }

    #[test]
    fn gate_detects_idr_nal_when_rai_is_not_set() {
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID));
        // Keyframe flagged only by an IDR NAL (type 5) in the payload, no RAI bit.
        let pes = [0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x00, 0x00, 0x01, 0x65];
        let out = g.push(&ts(VIDEO_PID, true, false, &pes));
        assert_eq!(packet_count(&out), 3, "IDR NAL should lock the gate");
        assert_eq!(
            pid_of(out.chunks_exact(TS_PACKET_LEN).nth(2).unwrap()),
            VIDEO_PID
        );
    }

    #[test]
    fn gate_does_not_lock_on_a_non_keyframe_pes_start() {
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID));
        // PES start (PUSI) but only a non-IDR slice NAL (type 1) and no RAI.
        let pes = [0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x00, 0x00, 0x01, 0x61];
        assert!(g.push(&ts(VIDEO_PID, true, false, &pes)).is_empty());
    }

    #[test]
    fn video_access_point_state_resolves_hevc_stream_type() {
        // HEVC (stream_type 0x24) must resolve the video PID so the codec-agnostic
        // random_access_indicator path can flag keyframes (broadcast HEVC sets RAI).
        let mut state = VideoAccessPointState::new();
        assert!(!state.observe(&pat(PMT_PID)));
        assert!(!state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x24)));
        assert!(!state.observe(&random_access_packet(AUDIO_PID)));
        assert!(
            state.observe(&random_access_packet(VIDEO_PID)),
            "a RAI packet on the HEVC video PID is a random-access point"
        );
    }

    #[test]
    fn gate_detects_hevc_irap_nal_when_rai_is_not_set() {
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt_codec(PMT_PID, VIDEO_PID, 0x24));
        // HEVC video. Keyframe flagged only by an IDR_W_RADL NAL (type 19), no RAI bit.
        // HEVC NAL header: byte0 = type << 1 = 19 << 1 = 0x26, byte1 = 0x01 (layer 0, tid 1).
        let pes = [
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x00, 0x00, 0x01, 0x26, 0x01,
        ];
        let out = g.push(&ts(VIDEO_PID, true, false, &pes));
        assert_eq!(packet_count(&out), 3, "HEVC IRAP NAL should lock the gate");
    }

    #[test]
    fn gate_does_not_treat_h264_pslice_as_hevc_irap() {
        // Guards the codec-gating: NAL byte 0x41 is an H.264 P-slice (type 1), but under HEVC's
        // (byte >> 1) & 0x3F it reads as 32 (VPS). An H.264 stream must not be mis-detected.
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID)); // H.264
        let pes = [0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x00, 0x00, 0x01, 0x41];
        assert!(g.push(&ts(VIDEO_PID, true, false, &pes)).is_empty());
    }

    #[test]
    fn gate_falls_back_to_passthrough_after_scan_budget() {
        let mut g = KeyframeGate::with_max_scan_packets(3);
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID));
        // 3rd scanned packet hits the budget with no keyframe -> passthrough begins here.
        let out = g.push(&ts(VIDEO_PID, false, false, &[0xAA; 16]));
        assert_eq!(packet_count(&out), 1, "budget exhausted -> stop gating");
        // and it stays open afterwards.
        let more = g.push(&ts(AUDIO_PID, false, false, &[0xCC; 16]));
        assert_eq!(packet_count(&more), 1);
    }

    #[test]
    fn gate_prepends_only_the_latest_cached_tables() {
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID));
        g.push(&pat(PMT_PID)); // tables repeat (as in real TS) before the keyframe
        g.push(&pmt(PMT_PID, VIDEO_PID));
        let out = g.push(&ts(VIDEO_PID, true, true, &[0xBB; 16]));
        // Exactly one PAT + one PMT precede the keyframe, not the whole history.
        assert_eq!(packet_count(&out), 3);
        let pkts: Vec<&[u8]> = out.chunks_exact(TS_PACKET_LEN).collect();
        assert_eq!(pid_of(pkts[0]), 0);
        assert_eq!(pid_of(pkts[1]), PMT_PID);
        assert_eq!(pid_of(pkts[2]), VIDEO_PID);
    }

    #[test]
    fn gate_handles_packets_split_across_pushes() {
        let mut g = KeyframeGate::new();
        let mut input = Vec::new();
        input.extend(pat(PMT_PID));
        input.extend(pmt(PMT_PID, VIDEO_PID));
        input.extend(ts(VIDEO_PID, false, false, &[0xAA; 16]));
        input.extend(ts(VIDEO_PID, true, true, &[0xBB; 16]));
        // Drip the bytes in awkward fragments straddling packet boundaries.
        let mut out = Vec::new();
        for fragment in input.chunks(57) {
            out.extend(g.push(fragment));
        }
        assert!(is_aligned(&out));
        let pkts: Vec<&[u8]> = out.chunks_exact(TS_PACKET_LEN).collect();
        assert_eq!(pkts.len(), 3); // PAT, PMT, keyframe (mid-GOP dropped)
        assert_eq!(pid_of(pkts[2]), VIDEO_PID);
    }

    #[test]
    fn gate_locks_on_real_encoder_keyframe() {
        // A genuine libx264 MPEG-TS (committed). ffprobe ground truth: video PID 0x100,
        // PMT PID 0x1000, keyframes at byte offsets 564 and 9400, PAT/PMT recurring throughout.
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/vectors/media/h264-keyframes.ts"
        );
        let data = std::fs::read(path).expect("fixture present");
        const FIX_VIDEO_PID: u16 = 0x0100;
        const FIX_PMT_PID: u16 = 0x1000;
        const KEYFRAME2: usize = 9400;
        // Join the stream mid-GOP: a non-keyframe video packet between the two keyframes.
        const JOIN: usize = 4136;

        let mut g = KeyframeGate::new();
        let out = g.push(&data[JOIN..]);
        assert!(is_aligned(&out) && !out.is_empty());

        let pkts: Vec<&[u8]> = out.chunks_exact(TS_PACKET_LEN).collect();
        assert_eq!(pid_of(pkts[0]), 0, "tables prepended: PAT first");
        assert_eq!(pid_of(pkts[1]), FIX_PMT_PID, "then PMT");
        let first_video = pkts
            .iter()
            .find(|p| pid_of(p) == FIX_VIDEO_PID)
            .expect("video emitted");
        // The first picture the player sees is the real keyframe at byte 9400 — not the
        // mid-GOP packet we joined on.
        assert_eq!(*first_video, &data[KEYFRAME2..KEYFRAME2 + TS_PACKET_LEN]);
    }

    #[test]
    fn aligned_stream_has_sync_offset_zero() {
        let mut s = packet(0);
        s.extend(packet(1));
        assert_eq!(find_sync_offset(&s), Some(0));
        assert!(is_aligned(&s));
        assert_eq!(packet_count(&s), 2);
    }

    #[test]
    fn finds_offset_when_stream_is_shifted() {
        let mut s = vec![0xAA, 0xBB, 0xCC]; // 3 junk bytes before the first packet
        s.extend(packet(0));
        s.extend(packet(1));
        assert_eq!(find_sync_offset(&s), Some(3));
        assert!(!is_aligned(&s)); // length isn't a packet multiple from offset 0
    }

    #[test]
    fn rejects_non_ts_garbage() {
        let junk = vec![0u8; 600];
        assert_eq!(find_sync_offset(&junk), None);
        assert!(!is_aligned(&junk));
    }

    #[test]
    fn is_aligned_false_on_partial_packet() {
        let mut s = packet(0);
        s.truncate(100);
        assert!(!is_aligned(&s));
    }

    #[test]
    fn resync_passes_through_already_aligned_stream() {
        let mut r = TsResync::new();
        let mut s = packet(1);
        s.extend(packet(2));
        s.extend(packet(3)); // 3 packets; lookahead holds back the last
        let out = r.push(&s);
        assert!(is_aligned(&out));
        assert_eq!(packet_count(&out), 2);
        // the held-back final packet flushes once another arrives
        let out2 = r.push(&packet(4));
        assert!(is_aligned(&out2));
    }

    #[test]
    fn resync_drops_junk_between_aligned_runs() {
        // Two aligned packets, then 96 bytes of boundary junk, then aligned packets again
        // (mimics the Acestream −per-piece drift).
        let mut r = TsResync::new();
        let mut s = packet(1);
        s.extend(packet(2));
        s.extend(vec![0x00u8; 96]); // misaligned boundary bytes
        for k in 0..4 {
            s.extend(packet(10 + k));
        }
        let out = r.push(&s);
        assert!(is_aligned(&out), "output must be packet-aligned");
        // every emitted packet starts with sync; junk was discarded
        assert!(out.chunks_exact(TS_PACKET_LEN).all(|p| p[0] == TS_SYNC));
        // we keep most packets (6 in, minus ≤1 lookahead) — junk doesn't corrupt the run
        assert!(packet_count(&out) >= 4);
    }

    #[test]
    fn resync_handles_split_pushes() {
        let mut r = TsResync::new();
        let mut s = packet(1);
        s.extend(packet(2));
        s.extend(packet(3));
        // feed in arbitrary fragments
        let mut out = r.push(&s[..50]);
        out.extend(r.push(&s[50..200]));
        out.extend(r.push(&s[200..]));
        out.extend(r.push(&packet(4))); // flush trailer
        assert!(is_aligned(&out));
        assert!(packet_count(&out) >= 3);
    }

    #[test]
    fn resync_caps_unconfirmed_tail_on_junk_flood() {
        // A hostile peer supplies complete pieces of non-TS data that never sync-lock.
        // The unconfirmed tail must stay bounded to the sync lookahead instead of retaining
        // every pushed byte (a memory-exhaustion DoS — issue #14).
        let mut r = TsResync::new();
        let junk = vec![0x00u8; 64 * 1024]; // no 0x47, so no confirmable packet ever
        for _ in 0..16 {
            let out = r.push(&junk);
            assert!(out.is_empty(), "non-TS junk must not emit packets");
        }
        assert!(
            r.buf.len() <= MAX_UNSYNCED_TAIL,
            "unconfirmed tail buffer must be capped to the sync lookahead, got {} bytes",
            r.buf.len()
        );
    }

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
    fn sanitize_service_name_rejects_all_control_chars() {
        assert_eq!(sanitize_service_name("\0\u{1}\u{2}"), None);
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

    #[test]
    fn table_prefix_appends_sdt_with_pat_program_number_when_titled() {
        // PMT program_number is 1 (see the `pat`/`pmt` helpers).
        let mut state = VideoAccessPointState::with_service_name(Some("My Channel".to_string()));
        state.observe(&pat(PMT_PID));
        state.observe(&pmt(PMT_PID, VIDEO_PID));
        let prefix = state.table_prefix().unwrap();
        // PAT + PMT + one SDT packet.
        assert_eq!(prefix.len(), TS_PACKET_LEN * 3);
        assert_eq!(
            read_sdt_service_name(&prefix).as_deref(),
            Some("My Channel")
        );
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

    #[test]
    fn resync_recovers_alignment_after_capped_junk_flood() {
        // Capping the tail must not break re-locking: after a junk flood, a real aligned
        // run that follows still sync-locks and is emitted.
        let mut r = TsResync::new();
        let junk = vec![0x13u8; 64 * 1024]; // arbitrary non-sync filler
        for _ in 0..8 {
            let _ = r.push(&junk);
        }
        let mut good = packet(1);
        for k in 2..6 {
            good.extend(packet(k));
        }
        let out = r.push(&good);
        assert!(
            is_aligned(&out),
            "must re-lock on the aligned run after junk"
        );
        assert!(packet_count(&out) >= 3, "should emit the recovered packets");
    }
}
