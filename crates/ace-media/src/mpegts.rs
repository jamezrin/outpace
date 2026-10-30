//! MPEG-TS packet basics (pure).
//!
//! An MPEG-TS stream is a sequence of fixed 188-byte packets, each starting with the
//! sync byte `0x47`. Before re-exposing or segmenting we must confirm the byte stream is
//! packet-aligned (and find the alignment offset if it isn't).

use std::time::Duration;

/// MPEG-TS sync byte at the start of every packet.
pub const TS_SYNC: u8 = 0x47;
/// MPEG-TS packet length in bytes.
pub const TS_PACKET_LEN: usize = 188;

const PCR_MODULUS: u64 = 1 << 33;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsTiming {
    pub pid: u16,
    pub pcr: Option<u64>,
    pub discontinuity: bool,
}

/// Parse the PID, PCR base, and discontinuity indicator from a complete TS packet.
pub fn ts_timing(packet: &[u8; TS_PACKET_LEN]) -> TsTiming {
    if packet[0] != TS_SYNC || packet[3] & 0x20 == 0 {
        return TsTiming {
            pid: 0,
            pcr: None,
            discontinuity: false,
        };
    }
    let pid = ts_pid(packet);
    let adaptation_len = packet[4] as usize;
    if adaptation_len == 0 || adaptation_len > 183 {
        return TsTiming {
            pid,
            pcr: None,
            discontinuity: false,
        };
    }
    let flags = packet[5];
    let pcr = if flags & 0x10 != 0 && adaptation_len >= 7 {
        Some(
            ((packet[6] as u64) << 25)
                | ((packet[7] as u64) << 17)
                | ((packet[8] as u64) << 9)
                | ((packet[9] as u64) << 1)
                | ((packet[10] as u64) >> 7),
        )
    } else {
        None
    };
    TsTiming {
        pid,
        pcr,
        discontinuity: flags & 0x80 != 0,
    }
}

fn pcr_delta(start: u64, end: u64) -> Option<u64> {
    let delta = (end + PCR_MODULUS - start) % PCR_MODULUS;
    (delta <= PCR_MODULUS / 2).then_some(delta)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockObservation {
    Ignored,
    Advanced(Duration),
    Reset,
}

#[derive(Debug, Default)]
pub struct SelectedPcrClock {
    pid: Option<u16>,
    start: Option<u64>,
    last: Option<u64>,
}

impl SelectedPcrClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn elapsed(&self) -> Option<Duration> {
        Some(Duration::from_secs_f64(
            pcr_delta(self.start?, self.last?)? as f64 / 90_000.0,
        ))
    }

    pub fn observe(&mut self, timing: TsTiming) -> ClockObservation {
        if timing.discontinuity {
            self.reset();
            return ClockObservation::Reset;
        }
        let Some(pcr) = timing.pcr else {
            return ClockObservation::Ignored;
        };
        match self.pid {
            Some(pid) if pid != timing.pid => return ClockObservation::Ignored,
            None => self.pid = Some(timing.pid),
            _ => {}
        }
        if self.last.is_some_and(|last| pcr_delta(last, pcr).is_none()) {
            self.reset();
            return ClockObservation::Reset;
        }
        self.start.get_or_insert(pcr);
        self.last = Some(pcr);
        ClockObservation::Advanced(self.elapsed().unwrap_or_default())
    }
}

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
/// byte `0x47` recurs every 188 bytes for up to `PROBE_PACKETS` packets. Returns `None`
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
    buf.as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .all(|p| p[0] == TS_SYNC)
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
/// and buffers the unconfirmed tail (bounded to `MAX_UNSYNCED_TAIL`) until more data arrives.
#[derive(Default)]
pub struct TsResync {
    buf: Vec<u8>,
    locked: bool,
    acquired_sync: bool,
}

/// Loss immediately before `output_offset` in one push's aligned output. An offset at the
/// end arms recovery for a later push; offsets are ordered and packet aligned.
#[derive(Debug, PartialEq, Eq)]
pub struct TsResyncBoundary {
    pub output_offset: usize,
    pub discarded_bytes: usize,
}

pub struct TsResyncOutput {
    /// Selected original ranges in the previous retained tail followed by this input.
    pub selected: Vec<std::ops::Range<usize>>,
    /// Prefix removed from that combined input, including discarded bytes.
    pub consumed: usize,
    pub bytes: Vec<u8>,
    pub discarded_bytes: usize,
    pub boundaries: Vec<TsResyncBoundary>,
}

impl TsResync {
    pub fn new() -> Self {
        TsResync {
            buf: Vec::new(),
            locked: false,
            acquired_sync: false,
        }
    }

    /// Append `data`; return all newly emittable sync-locked packets (a whole number of
    /// 188-byte packets, each starting with `0x47`). Initial/reacquired sync requires a pair
    /// of packet starts. Once locked, retain the complete packet at an expected sync start
    /// even when its successor is misaligned. One packet of lookahead stays buffered.
    pub fn push(&mut self, data: &[u8]) -> Vec<u8> {
        self.push_report(data).bytes
    }

    /// Like [`Self::push`], while reporting bytes discarded after sync was first acquired.
    /// Callers can treat such loss as a media discontinuity instead of silently joining
    /// transport packets across an unrecoverable boundary.
    pub fn push_report(&mut self, data: &[u8]) -> TsResyncOutput {
        self.buf.extend_from_slice(data);
        let n = self.buf.len();
        let mut out = Vec::new();
        let mut selected: Vec<std::ops::Range<usize>> = Vec::new();
        let mut discarded_bytes = 0;
        let mut boundaries: Vec<TsResyncBoundary> = Vec::new();
        let mut report_loss = |offset, count| {
            discarded_bytes += count;
            if let Some(last) = boundaries.last_mut().filter(|b| b.output_offset == offset) {
                last.discarded_bytes += count;
            } else {
                boundaries.push(TsResyncBoundary {
                    output_offset: offset,
                    discarded_bytes: count,
                });
            }
        };
        let mut i = 0;
        while i + 2 * TS_PACKET_LEN <= n {
            if self.buf[i] == TS_SYNC && (self.locked || self.buf[i + TS_PACKET_LEN] == TS_SYNC) {
                if let Some(last) = selected.last_mut().filter(|last| last.end == i) {
                    last.end += TS_PACKET_LEN;
                } else {
                    selected.push(i..i + TS_PACKET_LEN);
                }
                out.extend_from_slice(&self.buf[i..i + TS_PACKET_LEN]);
                self.locked = true;
                self.acquired_sync = true;
                i += TS_PACKET_LEN;
            } else {
                // Lost lock: scan forward to the next confirmable packet start.
                self.locked = false;
                let mut j = i + 1;
                while j + TS_PACKET_LEN < n
                    && !(self.buf[j] == TS_SYNC && self.buf[j + TS_PACKET_LEN] == TS_SYNC)
                {
                    j += 1;
                }
                if j + TS_PACKET_LEN < n {
                    if self.acquired_sync {
                        report_loss(out.len(), j - i);
                    }
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
        let mut consumed = i;
        if self.buf.len() > MAX_UNSYNCED_TAIL {
            let excess = self.buf.len() - MAX_UNSYNCED_TAIL;
            if self.acquired_sync {
                report_loss(out.len(), excess);
            }
            self.locked = false;
            self.buf.drain(0..excess);
            consumed += excess;
        }
        TsResyncOutput {
            selected,
            consumed,
            bytes: out,
            discarded_bytes,
            boundaries,
        }
    }
}

/// Tracks the MPEG-TS tables and video PID needed to identify a candidate access point.
#[derive(Default)]
pub struct VideoAccessPointState {
    allow_mpeg2: bool,
    preserve_transport_identity: bool,
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

    /// Enable packet-local MPEG-2 picture access detection. Legacy/raw gates retain their
    /// previous H.264/HEVC-only policy unless their owner explicitly opts in.
    pub fn enable_mpeg2(&mut self) {
        self.allow_mpeg2 = true;
    }

    /// Match synthesized service metadata to the current validated HLS transport.
    /// Legacy/raw consumers retain their historical SDT identity by default.
    pub fn preserve_transport_identity(&mut self) {
        self.preserve_transport_identity = true;
    }

    pub fn has_service_name(&self) -> bool {
        self.service_name.is_some()
    }

    pub fn reset(&mut self) {
        let name = self.service_name.take();
        let allow_mpeg2 = self.allow_mpeg2;
        let preserve_transport_identity = self.preserve_transport_identity;
        *self = Self {
            service_name: name,
            allow_mpeg2,
            preserve_transport_identity,
            ..Self::default()
        };
    }

    pub fn observe(&mut self, packet: &[u8]) -> bool {
        if packet.len() != TS_PACKET_LEN || packet[0] != 0x47 {
            return false;
        }
        let pid = ts_pid(packet);
        if pid == 0 {
            if let Some(mapping) = parse_pat_first_program(packet) {
                self.cached_pat = Some(packet.to_vec());
                let program = mapping.map(|(program, _)| program);
                let pmt_pid = mapping.map(|(_, pid)| pid);
                if self.pmt_pid != pmt_pid || self.program_number != program {
                    self.cached_pmt = None;
                    self.video_pid = None;
                    self.video_codec = None;
                }
                self.program_number = program;
                self.pmt_pid = pmt_pid;
            }
        } else if Some(pid) == self.pmt_pid {
            if let Some(video) = parse_pmt_video(packet, self.program_number, self.allow_mpeg2) {
                self.cached_pmt = Some(packet.to_vec());
                self.video_pid = video.map(|(pid, _)| pid);
                self.video_codec = video.map(|(_, codec)| codec);
            }
        }
        Some(pid) == self.video_pid && is_random_access_point(packet, self.video_codec)
    }

    pub fn table_prefix(&self) -> Option<Vec<u8>> {
        let mut prefix = Vec::with_capacity(TS_PACKET_LEN * 3);
        prefix.extend_from_slice(self.cached_pat.as_ref()?);
        prefix.extend_from_slice(self.cached_pmt.as_ref()?);
        if let (Some(name), Some(program)) = (self.service_name.as_deref(), self.program_number) {
            let sdt = if self.preserve_transport_identity {
                let pat = self.cached_pat.as_ref()?;
                let start = psi_section_start(pat)?;
                let identity = pat.get(start + 3..start + 5)?;
                build_sdt_with_transport(
                    program,
                    name,
                    u16::from_be_bytes([identity[0], identity[1]]),
                )
            } else {
                build_sdt(program, name)
            };
            prefix.extend_from_slice(&sdt);
        }
        Some(prefix)
    }
}

/// Per-client gate that drops a stream's opening packets until a candidate video access point.
/// RAI and packet-local NAL detection are heuristics: without parsing the complete access unit
/// and its dependencies, they do not guarantee an independently decodable picture.
///
/// Feed it sync-locked, 188-aligned TS packets (e.g. [`TsResync`] output). Until it locks it
/// emits nothing; on the first random-access point it emits the most-recent PAT and PMT
/// followed by the keyframe packet, then passes everything through. A titled gate repeats its
/// synthesized table prefix at later video access points and filters upstream SDT; an untitled
/// gate remains byte-for-byte passthrough after lock. If no keyframe is found within
/// `max_scan_packets`, it gives up gating and passes through (better imperfect video than a
/// starved client).
pub struct KeyframeGate {
    buf: Vec<u8>,
    locked: bool,
    access: VideoAccessPointState,
    scanned: usize,
    max_scan_packets: usize,
    filter_sdt: bool,
    // Belongs to this client's emitted PID, not the table cache or recovery run.
    next_sdt_counter: u8,
    mark_discontinuity: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateResume {
    /// A video PUSI with RAI or a packet-local IDR/IRAP NAL; not a decodability guarantee.
    AccessPoint,
    /// Explicit bounded passthrough fallback, without an observed access point.
    ScanBudget,
}

pub struct KeyframeGateOutput {
    /// Original packet ranges forwarded by this call; generated tables/markers excluded.
    pub forwarded: Vec<std::ops::Range<usize>>,
    pub bytes: Vec<u8>,
    /// Input bytes actually withheld, excluding the resumed packet and inserted tables/marker.
    pub withheld_bytes: usize,
    pub resumed: Option<GateResume>,
}

/// Default packet budget before the gate gives up looking for a keyframe and passes through.
/// ~60k packets ≈ 11 MB ≈ a few seconds of HD — far longer than any sane GOP.
const DEFAULT_MAX_SCAN_PACKETS: usize = 60_000;

impl KeyframeGate {
    pub fn new() -> Self {
        Self::with_max_scan_packets(DEFAULT_MAX_SCAN_PACKETS)
    }

    /// Observe current tables while passing initial media through unchanged. A later
    /// same-stream transport loss can rearm without waiting for repeated PSI.
    pub fn new_passthrough() -> Self {
        Self {
            locked: true,
            ..Self::new()
        }
    }

    pub fn is_locked(&self) -> bool {
        self.locked
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
            filter_sdt: false,
            next_sdt_counter: 0,
            mark_discontinuity: false,
        }
    }

    /// Like [`new`](Self::new) but injects a synthesized SDT `service_name` at keyframe lock and
    /// later video access points. Drops upstream SDT (PID 0x0011) packets from the post-lock
    /// passthrough, so the injected title is the only SDT the client sees.
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
            next_sdt_counter: 0,
            mark_discontinuity: false,
        }
    }

    /// Re-arm the gate (drop its buffer and re-require a keyframe) while preserving its
    /// configured title, so a mid-stream reset keeps injecting/filtering the SDT.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.locked = false;
        self.scanned = 0;
        self.access.reset();
        self.mark_discontinuity = false;
    }

    /// Re-arm after known media loss and mark the first resumed video access point so MPEG-TS
    /// decoders reset continuity/timestamp state at the same boundary.
    pub fn reset_for_discontinuity(&mut self) {
        self.reset();
        self.mark_discontinuity = true;
    }

    /// Re-arm after byte loss within the same stream, retaining only complete learned tables
    /// and video metadata. Partial input and the scan budget belong to the lost region.
    /// For an unrelated stream or unknown table changes during subscriber lag, use the fresh
    /// [`Self::reset_for_discontinuity`] instead.
    pub fn rearm_for_discontinuity(&mut self) {
        self.buf.clear();
        self.locked = false;
        self.scanned = 0;
        self.mark_discontinuity = true;
    }

    /// Append `data` (assumed 188-aligned TS) and return the bytes to forward to the client.
    pub fn push(&mut self, data: &[u8]) -> Vec<u8> {
        self.push_report(data).bytes
    }

    pub fn push_report(&mut self, data: &[u8]) -> KeyframeGateOutput {
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        let mut forwarded: Vec<std::ops::Range<usize>> = Vec::new();
        let mut withheld_bytes = 0;
        let mut resumed = None;
        let mut i = 0;
        while i + TS_PACKET_LEN <= self.buf.len() {
            let pkt = &self.buf[i..i + TS_PACKET_LEN];
            if self.filter_sdt && ts_pid(pkt) == SDT_PID {
                // Filter in both phases: even the scan-budget trigger must not
                // publish upstream SDT into this gate's owned continuity sequence.
                // Count it toward scanning, but resume only on a forwardable packet
                // so the recovery marker is attached to media rather than discarded.
                if !self.locked {
                    self.scanned = self.scanned.saturating_add(1);
                }
                i += TS_PACKET_LEN;
                continue;
            }
            if self.locked {
                let access_point = self.access.observe(pkt);
                if self.filter_sdt && access_point {
                    append_gate_tables(&self.access, &mut self.next_sdt_counter, &mut out);
                }
                if let Some(last) = forwarded.last_mut().filter(|last| last.end == i) {
                    last.end += TS_PACKET_LEN;
                } else {
                    forwarded.push(i..i + TS_PACKET_LEN);
                }
                out.extend_from_slice(pkt);
                i += TS_PACKET_LEN;
                continue;
            }
            self.scanned += 1;
            if self.access.observe(pkt) {
                // Lock on the keyframe: hand the player the tables, then the keyframe.
                append_gate_tables(&self.access, &mut self.next_sdt_counter, &mut out);
                append_resumed_packet(&mut self.mark_discontinuity, None, pkt, &mut out);
                self.locked = true;
                resumed = Some(GateResume::AccessPoint);
            } else if self.scanned >= self.max_scan_packets {
                // Safety fallback: never found a keyframe; passthrough from here.
                // The discontinuity indicator is PID-specific. Prefer the video PID learned
                // from the PMT; if tables never arrived, marking the triggering PID is an
                // explicit best effort because no decoder-visible video PID is known.
                append_resumed_packet(
                    &mut self.mark_discontinuity,
                    self.access.video_pid,
                    pkt,
                    &mut out,
                );
                self.locked = true;
                resumed = Some(GateResume::ScanBudget);
            } else {
                withheld_bytes += TS_PACKET_LEN;
            }
            if self.locked {
                if let Some(last) = forwarded.last_mut().filter(|last| last.end == i) {
                    last.end += TS_PACKET_LEN;
                } else {
                    forwarded.push(i..i + TS_PACKET_LEN);
                }
            }
            // otherwise: drop this prefix packet and keep scanning.
            i += TS_PACKET_LEN;
        }
        self.buf.drain(0..i);
        KeyframeGateOutput {
            forwarded,
            bytes: out,
            withheld_bytes,
            resumed,
        }
    }
}

/// Only emitted synthesized SDTs consume the gate's continuity sequence. Cached
/// PAT/PMT packets retain their upstream headers, and filtering consumes no counter.
fn append_gate_tables(
    access: &VideoAccessPointState,
    next_sdt_counter: &mut u8,
    out: &mut Vec<u8>,
) {
    let Some(mut prefix) = access.table_prefix() else {
        return;
    };
    for packet in prefix.as_chunks_mut::<TS_PACKET_LEN>().0 {
        if ts_pid(packet) == SDT_PID {
            packet[3] = (packet[3] & 0xf0) | *next_sdt_counter;
            *next_sdt_counter = (*next_sdt_counter + 1) & 0x0f;
        }
    }
    out.extend_from_slice(&prefix);
}

fn append_resumed_packet(
    mark_discontinuity: &mut bool,
    marker_pid: Option<u16>,
    packet: &[u8],
    out: &mut Vec<u8>,
) {
    if !std::mem::take(mark_discontinuity) {
        out.extend_from_slice(packet);
        return;
    }

    let marker_pid = marker_pid.unwrap_or_else(|| ts_pid(packet));
    if marker_pid != ts_pid(packet) {
        out.extend_from_slice(&discontinuity_packet(marker_pid, packet[3] & 0x0f));
        out.extend_from_slice(packet);
        return;
    }

    let mut resumed = packet.to_vec();
    if !set_discontinuity_indicator(&mut resumed) {
        out.extend_from_slice(&discontinuity_packet(ts_pid(packet), packet[3] & 0x0f));
    }
    out.extend_from_slice(&resumed);
}

fn set_discontinuity_indicator(packet: &mut [u8]) -> bool {
    let afc = (packet[3] >> 4) & 0x03;
    if matches!(afc, 0b10 | 0b11) && packet[4] >= 1 {
        packet[5] |= 0x80;
        true
    } else {
        false
    }
}

fn discontinuity_packet(pid: u16, continuity_counter: u8) -> [u8; TS_PACKET_LEN] {
    let mut packet = [0xff; TS_PACKET_LEN];
    packet[0] = TS_SYNC;
    packet[1] = ((pid >> 8) as u8) & 0x1f;
    packet[2] = pid as u8;
    packet[3] = 0x20 | continuity_counter;
    packet[4] = 183;
    packet[5] = 0x80;
    packet
}

/// PID (13-bit) of a TS packet.
pub fn ts_pid(pkt: &[u8]) -> u16 {
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

/// Candidate access point: video PES start (PUSI) with random_access_indicator or a
/// packet-local H.264 IDR / HEVC IRAP NAL or MPEG-2 I-picture header. These heuristics
/// do not parse a complete picture or prove that its decoding dependencies are available.
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

/// Scan packet-local PES elementary bytes for an Annex-B start code `00 00 01` followed
/// by a keyframe NAL for the stream's `codec`: H.264 IDR (5), or HEVC IRAP (16..=21).
/// Parameter sets alone do not contain a picture and must not open the gate.
/// The scan is codec-gated because the two codecs read the NAL-type bits differently (H.264 uses
/// `byte & 0x1F`, HEVC uses `(byte >> 1) & 0x3F`), so an H.264 slice type would otherwise be
/// interpreted under the wrong codec. Without a resolved codec, returns false and
/// leaves keyframe detection to the codec-agnostic random_access_indicator.
/// MPEG-2 instead requires its first complete picture header to be an I-picture.
fn payload_has_irap(pkt: &[u8], codec: Option<VideoCodec>) -> bool {
    let Some(codec) = codec else {
        return false;
    };
    let Some(off) = ts_payload_offset(pkt) else {
        return false;
    };
    let Some(elementary) = video_pes_elementary_payload(&pkt[off..]) else {
        return false;
    };
    if codec == VideoCodec::Mpeg2 {
        return mpeg2_has_i_picture(elementary);
    }
    elementary.windows(4).any(|w| {
        if w[0] != 0 || w[1] != 0 || w[2] != 1 {
            return false;
        }
        match codec {
            VideoCodec::Mpeg2 => false,
            VideoCodec::H264 => matches!(w[3] & 0x1F, 5),
            VideoCodec::Hevc => matches!((w[3] >> 1) & 0x3F, 16..=21),
        }
    })
}

/// Borrow elementary bytes after a complete packet-local video PES header, bounded by
/// the available packet payload and, when nonzero, the declared PES end.
/// Split PES headers deliberately remain false negatives: reporting access on their
/// continuation would discard the earlier PES-start packet needed by the decoder.
fn video_pes_elementary_payload(payload: &[u8]) -> Option<&[u8]> {
    if payload.len() < 9
        || payload[..3] != [0, 0, 1]
        || !(0xe0..=0xef).contains(&payload[3])
        || payload[6] & 0xc0 != 0x80
    {
        return None;
    }
    let declared_length = u16::from_be_bytes([payload[4], payload[5]]) as usize;
    let end = if declared_length == 0 {
        payload.len()
    } else {
        (6 + declared_length).min(payload.len())
    };
    let start = 9 + payload[8] as usize;
    if start > end || !mpeg2_pes_optional_header_valid(payload[7], &payload[9..start]) {
        return None;
    }
    Some(&payload[start..end])
}

/// Inspect the first complete MPEG-2 picture header in elementary bytes. Split picture
/// headers remain false negatives. An I-picture indicates intra coding, not complete
/// decoding independence.
fn mpeg2_has_i_picture(elementary: &[u8]) -> bool {
    for (offset, code) in elementary.windows(4).enumerate() {
        if code == [0, 0, 1, 0] {
            // The first picture alone decides: a later I cannot make an earlier P/B
            // safe to include when cutting at the start of this PES packet.
            return elementary
                .get(offset + 4..offset + 8)
                .is_some_and(|header| (header[1] >> 3) & 7 == 1);
        }
    }
    false
}

/// Validate only the PES optional-header envelope needed before trusting picture bytes.
/// This is packet-local syntax validation, not timestamp interpretation or PES reassembly.
fn mpeg2_pes_optional_header_valid(flags: u8, mut data: &[u8]) -> bool {
    fn take<'a>(data: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
        let field = data.get(..len)?;
        *data = &data[len..];
        Some(field)
    }
    fn timestamp(field: &[u8], prefix: u8) -> bool {
        field[0] >> 4 == prefix && field[0] & 1 == 1 && field[2] & 1 == 1 && field[4] & 1 == 1
    }
    let valid = (|| -> Option<()> {
        match flags >> 6 {
            0 => {}
            1 => return None, // forbidden PTS_DTS_flags
            2 => {
                if !timestamp(take(&mut data, 5)?, 2) {
                    return None;
                }
            }
            3 => {
                if !timestamp(take(&mut data, 5)?, 3) || !timestamp(take(&mut data, 5)?, 1) {
                    return None;
                }
            }
            _ => unreachable!(),
        }
        if flags & 0x20 != 0 {
            let escr = take(&mut data, 6)?;
            if escr[0] & 4 == 0 || escr[2] & 4 == 0 || escr[4] & 4 == 0 || escr[5] & 1 == 0 {
                return None;
            }
        }
        if flags & 0x10 != 0 {
            let rate = take(&mut data, 3)?;
            if rate[0] & 0x80 == 0 || rate[2] & 1 == 0 {
                return None;
            }
        }
        if flags & 8 != 0 {
            let trick = take(&mut data, 1)?[0];
            if trick >> 5 > 4 || (trick >> 5 == 2 && trick & 7 != 7) {
                return None;
            }
        }
        if flags & 4 != 0 && take(&mut data, 1)?[0] & 0x80 == 0 {
            return None;
        }
        if flags & 2 != 0 {
            take(&mut data, 2)?; // previous_PES_packet_CRC: presence only
        }
        if flags & 1 != 0 {
            let extension = take(&mut data, 1)?[0];
            if extension & 0x0e != 0x0e {
                return None;
            }
            if extension & 0x80 != 0 {
                take(&mut data, 16)?; // PES_private_data
            }
            if extension & 0x40 != 0 {
                let len = take(&mut data, 1)?[0] as usize;
                take(&mut data, len)?; // opaque pack header, bounded by its own length
            }
            if extension & 0x20 != 0 {
                let sequence = take(&mut data, 2)?;
                if sequence[0] & 0x80 == 0 || sequence[1] & 0x80 == 0 {
                    return None;
                }
            }
            if extension & 0x10 != 0 && take(&mut data, 2)?[0] & 0xc0 != 0x40 {
                return None;
            }
            if extension & 1 != 0 {
                let length = take(&mut data, 1)?[0];
                if length & 0x80 == 0 {
                    return None;
                }
                let mut extra = take(&mut data, (length & 0x7f) as usize)?;
                let selector = take(&mut extra, 1)?[0];
                if selector & 0x80 != 0 {
                    if selector & 0x7e != 0x7e {
                        return None;
                    }
                    if selector & 1 == 0 && !timestamp(take(&mut extra, 5)?, 0xf) {
                        return None;
                    }
                }
                if !extra.iter().all(|&byte| byte == 0xff) {
                    return None;
                }
            }
        }
        data.iter().all(|&byte| byte == 0xff).then_some(())
    })();
    valid.is_some()
}

/// Only complete, current single-packet tables may replace the cached metadata. Multi-packet
/// PSI remains unsupported here; a fragment must not become a synthesized table prefix.
fn current_psi_section(pkt: &[u8], table_id: u8, minimum_length: usize) -> Option<(usize, usize)> {
    if !ts_pusi(pkt) {
        return None;
    }
    let sec = psi_section_start(pkt)?;
    if *pkt.get(sec)? != table_id || *pkt.get(sec + 5)? & 1 == 0 {
        return None;
    }
    let length = ((pkt.get(sec + 1)? & 0x0f) as usize) << 8 | *pkt.get(sec + 2)? as usize;
    let end = sec.checked_add(3)?.checked_add(length)?;
    (length >= minimum_length && end <= pkt.len()).then_some((sec, end - 4))
}

/// First real program of a current PAT, or no mapping when the table removes all programs.
fn parse_pat_first_program(pkt: &[u8]) -> Option<Option<(u16, u16)>> {
    let (sec, end) = current_psi_section(pkt, 0x00, 9)?;
    if (end - (sec + 8)) % 4 != 0 {
        return None;
    }
    let mut pos = sec + 8;
    while pos + 4 <= end {
        let program = ((pkt[pos] as u16) << 8) | pkt[pos + 1] as u16;
        let pid = (((pkt[pos + 2] & 0x1F) as u16) << 8) | pkt[pos + 3] as u16;
        if program != 0 {
            return Some(Some((program, pid)));
        }
        pos += 4;
    }
    Some(None)
}

/// Elementary-stream video codec, as identified from the PMT `stream_type`. Determines which
/// Annex-B NAL layout [`payload_has_irap`] scans for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum VideoCodec {
    Mpeg2,
    H264,
    Hevc,
}

/// Parse a PMT packet, returning the elementary PID and codec of the first recognized video
/// stream: H.264 (`stream_type 0x1B`) or HEVC (`0x24`).
fn parse_pmt_video(
    pkt: &[u8],
    program: Option<u16>,
    allow_mpeg2: bool,
) -> Option<Option<(u16, VideoCodec)>> {
    let (sec, end) = current_psi_section(pkt, 0x02, 13)?;
    let table_program = u16::from_be_bytes([pkt[sec + 3], pkt[sec + 4]]);
    if program != Some(table_program) {
        return None;
    }
    let program_info_length =
        ((pkt.get(sec + 10)? & 0x0F) as usize) << 8 | *pkt.get(sec + 11)? as usize;
    let mut pos = sec + 12 + program_info_length;
    let mut video = None;
    while pos + 5 <= end {
        let stream_type = pkt[pos];
        let epid = (((pkt[pos + 1] & 0x1F) as u16) << 8) | pkt[pos + 2] as u16;
        let es_info_length = ((pkt[pos + 3] & 0x0F) as usize) << 8 | pkt[pos + 4] as usize;
        if video.is_none() {
            video = match stream_type {
                0x02 if allow_mpeg2 => Some((epid, VideoCodec::Mpeg2)),
                0x1B => Some((epid, VideoCodec::H264)),
                0x24 => Some((epid, VideoCodec::Hevc)),
                _ => None,
            };
        }
        pos += 5 + es_info_length;
    }
    (pos == end).then_some(video)
}

/// Start offset of the PSI section within a table packet (skips payload offset + pointer_field).
fn psi_section_start(pkt: &[u8]) -> Option<usize> {
    let po = ts_payload_offset(pkt)?;
    let pointer = *pkt.get(po)? as usize;
    let sec = po + 1 + pointer;
    (sec < TS_PACKET_LEN).then_some(sec)
}

/// CRC-32/MPEG-2 (poly 0x04C11DB7, MSB-first, init 0xFFFFFFFF, no final XOR) — the CRC used by
/// MPEG-2 PSI/SI sections. Canonical impl for the workspace; `ace-engine`'s RTMP→TS PSI
/// synthesis reuses it rather than keeping its own copy.
pub fn mpeg_crc32(data: &[u8]) -> u32 {
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
/// [`sanitize_service_name`] so the section fits one packet. Starts at continuity counter
/// zero; an owner repeating the packet must assign its output continuity sequence.
fn build_sdt(service_id: u16, service_name: &str) -> [u8; TS_PACKET_LEN] {
    build_sdt_with_transport(service_id, service_name, 1)
}

fn build_sdt_with_transport(
    service_id: u16,
    service_name: &str,
    transport_stream_id: u16,
) -> [u8; TS_PACKET_LEN] {
    let name = service_name.as_bytes();

    // Service descriptor (0x48): service_type + provider + service_name.
    let mut desc = vec![
        SERVICE_DESCRIPTOR_TAG,
        (3 + SERVICE_PROVIDER_NAME.len() + name.len()) as u8, // descriptor_length
        0x01,                                                 // service_type = digital television
        SERVICE_PROVIDER_NAME.len() as u8,
    ];
    desc.extend_from_slice(SERVICE_PROVIDER_NAME);
    desc.push(name.len() as u8);
    desc.extend_from_slice(name);

    // SDT section (table_id through CRC).
    let mut sec = Vec::new();
    sec.push(SDT_TABLE_ID);
    let section_length = 8 + 5 + desc.len() + 4; // tsid..reserved, loop header, descriptors, CRC
    sec.push(0xF0 | ((section_length >> 8) as u8 & 0x0F)); // syntax=1 + reserved bits + len hi
    sec.push((section_length & 0xFF) as u8);
    sec.extend_from_slice(&transport_stream_id.to_be_bytes());
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
    for pkt in ts.as_chunks::<TS_PACKET_LEN>().0.iter() {
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

    fn timing(pid: u16, pcr: Option<u64>) -> TsTiming {
        TsTiming {
            pid,
            pcr,
            discontinuity: false,
        }
    }

    #[test]
    fn selected_pcr_clock_ignores_other_pids_and_handles_wrap() {
        let mut clock = SelectedPcrClock::new();
        assert_eq!(
            clock.observe(timing(0x100, Some(PCR_MODULUS - 45_000))),
            ClockObservation::Advanced(Duration::ZERO)
        );
        assert_eq!(
            clock.observe(timing(0x101, Some(90_000))),
            ClockObservation::Ignored
        );
        assert_eq!(
            clock.observe(timing(0x100, Some(45_000))),
            ClockObservation::Advanced(Duration::from_secs(1))
        );
    }

    #[test]
    fn selected_pcr_clock_resets_on_backward_jump_or_discontinuity() {
        let mut clock = SelectedPcrClock::new();
        clock.observe(timing(0x100, Some(180_000)));
        assert_eq!(
            clock.observe(timing(0x100, Some(90_000))),
            ClockObservation::Reset
        );
        clock.observe(timing(0x100, Some(180_000)));
        assert_eq!(
            clock.observe(TsTiming {
                discontinuity: true,
                ..timing(0x100, Some(270_000))
            }),
            ClockObservation::Reset
        );
    }

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

    /// Return a PSI packet with the MPEG-2 CRC filled in, for tests that exercise table parsing.
    fn with_valid_psi_crc(mut packet: Vec<u8>) -> Vec<u8> {
        let section_start = 5; // TS header + pointer_field
        let section_len = 3
            + (((packet[section_start + 1] as usize & 0x0F) << 8)
                | packet[section_start + 2] as usize);
        let crc_at = section_start + section_len - 4;
        let crc = mpeg_crc32(&packet[section_start..crc_at]);
        packet[crc_at..crc_at + 4].copy_from_slice(&crc.to_be_bytes());
        packet
    }

    fn valid_pat(pmt_pid: u16) -> Vec<u8> {
        with_valid_psi_crc(pat(pmt_pid))
    }

    fn valid_pmt_codec(pmt_pid: u16, video_pid: u16, stream_type: u8) -> Vec<u8> {
        with_valid_psi_crc(pmt_codec(pmt_pid, video_pid, stream_type))
    }

    fn pes_nal(nal: &[u8]) -> Vec<u8> {
        let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        pes.extend_from_slice(&[0x00, 0x00, 0x01]);
        pes.extend_from_slice(nal);
        pes
    }

    // Synthetic MPEG-2 video PES carrying one picture header. `picture_type` is
    // encoded after the 10-bit temporal_reference, as specified by MPEG-2 video.
    fn mpeg2_picture_pes(picture_type: u8, temporal_reference: u16) -> Vec<u8> {
        let mut pes = vec![0, 0, 1, 0xE0, 0, 0, 0x80, 0, 0];
        pes.extend_from_slice(&[
            0,
            0,
            1,
            0x00,
            ((temporal_reference >> 2) & 0xff) as u8,
            (((temporal_reference & 3) << 6) as u8) | ((picture_type & 7) << 3),
            0x55,
            0x66,
        ]);
        pes
    }

    fn mpeg2_packet(payload: &[u8]) -> Vec<u8> {
        ts(VIDEO_PID, true, false, payload)
    }

    fn mpeg2_packet_exact(payload: &[u8], pusi: bool, pid: u16) -> Vec<u8> {
        assert!(payload.len() <= TS_PACKET_LEN - 5);
        let mut packet = vec![0xff; TS_PACKET_LEN];
        packet[0] = TS_SYNC;
        packet[1] = ((pid >> 8) as u8 & 0x1f) | if pusi { 0x40 } else { 0 };
        packet[2] = pid as u8;
        packet[3] = 0x30;
        packet[4] = (TS_PACKET_LEN - 5 - payload.len()) as u8;
        if packet[4] > 0 {
            packet[5] = 0;
        }
        let start = 5 + packet[4] as usize;
        packet[start..start + payload.len()].copy_from_slice(payload);
        packet
    }

    fn gate_with_valid_video(stream_type: u8) -> KeyframeGate {
        let mut gate = KeyframeGate::new();
        gate.push(&valid_pat(PMT_PID));
        gate.push(&valid_pmt_codec(PMT_PID, VIDEO_PID, stream_type));
        gate
    }

    fn pes_with_private_data(fake_nal: &[u8], elementary: &[u8]) -> Vec<u8> {
        let mut pes = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0x01, 17, 0x8e];
        let mut private_data = [0x55; 16];
        private_data[4..4 + fake_nal.len()].copy_from_slice(fake_nal);
        pes.extend_from_slice(&private_data);
        pes.extend_from_slice(elementary);
        pes
    }

    fn pes_with_pts(elementary: &[u8]) -> Vec<u8> {
        let mut pes = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0x80, 5];
        pes.extend_from_slice(&[0x21, 0, 1, 0, 1]);
        pes.extend_from_slice(elementary);
        pes
    }

    /// True iff `pkt` is on `pid` (single-packet helper for assertions).
    fn pid_of(pkt: &[u8]) -> u16 {
        (((pkt[1] & 0x1F) as u16) << 8) | pkt[2] as u16
    }

    const PMT_PID: u16 = 0x0100;
    const VIDEO_PID: u16 = 0x0101;
    const AUDIO_PID: u16 = 0x0102;

    #[test]
    fn pes_access_private_data_fake_h264_idr_does_not_open_access_or_client_gate() {
        let mut state = VideoAccessPointState::new();
        state.observe(&valid_pat(PMT_PID));
        state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x1b));
        let mut gate = gate_with_valid_video(0x1b);
        let false_start = mpeg2_packet(&pes_with_private_data(&[0, 0, 1, 0x65], &[0, 0, 1, 0x41]));

        let access_point = state.observe(&false_start);
        let withheld = gate.push_report(&false_start);
        assert!(
            !access_point
                && !gate.is_locked()
                && withheld.resumed.is_none()
                && withheld.bytes.is_empty()
                && withheld.withheld_bytes == TS_PACKET_LEN,
            "fake PES-header IDR opened access={access_point}, locked={}, resumed={:?}",
            gate.is_locked(),
            withheld.resumed
        );

        let idr = mpeg2_packet(&pes_nal(&[0x65]));
        assert!(state.observe(&idr));
        let resumed = gate.push_report(&idr);
        assert!(gate.is_locked());
        assert_eq!(resumed.resumed, Some(GateResume::AccessPoint));
        assert!(resumed
            .bytes
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .any(|p| p[2] == VIDEO_PID as u8));
    }

    #[test]
    fn pes_access_private_data_fake_hevc_irap_does_not_open_access_or_client_gate() {
        let mut state = VideoAccessPointState::new();
        state.observe(&valid_pat(PMT_PID));
        state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x24));
        let mut gate = gate_with_valid_video(0x24);
        let false_start = mpeg2_packet(&pes_with_private_data(
            &[0, 0, 1, 0x26, 0x01],
            &[0, 0, 1, 0x02, 0x01],
        ));

        let access_point = state.observe(&false_start);
        let withheld = gate.push_report(&false_start);
        assert!(
            !access_point
                && !gate.is_locked()
                && withheld.resumed.is_none()
                && withheld.bytes.is_empty()
                && withheld.withheld_bytes == TS_PACKET_LEN,
            "fake PES-header IRAP opened access={access_point}, locked={}, resumed={:?}",
            gate.is_locked(),
            withheld.resumed
        );

        let irap = mpeg2_packet(&pes_nal(&[0x26, 0x01]));
        assert!(state.observe(&irap));
        let resumed = gate.push_report(&irap);
        assert!(gate.is_locked());
        assert_eq!(resumed.resumed, Some(GateResume::AccessPoint));
    }

    #[test]
    fn pes_access_declared_length_excludes_trailing_fake_h264_idr() {
        let mut pes = vec![0, 0, 1, 0xe0, 0, 7, 0x80, 0, 0];
        pes.extend_from_slice(&[0, 0, 1, 0x41]);
        pes.extend_from_slice(&[0, 0, 1, 0x65]);
        let packet = mpeg2_packet(&pes);
        let mut state = VideoAccessPointState::new();
        state.observe(&valid_pat(PMT_PID));
        state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x1b));
        let mut gate = gate_with_valid_video(0x1b);

        let access_point = state.observe(&packet);
        let withheld = gate.push_report(&packet);
        assert!(
            !access_point
                && !gate.is_locked()
                && withheld.resumed.is_none()
                && withheld.bytes.is_empty()
                && withheld.withheld_bytes == TS_PACKET_LEN,
            "trailing fake IDR opened access={access_point}, locked={}, resumed={:?}",
            gate.is_locked(),
            withheld.resumed
        );
    }

    #[test]
    fn pes_access_real_h264_keys_with_no_optional_header_and_pts() {
        for keyframe in [pes_nal(&[0x65]), pes_with_pts(&[0, 0, 1, 0x65])] {
            let mut state = VideoAccessPointState::new();
            state.observe(&valid_pat(PMT_PID));
            state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x1b));
            assert!(state.observe(&mpeg2_packet(&keyframe)));

            let mut gate = gate_with_valid_video(0x1b);
            let resumed = gate.push_report(&mpeg2_packet(&keyframe));
            assert!(gate.is_locked());
            assert_eq!(resumed.resumed, Some(GateResume::AccessPoint));
        }
    }

    #[test]
    fn pes_access_real_hevc_irap_with_no_optional_header_and_pts() {
        for keyframe in [pes_nal(&[0x26, 0x01]), pes_with_pts(&[0, 0, 1, 0x26, 0x01])] {
            let mut state = VideoAccessPointState::new();
            state.observe(&valid_pat(PMT_PID));
            state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x24));
            assert!(state.observe(&mpeg2_packet(&keyframe)));

            let mut gate = gate_with_valid_video(0x24);
            let resumed = gate.push_report(&mpeg2_packet(&keyframe));
            assert!(gate.is_locked());
            assert_eq!(resumed.resumed, Some(GateResume::AccessPoint));
        }
    }

    #[test]
    fn pes_access_malformed_optional_header_stays_closed() {
        let mut malformed = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0x81, 22, 0x11, 0, 1, 0, 1, 0x8e];
        let mut private_data = [0x55; 16];
        private_data[4..8].copy_from_slice(&[0, 0, 1, 0x65]);
        malformed.extend_from_slice(&private_data);
        malformed.extend_from_slice(&[0, 0, 1, 0x41]);

        let mut state = VideoAccessPointState::new();
        state.observe(&valid_pat(PMT_PID));
        state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x1b));
        let mut gate = gate_with_valid_video(0x1b);
        let packet = mpeg2_packet(&malformed);

        let access_point = state.observe(&packet);
        let withheld = gate.push_report(&packet);
        assert!(
            !access_point
                && !gate.is_locked()
                && withheld.resumed.is_none()
                && withheld.bytes.is_empty(),
            "malformed optional PES opened access={access_point}, locked={}, resumed={:?}",
            gate.is_locked(),
            withheld.resumed
        );
    }

    #[test]
    fn pes_access_split_fixed_and_optional_headers_reject_continuations_then_recover() {
        for (stream_type, fake_nal, nonkey, key_nal) in [
            (
                0x1b,
                &[0, 0, 1, 0x65][..],
                &[0, 0, 1, 0x41][..],
                &[0x65][..],
            ),
            (
                0x24,
                &[0, 0, 1, 0x26, 0x01][..],
                &[0, 0, 1, 0x02, 0x01][..],
                &[0x26, 0x01][..],
            ),
        ] {
            let complete = pes_with_private_data(fake_nal, nonkey);
            for split_at in [8, 20] {
                let mut state = VideoAccessPointState::new();
                state.observe(&valid_pat(PMT_PID));
                state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, stream_type));
                let mut gate = gate_with_valid_video(stream_type);
                let start = mpeg2_packet_exact(&complete[..split_at], true, VIDEO_PID);
                let continuation = mpeg2_packet_exact(&complete[split_at..], false, VIDEO_PID);

                for packet in [&start, &continuation] {
                    let access_point = state.observe(packet);
                    let withheld = gate.push_report(packet);
                    assert!(
                        !access_point
                            && !gate.is_locked()
                            && withheld.resumed.is_none()
                            && withheld.bytes.is_empty(),
                        "split PES at byte {split_at} opened access={access_point}, locked={}, resumed={:?}",
                        gate.is_locked(),
                        withheld.resumed
                    );
                }

                let real_key = mpeg2_packet(&pes_nal(key_nal));
                assert!(
                    state.observe(&real_key),
                    "complete elementary key must recover"
                );
                let resumed = gate.push_report(&real_key);
                assert!(gate.is_locked());
                assert_eq!(resumed.resumed, Some(GateResume::AccessPoint));
            }
        }
    }

    #[test]
    fn pes_access_random_access_indicator_remains_independent_of_pes_validation() {
        let malformed_pes = [0, 0, 1, 0xe0, 0, 0, 0, 0, 1, 0, 0, 1, 0x65];
        let packet = ts(VIDEO_PID, true, true, &malformed_pes);
        let mut state = VideoAccessPointState::new();
        state.observe(&valid_pat(PMT_PID));
        state.observe(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x1b));
        let mut gate = gate_with_valid_video(0x1b);

        assert!(state.observe(&packet));
        let resumed = gate.push_report(&packet);
        assert!(gate.is_locked());
        assert_eq!(resumed.resumed, Some(GateResume::AccessPoint));
    }

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
    fn video_access_point_state_mpeg2_random_access_is_opt_in_and_survives_reset() {
        let pat = pat(PMT_PID);
        let mpeg2_pmt = pmt_codec(PMT_PID, VIDEO_PID, 0x02);

        // The default/raw policy remains H.264/HEVC-only; a no-RAI MPEG-2 I is refused.
        let mut default_state = VideoAccessPointState::new();
        default_state.observe(&pat);
        default_state.observe(&mpeg2_pmt);
        assert!(!default_state.observe(&random_access_packet(VIDEO_PID)));
        assert!(!default_state.observe(&mpeg2_packet(&mpeg2_picture_pes(1, 0x2ab))));

        // Strict normalization is opt-in and enables MPEG-2 picture access detection;
        // a discontinuity reset clears tables but must retain that policy bit.
        let mut enabled_state = VideoAccessPointState::new();
        enabled_state.enable_mpeg2();
        enabled_state.observe(&pat);
        enabled_state.observe(&mpeg2_pmt);
        assert!(enabled_state.observe(&mpeg2_packet(&mpeg2_picture_pes(1, 0x2ab))));
        assert!(enabled_state.observe(&random_access_packet(VIDEO_PID)));
        enabled_state.reset();
        assert!(!enabled_state.observe(&mpeg2_packet(&mpeg2_picture_pes(1, 0x2ab))));
        enabled_state.observe(&pat);
        enabled_state.observe(&mpeg2_pmt);
        assert!(enabled_state.observe(&mpeg2_packet(&mpeg2_picture_pes(1, 0x2ab))));
    }

    #[test]
    fn mpeg2_picture_access_requires_the_first_complete_i_picture_header() {
        for (picture_type, expected) in [
            (1, true),
            (2, false),
            (3, false),
            (0, false),
            (4, false),
            (7, false),
        ] {
            let mut state = VideoAccessPointState::new();
            state.enable_mpeg2();
            state.observe(&pat(PMT_PID));
            state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
            assert_eq!(
                state.observe(&mpeg2_packet(&mpeg2_picture_pes(picture_type, 0x2ab))),
                expected,
                "picture coding type {picture_type}, including nonzero temporal_reference"
            );
        }

        for prefix in [
            vec![0, 0, 1, 0xB3],
            vec![0, 0, 1, 0xB8],
            vec![0, 0, 1, 0xB5],
            [vec![0, 0, 1, 0xB3], mpeg2_picture_pes(2, 4)[9..].to_vec()].concat(),
            [vec![0, 0, 1, 0xB3], mpeg2_picture_pes(3, 4)[9..].to_vec()].concat(),
            [
                mpeg2_picture_pes(2, 4)[9..].to_vec(),
                mpeg2_picture_pes(1, 4)[9..].to_vec(),
            ]
            .concat(),
        ] {
            let mut pes = vec![0, 0, 1, 0xE0, 0, 0, 0x80, 0, 0];
            pes.extend(prefix);
            let mut state = VideoAccessPointState::new();
            state.enable_mpeg2();
            state.observe(&pat(PMT_PID));
            state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
            assert!(!state.observe(&mpeg2_packet(&pes)));
        }

        // Each short length of the picture code/base header is genuinely payload-
        // truncated; adaptation stuffing must not provide extra scan bytes.
        let picture = mpeg2_picture_pes(1, 7);
        for len in 0..8 {
            let packet = mpeg2_packet_exact(&picture[..9 + len], true, VIDEO_PID);
            let mut state = VideoAccessPointState::new();
            state.enable_mpeg2();
            state.observe(&pat(PMT_PID));
            state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
            assert!(!state.observe(&packet), "truncated picture bytes: {len}");
        }
    }

    #[test]
    fn mpeg2_pes_header_and_declared_length_bound_picture_search() {
        // Missing prefix, wrong stream id, bad MPEG-2 marker, truncated fixed header,
        // and optional-header length larger than bytes declared by PES_packet_length.
        let mut malformed = vec![
            vec![0, 0, 2, 0xE0, 0, 0, 0x80, 0, 0],
            vec![0, 0, 1, 0xC0, 0, 0, 0x80, 0, 0],
            vec![0, 0, 1, 0xE0, 0, 0, 0, 0, 0],
            vec![0, 0, 1, 0xE0, 0, 0, 0x80],
        ];
        let mut optional = vec![0, 0, 1, 0xE0, 0, 3, 0x80, 0, 8];
        optional.extend(mpeg2_picture_pes(1, 1)[9..].iter().copied());
        malformed.push(optional);

        for pes in malformed {
            let mut state = VideoAccessPointState::new();
            state.enable_mpeg2();
            state.observe(&pat(PMT_PID));
            state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
            assert!(!state.observe(&mpeg2_packet(&pes)));
        }

        // PES_packet_length ends before a fake I picture in TS stuffing/trailing bytes.
        let mut pes = mpeg2_picture_pes(2, 9);
        pes[4..6].copy_from_slice(&[0, 4]); // 3 optional bytes + one elementary byte
        pes.extend_from_slice(&mpeg2_picture_pes(1, 9)[9..]);
        let mut state = VideoAccessPointState::new();
        state.enable_mpeg2();
        state.observe(&pat(PMT_PID));
        state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
        assert!(!state.observe(&mpeg2_packet(&pes)));

        // A complete fake I picture in valid private_data is skipped; the real
        // elementary body starts with a P picture and therefore cannot qualify.
        let mut fake_private = mpeg2_picture_pes(1, 5)[9..].to_vec();
        fake_private.resize(16, 0x55);
        let mut pes = vec![0, 0, 1, 0xE0, 0, 0, 0x80, 1, 17, 0x8e];
        pes.extend(fake_private);
        pes.extend_from_slice(&mpeg2_picture_pes(2, 6)[9..]);
        assert!(!state.observe(&mpeg2_packet(&pes)));

        // A valid PTS-bearing MPEG-2 PES skips its five optional-header bytes.
        let mut pts_pes = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0x80, 5];
        pts_pes.extend_from_slice(&[0x21, 0, 1, 0, 1]);
        pts_pes.extend_from_slice(&mpeg2_picture_pes(1, 1)[9..]);
        assert!(state.observe(&mpeg2_packet(&pts_pes)));

        // PTS and DTS both present, with their respective timestamp prefixes.
        let mut pts_dts_pes = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0xc0, 10];
        pts_dts_pes.extend_from_slice(&[0x31, 0, 1, 0, 1, 0x11, 0, 1, 0, 1]);
        pts_dts_pes.extend_from_slice(&mpeg2_picture_pes(1, 2)[9..]);
        assert!(state.observe(&mpeg2_packet(&pts_dts_pes)));

        // Flag-selected optional fields must fit the declared optional-data length,
        // and timestamp prefixes / marker bits must be valid before scanning body.
        let mut bad_optional = Vec::new();
        for (flags, data) in [
            (0x80, vec![]),                                   // PTS absent
            (0x40, vec![0x11, 0, 1, 0, 1]),                   // forbidden PTS_DTS_flags = 01
            (0x80, vec![0x11, 0, 1, 0, 1]),                   // wrong PTS prefix
            (0x80, vec![0x21, 0, 0, 0, 1]),                   // bad PTS marker
            (0xc0, vec![0x31, 0, 1, 0, 1]),                   // DTS missing
            (0xc0, vec![0x21, 0, 1, 0, 1, 0x11, 0, 1, 0, 1]), // wrong PTS prefix
            (0xc0, vec![0x31, 0, 1, 0, 1, 0x21, 0, 1, 0, 1]), // wrong DTS prefix
            (0xc0, vec![0x31, 0, 1, 0, 1, 0x11, 0, 0, 0, 1]), // bad DTS marker
            (0x20, vec![0; 6]),                               // ESCR markers invalid
            (0x10, vec![0; 3]),                               // ES rate markers invalid
            (0x08, vec![0xe0]),                               // reserved trick-mode control
            (0x04, vec![0; 1]),                               // additional copy info marker invalid
            (0x02, vec![0; 1]),                               // CRC truncated
            (0x01, vec![0; 1]),                               // extension missing selector
        ] {
            let mut pes = vec![0, 0, 1, 0xe0, 0, 0, 0x80, flags, data.len() as u8];
            pes.extend(data);
            pes.extend_from_slice(&mpeg2_picture_pes(1, 3)[9..]);
            bad_optional.push(pes);
        }
        // The declared optional header ends after valid stuffing; an I picture
        // immediately afterward is elementary body and remains eligible.
        let mut bounded = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0, 2, 0xff, 0xff];
        bounded.extend_from_slice(&mpeg2_picture_pes(1, 4)[9..]);
        assert!(state.observe(&mpeg2_packet(&bounded)));
        for pes in bad_optional {
            assert!(
                !state.observe(&mpeg2_packet(&pes)),
                "malformed optional PES: {pes:02x?}"
            );
        }

        // An unbounded PES body may continue beyond this TS packet; scan only the
        // available packet payload while still validating its complete PES header.
        let mut long_body = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0x80, 5];
        long_body.extend_from_slice(&[0x21, 0, 1, 0, 1]);
        long_body.extend_from_slice(&mpeg2_picture_pes(1, 6)[9..]);
        assert!(state.observe(&mpeg2_packet(&long_body)));

        // A plausible picture code in adaptation bytes is outside the TS payload.
        let pes = mpeg2_picture_pes(1, 1);
        let mut packet = mpeg2_packet_exact(&pes[..9], true, VIDEO_PID);
        packet[6..15].copy_from_slice(&[0, 0, 1, 0, 0, 0, 8, 0x55, 0x66]);
        assert!(!state.observe(&packet));
    }

    #[test]
    fn mpeg2_optional_extension_fields_validate_and_skip_to_body() {
        fn pes(extension_data: &[u8]) -> Vec<u8> {
            let mut pes = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 1, extension_data.len() as u8];
            pes.extend_from_slice(extension_data);
            pes.extend_from_slice(&mpeg2_picture_pes(1, 0x155)[9..]);
            pes
        }
        let mut state = VideoAccessPointState::new();
        state.enable_mpeg2();
        state.observe(&pat(PMT_PID));
        state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));

        let valid = [
            vec![0x8e].into_iter().chain(vec![0x55; 16]).collect(), // private_data
            vec![0x4e, 2, 0x11, 0x22],                              // pack header and its length
            vec![0x2e, 0x80, 0x80],                                 // sequence counter markers
            vec![0x1e, 0x40, 0x00],                                 // PSTD buffer scale/size prefix
            vec![0x0f, 0x81, 0x00], // extension2, length marker and selector
            vec![0x0f, 0x86, 0xfe, 0xf1, 0, 1, 0, 1], // TREF selector and timestamp
        ];
        for data in valid {
            assert!(
                state.observe(&mpeg2_packet(&pes(&data))),
                "valid extension: {data:02x?}"
            );
        }

        let malformed = [
            vec![0x8e].into_iter().chain(vec![0x55; 15]).collect(), // private_data truncated
            vec![0x4e, 3, 0x11, 0x22], // pack length exceeds optional data
            vec![0x2e, 0x00, 0x80],    // first sequence marker bit cleared
            vec![0x2e, 0x80, 0x00],    // second sequence marker bit cleared
            vec![0x1e, 0x00, 0x00],    // PSTD '01' marker corrupted
            vec![0x0f, 0x01, 0x00],    // extension2 length marker absent
            vec![0x0f, 0x86, 0xfc, 0xf1, 0, 1, 0, 1], // TREF selector bits corrupted
            vec![0x0f, 0x86, 0xfe, 0xf0, 0, 1, 0, 1], // TREF timestamp marker corrupted
        ];
        for data in malformed {
            assert!(
                !state.observe(&mpeg2_packet(&pes(&data))),
                "malformed extension: {data:02x?}"
            );
        }
    }

    #[test]
    fn mpeg2_access_respects_pid_pusi_codec_and_pmt_replacement() {
        let pes = mpeg2_picture_pes(1, 3);
        let mut state = VideoAccessPointState::new();
        state.enable_mpeg2();
        state.observe(&pat(PMT_PID));
        state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
        assert!(!state.observe(&mpeg2_packet_exact(&pes, true, AUDIO_PID)));
        assert!(!state.observe(&mpeg2_packet_exact(&pes, false, VIDEO_PID)));
        assert!(!state.observe(&mpeg2_packet_exact(&pes, true, VIDEO_PID + 1)));

        state.observe(&pmt_codec(PMT_PID, AUDIO_PID, 0x02));
        assert!(!state.observe(&mpeg2_packet(&pes)));
        state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x1b));
        assert!(!state.observe(&mpeg2_packet(&pes)));

        // Removing the selected MPEG-2 elementary stream invalidates the old PID.
        let mut removed = pmt_codec(PMT_PID, VIDEO_PID, 0x02);
        // Shrink section_length to PAT-like fixed PMT fields plus CRC, no ES entries.
        removed[7] = 13;
        state.observe(&removed);
        assert!(!state.observe(&mpeg2_packet(&pes)));
    }

    #[test]
    fn mpeg2_packet_local_split_headers_reject_then_recover() {
        let mut state = VideoAccessPointState::new();
        state.enable_mpeg2();
        state.observe(&pat(PMT_PID));
        state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
        let pes = mpeg2_picture_pes(1, 2);
        for split in [1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 14, 15] {
            assert!(!state.observe(&mpeg2_packet_exact(&pes[..split], true, VIDEO_PID))); // bounded by this TS packet, never reassembled
            assert!(!state.observe(&mpeg2_packet_exact(&pes[split..], false, VIDEO_PID)));
        }
        state.reset();
        assert!(!state.observe(&mpeg2_packet(&pes)));
        state.observe(&pat(PMT_PID));
        state.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x02));
        assert!(state.observe(&mpeg2_packet(&pes)));
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
        let pkts: Vec<&[u8]> = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .collect();
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
        let pes = pes_nal(&[0x65]);
        let out = g.push(&ts(VIDEO_PID, true, false, &pes));
        assert_eq!(packet_count(&out), 3, "IDR NAL should lock the gate");
        assert_eq!(
            pid_of(
                out.as_chunks::<TS_PACKET_LEN>()
                    .0
                    .iter()
                    .map(|packet| packet.as_slice())
                    .nth(2)
                    .unwrap()
            ),
            VIDEO_PID
        );
    }

    #[test]
    fn gate_does_not_lock_on_a_non_keyframe_pes_start() {
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID));
        // PES start (PUSI) but only a non-IDR slice NAL (type 1) and no RAI.
        let pes = pes_nal(&[0x61]);
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

        let mut mpeg2_looking = VideoAccessPointState::new();
        mpeg2_looking.enable_mpeg2();
        mpeg2_looking.observe(&pat(PMT_PID));
        mpeg2_looking.observe(&pmt_codec(PMT_PID, VIDEO_PID, 0x24));
        assert!(
            !mpeg2_looking.observe(&mpeg2_packet(&mpeg2_picture_pes(1, 0x2ab))),
            "MPEG-2-looking payload must not be accepted under HEVC"
        );
    }

    #[test]
    fn gate_detects_hevc_irap_nal_when_rai_is_not_set() {
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt_codec(PMT_PID, VIDEO_PID, 0x24));
        // HEVC video. Keyframe flagged only by an IDR_W_RADL NAL (type 19), no RAI bit.
        // HEVC NAL header: byte0 = type << 1 = 19 << 1 = 0x26, byte1 = 0x01 (layer 0, tid 1).
        let pes = pes_nal(&[0x26, 0x01]);
        let out = g.push(&ts(VIDEO_PID, true, false, &pes));
        assert_eq!(packet_count(&out), 3, "HEVC IRAP NAL should lock the gate");
    }

    #[test]
    fn gate_h264_sps_only_pusi_does_not_open_but_idr_does() {
        let mut gate = gate_with_valid_video(0x1B);
        let sps = gate.push(&ts(
            VIDEO_PID,
            true,
            false,
            &pes_nal(&[0x67, 0x42, 0x00, 0x1E]),
        ));
        assert!(sps.is_empty(), "SPS alone must not open the gate");

        let wrong_pid = gate.push(&ts(AUDIO_PID, true, false, &pes_nal(&[0x65, 0x88])));
        assert!(
            wrong_pid.is_empty(),
            "an IDR on the wrong PID must not open the gate"
        );
        let no_pusi = gate.push(&ts(VIDEO_PID, false, false, &pes_nal(&[0x65, 0x88])));
        assert!(
            no_pusi.is_empty(),
            "an IDR without PUSI must not open the gate"
        );

        let idr = gate.push(&ts(VIDEO_PID, true, false, &pes_nal(&[0x65, 0x88])));
        assert_eq!(packet_count(&idr), 3, "an actual IDR opens the gate");
        assert_eq!(pid_of(&idr[2 * TS_PACKET_LEN..]), VIDEO_PID);
    }

    #[test]
    fn gate_hevc_parameter_sets_do_not_open_but_irap_does() {
        for nal in [&[0x40, 0x01][..], &[0x42, 0x01][..], &[0x44, 0x01][..]] {
            let mut gate = gate_with_valid_video(0x24);
            let held = gate.push(&ts(VIDEO_PID, true, false, &pes_nal(nal)));
            assert!(held.is_empty(), "HEVC VPS/SPS/PPS alone must not open");
            let wrong_pid = gate.push(&ts(AUDIO_PID, true, false, &pes_nal(&[0x26, 0x01, 0x80])));
            assert!(
                wrong_pid.is_empty(),
                "an IRAP on the wrong PID must not open the gate"
            );
            let no_pusi = gate.push(&ts(VIDEO_PID, false, false, &pes_nal(&[0x26, 0x01, 0x80])));
            assert!(
                no_pusi.is_empty(),
                "an IRAP without PUSI must not open the gate"
            );
            let irap = gate.push(&ts(VIDEO_PID, true, false, &pes_nal(&[0x26, 0x01, 0x80])));
            assert_eq!(packet_count(&irap), 3, "HEVC IRAP opens the gate");
        }
    }

    #[test]
    fn gate_rai_with_parameter_sets_still_opens() {
        let mut gate = gate_with_valid_video(0x1B);
        let out = gate.push(&ts(
            VIDEO_PID,
            true,
            true,
            &pes_nal(&[0x67, 0x42, 0x00, 0x1E]),
        ));
        assert_eq!(packet_count(&out), 3, "RAI remains an opening signal");
    }

    #[test]
    fn gate_parameter_only_scan_exhaustion_reports_scan_budget() {
        let mut gate = KeyframeGate::with_max_scan_packets(4);
        gate.push(&valid_pat(PMT_PID));
        gate.push(&valid_pmt_codec(PMT_PID, VIDEO_PID, 0x1B));
        let sps = gate.push_report(&ts(
            VIDEO_PID,
            true,
            false,
            &pes_nal(&[0x67, 0x42, 0x00, 0x1E]),
        ));
        assert!(sps.bytes.is_empty(), "SPS alone must stay held");
        assert_eq!(
            sps.resumed, None,
            "PAT, PMT, and SPS do not exhaust budget 4"
        );

        let ordinary = gate.push_report(&ts(VIDEO_PID, false, false, &[0x11]));
        assert_eq!(ordinary.resumed, Some(GateResume::ScanBudget));
        assert_eq!(
            packet_count(&ordinary.bytes),
            1,
            "only the ordinary packet resumes output"
        );
    }

    #[test]
    fn gate_does_not_treat_h264_pslice_as_hevc_irap() {
        // NAL byte 0x21 is an H.264 P-slice (type 1), but under HEVC's
        // (byte >> 1) & 0x3F it reads as 16 (IRAP). An H.264 stream must use H.264's type bits.
        let mut g = KeyframeGate::new();
        g.push(&pat(PMT_PID));
        g.push(&pmt(PMT_PID, VIDEO_PID)); // H.264
        let pes = pes_nal(&[0x21, 0x01]);
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
    fn discontinuity_reset_marks_access_point_with_existing_adaptation_field() {
        let mut gate = KeyframeGate::new();
        gate.reset_for_discontinuity();

        let out = gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        let first_video = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .find(|packet| pid_of(packet) == VIDEO_PID)
            .expect("resumed video packet");

        assert!(ts_timing(first_video.try_into().unwrap()).discontinuity);
        assert_eq!(packet_count(&out), 3, "no marker packet is needed");
    }

    #[test]
    fn discontinuity_reset_inserts_adaptation_only_marker_when_needed() {
        let mut gate = KeyframeGate::new();
        gate.reset_for_discontinuity();
        let idr = pes_nal(&[0x65]);

        let out = gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                ts(VIDEO_PID, true, false, &idr),
            ]
            .concat(),
        );
        let video: Vec<_> = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .filter(|packet| pid_of(packet) == VIDEO_PID)
            .collect();

        assert_eq!(
            video.len(),
            2,
            "marker must precede payload-only access point"
        );
        assert!(ts_timing(video[0].try_into().unwrap()).discontinuity);
        assert_eq!((video[0][3] >> 4) & 0x03, 0b10);
        assert_eq!(video[1], ts(VIDEO_PID, true, false, &idr));
    }

    #[test]
    fn discontinuity_reset_marks_scan_budget_fallback() {
        let mut gate = KeyframeGate::with_max_scan_packets(1);
        gate.reset_for_discontinuity();
        let resumed = ts(VIDEO_PID, false, false, &[0xAA; 16]);

        let out = gate.push(&resumed);
        let packets: Vec<_> = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .collect();

        assert_eq!(packets.len(), 2, "fallback needs a separate marker packet");
        assert!(ts_timing(packets[0].try_into().unwrap()).discontinuity);
        assert_eq!(packets[1], resumed);
    }

    #[test]
    fn discontinuity_fallback_marks_known_video_pid_when_audio_exhausts_budget() {
        let mut gate = KeyframeGate::with_max_scan_packets(3);
        gate.reset_for_discontinuity();
        let audio = ts(AUDIO_PID, false, false, &[0xCC; 16]);

        let out = gate.push(&[pat(PMT_PID), pmt(PMT_PID, VIDEO_PID), audio.clone()].concat());
        let packets: Vec<_> = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .collect();

        assert_eq!(packets.len(), 2);
        assert_eq!(pid_of(packets[0]), VIDEO_PID);
        assert!(ts_timing(packets[0].try_into().unwrap()).discontinuity);
        assert_eq!(packets[1], audio);
    }

    #[test]
    fn ordinary_reset_clears_pending_discontinuity_marker() {
        let mut gate = KeyframeGate::new();
        gate.reset_for_discontinuity();
        gate.reset();

        let out = gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        let first_video = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .find(|packet| pid_of(packet) == VIDEO_PID)
            .expect("resumed video packet");

        assert!(!ts_timing(first_video.try_into().unwrap()).discontinuity);
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
        let pkts: Vec<&[u8]> = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .collect();
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
        let pkts: Vec<&[u8]> = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .collect();
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

        let pkts: Vec<&[u8]> = out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .collect();
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
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|p| read_sdt_service_name(p).as_deref() == Some("Upstream")));
    }

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
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .filter(|packet| read_sdt_service_name(packet).as_deref() == Some("Titled"))
            .count();
        assert_eq!(titled_sdt_count, 2);
        assert_eq!(
            out.as_chunks::<TS_PACKET_LEN>()
                .0
                .iter()
                .map(|packet| packet.as_slice())
                .filter(|packet| *packet == ordinary.as_slice())
                .count(),
            1
        );
    }

    fn sdt_counters(bytes: &[u8]) -> Vec<u8> {
        bytes
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .filter(|packet| ts_pid(packet) == SDT_PID)
            .map(|packet| packet[3] & 0x0f)
            .collect()
    }

    #[test]
    fn injected_sdt_counters_advance_and_wrap_without_changing_sections() {
        let mut gate = KeyframeGate::with_service_name(Some("Titled".into()));
        let opening = [pat(PMT_PID), pmt(PMT_PID, VIDEO_PID)].concat();
        assert!(gate.push(&opening).is_empty());
        let mut output = Vec::new();
        for _ in 0..20 {
            output.extend(gate.push(&random_access_packet(VIDEO_PID)));
        }
        assert_eq!(
            sdt_counters(&output),
            (0..20).map(|n| n % 16).collect::<Vec<_>>()
        );
        let expected = build_sdt(1, "Titled");
        for packet in output
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .filter(|p| ts_pid(p) == SDT_PID)
        {
            assert_eq!(&packet[..3], &expected[..3]);
            assert_eq!(packet[3] & 0xf0, expected[3] & 0xf0);
            assert_eq!(
                &packet[4..],
                &expected[4..],
                "SDT section and CRC must stay unchanged"
            );
        }
    }

    #[test]
    fn injected_sdt_counter_survives_all_gate_resets() {
        for reset in [
            KeyframeGate::reset,
            KeyframeGate::reset_for_discontinuity,
            KeyframeGate::rearm_for_discontinuity,
        ] {
            let mut gate = KeyframeGate::with_service_name(Some("Titled".into()));
            let opening = [
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat();
            assert_eq!(sdt_counters(&gate.push(&opening)), vec![0]);
            reset(&mut gate);
            assert_eq!(sdt_counters(&gate.push(&opening)), vec![1]);
        }
    }

    #[test]
    fn injected_sdt_counter_excludes_upstream_sdt_at_scan_budget_fallback() {
        let mut gate = KeyframeGate::with_service_name(Some("Titled".into()));
        let opening = [
            pat(PMT_PID),
            pmt(PMT_PID, VIDEO_PID),
            random_access_packet(VIDEO_PID),
        ]
        .concat();
        assert_eq!(sdt_counters(&gate.push(&opening)), vec![0]);
        gate.rearm_for_discontinuity();
        gate.max_scan_packets = 1;
        let mut upstream = upstream_sdt();
        upstream[3] = (upstream[3] & 0xf0) | 9;
        let ordinary = ts(VIDEO_PID, false, false, &[0x11, 0x22]);
        let fallback = gate.push_report(&[upstream, ordinary].concat());
        assert_eq!(fallback.resumed, Some(GateResume::ScanBudget));
        assert!(
            sdt_counters(&fallback.bytes).is_empty(),
            "fallback must not leak upstream SDT"
        );
        assert!(fallback
            .bytes
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|p| ts_pid(p) == VIDEO_PID && ts_timing(p.try_into().unwrap()).discontinuity));
        assert_eq!(
            sdt_counters(&gate.push(&random_access_packet(VIDEO_PID))),
            vec![1]
        );
    }

    #[test]
    fn injected_sdt_counter_ignores_filtered_sdt_and_preserves_cached_psi() {
        let mut gate = KeyframeGate::with_service_name(Some("Titled".into()));
        let mut pat_first = pat(PMT_PID);
        let mut pmt_first = pmt(PMT_PID, VIDEO_PID);
        pat_first[3] = (pat_first[3] & 0xf0) | 7;
        pmt_first[3] = (pmt_first[3] & 0xf0) | 11;
        let mut pat_next = pat_first.clone();
        let mut pmt_next = pmt_first.clone();
        pat_next[3] = (pat_next[3] & 0xf0) | 8;
        pmt_next[3] = (pmt_next[3] & 0xf0) | 12;
        let mut output = gate.push(
            &[
                pat_first.clone(),
                pmt_first.clone(),
                upstream_sdt(),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        output.extend(
            gate.push(
                &[
                    upstream_sdt(),
                    upstream_sdt(),
                    pat_next.clone(),
                    pmt_next.clone(),
                    random_access_packet(VIDEO_PID),
                ]
                .concat(),
            ),
        );
        assert_eq!(sdt_counters(&output), vec![0, 1]);
        let psi: Vec<_> = output
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .filter(|p| ts_pid(p) == 0 || ts_pid(p) == PMT_PID)
            .map(<[u8]>::to_vec)
            .collect();
        assert_eq!(
            psi,
            vec![
                pat_first,
                pmt_first,
                pat_next.clone(),
                pmt_next.clone(),
                pat_next,
                pmt_next
            ]
        );
        assert!(output
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .filter(|p| ts_pid(p) == SDT_PID)
            .all(|p| read_sdt_service_name(p).as_deref() == Some("Titled")));
    }

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

    #[test]
    fn keyframe_gate_reset_preserves_title_and_rearms() {
        let mut gate = KeyframeGate::with_service_name(Some("Kept".to_string()));
        // Lock on a first keyframe.
        let mut first = Vec::new();
        first.extend_from_slice(&pat(PMT_PID));
        first.extend_from_slice(&pmt(PMT_PID, VIDEO_PID));
        first.extend_from_slice(&random_access_packet(VIDEO_PID));
        assert_eq!(
            read_sdt_service_name(&gate.push(&first)).as_deref(),
            Some("Kept")
        );
        // Reset re-arms (drops lock) but must keep the title: the next locked output injects
        // the SDT again.
        gate.reset();
        let mut second = Vec::new();
        second.extend_from_slice(&pat(PMT_PID));
        second.extend_from_slice(&pmt(PMT_PID, VIDEO_PID));
        second.extend_from_slice(&random_access_packet(VIDEO_PID));
        assert_eq!(
            read_sdt_service_name(&gate.push(&second)).as_deref(),
            Some("Kept")
        );
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
        assert!(out
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .all(|p| p[0] == TS_SYNC));
        // we keep most packets (6 in, minus ≤1 lookahead) — junk doesn't corrupt the run
        assert!(packet_count(&out) >= 4);
    }

    #[test]
    fn resync_keeps_last_complete_packet_before_junk() {
        let mut resync = TsResync::new();
        let input = [packet(1), packet(2), vec![0; 8], packet(3), packet(4)].concat();
        assert_eq!(
            resync.push_report(&input).bytes,
            [packet(1), packet(2), packet(3)].concat()
        );
    }

    #[test]
    fn resync_reports_every_aligned_loss_offset() {
        let mut resync = TsResync::new();
        let input = [
            packet(1),
            packet(2),
            vec![0; 8],
            packet(3),
            packet(4),
            vec![0; 12],
            packet(5),
            packet(6),
        ]
        .concat();
        let report = resync.push_report(&input);
        assert_eq!(
            report.bytes,
            [packet(1), packet(2), packet(3), packet(4), packet(5)].concat()
        );
        assert_eq!(report.discarded_bytes, 20);
        assert_eq!(
            report.boundaries,
            vec![
                TsResyncBoundary {
                    output_offset: 376,
                    discarded_bytes: 8
                },
                TsResyncBoundary {
                    output_offset: 752,
                    discarded_bytes: 12
                }
            ]
        );
    }

    #[test]
    fn resync_reports_tail_loss_at_end_and_keeps_unsynced_bound() {
        let mut resync = TsResync::new();
        let report = resync.push_report(&[packet(1), packet(2), vec![0; 1024]].concat());
        assert_eq!(report.bytes, [packet(1), packet(2)].concat());
        assert_eq!(
            report.boundaries,
            vec![TsResyncBoundary {
                output_offset: 376,
                discarded_bytes: 649
            }]
        );
        assert_eq!(resync.buf.len(), 375);
        let report = resync.push_report(&[packet(3), packet(4)].concat());
        assert_eq!(report.bytes, packet(3));
        assert_eq!(
            report.boundaries,
            vec![TsResyncBoundary {
                output_offset: 0,
                discarded_bytes: 375
            }]
        );
    }

    #[test]
    fn gate_report_counts_withheld_input_without_inserted_tables() {
        let mut gate = KeyframeGate::new_passthrough();
        assert_eq!(
            gate.push_report(&[pat(PMT_PID), pmt(PMT_PID, VIDEO_PID)].concat())
                .withheld_bytes,
            0
        );
        gate.rearm_for_discontinuity();
        let input = [
            ts(VIDEO_PID, false, false, &[0x41]),
            ts(AUDIO_PID, false, false, &[0x11]),
            random_access_packet(VIDEO_PID),
        ]
        .concat();
        let report = gate.push_report(&input);
        assert_eq!(report.withheld_bytes, 376);
        assert_eq!(report.resumed, Some(GateResume::AccessPoint));
        assert_eq!(report.bytes.len(), 564);
        let following = gate.push_report(&ts(AUDIO_PID, false, false, &[0x22]));
        assert_eq!(following.withheld_bytes, 0);
        assert_eq!(following.resumed, None);
    }

    #[test]
    fn discontinuity_gate_resumes_without_repeating_tables() {
        let mut gate = KeyframeGate::new();
        gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        gate.rearm_for_discontinuity();
        assert!(gate
            .push(&ts(VIDEO_PID, true, false, &pes_nal(&[0x41])))
            .is_empty());
        let resumed = gate.push(&random_access_packet(VIDEO_PID));
        assert_eq!(resumed.len(), 3 * TS_PACKET_LEN);
        assert_eq!(pid_of(&resumed[2 * TS_PACKET_LEN..]), VIDEO_PID);
        assert!(ts_timing(resumed[2 * TS_PACKET_LEN..].try_into().unwrap()).discontinuity);
    }

    #[test]
    fn discontinuity_gate_uses_metadata_updated_during_passthrough() {
        let mut gate = KeyframeGate::new();
        gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        let new_video = VIDEO_PID + 2;
        gate.push(&pmt_codec(PMT_PID, new_video, 0x24));
        gate.rearm_for_discontinuity();
        assert!(gate.push(&random_access_packet(VIDEO_PID)).is_empty());
        // HEVC IDR_W_RADL (NAL type19); no RAI. Cached codec is required.
        let resumed = gate.push(&ts(new_video, true, false, &pes_nal(&[0x26, 0x01])));
        assert!(!resumed.is_empty());
        assert_eq!(pid_of(&resumed[2 * TS_PACKET_LEN..]), new_video);
    }

    #[test]
    fn cached_rearm_rejects_old_program_on_unchanged_pmt_pid() {
        let mut gate = KeyframeGate::new();
        gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        let mut new_pat = pat(PMT_PID);
        new_pat[14] = 2; // program_number 2, same PMT PID
        gate.push(&new_pat);
        gate.rearm_for_discontinuity();
        assert!(gate.push(&random_access_packet(VIDEO_PID)).is_empty());
        assert!(
            gate.push(&[pmt(PMT_PID, VIDEO_PID), random_access_packet(VIDEO_PID)].concat())
                .is_empty(),
            "old-program PMT must not restore stale video"
        );
        let mut new_pmt = pmt(PMT_PID, VIDEO_PID);
        new_pmt[9] = 2;
        assert!(!gate
            .push(&[new_pmt, random_access_packet(VIDEO_PID)].concat())
            .is_empty());
    }

    #[test]
    fn cached_rearm_ignores_incomplete_or_not_current_psi() {
        let mut gate = KeyframeGate::new();
        gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        let mut incomplete = pat(PMT_PID + 2);
        incomplete[7] = 200; // section extends beyond this packet
        let mut next_table = pat(PMT_PID + 2);
        next_table[10] &= !1; // current_next_indicator 0
        gate.push(&[incomplete, next_table].concat());
        gate.rearm_for_discontinuity();
        assert!(
            !gate.push(&random_access_packet(VIDEO_PID)).is_empty(),
            "only complete current PSI replaces the cache"
        );
    }

    #[test]
    fn cached_rearm_rejects_video_removed_from_current_pmt() {
        let mut gate = KeyframeGate::new();
        gate.push(
            &[
                pat(PMT_PID),
                pmt(PMT_PID, VIDEO_PID),
                random_access_packet(VIDEO_PID),
            ]
            .concat(),
        );
        gate.push(&pmt_codec(PMT_PID, VIDEO_PID, 0x03));
        gate.rearm_for_discontinuity();
        assert!(gate.push(&random_access_packet(VIDEO_PID)).is_empty());
    }

    #[test]
    fn resync_reports_post_lock_boundary_loss_but_not_contiguous_carry() {
        let mut r = TsResync::new();
        let contiguous = [packet(1), packet(2), packet(3)].concat();
        let first = r.push_report(&contiguous[..250]);
        assert_eq!(first.discarded_bytes, 0);
        let second = r.push_report(&contiguous[250..]);
        assert_eq!(second.discarded_bytes, 0);

        let boundary = [vec![0_u8; 96], packet(4), packet(5)].concat();
        let resumed = r.push_report(&boundary);
        assert_eq!(resumed.discarded_bytes, 96);
        assert!(is_aligned(&resumed.bytes));
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

    // Supplemental parser-loop provenance proofs, independent of actor scheduling.
    mod output_parser_proof {
        use super::*;

        fn fixture() -> Vec<u8> {
            [
                pat(0x100),
                pmt(0x100, 0x101),
                upstream_sdt(),
                ts(0x101, true, false, &pes_nal(&[0x67, 1, 2])),
                ts(0x101, true, false, &pes_nal(&[0x65, 3, 4])),
                ts(0x101, false, false, &[8; 30]),
                ts(0x101, true, true, &pes_nal(&[0x65, 5])),
            ]
            .concat()
        }

        #[test]
        fn selected_ranges_reconstruct_actual_resync_output() {
            let run = fixture();
            let input = [
                vec![0x23; 19],
                run.clone(),
                vec![0x23; 801],
                run.clone(),
                vec![0x24; 397],
                run.clone(),
                vec![0x25; 401],
            ]
            .concat();
            let mut cuts = vec![0, input.len()];
            for base in (0..input.len()).step_by(188) {
                for delta in [0, 1, 3, 4, 5, 13, 187] {
                    if base + delta < input.len() {
                        cuts.push(base + delta);
                    }
                }
            }
            for split in cuts {
                let mut parser = TsResync::new();
                let mut retained = Vec::new();
                for part in [&input[..split], &input[split..], run.as_slice()] {
                    retained.extend_from_slice(part);
                    let report = parser.push_report(part);
                    let mut last = 0;
                    let mut selected = Vec::new();
                    for range in report.selected {
                        assert!(last <= range.start && range.start < range.end);
                        assert!(range.end <= report.consumed && range.end <= retained.len());
                        assert_eq!(range.len() % 188, 0);
                        selected.extend_from_slice(&retained[range.clone()]);
                        last = range.end;
                    }
                    assert_eq!(selected, report.bytes, "split={split}");
                    retained.drain(..report.consumed);
                    assert_eq!(retained, parser.buf);
                    assert!(retained.len() <= MAX_UNSYNCED_TAIL);
                }
            }
        }

        fn gated(parts: &[&[u8]]) -> Vec<u8> {
            let mut gate = KeyframeGate::with_service_name(Some("Synthetic proof".into()));
            let mut out = Vec::new();
            for part in parts {
                let mut combined = gate.buf.clone();
                combined.extend_from_slice(part);
                let report = gate.push_report(part);
                let mut last = 0;
                let mut cursor = 0;
                for range in report.forwarded {
                    assert!(last <= range.start && range.start < range.end);
                    assert!(range.end <= combined.len());
                    assert_eq!(range.start % 188, 0);
                    assert_eq!(range.len() % 188, 0);
                    for packet in combined[range.clone()].as_chunks::<188>().0 {
                        // Forwarded original packets occur unchanged and in order;
                        // generated cached tables/SDT may lie between them.
                        let at = report.bytes[cursor..]
                            .windows(188)
                            .position(|p| p == packet)
                            .expect("forwarded packet absent");
                        cursor += at + 188;
                    }
                    last = range.end;
                }
                out.extend(report.bytes);
            }
            out
        }

        #[test]
        fn titled_gate_partition_bytes_and_forwarded_ranges() {
            let input = [fixture(), fixture(), ts(0x101, false, false, &[9; 23])].concat();
            let suffix = fixture();
            let baseline = gated(&[&input, &suffix]);
            assert!(!baseline.is_empty());
            for base in (0..input.len()).step_by(188) {
                for delta in [0, 1, 3, 4, 5, 6, 9, 13, 14, 15, 16, 187] {
                    let split = base + delta;
                    if split <= input.len() {
                        assert_eq!(
                            gated(&[&input[..split], &input[split..], &suffix]),
                            baseline,
                            "split={split}"
                        );
                    }
                }
            }
        }
    }
}
