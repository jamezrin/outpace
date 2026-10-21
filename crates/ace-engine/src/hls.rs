//! Live HLS packaging and playlist state over a session's TS broadcast.
//!
//! [`HlsPackager`] consumes [`crate::session::StreamSession`] events in a background task,
//! tracks PCR timing and discontinuities, and owns startup readiness, the retained segment
//! window, and live playlist rendering. It uses [`ace_media::mpegts::SelectedPcrClock`] and
//! [`ace_media::mpegts::VideoAccessPointState`] for pure media analysis. Segments carry cached
//! PAT/PMT tables and prefer video access-point boundaries; the packet ceiling can force a
//! mid-GOP continuation once the target duration is reached, or discard an incomplete run.
//!
//! [`crate::manager`] manages shared packagers and session lifecycle. [`crate::http`] handles
//! HTTP playlist and segment requests, using this module's readiness and media accessors;
//! [`crate::routes`] only parses compatibility routes. [`VodHlsLayout`] separately describes
//! the static segment geometry and playlist for VOD.

use crate::config::HlsConfig;
use crate::session::{StreamEvent, StreamSession};
use ace_media::audio::LiveAudioSelection;
use ace_media::mpegts::{
    ts_pid, ts_timing, ClockObservation, SelectedPcrClock, TsTiming, VideoAccessPointState,
};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TS_PACKET: usize = 188;
#[cfg(test)]
const PCR_MODULUS: u64 = 1 << 33;
const SDT_PID: u16 = 0x0011;

/// A monotonically increasing token fencing writes from retired workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GenerationId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsFailure {
    Unsupported,
    Deadline,
    InputOverflow,
    Admission,
    ChildExit,
    Pipe,
    Pump,
    OutputInvalid,
    OutputStall,
    RecoveryLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsStatus {
    Starting,
    Ready,
    Recovering,
    Failed(HlsFailure),
    Stopped,
}

pub struct HlsPackager {
    state: Mutex<HlsState>,
    ready: tokio::sync::Notify,
    /// Hard packet ceiling for every segment. PCR/random-access timing may cut earlier, but never
    /// later, so configuration-time retained-memory calculations match the runtime invariant.
    max_segment_bytes: usize,
    /// Number of segments retained in the sliding window.
    window: usize,
    /// Requested segment duration (seconds).
    seg_duration: f32,
    /// Number of completed segments required before advertising startup readiness.
    startup_segments: usize,
    startup_timeout: Duration,
}

struct HlsState {
    generation: GenerationId,
    status: HlsStatus,
    deadline: Option<tokio::time::Instant>,
    strict: bool,
    generation_segments: usize,
    // The owner already announced this generation. An opening mux marker must
    // not discard its freshly validated prefix; expire at accepted access point.
    generation_opening: bool,
    audio: LiveAudioSelection,
    /// One source packet staged before filtering, outside the bounded segment buffer.
    input: Vec<u8>,
    /// Sequence number of the first retained segment.
    media_seq: u64,
    discontinuity_sequence: u64,
    segments: VecDeque<HlsSegment>,
    cur: Vec<u8>,
    scanned_packets: usize,
    clock: SelectedPcrClock,
    /// Selected PCR retained only to rebase the clock at a segment boundary.
    boundary_timing: Option<TsTiming>,
    max_segment_duration: f32,
    discontinuity_pending: bool,
    /// A provider recovery announces its boundary out of band and also marks the first resumed
    /// access point in band. Consume that marker as confirmation instead of resetting twice.
    deduplicate_source_marker: bool,
    last_access: Option<Instant>,
    access: VideoAccessPointState,
    awaiting_access_point: bool,
    /// Set when `access` carries a title, so upstream SDT-PID packets must be dropped from
    /// segment bodies to avoid duplicating (or conflicting with) our synthesized SDT.
    filter_sdt: bool,
}

struct HlsSegment {
    bytes: Bytes,
    duration: f32,
    discontinuity: bool,
}

impl HlsPackager {
    fn new(config: HlsConfig, service_name: Option<String>) -> Arc<HlsPackager> {
        let seg_packets = config.segment_packets.max(3);
        let max_segment_bytes = seg_packets
            .checked_mul(TS_PACKET)
            .expect("HLS config must be validated before packager construction");
        let mut access = VideoAccessPointState::with_service_name(service_name);
        let mut filter_sdt = access.has_service_name();
        // The injected SDT costs one extra prefix packet, so a titled segment must fit
        // [PAT][PMT][SDT] (3) plus a keyframe = 4 packets. If the configured ceiling can't hold
        // that, serve this stream WITHOUT the title SDT rather than stalling every segment forever.
        if filter_sdt && seg_packets < 4 {
            crate::alog!(
                "[hls] segment ceiling too small ({seg_packets} packets) for SDT service_name; \
                 serving without title SDT (needs >= 4 packets)"
            );
            access = VideoAccessPointState::new();
            filter_sdt = false;
        }
        access.enable_mpeg2();
        Arc::new(HlsPackager {
            state: Mutex::new(HlsState {
                generation: GenerationId(0),
                status: HlsStatus::Starting,
                deadline: None,
                strict: false,
                generation_segments: 0,
                generation_opening: false,
                audio: LiveAudioSelection::default(),
                input: Vec::with_capacity(TS_PACKET),
                media_seq: 0,
                discontinuity_sequence: 0,
                segments: VecDeque::new(),
                cur: Vec::new(),
                scanned_packets: 0,
                clock: SelectedPcrClock::new(),
                boundary_timing: None,
                max_segment_duration: config.segment_duration_secs(),
                discontinuity_pending: false,
                deduplicate_source_marker: false,
                last_access: None,
                access,
                awaiting_access_point: true,
                filter_sdt,
            }),
            ready: tokio::sync::Notify::new(),
            max_segment_bytes,
            window: config.window_segments.max(1),
            seg_duration: config.segment_duration_secs(),
            startup_segments: config.startup_segments.max(1),
            startup_timeout: Duration::from_millis(config.startup_timeout_ms),
        })
    }

    /// Create readable media state only; the manager separately owns its worker.
    pub fn create(session: &StreamSession, config: HlsConfig, strict: bool) -> Arc<Self> {
        let media = Self::new(config, session.metadata().title.clone());
        {
            let mut st = media.state.lock().unwrap();
            st.strict = strict;
            st.generation_opening = strict;
            if strict {
                st.access.enable_mpeg2();
                st.access.preserve_transport_identity();
            }
            st.deadline = Some(tokio::time::Instant::now() + media.startup_timeout);
        }
        media
    }

    /// Fence old output and reset only incomplete media. Completed sequences remain readable.
    pub fn begin_generation(
        &self,
        generation: GenerationId,
        deadline: tokio::time::Instant,
        recovering: bool,
    ) {
        let mut st = self.state.lock().unwrap();
        if matches!(st.status, HlsStatus::Stopped | HlsStatus::Failed(_))
            || generation <= st.generation
        {
            return;
        }
        st.generation = generation;
        st.strict = true;
        st.access.enable_mpeg2();
        st.access.preserve_transport_identity();
        st.deadline = Some(deadline);
        st.status = if recovering {
            HlsStatus::Recovering
        } else {
            HlsStatus::Starting
        };
        st.generation_segments = 0;
        Self::reset_partial(&mut st, false);
        st.generation_opening = true;
        st.discontinuity_pending = recovering;
        self.ready.notify_waiters();
    }

    pub fn feed_generation(&self, generation: GenerationId, data: &[u8]) {
        let mut st = self.state.lock().unwrap();
        if generation != st.generation
            || matches!(st.status, HlsStatus::Stopped | HlsStatus::Failed(_))
        {
            return;
        }
        self.feed_locked(&mut st, data);
    }

    pub fn fail_generation(&self, generation: GenerationId, reason: HlsFailure) {
        let mut st = self.state.lock().unwrap();
        if generation == st.generation
            && !matches!(st.status, HlsStatus::Stopped | HlsStatus::Failed(_))
        {
            st.status = HlsStatus::Failed(reason);
            Self::reset_partial(&mut st, false);
            self.ready.notify_waiters();
        }
    }

    pub fn stop(&self) {
        let mut st = self.state.lock().unwrap();
        st.status = HlsStatus::Stopped;
        Self::reset_partial(&mut st, false);
        self.ready.notify_waiters();
    }

    pub fn status(&self) -> HlsStatus {
        let mut st = self.state.lock().unwrap();
        self.expire(&mut st);
        st.status
    }

    fn expire(&self, st: &mut HlsState) {
        if st.strict
            && matches!(st.status, HlsStatus::Starting | HlsStatus::Recovering)
            && st
                .deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            st.status = HlsStatus::Failed(HlsFailure::Deadline);
            self.ready.notify_waiters();
        }
    }

    pub fn playlist_snapshot(
        &self,
        prefix: &str,
        native_access: bool,
    ) -> Result<String, HlsStatus> {
        let mut st = self.state.lock().unwrap();
        self.expire(&mut st);
        if st.status != HlsStatus::Ready {
            return Err(st.status);
        }
        if native_access {
            st.last_access = Some(Instant::now());
        }
        Ok(Self::render_playlist(&st, prefix))
    }

    pub async fn wait_available(&self, timeout: Duration) -> HlsStatus {
        let request_deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (status, deadline) = {
                let mut st = self.state.lock().unwrap();
                self.expire(&mut st);
                (st.status, if st.strict { st.deadline } else { None })
            };
            if !matches!(status, HlsStatus::Starting | HlsStatus::Recovering) {
                return status;
            }
            let until = deadline.map_or(request_deadline, |d| d.min(request_deadline));
            if tokio::time::timeout_at(until, notified).await.is_err() {
                return self.status();
            }
        }
    }

    pub(crate) async fn run_native(
        self: Arc<Self>,
        mut rx: tokio::sync::broadcast::Receiver<StreamEvent>,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) {
        use tokio::sync::broadcast::error::RecvError;
        loop {
            tokio::select! {
                biased;
                _ = cancel.changed() => { if *cancel.borrow() { break; } }
                event = rx.recv() => match event {
                    Ok(StreamEvent::Data(chunk)) => self.feed(&chunk),
                    Ok(StreamEvent::Discontinuity) => self.discontinuity(),
                    Err(RecvError::Lagged(_)) => self.receiver_lag(),
                    Err(RecvError::Closed) => break,
                }
            }
        }
        self.stop();
    }

    /// Drop media accumulated before a known gap and mark the first complete segment after it.
    fn discontinuity(&self) {
        self.reset_for_gap(true);
    }

    /// Receiver lag has no paired in-band marker contract, so any later transport marker remains
    /// an independent discontinuity and must take the normal reset path.
    fn receiver_lag(&self) {
        self.reset_for_gap(false);
    }

    fn reset_for_gap(&self, deduplicate_source_marker: bool) {
        let mut st = self.state.lock().unwrap();
        Self::reset_partial(&mut st, deduplicate_source_marker);
    }

    fn reset_partial(st: &mut HlsState, deduplicate_source_marker: bool) {
        st.generation_opening = false;
        st.cur.clear();
        st.scanned_packets = 0;
        st.clock.reset();
        st.boundary_timing = None;
        st.discontinuity_pending = true;
        st.deduplicate_source_marker = deduplicate_source_marker;
        st.access.reset();
        st.audio.reset();
        st.input.clear();
        st.awaiting_access_point = true;
    }

    /// Append contiguous TS bytes, emitting PCR-timed, preferably random-access-aligned segments.
    fn feed(&self, data: &[u8]) {
        let mut st = self.state.lock().unwrap();
        if matches!(st.status, HlsStatus::Stopped | HlsStatus::Failed(_)) {
            return;
        }
        self.feed_locked(&mut st, data);
    }

    fn feed_locked(&self, st: &mut HlsState, data: &[u8]) {
        self.expire(st);
        if matches!(st.status, HlsStatus::Stopped | HlsStatus::Failed(_)) {
            return;
        }
        let mut remaining = data;
        while !remaining.is_empty() {
            let take = (TS_PACKET - st.input.len()).min(remaining.len());
            st.input.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if st.input.len() != TS_PACKET {
                continue;
            }
            let mut packet: [u8; TS_PACKET] = st.input.as_slice().try_into().unwrap();
            st.input.clear();
            if st.strict && st.generation_opening && packet[3] & 0x20 != 0 && packet[4] > 0 {
                // Enabled owners fence source markers before publication. A new
                // mux's opening marker belongs to the same announced boundary.
                packet[5] &= !0x80;
            }
            // Transport markers must reset state before an excluded PID can be filtered.
            if ts_timing(&packet).discontinuity && !st.deduplicate_source_marker {
                st.audio.reset();
                st.access.reset();
                self.discard_incomplete_run(st);
                continue;
            }
            let result = st.audio.observe(packet);
            if result.changed {
                let pending = st.discontinuity_pending || !st.awaiting_access_point;
                st.access.reset();
                self.discard_incomplete_run(st);
                st.discontinuity_pending = pending;
                let tables: Vec<_> = st.audio.tables().copied().collect();
                for table in tables {
                    st.access.observe(&table);
                }
            }
            if let Some(packet) = result.packet {
                self.feed_packet(st, &packet);
            } else if ts_timing(&packet).discontinuity {
                st.deduplicate_source_marker = false;
            }
        }
    }

    fn feed_packet(&self, st: &mut HlsState, packet: &[u8; TS_PACKET]) {
        if st.cur.len() == self.max_segment_bytes {
            self.scan_lookahead(st, packet);
        } else {
            st.cur.extend_from_slice(packet);
            self.scan(st);
        }
    }

    fn scan(&self, st: &mut HlsState) {
        while st.scanned_packets < st.cur.len() / TS_PACKET {
            let packet_offset = st.scanned_packets * TS_PACKET;
            let mut packet = [0; TS_PACKET];
            packet.copy_from_slice(&st.cur[packet_offset..packet_offset + TS_PACKET]);
            let mut timing = ts_timing(&packet);
            let is_access_point = st.access.observe(&packet);
            let duplicate_source_marker = timing.discontinuity && st.deduplicate_source_marker;

            if timing.discontinuity {
                if st.deduplicate_source_marker {
                    // Keep the packet bytes unchanged for downstream consumers, but normalize
                    // this already-announced boundary for the HLS clock. An accepted opening
                    // access point is scanned twice (once to prepend tables, once for timing),
                    // so retain the token until that second pass.
                    timing.discontinuity = false;
                    if !st.awaiting_access_point {
                        st.deduplicate_source_marker = false;
                    }
                } else {
                    st.access.reset();
                    st.cur.drain(..packet_offset + TS_PACKET);
                    st.scanned_packets = 0;
                    st.clock.reset();
                    st.boundary_timing = None;
                    st.discontinuity_pending = true;
                    st.awaiting_access_point = true;
                    continue;
                }
            }

            // Drop a titled stream's upstream SDT before it becomes segment content, but only
            // after the discontinuity check above: `ts_timing` flags discontinuity PID-agnostically,
            // so a discontinuity-marked SDT must take the reset path, not be silently filtered.
            if st.filter_sdt && ts_pid(&packet) == SDT_PID {
                if duplicate_source_marker {
                    st.deduplicate_source_marker = false;
                }
                st.cur.drain(packet_offset..packet_offset + TS_PACKET);
                continue; // scanned_packets unchanged: the next packet slides into this offset
            }

            if st.awaiting_access_point {
                let Some(prefix) = is_access_point.then(|| st.access.table_prefix()).flatten()
                else {
                    if duplicate_source_marker {
                        st.deduplicate_source_marker = false;
                    }
                    st.cur.drain(..packet_offset + TS_PACKET);
                    st.scanned_packets = 0;
                    continue;
                };
                if prefix.len() + TS_PACKET > self.max_segment_bytes {
                    if duplicate_source_marker {
                        st.deduplicate_source_marker = false;
                    }
                    st.cur.drain(..packet_offset + TS_PACKET);
                    st.scanned_packets = 0;
                    st.discontinuity_pending = true;
                    continue;
                }
                // A source event may resume without an in-band marker. Once its first clean
                // access point is accepted, do not let the recovery token swallow a later,
                // unrelated transport discontinuity.
                if !duplicate_source_marker {
                    st.deduplicate_source_marker = false;
                }
                let trailing = st.cur.split_off(packet_offset + TS_PACKET);
                st.cur.clear();
                st.cur.extend_from_slice(&prefix);
                st.cur.extend_from_slice(&packet);
                st.cur.extend_from_slice(&trailing);
                st.scanned_packets = prefix.len() / TS_PACKET;
                st.awaiting_access_point = false;
                st.generation_opening = false;
                continue;
            }

            let observation = Self::observe_clock(st, timing);
            if observation == ClockObservation::Reset {
                st.access.reset();
                st.audio.reset();
                st.cur.drain(..packet_offset + TS_PACKET);
                st.scanned_packets = 0;
                st.discontinuity_pending = true;
                st.deduplicate_source_marker = false;
                st.awaiting_access_point = true;
                continue;
            }
            let elapsed = st.clock.elapsed().map(|duration| duration.as_secs_f32());
            let target_reached = elapsed.is_some_and(|d| d >= self.seg_duration);
            if packet_offset > 0 && target_reached && is_access_point {
                let duration = elapsed.unwrap_or(self.seg_duration);
                self.emit(st, packet_offset, duration);
                let prefix = st
                    .access
                    .table_prefix()
                    .expect("an observed video access point has PAT/PMT state");
                st.cur.splice(0..0, prefix.iter().copied());
                st.scanned_packets = prefix.len() / TS_PACKET;
                st.clock.reset();
                if let Some(boundary) = st.boundary_timing {
                    st.clock.observe(boundary);
                }
                continue;
            }
            st.scanned_packets += 1;
        }
    }

    /// Classify the packet after a ceiling-full run. A clean access point completes the retained
    /// run; any other packet proves that run cannot end cleanly within its configured bound.
    fn scan_lookahead(&self, st: &mut HlsState, packet: &[u8; TS_PACKET]) {
        let mut timing = ts_timing(packet);
        let is_access_point = st.access.observe(packet);

        if timing.discontinuity {
            if st.deduplicate_source_marker {
                st.deduplicate_source_marker = false;
                timing.discontinuity = false;
            } else {
                st.access.reset();
                self.discard_incomplete_run(st);
                return;
            }
        }

        if Self::observe_clock(st, timing) == ClockObservation::Reset {
            st.access.reset();
            st.audio.reset();
            self.discard_incomplete_run(st);
            return;
        }

        // Drop a titled stream's upstream SDT lookahead packet only after the discontinuity and
        // PCR-regression safety checks above (both PID-agnostic), so a flagged SDT still forces a
        // reset. `feed` already consumed the source packet, so returning here refills cleanly.
        if st.filter_sdt && ts_pid(packet) == SDT_PID {
            return;
        }

        let elapsed = st.clock.elapsed().map(|duration| duration.as_secs_f32());

        // Once a full target duration has elapsed, emit the ceiling-full segment even if this
        // boundary packet is not a keyframe. On a peaky or long-GOP stream (e.g. 2160p HEVC in a
        // high-motion scene) the next keyframe can land just past the byte ceiling; discarding
        // the run there would stall the playlist. Emitting keeps playback advancing. A run forced
        // out this way does not end on a keyframe, so the continuation segment starts mid-GOP --
        // seamless for sequential live playback, with the next keyframe re-aligning boundaries.
        // It is not a transport discontinuity, so no #EXT-X-DISCONTINUITY is emitted.
        if elapsed.is_some_and(|duration| duration >= self.seg_duration) {
            let duration = elapsed.unwrap_or(self.seg_duration);
            let boundary_timing = st.boundary_timing;
            self.emit(st, self.max_segment_bytes, duration);
            let prefix = st
                .access
                .table_prefix()
                .expect("an accumulated run past the ceiling has cached PAT/PMT state");
            self.seed_clean_run(st, &prefix, packet, timing);
            st.boundary_timing = boundary_timing;
            st.clock.reset();
            if let Some(boundary) = boundary_timing {
                st.clock.observe(boundary);
            }
            return;
        }

        self.discard_incomplete_run(st);
        if is_access_point {
            let prefix = st
                .access
                .table_prefix()
                .expect("an observed video access point has PAT/PMT state");
            self.seed_clean_run(st, &prefix, packet, timing);
        }
    }

    fn seed_clean_run(
        &self,
        st: &mut HlsState,
        prefix: &[u8],
        packet: &[u8; TS_PACKET],
        timing: TsTiming,
    ) {
        debug_assert!(prefix.len() + TS_PACKET <= self.max_segment_bytes);
        st.cur.clear();
        st.cur.extend_from_slice(prefix);
        st.cur.extend_from_slice(packet);
        st.scanned_packets = st.cur.len() / TS_PACKET;
        st.clock.reset();
        st.boundary_timing = None;
        Self::observe_clock(st, timing);
        st.awaiting_access_point = false;
    }

    fn discard_incomplete_run(&self, st: &mut HlsState) {
        st.cur.clear();
        st.scanned_packets = 0;
        st.clock.reset();
        st.boundary_timing = None;
        st.discontinuity_pending = true;
        st.deduplicate_source_marker = false;
        st.awaiting_access_point = true;
    }

    fn observe_clock(st: &mut HlsState, mut timing: TsTiming) -> ClockObservation {
        if st.audio.pcr_pid().is_some_and(|pid| pid != timing.pid) {
            timing.pcr = None;
        }
        let observation = st.clock.observe(timing);
        match observation {
            ClockObservation::Advanced(_) => st.boundary_timing = Some(timing),
            ClockObservation::Reset => st.boundary_timing = None,
            ClockObservation::Ignored => {}
        }
        observation
    }

    fn emit(&self, st: &mut HlsState, end: usize, duration: f32) {
        let was_ready = st.status == HlsStatus::Ready;
        // Evict before splitting/pushing so emission never transiently retains `window + 1`
        // completed segments in addition to the current segment. Configuration accounts for
        // exactly `window` completed segments plus `cur`.
        while st.segments.len() >= self.window {
            if st
                .segments
                .pop_front()
                .is_some_and(|segment| segment.discontinuity)
            {
                st.discontinuity_sequence += 1;
            }
            st.media_seq += 1;
        }
        let rest = st.cur.split_off(end);
        let mut seg = std::mem::replace(&mut st.cur, rest);
        st.audio.emitted(&mut seg);
        let discontinuity = std::mem::take(&mut st.discontinuity_pending);
        st.segments.push_back(HlsSegment {
            bytes: Bytes::from(seg),
            duration,
            discontinuity,
        });
        st.generation_segments = st.generation_segments.saturating_add(1);
        let count = if st.strict {
            st.generation_segments
        } else {
            st.segments.len()
        };
        if !was_ready && count >= self.startup_segments {
            st.status = HlsStatus::Ready;
            self.ready.notify_waiters();
        }
        st.max_segment_duration = st.max_segment_duration.max(duration);
        st.scanned_packets = 0;
    }

    /// Whether the configured startup cushion of independently joinable segments is available.
    pub fn is_ready(&self) -> bool {
        self.status() == HlsStatus::Ready
    }

    pub fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    /// Wait up to `timeout` for the first independently joinable live segment.
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        self.wait_available(timeout).await == HlsStatus::Ready
    }

    /// Render the live media playlist; segment URIs are absolute under the given prefix.
    pub fn playlist(&self, network: &str, id: &str) -> String {
        self.playlist_with_segment_prefix(&format!("/streams/{network}/{id}/seg"))
    }

    /// Record native HLS demand before a newly created packager is returned to its caller.
    pub(crate) fn record_native_access(&self) {
        self.state.lock().unwrap().last_access = Some(Instant::now());
    }

    /// Render a native live playlist with a caller-provided segment route and refresh activity.
    pub fn playlist_with_segment_prefix(&self, segment_prefix: &str) -> String {
        let mut st = self.state.lock().unwrap();
        st.last_access = Some(Instant::now());
        Self::render_playlist(&st, segment_prefix)
    }

    /// Render the retained live window for a compatibility route without refreshing native HLS
    /// activity. Compatibility lifetime is controlled by its explicit subscription pin.
    pub fn compatibility_playlist_with_segment_prefix(&self, segment_prefix: &str) -> String {
        Self::render_playlist(&self.state.lock().unwrap(), segment_prefix)
    }

    fn render_playlist(st: &HlsState, segment_prefix: &str) -> String {
        let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:3\n");
        out.push_str(&format!(
            "#EXT-X-TARGETDURATION:{}\n",
            st.max_segment_duration.ceil() as u64
        ));
        if let Some(offset) = Self::advisory_start_offset(st) {
            out.push_str(&format!(
                "#EXT-X-START:TIME-OFFSET=-{offset:.3},PRECISE=NO\n"
            ));
        }
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", st.media_seq));
        if st.discontinuity_sequence != 0 {
            out.push_str(&format!(
                "#EXT-X-DISCONTINUITY-SEQUENCE:{}\n",
                st.discontinuity_sequence
            ));
        }
        for (i, segment) in st.segments.iter().enumerate() {
            if segment.discontinuity {
                out.push_str("#EXT-X-DISCONTINUITY\n");
            }
            out.push_str(&format!("#EXTINF:{:.3},\n", segment.duration));
            out.push_str(&format!(
                "{segment_prefix}/{}.ts\n",
                st.media_seq + i as u64
            ));
        }
        out
    }

    fn advisory_start_offset(st: &HlsState) -> Option<f32> {
        let safety_distance = 3.0 * st.max_segment_duration.ceil();
        let mut suffix = 0.0;
        let mut safe_boundary = None;
        for index in (1..st.segments.len()).rev() {
            suffix += st.segments[index].duration;
            if suffix >= safety_distance {
                safe_boundary = Some((index, suffix));
                break;
            }
        }
        let (index, suffix) = safe_boundary?;
        if index > 1 {
            Some(suffix + st.segments[index - 1].duration)
        } else {
            Some(suffix)
        }
    }

    /// Fetch a retained segment by its absolute sequence number.
    pub fn segment(&self, seq: u64) -> Option<Bytes> {
        let mut st = self.state.lock().unwrap();
        let bytes = Self::retained_segment(&st, seq);
        if bytes.is_some() {
            st.last_access = Some(Instant::now());
        }
        bytes
    }

    /// Fetch a retained segment for a compatibility route without refreshing native HLS activity.
    pub fn compatibility_segment(&self, seq: u64) -> Option<Bytes> {
        Self::retained_segment(&self.state.lock().unwrap(), seq)
    }

    fn retained_segment(st: &HlsState, seq: u64) -> Option<Bytes> {
        if seq < st.media_seq {
            return None;
        }
        st.segments
            .get((seq - st.media_seq) as usize)
            .map(|segment| segment.bytes.clone())
    }

    pub(crate) fn was_accessed_within(&self, now: Instant, grace: Duration) -> bool {
        self.state
            .lock()
            .unwrap()
            .last_access
            .is_some_and(|last_access| now.saturating_duration_since(last_access) < grace)
    }

    #[cfg(test)]
    pub(crate) fn set_last_access_for_test(&self, last_access: Instant) {
        self.state.lock().unwrap().last_access = Some(last_access);
    }

    #[cfg(test)]
    pub(crate) fn last_access_for_test(&self) -> Option<Instant> {
        self.state.lock().unwrap().last_access
    }
}

/// Byte-range HLS layout for a finite, length-known VOD.
///
/// Unlike [`HlsPackager`] (a live sliding window fed by a broadcast), a VOD's whole geometry is
/// known up front, so segmentation is a pure function of the total length and configured segment
/// size — no state, no retained window. Each segment maps to an inclusive byte range that the
/// HTTP layer serves via `VodContent::open_range` (which SHA-1-verifies the covering pieces).
///
/// Segmentation is byte-based and aligned to whole 188-byte TS packets, exactly like the live
/// packager: it assumes MPEG-TS content and is not keyframe-aware (fine for tolerant players;
/// precise GOP segmentation is a documented follow-up).
pub struct VodHlsLayout {
    total: u64,
    /// Segment size in bytes (a whole number of 188-byte TS packets, >= 188).
    seg_bytes: u64,
    /// Nominal duration of a full segment (seconds).
    seg_duration: f32,
}

impl VodHlsLayout {
    pub fn new(total: u64, config: HlsConfig) -> VodHlsLayout {
        VodHlsLayout {
            total,
            seg_bytes: (config.segment_packets.max(3) * TS_PACKET) as u64,
            seg_duration: config.segment_duration_secs(),
        }
    }

    /// Number of segments covering the whole file (ceil division). Zero-length → 0 segments.
    pub fn segment_count(&self) -> u64 {
        self.total.div_ceil(self.seg_bytes)
    }

    /// Inclusive byte range `[start, end]` for segment `index`, or `None` if `index` is past the
    /// last segment. The final segment is clamped to the last byte, so it may be shorter than a
    /// full segment.
    pub fn segment_range(&self, index: u64) -> Option<(u64, u64)> {
        if index >= self.segment_count() {
            return None;
        }
        let start = index * self.seg_bytes;
        let end = ((index + 1) * self.seg_bytes).min(self.total) - 1;
        Some((start, end))
    }

    /// Render the VOD media playlist: a static `#EXT-X-PLAYLIST-TYPE:VOD` list of every segment,
    /// terminated by `#EXT-X-ENDLIST`. Per-segment `#EXTINF` durations are the nominal segment
    /// duration, with the trailing partial segment scaled by its byte fraction.
    pub fn playlist(&self, network: &str, id: &str) -> String {
        let count = self.segment_count();
        let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n");
        out.push_str(&format!(
            "#EXT-X-TARGETDURATION:{}\n",
            self.seg_duration.ceil() as u64
        ));
        out.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");
        for i in 0..count {
            let (start, end) = self.segment_range(i).expect("i < count");
            let dur = self.seg_duration * (end - start + 1) as f32 / self.seg_bytes as f32;
            out.push_str(&format!("#EXTINF:{dur:.3},\n"));
            out.push_str(&format!("/vod/{network}/{id}/seg/{i}.ts\n"));
        }
        out.push_str("#EXT-X-ENDLIST\n");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HlsConfig;

    fn pkg() -> Arc<HlsPackager> {
        HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 3,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            None,
        )
    }

    fn packets(n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n * TS_PACKET];
        for k in 0..n {
            v[k * TS_PACKET] = 0x47;
            v[k * TS_PACKET + 3] = 0x10;
        }
        v
    }

    fn timed_pkg(duration_ms: u64) -> Arc<HlsPackager> {
        HlsPackager::new(
            HlsConfig {
                segment_packets: 100,
                window_segments: 6,
                segment_duration_ms: duration_ms,
                ..HlsConfig::default()
            },
            None,
        )
    }

    fn random_access_packet(marker: u8) -> Vec<u8> {
        let mut packet = packets(1);
        packet[1] = 0x40 | ((VIDEO_PID >> 8) as u8 & 0x1f);
        packet[2] = VIDEO_PID as u8;
        packet[3] = 0x30;
        packet[4] = 1;
        packet[5] = 0x40;
        packet[6] = marker;
        packet
    }

    fn with_pid(mut packet: Vec<u8>, pid: u16) -> Vec<u8> {
        packet[1] = (packet[1] & 0xe0) | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet
    }

    fn psi(pid: u16, section: &[u8]) -> Vec<u8> {
        let mut packet = vec![0xff; TS_PACKET];
        packet[0] = 0x47;
        packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet[3] = 0x10;
        packet[4] = 0;
        packet[5..5 + section.len()].copy_from_slice(section);
        packet
    }

    fn pat(pmt_pid: u16) -> Vec<u8> {
        let section_length = 5 + 4 + 4;
        let mut section = vec![0x00, 0xb0, section_length as u8, 0x00, 0x01, 0xc1, 0, 0];
        section.extend_from_slice(&[
            0x00,
            0x01,
            0xe0 | ((pmt_pid >> 8) as u8 & 0x1f),
            pmt_pid as u8,
        ]);
        section.extend_from_slice(&[0; 4]);
        psi(0, &section)
    }

    fn pmt(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
        let section_length = 5 + 4 + 5 + 4;
        let mut section = vec![0x02, 0xb0, section_length as u8, 0x00, 0x01, 0xc1, 0, 0];
        section.extend_from_slice(&[
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
            0x1b,
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
        ]);
        section.extend_from_slice(&[0; 4]);
        psi(pmt_pid, &section)
    }

    fn pcr_packet(
        pid: u16,
        pcr: u64,
        random_access: bool,
        discontinuity: bool,
        marker: u8,
    ) -> Vec<u8> {
        let mut packet = vec![0xff; TS_PACKET];
        packet[0] = 0x47;
        packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet[3] = 0x30;
        packet[4] = 7;
        packet[5] =
            0x10 | if random_access { 0x40 } else { 0 } | if discontinuity { 0x80 } else { 0 };
        packet[6] = (pcr >> 25) as u8;
        packet[7] = (pcr >> 17) as u8;
        packet[8] = (pcr >> 9) as u8;
        packet[9] = (pcr >> 1) as u8;
        packet[10] = ((pcr & 1) << 7) as u8 | 0x7e;
        packet[11] = 0;
        packet[12] = marker;
        packet
    }

    fn video_access_packet(pid: u16, pcr: u64) -> Vec<u8> {
        pcr_packet(pid, pcr, true, false, (pcr / 90_000) as u8)
    }

    fn mpeg2_pes_packet(pcr: u64, picture_type: u8) -> Vec<u8> {
        let mut packet = pcr_packet(VIDEO_PID, pcr, false, false, 0);
        let pes = [
            0,
            0,
            1,
            0xe0,
            0,
            0,
            0x80,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            ((picture_type & 7) << 3),
            0x55,
            0x66,
        ];
        packet[1] |= 0x40; // PUSI
        packet[12..12 + pes.len()].copy_from_slice(&pes);
        packet
    }

    fn mpeg2_b3_packet(pcr: u64) -> Vec<u8> {
        let mut packet = mpeg2_pes_packet(pcr, 1);
        packet[21..25].copy_from_slice(&[0, 0, 1, 0xb3]);
        packet
    }

    fn clean_pkg(segment_packets: usize) -> Arc<HlsPackager> {
        HlsPackager::new(
            HlsConfig {
                segment_packets,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..HlsConfig::default()
            },
            None,
        )
    }

    const PMT_PID: u16 = 0x0100;
    const VIDEO_PID: u16 = 0x0101;
    const AUDIO_PID: u16 = 0x0102;

    fn feed_one_clean_segment(p: &HlsPackager) {
        feed_clean_run(p, 0, 1);
    }

    fn feed_clean_run(p: &HlsPackager, start_pcr: u64, segment_count: usize) {
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, start_pcr));
        for i in 1..=segment_count {
            p.feed(&video_access_packet(
                VIDEO_PID,
                start_pcr + i as u64 * 108_000,
            ));
        }
    }

    fn valid_crc(mut packet: Vec<u8>) -> Vec<u8> {
        let end = 5 + 3 + (((packet[6] & 15) as usize) << 8 | packet[7] as usize);
        let crc = ace_media::mpegts::mpeg_crc32(&packet[5..end - 4]);
        packet[end - 4..end].copy_from_slice(&crc.to_be_bytes());
        packet
    }

    fn audio_pmt(types: &[u8], pcr_pid: u16) -> Vec<u8> {
        let mut section = vec![
            2,
            0xb0,
            0,
            0,
            1,
            0xc1,
            0,
            0,
            0xe0 | (pcr_pid >> 8) as u8,
            pcr_pid as u8,
            0xf0,
            0,
            0x1b,
            0xe1,
            1,
            0xf0,
            0,
        ];
        for (i, ty) in types.iter().enumerate() {
            section.extend_from_slice(&[*ty, 0xe1, 2 + i as u8, 0xf0, 0]);
        }
        section.extend_from_slice(&[0; 4]);
        section[2] = (section.len() - 3) as u8;
        valid_crc(psi(PMT_PID, &section))
    }

    #[test]
    fn live_hls_prefers_first_adts_and_removes_competing_audio() {
        for types in [&[0x03, 0x0f, 0x0f][..], &[0x0f, 0x04, 0x11][..]] {
            let p = timed_pkg(1000);
            p.feed(&valid_crc(pat(PMT_PID)));
            p.feed(&audio_pmt(types, VIDEO_PID));
            p.feed(&video_access_packet(VIDEO_PID, 0));
            for i in 0..3 {
                p.feed(&with_pid(packets(1), AUDIO_PID + i));
            }
            p.feed(&audio_pmt(types, VIDEO_PID));
            p.feed(&video_access_packet(VIDEO_PID, 108_000));
            let seg = p.segment(0).unwrap();
            let selected = AUDIO_PID + types.iter().position(|ty| *ty == 0x0f).unwrap() as u16;
            for i in 0..3 {
                assert_eq!(
                    seg.as_chunks::<TS_PACKET>()
                        .0
                        .iter()
                        .map(|packet| packet.as_slice())
                        .any(|pkt| ts_pid(pkt) == AUDIO_PID + i),
                    AUDIO_PID + i == selected,
                    "competing audio leaked"
                );
            }
            for pkt in seg
                .as_chunks::<TS_PACKET>()
                .0
                .iter()
                .map(|packet| packet.as_slice())
                .filter(|pkt| ts_pid(pkt) == PMT_PID)
            {
                let end = 8 + pkt[7] as usize;
                assert_eq!(ace_media::mpegts::mpeg_crc32(&pkt[5..end]), 0);
                assert_eq!(pkt[7], 23); // fixed PMT + video + one audio + CRC
            }
        }
    }

    #[test]
    fn live_hls_without_adts_preserves_audio_and_tables() {
        let p = timed_pkg(1000);
        let table = audio_pmt(&[0x03, 0x11, 0x06], VIDEO_PID);
        p.feed(&valid_crc(pat(PMT_PID)));
        p.feed(&table);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        for i in 0..3 {
            p.feed(&with_pid(packets(1), AUDIO_PID + i));
        }
        p.feed(&video_access_packet(VIDEO_PID, 108_000));
        let seg = p.segment(0).unwrap();
        assert_eq!(&seg[TS_PACKET..2 * TS_PACKET], &table);
        for i in 0..3 {
            assert!(seg
                .as_chunks::<TS_PACKET>()
                .0
                .iter()
                .map(|packet| packet.as_slice())
                .any(|pkt| ts_pid(pkt) == AUDIO_PID + i));
        }
    }

    #[test]
    fn live_hls_retains_removed_audio_pcr_without_pes_and_uses_declared_clock() {
        let p = timed_pkg(1000);
        p.feed(&valid_crc(pat(PMT_PID)));
        p.feed(&audio_pmt(&[0x03, 0x0f], AUDIO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 900_000));
        p.feed(&pcr_packet(AUDIO_PID, 0, false, false, 0x55));
        p.feed(&pcr_packet(AUDIO_PID, 108_000, false, false, 0x56));
        p.feed(&video_access_packet(VIDEO_PID, 1)); // unrelated backwards clock
        let seg = p.segment(0).expect("declared audio PCR controls boundary");
        let clock = seg
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .find(|pkt| ts_pid(pkt) == AUDIO_PID)
            .unwrap();
        assert_eq!(clock[3] & 0x30, 0x20);
        assert_eq!(clock[4], 183);
        assert_eq!(clock[1] & 0x40, 0);
        assert!(clock[12..].iter().all(|byte| *byte == 0xff));
    }

    #[test]
    fn live_hls_pat_transport_switch_requires_fresh_prefix_and_discontinuity() {
        let p = timed_pkg(1000);
        let old_pat = valid_crc(pat(PMT_PID));
        p.feed(&old_pat);
        p.feed(&audio_pmt(&[3, 15], VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&with_pid(packets(1), 777));
        let mut version_repeat = old_pat.clone();
        version_repeat[10] = 0xc7;
        p.feed(&valid_crc(version_repeat.clone()));
        assert!(!p.state.lock().unwrap().awaiting_access_point);
        let mut switched = version_repeat;
        switched[9] = 2;
        let switched = valid_crc(switched);
        p.feed(&switched);
        {
            let st = p.state.lock().unwrap();
            assert!(st.awaiting_access_point);
            assert!(st.access.table_prefix().is_none());
        }
        p.feed(&video_access_packet(VIDEO_PID, 108_000));
        assert!(p.segment(0).is_none()); // no old PMT may unlock the new transport
        let fresh = audio_pmt(&[3, 0x11], VIDEO_PID);
        p.feed(&fresh);
        p.feed(&video_access_packet(VIDEO_PID, 216_000));
        let audio = with_pid(packets(1), AUDIO_PID);
        p.feed(&audio);
        p.feed(&video_access_packet(VIDEO_PID, 324_000));
        let segment = p.segment(0).unwrap();
        assert_eq!(&segment[..TS_PACKET], switched.as_slice());
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], fresh.as_slice());
        assert!(segment
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|pkt| pkt == audio));
        assert!(!segment
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|pkt| ts_pid(pkt) == 777));
        assert!(p
            .playlist("test", "synthetic")
            .contains("#EXT-X-DISCONTINUITY"));
    }

    #[test]
    fn live_hls_losing_adts_restores_original_prefix_and_marks_fresh_run() {
        let p = timed_pkg(1000);
        let pat = valid_crc(pat(PMT_PID));
        p.feed(&pat);
        p.feed(&audio_pmt(&[3, 15], VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&with_pid(packets(1), 777));
        let mut plain = audio_pmt(&[3, 0x11, 6], VIDEO_PID);
        plain[10] = 0xc1 | (17 << 1);
        let plain = valid_crc(plain);
        let mut original = vec![0xff; TS_PACKET];
        original[..4].copy_from_slice(&plain[..4]);
        original[3] = 0x39;
        original[4] = 2;
        original[5] = 0;
        original[7..].copy_from_slice(&plain[4..TS_PACKET - 3]);
        p.feed(&original);
        p.feed(&video_access_packet(VIDEO_PID, 108_000));
        p.feed(&original); // upstream repetition must remain unchanged too
        for i in 0..3 {
            p.feed(&with_pid(packets(1), AUDIO_PID + i));
        }
        p.feed(&video_access_packet(VIDEO_PID, 216_000));
        let segment = p.segment(0).unwrap();
        assert_eq!(&segment[..TS_PACKET], pat.as_slice());
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], original.as_slice());
        let pmts: Vec<_> = segment
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .filter(|pkt| ts_pid(pkt) == PMT_PID)
            .collect();
        assert_eq!(pmts.len(), 2);
        assert!(pmts.iter().all(|pkt| *pkt == original));
        for i in 0..3 {
            assert!(segment
                .as_chunks::<TS_PACKET>()
                .0
                .iter()
                .map(|packet| packet.as_slice())
                .any(|pkt| ts_pid(pkt) == AUDIO_PID + i));
        }
        assert!(!segment
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|pkt| ts_pid(pkt) == 777));
        assert!(p
            .playlist("test", "synthetic")
            .contains("#EXT-X-DISCONTINUITY"));
    }

    #[test]
    fn live_hls_table_change_discards_unfinished_run() {
        let p = timed_pkg(1000);
        p.feed(&valid_crc(pat(PMT_PID)));
        p.feed(&audio_pmt(&[0x03, 0x0f], VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&with_pid(packets(1), 777));
        p.feed(&audio_pmt(&[0x0f, 0x03], VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 108_000));
        p.feed(&video_access_packet(VIDEO_PID, 216_000));
        let seg = p.segment(0).unwrap();
        assert!(!seg
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|pkt| ts_pid(pkt) == 777));
        assert!(p
            .playlist("test", "synthetic")
            .contains("#EXT-X-DISCONTINUITY"));
    }

    #[test]
    fn live_hls_late_validated_selection_discards_old_audio_and_cached_tables() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID)); // legacy non-validated video metadata
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&with_pid(packets(1), AUDIO_PID));
        p.feed(&valid_crc(pat(PMT_PID)));
        p.feed(&audio_pmt(&[3, 15], VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 108_000));
        p.feed(&video_access_packet(VIDEO_PID, 216_000));
        let seg = p.segment(0).unwrap();
        assert!(!seg
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|pkt| ts_pid(pkt) == AUDIO_PID));
        assert!(p
            .playlist("test", "synthetic")
            .contains("#EXT-X-DISCONTINUITY"));
    }

    #[test]
    fn live_hls_selection_filters_ceiling_lookahead_and_counts_prefix_pmt_continuity() {
        let p = clean_pkg(4);
        let table = audio_pmt(&[3, 15], VIDEO_PID);
        p.feed(&valid_crc(pat(PMT_PID)));
        p.feed(&table);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&table); // fills ceiling with a repeated PMT
        p.feed(&with_pid(packets(1), AUDIO_PID)); // must not force a ceiling cut
        p.feed(&video_access_packet(VIDEO_PID, 108_000));
        p.feed(&table);
        p.feed(&video_access_packet(VIDEO_PID, 216_000));
        let mut counters = Vec::new();
        for seq in 0..2 {
            let seg = p.segment(seq).unwrap();
            assert!(seg.len() <= 4 * TS_PACKET);
            assert!(!seg
                .as_chunks::<TS_PACKET>()
                .0
                .iter()
                .map(|packet| packet.as_slice())
                .any(|pkt| ts_pid(pkt) == AUDIO_PID));
            for pkt in seg
                .as_chunks::<TS_PACKET>()
                .0
                .iter()
                .map(|packet| packet.as_slice())
                .filter(|pkt| ts_pid(pkt) == PMT_PID)
            {
                counters.push(pkt[3] & 15);
                assert_eq!(
                    ace_media::mpegts::mpeg_crc32(&pkt[5..8 + pkt[7] as usize]),
                    0
                );
            }
        }
        assert_eq!(counters, vec![0, 1, 2, 3]);
    }

    #[test]
    fn live_hls_gap_and_lag_clear_selection_and_partial_source_packet() {
        for lag in [false, true] {
            let p = timed_pkg(1000);
            p.feed(&valid_crc(pat(PMT_PID)));
            p.feed(&audio_pmt(&[3, 15], VIDEO_PID));
            p.feed(&video_access_packet(VIDEO_PID, 0));
            p.feed(&audio_pmt(&[3, 15], VIDEO_PID)[..37]);
            if lag {
                p.receiver_lag();
            } else {
                p.discontinuity();
            }
            p.feed(&valid_crc(pat(PMT_PID)));
            p.feed(&audio_pmt(&[0x11, 3], VIDEO_PID)); // no ADTS: old filter must be gone
            p.feed(&video_access_packet(VIDEO_PID, 900_000));
            p.feed(&with_pid(packets(1), AUDIO_PID));
            p.feed(&video_access_packet(VIDEO_PID, 1_008_000));
            let seg = p.segment(0).unwrap();
            assert!(seg
                .as_chunks::<TS_PACKET>()
                .0
                .iter()
                .map(|packet| packet.as_slice())
                .any(|pkt| ts_pid(pkt) == AUDIO_PID));
            assert!(p
                .playlist("test", "synthetic")
                .contains("#EXT-X-DISCONTINUITY"));
        }
    }

    #[test]
    fn live_hls_excluded_audio_discontinuity_is_processed_before_filtering() {
        let p = timed_pkg(1000);
        p.feed(&valid_crc(pat(PMT_PID)));
        p.feed(&audio_pmt(&[3, 15], VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&with_pid(packets(1), 777));
        p.feed(&pcr_packet(AUDIO_PID, 0, false, true, 0));
        p.feed(&valid_crc(pat(PMT_PID)));
        p.feed(&audio_pmt(&[3, 15], VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 900_000));
        p.feed(&video_access_packet(VIDEO_PID, 1_008_000));
        let seg = p.segment(0).unwrap();
        assert!(!seg
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|pkt| ts_pid(pkt) == 777));
        assert!(p
            .playlist("test", "synthetic")
            .contains("#EXT-X-DISCONTINUITY"));
    }

    #[test]
    fn clean_first_segment_discards_partial_gop_and_starts_with_tables_and_video_access() {
        let p = clean_pkg(12);
        let partial_gop_packet = with_pid(packets(1), VIDEO_PID);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        let first_access = video_access_packet(VIDEO_PID, 0);

        p.feed(&partial_gop_packet);
        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&first_access);
        p.feed(&pcr_packet(VIDEO_PID, 45_000, false, false, 7));
        p.feed(&video_access_packet(VIDEO_PID, 108_000));

        let segment = p.segment(0).expect("first clean segment");
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&segment[TS_PACKET * 2..TS_PACKET * 3], &first_access);
        assert!(!segment
            .windows(TS_PACKET)
            .any(|packet| packet == partial_gop_packet));
    }

    #[test]
    fn clean_subsequent_segment_repeats_tables_and_ignores_non_video_random_access() {
        let p = clean_pkg(12);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        let non_video_access = pcr_packet(AUDIO_PID, 108_000, true, false, 8);
        let second_access = video_access_packet(VIDEO_PID, 216_000);

        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 108_000, false, false, 7));
        p.feed(&non_video_access);
        p.feed(&second_access);
        p.feed(&pcr_packet(VIDEO_PID, 270_000, false, false, 9));
        p.feed(&video_access_packet(VIDEO_PID, 324_000));

        let first = p.segment(0).expect("first clean segment");
        assert!(first
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|packet| packet == non_video_access));
        let second = p.segment(1).expect("second clean segment");
        assert_eq!(&second[..TS_PACKET], &pat);
        assert_eq!(&second[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&second[TS_PACKET * 2..TS_PACKET * 3], &second_access);
    }

    #[test]
    fn clean_discontinuity_requires_fresh_tables_before_restarting() {
        let p = clean_pkg(12);
        let stale_access = video_access_packet(VIDEO_PID, 0);
        let fresh_pat = pat(PMT_PID);
        let fresh_pmt = pmt(PMT_PID, VIDEO_PID);
        let fresh_access = video_access_packet(VIDEO_PID, 90_000);

        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.discontinuity();
        p.feed(&stale_access);
        p.feed(&fresh_pat);
        p.feed(&fresh_pmt);
        p.feed(&fresh_access);
        p.feed(&video_access_packet(VIDEO_PID, 198_000));

        let segment = p.segment(0).expect("first post-gap clean segment");
        assert_eq!(&segment[..TS_PACKET], &fresh_pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &fresh_pmt);
        assert_eq!(&segment[TS_PACKET * 2..TS_PACKET * 3], &fresh_access);
        assert!(!segment
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|packet| packet == stale_access));
    }

    #[test]
    fn source_event_then_marked_resumed_access_starts_the_post_gap_segment() {
        let p = clean_pkg(12);
        let fresh_pat = pat(PMT_PID);
        let fresh_pmt = pmt(PMT_PID, VIDEO_PID);
        let marked_access = pcr_packet(VIDEO_PID, 900_000, true, true, 7);

        feed_one_clean_segment(&p);
        p.discontinuity();
        p.feed(&fresh_pat);
        p.feed(&fresh_pmt);
        p.feed(&marked_access);
        p.feed(&video_access_packet(VIDEO_PID, 1_008_000));

        let segment = p.segment(1).expect(
            "the source event and its in-band marker describe one boundary, so the marked access point must be consumed",
        );
        assert_eq!(&segment[..TS_PACKET], &fresh_pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &fresh_pmt);
        assert_eq!(&segment[TS_PACKET * 2..TS_PACKET * 3], &marked_access);
        assert_eq!(
            p.playlist("test", "event-marker")
                .matches("#EXT-X-DISCONTINUITY\n")
                .count(),
            1
        );
    }

    #[test]
    fn unmarked_resumed_access_closes_source_marker_dedup_window() {
        let p = clean_pkg(12);
        let first_pat = pat(PMT_PID);
        let first_pmt = pmt(PMT_PID, VIDEO_PID);
        let unmarked_resume = video_access_packet(VIDEO_PID, 900_000);
        let genuine_marker = pcr_packet(VIDEO_PID, 2_000_000, true, true, 8);
        let second_pat = pat(PMT_PID);
        let second_pmt = pmt(PMT_PID, VIDEO_PID);
        let post_marker_access = video_access_packet(VIDEO_PID, 2_100_000);

        feed_one_clean_segment(&p);
        p.discontinuity();
        p.feed(&first_pat);
        p.feed(&first_pmt);
        p.feed(&unmarked_resume);
        p.feed(&video_access_packet(VIDEO_PID, 1_008_000));

        p.feed(&genuine_marker);
        p.feed(&second_pat);
        p.feed(&second_pmt);
        p.feed(&post_marker_access);
        p.feed(&video_access_packet(VIDEO_PID, 2_208_000));

        let segment = p
            .segment(2)
            .expect("the later genuine marker must start another clean segment");
        assert_eq!(&segment[..TS_PACKET], &second_pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &second_pmt);
        assert_eq!(&segment[TS_PACKET * 2..TS_PACKET * 3], &post_marker_access);
        assert!(!segment
            .as_chunks::<TS_PACKET>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .any(|packet| packet == genuine_marker));
        assert_eq!(
            p.playlist("test", "unmarked-resume")
                .matches("#EXT-X-DISCONTINUITY\n")
                .count(),
            2
        );
    }

    #[test]
    fn clean_hard_ceiling_discards_incomplete_run_and_restarts_at_next_access_point() {
        let p = clean_pkg(5);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);

        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 9_000, false, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 18_000, false, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 27_000, false, false, 3));

        assert!(p.segment(0).is_none());
        assert!(p.state.lock().unwrap().cur.is_empty());

        let restarted_access = video_access_packet(VIDEO_PID, 108_000);
        p.feed(&restarted_access);
        p.feed(&video_access_packet(VIDEO_PID, 216_000));

        let segment = p.segment(0).expect("clean segment after ceiling reset");
        assert_eq!(segment.len(), 3 * TS_PACKET);
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&segment[TS_PACKET * 2..], &restarted_access);
        assert!(p
            .playlist("test", "ceiling")
            .contains("#EXT-X-DISCONTINUITY"));
        let st = p.state.lock().unwrap();
        assert!(st.cur.len() <= p.max_segment_bytes);
        assert!(st
            .segments
            .iter()
            .all(|segment| segment.bytes.len() <= p.max_segment_bytes));
    }

    #[test]
    fn hard_ceiling_emits_oversized_segment_instead_of_stalling_on_peaky_gop() {
        // A peaky / long-GOP stream can fill the byte ceiling after a full target duration has
        // elapsed but before the next keyframe arrives. Discarding the run there stalls the
        // playlist (the #137 4K-HEVC bug); instead the packager emits the ceiling-full segment
        // and continues. The forced cut is not a transport discontinuity, so no
        // #EXT-X-DISCONTINUITY is introduced.
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 5,
                window_segments: 4,
                segment_duration_ms: 100,
                ..HlsConfig::default()
            },
            None,
        );
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 45_000, false, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 90_000, false, false, 2));
        // Ceiling is full; this packet arrives past the target duration with no keyframe.
        p.feed(&pcr_packet(VIDEO_PID, 135_000, false, false, 3));

        let segment = p
            .segment(0)
            .expect("ceiling-full segment is emitted, not discarded");
        assert_eq!(segment.len(), 5 * TS_PACKET);
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert!(!p.playlist("test", "peaky").contains("#EXT-X-DISCONTINUITY"));
    }

    #[test]
    fn clean_three_packet_ceiling_uses_next_access_point_as_boundary_lookahead() {
        let p = clean_pkg(3);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        let first_access = video_access_packet(VIDEO_PID, 0);

        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&first_access);
        let boundary = video_access_packet(VIDEO_PID, 108_000);
        p.feed(&boundary[..100]);
        assert!(p.segment(0).is_none());
        assert_eq!(p.state.lock().unwrap().cur.len(), 3 * TS_PACKET);
        p.feed(&boundary[100..]);

        let segment = p.segment(0).expect("three-packet clean segment");
        assert_eq!(segment.len(), 3 * TS_PACKET);
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&segment[TS_PACKET * 2..], &first_access);
        let st = p.state.lock().unwrap();
        assert!(st.cur.len() <= 3 * TS_PACKET);
        assert!(st
            .segments
            .iter()
            .all(|segment| segment.bytes.len() <= 3 * TS_PACKET));
    }

    #[test]
    fn payload_only_packet_fixtures_have_valid_afc() {
        let partial_gop = with_pid(packets(1), VIDEO_PID);
        let pcr_disappeared = with_pid(packets(1), VIDEO_PID);

        assert_eq!(partial_gop[3] & 0x30, 0x10);
        assert_eq!(pcr_disappeared[3] & 0x30, 0x10);
    }

    #[tokio::test]
    async fn readiness_waits_for_first_completed_segment_and_then_stays_ready() {
        let p = clean_pkg(6);
        let stale = Instant::now() - Duration::from_secs(60);
        p.state.lock().unwrap().last_access = Some(stale);

        assert!(!p.is_ready());
        assert!(!p.wait_ready(Duration::from_millis(1)).await);
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));

        feed_one_clean_segment(&p);
        assert!(p.wait_ready(Duration::from_millis(50)).await);
        assert!(p.is_ready());
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));
    }

    #[tokio::test]
    async fn readiness_waits_for_configured_completed_segment_count() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 3,
                startup_timeout_ms: 20,
            },
            None,
        );

        feed_clean_run(&p, 0, 2);
        assert!(!p.is_ready());
        assert_eq!(p.startup_timeout(), Duration::from_millis(20));
        feed_clean_run(&p, 1_000_000, 1);
        assert!(p.wait_ready(Duration::from_millis(20)).await);
    }

    #[test]
    fn playlist_start_uses_irregular_retained_durations_inside_safe_edge() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 8,
                segment_duration_ms: 5000,
                startup_segments: 1,
                startup_timeout_ms: 5000,
            },
            None,
        );
        let mut state = p.state.lock().unwrap();
        state.max_segment_duration = 7.0;
        state.segments = [4.0, 6.0, 5.0, 7.0, 4.0, 6.0, 5.0, 7.0]
            .into_iter()
            .map(|duration| HlsSegment {
                bytes: Bytes::new(),
                duration,
                discontinuity: false,
            })
            .collect();
        drop(state);

        let playlist = p.playlist("test", "synthetic");
        assert!(playlist.contains("#EXT-X-START:TIME-OFFSET=-29.000,PRECISE=NO"));
    }

    #[test]
    fn playlist_and_valid_segment_reads_refresh_activity() {
        let p = pkg();
        feed_one_clean_segment(&p);
        let stale = Instant::now() - Duration::from_secs(60);

        p.state.lock().unwrap().last_access = Some(stale);
        let _playlist = p.playlist("test", "active");
        assert!(p
            .state
            .lock()
            .unwrap()
            .last_access
            .is_some_and(|at| at > stale));

        p.state.lock().unwrap().last_access = Some(stale);
        assert!(p.segment(0).is_some());
        assert!(p
            .state
            .lock()
            .unwrap()
            .last_access
            .is_some_and(|at| at > stale));
    }

    #[test]
    fn evicted_and_future_segment_probes_do_not_refresh_activity() {
        let p = pkg();
        feed_clean_run(&p, 0, 5);
        let stale = Instant::now() - Duration::from_secs(60);
        p.state.lock().unwrap().last_access = Some(stale);

        assert!(p.segment(1).is_none());
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));
        assert!(p.segment(99).is_none());
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));
    }

    #[test]
    fn hls_generation_disabled_mode_preserves_readiness_after_gap() {
        let p = clean_pkg(6);
        feed_one_clean_segment(&p);
        assert!(p.is_ready());

        p.discontinuity();

        assert!(
            p.is_ready(),
            "legacy disabled readiness remains based on retained segments"
        );
    }

    #[tokio::test]
    async fn hls_generation_retained_segments_do_not_satisfy_new_startup_count() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                segment_duration_ms: 1000,
                startup_segments: 2,
                ..HlsConfig::default()
            },
            None,
        );
        feed_clean_run(&p, 0, 2);
        assert_eq!(p.status(), HlsStatus::Ready);
        let retained = p.segment(0).expect("old segment retained");

        p.begin_generation(
            GenerationId(2),
            tokio::time::Instant::now() + Duration::from_secs(5),
            true,
        );

        assert_eq!(p.segment(0).as_deref(), Some(retained.as_ref()));
        assert_eq!(p.status(), HlsStatus::Recovering);
        assert_eq!(
            p.wait_available(Duration::from_millis(1)).await,
            HlsStatus::Recovering
        );
    }

    #[tokio::test]
    async fn hls_generation_requires_exact_new_segments_and_marks_only_first_discontinuous() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                segment_duration_ms: 1000,
                startup_segments: 2,
                ..HlsConfig::default()
            },
            None,
        );
        feed_clean_run(&p, 0, 2);
        assert_eq!(p.status(), HlsStatus::Ready);
        p.begin_generation(
            GenerationId(2),
            tokio::time::Instant::now() + Duration::from_secs(10),
            true,
        );
        feed_clean_run_generation(&p, GenerationId(2), 2_000_000, 1);
        assert_eq!(
            p.status(),
            HlsStatus::Recovering,
            "one new segment is below the strict threshold"
        );
        feed_clean_run_generation(&p, GenerationId(2), 4_000_000, 1);
        assert_eq!(p.status(), HlsStatus::Ready);
        let st = p.state.lock().unwrap();
        assert_eq!(
            st.segments
                .iter()
                .filter(|segment| segment.discontinuity)
                .count(),
            1
        );
        assert!(
            st.segments[2].discontinuity,
            "first segment of the new generation is discontinuous"
        );
        assert!(
            !st.segments[3].discontinuity,
            "later segments must not repeat the boundary"
        );
    }

    #[tokio::test]
    async fn strict_generation_opening_discontinuity_marker_does_not_reset_fresh_prefix() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 12,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..HlsConfig::default()
            },
            None,
        );
        feed_clean_run(&p, 0, 1);
        assert_eq!(p.status(), HlsStatus::Ready);
        let old = p.segment(0).expect("old generation segment");

        p.begin_generation(
            GenerationId(2),
            tokio::time::Instant::now() + Duration::from_secs(5),
            true,
        );
        let fresh_pat = pat(PMT_PID);
        let fresh_pmt = pmt(PMT_PID, VIDEO_PID);
        let marked_opening = pcr_packet(VIDEO_PID, 2_000_000, true, true, 17);
        p.feed_generation(GenerationId(2), &fresh_pat);
        p.feed_generation(GenerationId(2), &fresh_pmt);
        p.feed_generation(GenerationId(2), &marked_opening);
        p.feed_generation(GenerationId(2), &video_access_packet(VIDEO_PID, 2_108_000));

        assert_eq!(
            p.status(),
            HlsStatus::Ready,
            "marked opening is already fenced by generation 2"
        );
        assert_eq!(
            p.segment(0).as_deref(),
            Some(old.as_ref()),
            "old segment remains retrievable"
        );
        let replacement = p
            .segment(1)
            .expect("fresh prefix and marked opening form a segment");
        assert_eq!(&replacement[..TS_PACKET], fresh_pat.as_slice());
        assert_eq!(&replacement[TS_PACKET..TS_PACKET * 2], fresh_pmt.as_slice());
        let mut accepted_opening = marked_opening;
        accepted_opening[5] &= !0x80; // consume only the mux discontinuity framing bit
        assert_eq!(
            &replacement[TS_PACKET * 2..TS_PACKET * 3],
            accepted_opening.as_slice()
        );
        let st = p.state.lock().unwrap();
        assert_eq!(
            st.segments
                .iter()
                .filter(|segment| segment.discontinuity)
                .count(),
            1
        );
        assert!(
            st.segments[1].discontinuity,
            "only the first replacement boundary is marked"
        );
    }

    #[test]
    fn hls_generation_stale_feed_cannot_publish_segments() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..HlsConfig::default()
            },
            None,
        );
        p.begin_generation(
            GenerationId(8),
            tokio::time::Instant::now() + Duration::from_secs(5),
            false,
        );
        feed_clean_run_generation(&p, GenerationId(8), 0, 1);
        let before = p.segment(0).unwrap();
        feed_clean_run_generation(&p, GenerationId(7), 900_000, 3);
        assert_eq!(p.segment(0).as_deref(), Some(before.as_ref()));
        assert!(p.segment(1).is_none());
    }

    #[test]
    fn hls_generation_terminal_failure_cannot_be_resurrected() {
        let p = HlsPackager::new(HlsConfig::default(), None);
        p.begin_generation(
            GenerationId(4),
            tokio::time::Instant::now() + Duration::from_secs(5),
            false,
        );
        p.fail_generation(GenerationId(4), HlsFailure::ChildExit);
        p.begin_generation(
            GenerationId(5),
            tokio::time::Instant::now() + Duration::from_secs(5),
            true,
        );
        feed_clean_run_generation(&p, GenerationId(5), 0, 3);
        assert_eq!(p.status(), HlsStatus::Failed(HlsFailure::ChildExit));
        assert!(p.segment(0).is_none());
    }

    #[tokio::test]
    async fn hls_generation_deadline_fails_wait_and_is_terminal() {
        let p = HlsPackager::new(HlsConfig::default(), None);
        p.begin_generation(
            GenerationId(1),
            tokio::time::Instant::now() - Duration::from_millis(1),
            false,
        );
        assert_eq!(
            p.wait_available(Duration::from_secs(5)).await,
            HlsStatus::Failed(HlsFailure::Deadline)
        );
        p.begin_generation(
            GenerationId(2),
            tokio::time::Instant::now() + Duration::from_secs(5),
            true,
        );
        assert_eq!(p.status(), HlsStatus::Failed(HlsFailure::Deadline));
    }

    #[tokio::test]
    async fn hls_generation_boundary_snapshot_is_atomic_with_fence() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..HlsConfig::default()
            },
            None,
        );
        p.begin_generation(
            GenerationId(1),
            tokio::time::Instant::now() + Duration::from_secs(5),
            false,
        );
        feed_clean_run_generation(&p, GenerationId(1), 0, 1);
        let rendered = p.playlist_snapshot("/seg", false).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let left = p.clone();
        let left_barrier = barrier.clone();
        let fence = tokio::task::spawn_blocking(move || {
            left_barrier.wait();
            left.begin_generation(
                GenerationId(2),
                tokio::time::Instant::now() + Duration::from_secs(5),
                true,
            );
        });
        let right = p.clone();
        let right_barrier = barrier.clone();
        let snapshot = tokio::task::spawn_blocking(move || {
            right_barrier.wait();
            right.playlist_snapshot("/seg", false)
        });
        barrier.wait();
        fence.await.unwrap();
        let result = snapshot.await.unwrap();
        assert!(result == Err(HlsStatus::Recovering) || result == Ok(rendered));
        assert_eq!(
            p.playlist_snapshot("/seg", false),
            Err(HlsStatus::Recovering)
        );
    }

    #[tokio::test]
    async fn hls_generation_failed_and_stopped_wake_all_waiters() {
        for terminal in [HlsStatus::Failed(HlsFailure::ChildExit), HlsStatus::Stopped] {
            let p = HlsPackager::new(HlsConfig::default(), None);
            p.begin_generation(
                GenerationId(1),
                tokio::time::Instant::now() + Duration::from_secs(30),
                false,
            );
            let mut waiters = Vec::new();
            for _ in 0..3 {
                let p = p.clone();
                waiters.push(tokio::spawn(async move {
                    p.wait_available(Duration::from_secs(10)).await
                }));
            }
            tokio::task::yield_now().await;
            match terminal {
                HlsStatus::Stopped => p.stop(),
                HlsStatus::Failed(reason) => p.fail_generation(GenerationId(1), reason),
                _ => unreachable!(),
            }
            for waiter in waiters {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(1), waiter)
                        .await
                        .unwrap()
                        .unwrap(),
                    terminal
                );
            }
        }
    }

    #[test]
    fn hls_generation_playlist_snapshot_is_unavailable_after_boundary_fence() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..HlsConfig::default()
            },
            None,
        );
        p.begin_generation(
            GenerationId(4),
            tokio::time::Instant::now() + Duration::from_secs(5),
            false,
        );
        feed_clean_run_generation(&p, GenerationId(4), 0, 1);
        let ready = p.playlist_snapshot("/seg", false);
        assert!(ready.is_ok());
        let retained = p.segment(0).expect("completed segment remains retained");
        p.begin_generation(
            GenerationId(5),
            tokio::time::Instant::now() + Duration::from_secs(5),
            true,
        );
        assert_eq!(
            p.playlist_snapshot("/seg", false),
            Err(HlsStatus::Recovering)
        );
        assert_eq!(p.segment(0).as_deref(), Some(retained.as_ref()));
    }

    #[test]
    fn pcr_duration_cuts_before_random_access_packet_and_drives_extinf() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        for (pcr, key, marker) in [
            (0, true, 1),
            (45_000, false, 2),
            (108_000, true, 3),
            (153_000, false, 4),
            (216_000, true, 5),
        ] {
            p.feed(&pcr_packet(VIDEO_PID, pcr, key, false, marker));
        }

        assert_eq!(p.segment(0).unwrap().len(), 4 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap()[2 * TS_PACKET + 12], 3);
        let playlist = p.playlist("test", "timed");
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
        assert!(playlist.contains("#EXT-X-TARGETDURATION:2"));
    }

    #[test]
    fn default_ceiling_allows_a_timed_keyframe_beyond_256_packets() {
        let p = HlsPackager::new(HlsConfig::default(), None);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        let input: Vec<u8> = (0..=1300)
            .flat_map(|i| pcr_packet(VIDEO_PID, i * 360, matches!(i, 0 | 1300), false, i as u8))
            .collect();

        p.feed(&input);

        assert_eq!(p.segment(0).unwrap().len(), 1302 * TS_PACKET);
        assert!(p
            .playlist("test", "default-ceiling")
            .contains("#EXTINF:5.200,"));
    }

    #[test]
    fn pcr_mode_never_exceeds_the_hard_packet_ceiling() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 5,
                window_segments: 4,
                segment_duration_ms: 60_000,
                ..HlsConfig::default()
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 9_000, false, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 18_000, false, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 27_000, false, false, 3));

        assert!(p.segment(0).is_none());
        let st = p.state.lock().unwrap();
        assert!(st.cur.is_empty());
        assert!(st
            .segments
            .iter()
            .all(|segment| segment.bytes.len() <= p.max_segment_bytes));
    }

    #[test]
    fn hard_ceiling_preserves_post_discontinuity_boundary() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.discontinuity();
        feed_clean_run(&p, 90_000, 1);

        let first = p.segment(0).unwrap();
        assert_eq!(first.len(), 3 * TS_PACKET);
        assert_eq!(&first[..TS_PACKET], &pat(PMT_PID));
        assert_eq!(p.state.lock().unwrap().cur.len(), 3 * TS_PACKET);
        assert_eq!(
            p.playlist("test", "bounded-gap")
                .matches("#EXT-X-DISCONTINUITY")
                .count(),
            1
        );
    }

    #[test]
    fn random_access_packet_need_not_carry_the_pcr_it_is_aligned_against() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 108_000, false, false, 2));
        p.feed(&random_access_packet(3));
        p.feed(&pcr_packet(VIDEO_PID, 216_000, false, false, 4));
        p.feed(&random_access_packet(5));

        assert_eq!(p.segment(0).unwrap().len(), 4 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap()[2 * TS_PACKET + 6], 3);
        assert_eq!(
            p.playlist("test", "split-pcr")
                .matches("#EXTINF:1.200,")
                .count(),
            2
        );
    }

    #[test]
    fn pcr_wrap_is_measured_as_forward_time() {
        let p = timed_pkg(1000);
        let start = PCR_MODULUS - 45_000;
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, start, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 63_000, true, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 171_000, true, false, 3));

        let playlist = p.playlist("test", "wrap");
        assert!(playlist.contains("#EXTINF:1.200,"));
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
    }

    #[test]
    fn transport_discontinuity_discards_partial_segment_and_marks_next_one() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 45_000, false, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 900_000, true, true, 3));
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 900_000, true, false, 4));
        p.feed(&pcr_packet(VIDEO_PID, 1_008_000, true, false, 5));

        assert_eq!(p.segment(0).unwrap()[2 * TS_PACKET + 12], 4);
        let playlist = p.playlist("test", "disc");
        assert!(playlist.contains("#EXT-X-DISCONTINUITY\n#EXTINF:1.200,"));
    }

    #[test]
    fn packet_ceiling_drops_a_run_when_pcr_disappears() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 5,
                window_segments: 6,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&with_pid(packets(1), VIDEO_PID));
        p.feed(&with_pid(packets(1), VIDEO_PID));
        p.feed(&with_pid(packets(1), VIDEO_PID));

        assert!(p.segment(0).is_none());
        assert!(p.state.lock().unwrap().cur.is_empty());
    }

    #[test]
    fn unrelated_pcr_pid_does_not_corrupt_the_selected_clock() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, 100));
        p.feed(&pcr_packet(100, 0, true, false, 1));
        // A numerically unrelated clock on another program must not look like a backwards jump.
        p.feed(&pcr_packet(200, PCR_MODULUS - 1, false, false, 2));
        p.feed(&pcr_packet(100, 108_000, true, false, 3));
        p.feed(&pcr_packet(100, 216_000, true, false, 4));

        let playlist = p.playlist("test", "multiprogram");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
    }

    #[test]
    fn no_pcr_lookahead_boundary_keeps_the_selected_clock_pid() {
        let p = clean_pkg(4);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 108_000, false, false, 2));

        // This access point completes the full run using elapsed time from VIDEO_PID, but it
        // carries no PCR of its own. The next clean run must retain VIDEO_PID as its clock.
        p.feed(&random_access_packet(3));
        assert!(p.segment(0).is_some());

        // A backwards-looking PCR from another program must remain filtered. The selected video
        // clock can then advance to the next access point without a false discontinuity.
        p.feed(&pcr_packet(AUDIO_PID, 0, false, false, 4));
        p.feed(&pcr_packet(VIDEO_PID, 216_000, true, false, 5));

        let playlist = p.playlist("test", "lookahead-clock");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
        assert!(p.segment(1).is_some());
    }

    #[test]
    fn target_duration_never_decreases_as_segments_slide() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 100,
                window_segments: 1,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 180_000, true, false, 2));
        assert!(p
            .playlist("test", "target")
            .contains("#EXT-X-TARGETDURATION:2"));

        p.feed(&pcr_packet(VIDEO_PID, 270_000, true, false, 3));
        assert!(p
            .playlist("test", "target")
            .contains("#EXT-X-TARGETDURATION:2"));
    }

    #[test]
    fn fills_segments_and_slides_window() {
        let p = pkg();
        feed_clean_run(&p, 0, 5);
        let pl = p.playlist("test", "abc");
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:2"));
        assert!(pl.contains("/streams/test/abc/seg/2.ts"));
        assert!(pl.contains("/streams/test/abc/seg/4.ts"));
        assert!(!pl.contains("seg/1.ts"));
        // Evicted + future segments are gone; retained ones are PAT + PMT + access point.
        assert!(p.segment(1).is_none());
        assert_eq!(p.segment(3).unwrap().len(), 3 * TS_PACKET);
        assert!(p.segment(99).is_none());
    }

    #[test]
    fn alternate_playlist_prefix_reuses_native_sequence_and_discontinuity_state() {
        let p = pkg();
        feed_one_clean_segment(&p);
        p.discontinuity();
        feed_clean_run(&p, 1_000_000, 1);

        let native = p.playlist("test", "abc");
        let compat = p.compatibility_playlist_with_segment_prefix("/ace/c/client-token");

        assert!(native.contains("/streams/test/abc/seg/0.ts"));
        assert!(compat.contains("/ace/c/client-token/0.ts"));
        assert!(compat.contains("/ace/c/client-token/1.ts"));
        let native_metadata: Vec<_> = native
            .lines()
            .filter(|line| line.starts_with('#'))
            .collect();
        let compat_metadata: Vec<_> = compat
            .lines()
            .filter(|line| line.starts_with('#'))
            .collect();
        assert_eq!(
            native_metadata, compat_metadata,
            "changing the URI prefix must preserve every playlist metadata, sequence, duration, and discontinuity line"
        );
    }

    #[test]
    fn partial_packets_buffer_until_full() {
        let p = pkg();
        let stream = [
            pat(PMT_PID),
            pmt(PMT_PID, VIDEO_PID),
            video_access_packet(VIDEO_PID, 0),
            video_access_packet(VIDEO_PID, 108_000),
        ]
        .concat();
        p.feed(&stream[..stream.len() - 1]);
        assert!(p.playlist("test", "x").lines().all(|l| !l.contains("seg/")));
        p.feed(&stream[stream.len() - 1..]);
        assert!(p.playlist("test", "x").contains("seg/0.ts"));
    }

    #[test]
    fn receiver_lag_discards_partial_media_and_marks_only_first_post_gap_segment() {
        let p = pkg();
        let pre_gap = vec![0x11; TS_PACKET];
        p.feed(&pre_gap);
        p.receiver_lag();
        feed_clean_run(&p, 0, 2);

        assert_eq!(p.segment(0).unwrap().len(), 3 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap().len(), 3 * TS_PACKET);
        let playlist = p.playlist("test", "gap");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 1);
        assert!(
            playlist.contains("#EXT-X-DISCONTINUITY\n#EXTINF:1.200,\n/streams/test/gap/seg/0.ts")
        );
    }

    #[test]
    fn discontinuity_metadata_slides_with_its_segment() {
        let p = pkg();
        feed_one_clean_segment(&p);
        p.discontinuity();
        feed_clean_run(&p, 1_000_000, 3);
        let retained = p.playlist("test", "slide");
        assert!(retained.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert_eq!(retained.matches("#EXT-X-DISCONTINUITY").count(), 1);

        for i in 4..=6 {
            p.feed(&video_access_packet(VIDEO_PID, 1_000_000 + i * 108_000));
        }
        let evicted = p.playlist("test", "slide");
        assert!(evicted.contains("#EXT-X-MEDIA-SEQUENCE:4"));
        assert!(evicted.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1"));
        assert_eq!(evicted.matches("#EXT-X-DISCONTINUITY\n").count(), 0);
    }

    #[test]
    fn repeated_gaps_keep_distinct_markers_and_monotonic_sequences() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 5,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            None,
        );
        feed_clean_run(&p, 0, 1);
        p.discontinuity();
        feed_clean_run(&p, 1_000_000, 1);
        p.discontinuity();
        feed_clean_run(&p, 2_000_000, 1);

        let playlist = p.playlist("test", "repeat");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 2);
        assert!(playlist.contains("seg/0.ts"));
        assert!(playlist.contains("seg/1.ts"));
        assert!(playlist.contains("seg/2.ts"));
        assert_eq!(p.segment(0).unwrap().len(), 3 * TS_PACKET);
        assert_eq!(p.segment(2).unwrap().len(), 3 * TS_PACKET);
    }

    #[test]
    fn configured_hls_settings_control_playlist_and_segments() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 2,
                segment_duration_ms: 2500,
                ..HlsConfig::default()
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        for i in 1..=3 {
            p.feed(&video_access_packet(VIDEO_PID, i * 270_000));
        }

        let pl = p.playlist("test", "abc");

        assert!(pl.contains("#EXT-X-TARGETDURATION:3"));
        assert!(pl.contains("#EXTINF:3.000,"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert!(p.segment(0).is_none());
        assert_eq!(p.segment(2).unwrap().len(), 3 * TS_PACKET);
    }

    fn vod_config() -> HlsConfig {
        // Minimum 3 packets/segment => 564-byte segments, 2.0s nominal per full segment.
        HlsConfig {
            segment_packets: 3,
            window_segments: 6,
            segment_duration_ms: 2000,
            ..HlsConfig::default()
        }
    }

    #[test]
    fn vod_segment_count_is_ceil_division_of_total_over_segment_bytes() {
        // 564-byte segments.
        assert_eq!(VodHlsLayout::new(0, vod_config()).segment_count(), 0);
        assert_eq!(VodHlsLayout::new(1, vod_config()).segment_count(), 1);
        assert_eq!(VodHlsLayout::new(564, vod_config()).segment_count(), 1);
        assert_eq!(VodHlsLayout::new(565, vod_config()).segment_count(), 2);
        assert_eq!(VodHlsLayout::new(564 * 3, vod_config()).segment_count(), 3);
    }

    #[test]
    fn vod_segment_range_is_aligned_with_a_clamped_final_segment() {
        // 1500 bytes over 564-byte segments => [0,564), [564,1128), [1128,1500).
        let layout = VodHlsLayout::new(1500, vod_config());
        assert_eq!(layout.segment_range(0), Some((0, 563)));
        assert_eq!(layout.segment_range(1), Some((564, 1127)));
        assert_eq!(layout.segment_range(2), Some((1128, 1499))); // clamped, shorter
        assert_eq!(layout.segment_range(3), None); // past the last segment
    }

    #[test]
    fn vod_playlist_is_a_terminated_vod_list_of_every_segment() {
        // 1500 bytes => 3 segments; last is 372/564 of a full 2.0s segment.
        let pl = VodHlsLayout::new(1500, vod_config()).playlist("memvod", "abc");
        assert!(pl.contains("#EXT-X-PLAYLIST-TYPE:VOD"));
        assert!(pl.contains("#EXT-X-TARGETDURATION:2"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:0"));
        assert!(pl.contains("/vod/memvod/abc/seg/0.ts"));
        assert!(pl.contains("/vod/memvod/abc/seg/2.ts"));
        assert!(!pl.contains("/vod/memvod/abc/seg/3.ts"));
        assert!(pl.trim_end().ends_with("#EXT-X-ENDLIST"));
        // Full segments advertise the nominal 2.0s; the trailing partial is scaled by bytes.
        assert!(pl.contains("#EXTINF:2.000,"));
        assert!(pl.contains(&format!("#EXTINF:{:.3},", 2.0 * 372.0 / 564.0)));
    }

    #[test]
    fn minimum_segment_packets_is_preserved_at_construction() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 3,
                window_segments: 2,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            None,
        );

        assert_eq!(p.max_segment_bytes, 3 * TS_PACKET);
    }

    #[test]
    fn segments_replace_upstream_sdt_with_titled_sdt() {
        use ace_media::mpegts::read_sdt_service_name;
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 100,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
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
            .filter(|c| {
                c.len() == TS_PACKET && ((((c[1] & 0x1f) as u16) << 8) | c[2] as u16) == 0x0011
            })
            .count();
        assert_eq!(sdt_packets, 1);
    }

    #[test]
    fn strict_titled_sdt_matches_output_pat_identity_and_suppresses_source_sdt() {
        fn pat_with_identity(tsid: u16, program: u16, pmt_pid: u16) -> Vec<u8> {
            let mut section = vec![
                0x00,
                0xb0,
                13,
                (tsid >> 8) as u8,
                tsid as u8,
                0xc1,
                0,
                0,
                (program >> 8) as u8,
                program as u8,
                0xe0 | ((pmt_pid >> 8) as u8 & 0x1f),
                pmt_pid as u8,
            ];
            section.extend_from_slice(&[0; 4]);
            psi(0, &section)
        }

        fn pmt_with_identity(program: u16, pid: u16) -> Vec<u8> {
            let mut section = vec![
                0x02,
                0xb0,
                18,
                (program >> 8) as u8,
                program as u8,
                0xc1,
                0,
                0,
                0xe0 | ((pid >> 8) as u8 & 0x1f),
                pid as u8,
                0xf0,
                0,
                0x1b,
                0xe0 | ((pid >> 8) as u8 & 0x1f),
                pid as u8,
                0xf0,
                0,
            ];
            section.extend_from_slice(&[0; 4]);
            psi(PMT_PID, &section)
        }

        let tsid = 7;
        let program = 9;
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 100,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            Some("Synthetic Service".to_string()),
        );
        p.begin_generation(
            GenerationId(1),
            tokio::time::Instant::now() + Duration::from_secs(5),
            false,
        );
        p.feed_generation(GenerationId(1), &pat_with_identity(tsid, program, PMT_PID));
        p.feed_generation(GenerationId(1), &pmt_with_identity(program, VIDEO_PID));
        p.feed_generation(GenerationId(1), &with_pid(packets(1), 0x0011)); // competing source SDT
        p.feed_generation(GenerationId(1), &video_access_packet(VIDEO_PID, 0));
        p.feed_generation(GenerationId(1), &video_access_packet(VIDEO_PID, 108_000));

        let segment = p
            .segment(0)
            .expect("strict titled generation emitted a segment");
        let (packets, remainder) = segment.as_chunks::<TS_PACKET>();
        assert!(remainder.is_empty(), "emitted TS segment is packet aligned");
        let pat_packet = packets
            .iter()
            .find(|packet| ace_media::mpegts::ts_pid(*packet) == 0)
            .expect("output PAT is present");
        let pat_section = &pat_packet[5..];
        assert_eq!(u16::from_be_bytes([pat_section[3], pat_section[4]]), tsid);
        assert_eq!(
            u16::from_be_bytes([pat_section[8], pat_section[9]]),
            program
        );

        let sdt_packets: Vec<_> = packets
            .iter()
            .filter(|packet| ace_media::mpegts::ts_pid(*packet) == 0x0011)
            .collect();
        assert_eq!(
            sdt_packets.len(),
            1,
            "source SDT is suppressed; only synthesized SDT remains"
        );
        let sdt_section = &sdt_packets[0][5..];
        assert_eq!(sdt_section[0], 0x42);
        assert_eq!(u16::from_be_bytes([sdt_section[3], sdt_section[4]]), tsid);
        assert_eq!(
            u16::from_be_bytes([sdt_section[11], sdt_section[12]]),
            program
        );
        assert_eq!(
            ace_media::mpegts::read_sdt_service_name(&segment).as_deref(),
            Some("Synthetic Service"),
        );
    }

    #[test]
    fn ceiling_lookahead_drops_upstream_sdt_on_titled_stream() {
        use ace_media::mpegts::read_sdt_service_name;
        // segment_packets: 4 -> the titled prefix [PAT][PMT][SDT] plus the opening keyframe fill
        // the ceiling exactly, so subsequent packets are classified by `scan_lookahead`. An
        // upstream SDT that reaches the lookahead must be dropped there: without the filter it
        // would discard the whole ceiling-full run (an SDT is not a keyframe and carries no PCR),
        // and segment 0 would never emit.
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 4,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            Some("Ceiling Title".to_string()),
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0)); // fills ceiling with [PAT][PMT][SDT][keyframe]
        p.feed(&with_pid(packets(1), 0x0011)); // upstream SDT -> arrives in lookahead, filtered
        p.feed(&video_access_packet(VIDEO_PID, 108_000)); // 1.2s elapsed -> emits ceiling segment
        let seg = p
            .segment(0)
            .expect("the ceiling-full segment emits, not discarded by the upstream SDT");
        assert_eq!(seg.len(), 4 * TS_PACKET);
        // The injected SDT carries the title, and it is the only SDT-PID packet in the segment.
        assert_eq!(
            read_sdt_service_name(&seg).as_deref(),
            Some("Ceiling Title")
        );
        let sdt_packets = seg
            .chunks(TS_PACKET)
            .filter(|c| {
                c.len() == TS_PACKET && ((((c[1] & 0x1f) as u16) << 8) | c[2] as u16) == 0x0011
            })
            .count();
        assert_eq!(sdt_packets, 1);
    }

    #[test]
    fn titled_stream_below_sdt_ceiling_serves_without_stalling() {
        // segment_packets=3 can't fit [PAT][PMT][SDT] + keyframe; must fall back to untitled and
        // still emit segments (no permanent stall), just without the injected title SDT.
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 3,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            Some("Too Big".to_string()),
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&video_access_packet(VIDEO_PID, 108_000));
        let seg = p
            .segment(0)
            .expect("a segment should still emit at a sub-SDT ceiling");
        assert!(ace_media::mpegts::read_sdt_service_name(&seg).is_none());
    }

    #[test]
    fn normal_hls_mpeg2_start_and_target_cut_retain_original_pes_packets() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 20,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            None,
        );
        let pat_packet = pat(PMT_PID);
        let pmt_packet = pmt_codec_mpeg2(PMT_PID, VIDEO_PID);
        let first = mpeg2_pes_packet(0, 1);
        let between = pcr_packet(VIDEO_PID, 108_000, false, false, 0);
        let second = mpeg2_pes_packet(180_000, 1);
        let third = mpeg2_pes_packet(270_000, 1);
        let stream = [
            pat_packet.clone(),
            pmt_packet.clone(),
            first.clone(),
            between.clone(),
            second.clone(),
            third,
        ]
        .concat();
        for chunk in stream.chunks(73) {
            p.feed(chunk);
        }

        assert_eq!(
            p.segment(0).unwrap().as_ref(),
            [pat_packet, pmt_packet, first, between].concat()
        );
        let next_segment = p.segment(1).unwrap();
        assert_eq!(&next_segment[2 * TS_PACKET..], second.as_slice());

        // Reset requires fresh tables but keeps normal HLS's MPEG-2 opt-in active.
        p.discontinuity();
        let reset_pat = pat(PMT_PID);
        let reset_pmt = pmt_codec_mpeg2(PMT_PID, VIDEO_PID);
        let reset_i = mpeg2_pes_packet(300_000, 1);
        let next_i = mpeg2_pes_packet(408_000, 1);
        p.feed(&reset_pat);
        p.feed(&reset_pmt);
        p.feed(&reset_i);
        p.feed(&next_i);
        assert_eq!(
            p.segment(2).unwrap().as_ref(),
            [reset_pat, reset_pmt, reset_i].concat()
        );
    }

    #[test]
    fn normal_hls_mpeg2_ceiling_lookahead_cuts_on_i_picture() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 4,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            Some("Fits exactly".into()),
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt_codec_mpeg2(PMT_PID, VIDEO_PID));
        let first = mpeg2_pes_packet(0, 1);
        let second = mpeg2_pes_packet(180_000, 1);
        let third = mpeg2_pes_packet(270_000, 1);
        p.feed(&first); // PAT + PMT + SDT + I fills the configured ceiling
        p.feed(&pcr_packet(VIDEO_PID, 108_000, false, false, 0)); // classified in lookahead
        p.feed(&second); // I in lookahead emits the ceiling-full segment
        let segment = p
            .segment(0)
            .expect("I picture in lookahead closes the full segment");
        assert_eq!(segment.len(), 4 * TS_PACKET);
        assert_eq!(&segment[3 * TS_PACKET..], first.as_slice());
        p.feed(&third);
        let next_segment = p.segment(1).unwrap();
        assert!(next_segment.ends_with(&second));
        assert_eq!(
            ace_media::mpegts::read_sdt_service_name(&segment).as_deref(),
            Some("Fits exactly")
        );
    }

    #[test]
    fn normal_hls_mpeg2_non_i_and_split_pes_cannot_cut_at_target() {
        let p = clean_pkg(20);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt_codec_mpeg2(PMT_PID, VIDEO_PID));
        let first = mpeg2_pes_packet(0, 1);
        let b3 = mpeg2_b3_packet(108_000);
        let p_picture = mpeg2_pes_packet(180_000, 2);
        let b_picture = mpeg2_pes_packet(216_000, 3);
        let next_i = mpeg2_pes_packet(270_000, 1);
        p.feed(&first);
        p.feed(&b3);
        p.feed(&p_picture);
        p.feed(&b_picture);
        assert!(p.segment(0).is_none(), "B3/P/B must not trigger target cut");
        p.feed(&next_i);
        let segment = p.segment(0).expect("next complete I closes the segment");
        assert!(segment.ends_with(&b_picture));
        p.feed(&mpeg2_pes_packet(360_000, 1));
        assert!(p.segment(1).unwrap().ends_with(&next_i));

        let split = clean_pkg(20);
        split.feed(&pat(PMT_PID));
        split.feed(&pmt_codec_mpeg2(PMT_PID, VIDEO_PID));
        let pes = [
            0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 0, 0, 1, 0, 0, 8, 0x55, 0x66,
        ];
        let fragment = |bytes: &[u8], pusi: bool| {
            let mut packet = vec![0xff; TS_PACKET];
            packet[0] = 0x47;
            packet[1] = ((VIDEO_PID >> 8) as u8 & 0x1f) | if pusi { 0x40 } else { 0 };
            packet[2] = VIDEO_PID as u8;
            packet[3] = 0x30;
            packet[4] = (TS_PACKET - 5 - bytes.len()) as u8;
            packet[5] = 0;
            let start = 5 + packet[4] as usize;
            packet[start..start + bytes.len()].copy_from_slice(bytes);
            packet
        };
        split.feed(&fragment(&pes[..15], true));
        split.feed(&fragment(&pes[15..], false));
        assert!(
            split.segment(0).is_none(),
            "split PES picture header is rejected"
        );
        split.feed(&mpeg2_pes_packet(108_000, 1));
        split.feed(&mpeg2_pes_packet(216_000, 1));
        assert!(
            split.segment(0).is_some(),
            "a fresh complete I can start HLS"
        );
    }

    #[test]
    fn malformed_pes_timestamps_cannot_start_or_cut_normal_hls() {
        let p = clean_pkg(20);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt_codec_mpeg2(PMT_PID, VIDEO_PID));

        let mut malformed_start = mpeg2_pes_packet(0, 1);
        // PTS is declared, but optional-header data length is zero.
        malformed_start[19] = 0x80;
        p.feed(&malformed_start);
        assert!(p.segment(0).is_none(), "missing PTS cannot start a segment");

        p.feed(&mpeg2_pes_packet(90_000, 1));
        p.feed(&mpeg2_b3_packet(108_000));
        let mut malformed_cut = mpeg2_pes_packet(180_000, 1);
        // Forbidden PTS_DTS_flags = 01 must not provide cut evidence.
        malformed_cut[19] = 0x40;
        p.feed(&malformed_cut);
        assert!(p.segment(0).is_none(), "malformed PES cannot cut a segment");

        p.feed(&mpeg2_pes_packet(270_000, 1));
        assert!(
            p.segment(0).is_some(),
            "a later valid I can start and cut HLS"
        );
    }

    #[test]
    fn strict_mpeg2_hls_title_fallback_and_non_i_rejection() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 3,
                window_segments: 4,
                segment_duration_ms: 1000,
                ..HlsConfig::default()
            },
            Some("Synthetic title".into()),
        );
        p.begin_generation(
            GenerationId(1),
            tokio::time::Instant::now() + Duration::from_secs(5),
            false,
        );
        p.feed_generation(GenerationId(1), &pat(PMT_PID));
        p.feed_generation(GenerationId(1), &pmt_codec_mpeg2(PMT_PID, VIDEO_PID));
        p.feed_generation(GenerationId(1), &mpeg2_pes_packet(0, 2));
        p.feed_generation(GenerationId(1), &mpeg2_pes_packet(108_000, 3));
        assert!(
            p.segment(0).is_none(),
            "P/B pictures cannot start or cut segments"
        );
        let i = mpeg2_pes_packet(216_000, 1);
        p.feed_generation(GenerationId(1), &i);
        p.feed_generation(GenerationId(1), &mpeg2_pes_packet(324_000, 1));
        let seg = p
            .segment(0)
            .expect("small-title fallback still permits MPEG-2 start");
        assert!(ace_media::mpegts::read_sdt_service_name(&seg).is_none());
        assert!(seg.ends_with(&i));
    }

    fn pmt_codec_mpeg2(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
        let section_length = 5 + 4 + 5 + 4;
        let mut section = vec![0x02, 0xb0, section_length as u8, 0, 1, 0xc1, 0, 0];
        section.extend_from_slice(&[
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
            0x02,
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
        ]);
        section.extend_from_slice(&[0; 4]);
        psi(pmt_pid, &section)
    }
    fn feed_clean_run_generation(
        p: &HlsPackager,
        generation: GenerationId,
        start_pcr: u64,
        count: usize,
    ) {
        for packet in [
            pat(PMT_PID),
            pmt(PMT_PID, VIDEO_PID),
            video_access_packet(VIDEO_PID, start_pcr),
        ] {
            p.feed_generation(generation, &packet);
        }
        for i in 1..=count {
            p.feed_generation(
                generation,
                &video_access_packet(VIDEO_PID, start_pcr + i as u64 * 108_000),
            );
        }
    }
}
