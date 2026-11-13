//! Bounded live-HLS audio classification, normalization, and generation recovery.

pub(crate) mod live_source;
pub mod process;

pub use process::{
    build_command, run_child, BudgetedInput, ChildAdmission, InputBudget, PreparedNormalizer,
};

use crate::config::HlsConfig;
use crate::hls::{GenerationId, HlsFailure, HlsPackager, HlsStatus};
use crate::manager::{CleanupTracker, WorkerControl};
use crate::session::{StreamEvent, StreamSession};
use ace_media::audio::{AudioCodec, LayoutDecision, LiveAudioSelection, TransportLayout};
use ace_media::mpegts::{ts_timing, VideoAccessPointState};
use futures::FutureExt;
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Instant;

#[cfg(test)]
static PANIC_NEXT_PRODUCE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Constant-space selected MPEG ES validator. PES headers are bounded by their
/// one-byte optional-header length; frame boundaries are computed, never searched.
/// Two consecutive headers establish evidence before child admission. The same
/// state continues checking all subsequent frames while the generation runs.
pub struct MpegAudioProbe {
    pid: u16,
    pes: [u8; 9],
    pes_used: usize,
    optional: usize,
    in_pes: bool,
    payload_left: Option<usize>,
    header: [u8; 4],
    header_used: usize,
    frame_left: usize,
    headers: u8,
    signature: Option<(u8, u8, u8)>,
    failed: bool,
}
impl MpegAudioProbe {
    pub fn new(pid: u16) -> Self {
        Self {
            pid,
            pes: [0; 9],
            pes_used: 0,
            optional: 0,
            in_pes: false,
            payload_left: None,
            header: [0; 4],
            header_used: 0,
            frame_left: 0,
            headers: 0,
            signature: None,
            failed: false,
        }
    }
    pub fn observe(&mut self, packet: &[u8; 188]) -> Result<bool, HlsFailure> {
        if self.failed {
            return Err(HlsFailure::Unsupported);
        }
        let result = self.observe_inner(packet);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn observe_inner(&mut self, packet: &[u8; 188]) -> Result<bool, HlsFailure> {
        if ace_media::mpegts::ts_pid(packet) != self.pid {
            return Ok(self.headers >= 2);
        }
        if !validate_packet(packet)? || packet[3] & 0x10 == 0 {
            return Ok(self.headers >= 2);
        }
        let offset = if packet[3] & 0x20 != 0 {
            5 + usize::from(packet[4])
        } else {
            4
        };
        if packet[1] & 0x40 != 0 {
            if self.in_pes
                && (self.pes_used < 9
                    || self.optional != 0
                    || self.payload_left.is_some_and(|n| n != 0))
            {
                return Err(HlsFailure::Unsupported);
            }
            self.in_pes = true;
            self.pes_used = 0;
            self.optional = 0;
            self.payload_left = None;
        }
        if !self.in_pes {
            return Ok(false);
        }
        for &byte in &packet[offset..] {
            if self.pes_used < 9 {
                self.pes[self.pes_used] = byte;
                self.pes_used += 1;
                if self.pes_used == 9 {
                    if self.pes[..3] != [0, 0, 1]
                        || !(0xc0..=0xdf).contains(&self.pes[3])
                        || self.pes[6] & 0xc0 != 0x80
                        || self.pes[6] & 0x30 != 0
                        || self.pes[7] & 0xc0 == 0x40
                    {
                        return Err(HlsFailure::Unsupported);
                    }
                    self.optional = usize::from(self.pes[8]);
                    let length = usize::from(u16::from_be_bytes([self.pes[4], self.pes[5]]));
                    if length != 0 {
                        self.payload_left = Some(
                            length
                                .checked_sub(3 + self.optional)
                                .ok_or(HlsFailure::Unsupported)?,
                        );
                    }
                }
            } else if self.optional != 0 {
                self.optional -= 1;
            } else if self.payload_left == Some(0) {
                if byte != 0xff {
                    return Err(HlsFailure::Unsupported);
                }
            } else {
                if let Some(left) = &mut self.payload_left {
                    *left -= 1;
                }
                if self.frame_left != 0 {
                    self.frame_left -= 1;
                    continue;
                }
                self.header[self.header_used] = byte;
                self.header_used += 1;
                if self.header_used == 4 {
                    let (size, signature) = mpeg_frame_size(self.header)?;
                    if self.signature.is_some_and(|previous| previous != signature) {
                        return Err(HlsFailure::Unsupported);
                    }
                    self.signature = Some(signature);
                    self.headers = self.headers.saturating_add(1).min(2);
                    self.header_used = 0;
                    self.frame_left = size - 4;
                }
            }
        }
        Ok(self.headers >= 2)
    }
}

fn mpeg_frame_size(h: [u8; 4]) -> Result<(usize, (u8, u8, u8)), HlsFailure> {
    let version = (h[1] >> 3) & 3;
    let layer = (h[1] >> 1) & 3;
    let bitrate_index = usize::from(h[2] >> 4);
    let rate_index = (h[2] >> 2) & 3;
    if h[0] != 0xff
        || h[1] & 0xe0 != 0xe0
        || version == 1
        || !matches!(layer, 1 | 2)
        || !(1..15).contains(&bitrate_index)
        || rate_index == 3
        || h[3] & 3 == 2
    {
        return Err(HlsFailure::Unsupported);
    }
    const MPEG1_L2: [usize; 14] = [
        32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
    ];
    const MPEG1_L3: [usize; 14] = [
        32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
    ];
    const MPEG2: [usize; 14] = [8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160];
    let kbps = if version != 3 {
        MPEG2[bitrate_index - 1]
    } else if layer == 2 {
        MPEG1_L2[bitrate_index - 1]
    } else {
        MPEG1_L3[bitrate_index - 1]
    };
    let rate = [44100, 48000, 32000][usize::from(rate_index)]
        >> match version {
            3 => 0,
            2 => 1,
            _ => 2,
        };
    let coefficient = if layer == 1 && version != 3 {
        72000
    } else {
        144000
    };
    let size = coefficient * kbps / rate + usize::from((h[2] >> 1) & 1);
    Ok((size, (version, layer, rate_index)))
}

pub struct ClassifiedOpening {
    pub layout: TransportLayout,
    pub tables: Vec<[u8; 188]>,
    pub packet: [u8; 188],
}

/// Pre-opening payload is discarded as it is scanned, so this bounds evidence, not
/// memory. Opening needs fresh PAT/PMT and then the next video access point, which
/// can be a whole GOP away: a 2 s GOP at 8 Mbit/s is about 2 MB, so the 1 MiB input
/// budget is far too small here. 16 MiB covers multi-second GOPs at high bitrates
/// while still failing fast on a source that never opens; the startup deadline
/// bounds the wait in time.
pub(crate) const CLASSIFY_SCAN_BYTES: usize = 16 * 1024 * 1024;

/// Pre-opening payload can never be replayed. Count it toward the scan ceiling,
/// discard it immediately, and retain only the strict selector's two fresh tables.
/// This avoids allocating a history whose sole eventual operation would be drop.
pub struct Classifier {
    selector: LiveAudioSelection,
    access: VideoAccessPointState,
    scanned: usize,
    mpeg: Option<MpegAudioProbe>,
    mpeg_layout: Option<TransportLayout>,
}
impl Default for Classifier {
    fn default() -> Self {
        let mut access = VideoAccessPointState::new();
        access.enable_mpeg2();
        Self {
            selector: LiveAudioSelection::default(),
            access,
            scanned: 0,
            mpeg: None,
            mpeg_layout: None,
        }
    }
}
impl Classifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, packet: [u8; 188]) -> Result<Option<ClassifiedOpening>, HlsFailure> {
        self.scanned += 188;
        if self.scanned > CLASSIFY_SCAN_BYTES {
            return Err(HlsFailure::InputOverflow);
        }
        if !validate_packet(&packet)? {
            return Ok(None);
        }
        self.selector.observe(packet);
        match self.selector.layout() {
            LayoutDecision::Unsupported => Err(HlsFailure::Unsupported),
            LayoutDecision::Pending => {
                self.access.reset();
                Ok(None)
            }
            LayoutDecision::Supported(layout) => {
                if self.mpeg_layout.as_ref() != Some(&layout) {
                    self.mpeg = layout
                        .audio
                        .as_ref()
                        .filter(|a| a.codec == AudioCodec::MpegAudio)
                        .map(|a| MpegAudioProbe::new(a.pid));
                    self.mpeg_layout = Some(layout.clone());
                }
                if let Some(probe) = &mut self.mpeg {
                    if !probe.observe(&packet)? {
                        return Ok(None);
                    }
                }
                // Only validated matching PSI enters the access detector; malformed
                // tables cannot overwrite its last-good state independently.
                let tables: Vec<_> = self.selector.tables().copied().collect();
                for table in &tables {
                    self.access.observe(table);
                }
                if ace_media::mpegts::ts_pid(&packet) == layout.video_pid
                    && self.access.observe(&packet)
                    && tables.len() == 2
                {
                    Ok(Some(ClassifiedOpening {
                        layout,
                        tables,
                        packet,
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// A transport-error packet is damaged evidence, not a new unsupported layout.
/// Discard it before markers, table observation or native/child publication.
fn validate_packet(packet: &[u8; 188]) -> Result<bool, HlsFailure> {
    if packet[0] != 0x47 || packet[3] & 0xc0 != 0 {
        return Err(HlsFailure::Unsupported);
    }
    if packet[1] & 0x80 != 0 {
        return Ok(false);
    }
    if packet[3] & 0x30 == 0
        || (packet[3] & 0x20 != 0
            && (packet[4] > 183 || (packet[3] & 0x10 != 0 && packet[4] > 182)))
    {
        Err(HlsFailure::Unsupported)
    } else {
        Ok(true)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryReason {
    Source,
    Lag,
    Layout,
    Transport,
}

/// A generation that has served this long before its recovery was playing, not
/// churning: upstream reconnects on a flaky source end such generations every
/// 15-30 s. Its recovery does not count toward the three-per-minute limit, which
/// still stops generations that fail again soon after opening.
const STABLE_GENERATION: Duration = Duration::from_secs(10);

pub struct RecoveryWindow {
    invalidations: VecDeque<Instant>,
    generation: GenerationId,
    paired_marker: bool,
    opened_at: Option<Instant>,
    last_counted: bool,
}
impl Default for RecoveryWindow {
    fn default() -> Self {
        Self {
            invalidations: VecDeque::new(),
            generation: GenerationId(1),
            paired_marker: false,
            opened_at: None,
            last_counted: false,
        }
    }
}
impl RecoveryWindow {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn invalidate(
        &mut self,
        now: Instant,
        reason: RecoveryReason,
    ) -> Result<GenerationId, HlsFailure> {
        if reason == RecoveryReason::Transport && self.paired_marker {
            return Ok(self.generation);
        }
        while self
            .invalidations
            .front()
            .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
        {
            self.invalidations.pop_front();
        }
        let stable = self
            .opened_at
            .take()
            .is_some_and(|opened| now.duration_since(opened) >= STABLE_GENERATION);
        if !stable {
            if self.invalidations.len() == 3 {
                return Err(HlsFailure::RecoveryLimit);
            }
            self.invalidations.push_back(now);
        }
        self.last_counted = !stable;
        self.generation.0 += 1;
        self.paired_marker = reason == RecoveryReason::Source;
        Ok(self.generation)
    }
    pub fn opening(&mut self, now: Instant) {
        self.paired_marker = false;
        self.opened_at = Some(now);
    }
    /// Whether the latest admitted recovery counted toward the limit.
    pub fn last_counted(&self) -> bool {
        self.last_counted
    }
    /// Recoveries currently counted inside the rolling window.
    pub fn counted(&self) -> usize {
        self.invalidations.len()
    }
    pub fn marker_is_paired(&self) -> bool {
        self.paired_marker
    }
}

/// Creation does no I/O. The tracker owns the only supervisor and its cleanup.
pub(crate) fn spawn_worker(
    session: &StreamSession,
    media: Arc<HlsPackager>,
    config: HlsConfig,
    prepared: Option<PreparedNormalizer>,
    tracker: &CleanupTracker,
) -> WorkerControl {
    let rx = session.raw_receiver();
    let source_state = session.playback_state();
    let deadline = Instant::now() + Duration::from_millis(config.startup_timeout_ms);
    tracker.spawn(move |cancel| async move {
        match prepared {
            None => media.run_native(rx, cancel, source_state).await,
            Some(prepared) => {
                let mut worker = EnabledWorker {
                    media: media.clone(),
                    prepared,
                    source: PacketSource::new(rx),
                    recovery: RecoveryWindow::new(),
                    config,
                    deadline,
                    seed: None,
                    budget: InputBudget::new(),
                };
                match worker.run(cancel).await {
                    Err(reason) => {
                        crate::alog!(
                            "[audio] hls normalization failed: reason={} generation={}",
                            reason.code(),
                            worker.recovery.generation.0
                        );
                        media.fail_generation(worker.recovery.generation, reason)
                    }
                    Ok(WorkerEnd::Eof) => media.finish(),
                    Ok(WorkerEnd::Cancelled) => media.stop(),
                }
            }
        }
    })
}

enum SourceItem {
    Packet([u8; 188]),
    Recovery(RecoveryReason),
    Failed(HlsFailure),
    Closed,
}
enum PacketReader {
    Broadcast(broadcast::Receiver<StreamEvent>),
    Direct {
        source: Option<Box<dyn crate::provider::TsSource>>,
        stats: Arc<std::sync::Mutex<crate::provider::SourceStats>>,
    },
}
/// Views up to this size are detached into the input budget, so a small view never
/// pins a large owner. Larger views are consumed in place: they already cost at
/// least their own length, and their size is the source's piece length, which can
/// exceed the whole budget. Half the budget leaves room for replay and stdin queue.
const DETACH_MAX_BYTES: usize = process::INPUT_BYTES / 2;

enum SourceChunk {
    Detached(BudgetedInput),
    InPlace(bytes::Bytes),
}
impl SourceChunk {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Detached(chunk) => chunk.as_slice(),
            Self::InPlace(chunk) => chunk,
        }
    }
    fn len(&self) -> usize {
        self.as_slice().len()
    }
}

struct PacketSource {
    rx: PacketReader,
    chunk: Option<SourceChunk>,
    offset: usize,
    partial: [u8; 188],
    staged: usize,
    #[cfg(test)]
    probe: Arc<PacketSourceProbe>,
}
#[cfg(test)]
#[derive(Default)]
struct PacketSourceProbe {
    retained_bytes: std::sync::atomic::AtomicUsize,
}
impl PacketSource {
    fn new(rx: broadcast::Receiver<StreamEvent>) -> Self {
        Self::from_reader(PacketReader::Broadcast(rx))
    }
    fn from_reader(rx: PacketReader) -> Self {
        Self {
            rx,
            chunk: None,
            offset: 0,
            partial: [0; 188],
            staged: 0,
            #[cfg(test)]
            probe: Arc::new(PacketSourceProbe::default()),
        }
    }
    fn discard_partial(&mut self) {
        self.staged = 0;
        #[cfg(test)]
        self.update_probe();
    }
    #[cfg(test)]
    fn update_probe(&self) {
        let retained = self
            .chunk
            .as_ref()
            .map_or(0, |chunk| chunk.len().saturating_sub(self.offset));
        self.probe
            .retained_bytes
            .store(retained + self.staged, std::sync::atomic::Ordering::Relaxed);
    }
    async fn next(&mut self, budget: &InputBudget) -> SourceItem {
        loop {
            if let Some(chunk) = &self.chunk {
                let n = (188 - self.staged).min(chunk.len() - self.offset);
                self.partial[self.staged..self.staged + n]
                    .copy_from_slice(&chunk.as_slice()[self.offset..self.offset + n]);
                self.offset += n;
                self.staged += n;
                if self.offset == chunk.len() {
                    self.chunk = None;
                    self.offset = 0;
                }
                #[cfg(test)]
                self.update_probe();
                if self.staged == 188 {
                    self.staged = 0;
                    #[cfg(test)]
                    self.update_probe();
                    return SourceItem::Packet(self.partial);
                }
            } else {
                let (event, gap) = match &mut self.rx {
                    PacketReader::Broadcast(rx) => (rx.recv().await, false),
                    PacketReader::Direct { source, stats } => {
                        let source = source
                            .as_mut()
                            .expect("owned source available until cleanup");
                        let bytes = source.next().await;
                        *stats.lock().unwrap() = source.stats();
                        let gap = source.take_discontinuity();
                        (
                            bytes
                                .map(StreamEvent::Data)
                                .ok_or(broadcast::error::RecvError::Closed),
                            gap,
                        )
                    }
                };
                match event {
                    Ok(
                        StreamEvent::Data(chunk) | StreamEvent::NormalizedData { bytes: chunk, .. },
                    ) => {
                        // Bytes may be a tiny view of an arbitrarily large owner.
                        // Reserve and detach small views synchronously, then drop
                        // that owner before returning a packet or reaching any await.
                        self.chunk = if chunk.len() > DETACH_MAX_BYTES {
                            Some(SourceChunk::InPlace(chunk))
                        } else {
                            let detached = budget.try_copy(&chunk);
                            drop(chunk);
                            match detached {
                                Ok(chunk) => Some(SourceChunk::Detached(chunk)),
                                Err(reason) => return SourceItem::Failed(reason),
                            }
                        };
                        self.offset = 0;
                        #[cfg(test)]
                        self.update_probe();
                        if gap {
                            self.discard_partial();
                            return SourceItem::Recovery(RecoveryReason::Source);
                        }
                    }
                    Ok(StreamEvent::Discontinuity) => {
                        self.discard_partial();
                        return SourceItem::Recovery(RecoveryReason::Source);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        self.discard_partial();
                        return SourceItem::Recovery(RecoveryReason::Lag);
                    }
                    Err(broadcast::error::RecvError::Closed) => return SourceItem::Closed,
                }
            }
        }
    }
}

trait WorkerTarget: Send + Sync {
    type Output: process::ProcessOutput;
    fn status(&self) -> HlsStatus;
    fn begin_generation(&self, generation: GenerationId, deadline: Instant, recovery: bool);
    fn fail_generation(&self, generation: GenerationId, reason: HlsFailure);
    fn child_output(&self, generation: GenerationId) -> Self::Output;
    fn feed_initial(
        &self,
        generation: GenerationId,
        packets: &[[u8; 188]],
        cancel: &mut watch::Receiver<bool>,
    ) -> impl std::future::Future<Output = Result<(), HlsFailure>> + Send;
    fn feed_packet(
        &self,
        generation: GenerationId,
        packet: &[u8; 188],
        cancel: &mut watch::Receiver<bool>,
    ) -> impl std::future::Future<Output = Result<(), HlsFailure>> + Send;
    fn flush_pending(&self, generation: GenerationId);
}
impl WorkerTarget for Arc<HlsPackager> {
    type Output = process::HlsOutput;
    fn status(&self) -> HlsStatus {
        HlsPackager::status(self)
    }
    fn begin_generation(&self, generation: GenerationId, deadline: Instant, recovery: bool) {
        HlsPackager::begin_generation(self, generation, deadline, recovery);
    }
    fn fail_generation(&self, generation: GenerationId, reason: HlsFailure) {
        HlsPackager::fail_generation(self, generation, reason);
    }
    fn child_output(&self, generation: GenerationId) -> Self::Output {
        process::HlsOutput {
            generation,
            media: self.clone(),
        }
    }
    fn flush_pending(&self, _generation: GenerationId) {}
    async fn feed_initial(
        &self,
        generation: GenerationId,
        packets: &[[u8; 188]],
        _cancel: &mut watch::Receiver<bool>,
    ) -> Result<(), HlsFailure> {
        for packet in packets {
            self.feed_generation(generation, packet);
        }
        Ok(())
    }
    async fn feed_packet(
        &self,
        generation: GenerationId,
        packet: &[u8; 188],
        _cancel: &mut watch::Receiver<bool>,
    ) -> Result<(), HlsFailure> {
        self.feed_generation(generation, packet);
        Ok(())
    }
}

struct EnabledWorker<M = Arc<HlsPackager>> {
    media: M,
    prepared: PreparedNormalizer,
    source: PacketSource,
    recovery: RecoveryWindow,
    config: HlsConfig,
    deadline: Instant,
    seed: Option<[u8; 188]>,
    budget: InputBudget,
}
/// How a worker that did not fail ended: the source closed, or it was told to stop.
#[derive(Debug, PartialEq, Eq)]
enum WorkerEnd {
    Eof,
    Cancelled,
}
enum Turn {
    Recover(RecoveryReason, Option<[u8; 188]>),
    Eof,
    Stop,
}
impl<M: WorkerTarget> EnabledWorker<M> {
    fn invalidate(
        &mut self,
        reason: RecoveryReason,
        seed: Option<[u8; 188]>,
    ) -> Result<(), HlsFailure> {
        let old = self.recovery.generation;
        let generation = match self.recovery.invalidate(Instant::now(), reason) {
            Ok(generation) => generation,
            Err(reason) => {
                // Readiness cannot remain visible during the following child reap.
                self.media.fail_generation(old, reason);
                return Err(reason);
            }
        };
        if generation == old {
            return Ok(());
        }
        crate::alog!(
            "[audio] recovery: reason={:?} generation={} counted={} window={}/3",
            reason,
            generation.0,
            self.recovery.last_counted(),
            self.recovery.counted()
        );
        // A recovery while still starting preserves that original absolute clock.
        if self.media.status() == HlsStatus::Ready {
            self.deadline = Instant::now() + Duration::from_millis(self.config.startup_timeout_ms);
        }
        self.media.begin_generation(generation, self.deadline, true);
        self.source.discard_partial();
        self.seed = seed;
        Ok(())
    }
    async fn run(&mut self, mut cancel: watch::Receiver<bool>) -> Result<WorkerEnd, HlsFailure> {
        self.media
            .begin_generation(self.recovery.generation, self.deadline, false);
        loop {
            let mut classifier = Classifier::new();
            let opening = loop {
                if *cancel.borrow() {
                    return Ok(WorkerEnd::Cancelled);
                }
                let item = if let Some(packet) = self.seed.take() {
                    SourceItem::Packet(packet)
                } else {
                    tokio::select! { biased;
                        _ = process::cancelled(&mut cancel) => return Ok(WorkerEnd::Cancelled),
                        _ = tokio::time::sleep_until(self.deadline) => return Err(HlsFailure::Deadline),
                        item = self.source.next(&self.budget) => item,
                    }
                };
                match item {
                    SourceItem::Failed(reason) => return Err(reason),
                    SourceItem::Closed => return Ok(WorkerEnd::Eof),
                    SourceItem::Recovery(reason) => {
                        self.invalidate(reason, None)?;
                        classifier = Classifier::new();
                    }
                    SourceItem::Packet(mut packet) => {
                        if validate_packet(&packet)? && ts_timing(&packet).discontinuity {
                            if !self.recovery.marker_is_paired() {
                                self.invalidate(RecoveryReason::Transport, None)?;
                                classifier = Classifier::new();
                            }
                            clear_marker(&mut packet);
                        }
                        if let Some(opening) = classifier.observe(packet)? {
                            break opening;
                        }
                    }
                }
            };
            self.recovery.opening(Instant::now());
            let transform = opening
                .layout
                .audio
                .as_ref()
                .is_some_and(|a| matches!(a.codec, AudioCodec::Ac3 | AudioCodec::MpegAudio));
            let mut mpeg = classifier.mpeg;
            let mut selector = classifier.selector;
            let layout = opening.layout;
            let mut initial = opening.tables;
            initial.push(opening.packet);
            if !transform {
                self.media
                    .feed_initial(self.recovery.generation, &initial, &mut cancel)
                    .await?;
                match AssertUnwindSafe(self.produce(
                    &mut selector,
                    &layout,
                    &mut mpeg,
                    None,
                    &mut cancel,
                ))
                .catch_unwind()
                .await
                .unwrap_or(Err(HlsFailure::Pump))?
                {
                    Turn::Stop => return Ok(WorkerEnd::Cancelled),
                    Turn::Eof => return Ok(WorkerEnd::Eof),
                    Turn::Recover(reason, seed) => self.invalidate(reason, seed)?,
                }
            } else {
                // No producer task: poll the child supervisor and input producer in
                // this tracker-owned supervisor. It remains owned through cleanup.
                // Charge all retained replay storage before a child exists. The
                // producer moves that same allocation into the bounded channel.
                let replay = initial
                    .into_iter()
                    .flatten()
                    .collect::<Vec<u8>>()
                    .into_boxed_slice()
                    .into_vec();
                let replay = self.budget.acquire_owned(replay, &mut cancel).await?;
                let (tx, rx) = mpsc::channel(16);
                let (child_cancel, child_rx) = watch::channel(false);
                let child = process::run_target_child(
                    self.prepared.clone(),
                    layout.clone(),
                    self.media.child_output(self.recovery.generation),
                    rx,
                    child_rx,
                    self.deadline,
                );
                tokio::pin!(child);
                let turn = {
                    let producer = AssertUnwindSafe(async {
                        tokio::select! { biased;
                            _ = process::cancelled(&mut cancel) => return Ok(Turn::Stop),
                            sent = tx.send(replay) => sent.map_err(|_|HlsFailure::Pipe)?,
                        }
                        self.produce(&mut selector, &layout, &mut mpeg, Some(&tx), &mut cancel)
                            .await
                    })
                    .catch_unwind();
                    tokio::pin!(producer);
                    tokio::select! { biased;
                        result = &mut child => { result?; return Ok(WorkerEnd::Cancelled); },
                        turn = &mut producer => turn.unwrap_or(Err(HlsFailure::Pump)),
                    }
                };
                match turn {
                    Ok(Turn::Recover(reason, seed)) => {
                        // Fence before signalling cancellation; no old stdout can
                        // enter the replacement generation during graceful reap.
                        let invalidation = std::panic::catch_unwind(AssertUnwindSafe(|| {
                            self.invalidate(reason, seed)
                        }))
                        .unwrap_or(Err(HlsFailure::Pump));
                        if let Err(reason) = invalidation {
                            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                                self.media.fail_generation(self.recovery.generation, reason)
                            }));
                        }
                        child_cancel.send_replace(true);
                        let cleanup = child.await;
                        invalidation?;
                        cleanup?;
                    }
                    other => {
                        if let Err(reason) = &other {
                            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                                self.media
                                    .fail_generation(self.recovery.generation, *reason)
                            }));
                        }
                        child_cancel.send_replace(true);
                        let _ = child.await;
                        return other.map(|turn| match turn {
                            Turn::Eof => WorkerEnd::Eof,
                            _ => WorkerEnd::Cancelled,
                        });
                    }
                }
            }
        }
    }
    async fn produce(
        &mut self,
        selector: &mut LiveAudioSelection,
        layout: &TransportLayout,
        mpeg: &mut Option<MpegAudioProbe>,
        sender: Option<&mpsc::Sender<BudgetedInput>>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<Turn, HlsFailure> {
        #[cfg(test)]
        if PANIC_NEXT_PRODUCE.swap(false, std::sync::atomic::Ordering::SeqCst) {
            panic!("injected Task4 producer panic while a child supervisor is active");
        }
        loop {
            let deadline = if self.media.status() == HlsStatus::Ready {
                None
            } else {
                Some(self.deadline)
            };
            let item = {
                let reading = self.source.next(&self.budget);
                tokio::pin!(reading);
                let mut flushed = false;
                tokio::select! { biased;
                    _ = process::cancelled(cancel) => return Ok(Turn::Stop),
                    _ = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending().await } } => return Err(HlsFailure::Deadline),
                    item = std::future::poll_fn(|cx| {
                        let result = std::future::Future::poll(reading.as_mut(),cx);
                        if sender.is_none() && result.is_pending() && !flushed {
                            self.media.flush_pending(self.recovery.generation);
                            flushed = true;
                        }
                        result
                    }) => item,
                }
            };
            match item {
                SourceItem::Failed(reason) => return Err(reason),
                SourceItem::Closed => {
                    if sender.is_none() {
                        self.media.flush_pending(self.recovery.generation);
                    }
                    return Ok(Turn::Eof);
                }
                SourceItem::Recovery(reason) => return Ok(Turn::Recover(reason, None)),
                SourceItem::Packet(mut packet) => {
                    if !validate_packet(&packet)? {
                        continue;
                    }
                    if ts_timing(&packet).discontinuity {
                        return Ok(Turn::Recover(RecoveryReason::Transport, None));
                    }
                    let selected = selector.observe(packet);
                    match selector.layout() {
                        LayoutDecision::Unsupported => return Err(HlsFailure::Unsupported),
                        LayoutDecision::Supported(current) if current == *layout => {}
                        _ => return Ok(Turn::Recover(RecoveryReason::Layout, Some(packet))),
                    }
                    if let Some(probe) = mpeg {
                        probe.observe(&packet)?;
                    }
                    clear_marker(&mut packet);
                    match sender {
                        Some(sender) => {
                            let input = self.budget.try_copy(&packet)?;
                            tokio::select! { biased;
                                _ = process::cancelled(cancel) => return Ok(Turn::Stop),
                                sent = sender.send(input) => sent.map_err(|_|HlsFailure::Pipe)?,
                            }
                        }
                        None => {
                            if let Some(output) = copy_output(selected) {
                                self.media
                                    .feed_packet(self.recovery.generation, &output, cancel)
                                    .await?;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// What the AAC copy path publishes for one source packet: the selector's output, not the raw
/// packet. The opening was fed from the selector's (rewritten) tables, so forwarding raw PSI
/// afterwards would show the packager a different PMT at the first repetition; it treats that
/// as a layout change and discards the run in progress. Unselected audio PIDs are dropped too.
pub(crate) fn copy_output(selected: ace_media::audio::AudioPacket) -> Option<[u8; 188]> {
    let mut packet = selected.packet?;
    clear_marker(&mut packet);
    Some(packet)
}

fn clear_marker(packet: &mut [u8; 188]) {
    if packet[3] & 0x20 != 0 && packet[4] > 0 {
        packet[5] &= !0x80;
    }
}

pub use process::{preflight, HlsPreflightError};
#[cfg(test)]
mod task4_tests {
    use super::*;

    use super::process::test_fixtures as fixtures;
    use crate::provider::{SourceStats, TsSource};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct OneChunkSource(Option<Bytes>);
    #[async_trait::async_trait]
    impl TsSource for OneChunkSource {
        async fn next(&mut self) -> Option<Bytes> {
            if self.0.is_some() {
                self.0.take()
            } else {
                std::future::pending().await
            }
        }
        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
    }

    fn worker_fixture(
        mode: process::test_support::ChildMode,
    ) -> (
        EnabledWorker,
        Arc<StreamSession>,
        process::test_support::Counters,
    ) {
        let (bytes, _) = process::test_smoke_input();
        let session = StreamSession::start(Box::new(OneChunkSource(Some(Bytes::from(bytes)))), 8);
        let config = HlsConfig {
            segment_packets: 128,
            window_segments: 4,
            segment_duration_ms: 1000,
            startup_segments: 1,
            startup_timeout_ms: 5000,
        };
        let media = HlsPackager::create(&session, config, true);
        let (prepared, counters) = process::test_support::fake_prepared_child(mode);
        let worker = EnabledWorker {
            media,
            prepared,
            source: PacketSource::new(session.raw_receiver()),
            recovery: RecoveryWindow::new(),
            config,
            deadline: Instant::now() + Duration::from_secs(5),
            seed: None,
            budget: InputBudget::new(),
        };
        (worker, session, counters)
    }

    struct TrackedBacking {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for TrackedBacking {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for TrackedBacking {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn valid_mpeg_audio_source(minimum_len: usize) -> Vec<u8> {
        let mut input = fixtures::transport_prefix();
        let pmt = fixtures::pmt_section(9, 256, 256, 0x1b, 257, 0x03);
        input[188..376].copy_from_slice(&fixtures::psi_packet(4096, 2, &pmt));
        input.truncate(2 * 188);
        for packet in fixtures::mpeg_layer2_audio_packets(257) {
            input.extend_from_slice(&packet);
        }
        input.extend_from_slice(&fixtures::video_access_packet(256));
        for pcr in [0, 108_000, 216_000] {
            input.extend_from_slice(&fixtures::pcr_packet(256, pcr));
        }
        let pad = fixtures::pcr_packet(256, 216_000);
        while input.len() < minimum_len {
            input.extend_from_slice(&pad);
        }
        input
    }

    fn worker_for_receiver(
        rx: broadcast::Receiver<StreamEvent>,
    ) -> (
        EnabledWorker,
        Arc<StreamSession>,
        process::test_support::Counters,
        Arc<PacketSourceProbe>,
    ) {
        let session = StreamSession::start(Box::new(OneChunkSource(None)), 2);
        let config = HlsConfig {
            segment_packets: 128,
            window_segments: 4,
            segment_duration_ms: 1000,
            startup_segments: 1,
            startup_timeout_ms: 15_000,
        };
        let media = HlsPackager::create(&session, config, true);
        let (prepared, counters) = process::test_support::fake_prepared_child(
            process::test_support::ChildMode::ReadyThenStall,
        );
        let source = PacketSource::new(rx);
        let probe = source.probe.clone();
        let worker = EnabledWorker {
            media,
            prepared,
            source,
            recovery: RecoveryWindow::new(),
            config,
            deadline: Instant::now() + Duration::from_secs(15),
            seed: None,
            budget: InputBudget::new(),
        };
        (worker, session, counters, probe)
    }

    async fn wait_for_blocked_transform(
        task: &tokio::task::JoinHandle<Result<WorkerEnd, HlsFailure>>,
        counters: &process::test_support::Counters,
        probe: &PacketSourceProbe,
    ) -> bool {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let child = counters.snapshot();
                let retained = probe.retained_bytes.load(Ordering::Relaxed);
                if task.is_finished() {
                    return false;
                }
                if child.children_started == 1 && child.stdin_write_waits > 0 && retained > 0 {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    /// A source chunk too large to detach is consumed in place: the transform
    /// starts, the budget never exceeds its bound, and the owner is released once
    /// the worker stops holding it.
    async fn large_chunk_is_consumed_in_place(logical_bytes: usize) {
        let _serial = process::test_support::process_test_lock().await;
        let (tx, rx) = broadcast::channel(1);
        let (mut worker, _session, counters, probe) = worker_for_receiver(rx);
        let budget = worker.budget.clone();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move { worker.run(cancel_rx).await });

        let source = valid_mpeg_audio_source(logical_bytes);
        assert!(source.len() > DETACH_MAX_BYTES);
        assert_eq!(source.len() % 188, 0);
        let logical_len = source.len();
        let drops = Arc::new(AtomicUsize::new(0));
        tx.send(StreamEvent::Data(Bytes::from_owner(TrackedBacking {
            bytes: source,
            drops: drops.clone(),
        })))
        .unwrap();

        let blocked = wait_for_blocked_transform(&task, &counters, &probe).await;
        let charged = budget.charged();
        let retained_tail = probe.retained_bytes.load(Ordering::Relaxed);
        println!(
            "large chunk consumed in place: logical={logical_len} charged={charged} retained_tail={retained_tail} blocked={blocked}"
        );
        assert!(
            blocked,
            "the transform starts on a chunk larger than the detach bound"
        );
        assert!(charged <= process::INPUT_BYTES);
        assert!(
            retained_tail > 0,
            "the unconsumed tail is still held in place"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0, "owner held while in use");

        cancel_tx.send_replace(true);
        let result = tokio::time::timeout(Duration::from_secs(4), task)
            .await
            .expect("worker cancellation completes child cleanup")
            .expect("worker task joins");
        drop(tx);
        assert_eq!(result, Ok(WorkerEnd::Cancelled));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "owner released with the worker"
        );
        let cleanup = counters.snapshot();
        assert_eq!(cleanup.children_started, 1);
        assert_eq!(cleanup.children_reaped, 1);
        assert_eq!(cleanup.active_tasks, 0);
    }

    #[tokio::test]
    async fn hls_audio_packet_source_budget_chunk_above_budget_is_consumed_in_place() {
        large_chunk_is_consumed_in_place(9 * 1024 * 1024).await;
    }

    #[tokio::test]
    async fn hls_audio_packet_source_budget_chunk_near_budget_leaves_room_for_replay() {
        // One packet below the budget: detaching it would leave no permits for the
        // replay, so it is consumed in place as well.
        large_chunk_is_consumed_in_place(process::INPUT_BYTES / 188 * 188).await;
    }

    #[tokio::test]
    async fn hls_audio_packet_source_budget_tiny_view_drops_oversized_backing_before_backpressure()
    {
        let _serial = process::test_support::process_test_lock().await;
        let (tx, rx) = broadcast::channel(1);
        let mut eviction_observer = tx.subscribe();
        let (mut worker, _session, counters, probe) = worker_for_receiver(rx);
        let media = worker.media.clone();
        let budget = worker.budget.clone();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move { worker.run(cancel_rx).await });

        let opening = valid_mpeg_audio_source(0);
        tx.send(StreamEvent::Data(Bytes::from(opening))).unwrap();
        assert!(matches!(
            eviction_observer.try_recv(),
            Ok(StreamEvent::Data(_))
        ));
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if counters.snapshot().children_started == 1 && media.status() == HlsStatus::Ready {
                    break;
                }
                if task.is_finished() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("initial MPEG-audio opening either starts transform or fails closed");

        const BACKING_BYTES: usize = 16 * 1024 * 1024;
        // The largest view that is still detached: it must release its owner.
        let logical = valid_mpeg_audio_source(DETACH_MAX_BYTES - 187);
        let logical_len = logical.len();
        assert!(logical_len <= DETACH_MAX_BYTES && logical_len > 400 * 1024);
        let mut backing = vec![0; BACKING_BYTES];
        backing[..logical_len].copy_from_slice(&logical);
        let allocation_bytes = backing.capacity();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Bytes::from_owner(TrackedBacking {
            bytes: backing,
            drops: drops.clone(),
        });
        let view = owner.slice(0..logical_len);
        drop(owner);
        tx.send(StreamEvent::Data(view)).unwrap();

        let blocked = wait_for_blocked_transform(&task, &counters, &probe).await;
        let eviction_packet = fixtures::pcr_packet(256, 216_000);
        for _ in 0..4 {
            let _ = tx.send(StreamEvent::Data(Bytes::copy_from_slice(&eviction_packet)));
        }
        assert!(matches!(
            eviction_observer.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(n)) if n > 0
        ));
        drop(eviction_observer);
        drop(tx);

        let retained_tail = probe.retained_bytes.load(Ordering::Relaxed);
        let charged = budget.charged();
        let owner_drops_before_cancel = drops.load(Ordering::SeqCst);
        println!(
            "tiny view while child blocked after eviction: logical={} backing_capacity={} charged={} retained_tail={} owner_drops={} blocked={}",
            logical_len,
            allocation_bytes,
            charged,
            retained_tail,
            owner_drops_before_cancel,
            blocked
        );
        cancel_tx.send_replace(true);
        let _ = tokio::time::timeout(Duration::from_secs(4), task)
            .await
            .expect("worker cancellation completes child cleanup")
            .expect("worker task joins");

        assert!(
            blocked,
            "fake child stdin must be blocked while source tail is retained"
        );
        assert_eq!(
            owner_drops_before_cancel, 1,
            "the original {allocation_bytes}-byte owner must be released before backpressure; charged={charged}, source_tail={retained_tail}"
        );
        assert!(
            charged >= retained_tail,
            "budget charges {charged} bytes while PacketSource retains {retained_tail} logical tail bytes"
        );
        assert!(charged <= process::INPUT_BYTES);
        let cleanup = counters.snapshot();
        assert_eq!(cleanup.children_started, 1);
        assert_eq!(cleanup.children_reaped, 1);
        assert_eq!(cleanup.pumps_joined, 3);
        assert_eq!(cleanup.active_tasks, 0);
    }

    #[test]
    fn hls_audio_classification_discards_before_opening_and_returns_fresh_tables() {
        let source = fixtures::transport_prefix();
        let mut classifier = Classifier::new();
        let mut opening = None;
        let (packets, remainder) = source.as_chunks::<188>();
        assert!(remainder.is_empty(), "classifier fixture is packet aligned");
        for packet in packets {
            if let Some(classified) = classifier.observe(*packet).unwrap() {
                opening = Some(classified);
            }
        }
        let opening = opening.expect("tables followed by opening access point");
        assert_eq!(opening.tables.len(), 2);
        assert_eq!(opening.packet[0], 0x47);
        assert_eq!(opening.layout.program_id, 9);
        assert_eq!(opening.layout.video_pid, 256);
        assert_eq!(opening.layout.audio.unwrap().pid, 257);
    }

    #[tokio::test]
    async fn hls_audio_packet_source_consumes_a_chunk_larger_than_the_input_budget() {
        // Source chunks are whole pieces; piece length is a per-stream property and
        // can exceed the 1 MiB budget (a live source delivered 1,048,664 bytes).
        const PACKETS: usize = 2 * process::INPUT_BYTES / 188;
        let mut chunk = Vec::with_capacity(PACKETS * 188);
        for i in 0..PACKETS {
            let mut packet = [0u8; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10]);
            packet[4..12].copy_from_slice(&(i as u64).to_be_bytes());
            chunk.extend_from_slice(&packet);
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Bytes::from_owner(TrackedBacking {
            bytes: chunk,
            drops: drops.clone(),
        });
        let mut source = PacketSource::from_reader(PacketReader::Direct {
            source: Some(Box::new(OneChunkSource(Some(owner)))),
            stats: Arc::new(std::sync::Mutex::new(SourceStats::default())),
        });
        let budget = InputBudget::new();
        for i in 0..PACKETS {
            assert_eq!(
                drops.load(Ordering::SeqCst),
                0,
                "owner held before packet {i}"
            );
            match source.next(&budget).await {
                SourceItem::Packet(packet) => {
                    assert_eq!(packet[4..12], (i as u64).to_be_bytes(), "packet {i}");
                }
                SourceItem::Failed(reason) => panic!("packet {i}: failed with {reason:?}"),
                _ => panic!("packet {i}: unexpected item"),
            }
            assert_eq!(budget.charged(), 0, "an in-place chunk is not copied");
        }
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "owner released once consumed"
        );
    }

    /// Bounded synthetic benchmark: packets per second out of `PacketSource` for
    /// typical small chunks and for piece-sized chunks. Run in release:
    /// `cargo test --release -p ace-engine --lib packet_source_measurement -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn packet_source_measurement() {
        struct Chunks(Vec<Bytes>);
        #[async_trait::async_trait]
        impl TsSource for Chunks {
            async fn next(&mut self) -> Option<Bytes> {
                self.0.pop()
            }
            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }
        }
        const TOTAL: usize = 512 * 1024 * 1024;
        for chunk_bytes in [32 * 1024 / 188 * 188, 768 * 1024 / 188 * 188] {
            let mut packet = [0u8; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10]);
            let chunk = Bytes::from(packet.repeat(chunk_bytes / 188));
            let chunks = vec![chunk; TOTAL / chunk_bytes];
            let packets = chunks.len() * chunk_bytes / 188;
            let mut source = PacketSource::from_reader(PacketReader::Direct {
                source: Some(Box::new(Chunks(chunks))),
                stats: Arc::new(std::sync::Mutex::new(SourceStats::default())),
            });
            let budget = InputBudget::new();
            let started = std::time::Instant::now();
            let mut seen = 0usize;
            loop {
                match source.next(&budget).await {
                    SourceItem::Packet(p) => seen += std::hint::black_box(p)[0] as usize / 0x47,
                    SourceItem::Closed => break,
                    _ => panic!("unexpected item"),
                }
            }
            let elapsed = started.elapsed();
            assert_eq!(seen, packets);
            println!(
                "packet_source chunk={chunk_bytes} packets={packets} elapsed_ms={} mpkt_per_s={:.1} mib_per_s={:.0}",
                elapsed.as_millis(),
                packets as f64 / elapsed.as_secs_f64() / 1e6,
                (packets * 188) as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
            );
        }
    }

    #[tokio::test]
    async fn hls_audio_packet_source_detaches_a_view_at_the_detach_bound() {
        let chunk = Bytes::from(vec![0x47; DETACH_MAX_BYTES / 188 * 188]);
        let mut source = PacketSource::from_reader(PacketReader::Direct {
            source: Some(Box::new(OneChunkSource(Some(chunk.clone())))),
            stats: Arc::new(std::sync::Mutex::new(SourceStats::default())),
        });
        let budget = InputBudget::new();
        assert!(matches!(source.next(&budget).await, SourceItem::Packet(_)));
        assert_eq!(
            budget.charged(),
            chunk.len(),
            "views up to the bound are copied"
        );
    }

    #[test]
    fn hls_audio_classification_waits_through_a_long_gop_for_the_next_access_point() {
        // A 2 s GOP at about 7 Mbit/s puts keyframes 1.6-1.8 MB apart. A generation
        // that starts just after one must scan past more than 1 MiB of inter frames.
        let source = fixtures::transport_prefix();
        let pat: [u8; 188] = source[..188].try_into().unwrap();
        let pmt: [u8; 188] = source[188..376].try_into().unwrap();
        let access: [u8; 188] = source[376..564].try_into().unwrap();
        let mut classifier = Classifier::new();
        assert!(classifier.observe(pat).unwrap().is_none());
        assert!(classifier.observe(pmt).unwrap().is_none());
        const GOP_BYTES: usize = 2 * 1024 * 1024;
        let mut inter = [0x55; 188];
        inter[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10]); // PID 256, payload only
        for i in 0..GOP_BYTES / 188 {
            inter[3] = 0x10 | (i as u8 & 0x0f);
            assert!(classifier.observe(inter).unwrap().is_none());
        }
        let opening = classifier
            .observe(access)
            .expect("a long GOP is not an overflow")
            .expect("the next access point opens the generation");
        assert_eq!(opening.tables, vec![pat, pmt]);
    }

    #[test]
    fn hls_audio_classification_ignores_malformed_tei_and_preserves_last_valid_tables() {
        let source = fixtures::transport_prefix();
        let mut classifier = Classifier::new();
        let pat: [u8; 188] = source[..188].try_into().unwrap();
        let pmt: [u8; 188] = source[188..376].try_into().unwrap();
        classifier.observe(pat).unwrap();
        classifier.observe(pmt).unwrap();

        let mut tei_pmt = pmt;
        tei_pmt[1] |= 0x80;
        assert!(
            matches!(classifier.observe(tei_pmt), Ok(None)),
            "a malformed table packet must not terminate classification"
        );

        let opening_packet: [u8; 188] = source[376..564].try_into().unwrap();
        let opening = classifier.observe(opening_packet).unwrap().unwrap();
        assert_eq!(opening.tables, vec![pat, pmt]);
        assert_eq!(opening.layout.program_id, 9);
    }

    #[test]
    fn hls_audio_classification_ignores_bad_crc_noncurrent_and_wrong_program_tables() {
        let source = fixtures::transport_prefix();
        let pat: [u8; 188] = source[..188].try_into().unwrap();
        let pmt: [u8; 188] = source[188..376].try_into().unwrap();
        let mut classifier = Classifier::new();
        classifier.observe(pat).unwrap();
        classifier.observe(pmt).unwrap();

        let mut bad_crc = pmt;
        bad_crc[30] ^= 0x01;
        assert!(matches!(classifier.observe(bad_crc), Ok(None)));

        let mut noncurrent = pmt;
        noncurrent[10] &= !1;
        fixtures::set_psi_crc(&mut noncurrent);
        assert!(matches!(classifier.observe(noncurrent), Ok(None)));

        let mut wrong_program = pmt;
        wrong_program[9] = 10;
        fixtures::set_psi_crc(&mut wrong_program);
        assert!(matches!(classifier.observe(wrong_program), Ok(None)));

        let access: [u8; 188] = source[376..564].try_into().unwrap();
        let opening = classifier.observe(access).unwrap().unwrap();
        assert_eq!(opening.tables, vec![pat, pmt]);
    }

    #[test]
    fn hls_audio_classification_rejects_current_unsupported_audio_evidence() {
        let pat = fixtures::psi_packet(0, 0, &fixtures::pat_section(7, 9, 4096));
        let unsupported = fixtures::pmt_packet_with_section(vec![
            0x02, 0xb0, 0x17, 0x00, 0x09, 0xc1, 0, 0, 0xe1, 0x00, 0xf0, 0, 0x1b, 0xe1, 0x00, 0xf0,
            0, 0x11, 0xe1, 0x01, 0xf0, 0,
        ]);
        let mut classifier = Classifier::new();
        classifier.observe(pat).unwrap();
        assert!(matches!(
            classifier.observe(unsupported),
            Err(HlsFailure::Unsupported)
        ));
    }

    #[test]
    fn hls_audio_classification_layout_identity_ignores_version_and_tracks_semantics() {
        let source = fixtures::transport_prefix();
        let mut classifier = Classifier::new();
        classifier
            .observe(source[..188].try_into().unwrap())
            .unwrap();
        classifier
            .observe(source[188..376].try_into().unwrap())
            .unwrap();
        let opening = classifier
            .observe(source[376..564].try_into().unwrap())
            .unwrap()
            .unwrap();
        let baseline = opening.layout;

        let mut version_pat: [u8; 188] = source[..188].try_into().unwrap();
        version_pat[10] = 0xc3; // PAT version increments without changing its mapping.
        fixtures::set_psi_crc(&mut version_pat);
        classifier.observe(version_pat).unwrap();
        assert_eq!(
            classifier.selector.layout(),
            LayoutDecision::Supported(baseline.clone())
        );

        let semantic_pmt = fixtures::psi_packet(
            4096,
            2,
            &fixtures::pmt_section(9, 256, 256, 0x1b, 258, 0x0f),
        );
        classifier.observe(semantic_pmt).unwrap();
        let LayoutDecision::Supported(changed) = classifier.selector.layout() else {
            panic!("a valid selected audio PID change remains supported")
        };
        assert_ne!(
            changed, baseline,
            "selected PID is semantic layout identity"
        );
        assert_eq!(changed.audio.as_ref().unwrap().pid, 258);
    }

    #[test]
    fn hls_audio_recovery_lag_does_not_create_a_paired_transport_marker() {
        let mut recovery = RecoveryWindow::new();
        recovery
            .invalidate(Instant::now(), RecoveryReason::Lag)
            .unwrap();
        assert!(!recovery.marker_is_paired());
    }

    #[tokio::test]
    async fn hls_audio_starting_recoveries_keep_original_absolute_deadline() {
        let _serial = process::test_support::process_test_lock().await;
        let (mut worker, _session, _counters) =
            worker_fixture(process::test_support::ChildMode::BlockStdin);
        let original = worker.deadline;
        worker.invalidate(RecoveryReason::Source, None).unwrap();
        assert_eq!(
            worker.deadline, original,
            "recovery during Starting must not restart startup time"
        );
        worker.invalidate(RecoveryReason::Lag, None).unwrap();
        assert_eq!(
            worker.deadline, original,
            "a second pre-ready recovery shares the original deadline"
        );
    }

    #[tokio::test]
    async fn hls_audio_packet_source_discards_partial_chunk_on_source_recovery() {
        let (tx, rx) = broadcast::channel(8);
        let mut source = PacketSource::new(rx);
        let budget = InputBudget::new();
        tx.send(StreamEvent::Data(Bytes::from(vec![0x11; 100])))
            .unwrap();
        tx.send(StreamEvent::Discontinuity).unwrap();
        let fresh = [0x47; 188];
        tx.send(StreamEvent::Data(Bytes::copy_from_slice(&fresh)))
            .unwrap();
        assert!(matches!(
            source.next(&budget).await,
            SourceItem::Recovery(RecoveryReason::Source)
        ));
        assert!(
            matches!(source.next(&budget).await, SourceItem::Packet(packet) if packet == fresh)
        );
    }

    #[tokio::test]
    async fn hls_audio_worker_panic_while_transforming_reaps_child_and_joins_pumps() {
        let _serial = process::test_support::process_test_lock().await;
        let (mut worker, _session, counters) =
            worker_fixture(process::test_support::ChildMode::BlockStdin);
        let media = worker.media.clone();
        PANIC_NEXT_PRODUCE.store(true, std::sync::atomic::Ordering::SeqCst);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move { worker.run(cancel_rx).await });
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("worker panic task should terminate")
            .expect("worker should catch the producer panic and finish cleanup");
        assert_eq!(result, Err(HlsFailure::Pump));
        assert_eq!(media.status(), HlsStatus::Failed(HlsFailure::Pump));
        let snapshot = counters.snapshot();
        assert_eq!(
            snapshot.children_started, 1,
            "transform child was active during panic"
        );
        assert_eq!(
            snapshot.children_reaped, 1,
            "producer panic must await child cleanup"
        );
        assert_eq!(
            snapshot.pumps_started, snapshot.pumps_joined,
            "all child pumps must join"
        );
        assert_eq!(snapshot.active_tasks, 0);
    }

    #[tokio::test]
    async fn hls_audio_fourth_recovery_fences_ready_generation_immediately() {
        let (mut worker, _session, _counters) =
            worker_fixture(process::test_support::ChildMode::BlockStdin);
        let media = worker.media.clone();
        let source = process::test_fixtures::ready_output_fixture(12);
        for _ in 0..3 {
            worker.invalidate(RecoveryReason::Layout, None).unwrap();
            let generation = worker.recovery.generation;
            media.feed_generation(generation, &source);
            assert_eq!(
                media.status(),
                HlsStatus::Ready,
                "recovery refills current generation {generation:?}"
            );
        }
        assert_eq!(
            worker.invalidate(RecoveryReason::Source, None),
            Err(HlsFailure::RecoveryLimit),
        );
        assert_eq!(
            media.status(),
            HlsStatus::Failed(HlsFailure::RecoveryLimit),
            "the fourth invalidation must fail readiness before child reap can await",
        );
    }

    #[test]
    fn hls_audio_recovery_of_a_stable_generation_does_not_count_toward_the_limit() {
        // Upstream reconnects every 15 s each end a generation that was serving.
        // That is a flaky source, not churn, and must not end playback.
        let start = tokio::time::Instant::now();
        let mut recovery = RecoveryWindow::new();
        recovery.opening(start);
        for i in 1..=4u64 {
            let now = start + Duration::from_secs(15 * i);
            assert!(
                recovery.invalidate(now, RecoveryReason::Source).is_ok(),
                "recovery {i} after a stable generation"
            );
            recovery.opening(now + Duration::from_millis(500));
        }
        // Generations that fail again right after opening still exhaust the window.
        let later = start + Duration::from_secs(70);
        for _ in 0..3 {
            assert!(recovery.invalidate(later, RecoveryReason::Layout).is_ok());
            recovery.opening(later);
        }
        assert_eq!(
            recovery.invalidate(later, RecoveryReason::Layout),
            Err(crate::hls::HlsFailure::RecoveryLimit)
        );
    }

    #[test]
    fn hls_audio_recovery_window_allows_three_then_fails_fourth() {
        let now = tokio::time::Instant::now();
        let mut recovery = RecoveryWindow::new();
        for _ in 0..3 {
            assert!(recovery.invalidate(now, RecoveryReason::Layout).is_ok());
        }
        assert_eq!(
            recovery.invalidate(now, RecoveryReason::Transport),
            Err(crate::hls::HlsFailure::RecoveryLimit)
        );
        assert!(recovery
            .invalidate(
                now + std::time::Duration::from_secs(61),
                RecoveryReason::Lag
            )
            .is_ok());
    }

    #[tokio::test]
    async fn hls_audio_preflight_disabled_ignores_invalid_executable_path() {
        let config = crate::config::HlsAudioConfig {
            normalize: false,
            ffmpeg_path: std::path::PathBuf::new(),
        };
        assert!(preflight(&config).await.unwrap().is_none());
    }

    #[test]
    fn hls_audio_classification_budget_expires_before_opening_instead_of_sliding() {
        let mut classifier = Classifier::new();
        let mut payload = [0xff; 188];
        payload[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10]);
        // A sync-looking non-PSI packet is scanned and discarded. The next whole
        // packet would cross the scan cap, so no later opening can be accepted.
        for _ in 0..(CLASSIFY_SCAN_BYTES / 188) {
            assert!(classifier.observe(payload).unwrap().is_none());
        }
        assert!(matches!(
            classifier.observe(payload),
            Err(HlsFailure::InputOverflow)
        ));
    }

    #[test]
    fn hls_audio_recovery_source_marker_is_paired_until_opening_consumes_it() {
        let now = Instant::now();
        let mut recovery = RecoveryWindow::new();
        recovery.invalidate(now, RecoveryReason::Source).unwrap();
        assert!(recovery.marker_is_paired());
        assert_eq!(
            recovery.invalidate(now, RecoveryReason::Transport).unwrap(),
            GenerationId(2),
            "the paired transport marker must not create another generation"
        );
        recovery.opening(tokio::time::Instant::now());
        assert!(!recovery.marker_is_paired());
        assert_eq!(
            recovery.invalidate(now, RecoveryReason::Transport).unwrap(),
            GenerationId(3),
            "a marker after the opening packet is a fresh invalidation"
        );
    }

    #[test]
    fn hls_audio_recovery_expires_each_invalidation_at_sixty_seconds() {
        let now = Instant::now();
        let mut recovery = RecoveryWindow::new();
        for second in [0, 20, 40] {
            recovery
                .invalidate(now + Duration::from_secs(second), RecoveryReason::Lag)
                .unwrap();
        }
        assert_eq!(
            recovery.invalidate(now + Duration::from_secs(59), RecoveryReason::Source),
            Err(HlsFailure::RecoveryLimit)
        );
        assert!(recovery
            .invalidate(now + Duration::from_secs(60), RecoveryReason::Source)
            .is_ok());
    }

    #[tokio::test]
    async fn hls_audio_preflight_installed_ffmpeg_smoke_is_targeted_and_optional() {
        if std::env::var_os("OUTPACE_RUN_HLS_AUDIO_PREFLIGHT").as_deref()
            != Some(std::ffi::OsStr::new("1"))
        {
            return;
        }
        let _serial = process::test_support::process_test_lock().await;
        let config = crate::config::HlsAudioConfig {
            normalize: true,
            ffmpeg_path: "ffmpeg".into(),
        };
        let prepared = preflight(&config)
            .await
            .expect("installed FFmpeg capability and smoke preflight");
        assert!(prepared.is_some());
    }
}

#[cfg(test)]
mod mpeg_probe_tests {
    use super::process::test_fixtures as fixtures;
    use super::*;

    const AUDIO_PID: u16 = 257;

    fn header(version: u8, layer: u8, bitrate: u8, sample_rate: u8) -> [u8; 4] {
        let bits = 0x7ff_u32 << 21
            | (u32::from(version) << 19)
            | (u32::from(layer) << 17)
            | (1 << 16)
            | (u32::from(bitrate) << 12)
            | (u32::from(sample_rate) << 10);
        bits.to_be_bytes()
    }

    fn ts_payload(pid: u16, start: bool, payload: &[u8], cc: u8) -> [u8; 188] {
        assert!(payload.len() <= 184);
        let mut packet = [0xff; 188];
        packet[0] = 0x47;
        packet[1] = ((pid >> 8) as u8 & 0x1f) | if start { 0x40 } else { 0 };
        packet[2] = pid as u8;
        if payload.len() == 184 {
            packet[3] = 0x10 | (cc & 0x0f);
            packet[4..].copy_from_slice(payload);
        } else {
            packet[3] = 0x30 | (cc & 0x0f);
            let adaptation_len = 183 - payload.len();
            packet[4] = adaptation_len as u8;
            if adaptation_len > 0 {
                packet[5] = 0;
            }
            let offset = 5 + adaptation_len;
            packet[offset..offset + payload.len()].copy_from_slice(payload);
        }
        packet
    }

    fn pes_frames(frame_header: [u8; 4], frame_len: usize) -> Vec<[u8; 188]> {
        let mut payload = vec![0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0];
        for _ in 0..2 {
            payload.extend_from_slice(&frame_header);
            payload.resize(payload.len() + frame_len - 4, 0x55);
        }
        payload
            .chunks(184)
            .enumerate()
            .map(|(i, chunk)| ts_payload(AUDIO_PID, i == 0, chunk, i as u8))
            .collect()
    }

    fn confirm(header: [u8; 4], len: usize) -> Result<bool, HlsFailure> {
        let mut probe = MpegAudioProbe::new(AUDIO_PID);
        let mut accepted = false;
        for packet in pes_frames(header, len) {
            accepted |= probe.observe(&packet)?;
        }
        Ok(accepted)
    }

    #[test]
    fn mpeg_probe_confirms_layer_two_and_three_at_computed_frame_boundaries() {
        // MPEG-1 Layer II, 128 kbit/s at 44.1 kHz: 417-byte frame.
        assert!(confirm(header(3, 2, 8, 0), 417).unwrap());
        // MPEG-1 Layer III, 128 kbit/s at 44.1 kHz: bitrate index 9, 417-byte frame.
        assert!(confirm(header(3, 1, 9, 0), 417).unwrap());
        // MPEG-2 Layer III, 64 kbit/s at 22.05 kHz: 208-byte frame.
        assert!(confirm(header(2, 1, 8, 0), 208).unwrap());
    }

    #[test]
    fn mpeg_probe_rejects_layer_one_and_reserved_or_free_format_headers() {
        assert_eq!(
            confirm(header(3, 3, 8, 0), 417),
            Err(HlsFailure::Unsupported)
        );
        for invalid in [
            header(1, 2, 8, 0),
            header(3, 2, 15, 0),
            header(3, 2, 8, 3),
            header(3, 2, 0, 0),
        ] {
            assert_eq!(confirm(invalid, 417), Err(HlsFailure::Unsupported));
        }
    }

    #[test]
    fn mpeg_probe_does_not_scan_for_sync_or_accept_incomplete_or_unselected_data() {
        let valid = header(3, 2, 8, 0);
        let mut probe = MpegAudioProbe::new(AUDIO_PID);
        let mut payload = vec![0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0];
        payload.extend_from_slice(&[0x11; 9]); // Sync is off the PES ES boundary.
        payload.extend_from_slice(&valid);
        payload.resize(9 + 9 + 2 * 417, 0x55);
        let mut rejected = false;
        for (i, chunk) in payload.chunks(184).enumerate() {
            match probe.observe(&ts_payload(AUDIO_PID, i == 0, chunk, i as u8)) {
                Ok(false) => {}
                Ok(true) => panic!("a later sync candidate must not be scanned"),
                Err(HlsFailure::Unsupported) => rejected = true,
                Err(other) => panic!("unexpected probe error: {other:?}"),
            }
        }
        assert!(rejected, "invalid ES boundary header must be rejected");

        let mut incomplete = MpegAudioProbe::new(AUDIO_PID);
        let mut pes = vec![0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0];
        pes.extend_from_slice(&valid[..2]);
        assert!(!incomplete
            .observe(&ts_payload(AUDIO_PID, true, &pes, 0))
            .unwrap());
        assert!(!incomplete
            .observe(&ts_payload(AUDIO_PID + 1, true, &[0; 184], 0))
            .unwrap());
    }

    #[test]
    fn mpeg_probe_tracks_split_pes_and_es_headers_without_accepting_false_sync() {
        let h = header(3, 2, 8, 0);
        let mut probe = MpegAudioProbe::new(AUDIO_PID);
        let mut pes = vec![0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0];
        pes.extend_from_slice(&h[..2]);
        assert!(!probe
            .observe(&ts_payload(AUDIO_PID, true, &pes, 0))
            .unwrap());
        assert!(!probe
            .observe(&ts_payload(AUDIO_PID, false, &h[2..], 1))
            .unwrap());
        let mut rest = vec![0x33; 413];
        // Header for frame two starts exactly after the first frame's remaining bytes.
        rest.extend_from_slice(&h);
        rest.resize(rest.len() + 413, 0x44);
        let mut confirmed = false;
        for (i, chunk) in rest.chunks(184).enumerate() {
            confirmed |= probe
                .observe(&ts_payload(AUDIO_PID, false, chunk, (i + 2) as u8))
                .unwrap();
        }
        assert!(confirmed);
    }

    #[test]
    fn mpeg_probe_parses_optional_pes_header_split_across_transport_packets() {
        let h = header(3, 2, 8, 0);
        let mut probe = MpegAudioProbe::new(AUDIO_PID);
        let mut pes = vec![0, 0, 1, 0xc0, 0, 0, 0x80, 0, 5, 0x11, 0x22];
        assert!(!probe
            .observe(&ts_payload(AUDIO_PID, true, &pes, 0))
            .unwrap());
        pes.clear();
        pes.extend_from_slice(&[0x33, 0x44, 0x55]);
        pes.extend_from_slice(&h[..1]);
        assert!(!probe
            .observe(&ts_payload(AUDIO_PID, false, &pes, 1))
            .unwrap());
        let mut es = vec![h[1], h[2], h[3]];
        es.resize(3 + 413, 0x55);
        es.extend_from_slice(&h);
        es.resize(es.len() + 413, 0x66);
        let mut confirmed = false;
        for (i, chunk) in es.chunks(184).enumerate() {
            confirmed |= probe
                .observe(&ts_payload(AUDIO_PID, false, chunk, (i + 2) as u8))
                .unwrap();
        }
        assert!(confirmed);
    }

    #[test]
    fn classifier_gates_video_opening_on_supported_mpeg_layer_probe() {
        for (stream_type, layer, expected) in [
            (0x03, 2, true),
            (0x03, 1, true),
            (0x04, 1, true),
            (0x04, 3, false),
        ] {
            let mut classifier = Classifier::new();
            let pat = fixtures::psi_packet(0, 0, &fixtures::pat_section(7, 9, 4096));
            let pmt = fixtures::psi_packet(
                4096,
                2,
                &fixtures::pmt_section(9, 256, 256, 0x1b, AUDIO_PID, stream_type),
            );
            classifier.observe(pat).unwrap();
            classifier.observe(pmt).unwrap();
            assert!(classifier
                .observe(fixtures::video_access_packet(256))
                .unwrap()
                .is_none());
            let bitrate = if layer == 1 { 9 } else { 8 };
            let audio = pes_frames(header(3, layer, bitrate, 0), 417);
            let mut rejected = false;
            for packet in audio {
                match classifier.observe(packet) {
                    Ok(None) => {}
                    Ok(Some(_)) => panic!("audio packets cannot be video openings"),
                    Err(HlsFailure::Unsupported) => rejected = true,
                    Err(other) => panic!("unexpected classifier error: {other:?}"),
                }
            }
            assert_eq!(
                rejected, !expected,
                "PMT type 0x{stream_type:02x}, MPEG layer bits {layer}"
            );
            let video_open = classifier.observe(fixtures::video_access_packet(256));
            if expected {
                assert!(video_open.unwrap().is_some());
            } else {
                assert!(matches!(video_open, Err(HlsFailure::Unsupported)));
            }
        }

        let mut classifier = Classifier::new();
        classifier
            .observe(fixtures::psi_packet(
                0,
                0,
                &fixtures::pat_section(7, 9, 4096),
            ))
            .unwrap();
        classifier
            .observe(fixtures::psi_packet(
                4096,
                2,
                &fixtures::pmt_section(9, 256, 256, 0x1b, AUDIO_PID, 0x03),
            ))
            .unwrap();
        for packet in pes_frames(header(3, 2, 8, 0), 417) {
            classifier.observe(packet).unwrap();
        }
        assert!(classifier
            .observe(fixtures::video_access_packet(256))
            .unwrap()
            .is_some());
        assert!(
            matches!(
                classifier.observe(pes_frames(header(3, 3, 8, 0), 417)[0]),
                Err(HlsFailure::Unsupported)
            ),
            "a later Layer-I PES on the selected PID is rejected"
        );
    }
}
