//! Bounded live-HLS audio classification, normalization, and generation recovery.

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

pub struct ClassifiedOpening {
    pub layout: TransportLayout,
    pub tables: Vec<[u8; 188]>,
    pub packet: [u8; 188],
}

/// Pre-opening payload can never be replayed. Count it toward the scan ceiling,
/// discard it immediately, and retain only the strict selector's two fresh tables.
/// This avoids allocating a history whose sole eventual operation would be drop.
pub struct Classifier {
    selector: LiveAudioSelection,
    access: VideoAccessPointState,
    scanned: usize,
}
impl Default for Classifier {
    fn default() -> Self {
        let mut access = VideoAccessPointState::new();
        access.enable_mpeg2();
        Self {
            selector: LiveAudioSelection::default(),
            access,
            scanned: 0,
        }
    }
}
impl Classifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, packet: [u8; 188]) -> Result<Option<ClassifiedOpening>, HlsFailure> {
        self.scanned += 188;
        if self.scanned > process::INPUT_BYTES {
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

pub struct RecoveryWindow {
    invalidations: VecDeque<Instant>,
    generation: GenerationId,
    paired_marker: bool,
}
impl Default for RecoveryWindow {
    fn default() -> Self {
        Self {
            invalidations: VecDeque::new(),
            generation: GenerationId(1),
            paired_marker: false,
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
        if self.invalidations.len() == 3 {
            return Err(HlsFailure::RecoveryLimit);
        }
        self.invalidations.push_back(now);
        self.generation.0 += 1;
        self.paired_marker = reason == RecoveryReason::Source;
        Ok(self.generation)
    }
    pub fn opening(&mut self) {
        self.paired_marker = false;
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
    let deadline = Instant::now() + Duration::from_millis(config.startup_timeout_ms);
    tracker.spawn(move |cancel| async move {
        match prepared {
            None => media.run_native(rx, cancel).await,
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
                if let Err(reason) = worker.run(cancel).await {
                    media.fail_generation(worker.recovery.generation, reason);
                } else {
                    media.stop();
                }
            }
        }
    })
}

enum SourceItem {
    Packet([u8; 188]),
    Recovery(RecoveryReason),
    Closed,
}
struct PacketSource {
    rx: broadcast::Receiver<StreamEvent>,
    chunk: Option<bytes::Bytes>,
    offset: usize,
    partial: [u8; 188],
    staged: usize,
}
impl PacketSource {
    fn new(rx: broadcast::Receiver<StreamEvent>) -> Self {
        Self {
            rx,
            chunk: None,
            offset: 0,
            partial: [0; 188],
            staged: 0,
        }
    }
    fn discard_partial(&mut self) {
        self.staged = 0;
    }
    async fn next(&mut self) -> SourceItem {
        loop {
            if let Some(chunk) = &self.chunk {
                let n = (188 - self.staged).min(chunk.len() - self.offset);
                self.partial[self.staged..self.staged + n]
                    .copy_from_slice(&chunk[self.offset..self.offset + n]);
                self.offset += n;
                self.staged += n;
                if self.offset == chunk.len() {
                    self.chunk = None;
                    self.offset = 0;
                }
                if self.staged == 188 {
                    self.staged = 0;
                    return SourceItem::Packet(self.partial);
                }
            } else {
                match self.rx.recv().await {
                    Ok(StreamEvent::Data(chunk)) => {
                        self.chunk = Some(chunk);
                        self.offset = 0;
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

struct EnabledWorker {
    media: Arc<HlsPackager>,
    prepared: PreparedNormalizer,
    source: PacketSource,
    recovery: RecoveryWindow,
    config: HlsConfig,
    deadline: Instant,
    seed: Option<[u8; 188]>,
    budget: InputBudget,
}
enum Turn {
    Recover(RecoveryReason, Option<[u8; 188]>),
    Stop,
}
impl EnabledWorker {
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
        // A recovery while still starting preserves that original absolute clock.
        if self.media.status() == HlsStatus::Ready {
            self.deadline = Instant::now() + Duration::from_millis(self.config.startup_timeout_ms);
        }
        self.media.begin_generation(generation, self.deadline, true);
        self.source.discard_partial();
        self.seed = seed;
        Ok(())
    }
    async fn run(&mut self, mut cancel: watch::Receiver<bool>) -> Result<(), HlsFailure> {
        self.media
            .begin_generation(self.recovery.generation, self.deadline, false);
        loop {
            let mut classifier = Classifier::new();
            let opening = loop {
                if *cancel.borrow() {
                    return Ok(());
                }
                let item = if let Some(packet) = self.seed.take() {
                    SourceItem::Packet(packet)
                } else {
                    tokio::select! { biased;
                        _ = process::cancelled(&mut cancel) => return Ok(()),
                        _ = tokio::time::sleep_until(self.deadline) => return Err(HlsFailure::Deadline),
                        item = self.source.next() => item,
                    }
                };
                match item {
                    SourceItem::Closed => return Ok(()),
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
            self.recovery.opening();
            let transform = opening
                .layout
                .audio
                .as_ref()
                .is_some_and(|a| matches!(a.codec, AudioCodec::Ac3 | AudioCodec::MpegAudio));
            let mut selector = classifier.selector;
            let layout = opening.layout;
            let mut initial = opening.tables;
            initial.push(opening.packet);
            if !transform {
                for packet in initial {
                    self.media
                        .feed_generation(self.recovery.generation, &packet);
                }
                match AssertUnwindSafe(self.produce(&mut selector, &layout, None, &mut cancel))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(HlsFailure::Pump))?
                {
                    Turn::Stop => return Ok(()),
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
                let child = run_child(
                    self.prepared.clone(),
                    layout.clone(),
                    self.recovery.generation,
                    self.media.clone(),
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
                        self.produce(&mut selector, &layout, Some(&tx), &mut cancel)
                            .await
                    })
                    .catch_unwind();
                    tokio::pin!(producer);
                    tokio::select! { biased;
                        result = &mut child => { result?; return Ok(()); },
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
                        return other.map(|_| ());
                    }
                }
            }
        }
    }
    async fn produce(
        &mut self,
        selector: &mut LiveAudioSelection,
        layout: &TransportLayout,
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
            let item = tokio::select! { biased;
                _ = process::cancelled(cancel) => return Ok(Turn::Stop),
                _ = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending().await } } => return Err(HlsFailure::Deadline),
                item = self.source.next() => item,
            };
            match item {
                SourceItem::Closed => return Ok(Turn::Stop),
                SourceItem::Recovery(reason) => return Ok(Turn::Recover(reason, None)),
                SourceItem::Packet(mut packet) => {
                    if !validate_packet(&packet)? {
                        continue;
                    }
                    if ts_timing(&packet).discontinuity {
                        return Ok(Turn::Recover(RecoveryReason::Transport, None));
                    }
                    selector.observe(packet);
                    match selector.layout() {
                        LayoutDecision::Unsupported => return Err(HlsFailure::Unsupported),
                        LayoutDecision::Supported(current) if current == *layout => {}
                        _ => return Ok(Turn::Recover(RecoveryReason::Layout, Some(packet))),
                    }
                    clear_marker(&mut packet);
                    if let Some(sender) = sender {
                        let input = self.budget.acquire_copy(&packet, cancel).await?;
                        tokio::select! { biased;
                            _ = process::cancelled(cancel) => return Ok(Turn::Stop),
                            sent = sender.send(input) => sent.map_err(|_|HlsFailure::Pipe)?,
                        }
                    } else {
                        self.media
                            .feed_generation(self.recovery.generation, &packet);
                    }
                }
            }
        }
    }
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
        tx.send(StreamEvent::Data(Bytes::from(vec![0x11; 100])))
            .unwrap();
        tx.send(StreamEvent::Discontinuity).unwrap();
        let fresh = [0x47; 188];
        tx.send(StreamEvent::Data(Bytes::copy_from_slice(&fresh)))
            .unwrap();
        assert!(matches!(
            source.next().await,
            SourceItem::Recovery(RecoveryReason::Source)
        ));
        assert!(matches!(source.next().await, SourceItem::Packet(packet) if packet == fresh));
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
        // packet would cross the 1 MiB cap, so no later opening can be accepted.
        for _ in 0..(process::INPUT_BYTES / 188) {
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
        recovery.opening();
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
